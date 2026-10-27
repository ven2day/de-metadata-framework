"""Small, testable DhanHQ v2 REST/auth client.

Market APIs accept a 24-hour access token. API key/secret credentials are used
only by Dhan's browser-consent flow and are deliberately kept separate from
normal market requests.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import tempfile
import threading
from typing import Callable, Iterable

import requests

from ingestion.env.DE_Ingestion_properties import (
    DHAN_ACCESS_TOKEN,
    DHAN_ACCESS_TOKEN_FILE,
    DHAN_API_KEY,
    DHAN_API_SECRET,
    DHAN_CLIENT_ID,
    DHAN_HTTP_TIMEOUT_SECONDS,
    DHAN_PIN,
    DHAN_TOTP_SECRET,
)


AUTH_BASE_URL = "https://auth.dhan.co"
API_BASE_URL = "https://api.dhan.co/v2"
ALLOWED_NSE_SEGMENTS = frozenset({"IDX_I", "NSE_EQ", "NSE_FNO", "NSE_CURRENCY"})
_AUTH_EXPECTED_FIELDS = {
    "access-token generation": "accessToken",
    "OAuth consent generation": "consentAppId",
    "OAuth consent consumption": "accessToken",
}
_AUTH_CODE_CATEGORIES = {
    "DH-901": "authentication_rejected", "DH-902": "access_denied",
    "DH-903": "account_rejected", "DH-904": "rate_limited",
    "DH-905": "invalid_request", "DH-906": "invalid_request",
    "DH-907": "invalid_request", "DH-908": "service_unavailable",
    "DH-909": "service_unavailable", "DH-910": "provider_rejected",
}
_AUTH_CATEGORIES = frozenset({
    "authentication_rejected", "access_denied", "account_rejected",
    "rate_limited", "invalid_request", "service_unavailable",
    "provider_rejected", "unexpected_response", "transport_error",
    "pin_rejected", "totp_rejected", "totp_setup_required",
    "client_id_rejected", "account_locked",
})
_AUTH_RESPONSE_KINDS = frozenset({
    "json_object", "json_array", "json_scalar", "invalid_json", "no_response",
})


def safe_auth_diagnostic(value: dict | None) -> dict:
    """Export fixed diagnostic fields only, including at the final CLI boundary.

    Even error codes can contain credentials. Only documented finite code
    constants are allowed, never arbitrary numeric/string provider values.
    """
    if not isinstance(value, dict) or not value:
        return {}
    status = value.get("http_status")
    category = value.get("category")
    code = value.get("provider_code")
    kind = value.get("response_kind")
    expected = value.get("expected_field")
    return {
        "http_status": status if type(status) is int and 100 <= status <= 599 else None,
        "category": category if isinstance(category, str) and category in _AUTH_CATEGORIES else "unexpected_response",
        "provider_code": code if isinstance(code, str) and code in _AUTH_CODE_CATEGORIES else None,
        "response_kind": kind if isinstance(kind, str) and kind in _AUTH_RESPONSE_KINDS else "no_response",
        "error_reported": value.get("error_reported") is True,
        "expected_field": expected if isinstance(expected, str) and expected in {"accessToken", "consentAppId", "dhanClientId"} else None,
    }


class DhanConfigError(ValueError):
    """The local Dhan configuration is incomplete or invalid."""


class DhanApiError(RuntimeError):
    """A sanitized error returned by DhanHQ."""

    def __init__(self, message: str, *, status_code: int | None = None,
                 diagnostic: dict | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.diagnostic = safe_auth_diagnostic(diagnostic)


def _is_retryable_api_error(exc: DhanApiError) -> bool:
    """Retry transport failures, throttling, and server-side failures only."""
    return (
        exc.status_code is None
        or exc.status_code == 429
        or exc.status_code >= 500
    )


@dataclass(frozen=True)
class DhanInstrument:
    exchange_segment: str
    security_id: str


@dataclass(frozen=True)
class DhanAccessToken:
    value: str
    client_id: str
    expiry_time: str | None = None


def parse_instruments(
    value: str | Iterable[str], *, max_instruments: int = 5000
) -> list[DhanInstrument]:
    """Parse ``SEGMENT:SECURITY_ID`` entries, preserving stable input order."""
    if isinstance(value, str):
        if not value.strip():
            raise DhanConfigError(
                "No Dhan instruments configured; set DHAN_INSTRUMENTS to entries "
                "such as NSE_EQ:1333"
            )
        raw_items = value.split(",")
    else:
        raw_items = list(value)

    instruments: list[DhanInstrument] = []
    seen: set[tuple[str, str]] = set()
    for raw in raw_items:
        item = str(raw).strip()
        if not item or item.count(":") != 1:
            raise DhanConfigError(
                f"Invalid Dhan instrument {item!r}; expected SEGMENT:SECURITY_ID"
            )
        segment, security_id = (part.strip() for part in item.split(":", 1))
        segment = segment.upper()
        if segment not in ALLOWED_NSE_SEGMENTS:
            raise DhanConfigError(
                f"Unsupported NSE segment {segment!r}; allowed: "
                f"{', '.join(sorted(ALLOWED_NSE_SEGMENTS))}"
            )
        # Dhan SecurityId is transported as a string on WebSocket and numeric in
        # REST payloads. Keep the canonical representation as a digit string.
        if not security_id.isdigit() or int(security_id) <= 0:
            raise DhanConfigError(
                f"Invalid Dhan SecurityId {security_id!r} in {item!r}; use the "
                "numeric Dhan SecurityId, not a ticker symbol"
            )
        canonical = str(int(security_id))
        key = (segment, canonical)
        if key not in seen:
            instruments.append(DhanInstrument(segment, canonical))
            seen.add(key)

    if not instruments:
        raise DhanConfigError("At least one Dhan instrument is required")
    if len(instruments) > max_instruments:
        raise DhanConfigError(
            f"Configured {len(instruments)} instruments; maximum is {max_instruments}"
        )
    return instruments


def instruments_payload(instruments: Iterable[DhanInstrument]) -> dict[str, list[int]]:
    payload: dict[str, list[int]] = {}
    for instrument in instruments:
        payload.setdefault(instrument.exchange_segment, []).append(
            int(instrument.security_id)
        )
    return payload


def _auth_error_reported(payload) -> bool:
    if not isinstance(payload, dict):
        return False
    layers = [payload]
    if isinstance(payload.get("data"), dict):
        layers.append(payload["data"])
    for layer in layers:
        for key in ("status", "success"):
            value = layer.get(key)
            if value is False or (isinstance(value, str) and value.strip().lower() in {"failure", "failed", "error", "rejected", "denied", "false"}):
                return True
        for key in ("errorCode", "error_code", "errorType", "errorMessage", "error", "errors"):
            value = layer.get(key)
            if value and value != "0":
                return True
        if isinstance(layer.get("code"), str) and layer["code"].strip().upper() in _AUTH_CODE_CATEGORIES:
            return True
    return False


def _auth_reported_hint(payload) -> str | None:
    """Classify explicit provider phrases without exporting provider text.

    These are hints about what the response reports, not independently verified
    credential failures. Ambiguous PIN-or-TOTP wording remains generic.
    """
    if not isinstance(payload, dict):
        return None
    layers = [payload] + ([payload["data"]] if isinstance(payload.get("data"), dict) else [])
    hints = set()
    for layer in layers:
        for key in ("errorMessage", "error_message", "message", "errorType"):
            value = layer.get(key)
            if not isinstance(value, str):
                continue
            text = " ".join(value[:4096].lower().replace("_", " ").replace("-", " ").split())
            if re.search(r"\bpin\s*(?:or|and|/|\||,)\s*(?:totp|otp)\b", text):
                return None
            if re.search(r"\btotp\s+(?:is\s+)?not\s+(?:enabled|registered|set\s*up)\b", text):
                hints.add("totp_setup_required")
            if re.search(r"\b(?:invalid|incorrect|wrong)\s*(?:m?pin)\b|\bpin\s+(?:is\s+)?(?:invalid|incorrect|mismatch)\b", text):
                hints.add("pin_rejected")
            if re.search(r"\b(?:invalid|incorrect|wrong|expired)\s*(?:totp|otp)\b|\b(?:totp|otp)\s+(?:is\s+)?(?:invalid|incorrect|expired|mismatch)\b", text):
                hints.add("totp_rejected")
            if re.search(r"\b(?:invalid|incorrect|unknown)\s+client\s*id\b", text):
                hints.add("client_id_rejected")
            if re.search(r"\baccount\s+(?:is\s+)?(?:locked|blocked)\b", text):
                hints.add("account_locked")
    return next(iter(hints)) if len(hints) == 1 else None


def _auth_diagnostic(payload, status: int | None, operation: str, *,
                     response_kind: str | None = None) -> dict:
    kind = response_kind or ("json_object" if isinstance(payload, dict) else "json_array" if isinstance(payload, list) else "json_scalar")
    code = None
    if isinstance(payload, dict):
        layers = [payload] + ([payload["data"]] if isinstance(payload.get("data"), dict) else [])
        for layer in layers:
            for key in ("errorCode", "error_code", "code"):
                value = layer.get(key)
                if isinstance(value, str) and value.strip().upper() in _AUTH_CODE_CATEGORIES:
                    code = value.strip().upper()
                    break
            if code:
                break
    hint = _auth_reported_hint(payload)
    reported = _auth_error_reported(payload) or hint is not None
    if status == 429:
        category = "rate_limited"
    elif status is not None and status >= 500:
        category = "service_unavailable"
    elif hint:
        category = hint
    elif status in {401, 403}:
        category = "authentication_rejected"
    elif code:
        category = _AUTH_CODE_CATEGORIES[code]
    elif status in {400, 422}:
        category = "invalid_request"
    elif status is None:
        category = "transport_error"
    elif reported:
        category = "provider_rejected"
    else:
        category = "unexpected_response"
    return safe_auth_diagnostic({
        "http_status": status, "category": category, "provider_code": code,
        "response_kind": kind, "error_reported": reported,
        "expected_field": _AUTH_EXPECTED_FIELDS.get(operation),
    })


def _auth_response_error(payload, status: int | None, operation: str) -> DhanApiError:
    diagnostic = _auth_diagnostic(payload, status, operation)
    hint = "Dhan did not issue the expected authentication result."
    if diagnostic["error_reported"]:
        hint = "Dhan reported an authentication-flow error; no token or consent was accepted."
    return DhanApiError(
        f"Dhan {operation} failed: {hint} No manual access token is required for PIN/TOTP login.",
        status_code=status, diagnostic=diagnostic,
    )


def _response_json(response: requests.Response, operation: str) -> dict:
    decoded = True
    try:
        payload = response.json()
    except (ValueError, json.JSONDecodeError):
        decoded = False
        payload = None
    if 200 <= response.status_code < 300 and isinstance(payload, dict):
        if operation in _AUTH_EXPECTED_FIELDS:
            expected_value = payload.get(_AUTH_EXPECTED_FIELDS[operation])
            if (response.status_code != 200 or _auth_error_reported(payload)
                    or not isinstance(expected_value, str) or not expected_value.strip()):
                raise _auth_response_error(payload, response.status_code, operation) from None
        return payload

    # Even a nominal errorCode/errorMessage may echo bearer tokens, PINs,
    # TOTP seeds, or a complete authentication URL. Do not copy any upstream
    # body value into an exception that may reach a log or browser stream.
    status = response.status_code
    if status in {401, 403}:
        message = "authentication or access was rejected; check credentials and account permissions"
    elif status == 429:
        message = "request rate limit reached; retry later"
    elif status >= 500:
        message = "the service is temporarily unavailable; retry later"
    elif status in {400, 422}:
        message = "the request was rejected; check the configured inputs"
    elif 200 <= status < 300:
        message = "the response was not a JSON object"
    else:
        message = "the request could not be completed"
    raise DhanApiError(
        f"Dhan {operation} failed (HTTP {status}): {message}",
        status_code=status,
        diagnostic=_auth_diagnostic(payload, status, operation,
                                    response_kind=None if decoded else "invalid_json")
        if operation in _AUTH_EXPECTED_FIELDS else None,
    ) from None


def _post(
    session: requests.Session,
    url: str,
    *,
    operation: str,
    timeout: int,
    **kwargs,
) -> dict:
    try:
        response = session.post(url, timeout=timeout, **kwargs)
    except requests.RequestException:
        # Do not include ``exc``: auth endpoint query strings contain PIN/TOTP.
        raise DhanApiError(
            f"Dhan {operation} request could not be completed",
            diagnostic=_auth_diagnostic(None, None, operation, response_kind="no_response")
            if operation in _AUTH_EXPECTED_FIELDS else None,
        ) from None
    return _response_json(response, operation)


def _get(
    session: requests.Session,
    url: str,
    *,
    operation: str,
    timeout: int,
    **kwargs,
) -> dict:
    try:
        response = session.get(url, timeout=timeout, **kwargs)
    except requests.RequestException:
        raise DhanApiError(
            f"Dhan {operation} request could not be completed",
            diagnostic=_auth_diagnostic(None, None, operation, response_kind="no_response")
            if operation in _AUTH_EXPECTED_FIELDS else None,
        ) from None
    return _response_json(response, operation)


def generate_access_token(
    client_id: str,
    pin: str,
    totp_code: str,
    *,
    session: requests.Session | None = None,
    timeout: int = DHAN_HTTP_TIMEOUT_SECONDS,
) -> DhanAccessToken:
    """Use Dhan's direct client-id/PIN/current-TOTP endpoint."""
    client_id, pin, totp_code = client_id.strip(), pin.strip(), totp_code.strip()
    if not client_id:
        raise DhanConfigError("DHAN_CLIENT_ID is required")
    if len(pin) != 6 or not pin.isascii() or not pin.isdigit():
        raise DhanConfigError("Dhan PIN must be a six-digit string")
    if len(totp_code) != 6 or not totp_code.isascii() or not totp_code.isdigit():
        raise DhanConfigError("Current Dhan TOTP must be a six-digit string")

    http = session or requests.Session()
    payload = _post(
        http,
        f"{AUTH_BASE_URL}/app/generateAccessToken",
        operation="access-token generation",
        timeout=timeout,
        params={"dhanClientId": client_id, "pin": pin, "totp": totp_code},
    )
    token = payload.get("accessToken")
    if not isinstance(token, str) or not token.strip():
        raise _auth_response_error(payload, 200, "access-token generation") from None
    returned_client = payload["dhanClientId"] if "dhanClientId" in payload else client_id
    expiry = payload.get("expiryTime")
    if (not isinstance(returned_client, str) or returned_client.strip() != client_id
            or (expiry is not None and not isinstance(expiry, str))):
        raise _auth_response_error(payload, 200, "access-token generation") from None
    parsed_expiry = _parse_expiry(expiry) if expiry else None
    if expiry and parsed_expiry is None:
        raise _auth_response_error(payload, 200, "access-token generation") from None
    return DhanAccessToken(token.strip(), returned_client.strip(), parsed_expiry.isoformat() if parsed_expiry else None)


def generate_oauth_consent(
    client_id: str,
    api_key: str,
    api_secret: str,
    *,
    session: requests.Session | None = None,
    timeout: int = DHAN_HTTP_TIMEOUT_SECONDS,
) -> str:
    if not all((client_id.strip(), api_key.strip(), api_secret.strip())):
        raise DhanConfigError(
            "DHAN_CLIENT_ID, DHAN_API_KEY, and DHAN_API_SECRET are required"
        )
    payload = _post(
        session or requests.Session(),
        f"{AUTH_BASE_URL}/app/generate-consent",
        operation="OAuth consent generation",
        timeout=timeout,
        params={"client_id": client_id.strip()},
        headers={"app_id": api_key.strip(), "app_secret": api_secret.strip()},
    )
    consent_id = payload.get("consentAppId")
    if not isinstance(consent_id, str) or not consent_id.strip():
        raise _auth_response_error(payload, 200, "OAuth consent generation") from None
    return consent_id.strip()


def consume_oauth_consent(
    token_id: str,
    api_key: str,
    api_secret: str,
    *,
    session: requests.Session | None = None,
    timeout: int = DHAN_HTTP_TIMEOUT_SECONDS,
) -> DhanAccessToken:
    if not all((token_id.strip(), api_key.strip(), api_secret.strip())):
        raise DhanConfigError("tokenId, DHAN_API_KEY, and DHAN_API_SECRET are required")
    payload = _get(
        session or requests.Session(),
        f"{AUTH_BASE_URL}/app/consumeApp-consent",
        operation="OAuth consent consumption",
        timeout=timeout,
        params={"tokenId": token_id.strip()},
        headers={"app_id": api_key.strip(), "app_secret": api_secret.strip()},
    )
    token = payload.get("accessToken")
    client_id = payload.get("dhanClientId")
    expiry = payload.get("expiryTime")
    if (not isinstance(token, str) or not token.strip()
            or not isinstance(client_id, str) or not client_id.strip()
            or (expiry is not None and not isinstance(expiry, str))):
        raise _auth_response_error(payload, 200, "OAuth consent consumption") from None
    parsed_expiry = _parse_expiry(expiry) if expiry else None
    if expiry and parsed_expiry is None:
        raise _auth_response_error(payload, 200, "OAuth consent consumption") from None
    return DhanAccessToken(token.strip(), client_id.strip(), parsed_expiry.isoformat() if parsed_expiry else None)


def _parse_expiry(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        # Dhan documents expiryTime as IST.
        from zoneinfo import ZoneInfo

        parsed = parsed.replace(tzinfo=ZoneInfo("Asia/Kolkata"))
    try:
        return parsed.astimezone(timezone.utc)
    except (OverflowError, ValueError):
        return None


def _jwt_expiry(value: str) -> str | None:
    """Read an unverified JWT exp claim only as a local refresh hint."""
    try:
        encoded = value.split(".", 2)[1]
        encoded += "=" * (-len(encoded) % 4)
        payload = json.loads(base64.urlsafe_b64decode(encoded).decode("utf-8"))
        expiry = datetime.fromtimestamp(int(payload["exp"]), tz=timezone.utc)
        return expiry.isoformat()
    except (IndexError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        return None


def read_token_file(path: str) -> DhanAccessToken | None:
    token_path = Path(path)
    if not path or not token_path.is_file():
        return None
    try:
        payload = json.loads(token_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise DhanConfigError(
            f"Dhan token file {token_path} is unreadable or malformed"
        ) from exc
    if not isinstance(payload, dict):
        raise DhanConfigError(
            f"Dhan token file {token_path} must contain a JSON object"
        )
    token = str(payload.get("access_token") or payload.get("accessToken") or "").strip()
    client_id = str(payload.get("client_id") or payload.get("dhanClientId") or "").strip()
    expiry = payload.get("expiry_time") or payload.get("expiryTime")
    if expiry is not None and not isinstance(expiry, str):
        raise DhanConfigError(
            f"Dhan token file {token_path} has a non-string expiry time"
        )
    if not token:
        raise DhanConfigError(f"Dhan token file {token_path} has no access token")
    return DhanAccessToken(token, client_id, expiry)


def write_token_file(token: DhanAccessToken, path: str) -> None:
    """Atomically persist a token without printing it or placing it in Git."""
    if not path:
        raise DhanConfigError("DHAN_ACCESS_TOKEN_FILE is empty")
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "access_token": token.value,
        "client_id": token.client_id,
        "expiry_time": token.expiry_time,
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }
    fd, temporary = tempfile.mkstemp(prefix=f".{target.name}.", dir=str(target.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(data, handle)
            handle.write("\n")
        try:
            os.chmod(temporary, 0o600)
        except OSError:
            pass
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class DhanTokenProvider:
    """Resolve a token in order: environment, token file, then PIN/TOTP seed."""

    def __init__(
        self,
        *,
        client_id: str = DHAN_CLIENT_ID,
        access_token: str = DHAN_ACCESS_TOKEN,
        token_file: str = DHAN_ACCESS_TOKEN_FILE,
        pin: str = DHAN_PIN,
        totp_secret: str = DHAN_TOTP_SECRET,
        session: requests.Session | None = None,
        refresh_skew_seconds: int = 60,
    ):
        self.client_id = client_id.strip()
        self.access_token = access_token.strip()
        self.token_file = token_file.strip()
        self.pin = pin.strip()
        self.totp_secret = totp_secret.replace(" ", "").strip()
        self.session = session or requests.Session()
        self.refresh_skew = timedelta(seconds=refresh_skew_seconds)
        self._lock = threading.Lock()
        self._cached: DhanAccessToken | None = None

    def _usable(self, token: DhanAccessToken | None) -> bool:
        if token is None or not token.value:
            return False
        expires = _parse_expiry(token.expiry_time)
        return expires is None or expires > datetime.now(timezone.utc) + self.refresh_skew

    def _generate(self) -> DhanAccessToken:
        missing = [
            name
            for name, value in (
                ("DHAN_CLIENT_ID", self.client_id),
                ("DHAN_PIN", self.pin),
                ("DHAN_TOTP_SECRET", self.totp_secret),
            )
            if not value
        ]
        if missing:
            suffix = (
                " API key/secret require the separate browser-consent flow."
                if DHAN_API_KEY or DHAN_API_SECRET
                else ""
            )
            raise DhanConfigError(
                "No usable Dhan access token. Configure DHAN_ACCESS_TOKEN or a "
                "token file; headless generation additionally requires "
                + ", ".join(missing)
                + "."
                + suffix
            )
        try:
            import pyotp
        except ImportError as exc:
            raise DhanConfigError(
                "pyotp is required for DHAN_TOTP_SECRET token generation"
            ) from exc
        try:
            code = pyotp.TOTP(self.totp_secret).now()
        except Exception:
            # pyotp/base64 errors can echo the supplied seed. Keep secrets out
            # of both application logs and chained tracebacks.
            raise DhanConfigError(
                "DHAN_TOTP_SECRET is not a valid Base32 TOTP seed"
            ) from None
        token = generate_access_token(
            self.client_id, self.pin, code, session=self.session
        )
        if self.token_file:
            write_token_file(token, self.token_file)
        return token

    def get_token(self, *, force_refresh: bool = False) -> DhanAccessToken:
        with self._lock:
            if not force_refresh and self._usable(self._cached):
                return self._cached  # type: ignore[return-value]
            if not force_refresh and self.access_token:
                candidate = DhanAccessToken(
                    self.access_token,
                    self.client_id,
                    _jwt_expiry(self.access_token),
                )
                if self._usable(candidate):
                    self._cached = candidate
                    return self._cached
            if self.token_file:
                file_token = read_token_file(self.token_file)
                file_changed = bool(
                    file_token
                    and (
                        self._cached is None
                        or file_token.value != self._cached.value
                    )
                )
                if self._usable(file_token) and (not force_refresh or file_changed):
                    if self.client_id and file_token and file_token.client_id not in (
                        "",
                        self.client_id,
                    ):
                        raise DhanConfigError(
                            "DHAN_CLIENT_ID does not match the configured token file"
                        )
                    self._cached = DhanAccessToken(
                        file_token.value,
                        file_token.client_id or self.client_id,
                        file_token.expiry_time,
                    )
                    return self._cached
            # If an explicit token was supplied and there are no headless renewal
            # credentials, return it on a reconnect; profile validation will give
            # the actionable expiry/auth error.
            if force_refresh and self.access_token and not (
                self.pin and self.totp_secret
            ):
                return DhanAccessToken(self.access_token, self.client_id, None)
            self._cached = self._generate()
            return self._cached


class DhanRestClient:
    def __init__(
        self,
        token: DhanAccessToken,
        *,
        session: requests.Session | None = None,
        timeout: int = DHAN_HTTP_TIMEOUT_SECONDS,
        provider: DhanTokenProvider | None = None,
    ):
        if not token.client_id:
            raise DhanConfigError("DHAN_CLIENT_ID is required for market data")
        self.provider = provider
        self.session = session or requests.Session()
        self.timeout = timeout
        self._set_token(token)

    def _set_token(self, token: DhanAccessToken) -> None:
        self.token = token
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "access-token": token.value,
            "client-id": token.client_id,
        }

    @classmethod
    def from_env(cls) -> "DhanRestClient":
        provider = DhanTokenProvider()
        return cls(
            provider.get_token(),
            session=provider.session,
            provider=provider,
        )

    def _with_auth_refresh(self, request: Callable[[], dict]) -> dict:
        try:
            return request()
        except DhanApiError as exc:
            if exc.status_code not in {401, 403} or self.provider is None:
                raise
            self._set_token(self.provider.get_token(force_refresh=True))
            return request()

    def profile(self) -> dict:
        # Official profile endpoint requires only the access-token header.
        return self._with_auth_refresh(
            lambda: _get(
                self.session,
                f"{API_BASE_URL}/profile",
                operation="profile",
                timeout=self.timeout,
                headers={"Accept": "application/json", "access-token": self.token.value},
            )
        )

    def validate_market_data_access(self) -> dict:
        profile = self.profile()
        profile_client_id = str(profile.get("dhanClientId") or "").strip()
        if not profile_client_id:
            raise DhanConfigError("Dhan profile did not report dhanClientId")
        if profile_client_id != self.token.client_id:
            raise DhanConfigError(
                "Dhan profile client ID does not match DHAN_CLIENT_ID"
            )
        plan = str(profile.get("dataPlan") or "").strip().lower()
        if plan != "active":
            raise DhanConfigError(
                "Dhan Data API plan is not active; live NSE quote/feed access requires an active data plan"
            )
        return profile

    def market_snapshot(
        self, instruments: list[DhanInstrument], mode: str = "quote"
    ) -> dict:
        mode = mode.strip().lower()
        if mode not in {"ltp", "ohlc", "quote"}:
            raise DhanConfigError("Dhan snapshot mode must be ltp, ohlc, or quote")
        if len(instruments) > 1000:
            raise DhanConfigError(
                "Dhan Market Quote accepts at most 1000 instruments per request"
            )
        payload = self._with_auth_refresh(
            lambda: _post(
                self.session,
                f"{API_BASE_URL}/marketfeed/{mode}",
                operation=f"marketfeed/{mode}",
                timeout=self.timeout,
                headers=self.headers,
                json=instruments_payload(instruments),
            )
        )
        if str(payload.get("status") or "").lower() != "success":
            raise DhanApiError(f"Dhan marketfeed/{mode} returned a non-success status")
        if not isinstance(payload.get("data"), dict):
            raise DhanApiError(f"Dhan marketfeed/{mode} response has no data object")
        return payload
