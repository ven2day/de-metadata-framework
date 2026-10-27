"""Read-only, non-secret Dhan setup inspection and snapshot form validation.

Setup inspection never constructs an API client, mints a token, or contacts Dhan.
Explicit snapshot form submission can resolve symbols against the public master.
Expiry claims are local hints, not authentication or entitlement verification.
"""

from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import re
from typing import Mapping
from zoneinfo import ZoneInfo

from ingestion.dhan.client import _jwt_expiry, _parse_expiry, parse_instruments, read_token_file
from ingestion.dhan.instruments import configured_selection, inspect_selection, resolve_selection


TIMEZONE = "Asia/Kolkata"
_DEFAULT_TOKEN_FILE = "/run/secrets/dhan-access-token.json"
_MODES = frozenset({"quote", "ohlc", "ltp"})
_CREDENTIAL_FIELDS = frozenset({
    "dhan_client_id", "dhan_access_token", "dhan_access_token_file",
    "dhan_api_key", "dhan_api_secret", "dhan_redirect_url", "dhan_pin", "dhan_totp_secret",
    "dhan_symbols_file", "symbols_file",
    "client_id", "access_token", "api_key", "api_secret", "secret", "pin", "totp", "totp_secret",
})


def india_date(now: datetime | None = None) -> str:
    instant = now or datetime.now(timezone.utc)
    return instant.astimezone(ZoneInfo(TIMEZONE)).date().isoformat()


def _text(environment: Mapping[str, str], name: str, default: str = "") -> str:
    return str(environment.get(name, default) or "").strip()


def _expiry(token: str, explicit: str | None, now: datetime) -> tuple[bool | None, bool]:
    """Return expired/unknown plus usable beyond the runtime's refresh skew."""
    try:
        hint = explicit or _jwt_expiry(token)
        parsed = _parse_expiry(hint)
    except Exception:
        return None, False
    if hint and parsed is None:
        return None, False
    if parsed is None:
        return None, True
    return parsed <= now, parsed > now + timedelta(seconds=60)


def _valid_seed(seed: str) -> bool:
    normalized = seed.replace(" ", "").upper()
    if len(normalized.rstrip("=")) < 16:
        return False
    try:
        return bool(base64.b32decode(normalized + "=" * (-len(normalized) % 8), casefold=True))
    except (ValueError, TypeError):
        return False


def reject_credentials(form: Mapping[str, str]) -> None:
    if any(str(key).lower() in _CREDENTIAL_FIELDS for key in form):
        raise ValueError("Dhan credentials must be configured on the server; this form does not accept credentials.")


def inspect_setup(environment: Mapping[str, str] | None = None,
                  now: datetime | None = None) -> dict:
    environment = os.environ if environment is None else environment
    instant = now or datetime.now(timezone.utc)
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=timezone.utc)
    missing: list[str] = []
    configured_client = _text(environment, "DHAN_CLIENT_ID")
    access_token = _text(environment, "DHAN_ACCESS_TOKEN")
    pin = _text(environment, "DHAN_PIN")
    seed = _text(environment, "DHAN_TOTP_SECRET")
    pin_totp = bool(re.fullmatch(r"[0-9]{6}", pin) and _valid_seed(seed))
    oauth = bool(_text(environment, "DHAN_API_KEY") and _text(environment, "DHAN_API_SECRET"))
    token_file = _text(environment, "DHAN_ACCESS_TOKEN_FILE", _DEFAULT_TOKEN_FILE)
    try:
        file_present = bool(token_file and Path(token_file).is_file())
    except OSError:
        file_present = False
    token_expired = None
    method = "missing"
    auth_ready = False
    client_ready = bool(configured_client)
    issue = ""

    # Match runtime precedence: a usable environment token wins before the file.
    if access_token:
        token_expired, usable = _expiry(access_token, None, instant)
        if usable:
            method, auth_ready = "access_token", client_ready
        else:
            issue = "The configured access token is expired, expiring, or has an invalid expiry hint."
    if method == "missing" and file_present:
        try:
            stored = read_token_file(token_file)
            if stored is None:
                raise ValueError("Unavailable token")
            token_expired, usable = _expiry(stored.value, stored.expiry_time, instant)
            mismatch = bool(configured_client and stored.client_id and stored.client_id != configured_client)
            client_ready = bool(stored.client_id or configured_client)
            if mismatch:
                method, issue = "token_file", "The configured client ID and saved token identity do not match."
                missing.append("DHAN_CLIENT_ID")
            elif usable:
                method, auth_ready = "token_file", client_ready
            else:
                issue = "The saved access token is expired, expiring, or has an invalid expiry hint."
        except Exception:
            # A malformed file also stops the runtime before PIN/TOTP fallback.
            method, issue = "token_file", "The saved Dhan token is unreadable or malformed. Recreate it with the authentication helper."
            missing.append("DHAN_ACCESS_TOKEN_FILE")
    if method == "missing":
        client_ready = bool(configured_client)
        if pin_totp:
            method, auth_ready = "pin_totp", client_ready
        elif oauth:
            method = "oauth_consent_required"
            issue = "API key and secret require browser consent. Complete the Dhan authentication helper before running a snapshot."
        elif access_token:
            method = "access_token"
        if not auth_ready:
            missing.append("DHAN_ACCESS_TOKEN")
            if pin and not re.fullmatch(r"[0-9]{6}", pin):
                missing.append("DHAN_PIN")
            if seed and not _valid_seed(seed):
                missing.append("DHAN_TOTP_SECRET")
    if not client_ready:
        missing.append("DHAN_CLIENT_ID")

    instruments = _text(environment, "DHAN_INSTRUMENTS")
    symbols = _text(environment, "DHAN_SYMBOLS")
    symbols_file = _text(environment, "DHAN_SYMBOLS_FILE")
    selection = inspect_selection(symbols=symbols, instruments=instruments, symbols_file=symbols_file,
                                  max_instruments=1000)
    instrument_count = selection["count"]
    instruments_valid = selection["valid"]
    if not instruments_valid:
        missing.append("DHAN_SYMBOLS_FILE" if symbols_file else
                       "DHAN_INSTRUMENTS" if instruments and not symbols else "DHAN_SYMBOLS")
    mode = _text(environment, "DHAN_SNAPSHOT_MODE", "quote").lower()
    mode_valid = mode in _MODES
    if not mode_valid:
        missing.append("DHAN_SNAPSHOT_MODE")
    ready = bool(auth_ready and client_ready and instruments_valid and mode_valid)
    if ready:
        message = "Local snapshot settings are ready to try. Dhan authentication and Data API entitlement have not been verified."
    elif issue:
        message = issue
    elif selection["mode"] == "conflict":
        message = "Set only one of DHAN_SYMBOLS, DHAN_SYMBOLS_FILE, or DHAN_INSTRUMENTS."
    elif not instruments_valid and symbols_file:
        message = ("The configured symbols.csv could not be read or validated. Use a UTF-8 CSV with one symbol "
                   "column and a symbol header; check the read-only Docker file mount.")
    elif not instruments_valid and symbols:
        message = "DHAN_SYMBOLS must contain at most 1,000 exact NSE symbols. Indices need the IDX_I: prefix."
    elif not instruments_valid and instruments:
        message = "DHAN_INSTRUMENTS must contain valid NSE segment/security-ID pairs, with at most 1,000 instruments."
    else:
        message = "Complete the missing local settings before running a Dhan snapshot."
    return {
        "configured": bool(client_ready and (access_token or file_present or pin_totp or oauth) and selection["configured"]),
        "credentials_ready": bool(auth_ready and client_ready),
        "ready_to_run": ready,
        "connection_verified": False,
        "client_id_configured": client_ready,
        "authentication": {
            "access_token_configured": bool(access_token),
            "token_file_present": file_present,
            "pin_totp_configured": pin_totp,
            "oauth_configured": oauth,
            "token_expired": token_expired,
            "method": method,
        },
        "instruments": {"configured": selection["configured"], "valid": instruments_valid,
                        "count": instrument_count, "selection_mode": selection["mode"],
                        "resolution_required": selection["resolution_required"]},
        "symbols": {"configured": bool(symbols or symbols_file),
                    "count": instrument_count if symbols or symbols_file else 0,
                    "syntax_valid": bool((symbols or symbols_file) and instruments_valid)},
        "snapshot_mode": mode if mode_valid else "quote",
        "trading_date": india_date(instant),
        "timezone": TIMEZONE,
        "missing": sorted(set(missing)),
        "message": message,
        "live": {"running": None, "status": "not_checked"},
    }


def snapshot_form(form: Mapping[str, str], environment: Mapping[str, str] | None = None,
                  now: datetime | None = None) -> dict[str, str]:
    """Validate an explicit snapshot request; credentials remain server-side."""
    environment = os.environ if environment is None else environment
    reject_credentials(form)
    today = india_date(now)
    selected_date = str(form.get("ingest_date") or today).strip()
    if selected_date != today:
        raise ValueError("Dhan REST snapshots require today's date in Asia/Kolkata.")
    mode = str(form.get("dhan_snapshot_mode") or _text(environment, "DHAN_SNAPSHOT_MODE", "quote")).strip().lower()
    if mode not in _MODES:
        raise ValueError("Dhan snapshot mode must be quote, ohlc, or ltp.")
    explicit_symbols = str(form.get("dhan_symbols") or "").strip()
    explicit_ids = str(form.get("dhan_instruments") or "").strip()
    if explicit_symbols and explicit_ids:
        raise ValueError("Supply symbols or explicit instruments, not both.")
    if explicit_symbols:
        symbols, instruments, symbols_file = explicit_symbols, "", ""
    elif explicit_ids:
        symbols, instruments, symbols_file = "", explicit_ids, ""
    else:
        symbols = _text(environment, "DHAN_SYMBOLS")
        instruments = _text(environment, "DHAN_INSTRUMENTS")
        symbols_file = _text(environment, "DHAN_SYMBOLS_FILE")
    # Read a server-configured CSV once. All validation below uses its canonical
    # symbols, and the normalized form/child job carries only resolved IDs.
    symbols, instruments = configured_selection(symbols=symbols, instruments=instruments,
                                                symbols_file=symbols_file, max_instruments=1000)
    selected_environment = dict(environment)
    selected_environment.update(DHAN_SYMBOLS=symbols, DHAN_SYMBOLS_FILE="",
                                DHAN_INSTRUMENTS=instruments, DHAN_SNAPSHOT_MODE=mode)
    if not inspect_setup(selected_environment, now)["ready_to_run"]:
        raise ValueError("Dhan is not ready locally. Check the setup status and complete authentication with the Dhan helper.")
    application = str(form.get("application_name") or "nse_market_snapshot").strip().lower().replace("-", "_").replace(" ", "_")
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", application):
        raise ValueError("Use a valid application name containing lowercase letters, numbers, and underscores.")
    write_mode = str(form.get("write_mode") or "overwrite").strip().lower()
    if write_mode not in {"overwrite", "append", "replace"}:
        raise ValueError("Snapshot write mode must be overwrite, append, or replace.")
    if symbols:
        parsed = resolve_selection(symbols=symbols, instruments=instruments, max_instruments=1000)
    else:
        try:
            parsed = parse_instruments(instruments, max_instruments=1000)
        except Exception:
            raise ValueError("Configure valid DHAN_INSTRUMENTS with at most 1,000 NSE instruments.") from None
    normalized = dict(form)
    normalized.update({
        "source_type": "dhan", "application_name": application,
        "ingest_date": today,
        "metadata_key": str(form.get("metadata_key") or "examples/dhan_market_snapshot.csv"),
        "output_database": str(form.get("output_database") or _text(environment, "LAKE_DATABASE", "de_lake")),
        "output_catalog": str(form.get("output_catalog") or _text(environment, "ICEBERG_CATALOG", "minio")),
        "write_mode": write_mode, "dhan_snapshot_mode": mode,
        "dhan_symbols": "",
        "dhan_instruments": ",".join(f"{item.exchange_segment}:{item.security_id}" for item in parsed),
    })
    return normalized
