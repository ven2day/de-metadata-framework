"""Interactive Dhan access-token helper.

The command writes the token to a gitignored file and never prints the token,
PIN, API secret, or TOTP value.
"""

from __future__ import annotations

import argparse
from getpass import getpass
import json
import sys

from ingestion.dhan.client import (
    AUTH_BASE_URL,
    DhanAccessToken,
    DhanApiError,
    DhanConfigError,
    DhanRestClient,
    DhanTokenProvider,
    _parse_expiry,
    consume_oauth_consent,
    generate_access_token,
    generate_oauth_consent,
    safe_auth_diagnostic,
    write_token_file,
)
from ingestion.env.DE_Ingestion_properties import (
    DHAN_ACCESS_TOKEN_FILE,
    DHAN_API_KEY,
    DHAN_API_SECRET,
    DHAN_CLIENT_ID,
    DHAN_PIN,
    DHAN_REDIRECT_URL,
    DHAN_TOTP_SECRET,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dhan-auth",
        description="Create or validate Dhan's 24-hour access token safely.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    oauth_start = sub.add_parser(
        "oauth-start", help="Start API-key/secret browser consent"
    )
    oauth_start.add_argument("--client-id", default=DHAN_CLIENT_ID)

    oauth_complete = sub.add_parser(
        "oauth-complete", help="Consume tokenId returned to the configured redirect URL"
    )
    oauth_complete.add_argument(
        "--token-id",
        default=None,
        help="Optional; omit to enter tokenId without exposing it in shell history",
    )
    oauth_complete.add_argument("--output", default=DHAN_ACCESS_TOKEN_FILE)

    direct = sub.add_parser(
        "totp", help="Generate with client ID, six-digit PIN, and current TOTP"
    )
    direct.add_argument("--client-id", default=DHAN_CLIENT_ID)
    direct.add_argument("--output", default=DHAN_ACCESS_TOKEN_FILE)

    sub.add_parser("status", help="Validate the configured token and data plan")
    return parser


def _oauth_start(client_id: str) -> int:
    consent_id = generate_oauth_consent(
        client_id, DHAN_API_KEY, DHAN_API_SECRET
    )
    print("Open this Dhan URL in a browser and complete login/TOTP:")
    print(f"{AUTH_BASE_URL}/login/consentApp-login?consentAppId={consent_id}")
    if DHAN_REDIRECT_URL:
        print(f"Expected redirect base: {DHAN_REDIRECT_URL}")
    print("Then run: docker compose run --rm dhan-auth oauth-complete")
    return 0


def _oauth_complete(token_id: str | None, output: str) -> int:
    value = token_id or getpass("tokenId from Dhan redirect: ").strip()
    token = consume_oauth_consent(value, DHAN_API_KEY, DHAN_API_SECRET)
    write_token_file(token, output)
    _print_saved(token)
    return 0


def _direct(client_id: str, output: str) -> int:
    pin = DHAN_PIN or getpass("Six-digit Dhan PIN: ").strip()
    if DHAN_TOTP_SECRET:
        try:
            import pyotp
        except ImportError as exc:
            raise DhanConfigError("pyotp is required for DHAN_TOTP_SECRET") from exc
        try:
            current_totp = pyotp.TOTP(DHAN_TOTP_SECRET).now()
        except Exception:
            raise DhanConfigError(
                "DHAN_TOTP_SECRET is not a valid Base32 TOTP seed"
            ) from None
    else:
        current_totp = getpass("Current six-digit Dhan TOTP: ").strip()
    token = generate_access_token(client_id, pin, current_totp)
    write_token_file(token, output)
    _print_saved(token)
    return 0


def _print_saved(token: DhanAccessToken) -> None:
    # A nominal success can still carry arbitrary strings in ancillary fields.
    # Print a normalized timestamp only, never an upstream expiry/path verbatim.
    try:
        expiry = _parse_expiry(token.expiry_time)
    except (TypeError, ValueError, AttributeError, OverflowError):
        expiry = None
    print("Dhan access token saved securely; expiry: " + (expiry.isoformat() if expiry else "unknown"))


def _status() -> int:
    provider = DhanTokenProvider()
    client = DhanRestClient(
        provider.get_token(),
        session=provider.session,
        provider=provider,
    )
    client.validate_market_data_access()
    # Successful responses can echo secrets too. Report completed verification
    # steps, not arbitrary values from nominally safe upstream profile fields.
    safe = {
        "token_verified": True,
        "account_identity_verified": True,
        "data_api_entitlement_verified": True,
    }
    print(json.dumps(safe, indent=2))
    return 0


def _print_auth_diagnostic(error: DhanApiError) -> None:
    diagnostic = safe_auth_diagnostic(error.diagnostic)
    if not diagnostic:
        return
    print("Diagnostic: " + json.dumps(diagnostic, sort_keys=True), file=sys.stderr)
    category = diagnostic["category"]
    if category == "rate_limited":
        step = "Stop login attempts and retry later. This helper does not retry automatically."
    elif category == "pin_rejected":
        step = "Dhan reported a PIN-related error. Verify the actual six-digit Dhan login PIN locally; do not substitute another password or repeatedly retry."
    elif category == "totp_rejected":
        step = "Dhan reported a TOTP-related error. Check that the seed matches the currently enabled Dhan authenticator and synchronize the host clock. Do not share the seed or rotating code."
    elif category == "totp_setup_required":
        step = "Dhan reported that TOTP setup is missing. Enable or reset TOTP in Dhan Web, then privately update the current seed in .env."
    elif category == "client_id_rejected":
        step = "Dhan reported a client-ID error. Verify DHAN_CLIENT_ID against your own Dhan account without posting it here."
    elif category == "account_locked":
        step = "Dhan reported an account lock/block. Stop login attempts and use Dhan Web or official support to recover access."
    elif category in {"transport_error", "service_unavailable"}:
        step = "Check connectivity and Dhan service availability, then try once later."
    elif category == "access_denied":
        step = "Check Dhan account/API permissions. A login token and Data API entitlement are separate checks."
    elif category == "account_rejected":
        step = "Check your account status in Dhan Web or contact Dhan support using only this safe diagnostic."
    elif category in {"authentication_rejected", "invalid_request", "provider_rejected"}:
        step = "For PIN/TOTP login, verify the client ID, actual six-digit login PIN, current Dhan TOTP setup/seed, and clock. For OAuth, complete browser consent with the current key/secret. Do not retry repeatedly."
    else:
        step = "Dhan returned an unexpected authentication result. Do not paste a token, raw response, or .env into chat; share only this Diagnostic line for investigation."
    print("Next step: " + step, file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "oauth-start":
            return _oauth_start(args.client_id)
        if args.command == "oauth-complete":
            return _oauth_complete(args.token_id, args.output)
        if args.command == "totp":
            return _direct(args.client_id, args.output)
        return _status()
    except (DhanConfigError, DhanApiError) as exc:
        print(f"Dhan authentication failed: {exc}", file=sys.stderr)
        if isinstance(exc, DhanApiError):
            _print_auth_diagnostic(exc)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
