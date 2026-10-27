"""Check the Dhan-first homepage without credentials or external API calls.

Run against the isolated de-compat UI after its health check passes. This
checks HTTP rendering, local-only setup status, CSRF, invalid-symbol rejection,
and rejected missing configuration. It deliberately does not download an
instrument master, mint a token, request market data, or start ingestion.
"""

from __future__ import annotations

import argparse
from datetime import datetime
from html.parser import HTMLParser
from uuid import uuid4
from zoneinfo import ZoneInfo

import requests


class Page(HTMLParser):
    def __init__(self, html: str):
        super().__init__()
        self.ids: dict[str, tuple[str, dict]] = {}
        self.inputs: list[dict] = []
        self.csrf_token = None
        self.feed(html)

    def handle_starttag(self, tag, attributes):
        fields = dict(attributes)
        if fields.get("id"):
            self.ids[fields["id"]] = (tag, fields)
        if tag in {"input", "select", "textarea"}:
            self.inputs.append(fields)
        if tag == "input" and fields.get("name") == "csrf_token":
            self.csrf_token = fields.get("value")
        if tag == "meta" and fields.get("name") == "csrf-token":
            self.csrf_token = self.csrf_token or fields.get("content")

    def element(self, element_id):
        assert element_id in self.ids, f"Homepage is missing #{element_id}"
        return self.ids[element_id][1]


def assert_safe_fields(value) -> None:
    """Reject credential-bearing fields without reading any actual secrets."""
    prohibited = {
        "access_token", "accesstoken", "token", "api_key", "apikey",
        "api_secret", "apisecret", "app_secret", "app_id", "authorization",
        "pin", "totp", "totp_secret", "totpsecret", "client_id", "dhanclientid",
        "raw", "headers", "traceback", "exception", "token_file_path",
    }
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = str(key).lower().replace("-", "_")
            assert normalized not in prohibited, f"Status exposed a prohibited field: {key}"
            assert_safe_fields(child)
    elif isinstance(value, list):
        for child in value:
            assert_safe_fields(child)


def setup_status(session, base_url):
    response = session.get(base_url + "/dhan/status", timeout=15)
    response.raise_for_status()
    assert "application/json" in response.headers.get("Content-Type", ""), "Dhan status must be JSON"
    assert "no-store" in response.headers.get("Cache-Control", "").lower(), "Dhan status is cacheable"
    status = response.json()
    assert isinstance(status, dict), "Dhan status must be an object"
    required = {
        "configured", "credentials_ready", "ready_to_run", "connection_verified", "authentication",
        "instruments", "symbols", "trading_date", "timezone", "missing", "message",
    }
    assert required <= status.keys(), "Dhan status is missing its public setup fields"
    assert_safe_fields(status)
    assert status["configured"] is False, "This smoke check requires the unconfigured isolated stack"
    assert status["credentials_ready"] is False, "The isolated fixture inherited Dhan credentials"
    assert status["ready_to_run"] is False, "Missing Dhan credentials must not enable ingestion"
    assert status["connection_verified"] is False, "Local setup inspection must not claim API verification"
    assert status["timezone"] == "Asia/Kolkata", "NSE dates must use the India timezone"
    expected_date = datetime.now(ZoneInfo("Asia/Kolkata")).date().isoformat()
    assert status["trading_date"] == expected_date, "Dhan setup date does not match the current India date"
    assert isinstance(status["authentication"], dict), "Authentication setup must be structured"
    authentication = status["authentication"]
    for key in ("access_token_configured", "token_file_present", "pin_totp_configured", "oauth_configured"):
        assert authentication.get(key) is False, "The isolated fixture inherited Dhan authentication settings"
    assert authentication.get("token_expired") is None and authentication.get("method") == "missing"
    instruments = status["instruments"]
    assert isinstance(instruments, dict), "Instrument setup must be structured"
    assert instruments.get("configured") is False and instruments.get("valid") is False and instruments.get("count") == 0, (
        "The isolated fixture must not inherit real Dhan instruments"
    )
    assert instruments.get("selection_mode") == "missing" and instruments.get("resolution_required") is False
    assert status["symbols"] == {"configured": False, "count": 0, "syntax_valid": False}, (
        "The isolated fixture must not inherit a real symbol selection"
    )
    assert isinstance(status["missing"], list) and status["missing"], "Missing setup needs actionable field names"
    assert isinstance(status["message"], str) and status["message"], "Missing setup needs an explanation"
    if "live" in status:
        assert status["live"].get("running") is None, "Local status must not invent collector health"
        assert status["live"].get("status") == "not_checked"
    return status


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:15001")
    base_url = parser.parse_args().base_url.rstrip("/")
    assert base_url in {"http://127.0.0.1:15001", "http://ui:5000"}, (
        "This smoke check only accepts the isolated compatibility UI"
    )

    anonymous = requests.Session()
    protected = anonymous.get(base_url + "/dhan/status", allow_redirects=False, timeout=15)
    assert protected.status_code == 302 and "/login" in protected.headers.get("Location", ""), (
        "Dhan setup status must require login"
    )
    session = requests.Session()
    login_response = session.get(base_url + "/login", timeout=15)
    login_response.raise_for_status()
    login = Page(login_response.text)
    assert login.csrf_token, "Login did not render CSRF protection"
    response = session.post(
        base_url + "/login",
        data={
            "username": "root", "password": "CompatOnly!Passw0rd2026",
            "csrf_token": login.csrf_token,
        },
        allow_redirects=False, timeout=15,
    )
    assert response.status_code == 302, "Compatibility root login failed"
    response = session.get(base_url + "/", timeout=15)
    response.raise_for_status()
    page = Page(response.text)
    assert page.csrf_token, "Homepage did not render CSRF protection"

    status = setup_status(session, base_url)
    dhan_radio = page.element("src-dhan")
    assert dhan_radio.get("value") == "dhan" and "checked" in dhan_radio, "Dhan is not the default source"
    assert "disabled" in page.element("run-btn"), "Unconfigured Dhan run button must start disabled"
    ingest_date = page.element("ingest_date")
    assert "readonly" in ingest_date, "Current-only Dhan snapshot date must be read-only"
    assert ingest_date.get("value") == status["trading_date"], "Homepage date differs from local setup status"
    assert page.element("application_name").get("value") == "nse_market_snapshot"
    metadata = page.element("metadata_key")
    assert metadata.get("value") == "examples/dhan_market_snapshot.csv" and "readonly" in metadata
    for layer in ("lake", "bronze", "silver", "gold"):
        page.element("workspace-layer-" + layer)
    for element_id in (
        "dhan-setup-state", "dhan-client-status", "dhan-auth-status",
        "dhan-instrument-count", "dhan-status-refresh", "dhan-missing-config",
        "nse-trading-date", "dhan_snapshot_mode", "dhan_symbols",
        "dhan-resolve-btn", "dhan-resolve-status", "dhan-resolve-results",
    ):
        page.element(element_id)
    credential_names = {
        "dhan_access_token", "dhan_api_key", "dhan_api_secret", "dhan_pin",
        "dhan_totp_secret", "access_token", "api_secret", "pin", "totp_secret",
    }
    assert not any(
        str(field.get("name", "")).lower() in credential_names for field in page.inputs
    ), "The homepage must not collect brokerage credentials"

    headers = {"X-CSRFToken": page.csrf_token}
    # These invalid requests must fail before any public-master download. A
    # successful resolution is covered with a mocked public master in unit
    # tests, rather than adding external network access to this HTTP fixture.
    resolve_csrf = session.post(base_url + "/dhan/resolve", data={"symbols": "<invalid>"}, timeout=15)
    assert resolve_csrf.status_code == 400, "Symbol resolution accepted a request without CSRF"
    for resolve_form in ({"symbols": ""}, {"symbols": "<invalid-symbol>"}):
        invalid_symbols = session.post(
            base_url + "/dhan/resolve", headers=headers, data=resolve_form, timeout=15,
        )
        assert invalid_symbols.status_code == 400, "Invalid symbols were not rejected before lookup"
        assert invalid_symbols.json().get("code") == "invalid_symbols"
        assert set(invalid_symbols.json()) == {"code", "error"}
        assert "no-store" in invalid_symbols.headers.get("Cache-Control", "").lower()
        assert "private" in invalid_symbols.headers.get("Cache-Control", "").lower()
        assert "<invalid-symbol>" not in invalid_symbols.text
        assert_safe_fields(invalid_symbols.json())
    resolve_marker = "SYNTHETIC-NO-ECHO-RESOLVE-CREDENTIAL-" + uuid4().hex
    rejected_resolve_credentials = session.post(
        base_url + "/dhan/resolve", headers=headers,
        data={"symbols": "RELIANCE", "DHAN_API_SECRET": resolve_marker}, timeout=15,
    )
    assert rejected_resolve_credentials.status_code == 400
    assert rejected_resolve_credentials.json().get("code") == "invalid_symbols"
    assert resolve_marker not in rejected_resolve_credentials.text, "Symbol lookup echoed a submitted credential"
    assert session.get(base_url + "/dhan/resolve", timeout=15).status_code == 405

    app_name = "compat_dhan_ui_" + uuid4().hex[:10]
    run_form = {
        "application_name": app_name, "source_type": "dhan",
        "ingest_date": status["trading_date"],
        "metadata_key": "examples/dhan_market_snapshot.csv", "write_mode": "overwrite",
    }
    denied_csrf = session.post(base_url + "/run", data=run_form, timeout=15)
    assert denied_csrf.status_code == 400, "Dhan execution accepted a request without CSRF"
    denied = session.post(base_url + "/run", headers=headers, data=run_form, timeout=15)
    assert denied.status_code == 400, "Missing Dhan setup must be rejected before launching a pipeline"
    assert isinstance(denied.json(), dict) and denied.json().get("error"), "Missing setup must return a clean JSON error"
    assert "--- Exit" not in denied.text and "Traceback" not in denied.text
    denied_symbols = session.post(
        base_url + "/run", headers=headers, data={**run_form, "dhan_symbols": "RELIANCE,HDFCBANK"}, timeout=15,
    )
    assert denied_symbols.status_code == 400, "Typed symbols bypassed missing brokerage authentication"
    assert "--- Exit" not in denied_symbols.text and "Traceback" not in denied_symbols.text

    # This is a synthetic marker, not an actual token. Secret inputs must never
    # be accepted or copied into an error message, even with missing setup.
    marker = "SYNTHETIC-DO-NOT-ECHO-CREDENTIAL-" + uuid4().hex
    credential_request = session.post(
        base_url + "/run", headers=headers,
        data={**run_form, "DHAN_ACCESS_TOKEN": marker}, timeout=15,
    )
    assert credential_request.status_code == 400, "Dhan run accepted credential fields from the browser"
    assert marker not in credential_request.text, "Dhan error echoed a submitted credential"
    no_mutation = session.post(base_url + "/dhan/status", headers=headers, timeout=15)
    assert no_mutation.status_code == 405, "Dhan setup status must remain read-only"
    after_status = setup_status(session, base_url)
    assert after_status["ready_to_run"] is False and after_status["connection_verified"] is False
    jobs = session.get(base_url + "/active-jobs", timeout=15)
    jobs.raise_for_status()
    assert not any(job.get("app_name") == app_name for job in jobs.json()), "Rejected Dhan request became an active job"

    print(
        "PASS: Dhan-first homepage -> private local-only setup status -> India date/four layers "
        "-> symbols controls/invalid lookup rejection -> disabled unconfigured run "
        "-> missing-config/credential/CSRF rejection; no Dhan lookup or market API requested"
    )


if __name__ == "__main__":
    main()
