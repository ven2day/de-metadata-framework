"""Offline authentication diagnostics must explain failure without echoing secrets."""

from __future__ import annotations

from contextlib import redirect_stderr, redirect_stdout
import io
import json
import traceback
import unittest
from unittest import mock

import requests

from ingestion.dhan import auth_cli
from ingestion.dhan.client import DhanAccessToken, DhanApiError, generate_access_token


CLIENT = "1000000001"
PIN = "123456"
TOTP = "654321"
CANARIES = (
    PIN, TOTP, "PRIVATE-ACCESS-TOKEN-CANARY", "PRIVATE-API-SECRET-CANARY",
    "PRIVATE-TOTP-SEED-CANARY", "PRIVATE-UPSTREAM-BODY-CANARY",
)
ECHO = "?pin=" + PIN + "&totp=" + TOTP + " " + " ".join(CANARIES[2:])
DIAGNOSTIC_FIELDS = {
    "http_status", "category", "provider_code", "response_kind",
    "error_reported", "expected_field",
}
KNOWN_PROVIDER_CODES = {f"DH-{number}" for number in range(901, 911)}
CATEGORIES = {
    "authentication_rejected", "access_denied", "account_rejected", "rate_limited",
    "invalid_request", "service_unavailable", "provider_rejected", "unexpected_response", "transport_error",
    "pin_rejected", "totp_rejected", "totp_setup_required", "client_id_rejected", "account_locked",
}
RESPONSE_KINDS = {"json_object", "json_array", "json_scalar", "invalid_json", "no_response"}
EXPECTED_FIELDS = {"accessToken", "consentAppId", "dhanClientId", None}


class Response:
    def __init__(self, status_code=200, payload=None, json_error=None):
        self.status_code = status_code
        self.payload = payload
        self.json_error = json_error

    def json(self):
        if self.json_error is not None:
            raise self.json_error
        return self.payload


class Session:
    def __init__(self, response):
        self.response = response
        self.last_post = None

    def post(self, url, **kwargs):
        self.last_post = (url, kwargs)
        return self.response


class DhanAuthDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        # A forgotten fake session must fail locally, never contact Dhan.
        self.network = mock.patch(
            "requests.sessions.Session.request",
            side_effect=AssertionError("External HTTP is forbidden in diagnostic tests"),
        )
        self.network.start()
        self.addCleanup(self.network.stop)

    def assert_redacted(self, value):
        rendered = value if isinstance(value, str) else json.dumps(value, sort_keys=True)
        for canary in CANARIES:
            self.assertNotIn(canary, rendered)
        self.assertNotIn("?pin=", rendered)
        self.assertNotIn("errorMessage", rendered)
        self.assertNotIn("authorization", rendered.lower())

    def assert_safe_diagnostic(self, error, *, status=200):
        diagnostic = error.diagnostic
        self.assertIsInstance(diagnostic, dict)
        self.assertLessEqual(set(diagnostic), DIAGNOSTIC_FIELDS)
        self.assertEqual(diagnostic.get("http_status"), status)
        self.assertIn(diagnostic.get("category"), CATEGORIES)
        self.assertIn(diagnostic.get("response_kind"), RESPONSE_KINDS)
        self.assertIn(diagnostic.get("expected_field"), EXPECTED_FIELDS)
        self.assertIsInstance(diagnostic.get("error_reported"), bool)
        if diagnostic.get("provider_code") is not None:
            self.assertIn(diagnostic["provider_code"], KNOWN_PROVIDER_CODES)
        self.assert_redacted(diagnostic)
        self.assert_redacted(str(error))
        self.assert_redacted("".join(traceback.format_exception(error)))
        return diagnostic

    def failure(self, payload, status=200, json_error=None):
        session = Session(Response(status, payload, json_error))
        with self.assertRaises(DhanApiError) as caught:
            generate_access_token(CLIENT, PIN, TOTP, session=session)
        self.assert_safe_diagnostic(caught.exception, status=status)
        return caught.exception

    def test_http_200_application_error_preserves_only_known_provider_code(self):
        error = self.failure({"errorCode": "DH-901", "errorMessage": ECHO})
        self.assertEqual(error.diagnostic["provider_code"], "DH-901")
        self.assertTrue(error.diagnostic["error_reported"])
        self.assertEqual(error.diagnostic["expected_field"], "accessToken")

    def test_only_finite_official_dh_codes_can_enter_diagnostics(self):
        for code in sorted(KNOWN_PROVIDER_CODES):
            with self.subTest(code=code):
                error = self.failure({"errorCode": code, "errorMessage": ECHO})
                self.assertEqual(error.diagnostic["provider_code"], code)

    def test_unknown_or_malformed_provider_codes_never_echo(self):
        for code in (PIN, int(PIN), ECHO, "DH-999", "DH-901 " + ECHO,
                     {"access_token": ECHO}, [ECHO]):
            with self.subTest(code_type=type(code).__name__):
                error = self.failure({"errorCode": code, "errorMessage": ECHO})
                self.assertNotIn(error.diagnostic.get("provider_code"), KNOWN_PROVIDER_CODES)

    def test_known_negative_auth_phrases_become_static_categories_without_echo(self):
        messages = (
            ('Invalid PIN', 'pin_rejected'),
            ('Invalid TOTP', 'totp_rejected'),
            ('TOTP is not enabled', 'totp_setup_required'),
            ('Invalid client id', 'client_id_rejected'),
            ('Account is locked', 'account_locked'),
        )
        for message, category in messages:
            with self.subTest(category=category):
                error = self.failure({'errorCode': PIN, 'errorMessage': message + '. ' + ECHO})
                self.assertEqual(error.diagnostic['category'], category)
                self.assertIsNone(error.diagnostic['provider_code'])
                self.assertNotIn(message, str(error))
                self.assertNotIn(message, json.dumps(error.diagnostic))

    def test_ambiguous_pin_or_totp_error_does_not_blame_one_credential(self):
        for field in ('errorMessage', 'message'):
            error = self.failure({'status': 'failure', field: 'Invalid PIN or TOTP. ' + ECHO})
            self.assertEqual(error.diagnostic['category'], 'provider_rejected')

    def test_arbitrary_message_status_and_nested_response_values_are_not_diagnostics(self):
        for field in ("message", "errorMessage", "error", "errorType", "status", "data", "response"):
            with self.subTest(field=field):
                self.failure({field: {"authorization": ECHO, "accessToken": ECHO}})

    def test_nested_access_token_is_not_unwrapped_as_success(self):
        error = self.failure({"data": {"accessToken": CANARIES[2], "dhanClientId": CLIENT}})
        self.assertEqual(error.diagnostic["expected_field"], "accessToken")

    def test_missing_or_invalid_token_diagnostic_preserves_actual_http_status(self):
        for status in (201, 202, 206):
            for payload in ({}, {'accessToken': {'secret': ECHO}}):
                with self.subTest(status=status, field_present=bool(payload)):
                    error = self.failure(payload, status)
                    self.assertEqual(error.status_code, status)
                    self.assertEqual(error.diagnostic['expected_field'], 'accessToken')

    def test_token_value_must_be_nonempty_string_not_an_arbitrary_object(self):
        for token in (None, "", "  ", {"secret": ECHO}, [ECHO], int(PIN), True, False):
            with self.subTest(token_type=type(token).__name__):
                self.failure({"accessToken": token, "dhanClientId": CLIENT})

    def test_explicit_empty_or_wrong_type_client_identity_is_not_treated_as_missing(self):
        for identity in (False, 0, [], {}, None, ''):
            with self.subTest(identity_type=type(identity).__name__):
                self.failure({'accessToken': CANARIES[2], 'dhanClientId': identity})
        # The direct flow may omit the identity entirely; only that case can
        # retain the identity supplied in the original request.
        session = Session(Response(payload={'accessToken': CANARIES[2]}))
        token = generate_access_token(CLIENT, PIN, TOTP, session=session)
        self.assertEqual(token.client_id, CLIENT)

    def test_explicit_failure_with_token_field_is_never_accepted(self):
        for marker in ({"status": "failure"}, {"status": "error"}, {"errorCode": "DH-901"}):
            with self.subTest(marker=next(iter(marker))):
                self.failure({**marker, "accessToken": CANARIES[2], "errorMessage": ECHO})

    def test_http_errors_keep_safe_diagnostics_without_response_body(self):
        for status in (400, 401, 403, 429, 503):
            with self.subTest(status=status):
                error = self.failure({"errorCode": "DH-901", "message": ECHO}, status)
                self.assertEqual(error.status_code, status)
                self.assertEqual(error.diagnostic["provider_code"], "DH-901")

    def test_nonobject_and_invalid_json_fail_without_decoder_or_body_echo(self):
        for payload in ([ECHO], ECHO, int(PIN), None):
            with self.subTest(payload_type=type(payload).__name__):
                self.failure(payload)
        self.failure(None, json_error=ValueError(ECHO))

    def test_transport_errors_do_not_echo_pin_totp_url_or_exception_context(self):
        session = mock.Mock()
        session.post.side_effect = requests.ConnectionError("https://auth.dhan.co/" + ECHO)
        with self.assertRaises(DhanApiError) as caught:
            generate_access_token(CLIENT, PIN, TOTP, session=session)
        diagnostic = self.assert_safe_diagnostic(caught.exception, status=None)
        self.assertEqual(diagnostic['category'], 'transport_error')
        self.assertEqual(diagnostic['response_kind'], 'no_response')
        self.assertTrue(caught.exception.__suppress_context__)

    def test_diagnostic_constructor_defensively_filters_untrusted_fields_and_values(self):
        error = DhanApiError('Safe failure', diagnostic={
            'http_status': int(PIN), 'category': ECHO, 'provider_code': PIN,
            'response_kind': {'authorization': ECHO}, 'error_reported': ECHO,
            'expected_field': [ECHO], 'body': ECHO, 'errorMessage': ECHO,
        })
        self.assert_safe_diagnostic(error, status=None)
        for invalid_status in (True, False, ECHO, 99, 600):
            with self.subTest(status_type=type(invalid_status).__name__):
                other = DhanApiError('Safe failure', diagnostic={'http_status': invalid_status})
                self.assertIsNone(other.diagnostic.get('http_status'))
                self.assert_redacted(other.diagnostic)

    def test_successful_token_response_and_request_shape_are_unchanged(self):
        payload = {"accessToken": CANARIES[2], "dhanClientId": CLIENT, "expiryTime": "2099-01-01T15:00:00+00:00"}
        session = Session(Response(payload=payload))
        output, errors = io.StringIO(), io.StringIO()
        with redirect_stdout(output), redirect_stderr(errors):
            token = generate_access_token(CLIENT, PIN, TOTP, session=session)
        self.assertEqual(token, DhanAccessToken(CANARIES[2], CLIENT, payload["expiryTime"]))
        self.assertEqual(output.getvalue(), "")
        self.assertEqual(errors.getvalue(), "")
        url, options = session.last_post
        self.assertEqual(url, "https://auth.dhan.co/app/generateAccessToken")
        self.assertEqual(options["params"], {"dhanClientId": CLIENT, "pin": PIN, "totp": TOTP})
        self.assertNotIn("json", options)
        self.assertNotIn("data", options)

    def test_empty_error_containers_do_not_reject_an_otherwise_valid_token(self):
        for value in (None, '', 0, '0', False, [], {}):
            for field in ('error', 'errors', 'errorMessage'):
                with self.subTest(field=field, value_type=type(value).__name__):
                    session = Session(Response(payload={'accessToken': CANARIES[2], 'dhanClientId': CLIENT,
                                                        field: value}))
                    token = generate_access_token(CLIENT, PIN, TOTP, session=session)
                    self.assertEqual(token.value, CANARIES[2])

    def test_documented_naive_expiry_is_normalized_from_india_time(self):
        session = Session(Response(payload={'accessToken': CANARIES[2], 'dhanClientId': CLIENT,
                                            'expiryTime': '2099-01-01T15:00:00'}))
        token = generate_access_token(CLIENT, PIN, TOTP, session=session)
        self.assertEqual(token.expiry_time, '2099-01-01T09:30:00+00:00')

    def test_invalid_expiry_is_rejected_before_saving_or_echoing(self):
        for expiry in (ECHO, [ECHO], {'expiry': ECHO}, int(PIN)):
            with self.subTest(expiry_type=type(expiry).__name__):
                self.failure({'accessToken': CANARIES[2], 'dhanClientId': CLIENT, 'expiryTime': expiry})
        result, output, errors, write = self.run_cli(Response(payload={
            'accessToken': CANARIES[2], 'dhanClientId': CLIENT, 'expiryTime': ECHO,
        }))
        self.assertEqual(result, 2)
        self.assertEqual(output, '')
        self.assert_redacted(errors)
        write.assert_not_called()

    def run_cli(self, response):
        output, errors = io.StringIO(), io.StringIO()
        with mock.patch("ingestion.dhan.client.requests.Session", return_value=Session(response)), \
             mock.patch.object(auth_cli, "DHAN_PIN", PIN), \
             mock.patch.object(auth_cli, "DHAN_TOTP_SECRET", ""), \
             mock.patch.object(auth_cli, "getpass", return_value=TOTP), \
             mock.patch.object(auth_cli, "write_token_file") as write, \
             redirect_stdout(output), redirect_stderr(errors):
            result = auth_cli.main(["totp", "--client-id", CLIENT, "--output", "synthetic-token.json"])
        return result, output.getvalue(), errors.getvalue(), write

    def test_cli_prints_safe_diagnostic_and_no_raw_provider_message(self):
        result, output, errors, write = self.run_cli(Response(payload={"errorCode": "DH-901", "errorMessage": ECHO}))
        self.assertEqual(result, 2)
        self.assertEqual(output, "")
        self.assertIn("http_status", errors)
        self.assertIn("DH-901", errors)
        self.assertIn("accessToken", errors)
        self.assertIn("Next step:", errors)
        diagnostic_line = next(line for line in errors.splitlines() if line.startswith('Diagnostic:'))
        self.assertLessEqual(set(json.loads(diagnostic_line.partition(':')[2])), DIAGNOSTIC_FIELDS)
        self.assert_redacted(errors)
        self.assertNotIn("Traceback", errors)
        write.assert_not_called()

    def test_cli_unknown_pinlike_error_code_is_redacted(self):
        result, output, errors, write = self.run_cli(Response(payload={"errorCode": PIN, "message": ECHO}))
        self.assertEqual(result, 2)
        self.assertEqual(output, "")
        self.assert_redacted(errors)
        write.assert_not_called()

    def test_cli_refilters_diagnostic_if_a_future_caller_mutates_it(self):
        error = DhanApiError('Safe authentication failure')
        error.diagnostic = {
            'http_status': True, 'category': ECHO, 'provider_code': PIN,
            'response_kind': [ECHO], 'error_reported': ECHO,
            'expected_field': ECHO, 'body': ECHO,
        }
        output, errors = io.StringIO(), io.StringIO()
        with mock.patch.object(auth_cli, '_direct', side_effect=error), \
             redirect_stdout(output), redirect_stderr(errors):
            result = auth_cli.main(['totp', '--client-id', CLIENT])
        self.assertEqual(result, 2)
        self.assertEqual(output.getvalue(), '')
        self.assert_redacted(errors.getvalue())

    def test_cli_success_saves_token_without_printing_token_or_diagnostics(self):
        payload = {"accessToken": CANARIES[2], "dhanClientId": CLIENT, "expiryTime": "2099-01-01T15:00:00+00:00"}
        result, output, errors, write = self.run_cli(Response(payload=payload))
        self.assertEqual(result, 0)
        self.assertEqual(errors, "")
        self.assertIn("saved securely", output)
        self.assertNotIn("diagnostic", output.lower())
        self.assert_redacted(output)
        write.assert_called_once_with(DhanAccessToken(CANARIES[2], CLIENT, payload["expiryTime"]), "synthetic-token.json")

    def test_cli_confirmation_filters_mocked_expiry_and_never_prints_output_path(self):
        token = DhanAccessToken(CANARIES[2], CLIENT, ECHO)
        output, errors = io.StringIO(), io.StringIO()
        with mock.patch.object(auth_cli, 'generate_access_token', return_value=token), \
             mock.patch.object(auth_cli, 'DHAN_PIN', PIN), \
             mock.patch.object(auth_cli, 'DHAN_TOTP_SECRET', ''), \
             mock.patch.object(auth_cli, 'getpass', return_value=TOTP), \
             mock.patch.object(auth_cli, 'write_token_file') as write, \
             redirect_stdout(output), redirect_stderr(errors):
            result = auth_cli.main(['totp', '--client-id', CLIENT, '--output', ECHO])
        self.assertEqual(result, 0)
        self.assertEqual(errors.getvalue(), '')
        self.assertIn('expiry: unknown', output.getvalue())
        self.assert_redacted(output.getvalue())
        write.assert_called_once_with(token, ECHO)

    def test_cli_status_outputs_verified_flags_not_arbitrary_profile_values(self):
        provider = mock.Mock()
        provider.get_token.return_value = DhanAccessToken(CANARIES[2], CLIENT)
        client = mock.Mock()
        client.validate_market_data_access.return_value = {
            key: ECHO for key in ('dhanClientId', 'tokenValidity', 'activeSegment', 'dataPlan', 'dataValidity')
        }
        output, errors = io.StringIO(), io.StringIO()
        with mock.patch.object(auth_cli, 'DhanTokenProvider', return_value=provider), \
             mock.patch.object(auth_cli, 'DhanRestClient', return_value=client), \
             redirect_stdout(output), redirect_stderr(errors):
            result = auth_cli.main(['status'])
        self.assertEqual(result, 0)
        self.assertEqual(errors.getvalue(), '')
        self.assertEqual(json.loads(output.getvalue()), {
            'token_verified': True, 'account_identity_verified': True, 'data_api_entitlement_verified': True,
        })
        self.assert_redacted(output.getvalue())
        provider.get_token.assert_called_once_with()
        client.validate_market_data_access.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
