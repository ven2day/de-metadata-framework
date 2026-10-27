"""Regression checks for secret-bearing upstream Dhan error responses."""

from __future__ import annotations

import traceback
import unittest

from ingestion.dhan.client import DhanApiError, _get, _post, _response_json


class Response:
    def __init__(self, status_code, payload=None, json_error=None):
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

    def post(self, _url, **_kwargs):
        return self.response

    def get(self, _url, **_kwargs):
        return self.response


class DhanErrorRedactionTests(unittest.TestCase):
    def test_error_body_message_and_code_never_reach_exception_or_traceback(self):
        secrets = (
            "123456", "654321", "private-access-token-canary",
            "private-api-secret-canary", "PRIVATE-TOTP-SEED-CANARY",
        )
        echoed = "?pin=" + secrets[0] + "&totp=" + secrets[1] + " " + " ".join(secrets[2:])
        for field in ("errorMessage", "message", "errorCode", "code"):
            with self.subTest(field=field):
                response = Response(400, {field: echoed, "nested": {"authorization": echoed}})
                try:
                    _response_json(response, "access-token generation")
                except DhanApiError as error:
                    rendered = "".join(traceback.format_exception(error))
                    self.assertEqual(error.status_code, 400)
                    self.assertIn("access-token generation", str(error))
                    self.assertIn("HTTP 400", str(error))
                    self.assertTrue(error.__suppress_context__)
                    for secret in secrets:
                        self.assertNotIn(secret, rendered)
                    self.assertNotIn("?pin=", rendered)
                else:
                    self.fail("A rejected Dhan response was accepted")

    def test_malformed_error_code_objects_and_lists_are_not_rendered(self):
        sentinel = "DO-NOT-LOG-THIS-CREDENTIAL"
        for payload in (
            {"errorCode": {"access_token": sentinel}, "message": [sentinel]},
            [sentinel], sentinel,
        ):
            with self.subTest(payload_type=type(payload).__name__):
                with self.assertRaises(DhanApiError) as caught:
                    _response_json(Response(503, payload), "profile")
                self.assertNotIn(sentinel, str(caught.exception))
                self.assertEqual(caught.exception.status_code, 503)

    def test_get_and_post_keep_safe_status_hints_without_echoing_response(self):
        for request in (_get, _post):
            for status, expected in (
                (401, "authentication"), (403, "authentication"),
                (429, "rate limit"), (503, "temporarily unavailable"),
            ):
                with self.subTest(request=request.__name__, status=status):
                    session = Session(Response(status, {"errorMessage": "API-SECRET-CANARY"}))
                    with self.assertRaises(DhanApiError) as caught:
                        request(session, "https://example.invalid", operation="profile", timeout=1)
                    self.assertIn(expected, str(caught.exception))
                    self.assertNotIn("API-SECRET-CANARY", str(caught.exception))
                    self.assertEqual(caught.exception.status_code, status)

    def test_json_decoder_error_details_are_not_chained(self):
        response = Response(502, json_error=ValueError("SECRET-IN-DECODER-ERROR"))
        try:
            _response_json(response, "profile")
        except DhanApiError as error:
            rendered = "".join(traceback.format_exception(error))
            self.assertNotIn("SECRET-IN-DECODER-ERROR", rendered)
        else:
            self.fail("A malformed upstream error was accepted")

    def test_successful_dictionary_response_is_returned_unchanged(self):
        payload = {"accessToken": "token-value", "data": {"message": "unchanged"}}
        self.assertIs(_response_json(Response(200, payload), "token"), payload)

    def test_successful_non_object_response_fails_without_echo(self):
        with self.assertRaisesRegex(DhanApiError, "not a JSON object") as caught:
            _response_json(Response(200, ["PRIVATE-TOKEN-CANARY"]), "profile")
        self.assertNotIn("PRIVATE-TOKEN-CANARY", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
