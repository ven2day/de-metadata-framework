from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import tempfile
import traceback
import unittest

import requests

from ingestion.dhan.client import (
    DhanAccessToken,
    DhanApiError,
    DhanConfigError,
    DhanRestClient,
    DhanTokenProvider,
    _is_retryable_api_error,
    _post,
    parse_instruments,
    read_token_file,
    write_token_file,
)
from ingestion.dhan.normalization import (
    MARKET_COLUMNS,
    normalize_live_record,
    normalize_market_snapshot,
)
from ingestion.pyfiles.args_parser import parse_args


class FakeResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class StaticSession:
    def __init__(self, *, get_response=None, post_response=None):
        self.get_response = get_response
        self.post_response = post_response
        self.last_post = None

    def get(self, _url, **_kwargs):
        return self.get_response

    def post(self, url, **kwargs):
        self.last_post = (url, kwargs)
        return self.post_response


class FailingSession:
    def post(self, _url, **_kwargs):
        raise requests.RequestException(
            "https://auth.dhan.co/app/generateAccessToken?pin=123456&totp=654321"
        )


class DhanClientTests(unittest.TestCase):
    def test_live_retry_policy_fails_closed_on_bad_requests(self):
        self.assertTrue(_is_retryable_api_error(DhanApiError("network")))
        self.assertTrue(_is_retryable_api_error(DhanApiError("rate", status_code=429)))
        self.assertTrue(_is_retryable_api_error(DhanApiError("server", status_code=503)))
        self.assertFalse(_is_retryable_api_error(DhanApiError("bad totp", status_code=400)))
        self.assertFalse(_is_retryable_api_error(DhanApiError("not found", status_code=404)))

    def test_instruments_are_nse_only_deduplicated_and_canonical(self):
        parsed = parse_instruments("nse_eq:001333,NSE_EQ:1333,IDX_I:13")
        self.assertEqual(
            [(item.exchange_segment, item.security_id) for item in parsed],
            [("NSE_EQ", "1333"), ("IDX_I", "13")],
        )
        with self.assertRaises(DhanConfigError):
            parse_instruments("BSE_EQ:500325")

    def test_auth_network_error_does_not_render_secret_url(self):
        with self.assertRaises(DhanApiError) as caught:
            _post(
                FailingSession(),
                "https://auth.dhan.co/app/generateAccessToken",
                operation="token",
                timeout=1,
            )
        rendered = "".join(
            traceback.format_exception(
                type(caught.exception), caught.exception, caught.exception.__traceback__
            )
        )
        self.assertNotIn("123456", rendered)
        self.assertNotIn("654321", rendered)
        self.assertTrue(caught.exception.__suppress_context__)

    def test_profile_validation_is_fail_closed(self):
        token = DhanAccessToken("token", "1000000001")
        missing_plan = StaticSession(
            get_response=FakeResponse(200, {"dhanClientId": "1000000001"})
        )
        with self.assertRaisesRegex(DhanConfigError, "plan is not active"):
            DhanRestClient(token, session=missing_plan).validate_market_data_access()

        wrong_client = StaticSession(
            get_response=FakeResponse(
                200, {"dhanClientId": "1000000002", "dataPlan": "Active"}
            )
        )
        with self.assertRaisesRegex(DhanConfigError, "does not match"):
            DhanRestClient(token, session=wrong_client).validate_market_data_access()

    def test_market_snapshot_contract(self):
        session = StaticSession(
            post_response=FakeResponse(
                200,
                {
                    "status": "success",
                    "data": {"NSE_EQ": {"1333": {"last_price": 100.5}}},
                },
            )
        )
        client = DhanRestClient(DhanAccessToken("token", "1000000001"), session=session)
        result = client.market_snapshot(parse_instruments("NSE_EQ:1333"), "ltp")
        self.assertEqual(result["status"], "success")
        _, kwargs = session.last_post
        self.assertEqual(kwargs["json"], {"NSE_EQ": [1333]})
        self.assertEqual(kwargs["headers"]["client-id"], "1000000001")

    def test_token_file_round_trip(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "token.json")
            expected = DhanAccessToken(
                "secret-token", "1000000001", "2026-10-25T12:00:00+05:30"
            )
            write_token_file(expected, path)
            self.assertEqual(read_token_file(path), expected)

    def test_force_refresh_loads_a_rotated_token_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "token.json")
            old = DhanAccessToken("old-token", "1000000001", "2099-01-01T00:00:00+05:30")
            new = DhanAccessToken("new-token", "1000000001", "2099-01-02T00:00:00+05:30")
            write_token_file(old, path)
            provider = DhanTokenProvider(
                client_id="1000000001",
                access_token="",
                token_file=path,
                pin="",
                totp_secret="",
            )
            self.assertEqual(provider.get_token().value, "old-token")
            write_token_file(new, path)
            self.assertEqual(provider.get_token(force_refresh=True).value, "new-token")

    def test_token_file_rejects_non_object_json(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "token.json"
            path.write_text("[]\n", encoding="utf-8")
            with self.assertRaisesRegex(DhanConfigError, "JSON object"):
                read_token_file(str(path))


class DhanNormalizationTests(unittest.TestCase):
    def test_snapshot_has_stable_columns_and_utc_exchange_time(self):
        rows = normalize_market_snapshot(
            {
                "status": "success",
                "data": {
                    "NSE_EQ": {
                        "1333": {
                            "last_price": 42.25,
                            "last_trade_time": "25/10/2026 09:15:00",
                            "ohlc": {"open": 40, "high": 43, "low": 39, "close": 41},
                        }
                    }
                },
            },
            "quote",
            received_at=datetime(2026, 10, 25, 4, 0, tzinfo=timezone.utc),
            batch_id="batch",
        )
        self.assertEqual(tuple(rows[0]), MARKET_COLUMNS)
        self.assertEqual(rows[0]["exchange_time"].isoformat(), "2026-10-25T03:45:00+00:00")
        self.assertEqual(rows[0]["trading_date"].isoformat(), "2026-10-25")

    def test_previous_close_preserves_previous_open_interest(self):
        row = normalize_live_record(
            {
                "type": "Previous Close",
                "exchange_segment": 2,
                "security_id": 49081,
                "prev_close": "125.50",
                "prev_OI": 9876,
            },
            sequence=1,
            received_at=datetime(2026, 10, 25, 4, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(row["close"], 125.5)
        self.assertEqual(row["previous_open_interest"], 9876)

    def test_official_live_packet_families_and_invalid_identity(self):
        base = {"exchange_segment": 1, "security_id": 1333}
        packets = (
            ({**base, "type": "Ticker Data", "LTP": 100.5, "LTT": "03:45:00"}, "ticker"),
            ({**base, "type": "Quote Data", "volume": 10, "LTQ": 2}, "quote"),
            ({**base, "type": "Full Data", "depth": [{"bid_quantity": 1}]}, "full"),
            ({**base, "type": "OI Data", "OI": 1234}, "open_interest"),
        )
        for sequence, (packet, expected_type) in enumerate(packets, start=1):
            with self.subTest(expected_type=expected_type):
                row = normalize_live_record(packet, sequence=sequence)
                self.assertEqual(row["feed_type"], expected_type)
                self.assertEqual(row["security_id"], "1333")

        with self.assertRaisesRegex(ValueError, "exchange_segment"):
            normalize_live_record(
                {"type": "Ticker Data", "exchange_segment": 99, "security_id": 1333},
                sequence=10,
            )
        with self.assertRaisesRegex(ValueError, "security_id"):
            normalize_live_record(
                {"type": "Ticker Data", "exchange_segment": 1},
                sequence=11,
            )


class DhanArgumentTests(unittest.TestCase):
    def test_live_landed_source_defaults_to_append(self):
        args = parse_args(
            [
                "--application-name",
                "nse-live",
                "--ingest-date",
                "2026-10-25",
                "--source-type",
                "dhan-live",
                "--source-key",
                "dhan/nse/date=YYYY-MM-DD/hour=01/minute=02/*.json",
                "--metadata-key",
                "examples/dhan_live_ticks.csv",
            ]
        )
        self.assertEqual(args.write_mode, "append")
        self.assertEqual(args.output_database, "de_lake")
        self.assertEqual(args.application_name, "nse_live")


if __name__ == "__main__":
    unittest.main()
