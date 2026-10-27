import csv
from datetime import datetime, timedelta, timezone
import io
import os
from pathlib import Path
import tempfile
from unittest import TestCase, mock

import requests

from ingestion.dhan import instruments as resolver


NOW = datetime(2026, 10, 26, 4, 0, tzinfo=timezone.utc)
HEADERS = ['SEM_EXM_EXCH_ID', 'SEM_SEGMENT', 'SEM_SMST_SECURITY_ID', 'SEM_INSTRUMENT_NAME',
           'SEM_TRADING_SYMBOL', 'SEM_SERIES', 'SEM_EXPIRY_DATE', 'SEM_CUSTOM_SYMBOL']


def row(symbol, security_id, *, exchange='NSE', segment='E', instrument='EQUITY', series='EQ', expiry='', display=''):
    return [exchange, segment, str(security_id), instrument, symbol, series, expiry, display]


def master(*rows):
    output = io.StringIO(newline='')
    writer = csv.writer(output)
    writer.writerow(HEADERS)
    writer.writerows(rows)
    return ('\ufeff' + output.getvalue()).encode('utf-8')


BODY = master(row('RELIANCE', 2885), row('HDFCBANK', 1333), row('ARE&M', 100),
              row('NIFTY 50', 13, segment='I', instrument='INDEX', series='X', expiry='0001-01-01'))


class Response:
    def __init__(self, body=BODY, *, status=200, length=None, url=resolver.MASTER_URL):
        self.body = body
        self.status_code = status
        self.url = url
        self.headers = {'Content-Length': str(len(body)) if length is None else length}
        self.closed = False

    def iter_content(self, chunk_size):
        for position in range(0, len(self.body), chunk_size):
            yield self.body[position:position + chunk_size]

    def close(self):
        self.closed = True


class Session:
    def __init__(self, response=None, error=None):
        self.response = response or Response()
        self.error = error
        self.calls = []
        self.headers = {'Authorization': 'must-never-leak', 'access-token': 'brokerage-secret'}
        self.auth = ('private-client', 'private-secret')

    def send(self, request, **kwargs):
        self.calls.append((request, kwargs))
        if self.error:
            raise self.error
        return self.response


def cache(path, body=BODY, *, age=timedelta(0)):
    path.write_bytes(body)
    timestamp = (NOW - age).timestamp()
    os.utime(path, (timestamp, timestamp))


class DhanSymbolTests(TestCase):
    def test_parser_defaults_cash_supports_explicit_segments_and_stably_deduplicates(self):
        selected = resolver.parse_symbols(' reliance ,NSE_EQ:RELIANCE,are&m,IDX_I:Nifty 50,NSE_FNO:NIFTY-Nov2026-FUT')
        self.assertEqual([(item.exchange_segment, item.symbol) for item in selected],
                         [('NSE_EQ', 'RELIANCE'), ('NSE_EQ', 'ARE&M'), ('IDX_I', 'NIFTY 50'),
                          ('NSE_FNO', 'NIFTY-NOV2026-FUT')])

    def test_parser_rejects_empty_unsupported_non_ascii_control_and_oversized_inputs(self):
        for value in ('', 'RELIANCE,', 'BSE_EQ:RELIANCE', 'NSE:RELIANCE', 'NSE_EQ:A:B',
                      'REL\nIANCE', 'RÉLIANCE', 'A' * 129, None):
            with self.subTest(value=value):
                with self.assertRaises(resolver.DhanInstrumentError) as caught:
                    resolver.parse_symbols(value)
                self.assertEqual(caught.exception.code, 'invalid_symbols')
        with self.assertRaises(resolver.DhanInstrumentError):
            resolver.parse_symbols('RELIANCE,HDFCBANK', max_instruments=1)
        with self.assertRaises(resolver.DhanInstrumentError):
            resolver.parse_symbols('RELIANCE,' * 100, max_instruments=1)

    def test_direct_ids_are_compatible_and_do_not_touch_master_or_network(self):
        with mock.patch.object(resolver, '_master') as load:
            selected = resolver.resolve_selection(instruments='NSE_EQ:001333,NSE_EQ:1333', max_instruments=1000)
        self.assertEqual([(item.exchange_segment, item.security_id) for item in selected], [('NSE_EQ', '1333')])
        self.assertEqual(set(selected[0].__dict__), {'exchange_segment', 'security_id'})
        load.assert_not_called()

    def test_symbols_and_explicit_ids_conflict_before_any_network_access(self):
        with mock.patch.object(resolver, '_master') as load:
            with self.assertRaises(resolver.DhanInstrumentError) as caught:
                resolver.resolve_selection(symbols='RELIANCE', instruments='NSE_EQ:2885')
        self.assertEqual(caught.exception.code, 'selection_conflict')
        load.assert_not_called()

    def test_cash_only_eq_series_exact_trading_symbol_and_index_requires_prefix(self):
        body = master(row('RELIANCE', 999, series='BE'), row('RELIANCE', 10, exchange='BSE'),
                      row('RELIANCE', 2885), row('RELIANCE', 2885),
                      row('OTHER', 55, display='HDFCBANK'),
                      row('NIFTY 50', 13, segment='I', instrument='INDEX', series='X'))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'master.csv'
            cache(path, body)
            selected = resolver.resolve_selection('RELIANCE,IDX_I:NIFTY 50', cache_path=path, now=NOW)
            self.assertEqual([item.security_id for item in selected], ['2885', '13'])
            for value in ('HDFCBANK', 'NIFTY 50', 'IDX_I:NIFTY  50', 'REL'):
                with self.subTest(value=value):
                    with self.assertRaises(resolver.DhanInstrumentError) as caught:
                        resolver.resolve_selection(value, cache_path=path, now=NOW)
                    self.assertEqual(caught.exception.code, 'unknown_symbol')

    def test_ambiguity_fails_closed_and_long_ids_are_never_float_converted(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'master.csv'
            cache(path, master(row('RELIANCE', 1), row('RELIANCE', 2)))
            with self.assertRaises(resolver.DhanInstrumentError) as caught:
                resolver.resolve_selection('RELIANCE', cache_path=path, now=NOW)
            self.assertEqual(caught.exception.code, 'ambiguous_symbol')
            cache(path, master(row('RELIANCE', '9007199254740993123')))
            self.assertEqual(resolver.resolve_selection('RELIANCE', cache_path=path, now=NOW)[0].security_id,
                             '9007199254740993123')

    def test_explicit_index_accepts_exact_provider_display_name_but_rejects_ambiguity(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'master.csv'
            nifty = row('NIFTY', 13, segment='I', instrument='INDEX', series='X', display='Nifty 50')
            cache(path, master(nifty))
            selected = resolver.resolve_selection('IDX_I:NIFTY 50', cache_path=path, now=NOW)
            self.assertEqual(selected[0].security_id, '13')
            with self.assertRaises(resolver.DhanInstrumentError):
                resolver.resolve_selection('NIFTY 50', cache_path=path, now=NOW)
            duplicate = row('OTHER INDEX', 99, segment='I', instrument='INDEX', series='X', display='Nifty 50')
            cache(path, master(nifty, duplicate))
            with self.assertRaises(resolver.DhanInstrumentError) as caught:
                resolver.resolve_selection('IDX_I:NIFTY 50', cache_path=path, now=NOW)
            self.assertEqual(caught.exception.code, 'ambiguous_symbol')

    def test_derivatives_need_exact_unexpired_contract_and_explicit_segment(self):
        body = master(row('NIFTY-Nov2026-FUT', 1001, segment='D', instrument='FUTIDX', series='', expiry='2026-11-24 14:30:00'),
                      row('NIFTY-Oct2026-FUT', 1002, segment='D', instrument='FUTIDX', series='', expiry='2026-10-25 14:30:00'),
                      row('USDINR-Nov2026-FUT', 2001, segment='C', instrument='FUTCUR', series='', expiry='2026-11-24 14:30:00'))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'master.csv'
            cache(path, body)
            selected = resolver.resolve_selection('NSE_FNO:NIFTY-Nov2026-FUT,NSE_CURRENCY:USDINR-Nov2026-FUT',
                                                  cache_path=path, now=NOW)
            self.assertEqual([item.security_id for item in selected], ['1001', '2001'])
            for symbol in ('NIFTY-Nov2026-FUT', 'NSE_FNO:NIFTY', 'NSE_FNO:NIFTY-Oct2026-FUT'):
                with self.assertRaises(resolver.DhanInstrumentError) as caught:
                    resolver.resolve_selection(symbol, cache_path=path, now=NOW)
                self.assertEqual(caught.exception.code, 'unknown_symbol')

    def test_download_is_fixed_https_bounded_and_has_no_session_auth_headers(self):
        session = Session()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'nested' / 'master.csv'
            selected = resolver.resolve_selection('RELIANCE,HDFCBANK', cache_path=path, session=session, now=NOW)
            self.assertEqual([item.security_id for item in selected], ['2885', '1333'])
            self.assertEqual(path.read_bytes(), BODY)
            self.assertEqual(list(path.parent.iterdir()), [path])
        request, kwargs = session.calls[0]
        self.assertEqual(request.url, resolver.MASTER_URL)
        self.assertEqual(request.method, 'GET')
        self.assertNotIn('Authorization', request.headers)
        self.assertNotIn('access-token', request.headers)
        self.assertNotIn('Cookie', request.headers)
        self.assertIs(kwargs['allow_redirects'], False)
        self.assertEqual(kwargs['timeout'], (5, 30))
        self.assertTrue(session.response.closed)

    def test_fresh_cache_is_reused_without_network_and_ttl_is_exactly_24_hours(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'master.csv'
            cache(path, age=timedelta(hours=23, minutes=59))
            session = Session()
            resolver.resolve_selection('RELIANCE', cache_path=path, session=session, clock=lambda: NOW)
            self.assertEqual(len(session.calls), 0)
            cache(path, age=timedelta(hours=24))
            resolver.resolve_selection('RELIANCE', cache_path=path, session=session, now=NOW)
            self.assertEqual(len(session.calls), 1)

    def test_stale_cache_is_not_used_when_refresh_fails_and_errors_are_static(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'private-cache-path.csv'
            cache(path, age=timedelta(days=2))
            session = Session(error=requests.RequestException('SECRET_TEXT and /private/path'))
            with self.assertRaises(resolver.DhanInstrumentError) as caught:
                resolver.resolve_selection('RELIANCE', cache_path=path, session=session, now=NOW)
            self.assertEqual(caught.exception.code, 'master_unavailable')
            self.assertNotIn('SECRET_TEXT', str(caught.exception))
            self.assertNotIn(str(path), str(caught.exception))
            self.assertEqual(path.read_bytes(), BODY)

    def test_readonly_resolution_never_downloads_missing_or_stale_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'master.csv'
            session = Session()
            for exists in (False, True):
                if exists:
                    cache(path, age=timedelta(days=2))
                with self.assertRaises(resolver.DhanInstrumentError) as caught:
                    resolver.resolve_selection('RELIANCE', cache_path=path, session=session, now=NOW, allow_refresh=False)
                self.assertEqual(caught.exception.code, 'master_unavailable')
            self.assertEqual(session.calls, [])

    def test_invalid_body_redirect_and_oversized_response_never_replace_cache(self):
        for response in (Response(b'<html>not CSV</html>'), Response(status=302),
                         Response(url='https://untrusted.invalid/master.csv'),
                         Response(length=str(resolver.MAX_MASTER_BYTES + 1))):
            with self.subTest(response=response), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / 'master.csv'
                with self.assertRaises(resolver.DhanInstrumentError) as caught:
                    resolver.resolve_selection('RELIANCE', cache_path=path, session=Session(response), now=NOW)
                self.assertEqual(caught.exception.code, 'master_unavailable')
                self.assertFalse(path.exists())
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(resolver, 'MAX_MASTER_BYTES', 200):
            with self.assertRaises(resolver.DhanInstrumentError):
                resolver.resolve_selection('RELIANCE', cache_path=Path(directory) / 'master.csv',
                                           session=Session(Response(length='1')), now=NOW)

    def test_failed_atomic_write_preserves_old_cache_and_removes_temporary_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'master.csv'
            cache(path, age=timedelta(days=2))
            with mock.patch.object(resolver.os, 'replace', side_effect=OSError('sensitive path')):
                with self.assertRaises(resolver.DhanInstrumentError) as caught:
                    resolver.resolve_selection('RELIANCE', cache_path=path, session=Session(), now=NOW)
            self.assertEqual(caught.exception.code, 'master_unavailable')
            self.assertEqual(path.read_bytes(), BODY)
            self.assertEqual(list(path.parent.iterdir()), [path])

    def test_status_is_syntax_only_and_never_reads_writes_or_fetches_master(self):
        with mock.patch.object(resolver, '_master') as load, \
             mock.patch.object(resolver.requests, 'Session') as network, \
             mock.patch.object(resolver.Path, 'read_bytes') as read:
            status = resolver.inspect_selection('RELIANCE,IDX_I:NIFTY 50')
            ids = resolver.inspect_selection(instruments='NSE_EQ:2885')
            conflict = resolver.inspect_selection('RELIANCE', 'NSE_EQ:2885')
        self.assertEqual((status['valid'], status['count'], status['resolution_required'], status['configured_validated']),
                         (True, 2, True, False))
        self.assertTrue(ids['configured_validated'])
        self.assertFalse(conflict['valid'])
        self.assertEqual(conflict['code'], 'selection_conflict')
        load.assert_not_called()
        network.assert_not_called()
        read.assert_not_called()

    def test_environment_selection_uses_the_injected_cache_path(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'master.csv'
            cache(path)
            environment = {'DHAN_SYMBOLS': 'HDFCBANK', 'DHAN_INSTRUMENTS': '',
                           'DHAN_INSTRUMENT_CACHE_FILE': str(path), 'DHAN_API_SECRET': 'must-not-be-used'}
            selected = resolver.resolve_from_env(environment, now=NOW, allow_refresh=False)
        self.assertEqual(selected[0].security_id, '1333')
