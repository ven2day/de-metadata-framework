import base64
from datetime import datetime, timedelta, timezone
import importlib
import json
import os
from pathlib import Path
import tempfile
from unittest import TestCase, mock

from ingestion.dhan.client import DhanInstrument
from ingestion.dhan.instruments import DhanInstrumentError, read_symbols_file
from ui.dhan_setup import india_date, inspect_setup, snapshot_form


NOW = datetime(2026, 10, 26, 0, 0, tzinfo=timezone.utc)
BASE_ENV = {
    'FLASK_SECRET_KEY': 'test-only-ui-dhan-key',
    'DHAN_ACCESS_TOKEN_FILE': '/tmp/no-saved-dhan-token-for-unit-tests.json',
}
READY_ENV = {
    **BASE_ENV, 'DHAN_CLIENT_ID': '1000000001', 'DHAN_ACCESS_TOKEN': 'opaque-test-token-secret',
    'DHAN_INSTRUMENTS': 'NSE_EQ:1333,NSE_EQ:1333,IDX_I:13',
}
SYMBOLS_ENV = {**READY_ENV, 'DHAN_INSTRUMENTS': '', 'DHAN_SYMBOLS': 'RELIANCE,HDFCBANK'}
RESOLVED = [DhanInstrument('NSE_EQ', '2885'), DhanInstrument('NSE_EQ', '1333')]


def jwt(expiry):
    payload = base64.urlsafe_b64encode(json.dumps({'exp': int(expiry.timestamp())}).encode()).decode().rstrip('=')
    return 'header.' + payload + '.signature'


class DhanSetupTests(TestCase):
    def test_missing_settings_are_nonsecret_and_unverified(self):
        result = inspect_setup(BASE_ENV, NOW)
        self.assertFalse(result['configured'])
        self.assertFalse(result['ready_to_run'])
        self.assertFalse(result['connection_verified'])
        self.assertEqual(result['authentication']['method'], 'missing')
        self.assertIn('DHAN_CLIENT_ID', result['missing'])
        self.assertIn('DHAN_ACCESS_TOKEN', result['missing'])
        self.assertIn('DHAN_SYMBOLS', result['missing'])
        self.assertFalse(result['credentials_ready'])
        self.assertEqual(result['instruments']['selection_mode'], 'missing')
        self.assertEqual(result['symbols'], {'configured': False, 'count': 0, 'syntax_valid': False})
        self.assertEqual(result['live'], {'running': None, 'status': 'not_checked'})
        self.assertNotIn(BASE_ENV['DHAN_ACCESS_TOKEN_FILE'], json.dumps(result))

    def test_configured_token_is_ready_to_try_without_claiming_a_connection(self):
        result = inspect_setup(READY_ENV, NOW)
        self.assertTrue(result['ready_to_run'])
        self.assertFalse(result['connection_verified'])
        self.assertEqual(result['authentication']['method'], 'access_token')
        self.assertIsNone(result['authentication']['token_expired'])
        self.assertEqual(result['instruments']['count'], 2)
        self.assertTrue(result['credentials_ready'])
        self.assertEqual(result['instruments']['selection_mode'], 'security_ids')
        self.assertFalse(result['instruments']['resolution_required'])
        rendered = json.dumps(result)
        for value in (READY_ENV['DHAN_ACCESS_TOKEN'], READY_ENV['DHAN_CLIENT_ID'], 'NSE_EQ:1333'):
            self.assertNotIn(value, rendered)

    def test_symbols_status_checks_syntax_without_reading_or_resolving_master(self):
        with mock.patch('ui.dhan_setup.resolve_selection') as resolve, \
             mock.patch('requests.sessions.Session.request') as network:
            result = inspect_setup(SYMBOLS_ENV, NOW)
        self.assertTrue(result['credentials_ready'])
        self.assertTrue(result['ready_to_run'])
        self.assertFalse(result['connection_verified'])
        self.assertEqual(result['symbols'], {'configured': True, 'count': 2, 'syntax_valid': True})
        self.assertEqual(result['instruments']['selection_mode'], 'symbols')
        self.assertTrue(result['instruments']['resolution_required'])
        self.assertNotIn('RELIANCE', json.dumps(result))
        resolve.assert_not_called()
        network.assert_not_called()

    def test_symbol_conflicts_and_invalid_syntax_are_not_ready(self):
        cases = (
            ({**READY_ENV, 'DHAN_SYMBOLS': 'RELIANCE'}, 'conflict'),
            ({**SYMBOLS_ENV, 'DHAN_SYMBOLS': '<script>secret</script>'}, 'symbols'),
            ({**SYMBOLS_ENV, 'DHAN_SYMBOLS': ','.join(f'STOCK{number}' for number in range(1001))}, 'symbols'),
        )
        with mock.patch('ui.dhan_setup.resolve_selection') as resolve:
            for environment, mode in cases:
                with self.subTest(mode=mode, count=len(environment['DHAN_SYMBOLS'])):
                    result = inspect_setup(environment, NOW)
                    self.assertTrue(result['credentials_ready'])
                    self.assertFalse(result['ready_to_run'])
                    self.assertFalse(result['instruments']['valid'])
                    self.assertEqual(result['instruments']['selection_mode'], mode)
                    self.assertNotIn('<script>', json.dumps(result))
        resolve.assert_not_called()

    def test_symbols_csv_status_is_local_readonly_and_does_not_export_selection(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'private-selection-location.csv'
            path.write_text('symbol\nRELIANCE\nHDFCBANK\n', encoding='utf-8')
            before = (path.read_bytes(), path.stat().st_mtime_ns)
            environment = {**READY_ENV, 'DHAN_INSTRUMENTS': '', 'DHAN_SYMBOLS_FILE': str(path)}
            with mock.patch('ui.dhan_setup.resolve_selection') as resolve, \
                 mock.patch('requests.sessions.Session.request') as network, \
                 mock.patch('ingestion.dhan.client.write_token_file') as write:
                result = inspect_setup(environment, NOW)
            self.assertTrue(result['configured'])
            self.assertTrue(result['ready_to_run'])
            self.assertFalse(result['connection_verified'])
            self.assertEqual(result['symbols'], {'configured': True, 'count': 2, 'syntax_valid': True})
            self.assertEqual(result['instruments'], {
                'configured': True, 'valid': True, 'count': 2,
                'selection_mode': 'symbols_file', 'resolution_required': True,
            })
            self.assertEqual((path.read_bytes(), path.stat().st_mtime_ns), before)
            for value in (str(path), 'RELIANCE', 'HDFCBANK', READY_ENV['DHAN_ACCESS_TOKEN']):
                self.assertNotIn(value, json.dumps(result))
            resolve.assert_not_called()
            network.assert_not_called()
            write.assert_not_called()

    def test_missing_malformed_and_conflicting_csv_settings_fail_closed_without_leaks(self):
        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory) / 'private-missing.csv'
            invalid = Path(directory) / 'private-invalid.csv'
            invalid.write_text('wrong_header\nSECRET_FILE_CONTENT\n', encoding='utf-8')
            cases = (
                (missing, '', 'symbols_file', 'symbols_file_unavailable'),
                (invalid, '', 'symbols_file', 'symbols_file_invalid'),
                (missing, 'NSE_EQ:1333', 'conflict', 'selection_conflict'),
            )
            with mock.patch('ui.dhan_setup.resolve_selection') as resolve:
                for path, ids, mode, code in cases:
                    with self.subTest(code=code):
                        environment = {**READY_ENV, 'DHAN_INSTRUMENTS': ids, 'DHAN_SYMBOLS_FILE': str(path)}
                        result = inspect_setup(environment, NOW)
                        self.assertFalse(result['ready_to_run'])
                        self.assertTrue(result['instruments']['configured'])
                        self.assertEqual(result['instruments']['selection_mode'], mode)
                        self.assertIn('DHAN_SYMBOLS_FILE', result['missing'])
                        self.assertEqual(result['symbols']['count'], 0)
                        with self.assertRaises(DhanInstrumentError) as caught:
                            snapshot_form({}, environment, NOW)
                        self.assertEqual(caught.exception.code, code)
                        rendered = json.dumps(result) + str(caught.exception)
                        for value in (str(path), 'SECRET_FILE_CONTENT'):
                            self.assertNotIn(value, rendered)
            resolve.assert_not_called()

    def test_oauth_key_secret_alone_require_consent(self):
        environment = {**READY_ENV, 'DHAN_ACCESS_TOKEN': '', 'DHAN_API_KEY': 'key-secret-marker',
                       'DHAN_API_SECRET': 'oauth-secret-marker'}
        result = inspect_setup(environment, NOW)
        self.assertFalse(result['ready_to_run'])
        self.assertTrue(result['authentication']['oauth_configured'])
        self.assertEqual(result['authentication']['method'], 'oauth_consent_required')
        self.assertNotIn('oauth-secret-marker', json.dumps(result))

    def test_expired_and_expiring_tokens_are_not_ready(self):
        for offset, expired in ((-1, True), (30, False)):
            result = inspect_setup({**READY_ENV, 'DHAN_ACCESS_TOKEN': jwt(NOW + timedelta(seconds=offset))}, NOW)
            self.assertFalse(result['ready_to_run'])
            self.assertIs(result['authentication']['token_expired'], expired)

    def test_saved_token_resolves_client_and_ist_expiry_without_exposing_values(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'token-secret-location.json'
            path.write_text(json.dumps({'access_token': 'saved-secret-marker', 'client_id': '1000000001',
                                        'expiry_time': '2026-10-26T10:30:00'}), encoding='utf-8')
            environment = {**READY_ENV, 'DHAN_CLIENT_ID': '', 'DHAN_ACCESS_TOKEN': '',
                           'DHAN_ACCESS_TOKEN_FILE': str(path)}
            result = inspect_setup(environment, NOW)
            self.assertTrue(result['ready_to_run'])
            self.assertTrue(result['client_id_configured'])
            self.assertEqual(result['authentication']['method'], 'token_file')
            self.assertNotIn(str(path), json.dumps(result))
            self.assertNotIn('saved-secret-marker', json.dumps(result))
            path.write_text(json.dumps({'access_token': 'saved-secret-marker', 'client_id': '1000000001',
                                        'expiry_time': '2026-10-26T04:00:00'}), encoding='utf-8')
            expired = inspect_setup(environment, NOW)
            self.assertFalse(expired['ready_to_run'])
            self.assertTrue(expired['authentication']['token_expired'])

    def test_malformed_or_mismatched_saved_token_is_not_ready(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'private-token-path.json'
            environment = {**READY_ENV, 'DHAN_ACCESS_TOKEN': '', 'DHAN_ACCESS_TOKEN_FILE': str(path)}
            for value in ('secret-invalid-json', json.dumps({'access_token': 'secret', 'client_id': 'different-client'})):
                path.write_text(value, encoding='utf-8')
                result = inspect_setup(environment, NOW)
                self.assertFalse(result['ready_to_run'])
                self.assertNotIn(str(path), json.dumps(result))
                self.assertNotIn(value, json.dumps(result))

    def test_headless_auth_requires_six_digit_pin_and_base32_seed(self):
        for seed, ready in (('654321', False), ('not-a-base32-secret', False), ('JBSWY3DPEHPK3PXP', True)):
            result = inspect_setup({**READY_ENV, 'DHAN_ACCESS_TOKEN': '', 'DHAN_PIN': '123456',
                                    'DHAN_TOTP_SECRET': seed}, NOW)
            self.assertEqual(result['ready_to_run'], ready)
            self.assertEqual(result['authentication']['pin_totp_configured'], ready)
            self.assertNotIn(seed, json.dumps(result))

    def test_invalid_and_oversized_rest_instrument_sets_are_rejected(self):
        for instruments in ('NSE_EQ:SECRET_VALUE', 'BSE_EQ:500325',
                            ','.join(f'NSE_EQ:{number}' for number in range(1, 1002))):
            result = inspect_setup({**READY_ENV, 'DHAN_INSTRUMENTS': instruments}, NOW)
            self.assertFalse(result['ready_to_run'])
            self.assertFalse(result['instruments']['valid'])
            self.assertIn('DHAN_INSTRUMENTS', result['missing'])
            self.assertNotIn('SECRET_VALUE', json.dumps(result))

    def test_snapshot_defaults_and_validation_use_india_date(self):
        instant = datetime(2026, 10, 25, 20, tzinfo=timezone.utc)
        self.assertEqual(india_date(instant), '2026-10-26')
        form = snapshot_form({'source_type': 'dhan'}, READY_ENV, instant)
        self.assertEqual(form['ingest_date'], '2026-10-26')
        self.assertEqual(form['application_name'], 'nse_market_snapshot')
        self.assertEqual(form['metadata_key'], 'examples/dhan_market_snapshot.csv')
        self.assertEqual(form['write_mode'], 'overwrite')
        self.assertEqual(form['dhan_instruments'], 'NSE_EQ:1333,IDX_I:13')
        for invalid in ({'ingest_date': '2026-10-25'}, {'dhan_snapshot_mode': 'quote;drop'},
                        {'dhan_instruments': 'NSE_EQ:bad'}, {'DHAN_API_SECRET': 'must-not-be-accepted'}):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    snapshot_form(invalid, READY_ENV, instant)

    def test_snapshot_symbols_override_saved_ids_and_resolve_once(self):
        with mock.patch('ui.dhan_setup.resolve_selection', return_value=RESOLVED) as resolve:
            form = snapshot_form({'dhan_symbols': 'RELIANCE,HDFCBANK'}, READY_ENV, NOW)
            replay = snapshot_form(form, READY_ENV, NOW)
        resolve.assert_called_once_with(symbols='RELIANCE,HDFCBANK', instruments='', max_instruments=1000)
        self.assertEqual(form['dhan_symbols'], '')
        self.assertEqual(form['dhan_instruments'], 'NSE_EQ:2885,NSE_EQ:1333')
        self.assertEqual(replay['dhan_instruments'], form['dhan_instruments'])
        self.assertEqual(READY_ENV['DHAN_INSTRUMENTS'], 'NSE_EQ:1333,NSE_EQ:1333,IDX_I:13')

    def test_snapshot_csv_is_read_and_resolved_once_then_replayed_as_ids(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'private-selection.csv'
            path.write_text('symbol\nRELIANCE\nHDFCBANK\n', encoding='utf-8')
            environment = {**READY_ENV, 'DHAN_INSTRUMENTS': '', 'DHAN_SYMBOLS_FILE': str(path)}
            with mock.patch('ingestion.dhan.instruments.read_symbols_file', wraps=read_symbols_file) as read, \
                 mock.patch('ui.dhan_setup.resolve_selection', return_value=RESOLVED) as resolve:
                form = snapshot_form({}, environment, NOW)
                replay = snapshot_form(form, environment, NOW)
            read.assert_called_once_with(str(path), max_instruments=1000)
            resolve.assert_called_once_with(symbols='RELIANCE,HDFCBANK', instruments='', max_instruments=1000)
            self.assertEqual(form['dhan_symbols'], '')
            self.assertEqual(form['dhan_instruments'], 'NSE_EQ:2885,NSE_EQ:1333')
            self.assertEqual(replay['dhan_instruments'], form['dhan_instruments'])
            self.assertNotIn(str(path), json.dumps(form))
            self.assertEqual(environment['DHAN_SYMBOLS_FILE'], str(path))

    def test_explicit_symbols_or_ids_override_csv_without_reading_it(self):
        environment = {**READY_ENV, 'DHAN_INSTRUMENTS': '', 'DHAN_SYMBOLS_FILE': '/private/unavailable.csv'}
        with mock.patch('ingestion.dhan.instruments.read_symbols_file') as read, \
             mock.patch('ui.dhan_setup.resolve_selection', return_value=RESOLVED) as resolve:
            symbols = snapshot_form({'dhan_symbols': 'RELIANCE,HDFCBANK'}, environment, NOW)
            ids = snapshot_form({'dhan_instruments': 'NSE_EQ:1333'}, environment, NOW)
        read.assert_not_called()
        resolve.assert_called_once_with(symbols='RELIANCE,HDFCBANK', instruments='', max_instruments=1000)
        self.assertEqual(symbols['dhan_instruments'], 'NSE_EQ:2885,NSE_EQ:1333')
        self.assertEqual(ids['dhan_instruments'], 'NSE_EQ:1333')

    def test_snapshot_never_accepts_a_symbols_file_path_from_the_request(self):
        with mock.patch('ingestion.dhan.instruments.read_symbols_file') as read, \
             mock.patch('ui.dhan_setup.resolve_selection') as resolve:
            for key in ('symbols_file', 'dhan_symbols_file', 'DHAN_SYMBOLS_FILE'):
                with self.subTest(key=key), self.assertRaises(ValueError) as caught:
                    snapshot_form({key: '/private/SENSITIVE.csv'}, READY_ENV, NOW)
                self.assertNotIn('SENSITIVE', str(caught.exception))
        read.assert_not_called()
        resolve.assert_not_called()

    def test_snapshot_rejects_invalid_local_settings_before_symbol_resolution(self):
        cases = (
            ({'ingest_date': '2000-01-01'}, SYMBOLS_ENV),
            ({'dhan_snapshot_mode': 'invalid'}, SYMBOLS_ENV),
            ({'dhan_symbols': '<invalid>'}, READY_ENV),
            ({'dhan_symbols': 'RELIANCE', 'dhan_instruments': 'NSE_EQ:1333'}, READY_ENV),
            ({}, {**READY_ENV, 'DHAN_SYMBOLS': 'RELIANCE'}),
            ({'dhan_symbols': 'RELIANCE'}, BASE_ENV),
            ({'dhan_symbols': 'RELIANCE', 'DHAN_ACCESS_TOKEN': 'must-not-echo'}, READY_ENV),
        )
        with mock.patch('ui.dhan_setup.resolve_selection') as resolve:
            for form, environment in cases:
                with self.subTest(fields=list(form)):
                    with self.assertRaises(ValueError):
                        snapshot_form(form, environment, NOW)
        resolve.assert_not_called()


class DhanUiTests(TestCase):
    @classmethod
    def setUpClass(cls):
        with mock.patch.dict(os.environ, BASE_ENV):
            cls.module = importlib.import_module('ui.app')

    def setUp(self):
        self.environment = mock.patch.dict(os.environ, BASE_ENV, clear=True)
        self.environment.start()
        self.old_config = {key: self.module.app.config.get(key) for key in ('TESTING', 'LOGIN_DISABLED', 'WTF_CSRF_ENABLED')}
        self.module.app.config.update(TESTING=True, LOGIN_DISABLED=True, WTF_CSRF_ENABLED=False)
        self.module.limiter.reset()

    def tearDown(self):
        self.module.app.config.update(self.old_config)
        self.environment.stop()

    def test_status_is_local_readonly_and_never_verifies_or_mints(self):
        with mock.patch('requests.sessions.Session.request') as network, \
             mock.patch('ingestion.dhan.client.DhanTokenProvider.get_token') as mint, \
             mock.patch('ingestion.dhan.client.write_token_file') as write, \
             mock.patch.object(self.module, 'resolve_dhan_selection') as ui_resolve, \
             mock.patch('ui.dhan_setup.resolve_selection') as setup_resolve, \
             mock.patch('docker.DockerClient') as docker:
            response = self.module.app.test_client().get('/dhan/status')
        self.assertEqual(response.status_code, 200)
        self.assertIn('no-store', response.headers['Cache-Control'])
        self.assertFalse(response.get_json()['connection_verified'])
        network.assert_not_called()
        mint.assert_not_called()
        write.assert_not_called()
        ui_resolve.assert_not_called()
        setup_resolve.assert_not_called()
        docker.assert_not_called()

    def test_resolve_requires_login_and_csrf(self):
        self.module.app.config.update(LOGIN_DISABLED=False, WTF_CSRF_ENABLED=False)
        with mock.patch.object(self.module, 'resolve_dhan_selection') as resolve:
            response = self.module.app.test_client().post('/dhan/resolve', data={'symbols': 'RELIANCE'})
        self.assertEqual(response.status_code, 302)
        self.assertIn('/login', response.headers['Location'])
        resolve.assert_not_called()
        self.module.app.config.update(LOGIN_DISABLED=True, WTF_CSRF_ENABLED=True)
        with mock.patch.object(self.module, 'resolve_dhan_selection') as resolve:
            response = self.module.app.test_client().post('/dhan/resolve', data={'symbols': 'RELIANCE'})
        self.assertEqual(response.status_code, 400)
        resolve.assert_not_called()

    def test_explicit_symbol_resolution_returns_only_public_mapping(self):
        with mock.patch.object(self.module, 'resolve_dhan_selection', return_value=RESOLVED) as resolve, \
             mock.patch('ingestion.dhan.client.DhanTokenProvider.get_token') as mint, \
             mock.patch('ingestion.dhan.client.write_token_file') as write, \
             mock.patch('docker.DockerClient') as docker:
            response = self.module.app.test_client().post('/dhan/resolve', data={'symbols': 'RELIANCE,HDFCBANK'})
        self.assertEqual(response.status_code, 200)
        self.assertIn('no-store', response.headers['Cache-Control'])
        self.assertIn('private', response.headers['Cache-Control'])
        self.assertEqual(response.get_json()['count'], 2)
        self.assertEqual(response.get_json()['instruments'], [
            {'symbol': 'RELIANCE', 'exchange_segment': 'NSE_EQ', 'security_id': '2885'},
            {'symbol': 'HDFCBANK', 'exchange_segment': 'NSE_EQ', 'security_id': '1333'},
        ])
        self.assertEqual(set(response.get_json()), {'count', 'instruments', 'message'})
        resolve.assert_called_once_with(symbols='RELIANCE,HDFCBANK', max_instruments=1000)
        mint.assert_not_called()
        write.assert_not_called()
        docker.assert_not_called()

    def test_resolve_uses_saved_symbols_when_form_is_blank(self):
        with mock.patch.dict(os.environ, SYMBOLS_ENV), \
             mock.patch.object(self.module, 'resolve_dhan_selection', return_value=RESOLVED) as resolve:
            response = self.module.app.test_client().post('/dhan/resolve', data={'symbols': ''})
        self.assertEqual(response.status_code, 200)
        resolve.assert_called_once_with(symbols='RELIANCE,HDFCBANK', max_instruments=1000)

    def test_resolve_previews_server_csv_without_brokerage_authentication(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'private-selection.csv'
            path.write_text('symbol\nRELIANCE\nHDFCBANK\n', encoding='utf-8')
            environment = {**BASE_ENV, 'DHAN_SYMBOLS_FILE': str(path)}
            before = (path.read_bytes(), path.stat().st_mtime_ns)
            with mock.patch.dict(os.environ, environment), \
                 mock.patch.object(self.module, 'resolve_dhan_selection', return_value=RESOLVED) as resolve, \
                 mock.patch('ingestion.dhan.client.DhanTokenProvider.get_token') as mint, \
                 mock.patch('ingestion.dhan.client.write_token_file') as write, \
                 mock.patch('docker.DockerClient') as docker:
                response = self.module.app.test_client().post('/dhan/resolve', data={'symbols': ''})
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.get_json()['count'], 2)
            self.assertNotIn(str(path), response.get_data(as_text=True))
            self.assertEqual((path.read_bytes(), path.stat().st_mtime_ns), before)
            resolve.assert_called_once_with(symbols='RELIANCE,HDFCBANK', max_instruments=1000)
            mint.assert_not_called()
            write.assert_not_called()
            docker.assert_not_called()

    def test_resolve_reports_static_csv_errors_and_typed_symbols_override_file(self):
        with tempfile.TemporaryDirectory() as directory:
            missing = Path(directory) / 'private-missing.csv'
            invalid = Path(directory) / 'private-invalid.csv'
            invalid.write_text('invalid\nSECRET_FILE_CONTENT\n', encoding='utf-8')
            with mock.patch.object(self.module, 'resolve_dhan_selection', return_value=RESOLVED) as resolve:
                for path, code in ((missing, 'symbols_file_unavailable'), (invalid, 'symbols_file_invalid')):
                    with self.subTest(code=code), mock.patch.dict(os.environ, {'DHAN_SYMBOLS_FILE': str(path)}):
                        response = self.module.app.test_client().post('/dhan/resolve', data={'symbols': ''})
                        self.assertEqual(response.status_code, 400)
                        self.assertEqual(response.get_json()['code'], code)
                        self.assertNotIn(str(path), response.get_data(as_text=True))
                        self.assertNotIn('SECRET_FILE_CONTENT', response.get_data(as_text=True))
                resolve.assert_not_called()
                with mock.patch.dict(os.environ, {'DHAN_SYMBOLS_FILE': str(missing)}), \
                     mock.patch('ingestion.dhan.instruments.read_symbols_file') as read:
                    response = self.module.app.test_client().post('/dhan/resolve', data={'symbols': 'RELIANCE,HDFCBANK'})
                self.assertEqual(response.status_code, 200)
                read.assert_not_called()
                resolve.assert_called_once_with(symbols='RELIANCE,HDFCBANK', max_instruments=1000)

    def test_web_requests_cannot_choose_a_local_symbols_file(self):
        with mock.patch.object(self.module, 'resolve_dhan_selection') as resolve, \
             mock.patch('ingestion.dhan.instruments.read_symbols_file') as read, \
             mock.patch('docker.DockerClient') as docker:
            for key in ('symbols_file', 'dhan_symbols_file', 'DHAN_SYMBOLS_FILE'):
                payload = {'symbols': 'RELIANCE', key: '/private/SENSITIVE.csv'}
                with self.subTest(key=key):
                    for response in (
                        self.module.app.test_client().post('/dhan/resolve', data=payload),
                        self.module.app.test_client().post('/dhan/resolve', json=payload),
                        self.module.app.test_client().post('/run', data={'source_type': 'dhan', **payload}),
                    ):
                        self.assertEqual(response.status_code, 400)
                        self.assertNotIn('SENSITIVE', response.get_data(as_text=True))
        resolve.assert_not_called()
        read.assert_not_called()
        docker.assert_not_called()

    def test_resolve_accepts_json_symbols_as_well_as_form_data(self):
        with mock.patch.object(self.module, 'resolve_dhan_selection', return_value=RESOLVED) as resolve:
            response = self.module.app.test_client().post('/dhan/resolve', json={'symbols': 'RELIANCE,HDFCBANK'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()['count'], 2)
        self.assertEqual(response.get_json()['instruments'][0]['symbol'], 'RELIANCE')
        resolve.assert_called_once_with(symbols='RELIANCE,HDFCBANK', max_instruments=1000)

    def test_resolve_json_rejects_credentials_and_wrong_types_before_download(self):
        payloads = ({'symbols': 'RELIANCE', 'DHAN_API_SECRET': 'SENSITIVE'},
                    {'symbols': ['RELIANCE']}, ['RELIANCE'],
                    {'symbols': False}, {'symbols': 0}, {'symbols': []}, {'symbols': None})
        with mock.patch.dict(os.environ, SYMBOLS_ENV), \
             mock.patch.object(self.module, 'resolve_dhan_selection') as resolve:
            for payload in payloads:
                with self.subTest(payload_type=type(payload).__name__):
                    response = self.module.app.test_client().post('/dhan/resolve', json=payload)
                    self.assertEqual(response.status_code, 400)
                    self.assertEqual(response.get_json()['code'], 'invalid_symbols')
                    self.assertNotIn('SENSITIVE', response.get_data(as_text=True))
        resolve.assert_not_called()

    def test_resolve_rejects_invalid_syntax_and_credentials_before_download(self):
        forms = ({'symbols': ''}, {'symbols': '<script>SENSITIVE</script>'},
                 {'symbols': 'RELIANCE', 'DHAN_API_SECRET': 'SENSITIVE'},
                 {'symbols': ','.join(f'STOCK{number}' for number in range(1001))})
        with mock.patch.object(self.module, 'resolve_dhan_selection') as resolve:
            for form in forms:
                with self.subTest(fields=list(form)):
                    response = self.module.app.test_client().post('/dhan/resolve', data=form)
                    self.assertEqual(response.status_code, 400)
                    self.assertEqual(response.get_json()['code'], 'invalid_symbols')
                    self.assertNotIn('SENSITIVE', response.get_data(as_text=True))
                    self.assertIn('no-store', response.headers['Cache-Control'])
        resolve.assert_not_called()

    def test_resolve_errors_have_static_safe_codes_and_messages(self):
        for code in ('unknown_symbol', 'ambiguous_symbol', 'selection_conflict', 'master_unavailable'):
            with self.subTest(code=code), \
                 mock.patch.object(self.module, 'resolve_dhan_selection', side_effect=DhanInstrumentError(code)):
                response = self.module.app.test_client().post('/dhan/resolve', data={'symbols': 'RELIANCE'})
                self.assertEqual(response.status_code, 503 if code == 'master_unavailable' else 400)
                self.assertEqual(response.get_json()['code'], code)
                self.assertEqual(set(response.get_json()), {'code', 'error'})
                self.assertIn('no-store', response.headers['Cache-Control'])
        with mock.patch.object(self.module, 'resolve_dhan_selection', side_effect=RuntimeError('SENSITIVE upstream response')):
            response = self.module.app.test_client().post('/dhan/resolve', data={'symbols': 'RELIANCE'})
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.get_json()['code'], 'master_unavailable')
        self.assertNotIn('SENSITIVE', response.get_data(as_text=True))

    def test_status_requires_login_and_snapshot_requires_csrf(self):
        self.module.app.config.update(LOGIN_DISABLED=False, WTF_CSRF_ENABLED=True)
        with mock.patch.object(self.module, 'inspect_dhan_setup') as inspect:
            response = self.module.app.test_client().get('/dhan/status')
        self.assertEqual(response.status_code, 302)
        inspect.assert_not_called()
        with mock.patch('docker.DockerClient') as docker:
            missing_csrf = self.module.app.test_client().post('/run', data={'source_type': 'dhan'})
        self.assertEqual(missing_csrf.status_code, 400)
        docker.assert_not_called()

    def test_missing_config_and_credentials_posts_fail_before_docker_launch(self):
        for form in ({'source_type': 'dhan'}, {'source_type': 'dhan', 'dhan_api_secret': 'secret-post-value'}):
            with mock.patch('docker.DockerClient') as docker:
                response = self.module.app.test_client().post('/run', data=form)
            self.assertEqual(response.status_code, 400)
            self.assertIsNotNone(response.get_json()['error'])
            self.assertNotIn('secret-post-value', response.get_data(as_text=True))
            docker.assert_not_called()

    def test_dhan_args_have_only_snapshot_fields_and_no_s3_key_requirement(self):
        with mock.patch.dict(os.environ, READY_ENV):
            args = self.module._build_pipeline_args({'source_type': 'dhan', 'dhan_snapshot_mode': 'ohlc'})
        self.assertEqual(args[args.index('--application-name') + 1], 'nse_market_snapshot')
        self.assertEqual(args[args.index('--dhan-mode') + 1], 'ohlc')
        self.assertEqual(args[args.index('--write-mode') + 1], 'overwrite')
        self.assertNotIn('--source-key', args)
        self.assertNotIn('--source-database', args)
        self.assertNotIn(READY_ENV['DHAN_ACCESS_TOKEN'], args)

    def test_explicit_snapshot_post_uses_default_contract_and_continues_to_bronze(self):
        ingestion = mock.Mock(id='dhan-snapshot-test')
        ingestion.logs.return_value = [b'snapshot complete\n']
        ingestion.wait.return_value = {'StatusCode': 0}
        bronze = mock.Mock(id='dhan-bronze-test')
        bronze.logs.return_value = [b'bronze complete\n']
        bronze.wait.return_value = {'StatusCode': 0}
        with mock.patch.dict(os.environ, READY_ENV), mock.patch('docker.DockerClient') as docker:
            docker.return_value.containers.run.side_effect = [ingestion, bronze]
            response = self.module.app.test_client().post('/run', data={
                'source_type': 'dhan', 'dhan_snapshot_mode': 'ltp',
            })
            output = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn('--- Exit 0 ---', output)
        self.assertIn('--- Bronze Exit 0 ---', output)
        calls = docker.return_value.containers.run.call_args_list
        args = calls[0].kwargs['command']
        self.assertEqual(args[args.index('--dhan-mode') + 1], 'ltp')
        self.assertEqual(args[args.index('--write-mode') + 1], 'overwrite')
        self.assertEqual(args[args.index('--metadata-key') + 1], 'examples/dhan_market_snapshot.csv')
        self.assertEqual(calls[1].kwargs['command'][0], 'bronze')
        self.assertFalse(any(key.startswith('DHAN_') for key in calls[1].kwargs['environment']))
        ingestion.remove.assert_called_once()
        bronze.remove.assert_called_once()

    def test_symbols_post_resolves_once_and_passes_only_canonical_ids_to_child(self):
        ingestion = mock.Mock(id='dhan-symbols-test')
        ingestion.logs.return_value = [b'snapshot complete\n']
        ingestion.wait.return_value = {'StatusCode': 0}
        with mock.patch.dict(os.environ, {**READY_ENV, 'DHAN_API_KEY': 'unrelated-api-key',
                                         'DHAN_API_SECRET': 'unrelated-api-secret'}), \
             mock.patch('ui.dhan_setup.resolve_selection', return_value=RESOLVED) as resolve, \
             mock.patch('docker.DockerClient') as docker:
            docker.return_value.containers.run.return_value = ingestion
            response = self.module.app.test_client().post('/run', data={
                'source_type': 'dhan', 'dhan_symbols': 'RELIANCE,HDFCBANK', 'skip_bronze': '1',
            })
            output = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn('--- Exit 0 ---', output)
        resolve.assert_called_once_with(symbols='RELIANCE,HDFCBANK', instruments='', max_instruments=1000)
        launch = docker.return_value.containers.run.call_args.kwargs
        args, environment = launch['command'], launch['environment']
        self.assertEqual(args[args.index('--dhan-instruments') + 1], 'NSE_EQ:2885,NSE_EQ:1333')
        self.assertNotIn('--dhan-symbols', args)
        self.assertNotIn('RELIANCE,HDFCBANK', args)
        self.assertEqual(environment['DHAN_SYMBOLS'], '')
        self.assertEqual(environment['DHAN_SYMBOLS_FILE'], '')
        self.assertEqual(environment['DHAN_INSTRUMENTS'], 'NSE_EQ:2885,NSE_EQ:1333')
        self.assertNotIn('DHAN_API_KEY', environment)
        self.assertNotIn('DHAN_API_SECRET', environment)
        ingestion.remove.assert_called_once()

    def test_csv_snapshot_resolves_once_and_child_needs_no_csv_mount(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'private-selection.csv'
            path.write_text('symbol\nRELIANCE\nHDFCBANK\n', encoding='utf-8')
            environment = {**READY_ENV, 'DHAN_INSTRUMENTS': '', 'DHAN_SYMBOLS_FILE': str(path)}
            ingestion = mock.Mock(id='dhan-csv-test')
            ingestion.logs.return_value = [b'snapshot complete\n']
            ingestion.wait.return_value = {'StatusCode': 0}
            with mock.patch.dict(os.environ, environment), \
                 mock.patch('ingestion.dhan.instruments.read_symbols_file', wraps=read_symbols_file) as read, \
                 mock.patch('ui.dhan_setup.resolve_selection', return_value=RESOLVED) as resolve, \
                 mock.patch('docker.DockerClient') as docker:
                docker.return_value.containers.run.return_value = ingestion
                response = self.module.app.test_client().post('/run', data={'source_type': 'dhan', 'skip_bronze': '1'})
                output = response.get_data(as_text=True)
            self.assertEqual(response.status_code, 200)
            self.assertIn('--- Exit 0 ---', output)
            read.assert_called_once_with(str(path), max_instruments=1000)
            resolve.assert_called_once_with(symbols='RELIANCE,HDFCBANK', instruments='', max_instruments=1000)
            launch = docker.return_value.containers.run.call_args.kwargs
            args, child_environment = launch['command'], launch['environment']
            self.assertEqual(args[args.index('--dhan-instruments') + 1], 'NSE_EQ:2885,NSE_EQ:1333')
            self.assertEqual(child_environment['DHAN_SYMBOLS'], '')
            self.assertEqual(child_environment['DHAN_SYMBOLS_FILE'], '')
            self.assertEqual(child_environment['DHAN_INSTRUMENTS'], 'NSE_EQ:2885,NSE_EQ:1333')
            self.assertNotIn(str(path), json.dumps(launch))

    def test_generic_sources_remain_unchanged_and_never_inherit_dhan_credentials(self):
        generic = {'application_name': 'legacy', 'source_type': 's3', 'ingest_date': '2026-10-20',
                   'metadata_key': 'legacy.csv', 'output_database': 'de_lake',
                   'source_bucket': 'source-bucket', 'source_key': 'source.csv'}
        args = self.module._build_pipeline_args(generic)
        self.assertEqual(args[-4:], ['--source-bucket', 'source-bucket', '--source-key', 'source.csv'])
        database = {**generic, 'source_type': 'database', 'source_database': 'public', 'source_table_name': 'prices'}
        db_args = self.module._build_pipeline_args(database)
        self.assertEqual(db_args[-4:], ['--source-database', 'public', '--source-table-name', 'prices'])
        with mock.patch.dict(os.environ, {**READY_ENV, 'DHAN_API_KEY': 'oauth-key',
                                         'DHAN_API_SECRET': 'oauth-secret', 'DHAN_PIN': '123456',
                                         'DHAN_TOTP_SECRET': 'JBSWY3DPEHPK3PXP'}):
            for source in ('s3', 'database', 'gold', ''):
                self.assertFalse(any(key.startswith('DHAN_') for key in self.module._job_environment(source)))
            dhan = self.module._job_environment('dhan')
            self.assertIn('DHAN_ACCESS_TOKEN', dhan)
            self.assertIn('DHAN_PIN', dhan)
            self.assertNotIn('DHAN_API_KEY', dhan)
            self.assertNotIn('DHAN_API_SECRET', dhan)
            self.assertNotIn('DHAN_REDIRECT_URL', dhan)

    def test_invalid_dhan_date_and_mode_fail_without_external_work(self):
        with mock.patch.dict(os.environ, READY_ENV), mock.patch('docker.DockerClient') as docker:
            for fields in ({'ingest_date': '2000-01-01'}, {'dhan_snapshot_mode': 'unsupported'}):
                response = self.module.app.test_client().post('/run', data={'source_type': 'dhan', **fields})
                self.assertEqual(response.status_code, 400)
        docker.assert_not_called()

    def test_dhan_scheduling_requires_explicit_cli_workflow(self):
        with mock.patch('ui.db.get_conn') as connection:
            response = self.module.app.test_client().post('/scheduled-jobs', json={
                'application_name': 'nse_market_snapshot', 'source_type': 'dhan',
                'cron_expression': '* * * * *',
            })
        self.assertEqual(response.status_code, 400)
        self.assertIn('CLI', response.get_json()['error'])
        connection.assert_not_called()
