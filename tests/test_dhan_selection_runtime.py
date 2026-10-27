from argparse import Namespace
from contextlib import redirect_stderr
from datetime import datetime
import io
from pathlib import Path
import tempfile
from unittest import TestCase, mock
from zoneinfo import ZoneInfo

from ingestion.dhan.client import DhanConfigError, DhanInstrument
from ingestion.dhan import live_collector
from ingestion.main import pipeline
from ingestion.pyfiles import args_parser
from ingestion.source import dhan_reader


def command(*selection):
    return ['--application-name', 'nse_snapshot', '--source-type', 'dhan',
            '--ingest-date', datetime.now(ZoneInfo('Asia/Kolkata')).date().isoformat(),
            '--metadata-key', 'examples/dhan_market_snapshot.csv', *selection]


def write_watchlist(path, symbols=('HDFCBANK', 'RELIANCE')):
    path.write_text('symbol\n' + '\n'.join(symbols) + '\n', encoding='utf-8')


class DhanSelectionArgumentTests(TestCase):
    def parse(self, flags=(), *, symbols='', instruments='', symbols_file=''):
        with mock.patch.multiple(args_parser, DHAN_SYMBOLS=symbols,
                                 DHAN_INSTRUMENTS=instruments, DHAN_SYMBOLS_FILE=symbols_file):
            return args_parser.parse_args(command(*flags))

    def test_env_file_is_used_only_when_no_explicit_selection_flag_exists(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'symbols.csv'
            write_watchlist(path)
            args = self.parse(symbols_file=str(path))
        self.assertEqual(args.dhan_symbols_file, str(path))
        self.assertIsNone(args.dhan_symbols)
        self.assertIsNone(args.dhan_instruments)

    def test_explicit_file_overrides_all_environment_selection_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'symbols.csv'
            write_watchlist(path)
            args = self.parse(('--dhan-symbols-file', str(path)), symbols='ENV_SYMBOL',
                              instruments='NSE_EQ:999', symbols_file='/not-readable/env.csv')
        self.assertEqual(args.dhan_symbols_file, str(path))
        self.assertIsNone(args.dhan_symbols)
        self.assertIsNone(args.dhan_instruments)

    def test_explicit_symbols_or_ids_override_environment_file_without_reading_it(self):
        for flag, value in (('--dhan-symbols', 'HDFCBANK'), ('--dhan-instruments', 'NSE_EQ:1333')):
            with self.subTest(flag=flag), mock.patch.object(args_parser, 'configured_selection') as configure:
                args = self.parse((flag, value), symbols='ENV_SYMBOL', instruments='NSE_EQ:999',
                                  symbols_file='/not-readable/private-env.csv')
            self.assertIsNone(args.dhan_symbols_file)
            self.assertEqual(getattr(args, flag[2:].replace('-', '_')), value)
            configure.assert_not_called()

    def test_three_way_conflicts_and_empty_explicit_flags_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'symbols.csv'
            write_watchlist(path)
            cases = [
                (('--dhan-symbols-file', str(path), '--dhan-symbols', 'HDFCBANK'), {}),
                (('--dhan-symbols-file', str(path), '--dhan-instruments', 'NSE_EQ:1333'), {}),
                ((), {'symbols': 'HDFCBANK', 'symbols_file': str(path)}),
                ((), {'instruments': 'NSE_EQ:1333', 'symbols_file': str(path)}),
                (('--dhan-symbols-file', ''), {'symbols': 'HDFCBANK'}),
            ]
            for flags, environment in cases:
                with self.subTest(flags=flags), redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit):
                        self.parse(flags, **environment)

    def test_bad_file_errors_do_not_expose_path_or_contents(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'private-token-name.csv'
            for content in (None, 'wrong_header\nSENSITIVE_CONTENT\n'):
                if content is not None:
                    path.write_text(content, encoding='utf-8')
                output = io.StringIO()
                with redirect_stderr(output), self.assertRaises(SystemExit):
                    self.parse(('--dhan-symbols-file', str(path)))
                self.assertNotIn(str(path), output.getvalue())
                self.assertNotIn('SENSITIVE_CONTENT', output.getvalue())

    def test_generic_source_does_not_read_dhan_watchlist(self):
        with mock.patch.multiple(args_parser, DHAN_SYMBOLS='ENV_SYMBOL', DHAN_INSTRUMENTS='NSE_EQ:999',
                                 DHAN_SYMBOLS_FILE='/private/nonexistent.csv'), \
             mock.patch.object(args_parser, 'configured_selection') as configure:
            args = args_parser.parse_args(['--application-name', 'legacy', '--source-type', 's3',
                                           '--source-key', 'data.csv', '--ingest-date', '2026-10-26',
                                           '--metadata-key', 'metadata.csv'])
        self.assertEqual(args.source_type, 's3')
        configure.assert_not_called()


class DhanSnapshotSelectionTests(TestCase):
    def read(self, **selection):
        spark = mock.Mock()
        api = mock.Mock()
        api.market_snapshot.return_value = {'status': 'success', 'data': {'NSE_EQ': {'1333': {'last_price': 100.0}}}}
        with mock.patch.object(dhan_reader, 'resolve_selection', return_value=[DhanInstrument('NSE_EQ', '1333')]) as resolve:
            frame = dhan_reader.read_dhan_snapshot(spark, client=api, **selection)
        self.assertIs(frame, spark.createDataFrame.return_value)
        return resolve

    def test_file_symbols_are_expanded_before_rest_resolution(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'symbols.csv'
            write_watchlist(path, ('HDFCBANK',))
            resolve = self.read(symbols_file=str(path))
        resolve.assert_called_once_with(symbols='HDFCBANK', instruments='', max_instruments=1000)

    def test_no_explicit_reader_selection_uses_env_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'symbols.csv'
            write_watchlist(path, ('HDFCBANK',))
            with mock.patch.multiple(dhan_reader, DHAN_SYMBOLS='', DHAN_INSTRUMENTS='', DHAN_SYMBOLS_FILE=str(path)):
                resolve = self.read()
        resolve.assert_called_once_with(symbols='HDFCBANK', instruments='', max_instruments=1000)

    def test_explicit_reader_ids_do_not_inherit_env_file_or_symbols(self):
        with mock.patch.multiple(dhan_reader, DHAN_SYMBOLS='ENV_SYMBOL', DHAN_INSTRUMENTS='NSE_EQ:999',
                                 DHAN_SYMBOLS_FILE='/private/not-readable.csv'), \
             mock.patch.object(dhan_reader, 'configured_selection') as configure:
            resolve = self.read(instruments='NSE_EQ:1333')
        resolve.assert_called_once_with(symbols='', instruments='NSE_EQ:1333', max_instruments=1000)
        configure.assert_not_called()

    def test_file_conflict_and_rest_limit_fail_before_any_api_call(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'symbols.csv'
            write_watchlist(path, tuple(f'SYMBOL{number}' for number in range(1001)))
            api = mock.Mock()
            for selection in ({'symbols_file': str(path)}, {'symbols_file': str(path), 'instruments': 'NSE_EQ:1333'}):
                with self.subTest(selection=selection), self.assertRaises(DhanConfigError):
                    dhan_reader.read_dhan_snapshot(mock.Mock(), client=api, **selection)
            api.validate_market_data_access.assert_not_called()
            api.market_snapshot.assert_not_called()


class DhanLiveSelectionTests(TestCase):
    def test_live_file_uses_5000_limit_and_sdk_receives_resolved_ids(self):
        from dhanhq import MarketFeed
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'symbols.csv'
            write_watchlist(path, ('HDFCBANK',))
            with mock.patch.object(live_collector, 'resolve_selection', return_value=[DhanInstrument('NSE_EQ', '1333')]) as resolve:
                selected = live_collector._sdk_instruments('', 'quote', symbols_file=str(path))
        resolve.assert_called_once_with(symbols='HDFCBANK', instruments='', max_instruments=5000)
        self.assertEqual(selected, [(MarketFeed.NSE, '1333', MarketFeed.Quote)])

    def test_live_file_conflict_fails_before_resolution(self):
        with mock.patch.object(live_collector, 'resolve_selection') as resolve:
            with self.assertRaises(DhanConfigError):
                live_collector._sdk_instruments('NSE_EQ:1333', 'quote', symbols_file='/private/not-readable.csv')
        resolve.assert_not_called()

    def test_no_file_preserves_legacy_live_resolver_signature(self):
        with mock.patch.object(live_collector, 'configured_selection') as configure, \
             mock.patch.object(live_collector, 'resolve_selection', return_value=[DhanInstrument('NSE_EQ', '1333')]) as resolve:
            live_collector._sdk_instruments('NSE_EQ:1333', 'quote')
        configure.assert_not_called()
        resolve.assert_called_once_with(symbols='', instruments='NSE_EQ:1333', max_instruments=5000)


class DhanPipelineSelectionTests(TestCase):
    def test_pipeline_forwards_explicit_file_without_adding_other_env_selection(self):
        spark, client, frame = mock.Mock(), mock.Mock(), mock.Mock()
        args = Namespace(application_name='nse_snapshot', ingest_date='2026-10-26')
        with mock.patch.object(pipeline, 'run_all_checks', return_value=client), \
             mock.patch.object(pipeline, 'get_spark_session', return_value=spark), \
             mock.patch.object(pipeline, 'read_metadata'), \
             mock.patch.object(pipeline, 'read_dhan_snapshot', return_value=frame) as read, \
             mock.patch.object(pipeline, 'validate_schema'), \
             mock.patch.object(pipeline, 'cast_columns', return_value=frame), \
             mock.patch.object(pipeline, 'apply_pii', return_value=frame), \
             mock.patch.object(pipeline, 'write_to_minio'):
            pipeline.run_dhan_pipeline(args, instruments=None, symbols_file='/run/watchlists/symbols.csv')
        read.assert_called_once_with(spark, instruments=None, mode='quote', symbols=None,
                                     symbols_file='/run/watchlists/symbols.csv', client=client)


class DhanSymbolArgumentTests(TestCase):
    """Keep the original inline-symbol/ID regressions beside CSV coverage."""

    def parse(self, flags=(), *, symbols='', instruments=''):
        with mock.patch.multiple(args_parser, DHAN_SYMBOLS=symbols,
                                 DHAN_INSTRUMENTS=instruments, DHAN_SYMBOLS_FILE=''):
            return args_parser.parse_args(command(*flags))

    def test_env_symbols_are_selected_without_ids(self):
        args = self.parse(symbols='RELIANCE,TCS')
        self.assertEqual(args.dhan_symbols, 'RELIANCE,TCS')
        self.assertIsNone(args.dhan_instruments)
        self.assertIsNone(args.dhan_symbols_file)

    def test_explicit_symbols_override_environment_ids(self):
        args = self.parse(('--dhan-symbols', 'HDFCBANK'), instruments='NSE_EQ:999')
        self.assertEqual(args.dhan_symbols, 'HDFCBANK')
        self.assertIsNone(args.dhan_instruments)

    def test_explicit_ids_override_environment_symbols(self):
        args = self.parse(('--dhan-instruments', 'NSE_EQ:1333'), symbols='RELIANCE')
        self.assertEqual(args.dhan_instruments, 'NSE_EQ:1333')
        self.assertIsNone(args.dhan_symbols)

    def test_conflicting_environment_symbols_and_ids_are_rejected_without_network(self):
        with mock.patch('requests.sessions.Session.request') as network, redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                self.parse(symbols='RELIANCE', instruments='NSE_EQ:1333')
        network.assert_not_called()

    def test_explicit_symbols_and_ids_are_rejected(self):
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            self.parse(('--dhan-symbols', 'HDFCBANK', '--dhan-instruments', 'NSE_EQ:1333'))


class DhanSymbolRuntimeTests(TestCase):
    def test_reader_explicit_symbols_resolve_before_market_request(self):
        events = []
        spark, api = mock.Mock(), mock.Mock()
        api.validate_market_data_access.side_effect = lambda: events.append('validate')
        payload = {'status': 'success', 'data': {'NSE_EQ': {'1333': {'last_price': 100.0}}}}

        def resolve(**kwargs):
            events.append('resolve')
            return [DhanInstrument('NSE_EQ', '1333')]

        def market(*args):
            events.append('market')
            return payload

        api.market_snapshot.side_effect = market
        with mock.patch.multiple(dhan_reader, DHAN_SYMBOLS='ENV_SYMBOL', DHAN_INSTRUMENTS='NSE_EQ:999',
                                 DHAN_SYMBOLS_FILE=''), \
             mock.patch.object(dhan_reader, 'resolve_selection', side_effect=resolve) as resolver:
            dhan_reader.read_dhan_snapshot(spark, symbols='HDFCBANK', client=api)
        resolver.assert_called_once_with(symbols='HDFCBANK', instruments='', max_instruments=1000)
        self.assertEqual(events, ['resolve', 'validate', 'market'])
        api.market_snapshot.assert_called_once_with([DhanInstrument('NSE_EQ', '1333')], 'quote')

    def test_reader_explicit_ids_do_not_inherit_environment_symbols(self):
        spark, api = mock.Mock(), mock.Mock()
        api.market_snapshot.return_value = {'status': 'success', 'data': {'NSE_EQ': {'1333': {'last_price': 100.0}}}}
        with mock.patch.multiple(dhan_reader, DHAN_SYMBOLS='RELIANCE', DHAN_INSTRUMENTS='', DHAN_SYMBOLS_FILE=''), \
             mock.patch.object(dhan_reader, 'resolve_selection', return_value=[DhanInstrument('NSE_EQ', '1333')]) as resolve:
            dhan_reader.read_dhan_snapshot(spark, instruments='NSE_EQ:1333', client=api)
        resolve.assert_called_once_with(symbols='', instruments='NSE_EQ:1333', max_instruments=1000)

    def test_live_sdk_explicit_symbols_are_resolved_to_numeric_ids(self):
        from dhanhq import MarketFeed
        with mock.patch.object(live_collector, 'resolve_selection', return_value=[DhanInstrument('NSE_EQ', '1333')]) as resolve:
            selected = live_collector._sdk_instruments('', 'quote', symbols='HDFCBANK')
        resolve.assert_called_once_with(symbols='HDFCBANK', instruments='', max_instruments=5000)
        self.assertEqual(selected, [(MarketFeed.NSE, '1333', MarketFeed.Quote)])
