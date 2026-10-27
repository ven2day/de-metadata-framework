"""Offline contract for a private local symbol watchlist."""

from pathlib import Path
import tempfile
from unittest import TestCase, mock

from ingestion.dhan import instruments as resolver


class DhanSymbolsFileTests(TestCase):
    def test_csv_bom_crlf_prefixes_blank_lines_and_stable_deduplication(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'symbols.csv'
            path.write_bytes(b'\xef\xbb\xbf Symbol \r\n reliance \r\n\r\nNSE_EQ:RELIANCE\r\nM&M\r\n"IDX_I:Nifty 50"\r\n')
            self.assertEqual(resolver.read_symbols_file(path), 'RELIANCE,M&M,IDX_I:NIFTY 50')

    def test_malformed_csv_and_wrong_column_shapes_fail_without_echoing_input(self):
        invalid = (
            b'', b'symbol\n', b'ticker\nRELIANCE\n', b'symbol,secret\nRELIANCE,PRIVATE\n',
            b'symbol\nRELIANCE,TCS\n', b'symbol\n"RELIANCE,TCS"\n', b'symbol\n"RELIANCE\n',
            b'symbol\n""\n', b'symbol\nBSE_EQ:PRIVATE\n', b'symbol\n\xff\n',
            b'symbol\nPRIVATE<script>\n', b'symbol\n"REL\nIANCE"\n',
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'private-watchlist.csv'
            for body in invalid:
                with self.subTest(body=body):
                    path.write_bytes(body)
                    with self.assertRaises(resolver.DhanInstrumentError) as caught:
                        resolver.read_symbols_file(path)
                    self.assertEqual(caught.exception.code, 'symbols_file_invalid')
                    self.assertNotIn('PRIVATE', str(caught.exception))
                    self.assertNotIn(str(path), str(caught.exception))

    def test_missing_or_directory_file_is_safe_unavailable_error(self):
        with tempfile.TemporaryDirectory() as directory:
            for path in (Path(directory) / 'private-missing.csv', Path(directory)):
                with self.assertRaises(resolver.DhanInstrumentError) as caught:
                    resolver.read_symbols_file(path)
                self.assertEqual(caught.exception.code, 'symbols_file_unavailable')
                self.assertNotIn(str(path), str(caught.exception))

    def test_file_and_symbol_count_limits(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'symbols.csv'
            path.write_bytes(b'symbol\nRELIANCE\nTCS\n')
            with self.assertRaises(resolver.DhanInstrumentError):
                resolver.read_symbols_file(path, max_instruments=1)
            with mock.patch.object(resolver, 'MAX_SYMBOLS_FILE_BYTES', 10):
                with self.assertRaises(resolver.DhanInstrumentError) as caught:
                    resolver.read_symbols_file(path)
            self.assertEqual(caught.exception.code, 'symbols_file_invalid')

    def test_three_way_selection_conflicts_fail_before_read_or_network(self):
        for selection in (
            {'symbols': 'RELIANCE', 'symbols_file': '/private/symbols.csv'},
            {'instruments': 'NSE_EQ:2885', 'symbols_file': '/private/symbols.csv'},
            {'symbols': 'RELIANCE', 'instruments': 'NSE_EQ:2885'},
            {'symbols': 'RELIANCE', 'instruments': 'NSE_EQ:2885', 'symbols_file': '/private/symbols.csv'},
        ):
            with self.subTest(selection=selection), \
                 mock.patch.object(resolver, 'read_symbols_file') as read, \
                 mock.patch.object(resolver.requests, 'Session') as network:
                with self.assertRaises(resolver.DhanInstrumentError) as caught:
                    resolver.configured_selection(**selection)
                self.assertEqual(caught.exception.code, 'selection_conflict')
                read.assert_not_called()
                network.assert_not_called()

    def test_file_is_reread_on_each_inspection_without_mapping_or_side_effects(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'symbols.csv'
            path.write_text('symbol\nRELIANCE\n', encoding='utf-8')
            environment = {'DHAN_SYMBOLS_FILE': str(path)}
            with mock.patch.object(resolver, '_master') as master, \
                 mock.patch.object(resolver.requests, 'Session') as network:
                first = resolver.inspect_selection(environment=environment)
                path.write_text('symbol\nRELIANCE\nTCS\n', encoding='utf-8')
                second = resolver.inspect_selection(environment=environment)
            self.assertEqual((first['mode'], first['count'], second['count']), ('symbols_file', 1, 2))
            self.assertTrue(second['valid'])
            self.assertTrue(second['resolution_required'])
            self.assertFalse(second['configured_validated'])
            self.assertNotIn(str(path), str(second))
            master.assert_not_called()
            network.assert_not_called()
            self.assertEqual(list(Path(directory).iterdir()), [path])

    def test_invalid_file_inspection_fails_closed_and_hides_contents(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'private-watchlist.csv'
            path.write_text('private-header\nPRIVATE\n', encoding='utf-8')
            result = resolver.inspect_selection(symbols_file=str(path))
            self.assertEqual((result['mode'], result['valid'], result['code']),
                             ('symbols_file', False, 'symbols_file_invalid'))
            self.assertNotIn('PRIVATE', str(result))
            self.assertNotIn(str(path), str(result))

    def test_environment_file_expands_before_normal_master_resolution(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'symbols.csv'
            path.write_text('symbol\nRELIANCE\nTCS\n', encoding='utf-8')
            environment = {'DHAN_SYMBOLS_FILE': str(path), 'DHAN_INSTRUMENT_CACHE_FILE': '/cache/master.csv'}
            with mock.patch.object(resolver, 'resolve_selection', return_value=['resolved']) as resolve:
                self.assertEqual(resolver.resolve_from_env(environment, max_instruments=1000), ['resolved'])
            resolve.assert_called_once_with(symbols='RELIANCE,TCS', instruments='',
                                            cache_path='/cache/master.csv', max_instruments=1000)

    def test_inline_and_legacy_selections_remain_compatible(self):
        self.assertEqual(resolver.configured_selection(symbols=' RELIANCE '), ('RELIANCE', ''))
        self.assertEqual(resolver.configured_selection(instruments=' NSE_EQ:2885 '), ('', 'NSE_EQ:2885'))
        self.assertEqual(resolver.configured_selection(), ('', ''))
