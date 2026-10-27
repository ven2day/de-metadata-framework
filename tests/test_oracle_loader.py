from datetime import date, datetime, time, timezone
from decimal import Decimal
from unittest import TestCase, mock
from uuid import UUID

from oracle_layer import oracle_loader as loader


class OracleLoaderTests(TestCase):
    def test_delete_and_insert_rolls_back_delete_and_earlier_batches_on_error(self):
        conn = mock.Mock()
        cursor = conn.cursor.return_value
        cursor.executemany.side_effect = [None, RuntimeError('second batch failed')]
        with mock.patch.object(loader, 'BATCH_SIZE', 1):
            with self.assertRaisesRegex(RuntimeError, 'second batch'):
                loader._load(conn, 'gold.ticks', 'delete_and_insert', 'trading_date',
                             '2026-10-25', ['event_id'], [['first'], ['second']])
        conn.commit.assert_not_called()
        conn.rollback.assert_called_once()
        self.assertFalse(conn.autocommit)
        self.assertEqual(cursor.execute.call_args.args[1],
                         [date(2026, 10, 25), date(2026, 10, 26)])
        cursor.close.assert_called_once()

    def test_append_commits_once_after_all_batches(self):
        conn = mock.Mock()
        with mock.patch.object(loader, 'BATCH_SIZE', 1):
            loader._load(conn, 'gold.ticks', 'append', None, None,
                         ['event_id'], [['first'], ['second']])
        self.assertEqual(conn.cursor.return_value.executemany.call_count, 2)
        conn.commit.assert_called_once()
        conn.rollback.assert_not_called()

    def test_append_failure_does_not_commit_partial_rows(self):
        conn = mock.Mock()
        conn.cursor.return_value.executemany.side_effect = RuntimeError('bad bind')
        with self.assertRaisesRegex(RuntimeError, 'bad bind'):
            loader._load(conn, 'gold.ticks', 'append', None, None, ['event_id'], [['x']])
        conn.commit.assert_not_called()
        conn.rollback.assert_called_once()

    def test_full_replacement_and_empty_partition_use_transactional_delete(self):
        for strategy, date_col in [('truncate', None), ('delete_and_insert', 'trading_date')]:
            with self.subTest(strategy=strategy):
                conn = mock.Mock()
                loader._load(conn, 'gold.ticks', strategy, date_col,
                             '2026-10-25', ['event_id'], [])
                self.assertTrue(conn.cursor.return_value.execute.call_args.args[0].startswith('DELETE FROM'))
                conn.commit.assert_called_once()
                conn.cursor.return_value.executemany.assert_not_called()

    def test_type_mapping_preserves_timezone_decimal_and_long_text(self):
        expected = {
            'timestamp(6) with time zone': 'TIMESTAMP(6) WITH TIME ZONE',
            'timestamp(9)': 'TIMESTAMP(9)',
            'decimal(38,18)': 'NUMBER(38,18)',
            'varchar': 'CLOB',
            'varchar(9000)': 'CLOB',
            'varchar(36)': 'VARCHAR2(36 CHAR)',
            'varbinary': 'BLOB',
            'boolean': 'NUMBER(1)',
        }
        for source, target in expected.items():
            with self.subTest(source=source):
                self.assertEqual(loader._trino_type_to_oracle(source), target)
        with self.assertRaisesRegex(ValueError, 'cast complex'):
            loader._trino_type_to_oracle('array(varchar)')

    def test_timezone_and_lob_input_sizes_are_explicit(self):
        conn = mock.Mock()
        value = datetime(2026, 10, 25, 9, tzinfo=timezone.utc)
        loader._load(conn, 'gold.ticks', 'append', None, None,
                     ['received_at', 'depth_json'], [[value, '[]']],
                     ['timestamp(6) with time zone', 'varchar'])
        conn.cursor.return_value.setinputsizes.assert_called_once_with(
            loader.oracledb.DB_TYPE_TIMESTAMP_TZ, loader.oracledb.DB_TYPE_CLOB)
        self.assertIs(conn.cursor.return_value.executemany.call_args.args[1][0][0], value)

    def test_identifiers_and_dates_reject_sql_fragments_before_mutation(self):
        conn = mock.Mock()
        for table, date_value in [('gold.ticks; DROP TABLE x', '2026-10-25'),
                                  ('gold.ticks', "2026-10-25' OR 1=1")]:
            with self.subTest(table=table, date=date_value):
                with self.assertRaises(ValueError):
                    loader._load(conn, table, 'delete_and_insert', 'trading_date',
                                 date_value, ['event_id'], [['x']])
        conn.cursor.assert_not_called()

    def test_trino_read_uses_configured_catalog_bound_date_and_closes_connection(self):
        conn = mock.Mock()
        cursor = conn.cursor.return_value
        cursor.description = [('event_id', 'varchar')]
        cursor.fetchall.return_value = [['x']]
        with mock.patch.object(loader.trino.dbapi, 'connect', return_value=conn) as connect:
            columns, rows, types = loader._read_silver('nse_silver', 'ticks', 'trading_date',
                                                      '2026-10-25', 'delete_and_insert', 'market')
        self.assertEqual(connect.call_args.kwargs['catalog'], 'market')
        self.assertIn('"market"."nse_silver"."ticks"', cursor.execute.call_args.args[0])
        self.assertEqual(cursor.execute.call_args.args[1], [date(2026, 10, 25)])
        self.assertEqual((columns, rows, types), (['event_id'], [['x']], ['varchar']))
        conn.close.assert_called_once()

    def test_scalar_conversion_keeps_decimal_and_handles_boolean_uuid_time(self):
        cursor = mock.Mock()
        decimal = Decimal('1234.567890123456789')
        value = UUID('bb6b33d7-b5eb-4d47-a6b7-7c6c5f59f823')
        loader._insert_batch(cursor, 'gold.ticks', ['price', 'flag', 'id', 'time'],
                             [[decimal, True, value, time(9, 15)]])
        self.assertEqual(cursor.executemany.call_args.args[1],
                         [[decimal, 1, str(value), '09:15:00']])
        self.assertIn('"PRICE"', cursor.executemany.call_args.args[0])
