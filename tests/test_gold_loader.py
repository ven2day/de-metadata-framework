from datetime import date
import importlib
import os
from threading import Event
from unittest import TestCase, mock

from trino.exceptions import TrinoUserError

from gold_layer.gold_loader import GoldConfig, build_parser, publish, resolve_tables


SCHEMA = [('event_id', 'varchar'), ('trading_date', 'date'), ('last_price', 'double')]


def user_error(name):
    return TrinoUserError({'errorName': name, 'message': name, 'errorType': 'USER_ERROR'})


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.description = None
        self.rows = []

    def execute(self, sql, parameters=None):
        self.connection.calls.append((sql, parameters))
        if sql.startswith('SELECT *'):
            if '"de_gold"' in sql:
                if self.connection.target_error:
                    raise self.connection.target_error
                self.description = self.connection.target_schema
            else:
                self.description = SCHEMA
        elif sql.startswith('SELECT COUNT'):
            self.rows = [[self.connection.count]]
        elif sql.startswith('CREATE OR REPLACE') and self.connection.publish_error:
            raise self.connection.publish_error

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class FakeConnection:
    def __init__(self, *, count=3, target_schema=SCHEMA, target_error=None, publish_error=None):
        self.count = count
        self.target_schema = target_schema
        self.target_error = target_error
        self.publish_error = publish_error
        self.calls = []

    def cursor(self):
        return FakeCursor(self)


class GoldLoaderTests(TestCase):
    def run_publish(self, connection, **kwargs):
        return publish(connection, silver_table='ticks', gold_table='ticks',
                       selected_date='2026-10-25', config=GoldConfig(), **kwargs)

    def test_replace_uses_one_atomic_ctas_and_separate_locations(self):
        conn = FakeConnection()
        self.assertEqual(self.run_publish(conn), 3)
        mutations = [sql for sql, _ in conn.calls if not sql.startswith('SELECT')]
        self.assertEqual(len(mutations), 2)  # namespace, then one atomic table replacement
        sql = mutations[-1]
        self.assertTrue(sql.startswith('CREATE OR REPLACE TABLE "minio"."de_gold"."ticks"'))
        self.assertIn("s3://de-data-gold/minio/de_gold/ticks/", sql)
        self.assertIn("s3://de-iceberg-warehouse-bucket/iceberg/minio/de_gold/ticks/", sql)
        self.assertNotIn('DROP', sql)
        self.assertNotIn('DELETE', sql)

    def test_date_replace_preserves_other_dates_with_bound_union(self):
        conn = FakeConnection()
        self.run_publish(conn, strategy='delete_and_insert', date_column='trading_date')
        sql, parameters = conn.calls[-1]
        self.assertIn('UNION ALL', sql)
        self.assertIn('IS NULL OR CAST("trading_date" AS DATE) <> ?', sql)
        self.assertEqual(parameters, [date(2026, 10, 25), date(2026, 10, 25)])

    def test_empty_replacements_and_schema_mismatch_do_not_mutate_table(self):
        for strategy in ('replace', 'delete_and_insert'):
            conn = FakeConnection(count=0)
            with self.assertRaisesRegex(ValueError, 'empty Gold replacement'):
                self.run_publish(conn, strategy=strategy, date_column='trading_date')
            self.assertFalse(any(sql.startswith(('CREATE', 'INSERT', 'DELETE', 'DROP')) for sql, _ in conn.calls))
        for strategy in ('append', 'delete_and_insert'):
            conn = FakeConnection(target_schema=list(reversed(SCHEMA)))
            with self.assertRaisesRegex(ValueError, 'order, and types'):
                self.run_publish(conn, strategy=strategy, date_column='trading_date')
            self.assertFalse(any(sql.startswith(('CREATE OR REPLACE', 'INSERT')) for sql, _ in conn.calls))

    def test_only_missing_table_error_allows_first_publish(self):
        conn = FakeConnection(target_error=user_error('TABLE_NOT_FOUND'))
        self.run_publish(conn, strategy='append')
        self.assertTrue(conn.calls[-1][0].startswith('CREATE OR REPLACE'))
        for error in (user_error('PERMISSION_DENIED'), user_error('SCHEMA_NOT_FOUND'), OSError('network')):
            conn = FakeConnection(target_error=error)
            with self.assertRaises(type(error)):
                self.run_publish(conn)
            self.assertFalse(any(sql.startswith('CREATE OR REPLACE') for sql, _ in conn.calls))

    def test_append_uses_atomic_insert_and_empty_append_is_noop(self):
        conn = FakeConnection()
        self.run_publish(conn, strategy='append')
        self.assertTrue(conn.calls[-1][0].startswith('INSERT INTO'))
        empty = FakeConnection(count=0)
        self.assertEqual(self.run_publish(empty, strategy='append'), 0)
        self.assertFalse(any(sql.startswith(('CREATE', 'INSERT')) for sql, _ in empty.calls))

    def test_failed_atomic_publish_never_runs_cleanup_delete_or_drop(self):
        conn = FakeConnection(publish_error=RuntimeError('commit rejected'))
        with self.assertRaisesRegex(RuntimeError, 'commit rejected'):
            self.run_publish(conn)
        self.assertFalse(any(sql.startswith(('DELETE', 'DROP')) for sql, _ in conn.calls))

    def test_names_and_namespace_aliases_are_guarded(self):
        for target in ('de_silver.ticks', 'de_lake.ticks', 'de_bronze.ticks',
                       'de_gold.ticks;drop', 'minio.de_gold.ticks'):
            with self.subTest(target=target):
                with self.assertRaises(ValueError):
                    resolve_tables('ticks', target, GoldConfig())
        with self.assertRaisesRegex(ValueError, 'must differ'):
            resolve_tables('ticks', 'ticks', GoldConfig(gold_schema='DE_SILVER'))
        with self.assertRaisesRegex(ValueError, 'Silver namespace'):
            resolve_tables('de_gold.ticks', 'ticks', GoldConfig())
        source, target = resolve_tables('minio.de_silver.ticks', 'de_gold.ticks', GoldConfig())
        self.assertEqual(source.schema, 'de_silver')
        self.assertEqual(target.schema, 'de_gold')

    def test_date_replacement_rejects_missing_or_wrong_type_column(self):
        for column in (None, 'unknown', 'event_id', 'trading_date OR 1=1'):
            with self.subTest(column=column):
                conn = FakeConnection()
                with self.assertRaises(ValueError):
                    self.run_publish(conn, strategy='delete_and_insert', date_column=column)
                self.assertFalse(any(sql.startswith('CREATE') for sql, _ in conn.calls))

    def test_default_is_replay_safe_replace_and_truncate_alias_is_atomic(self):
        args = build_parser().parse_args(['--application-name', 'ticks', '--silver-table', 'ticks',
                                        '--gold-table', 'ticks'])
        self.assertEqual(args.load_strategy, 'replace')
        self.assertIsNone(args.run_date)
        self.assertEqual(publish(FakeConnection(), silver_table='ticks', gold_table='ticks',
                                 config=GoldConfig()), 3)
        with self.assertRaisesRegex(ValueError, 'run_date is required'):
            publish(FakeConnection(), silver_table='ticks', gold_table='ticks',
                    strategy='delete_and_insert', date_column='trading_date', config=GoldConfig())
        with self.assertRaisesRegex(ValueError, 'YYYY-MM-DD'):
            publish(FakeConnection(), silver_table='ticks', gold_table='ticks',
                    selected_date='2026-99-99', config=GoldConfig())
        conn = FakeConnection()
        self.run_publish(conn, strategy='truncate')
        self.assertTrue(conn.calls[-1][0].startswith('CREATE OR REPLACE'))


class GoldUiTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.environment = mock.patch.dict(os.environ, {
            'FLASK_SECRET_KEY': 'test-only-gold-flask-key', 'GOLD_DATABASE': 'de_gold',
            'ICEBERG_CATALOG': 'minio', 'SILVER_DATABASE': 'de_silver',
            'VAULT_TOKEN': 'must-not-reach-gold', 'TRINO_HOST': 'trino', 'TRINO_PORT': '8080',
        })
        cls.environment.start()
        cls.module = importlib.import_module('ui.app')
        cls.module.app.config.update(TESTING=True, LOGIN_DISABLED=True, WTF_CSRF_ENABLED=False)

    @classmethod
    def tearDownClass(cls):
        cls.environment.stop()

    def test_retired_oracle_endpoint_does_not_launch_any_job(self):
        with mock.patch('docker.DockerClient') as docker:
            response = self.module.app.test_client().post('/run-oracle')
        self.assertEqual(response.status_code, 410)
        self.assertIn('/run-gold', response.get_json()['error'])
        docker.assert_not_called()

    def test_runtime_silver_and_bronze_tables_do_not_require_dbt_registration(self):
        for schema in ('de_silver', 'de_bronze'):
            fqn = f'minio.{schema}.runtime_created_ticks'
            self.assertEqual(self.module._fqn_to_dbt_ref(fqn),
                             f'"minio"."{schema}"."runtime_created_ticks"')
            payload = '{"target_table":"runtime_output","sources":[{"fqn":"' + fqn + '","alias":"src"}]}'
            with mock.patch('builtins.open', mock.mock_open()) as handle, \
                 mock.patch('os.makedirs'), \
                 mock.patch.object(self.module, '_ensure_silver_sources'):
                self.module._write_silver_dbt_model(payload, 'runtime_output')
            generated = handle().write.call_args.args[0]
            self.assertIn(f'FROM "minio"."{schema}"."runtime_created_ticks" src', generated)
            self.assertNotIn("ref(", generated)
            self.assertNotIn("source(", generated)

    def test_runtime_source_identifiers_reject_sql_fragments_and_incomplete_names(self):
        for fqn in ('ticks', 'de_bronze.ticks', 'minio.de_bronze.ticks;DROP TABLE x',
                    'minio.de_bronze."ticks"', 'minio..ticks', "{{ ref('ticks') }}", None):
            with self.subTest(fqn=fqn):
                with self.assertRaises(ValueError):
                    self.module._fqn_to_dbt_ref(fqn)

    def test_gold_job_has_no_vault_volume_or_secret_dependency(self):
        container = mock.Mock(id='gold-test')
        container.logs.return_value = [b'[gold] Publish completed\n']
        container.wait.return_value = {'StatusCode': 0}
        with mock.patch('docker.DockerClient') as docker:
            docker.return_value.containers.run.return_value = container
            response = self.module.app.test_client().post('/run-gold', data={
                'application_name': 'ticks', 'silver_table': 'ticks',
                'gold_table': 'ticks', 'run_date': '2026-10-25',
            })
            output = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn('--- Gold Exit 0 ---', output)
        kwargs = docker.return_value.containers.run.call_args.kwargs
        self.assertEqual(kwargs['command'][0], 'gold-load')
        self.assertNotIn('VAULT_TOKEN', kwargs['environment'])
        self.assertNotIn('volumes', kwargs)
        container.remove.assert_called_once()

    def test_transform_chain_validates_gold_before_launching_silver(self):
        response = self.module.app.test_client().post('/run-transform-load', json={
            'silver_payload': '{"target_table": "ticks"}', 'app_name': 'ticks',
            'silver_table': 'ticks', 'gold_table': 'de_silver.ticks', 'run_date': '2026-10-25',
        })
        self.assertEqual(response.status_code, 400)

    def test_transform_chain_finishes_gold_and_cleanup_after_browser_disconnect(self):
        release_silver = Event()
        cleaned = Event()
        process = mock.Mock(returncode=0)
        process.stdout = iter(['dbt complete\n'])
        process.wait.side_effect = lambda: release_silver.wait(3)
        container = mock.Mock(id='gold-after-disconnect')
        container.logs.return_value = [b'[gold] Publish completed\n']
        container.wait.return_value = {'StatusCode': 0}
        container.remove.side_effect = lambda **kwargs: cleaned.set()
        with mock.patch('boto3.client'), \
             mock.patch.object(self.module, '_write_silver_dbt_model', return_value='ticks'), \
             mock.patch('subprocess.Popen', return_value=process), \
             mock.patch('docker.DockerClient') as docker:
            docker.return_value.containers.run.return_value = container
            response = self.module.app.test_client().post('/run-transform-load', json={
                'silver_payload': '{"target_table": "ticks"}', 'app_name': 'ticks',
                'silver_table': 'ticks', 'gold_table': 'ticks', 'run_date': '2026-10-25',
            }, buffered=False)
            self.assertEqual(response.status_code, 200)
            response.close()
            release_silver.set()
            self.assertTrue(cleaned.wait(3), 'Gold should finish after the browser log stream closes')
            self.assertEqual(docker.return_value.containers.run.call_args.kwargs['command'][0], 'gold-load')

    def test_legacy_oracle_schedule_payload_is_rejected_without_database_write(self):
        with mock.patch('ui.db.get_conn') as get_conn:
            response = self.module.app.test_client().post('/scheduled-jobs', json={
                'application_name': 'ticks', 'layer_type': 'silver',
                'silver_sql': 'SELECT 1', 'oracle_table': 'legacy.ticks',
                'cron_expression': '0 18 * * 1-5',
            })
        self.assertEqual(response.status_code, 400)
        get_conn.assert_not_called()
