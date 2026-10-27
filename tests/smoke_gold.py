"""Verify Docker-only Iceberg Gold against the isolated compatibility catalog.

Uses unique tables and retains artifacts. Never contacts Oracle or Dhan.
Run with the compatibility Compose project's pipeline image, mounting /checks.
"""

from __future__ import annotations

import argparse
from datetime import date
from decimal import Decimal
import json
import os
import subprocess
import sys
from uuid import uuid4

import trino.dbapi


def execute(cursor, sql):
    cursor.execute(sql)
    return cursor.fetchall()


def publish_existing_source(source_schema, source_table, target_table, *,
                            gold_schema="compat_gold", strategy="replace",
                            date_column=None, run_date=None, expect_success=True,
                            host=None, port=None, catalog=None):
    environment = dict(os.environ)
    environment.update({
        "SILVER_DATABASE": source_schema,
        "GOLD_DATABASE": gold_schema,
        "TRINO_HOST": host or os.environ.get("TRINO_HOST", "trino"),
        "TRINO_PORT": str(port or os.environ.get("TRINO_PORT", "8080")),
        "ICEBERG_CATALOG": catalog or os.environ.get("ICEBERG_CATALOG", "minio"),
        # Demonstrate Gold has no dependency on the legacy Oracle/Vault path.
        "VAULT_ADDR": "http://127.0.0.1:1",
        "ORACLE_USER": "", "ORACLE_PASSWORD": "", "ORACLE_DSN": "",
    })
    command = [
        sys.executable, "-m", "gold_layer.gold_loader",
        "--application-name", "compatibility_gold",
        "--silver-table", source_table, "--gold-table", target_table,
        "--load-strategy", strategy,
    ]
    if date_column:
        command += ["--date-column", date_column]
    command += ["--run-date", run_date or "2026-10-21"]
    result = subprocess.run(command, env=environment, text=True, capture_output=True, timeout=180)
    if expect_success and result.returncode:
        raise AssertionError(f"Gold publish failed: {result.stdout}\n{result.stderr}")
    if not expect_success and result.returncode == 0:
        raise AssertionError("Unsafe or empty Gold publish unexpectedly succeeded")
    return result


def run_checks(host="trino", port=8080, catalog="minio"):
    token = uuid4().hex[:12]
    source_schema = "compat_gold_source"
    gold_schema = "compat_gold"
    source_table = "quotes_" + token
    target_table = "published_" + token
    source = f"{catalog}.{source_schema}.{source_table}"
    target = f"{catalog}.{gold_schema}.{target_table}"
    metadata_bucket = os.environ.get("ICEBERG_METADATA_BUCKET", "de-iceberg-warehouse-bucket")
    gold_bucket = os.environ.get("GOLD_DATA_BUCKET", "de-data-gold")
    connection = trino.dbapi.connect(
        host=host, port=port, user="compatibility-gold", catalog=catalog,
        request_timeout=90, max_attempts=2,
    )
    cursor = connection.cursor()
    try:
        execute(cursor, f"CREATE SCHEMA IF NOT EXISTS {catalog}.{source_schema} "
                        f"WITH (location = 's3://{metadata_bucket}/{source_schema}/')")
        execute(cursor, f"CREATE TABLE {source} WITH (format='PARQUET') AS "
                        "SELECT * FROM (VALUES "
                        "(1, DECIMAL '100.25', DATE '2026-10-21', TIMESTAMP '2026-10-21 04:00:00.123456', true, CAST(NULL AS VARCHAR)), "
                        "(2, DECIMAL '200.50', DATE '2026-10-21', TIMESTAMP '2026-10-21 04:01:00.123456', false, 'two'), "
                        "(3, DECIMAL '300.75', DATE '2026-10-22', TIMESTAMP '2026-10-22 04:00:00.123456', true, 'three'), "
                        "(4, DECIMAL '400.00', DATE '2026-10-22', TIMESTAMP '2026-10-22 04:01:00.123456', false, 'four') "
                        ") AS t(id, last_price, snapshot_date, quote_at, active, note)")

        def publish(strategy="replace", table=source_table, **kwargs):
            return publish_existing_source(
                source_schema, table, target_table, gold_schema=gold_schema,
                strategy=strategy, host=host, port=port, catalog=catalog, **kwargs,
            )

        def rows():
            return execute(cursor, f"SELECT * FROM {target} ORDER BY id")

        print("[gold-check] Replacing and replaying four typed rows", flush=True)
        original = execute(cursor, f"SELECT * FROM {source} ORDER BY id")
        for _ in range(2):
            publish()
            assert rows() == original, "Full Gold replay changed rows or native types"
        assert isinstance(rows()[0][1], Decimal)
        assert rows()[0][2] == date(2026, 10, 21)
        assert rows()[0][3].microsecond == 123456
        assert rows()[0][5] is None

        print("[gold-check] Appending, then atomically replacing duplicates", flush=True)
        publish("append")
        assert execute(cursor, f"SELECT count(*) FROM {target}")[0][0] == 8
        publish()
        assert rows() == original

        print("[gold-check] Date replacement preserves untouched dates and replays safely", flush=True)
        execute(cursor, f"UPDATE {source} SET last_price = DECIMAL '111.25' WHERE id = 1")
        for _ in range(2):
            publish("delete_and_insert", date_column="snapshot_date", run_date="2026-10-21")
            current = rows()
            assert len(current) == 4 and current[0][1] == Decimal("111.25")
            assert current[2:] == original[2:], "Date replacement modified another date"
        before_rejection = rows()

        print("[gold-check] Empty and invalid publishes must preserve existing Gold", flush=True)
        publish("delete_and_insert", date_column="snapshot_date", run_date="1900-01-01", expect_success=False)
        empty_table = source_table + "_empty"
        execute(cursor, f"CREATE TABLE {catalog}.{source_schema}.{empty_table} AS SELECT * FROM {source} WHERE false")
        publish(table=empty_table, expect_success=False)
        mismatched_table = source_table + "_bad"
        execute(cursor, f"CREATE TABLE {catalog}.{source_schema}.{mismatched_table} AS SELECT id FROM {source}")
        publish("append", table=mismatched_table, expect_success=False)
        publish("delete_and_insert", date_column="snapshot_date", run_date="2026-10-21'; DROP TABLE bad;--", expect_success=False)
        assert rows() == before_rejection, "Rejected publication changed existing Gold rows"
        publish_existing_source(
            source_schema, source_table, f"{source_schema}.{source_table}",
            gold_schema=gold_schema, host=host, port=port, catalog=catalog,
            expect_success=False,
        )
        assert execute(cursor, f"SELECT count(*) FROM {source}")[0][0] == 4

        files = execute(cursor, f'SELECT file_path FROM {catalog}.{gold_schema}."{target_table}$files"')
        assert files and all(row[0].startswith(f"s3://{gold_bucket}/") for row in files), (
            "Gold files were not written to their dedicated local bucket", files,
        )
        result = {"status": "passed", "source": source, "gold_table": target,
                  "gold_rows": 4, "oracle_required": False, "artifacts_retained": True}
        print(json.dumps(result, indent=2), flush=True)
        return result
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trino-host", default=os.environ.get("TRINO_HOST", "trino"))
    parser.add_argument("--trino-port", type=int, default=int(os.environ.get("TRINO_PORT", "8080")))
    parser.add_argument("--catalog", default=os.environ.get("ICEBERG_CATALOG", "minio"))
    args = parser.parse_args()
    run_checks(args.trino_host, args.trino_port, args.catalog)
