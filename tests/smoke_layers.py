"""Exercise real S3, Spark, Iceberg, HMS, Trino, and DBT with synthetic Dhan data.

Run inside the pipeline image after MinIO and the catalog services are healthy:

    spark-submit /checks/smoke_layers.py --metadata-file /examples/metadata/dhan_market_snapshot.csv

Mount this repository's tests directory at /checks and examples at /examples.
The test uses unique table/object names and retains its artifacts for inspection.
It does not contact Dhan, require market credentials, or contact Oracle.
"""

from __future__ import annotations

import argparse
from argparse import Namespace
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from uuid import uuid4

# spark-submit can put only the mounted script directory on sys.path.
sys.path.insert(0, "/app")

import boto3
import trino.dbapi
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType, StringType, TimestampType

from bronze_layer.bronze_processor import extract_bronze_keys, run_delta, run_full
from ingestion.dhan.normalization import json_ready, normalize_market_snapshot
from ingestion.env.DE_Ingestion_properties import (
    BRONZE_DATA_BUCKET,
    ICEBERG_CATALOG,
    ICEBERG_DATA_BUCKET,
    ICEBERG_METADATA_BUCKET,
    METADATA_S3_BUCKET,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_REGION,
    MINIO_SECRET_KEY,
    GOLD_DATA_BUCKET,
    SILVER_DATA_BUCKET,
    SOURCE_S3_ACCESS_KEY,
    SOURCE_S3_BUCKET,
    SOURCE_S3_ENDPOINT,
    SOURCE_S3_REGION,
    SOURCE_S3_SECRET_KEY,
)
from ingestion.main.pipeline import run_s3_pipeline
from ingestion.pyfiles.metadata_reader import read_metadata
from ingestion.pyfiles.spark_session import get_spark_session
from ingestion.sink.minio_writer import write_to_minio


DAY_ONE = "2026-10-21"
DAY_TWO = "2026-10-22"
LAKE_NAMESPACE = "compat_lake"
BRONZE_NAMESPACE = "compat_bronze"
SILVER_NAMESPACE = "compat_silver"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def stage(message: str) -> None:
    print(f"[compatibility] {message}", flush=True)


def expect_value_error(action, expected_message: str, failure_message: str) -> None:
    try:
        action()
    except ValueError as error:
        require(expected_message in str(error), f"Unexpected rejection: {error}")
    else:
        raise AssertionError(failure_message)


def quote_record(segment: str, security_id: str, price: float, at: str) -> dict:
    received_at = datetime.fromisoformat(at).replace(tzinfo=timezone.utc)
    return json_ready(
        normalize_market_snapshot(
            {
                "status": "success",
                "data": {
                    segment: {
                        security_id: {"last_price": price, "volume": 1234}
                    }
                },
            },
            "quote",
            received_at=received_at,
            batch_id="compatibility-fixture",
        )[0]
    )


def price_map(frame) -> dict[tuple[str, str], float]:
    return {
        (row["exchange_segment"], row["security_id"]): row["last_price"]
        for row in frame.select("exchange_segment", "security_id", "last_price").collect()
    }


def canonical_s3(path: str) -> str:
    return path.replace("s3a://", "s3://", 1).rstrip("/")


def assert_table_paths(spark, table: str, expected_data: str, expected_metadata: str) -> set[str]:
    expected_data = canonical_s3(expected_data)
    files = {
        canonical_s3(row["file_path"])
        for row in spark.table(f"{table}.files").select("file_path").collect()
    }
    require(bool(files), f"{table} has no physical data files")
    require(
        all(path.startswith(expected_data + "/") for path in files),
        f"{table} wrote data outside its own path: {sorted(files)}",
    )
    description = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").collect()
    locations = [
        row["data_type"]
        for row in description
        if str(row["col_name"]).strip().lower() == "location"
    ]
    require(bool(locations), f"{table} did not expose an Iceberg location")
    require(
        canonical_s3(locations[0]) == canonical_s3(expected_metadata),
        f"{table} has unexpected metadata location: {locations[0]}",
    )
    return files


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--metadata-file",
        type=Path,
        default=Path("/examples/metadata/dhan_market_snapshot.csv"),
    )
    parser.add_argument("--trino-host", default=os.environ.get("TRINO_HOST", "trino"))
    parser.add_argument(
        "--trino-port", type=int, default=int(os.environ.get("TRINO_PORT", "8080"))
    )
    parser.add_argument("--trino-catalog", default=ICEBERG_CATALOG)
    parser.add_argument("--dbt-project", type=Path, default=Path("/app/transformation"))
    options = parser.parse_args()
    for name in (ICEBERG_CATALOG, options.trino_catalog):
        require(bool(re.fullmatch(r"[a-z_][a-z0-9_]*", name)), "Invalid catalog identifier")
    require(options.metadata_file.is_file(), f"Missing metadata fixture: {options.metadata_file}")
    live_metadata_file = options.metadata_file.with_name("dhan_live_ticks.csv")
    require(live_metadata_file.is_file(), f"Missing live metadata fixture: {live_metadata_file}")
    require((options.dbt_project / "dbt_project.yml").is_file(), "DBT project is missing")

    app_name = "compat_" + uuid4().hex[:12]
    isolated_name = app_name + "_isolated"
    metadata_key = f"compatibility/{app_name}/metadata.csv"
    object_prefix = f"compatibility/{app_name}"
    lake_table = f"{ICEBERG_CATALOG}.{LAKE_NAMESPACE}.{app_name}"
    isolated_table = f"{ICEBERG_CATALOG}.{LAKE_NAMESPACE}.{isolated_name}"
    bronze_table = f"{ICEBERG_CATALOG}.{BRONZE_NAMESPACE}.{app_name}"
    silver_table = f"{options.trino_catalog}.{SILVER_NAMESPACE}.{app_name}"
    spark_silver_table = f"{ICEBERG_CATALOG}.{SILVER_NAMESPACE}.{app_name}"
    dbt_namespace = app_name + "_dbt"
    stage(f"Using unique application {app_name}; test artifacts will be retained")

    minio = boto3.client(
        "s3", endpoint_url=MINIO_ENDPOINT, region_name=MINIO_REGION,
        aws_access_key_id=MINIO_ACCESS_KEY, aws_secret_access_key=MINIO_SECRET_KEY,
    )
    source = boto3.client(
        "s3", endpoint_url=SOURCE_S3_ENDPOINT, region_name=SOURCE_S3_REGION,
        aws_access_key_id=SOURCE_S3_ACCESS_KEY, aws_secret_access_key=SOURCE_S3_SECRET_KEY,
    )
    for bucket in {
        METADATA_S3_BUCKET, ICEBERG_DATA_BUCKET, ICEBERG_METADATA_BUCKET,
        BRONZE_DATA_BUCKET, SILVER_DATA_BUCKET, GOLD_DATA_BUCKET,
    }:
        minio.head_bucket(Bucket=bucket)
    source.head_bucket(Bucket=SOURCE_S3_BUCKET)
    minio.put_object(
        Bucket=METADATA_S3_BUCKET, Key=metadata_key,
        Body=options.metadata_file.read_bytes(), ContentType="text/csv",
    )
    fixtures = {
        DAY_ONE: [
            quote_record("NSE_EQ", "1333", 100.0, DAY_ONE + "T04:00:00"),
            quote_record("NSE_EQ", "1333", 101.0, DAY_ONE + "T04:01:00"),
            quote_record("IDX_I", "13", 25000.0, DAY_ONE + "T04:00:00"),
        ],
        DAY_TWO: [
            quote_record("NSE_EQ", "1333", 102.0, DAY_TWO + "T04:00:00"),
            # An older quote arriving in today's file must not replace newer state.
            quote_record("IDX_I", "13", 24000.0, "2026-10-20T04:00:00"),
            quote_record("NSE_EQ", "11536", 150.0, DAY_TWO + "T04:00:00"),
        ],
    }
    for day, records in fixtures.items():
        source.put_object(
            Bucket=SOURCE_S3_BUCKET, Key=f"{object_prefix}/{day}.json",
            Body=("\n".join(json.dumps(row) for row in records) + "\n").encode(),
            ContentType="application/x-ndjson",
        )

    spark = get_spark_session()
    spark.conf.set("spark.sql.shuffle.partitions", "2")
    trino_connection = None
    try:
        def ingest(day: str, table_name: str = app_name) -> None:
            args = Namespace(
                application_name=app_name, ingest_date=day, source_type="dhan-live",
                source_bucket=SOURCE_S3_BUCKET, source_key=f"{object_prefix}/{day}.json",
                output_table_name=table_name, output_database=LAKE_NAMESPACE,
                output_catalog=ICEBERG_CATALOG, output_bucket=ICEBERG_DATA_BUCKET,
                metadata_key=metadata_key, partition=None, write_mode="overwrite",
            )
            run_s3_pipeline(
                args, source_key=args.source_key, source_bucket=SOURCE_S3_BUCKET,
                output_table_name=table_name, output_database=LAKE_NAMESPACE,
                output_catalog=ICEBERG_CATALOG, output_bucket=ICEBERG_DATA_BUCKET,
                metadata_key=metadata_key, write_mode="overwrite",
            )

        stage("Ingesting normalized Dhan JSON through actual metadata/casting/Lake writer")
        ingest(DAY_ONE)
        lake = spark.table(lake_table)
        require(lake.count() == 3, "Lake did not preserve all three source rows")
        for column, expected_type in {
            "security_id": StringType, "received_at": TimestampType,
            "last_price": DoubleType, "trading_date": DateType, "ingest_date": DateType,
        }.items():
            require(
                isinstance(lake.schema[column].dataType, expected_type),
                f"Lake column {column} has incompatible type {lake.schema[column].dataType}",
            )
        require(
            lake.filter(F.col("previous_open_interest").isNotNull()).count() == 0,
            "Nullable market fields were not preserved",
        )
        metadata = read_metadata(spark, key=metadata_key)
        keys, timestamp_column = extract_bronze_keys(metadata)
        require(set(keys) == {"exchange_segment", "security_id"}, f"Unexpected Bronze keys: {keys}")
        require(timestamp_column == "received_at", "Bronze ordering timestamp is incorrect")

        stage("Running Bronze FULL and checking latest quote wins")
        run_full(
            spark, app_name, DAY_ONE, keys, timestamp_column,
            ICEBERG_CATALOG, LAKE_NAMESPACE, BRONZE_NAMESPACE,
        )
        require(spark.table(bronze_table).count() == 2, "Bronze FULL did not deduplicate")
        require(
            price_map(spark.table(bronze_table)) == {
                ("NSE_EQ", "1333"): 101.0, ("IDX_I", "13"): 25000.0,
            },
            "Bronze FULL did not retain the latest instrument price",
        )

        stage("Replaying Lake partition, then checking Bronze DELTA and stale arrivals")
        ingest(DAY_ONE)
        require(spark.table(lake_table).count() == 3, "Lake overwrite replay duplicated rows")
        ingest(DAY_TWO)
        require(spark.table(lake_table).count() == 6, "Lake overwrite erased a previous date")
        expected_prices = {
            ("NSE_EQ", "1333"): 102.0, ("IDX_I", "13"): 25000.0,
            ("NSE_EQ", "11536"): 150.0,
        }
        for _ in range(2):
            run_delta(
                spark, app_name, DAY_TWO, keys, timestamp_column,
                ICEBERG_CATALOG, LAKE_NAMESPACE, BRONZE_NAMESPACE,
            )
            bronze = spark.table(bronze_table)
            require(bronze.count() == 5, "Bronze DELTA replay lost or duplicated a snapshot")
            latest = bronze.filter(F.col("snapshot_date") == F.lit(DAY_TWO).cast("date"))
            require(latest.count() == 3, "Bronze DELTA has the wrong instrument count")
            require(price_map(latest) == expected_prices, "Stale arrival replaced newer Bronze state")

        stage("Checking empty input fails without changing existing Lake or Bronze data")
        bronze_before = set(spark.table(bronze_table).collect())
        for bronze_runner in (run_full, run_delta):
            expect_value_error(
                lambda: bronze_runner(
                    spark, app_name, "2026-10-23", keys, timestamp_column,
                    ICEBERG_CATALOG, LAKE_NAMESPACE, BRONZE_NAMESPACE,
                ),
                "No Lake rows", f"{bronze_runner.__name__} accepted an absent Lake date",
            )
            require(
                set(spark.table(bronze_table).collect()) == bronze_before,
                f"{bronze_runner.__name__} changed the five existing Bronze rows after rejection",
            )
        lake_before = set(spark.table(lake_table).collect())
        empty_lake_input = spark.table(lake_table).drop("ingest_date").limit(0)
        empty_args = Namespace(
            application_name=app_name, ingest_date=DAY_TWO,
            output_bucket=ICEBERG_DATA_BUCKET, partition=None,
        )
        for mode in ("append", "overwrite", "replace"):
            expect_value_error(
                lambda: write_to_minio(
                    spark, empty_lake_input, empty_args, app_name,
                    LAKE_NAMESPACE, ICEBERG_CATALOG, mode,
                ),
                "empty Lake batch", f"Lake {mode} accepted an empty source batch",
            )
            require(
                set(spark.table(lake_table).collect()) == lake_before,
                f"Lake {mode} changed existing data after rejecting an empty source batch",
            )

        stage("Checking physical table isolation and separate data/metadata buckets")
        ingest(DAY_ONE, isolated_name)
        require(spark.table(isolated_table).count() == 3, "Isolated table has incorrect rows")
        require(spark.table(lake_table).count() == 6, "Writing a second table changed the first")
        main_path = f"{ICEBERG_CATALOG}/{LAKE_NAMESPACE}/{app_name}"
        isolated_path = f"{ICEBERG_CATALOG}/{LAKE_NAMESPACE}/{isolated_name}"
        main_files = assert_table_paths(
            spark, lake_table, f"s3://{ICEBERG_DATA_BUCKET}/{main_path}",
            f"s3://{ICEBERG_METADATA_BUCKET}/iceberg/{main_path}",
        )
        isolated_files = assert_table_paths(
            spark, isolated_table, f"s3://{ICEBERG_DATA_BUCKET}/{isolated_path}",
            f"s3://{ICEBERG_METADATA_BUCKET}/iceberg/{isolated_path}",
        )
        require(main_files.isdisjoint(isolated_files), "Two Lake tables share physical data files")
        bronze_path = f"{ICEBERG_CATALOG}/{BRONZE_NAMESPACE}/{app_name}"
        assert_table_paths(
            spark, bronze_table, f"s3://{BRONZE_DATA_BUCKET}/{bronze_path}",
            f"s3://{ICEBERG_METADATA_BUCKET}/iceberg/{bronze_path}",
        )

        stage("Checking Trino reads Spark's Lake and Bronze Iceberg tables")
        trino_connection = trino.dbapi.connect(
            host=options.trino_host, port=options.trino_port, user="compatibility-smoke",
            catalog=options.trino_catalog, schema=BRONZE_NAMESPACE,
            request_timeout=60, max_attempts=2,
        )
        cursor = trino_connection.cursor()
        trino_lake = f"{options.trino_catalog}.{LAKE_NAMESPACE}.{app_name}"
        trino_bronze = f"{options.trino_catalog}.{BRONZE_NAMESPACE}.{app_name}"
        cursor.execute(f"SELECT count(*) FROM {trino_lake}")
        require(cursor.fetchone()[0] == 6, "Trino cannot read all Spark Lake rows")
        cursor.execute(
            f"SELECT exchange_segment, security_id, last_price FROM {trino_bronze} "
            f"WHERE snapshot_date = DATE '{DAY_TWO}'"
        )
        require(
            {(row[0], row[1]): row[2] for row in cursor.fetchall()} == expected_prices,
            "Trino's Bronze results differ from Spark's results",
        )

        stage("Writing Silver through Trino, then reading it back through Spark")
        cursor.execute(
            f"CREATE SCHEMA IF NOT EXISTS {options.trino_catalog}.{SILVER_NAMESPACE} "
            f"WITH (location = 's3://{ICEBERG_METADATA_BUCKET}/{SILVER_NAMESPACE}/')"
        )
        cursor.fetchall()
        silver_path = f"{options.trino_catalog}/{SILVER_NAMESPACE}/{app_name}"
        cursor.execute(
            f"CREATE TABLE {silver_table} WITH (format = 'PARQUET', "
            f"location = 's3://{ICEBERG_METADATA_BUCKET}/iceberg/{silver_path}', "
            f"data_location = 's3://{SILVER_DATA_BUCKET}/{silver_path}') AS "
            f"SELECT exchange_segment, security_id, last_price, snapshot_date "
            f"FROM {trino_bronze} WHERE snapshot_date = DATE '{DAY_TWO}'"
        )
        cursor.fetchall()
        cursor.execute(f"SELECT count(*), sum(last_price) FROM {silver_table}")
        silver_count, silver_sum = cursor.fetchone()
        require(silver_count == 3 and silver_sum == 25252.0, "Trino Silver output is incorrect")
        silver = spark.table(spark_silver_table)
        require(silver.count() == 3, "Spark cannot read Trino's Silver table")
        require(price_map(silver) == expected_prices, "Spark read different Trino Silver values")
        require(isinstance(silver.schema["snapshot_date"].dataType, DateType), "Silver date type changed")
        assert_table_paths(
            spark, spark_silver_table, f"s3://{SILVER_DATA_BUCKET}/{silver_path}",
            f"s3://{ICEBERG_METADATA_BUCKET}/iceberg/{silver_path}",
        )

        dbt_environment = dict(os.environ)
        dbt_environment.update({
            "BRONZE_DATABASE": BRONZE_NAMESPACE,
            "SILVER_DATABASE": dbt_namespace,
            "TRINO_HOST": options.trino_host,
            "TRINO_PORT": str(options.trino_port),
            "ICEBERG_CATALOG": options.trino_catalog,
        })

        def dbt_build(model: str, variables: dict) -> None:
            subprocess.run(
                [
                    "dbt", "build", "--select", model,
                    "--project-dir", str(options.dbt_project),
                    "--profiles-dir", str(options.dbt_project),
                    "--target", "dev", "--threads", "2", "--no-partial-parse",
                    "--target-path", f"/tmp/{app_name}_dbt_target",
                    "--log-path", f"/tmp/{app_name}_dbt_logs",
                    "--vars", json.dumps(variables),
                ],
                env=dbt_environment, check=True, timeout=600,
            )

        stage("Building the actual NSE snapshot DBT model and replaying its MERGE")
        dbt_snapshot_table = f"{options.trino_catalog}.{dbt_namespace}.nse_market_snapshot"
        spark_dbt_snapshot = f"{ICEBERG_CATALOG}.{dbt_namespace}.nse_market_snapshot"
        for _ in range(2):
            dbt_build("nse_market_snapshot", {
                "snapshot_date": DAY_TWO, "dhan_snapshot_source_table": app_name,
            })
            cursor.execute(f"SELECT count(*) FROM {dbt_snapshot_table}")
            require(cursor.fetchone()[0] == 3, "DBT snapshot replay lost or duplicated rows")
            cursor.execute(
                f"SELECT exchange_segment, security_id, last_price FROM {dbt_snapshot_table} "
                f"WHERE snapshot_date = DATE '{DAY_TWO}'"
            )
            require(
                {(row[0], row[1]): row[2] for row in cursor.fetchall()} == expected_prices,
                "DBT snapshot values or historical date differ from Bronze",
            )
            spark.catalog.refreshTable(spark_dbt_snapshot)
            require(
                price_map(spark.table(spark_dbt_snapshot)) == expected_prices,
                "Spark cannot read the actual DBT snapshot output",
            )

        stage("Replaying live events through Lake append, Bronze FULL, and NSE DBT MERGE")
        live_app = app_name + "_live"
        live_metadata_key = f"{object_prefix}/live_metadata.csv"
        live_source_key = f"{object_prefix}/live.json"
        minio.put_object(
            Bucket=METADATA_S3_BUCKET, Key=live_metadata_key,
            Body=live_metadata_file.read_bytes(), ContentType="text/csv",
        )
        source.put_object(
            Bucket=SOURCE_S3_BUCKET, Key=live_source_key,
            Body=("\n".join(json.dumps(row) for row in fixtures[DAY_ONE]) + "\n").encode(),
            ContentType="application/x-ndjson",
        )
        for _ in range(2):
            live_args = Namespace(
                application_name=live_app, ingest_date=DAY_ONE, source_type="dhan-live",
                output_bucket=ICEBERG_DATA_BUCKET, partition=None,
            )
            run_s3_pipeline(
                live_args, source_key=live_source_key, source_bucket=SOURCE_S3_BUCKET,
                output_table_name=live_app, output_database=LAKE_NAMESPACE,
                output_catalog=ICEBERG_CATALOG, output_bucket=ICEBERG_DATA_BUCKET,
                metadata_key=live_metadata_key, write_mode="append",
            )
        require(
            spark.table(f"{ICEBERG_CATALOG}.{LAKE_NAMESPACE}.{live_app}").count() == 6,
            "Live Lake append failed to preserve the replayed source records",
        )
        live_metadata = read_metadata(spark, key=live_metadata_key)
        live_keys, live_timestamp = extract_bronze_keys(live_metadata)
        require(live_keys == ["event_id"], "Live Bronze must deduplicate by event identity")
        run_full(
            spark, live_app, DAY_ONE, live_keys, live_timestamp,
            ICEBERG_CATALOG, LAKE_NAMESPACE, BRONZE_NAMESPACE,
        )
        require(
            spark.table(f"{ICEBERG_CATALOG}.{BRONZE_NAMESPACE}.{live_app}").count() == 3,
            "Live Bronze did not deduplicate replayed events",
        )
        expected_events = {row["event_id"]: row["last_price"] for row in fixtures[DAY_ONE]}
        dbt_live_table = f"{options.trino_catalog}.{dbt_namespace}.nse_live_ticks"
        spark_dbt_live = f"{ICEBERG_CATALOG}.{dbt_namespace}.nse_live_ticks"
        for _ in range(2):
            dbt_build("nse_live_ticks", {
                "snapshot_date": DAY_ONE, "dhan_live_source_table": live_app,
            })
            cursor.execute(f"SELECT event_id, last_price FROM {dbt_live_table}")
            actual_events = cursor.fetchall()
            require(len(actual_events) == 3, "DBT live MERGE duplicated replayed events")
            require(dict(actual_events) == expected_events, "DBT live changed event identities or prices")
            spark.catalog.refreshTable(spark_dbt_live)
            spark_events = spark.table(spark_dbt_live).select("event_id", "last_price").collect()
            require(
                len(spark_events) == 3 and {
                    row["event_id"]: row["last_price"] for row in spark_events
                } == expected_events,
                "Spark cannot read the actual DBT live-event output",
            )

        stage("Publishing actual DBT Silver to local Iceberg Gold, then checking Spark readback")
        from smoke_gold import publish_existing_source, run_checks as gold_checks
        gold_schema = "compat_gold"
        gold_table = f"{options.trino_catalog}.{gold_schema}.{app_name}"
        for _ in range(2):
            publish_existing_source(
                dbt_namespace, "nse_market_snapshot", app_name, gold_schema=gold_schema,
                host=options.trino_host, port=options.trino_port, catalog=options.trino_catalog,
            )
            cursor.execute(f"SELECT count(*) FROM {gold_table}")
            require(cursor.fetchone()[0] == 3, "DBT-to-Gold replay lost or duplicated rows")
            spark.catalog.refreshTable(gold_table)
            require(price_map(spark.table(gold_table)) == expected_prices, "Spark Gold readback differs from Silver")
        gold_checks(options.trino_host, options.trino_port, options.trino_catalog)

        stage("PASS: Lake -> Bronze -> Trino/DBT Silver -> local Gold -> Spark interoperability and replay")
        print(json.dumps({
            "status": "passed", "application": app_name,
            "lake_table": lake_table, "lake_rows": 6,
            "bronze_table": bronze_table, "bronze_rows": 5,
            "silver_table": silver_table, "silver_rows": 3,
            "dbt_snapshot_table": dbt_snapshot_table, "dbt_snapshot_rows": 3,
            "dbt_live_table": dbt_live_table, "dbt_live_rows": 3,
            "gold_table": gold_table, "gold_rows": 3,
            "source_prefix": f"s3://{SOURCE_S3_BUCKET}/{object_prefix}/",
            "metadata_key": f"s3://{METADATA_S3_BUCKET}/{metadata_key}",
            "artifacts_retained": True,
            "dhan_authenticated": False, "oracle_required": False,
        }, indent=2), flush=True)
    finally:
        if trino_connection is not None:
            trino_connection.close()
        spark.stop()


if __name__ == "__main__":
    main()
