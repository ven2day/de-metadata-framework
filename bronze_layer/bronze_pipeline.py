import argparse
import logging
import os
import sys

import boto3

from ingestion.env.DE_Ingestion_properties import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, LOG_S3_BUCKET,
    ICEBERG_CATALOG, LAKE_DATABASE, BRONZE_DATABASE,
)
from ingestion.pyfiles.logger import get_logger, setup_log_file, close_log_file
from ingestion.pyfiles.spark_session import get_spark_session, get_active_session
from ingestion.pyfiles.metadata_reader import read_metadata
from bronze_layer.bronze_processor import extract_bronze_keys, run_full, run_delta

logger = get_logger(__name__)


def _parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="bronze-pipeline",
        description="Medallion bronze layer: deduplicates lake data into de_bronze.",
    )
    parser.add_argument("--application-name", dest="application_name", required=True)
    parser.add_argument("--run-date",         dest="run_date",         required=True,
                        help="Run date (YYYY-MM-DD)")
    parser.add_argument("--run-type",         dest="run_type",
                        choices=["full", "delta"], default="full",
                        help="full = lake run_date only; delta = lake + previous bronze snapshot")
    parser.add_argument("--catalog",          dest="catalog",          default=None,
                        help="Iceberg catalog name (default: ICEBERG_CATALOG env)")
    parser.add_argument("--lake-database",    dest="lake_database",    default=None,
                        help="Iceberg lake database (default: LAKE_DATABASE env)")
    parser.add_argument("--bronze-database",  dest="bronze_database",  default=None,
                        help="Iceberg bronze database (default: BRONZE_DATABASE env)")
    parser.add_argument("--metadata-key",     dest="metadata_key",     default=None,
                        help="S3 key of the metadata CSV (must contain primary_key / is_timestamp columns)")
    parser.add_argument("--log-level",        dest="log_level",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")
    parser.add_argument("--log-folder",       dest="log_folder",       default="bronze",
                        help="Top-level folder in the log S3 bucket (default: bronze)")
    return parser.parse_args(argv)


def _upload_log(local_path: str, s3_key: str) -> None:
    try:
        client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )
        client.upload_file(local_path, LOG_S3_BUCKET, s3_key, ExtraArgs={"ContentType": "text/plain"})
        logger.info("Log uploaded → s3://%s/%s", LOG_S3_BUCKET, s3_key)
    except Exception as exc:
        logger.error("Failed to upload log: %s", exc)


if __name__ == "__main__":
    args = _parse_args()
    get_logger(__name__, level=getattr(logging, args.log_level))

    app       = args.application_name.lower().replace("-", "_").replace(" ", "_")
    run_date  = args.run_date
    catalog   = args.catalog        or ICEBERG_CATALOG
    lake_db   = args.lake_database  or LAKE_DATABASE
    bronze_db = args.bronze_database or BRONZE_DATABASE

    local_log = f"/tmp/bronze_{app}_{run_date}.log"
    setup_log_file(local_log)

    pipeline_error = None
    app_id = None
    try:
        spark    = get_spark_session()
        metadata = read_metadata(spark, key=args.metadata_key)
        primary_keys, timestamp_col = extract_bronze_keys(metadata)
        logger.info("Primary keys: %s | Timestamp column: %s", primary_keys, timestamp_col)

        if args.run_type == "full":
            run_full(spark, app, run_date, primary_keys, timestamp_col, catalog, lake_db, bronze_db)
        else:
            run_delta(spark, app, run_date, primary_keys, timestamp_col, catalog, lake_db, bronze_db)

        logger.info("=== Bronze Pipeline Complete [app=%s, date=%s, type=%s] ===", app, run_date, args.run_type)

    except Exception as exc:
        pipeline_error = exc
        logger.error("Bronze pipeline failed: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        active = get_active_session()
        if active:
            try:
                app_id = active.sparkContext.applicationId
            except Exception:
                pass

        try:
            close_log_file()
        except Exception:
            pass

        final_log  = local_log
        s3_log_key = f"{args.log_folder}/{app}/{run_date}/bronze_{app}_{run_date}.log"
        if app_id:
            new_name = f"/tmp/bronze_{app}_{run_date}_{app_id}.log"
            try:
                os.rename(local_log, new_name)
                final_log = new_name
            except Exception:
                pass
            s3_log_key = f"{args.log_folder}/{app}/{run_date}/bronze_{app}_{run_date}_{app_id}.log"

        _upload_log(final_log, s3_log_key)
        try:
            os.remove(final_log)
        except Exception:
            pass
