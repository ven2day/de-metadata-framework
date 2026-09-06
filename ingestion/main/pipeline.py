import logging
import os
import sys
from argparse import Namespace

import boto3

from ingestion.env.DE_Ingestion_properties import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    LOG_S3_BUCKET,
    SOURCE_S3_BUCKET,
)
from ingestion.pyfiles.logger import get_logger, setup_log_file, close_log_file
from ingestion.pyfiles.args_parser import parse_args
from ingestion.pyfiles.email_notifier import send_pipeline_notification
from ingestion.pyfiles.connectivity_checker import run_all_checks
from ingestion.pyfiles.spark_session import get_spark_session, get_active_session
from ingestion.pyfiles.metadata_reader import read_metadata
from ingestion.pyfiles.schema_validator import validate_schema
from ingestion.pyfiles.type_caster import cast_columns
from ingestion.pyfiles.pii_processor import apply_pii
from ingestion.source.s3_reader import read_s3_file, resolve_s3_key
from ingestion.source.supabase_reader import read_supabase_table
from ingestion.sink.minio_writer import write_to_minio

logger = get_logger(__name__)


def run_s3_pipeline(
    args: Namespace,
    source_key: str,
    source_bucket: str | None = None,
    output_table_name: str | None = None,
    output_database: str | None = None,
    output_catalog: str | None = None,
    output_bucket: str | None = None,
    metadata_key: str | None = None,
    write_mode: str = "overwrite",
) -> None:
    logger.info(
        "=== ETL Pipeline Start [app=%s, date=%s, source=s3://%s/%s] ===",
        args.application_name, args.ingest_date, source_bucket or SOURCE_S3_BUCKET, source_key,
    )

    run_all_checks(args, source_bucket=source_bucket, minio_bucket=output_bucket)

    source_bucket = source_bucket or SOURCE_S3_BUCKET
    resolved_key  = resolve_s3_key(source_bucket, source_key, args.ingest_date)

    spark = get_spark_session()
    metadata = read_metadata(spark, key=metadata_key)
    df = read_s3_file(spark, resolved_key, bucket=source_bucket)

    validate_schema(df, metadata)
    df = cast_columns(df, metadata)
    df = apply_pii(df, metadata)

    write_to_minio(spark, df, args, output_table_name, output_database, output_catalog, write_mode)
    logger.info("=== ETL Pipeline Complete [app=%s, date=%s] ===", args.application_name, args.ingest_date)


def run_database_pipeline(
    args: Namespace,
    source_table_name: str,
    source_database: str | None = None,
    output_table_name: str | None = None,
    output_database: str | None = None,
    output_catalog: str | None = None,
    output_bucket: str | None = None,
    metadata_key: str | None = None,
    write_mode: str = "overwrite",
) -> None:
    logger.info(
        "=== ETL Pipeline Start [app=%s, date=%s, source=database:%s] ===",
        args.application_name, args.ingest_date, source_table_name,
    )

    run_all_checks(args, minio_bucket=output_bucket)

    spark = get_spark_session()
    metadata = read_metadata(spark, key=metadata_key)
    df = read_supabase_table(spark, source_table_name, schema=source_database)

    validate_schema(df, metadata)
    df = cast_columns(df, metadata)
    df = apply_pii(df, metadata)

    write_to_minio(spark, df, args, output_table_name, output_database, output_catalog, write_mode)
    logger.info("=== ETL Pipeline Complete [app=%s, date=%s] ===", args.application_name, args.ingest_date)


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
        logger.error("Failed to upload log to S3: %s", exc)


if __name__ == "__main__":
    args = parse_args()
    get_logger(__name__, level=getattr(logging, args.log_level))

    app  = args.application_name
    date = args.ingest_date

    local_log  = f"/tmp/{app}_{date}.log"

    setup_log_file(local_log)

    pipeline_error = None
    app_id = None
    try:
        if args.source_type == "s3":
            run_s3_pipeline(
                args=args,
                source_key=args.source_key,
                source_bucket=args.source_bucket,
                output_table_name=args.output_table_name,
                output_database=args.output_database,
                output_catalog=args.output_catalog,
                output_bucket=args.output_bucket,
                metadata_key=args.metadata_key,
                write_mode=args.write_mode,
            )
        else:
            run_database_pipeline(
                args=args,
                source_table_name=args.source_table_name,
                source_database=args.source_database,
                output_table_name=args.output_table_name,
                output_database=args.output_database,
                output_catalog=args.output_catalog,
                output_bucket=args.output_bucket,
                metadata_key=args.metadata_key,
                write_mode=args.write_mode,
            )
    except Exception as exc:
        pipeline_error = exc
        logger.error("Pipeline failed: %s", exc, exc_info=True)
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
        except Exception as exc:
            logger.error("Failed to close log file: %s", exc)

        final_log = local_log
        s3_log_key = f"{args.log_folder}/{app}/{date}/{app}_{date}.log"
        if app_id:
            new_name = f"/tmp/{app}_{date}_{app_id}.log"
            try:
                os.rename(local_log, new_name)
                final_log = new_name
            except Exception as exc:
                logger.error("Failed to rename log file: %s", exc)
            s3_log_key = f"{args.log_folder}/{app}/{date}/{app}_{date}_{app_id}.log"

        try:
            send_pipeline_notification(
                app_name=app,
                ingest_date=date,
                status="failed" if pipeline_error else "success",
                error_msg=str(pipeline_error) if pipeline_error else None,
                log_path=final_log if pipeline_error else None,
                app_id=app_id,
            )
        except Exception as exc:
            logger.error("Unexpected error during email notification: %s", exc)

        try:
            _upload_log(final_log, s3_log_key)
        except Exception as exc:
            logger.error("Failed to upload log to S3: %s", exc)

        try:
            os.remove(final_log)
        except Exception as exc:
            logger.error("Failed to remove local log file: %s", exc)
