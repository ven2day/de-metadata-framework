from argparse import Namespace

import psycopg2
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from ingestion.env.DE_Ingestion_properties import (
    SOURCE_S3_ENDPOINT,
    SOURCE_S3_ACCESS_KEY,
    SOURCE_S3_SECRET_KEY,
    SOURCE_S3_BUCKET,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET,
    SUPABASE_JDBC_URL,
    SUPABASE_DB_USER,
    SUPABASE_DB_PASSWORD,
)
from ingestion.pyfiles.logger import get_logger
from ingestion.pyfiles.vault_client import get_transit_encryption_key, get_encrypt_value

logger = get_logger(__name__)


def check_source_s3(bucket: str | None = None) -> None:
    bucket = bucket or SOURCE_S3_BUCKET
    logger.info("Checking source S3 connectivity")
    try:
        client = boto3.client(
            "s3",
            endpoint_url=SOURCE_S3_ENDPOINT,
            aws_access_key_id=SOURCE_S3_ACCESS_KEY,
            aws_secret_access_key=SOURCE_S3_SECRET_KEY,
        )
        client.head_bucket(Bucket=bucket)
        logger.info("Source S3 OK")
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        raise RuntimeError(f"Source S3 bucket '{bucket}' check failed (HTTP {code}): {exc}") from exc
    except BotoCoreError as exc:
        raise RuntimeError(f"Source S3 connection error: {exc}") from exc


def check_minio(bucket: str | None = None) -> None:
    bucket = bucket or MINIO_BUCKET
    logger.info("Checking MinIO connectivity")
    try:
        client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )
        client.head_bucket(Bucket=bucket)
        logger.info("MinIO OK")
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code == "404":
            try:
                client.create_bucket(Bucket=bucket)
                logger.info("MinIO bucket did not exist — created it")
                return
            except ClientError as create_exc:
                raise RuntimeError(
                    f"MinIO bucket '{bucket}' not found and could not be created: {create_exc}"
                ) from create_exc
        raise RuntimeError(f"MinIO bucket '{bucket}' check failed (HTTP {code}): {exc}") from exc
    except BotoCoreError as exc:
        raise RuntimeError(f"MinIO connection error: {exc}") from exc


def check_supabase() -> None:
    logger.info("Checking Supabase connectivity")

    password = get_encrypt_value(
        SUPABASE_DB_PASSWORD,
        key_name="supabase-pwd",
        key_type="encryption-key",
        mount_path='transit'
    )

    try:
        conn = psycopg2.connect(
            SUPABASE_JDBC_URL.replace("jdbc:", ""),
            user=SUPABASE_DB_USER,
            password=password,
        )
        conn.close()
        logger.info("Supabase OK — connected via JDBC URL")
    except Exception as exc:
        raise RuntimeError(f"Supabase connectivity check failed: {exc}") from exc


def run_all_checks(
        args: Namespace,
    source_bucket: str | None = None,
    minio_bucket: str | None = None,
) -> None:
    if args.source_type.upper() == "S3":
        check_source_s3(bucket=source_bucket)
    else:
        check_supabase()
    check_minio(bucket=minio_bucket)
    logger.info("All connectivity checks passed")
