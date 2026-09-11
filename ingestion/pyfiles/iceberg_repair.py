"""Utilities for repairing stale Iceberg metadata pointers in HMS."""
import json
import re
import uuid

import boto3
from botocore.client import Config
from pyspark.sql import SparkSession

from ingestion.env.DE_Ingestion_properties import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
from ingestion.pyfiles.logger import get_logger

logger = get_logger(__name__)

_MISSING_LOCATION_RE = re.compile(
    r"Location does not exist[:\s]+([^\s,]+)",
    re.IGNORECASE,
)
_FILE_NOT_FOUND_RE = re.compile(
    r"(?:FileNotFoundException|NoSuchFileException)[^\n]*?(s3[a]?://[^\s,]+\.metadata\.json)",
    re.IGNORECASE,
)


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT.strip(),
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _stub_body(table_location: str) -> bytes:
    return json.dumps({
        "format-version": 2,
        "table-uuid": str(uuid.uuid4()),
        "location": table_location,
        "last-sequence-number": 0,
        "last-updated-ms": 0,
        "last-column-id": 0,
        "current-schema-id": 0,
        "schemas": [{"schema-id": 0, "type": "struct", "fields": []}],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "properties": {},
        "current-snapshot-id": -1,
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
        "refs": {},
    }).encode()


def write_stub_metadata(bad_location: str, table_location: str) -> None:
    """Write an Iceberg v2 stub at bad_location so HMS can load → drop the table.

    bad_location may be a specific .metadata.json file path or a metadata/ directory;
    both cases are handled.
    """
    url = bad_location.rstrip("/")
    if not url.endswith(".metadata.json"):
        # HMS stored a directory path — synthesise a filename Iceberg will discover
        url = f"{url}/00000-{uuid.uuid4()}.metadata.json"
    stripped = url.replace("s3a://", "").replace("s3://", "")
    bucket, _, key = stripped.partition("/")
    _s3_client().put_object(Bucket=bucket, Key=key, Body=_stub_body(table_location))
    logger.info("Wrote stub metadata to s3://%s/%s", bucket, key)


def _extract_bad_location(error_msg: str) -> str | None:
    for pat in (_MISSING_LOCATION_RE, _FILE_NOT_FOUND_RE):
        m = pat.search(error_msg)
        if m:
            return m.group(1)
    return None


def drop_table_safe(spark: SparkSession, full_table: str, table_location: str) -> None:
    """DROP TABLE handling stale HMS metadata_location pointers gracefully."""
    try:
        spark.sql(f"DROP TABLE IF EXISTS {full_table}")
        logger.info("Dropped table %s", full_table)
    except Exception as e:
        bad_loc = _extract_bad_location(str(e))
        if bad_loc:
            logger.warning(
                "DROP TABLE failed (missing %s) — writing stub and retrying", bad_loc
            )
            write_stub_metadata(bad_loc, table_location)
            try:
                spark.sql(f"DROP TABLE IF EXISTS {full_table}")
                logger.info("Dropped table %s after stub repair", full_table)
            except Exception as retry_err:
                logger.warning("Stub-assisted drop failed (%s) — HMS entry may remain", retry_err)
        else:
            logger.info("DROP TABLE non-metadata error (%s) — table may not exist", e)


def table_exists_safe(spark: SparkSession, full_table: str, table_location: str) -> bool:
    """Return True if the table exists and is readable.

    If HMS has a stale metadata_location pointer, drops the broken entry and
    returns False so the caller can recreate the table cleanly.
    """
    try:
        return spark.catalog.tableExists(full_table)
    except Exception as e:
        err = str(e)
        if any(tok in err for tok in ("Location does not exist", "NotFoundException",
                                      "FileNotFoundException", "NoSuchFileException")):
            logger.warning(
                "tableExists raised location error for %s — dropping stale HMS entry", full_table
            )
            drop_table_safe(spark, full_table, table_location)
            return False
        raise
