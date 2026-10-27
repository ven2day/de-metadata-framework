import re
from datetime import datetime
from fnmatch import fnmatchcase

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import DataFrame, SparkSession

from ingestion.env.DE_Ingestion_properties import (
    SOURCE_S3_BUCKET,
    SOURCE_S3_ENDPOINT,
    SOURCE_S3_ACCESS_KEY,
    SOURCE_S3_REGION,
    SOURCE_S3_SECRET_KEY,
)
from ingestion.pyfiles.logger import get_logger

logger = get_logger(__name__)

_SUPPORTED_FORMATS = ("csv", "json", "parquet")

# Format tokens the user may embed in source_key (e.g. "file__YYYY-MM-DD.csv")
# Ordered longest-first to avoid partial matches (YYYY-MM-DD before YYYYMMDD).
_TOKEN_TO_STRFTIME = {
    "YYYY-MM-DD": "%Y-%m-%d",
    "DD-MM-YYYY": "%d-%m-%Y",
    "MM-DD-YYYY": "%m-%d-%Y",
    "YYYY_MM_DD": "%Y_%m_%d",
    "DD_MM_YYYY": "%d_%m_%Y",
    "MM_DD_YYYY": "%m_%d_%Y",
    "YYYYMMDD":   "%Y%m%d",
    "DDMMYYYY":   "%d%m%Y",
}

# Regex patterns that match date strings in S3 object keys (used during listing).
_DATE_PATTERNS = [
    (re.compile(r"\d{4}-\d{2}-\d{2}"), "%Y-%m-%d"),
    (re.compile(r"\d{4}_\d{2}_\d{2}"), "%Y_%m_%d"),
    (re.compile(r"\d{8}"),             "%Y%m%d"),
    (re.compile(r"\d{2}-\d{2}-\d{4}"), "%d-%m-%Y"),
    (re.compile(r"\d{2}_\d{2}_\d{4}"), "%d_%m_%Y"),
]


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=SOURCE_S3_ENDPOINT.strip(),
        aws_access_key_id=SOURCE_S3_ACCESS_KEY,
        aws_secret_access_key=SOURCE_S3_SECRET_KEY,
        region_name=SOURCE_S3_REGION,
    )


def _key_exists(client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def _matching_keys(client, bucket: str, pattern: str) -> list[str]:
    """List all objects matching a glob pattern, using paginated S3 listings."""
    wildcard_positions = [
        position for token in ("*", "?", "[")
        if (position := pattern.find(token)) >= 0
    ]
    prefix = pattern[: min(wildcard_positions)] if wildcard_positions else pattern
    paginator = client.get_paginator("list_objects_v2")
    matches: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        matches.extend(
            item["Key"]
            for item in page.get("Contents", [])
            if fnmatchcase(item["Key"], pattern)
        )
    return matches


def resolve_s3_key(bucket: str, key: str, ingest_date: str) -> str:
    """
    Resolve a general-format source key to the actual S3 object key.

    The '__' separator is required for any date-based resolution.
    Without it, only an exact key match is attempted.

    Resolution order:
    1. Exact key exists → return as-is.
    2. Key contains '__<TOKEN>' (e.g. '__YYYY-MM-DD') → substitute token
       with ingest_date in the corresponding format and verify the object exists.
    3. Key contains '__' (but no token) → list objects under the prefix before
       '__' and find one whose '__<date_part>' matches ingest_date in any
       supported format.
    4. No '__' in key and exact match failed → raise immediately; date
       substitution is not performed without the explicit separator.
    """
    dt = datetime.strptime(ingest_date, "%Y-%m-%d")
    client = _s3_client()

    # Immutable live-feed micro-batches are consumed with an explicit glob,
    # e.g. dhan/nse/date=2026-10-25/hour=09/minute=15/*.json.
    if any(token in key for token in ("*", "?", "[")):
        resolved_pattern = key
        for token, fmt in _TOKEN_TO_STRFTIME.items():
            resolved_pattern = resolved_pattern.replace(token, dt.strftime(fmt))
        matches = _matching_keys(client, bucket, resolved_pattern)
        if not matches:
            raise FileNotFoundError(
                f"No S3 objects match pattern '{resolved_pattern}' in bucket '{bucket}'"
            )
        logger.info(
            "Source pattern resolved to %d objects: s3://%s/%s",
            len(matches),
            bucket,
            resolved_pattern,
        )
        return resolved_pattern

    # Step 1 — exact match (always attempted first, regardless of __)
    if _key_exists(client, bucket, key):
        logger.info("Source key resolved (exact): s3://%s/%s", bucket, key)
        return key

    has_separator = "__" in key

    if not has_separator:
        raise FileNotFoundError(
            f"Source key '{key}' not found in bucket '{bucket}' and contains no '__' "
            f"date separator — date substitution requires the format "
            f"'<prefix>__<DATE_TOKEN>.<ext>' (e.g. 'folder/file__YYYY-MM-DD.csv')."
        )

    # Step 2 — token substitution: only when '__<TOKEN>' is present
    for token, fmt in _TOKEN_TO_STRFTIME.items():
        if f"__{token}" in key:
            candidate = key.replace(f"__{token}", f"__{dt.strftime(fmt)}")
            if _key_exists(client, bucket, candidate):
                logger.info("Source key resolved (token '%s'): s3://%s/%s", token, bucket, candidate)
                return candidate
            raise FileNotFoundError(
                f"Source key token '{token}' substituted to '{candidate}' "
                f"but object does not exist in bucket '{bucket}'."
            )

    # Step 3 — prefix listing: key has '__' but no recognised token
    # Build all date strings we'd accept for this ingest_date
    accepted_dates = {dt.strftime(fmt) for _, fmt in _DATE_PATTERNS}
    prefix = key.rsplit("__", 1)[0]  # everything before the last '__'

    candidates = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            obj_key = obj["Key"]
            remainder = obj_key[len(prefix):]       # e.g. "__2024-01-15.csv"
            if not remainder.startswith("__"):
                continue
            date_part = remainder[2:].split(".")[0]  # strip '__', drop extension
            if date_part in accepted_dates:
                candidates.append((obj["LastModified"], obj_key))

    if candidates:
        resolved = sorted(candidates, reverse=True)[0][1]
        logger.info("Source key resolved (listing): s3://%s/%s", bucket, resolved)
        return resolved

    raise FileNotFoundError(
        f"Cannot resolve source key '{key}' for ingest_date={ingest_date} "
        f"in bucket '{bucket}': no object found matching '__<date>' after prefix '{prefix}'."
    )


def read_s3_file(spark: SparkSession, key: str, bucket: str | None = None) -> DataFrame:
    bucket = bucket or SOURCE_S3_BUCKET
    ext = key.rsplit(".", 1)[-1].lower()

    if ext not in _SUPPORTED_FORMATS:
        raise ValueError(f"Unsupported format '.{ext}'. Supported: {_SUPPORTED_FORMATS}")

    path = f"s3a://{bucket}/{key}"
    logger.info("Reading %s file: s3://%s/%s", ext.upper(), bucket, key)

    if ext == "csv":
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    elif ext == "json":
        df = spark.read.json(path)
    else:
        df = spark.read.parquet(path)

    logger.info("Loaded %d columns", len(df.columns))
    return df
