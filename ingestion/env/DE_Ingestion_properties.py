"""Environment-backed configuration for the framework.

This module intentionally contains no real credentials. Local values are loaded
from ``.env`` (which is gitignored); containers receive the same names through
Docker Compose. Defaults target the bundled local stack. External systems are
left empty until their corresponding feature is used.
"""

import os

from dotenv import load_dotenv


load_dotenv()


def _text(name: str, default: str = "") -> str:
    """Read a text setting and remove accidental surrounding whitespace."""
    return os.getenv(name, default).strip()


def _integer(name: str, default: int) -> int:
    raw = _text(name, str(default))
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def _positive_integer(name: str, default: int) -> int:
    value = _integer(name, default)
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero, got {value}")
    return value


# PII encryption
SALT_KEY: str = os.getenv("SALT_KEY", "change-me-generate-a-32-char-random-string")

# Source S3 (MinIO-compatible)
SOURCE_S3_ENDPOINT: str = _text("SOURCE_S3_ENDPOINT", "http://127.0.0.1:9000")
# When source and sink are the bundled MinIO, inherit its credentials by default.
SOURCE_S3_ACCESS_KEY: str = (
    os.getenv("SOURCE_S3_ACCESS_KEY", "").strip()
    or os.getenv("MINIO_ACCESS_KEY", "minioadmin").strip()
)
SOURCE_S3_SECRET_KEY: str = (
    os.getenv("SOURCE_S3_SECRET_KEY", "").strip()
    or os.getenv("MINIO_SECRET_KEY", "minioadmin").strip()
)
SOURCE_S3_BUCKET: str = _text("SOURCE_S3_BUCKET", "de-source-data-bucket")
SOURCE_S3_REGION: str = _text("SOURCE_S3_REGION", "us-east-1")

# Metadata sheets
METADATA_S3_BUCKET: str = _text("METADATA_S3_BUCKET", "de-metadata-bucket")
METADATA_S3_KEY: str = _text("METADATA_S3_KEY", "")

# MinIO sink
MINIO_ENDPOINT: str = _text("MINIO_ENDPOINT", "http://127.0.0.1:9000")
MINIO_REGION: str = _text("MINIO_REGION", "us-east-1")
MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET: str = _text("MINIO_BUCKET", "de-data-lake")

# Supabase / PostgreSQL source (optional unless source_type=database)
SUPABASE_URL: str = _text("SUPABASE_URL", "")
SUPABASE_KEY: str = os.getenv("SUPABASE_KEY", "")
SUPABASE_JDBC_URL: str = _text("SUPABASE_JDBC_URL", "")
SUPABASE_DB_USER: str = _text("SUPABASE_DB_USER", "postgres")
SUPABASE_DB_PASSWORD: str = os.getenv("SUPABASE_DB_PASSWORD", "")

# DhanHQ / NSE data source
DHAN_CLIENT_ID: str = _text("DHAN_CLIENT_ID", "")
DHAN_ACCESS_TOKEN: str = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
DHAN_ACCESS_TOKEN_FILE: str = _text(
    "DHAN_ACCESS_TOKEN_FILE", "/run/secrets/dhan-access-token.json"
)
DHAN_API_KEY: str = os.getenv("DHAN_API_KEY", "").strip()
DHAN_API_SECRET: str = os.getenv("DHAN_API_SECRET", "").strip()
DHAN_REDIRECT_URL: str = _text("DHAN_REDIRECT_URL", "")
DHAN_PIN: str = os.getenv("DHAN_PIN", "").strip()
DHAN_TOTP_SECRET: str = os.getenv("DHAN_TOTP_SECRET", "").replace(" ", "").strip()
DHAN_INSTRUMENTS: str = _text("DHAN_INSTRUMENTS", "")
DHAN_SYMBOLS: str = _text("DHAN_SYMBOLS", "")
DHAN_SYMBOLS_FILE: str = _text("DHAN_SYMBOLS_FILE", "")
DHAN_INSTRUMENT_CACHE_FILE: str = _text(
    "DHAN_INSTRUMENT_CACHE_FILE", "/var/cache/dhan/instruments.csv"
)
DHAN_SNAPSHOT_MODE: str = _text("DHAN_SNAPSHOT_MODE", "quote").lower()
DHAN_HTTP_TIMEOUT_SECONDS: int = _positive_integer("DHAN_HTTP_TIMEOUT_SECONDS", 15)
DHAN_RAW_BUCKET: str = _text("DHAN_RAW_BUCKET", SOURCE_S3_BUCKET)
DHAN_RAW_PREFIX: str = _text("DHAN_RAW_PREFIX", "dhan/nse")
DHAN_FEED_MODE: str = _text("DHAN_FEED_MODE", "quote").lower()
DHAN_FLUSH_SECONDS: int = _positive_integer("DHAN_FLUSH_SECONDS", 5)
DHAN_BATCH_SIZE: int = _positive_integer("DHAN_BATCH_SIZE", 1000)
DHAN_SPOOL_DIR: str = _text("DHAN_SPOOL_DIR", "/var/lib/dhan-spool")

# Spark
SPARK_APP_NAME: str = _text("SPARK_APP_NAME", "DE-Metadata-Framework")
SPARK_MASTER: str = _text("SPARK_MASTER", "local[*]")
# Retained for the legacy config.py import. JARs are baked into the image.
SPARK_PACKAGES: str = _text("SPARK_PACKAGES", "")
HIVE_METASTORE_URI: str = _text(
    "HIVE_METASTORE_URI", "thrift://127.0.0.1:9083"
)

# Iceberg
ICEBERG_WAREHOUSE: str = _text(
    "ICEBERG_WAREHOUSE", "s3a://de-iceberg-warehouse-bucket/"
)
ICEBERG_DATA_BUCKET: str = _text("ICEBERG_DATA_BUCKET", "de-data-lake")
ICEBERG_METADATA_BUCKET: str = _text(
    "ICEBERG_METADATA_BUCKET", "de-iceberg-warehouse-bucket"
)
ICEBERG_CATALOG: str = _text("ICEBERG_CATALOG", "minio")
ICEBERG_DATABASE: str = _text("ICEBERG_DATABASE", "default")

# Medallion layers
LAKE_DATABASE: str = _text("LAKE_DATABASE", "de_lake")
BRONZE_DATABASE: str = _text("BRONZE_DATABASE", "de_bronze")
BRONZE_DATA_BUCKET: str = _text("BRONZE_DATA_BUCKET", "de-data-bronze")
TRANSFORMATION_DATABASE: str = _text(
    "TRANSFORMATION_DATABASE", "de_transformation"
)
TRANSFORMATION_DATA_BUCKET: str = _text(
    "TRANSFORMATION_DATA_BUCKET", "de-data-transformation"
)
SILVER_DATABASE: str = _text("SILVER_DATABASE", "de_silver")
SILVER_DATA_BUCKET: str = _text("SILVER_DATA_BUCKET", "de-data-silver")
GOLD_DATABASE: str = _text("GOLD_DATABASE", "de_gold")
GOLD_DATA_BUCKET: str = _text("GOLD_DATA_BUCKET", "de-data-gold")

# Logging
LOG_S3_BUCKET: str = _text("LOG_S3_BUCKET", "de-data-migration-logs")

# HashiCorp Vault
VAULT_ADDR: str = _text("VAULT_ADDR", "http://127.0.0.1:8200")
VAULT_TOKEN: str = os.getenv("VAULT_TOKEN", "").strip()
VAULT_NAMESPACE: str = _text("VAULT_NAMESPACE", "")
VAULT_PATH: str = _text("VAULT_PATH", "encryption/pii")

# Email notifications (optional, Brevo)
BREVO_API_KEY: str = os.getenv("BREVO_API_KEY", "").strip()
BREVO_FROM_EMAIL: str = _text("BREVO_FROM_EMAIL", "")
NOTIFY_EMAIL: str = _text("NOTIFY_EMAIL", "")
