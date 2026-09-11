import os
from dotenv import load_dotenv

load_dotenv()

# ── PII Encryption (HMAC-SHA256 salt) ─────────────────────────────────────────
SALT_KEY: str = os.getenv("SALT_KEY", "change-me-generate-a-32-char-random-string")

# ── Source S3 (MinIO-compatible) ──────────────────────────────────────────────
SOURCE_S3_ENDPOINT: str   = os.getenv("SOURCE_S3_ENDPOINT", "http://127.0.0.1:9000 ")
SOURCE_S3_ACCESS_KEY: str = os.getenv("SOURCE_S3_ACCESS_KEY", "minioadmin")
SOURCE_S3_SECRET_KEY: str = os.getenv("SOURCE_S3_SECRET_KEY", "minioadmin")
SOURCE_S3_BUCKET: str     = os.getenv("SOURCE_S3_BUCKET", "de-source-data-bucket")
METADATA_S3_BUCKET: str = os.getenv("METADATA_S3_BUCKET")

# ── MinIO (Sink — S3-compatible) ───────────────────────────────────────────────
MINIO_ENDPOINT: str   = os.getenv("MINIO_ENDPOINT", "http://127.0.0.1:9000 ")
MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET: str     = os.getenv("MINIO_BUCKET", "de-data-lake")

# ── Supabase (REST + JDBC) ────────────────────────────────────────────────────
SUPABASE_URL: str         = os.getenv("SUPABASE_URL", "https://your-project-ref.supabase.co")
SUPABASE_KEY: str         = os.getenv("SUPABASE_KEY", "your-anon-or-service-role-key")
SUPABASE_JDBC_URL: str    = os.getenv(
    "SUPABASE_JDBC_URL",
    "jdbc:postgresql://db.your-project-ref.supabase.co:5432/postgres",
)
SUPABASE_DB_USER: str     = os.getenv("SUPABASE_DB_USER", "postgres")
SUPABASE_DB_PASSWORD: str = os.getenv("SUPABASE_DB_PASSWORD", "your-db-password")


# ── Spark ──────────────────────────────────────────────────────────────────────
SPARK_APP_NAME: str = os.getenv("SPARK_APP_NAME", "DE-Metadata-Framework")
SPARK_MASTER: str   = os.getenv("SPARK_MASTER", "local[*]")

# ── Iceberg ────────────────────────────────────────────────────────────────────
ICEBERG_WAREHOUSE: str        = os.getenv("ICEBERG_WAREHOUSE", "s3a://de-iceberg-warehouse-bucket/")
ICEBERG_DATA_BUCKET: str      = os.getenv("ICEBERG_DATA_BUCKET", "de-data-lake")
ICEBERG_METADATA_BUCKET: str  = os.getenv("ICEBERG_METADATA_BUCKET", "de-iceberg-warehouse-bucket")
ICEBERG_CATALOG: str   = os.getenv("ICEBERG_CATALOG", "minio")
ICEBERG_DATABASE: str  = os.getenv("ICEBERG_DATABASE", "default")

# ── Medallion layers ───────────────────────────────────────────────────────────
LAKE_DATABASE: str            = os.getenv("LAKE_DATABASE",            "de_lake")
BRONZE_DATABASE: str          = os.getenv("BRONZE_DATABASE",          "de_bronze")
BRONZE_DATA_BUCKET: str       = os.getenv("BRONZE_DATA_BUCKET",       "de-data-bronze")
TRANSFORMATION_DATABASE: str  = os.getenv("TRANSFORMATION_DATABASE",  "de_transformation")

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_S3_BUCKET: str = os.getenv("LOG_S3_BUCKET", "de-data-migration-logs")

# ── HashiCorp Vault ────────────────────────────────────────────────────────────
VAULT_ADDR: str      = os.getenv("VAULT_ADDR", "http://127.0.0.1:8200")
VAULT_TOKEN: str     = os.getenv("VAULT_TOKEN", "")
VAULT_NAMESPACE: str = os.getenv("VAULT_NAMESPACE", "")
VAULT_PATH: str      = os.getenv("VAULT_PATH")

# ── Email notifications (Brevo) ────────────────────────────────────────────────
BREVO_API_KEY: str    = os.getenv("BREVO_API_KEY", "")
BREVO_FROM_EMAIL: str = os.getenv("BREVO_FROM_EMAIL", "")
NOTIFY_EMAIL: str     = os.getenv("NOTIFY_EMAIL", "")