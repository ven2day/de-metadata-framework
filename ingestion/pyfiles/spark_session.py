from pyspark.sql import SparkSession
from ingestion.env.DE_Ingestion_properties import (
    HIVE_METASTORE_URI,
    ICEBERG_CATALOG,
    ICEBERG_WAREHOUSE,
    METADATA_S3_BUCKET,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_REGION,
    MINIO_SECRET_KEY,
    SOURCE_S3_ACCESS_KEY,
    SOURCE_S3_ENDPOINT,
    SOURCE_S3_REGION,
    SOURCE_S3_SECRET_KEY,
    SPARK_APP_NAME,
    SPARK_MASTER,
)
from ingestion.pyfiles.logger import get_logger

logger = get_logger(__name__)

_SESSION: SparkSession | None = None


def get_spark_session() -> SparkSession:
    global _SESSION
    if _SESSION is not None:
        return _SESSION

    logger.info("Building SparkSession")

    catalog_prefix = f"spark.sql.catalog.{ICEBERG_CATALOG}"
    metadata_prefix = f"spark.hadoop.fs.s3a.bucket.{METADATA_S3_BUCKET}"
    builder = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.ui.showConsoleProgress", "false")
        .config(catalog_prefix, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{catalog_prefix}.type", "hive")
        .config(f"{catalog_prefix}.uri", HIVE_METASTORE_URI)
        .config(f"{catalog_prefix}.warehouse", ICEBERG_WAREHOUSE)
        .config(f"{catalog_prefix}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{catalog_prefix}.s3.endpoint", MINIO_ENDPOINT)
        .config(f"{catalog_prefix}.client.region", MINIO_REGION)
        .config(f"{catalog_prefix}.s3.path-style-access", "true")
        .config(f"{catalog_prefix}.s3.access-key-id", MINIO_ACCESS_KEY)
        .config(f"{catalog_prefix}.s3.secret-access-key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", SOURCE_S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", SOURCE_S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", SOURCE_S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint.region", SOURCE_S3_REGION)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(SOURCE_S3_ENDPOINT.startswith("https://")).lower())
        .config(f"{metadata_prefix}.endpoint", MINIO_ENDPOINT)
        .config(f"{metadata_prefix}.endpoint.region", MINIO_REGION)
        .config(f"{metadata_prefix}.access.key", MINIO_ACCESS_KEY)
        .config(f"{metadata_prefix}.secret.key", MINIO_SECRET_KEY)
        .config(f"{metadata_prefix}.path.style.access", "true")
        .config(
            f"{metadata_prefix}.connection.ssl.enabled",
            str(MINIO_ENDPOINT.startswith("https://")).lower(),
        )
    )
    _SESSION = builder.getOrCreate()

    _SESSION.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created [appId=%s]", _SESSION.sparkContext.applicationId)
    return _SESSION


def get_active_session() -> SparkSession | None:
    return _SESSION
