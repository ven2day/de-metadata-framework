from pyspark.sql import DataFrame, SparkSession, functions as F
from ingestion.env.DE_Ingestion_properties import METADATA_S3_BUCKET, METADATA_S3_KEY
from ingestion.pyfiles.logger import get_logger

logger = get_logger(__name__)

_REQUIRED_COLS = {"column_name", "datatype"}


def read_metadata(
    spark: SparkSession,
    bucket: str | None = None,
    key: str | None = None,
) -> DataFrame:
    bucket = bucket or METADATA_S3_BUCKET
    key = key or METADATA_S3_KEY
    if not bucket:
        raise ValueError("Metadata bucket is required (set METADATA_S3_BUCKET)")
    if not key:
        raise ValueError("Metadata key is required (pass --metadata-key or set METADATA_S3_KEY)")
    path = f"s3a://{bucket}/{key}"

    logger.info("Reading metadata sheet")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
    )

    missing_cols = _REQUIRED_COLS - set(df.columns)
    if missing_cols:
        raise ValueError(f"Metadata sheet missing required columns: {sorted(missing_cols)}")

    raw_names = [
        str(row["column_name"] or "").strip()
        for row in df.select("column_name").collect()
    ]
    if any(not name for name in raw_names):
        raise ValueError("Metadata sheet contains a blank column_name")
    lowered = [name.lower() for name in raw_names]
    duplicates = sorted({name for name in lowered if lowered.count(name) > 1})
    if duplicates:
        raise ValueError(
            f"Metadata sheet contains duplicate column definitions: {duplicates}"
        )

    df = (
        df
        .withColumn("column_name", F.trim(F.col("column_name")))
        .withColumn("datatype", F.lower(F.trim(F.col("datatype"))))
    )

    if "security_level" in df.columns:
        df = df.withColumn(
            "security_level",
            F.lower(F.trim(F.coalesce(F.col("security_level"), F.lit("none"))))
        )
    else:
        df = df.withColumn("security_level", F.lit("none"))

    if "data_length" in df.columns:
        df = df.withColumn("data_length", F.col("data_length").cast("int"))

    if "data_precision" in df.columns:
        df = df.withColumn("data_precision", F.col("data_precision").cast("int"))

    count = df.count()
    logger.info("Loaded metadata with %d column definitions", count)
    return df
