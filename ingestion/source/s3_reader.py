from pyspark.sql import DataFrame, SparkSession
from ingestion.env.DE_Ingestion_properties import SOURCE_S3_BUCKET
from ingestion.pyfiles.logger import get_logger

logger = get_logger(__name__)

_SUPPORTED_FORMATS = ("csv", "json", "parquet")


def read_s3_file(spark: SparkSession, key: str, bucket: str | None = None) -> DataFrame:
    bucket = bucket or SOURCE_S3_BUCKET
    ext = key.rsplit(".", 1)[-1].lower()

    if ext not in _SUPPORTED_FORMATS:
        raise ValueError(f"Unsupported format '.{ext}'. Supported: {_SUPPORTED_FORMATS}")

    path = f"s3a://{bucket}/{key}"
    logger.info("Reading %s file", ext.upper())

    if ext == "csv":
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    elif ext == "json":
        df = spark.read.json(path)
    else:
        df = spark.read.parquet(path)

    logger.info("Loaded %d columns", len(df.columns))
    return df
