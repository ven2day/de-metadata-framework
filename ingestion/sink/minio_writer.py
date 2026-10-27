from argparse import Namespace
from ast import literal_eval
import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import to_date, lit
from pyspark.sql.functions import partitioning

from ingestion.env.DE_Ingestion_properties import ICEBERG_CATALOG, ICEBERG_DATABASE, ICEBERG_DATA_BUCKET, ICEBERG_METADATA_BUCKET
from ingestion.pyfiles.iceberg_repair import drop_table_safe, table_exists_safe
from ingestion.pyfiles.logger import get_logger

logger = get_logger(__name__)


def write_to_minio(
    spark: SparkSession,
    df: DataFrame,
    args: Namespace,
    table_name: str | None,
    database: str | None = None,
    catalog: str | None = None,
    mode: str = "overwrite",
) -> None:
    database = database or ICEBERG_DATABASE
    catalog = catalog or ICEBERG_CATALOG
    table_name = table_name or args.application_name
    identifier = re.compile(r"^[a-z_][a-z0-9_]*$")
    for label, value in (
        ("catalog", catalog),
        ("database", database),
        ("table", table_name),
        ("application", args.application_name),
    ):
        if not identifier.fullmatch(value):
            raise ValueError(f"Invalid Iceberg {label} identifier: {value!r}")
    full_table = f"{catalog}.{database}.{table_name}"

    data_bucket = args.output_bucket or ICEBERG_DATA_BUCKET
    bucket_pattern = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
    for label, value in (
        ("data bucket", data_bucket),
        ("metadata bucket", ICEBERG_METADATA_BUCKET),
    ):
        if not bucket_pattern.fullmatch(value):
            raise ValueError(f"Invalid S3 {label}: {value!r}")
    mode = mode.strip().lower()
    if mode not in {"append", "overwrite", "replace"}:
        raise ValueError(f"Invalid Iceberg write mode: {mode!r}")
    if df.isEmpty():
        raise ValueError(
            "Refusing to publish an empty Lake batch; verify the source and requested date"
        )
    physical_name = f"{catalog}/{database}/{table_name}"
    data_path = f"s3a://{data_bucket}/{physical_name}"
    # "location" sets the Iceberg table's base directory in the HMS catalog entry.
    # Metadata files land at  {location}/metadata/*.json / *.avro
    # Data files are redirected to data_path via write.data.path.
    table_location = f"s3a://{ICEBERG_METADATA_BUCKET}/iceberg/{physical_name}"
    logger.info(
        "Writing Iceberg table '%s' (mode=%s, location=%s, data=%s)",
        full_table, mode, table_location, data_path,
    )

    # HMS requires the namespace to be registered before a table can be created in it.
    # This is a no-op if the namespace already exists.
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{database}")

    df = df.withColumn(
        "ingest_date",
        to_date(lit(args.ingest_date))
    )
    d = {}

    if args.partition is not None:
        logger.info("Added partition columns")
        d = literal_eval(args.partition)
        if not isinstance(d, dict):
            raise ValueError("--partition must be a Python/JSON-style mapping")
        for k, v in d.items():
            if not identifier.fullmatch(k):
                raise ValueError(f"Invalid partition column name: {k!r}")
            df = df.withColumn(
                k,
                lit(v)
            )

    writer = (
        df.writeTo(full_table)
        .using("iceberg")
        .tableProperty("location", table_location)
        .tableProperty("write.data.path", data_path)
        .partitionedBy(*d.keys(), partitioning.days("ingest_date"))
    )
    table_exists = table_exists_safe(spark, full_table, table_location)
    if mode == "append":
        if table_exists:
            writer.append()
        else:
            logger.info("Creating table before first append")
            writer.create()
    elif mode == "replace":
        if table_exists:
            drop_table_safe(spark, full_table, table_location)
        logger.info("Creating replacement table from dataframe")
        writer.create()
    else:
        if table_exists:
            writer.overwritePartitions()
        else:
            logger.info("Creating table before first partition overwrite")
            writer.create()

    logger.info("Write complete — table '%s'", full_table)
