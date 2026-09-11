from argparse import Namespace
from ast import literal_eval

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import months, to_date, lit, year, month
from pyspark.sql.functions import partitioning

from ingestion.env.DE_Ingestion_properties import ICEBERG_CATALOG, ICEBERG_DATABASE, ICEBERG_DATA_BUCKET, ICEBERG_METADATA_BUCKET
from ingestion.pyfiles.iceberg_repair import drop_table_safe
from ingestion.pyfiles.logger import get_logger

logger = get_logger(__name__)


def write_to_minio(
    spark: SparkSession,
    df: DataFrame,
    args: Namespace,
    table_name: str,
    database: str | None = None,
    catalog: str | None = None,
    mode: str = "overwrite",
) -> None:
    database = database or ICEBERG_DATABASE
    catalog = catalog or ICEBERG_CATALOG
    full_table = f"{catalog}.{database}.{table_name}"

    app_name = args.application_name
    data_path    = f"s3a://{ICEBERG_DATA_BUCKET}/{app_name}"
    # "location" sets the Iceberg table's base directory in the HMS catalog entry.
    # Metadata files land at  {location}/metadata/*.json / *.avro
    # Data files are redirected to data_path via write.data.path.
    table_location = f"s3a://{ICEBERG_METADATA_BUCKET}/de_lake/{app_name}"
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
        print(d, type(d))
        for k, v in d.items():
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
    if mode.strip() == "append":
        writer.append()
    elif mode.strip() == "replace":
        drop_table_safe(spark, full_table, table_location)
        logger.info("Creating table from dataframe !!")
        writer.create()
    else:
        writer.overwritePartitions()

    logger.info("Write complete — table '%s'", full_table)
