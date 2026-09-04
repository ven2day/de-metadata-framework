import json
from argparse import Namespace
from ast import literal_eval

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import months, to_date, lit, year, month, days

from ingestion.env.DE_Ingestion_properties import ICEBERG_CATALOG, ICEBERG_DATABASE
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

    logger.info("Writing Iceberg table '%s' (mode=%s)", full_table, mode)

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

    writer = df.writeTo(full_table).using("iceberg").partitionedBy(*d.keys(), days("ingest_date"))
    if mode.strip() == "append":
        writer.append()
    elif mode.strip() == "replace":
        try:
            spark.sql(f"""drop table {full_table}""")
        except Exception as e:
            logger.info("Table does not exists !!")
        finally:
            logger.info("Creating table from dataframe !!")
        writer.createOrReplace()
    else:
        writer.overwritePartitions()

    logger.info("Write complete — table '%s'", full_table)
