from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ingestion.env.DE_Ingestion_properties import (
    ICEBERG_CATALOG, ICEBERG_METADATA_BUCKET,
    BRONZE_DATA_BUCKET, BRONZE_DATABASE, LAKE_DATABASE,
)
from ingestion.pyfiles.logger import get_logger

logger = get_logger(__name__)


def extract_bronze_keys(metadata_df: DataFrame) -> tuple[list[str], str | None]:
    """Extract primary key column names and optional timestamp column from metadata."""
    truthy = {"y", "yes", "true", "1"}

    primary_keys: list[str] = []
    if "primary_key" in metadata_df.columns:
        rows = (
            metadata_df
            .filter(F.lower(F.trim(F.col("primary_key"))).isin(*truthy))
            .select("column_name")
            .collect()
        )
        primary_keys = [r["column_name"] for r in rows]

    timestamp_col: str | None = None
    if "is_timestamp" in metadata_df.columns:
        rows = (
            metadata_df
            .filter(F.lower(F.trim(F.col("is_timestamp"))).isin(*truthy))
            .select("column_name")
            .collect()
        )
        timestamp_col = rows[0]["column_name"] if rows else None

    return primary_keys, timestamp_col


def _dedup(df: DataFrame, primary_keys: list[str], timestamp_col: str | None) -> DataFrame:
    if not primary_keys:
        logger.warning("No primary_key columns defined — returning distinct rows")
        return df.distinct()

    if timestamp_col and timestamp_col in df.columns:
        logger.info("Deduplicating by primary_keys=%s ordered by %s desc (keep latest)", primary_keys, timestamp_col)
        w = Window.partitionBy(*primary_keys).orderBy(F.col(timestamp_col).desc())
        return (
            df.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

    logger.info("Deduplicating distinct rows by primary_keys=%s", primary_keys)
    return df.dropDuplicates(primary_keys)


def _write_bronze(
    spark: SparkSession,
    df: DataFrame,
    app_name: str,
    catalog: str,
    bronze_db: str,
) -> None:
    full_table = f"{catalog}.{bronze_db}.{app_name}"
    data_path  = f"s3a://{BRONZE_DATA_BUCKET}/{app_name}"
    meta_path  = f"s3a://{ICEBERG_METADATA_BUCKET}/bronze/{app_name}"

    logger.info("Writing bronze Iceberg table '%s' (data=%s, meta=%s)", full_table, data_path, meta_path)

    writer = (
        df.writeTo(full_table)
        .using("iceberg")
        .tableProperty("write.data.path", data_path)
        .tableProperty("write.meta.path", meta_path)
        .partitionedBy(F.months("snapshot_date"))
    )

    if spark.catalog.tableExists(full_table):
       writer.overwritePartitions()
    else:
        logger.info("Creating table for the first Time !!!")
        writer.createOrReplace()
    logger.info("Write complete — bronze table '%s'", full_table)


def run_full(
    spark: SparkSession,
    app_name: str,
    run_date: str,
    primary_keys: list[str],
    timestamp_col: str | None,
    catalog: str = ICEBERG_CATALOG,
    lake_db: str = LAKE_DATABASE,
    bronze_db: str = BRONZE_DATABASE,
) -> None:
    lake_table = f"{catalog}.{lake_db}.{app_name}"
    logger.info("=== Bronze FULL [app=%s, date=%s, source=%s] ===", app_name, run_date, lake_table)

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{bronze_db}")

    df = (
        spark.table(lake_table)
        .filter(F.col("ingest_date") == F.to_date(F.lit(run_date)))
        .drop("ingest_date")
    )
    logger.info("Lake rows for run_date=%s: %d", run_date, df.count())

    df = _dedup(df, primary_keys, timestamp_col)
    df = df.withColumn("snapshot_date", F.to_date(F.lit(run_date)))

    _write_bronze(spark,df, app_name, catalog, bronze_db)
    logger.info("=== Bronze FULL complete [app=%s, date=%s] ===", app_name, run_date)


def _bronze_table_exists(spark: SparkSession, bronze_table: str) -> bool:
    try:
        spark.table(bronze_table).limit(0)
        return True
    except Exception:
        return False


def run_delta(
    spark: SparkSession,
    app_name: str,
    run_date: str,
    primary_keys: list[str],
    timestamp_col: str | None,
    catalog: str = ICEBERG_CATALOG,
    lake_db: str = LAKE_DATABASE,
    bronze_db: str = BRONZE_DATABASE,
) -> None:
    lake_table   = f"{catalog}.{lake_db}.{app_name}"
    bronze_table = f"{catalog}.{bronze_db}.{app_name}"
    logger.info("=== Bronze DELTA [app=%s, date=%s] ===", app_name, run_date)

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{bronze_db}")

    # Step 1: read and dedup incoming lake batch for run_date
    df_new = (
        spark.table(lake_table)
        .filter(F.col("ingest_date") == F.to_date(F.lit(run_date)))
        .drop("ingest_date")
    )
    logger.info("Lake rows for run_date=%s: %d", run_date, df_new.count())

    df_new = _dedup(df_new, primary_keys, timestamp_col)
    df_new = df_new.withColumn("snapshot_date", F.to_date(F.lit(run_date)))

    # Step 2: first-time run — bronze table doesn't exist yet, fall back to full write
    if not _bronze_table_exists(spark, bronze_table):
        logger.info("Bronze table does not exist yet — running initial full write")
        _write_bronze(spark, df_new, app_name, catalog, bronze_db)
        logger.info("=== Bronze DELTA (initial) complete [app=%s, date=%s] ===", app_name, run_date)
        return

    # Step 3: MERGE INTO existing bronze table
    #   MATCHED     → UPDATE all columns (snapshot_date moves to run_date)
    #   NOT MATCHED → INSERT new row
    view = f"_bronze_src_{app_name}_{run_date.replace('-', '_')}"
    df_new.createOrReplaceTempView(view)

    if not primary_keys:
        raise ValueError(f"MERGE INTO requires at least one primary_key — none defined in metadata for '{app_name}'")

    on_clause = " AND ".join(f"target.`{pk}` = source.`{pk}`" for pk in primary_keys)

    merge_sql = f"""
        MERGE INTO {bronze_table} AS target
        USING {view} AS source
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """

    logger.info("MERGE INTO '%s' on primary_keys=%s", bronze_table, primary_keys)
    spark.sql(merge_sql)
    logger.info("MERGE INTO complete — '%s'", bronze_table)
    logger.info("=== Bronze DELTA complete [app=%s, date=%s] ===", app_name, run_date)
