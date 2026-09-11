from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import partitioning
from pyspark.sql.window import Window

from ingestion.env.DE_Ingestion_properties import (
    ICEBERG_CATALOG, ICEBERG_METADATA_BUCKET,
    BRONZE_DATA_BUCKET, BRONZE_DATABASE, LAKE_DATABASE,
)
from ingestion.pyfiles.iceberg_repair import table_exists_safe
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
    full_table     = f"{catalog}.{bronze_db}.{app_name}"
    data_path      = f"s3a://{BRONZE_DATA_BUCKET}/{app_name}"
    # "location" sets the Iceberg table's base directory in the HMS catalog entry.
    # Metadata files land at  {location}/metadata/*.json / *.avro
    # Data files are redirected to data_path via write.data.path.
    table_location = f"s3a://{ICEBERG_METADATA_BUCKET}/de_bronze/{app_name}"

    logger.info(
        "Writing bronze Iceberg table '%s' (location=%s, data=%s)",
        full_table, table_location, data_path,
    )

    writer = (
        df.writeTo(full_table)
        .using("iceberg")
        .tableProperty("location", table_location)
        .tableProperty("write.data.path", data_path)
        .partitionedBy(partitioning.days("snapshot_date"))
    )

    if table_exists_safe(spark, full_table, table_location):
        writer.overwritePartitions()
    else:
        logger.info("Creating table for the first Time !!!")
        writer.create()
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
    lake_table   = f"{catalog}.{lake_db}.{app_name}"
    bronze_table = f"{catalog}.{bronze_db}.{app_name}"
    logger.info("=== Bronze FULL [app=%s, date=%s] ===", app_name, run_date)
    logger.info("Lake table  : %s  (ingest_date = %s)", lake_table, run_date)
    logger.info("Bronze table: %s  (snapshot_date = %s)", bronze_table, run_date)

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
    logger.info("Lake table  : %s  (ingest_date = %s)", lake_table, run_date)

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{bronze_db}")

    # Step 1: read and dedup incoming lake batch for run_date
    df_new = (
        spark.table(lake_table)
        .filter(F.col("ingest_date") == F.to_date(F.lit(run_date)))
        .drop("ingest_date")
    )
    logger.info("Lake rows for run_date=%s: %d", run_date, df_new.count())
    df_new = _dedup(df_new, primary_keys, timestamp_col)

    # Step 2: first-time run — bronze table doesn't exist yet, fall back to full write
    if not _bronze_table_exists(spark, bronze_table):
        logger.info("Bronze table does not exist yet — running initial full write")
        df_new = df_new.withColumn("snapshot_date", F.to_date(F.lit(run_date)))
        _write_bronze(spark, df_new, app_name, catalog, bronze_db)
        logger.info("=== Bronze DELTA (initial) complete [app=%s, date=%s] ===", app_name, run_date)
        return

    # Step 3: find the latest snapshot strictly before run_date
    row = spark.sql(f"""
        SELECT MAX(snapshot_date) AS max_snap
        FROM {bronze_table}
        WHERE snapshot_date < '{run_date}'
    """).collect()[0]
    max_snap = row["max_snap"]

    logger.info("Bronze table: %s  (snapshot_date = %s)", bronze_table, max_snap)


    if max_snap is None:
        logger.info("No previous snapshot before %s — writing new partition as full snapshot", run_date)
        df_new = df_new.withColumn("snapshot_date", F.to_date(F.lit(run_date)))
        _write_bronze(spark, df_new, app_name, catalog, bronze_db)
        logger.info("=== Bronze DELTA (no prior snapshot) complete [app=%s, date=%s] ===", app_name, run_date)
        return

    # Step 4: read the previous snapshot and merge with the new lake batch
    #   New lake records take priority over the previous snapshot on primary keys.
    logger.info("Reading previous bronze snapshot at snapshot_date=%s", max_snap)
    df_prev = (
        spark.table(bronze_table)
        .filter(F.col("snapshot_date") == F.lit(str(max_snap)))
        .drop("snapshot_date")
    )
    logger.info("Previous snapshot rows: %d", df_prev.count())

    if primary_keys:
        # Tag sources: 0 = new (wins), 1 = previous (loses on conflict)
        df_union = (
            df_new.withColumn("_priority", F.lit(0))
            .unionByName(df_prev.withColumn("_priority", F.lit(1)))
        )
        w = Window.partitionBy(*primary_keys).orderBy(F.col("_priority"))
        df_merged = (
            df_union
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn", "_priority")
        )
    else:
        logger.warning("No primary_key columns — merging by distinct union")
        df_merged = df_new.unionByName(df_prev).distinct()

    df_merged = df_merged.withColumn("snapshot_date", F.to_date(F.lit(run_date)))
    logger.info("Merged snapshot rows: %d", df_merged.count())

    _write_bronze(spark, df_merged, app_name, catalog, bronze_db)
    logger.info("=== Bronze DELTA complete [app=%s, date=%s] ===", app_name, run_date)
