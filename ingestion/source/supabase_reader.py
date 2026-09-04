from pyspark.sql import DataFrame, SparkSession
from ingestion.env.DE_Ingestion_properties import (
    SUPABASE_JDBC_URL,
    SUPABASE_DB_USER,
    SUPABASE_DB_PASSWORD,
)
from ingestion.pyfiles.logger import get_logger
from ingestion.pyfiles.vault_client import get_encrypt_value

logger = get_logger(__name__)


def read_supabase_table(
    spark: SparkSession,
    source_table_name: str,
    schema: str | None = None,
    columns: list[str] | None = None,
    filters: dict[str, str] | None = None,
) -> DataFrame:
    qualified_table = f"{schema}.{source_table_name}" if schema else source_table_name
    col_expr = ", ".join(columns) if columns else "*"
    where_clause = (
        " WHERE " + " AND ".join(f"{c} = '{v}'" for c, v in filters.items())
        if filters
        else ""
    )
    query = f"(SELECT {col_expr} FROM {qualified_table}{where_clause}) AS spark_tbl"

    logger.info("Reading database table '%s'", qualified_table)

    password = get_encrypt_value(
        SUPABASE_DB_PASSWORD,
        key_name="supabase-pwd",
        key_type="encryption-key",
        mount_path='transit'
    )
    df = (
        spark.read
        .format("jdbc")
        .option("url", SUPABASE_JDBC_URL)
        .option("dbtable", query)
        .option("user", SUPABASE_DB_USER)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    logger.info("Loaded %d columns from '%s'", len(df.columns), qualified_table)
    return df
