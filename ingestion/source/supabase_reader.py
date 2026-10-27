import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from ingestion.env.DE_Ingestion_properties import (
    SUPABASE_JDBC_URL,
    SUPABASE_DB_USER,
    SUPABASE_DB_PASSWORD,
)
from ingestion.pyfiles.logger import get_logger
from ingestion.pyfiles.vault_client import get_encrypt_value

logger = get_logger(__name__)
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def read_supabase_table(
    spark: SparkSession,
    source_table_name: str,
    schema: str | None = None,
    columns: list[str] | None = None,
    filters: dict[str, str] | None = None,
) -> DataFrame:
    identifiers = [source_table_name]
    if schema:
        identifiers.append(schema)
    identifiers.extend(columns or [])
    identifiers.extend((filters or {}).keys())
    invalid = [value for value in identifiers if not _IDENTIFIER.fullmatch(value)]
    if invalid:
        raise ValueError(f"Invalid PostgreSQL identifier(s): {invalid}")
    qualified_table = f'"{schema}"."{source_table_name}"' if schema else f'"{source_table_name}"'

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
        .option("dbtable", qualified_table)
        .option("user", SUPABASE_DB_USER)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    if columns:
        df = df.select(*[F.col(name) for name in columns])
    for name, value in (filters or {}).items():
        df = df.filter(F.col(name) == F.lit(value))
    logger.info("Loaded %d columns from '%s'", len(df.columns), qualified_table)
    return df
