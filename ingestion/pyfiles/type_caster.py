from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DoubleType,
    FloatType, BooleanType, DateType, TimestampType, DecimalType,
)
from ingestion.pyfiles.logger import get_logger

logger = get_logger(__name__)

_TYPE_MAP = {
    "string":    StringType(),
    "str":       StringType(),
    "varchar":   StringType(),
    "text":      StringType(),
    "char":      StringType(),
    "integer":   IntegerType(),
    "int":       IntegerType(),
    "smallint":  IntegerType(),
    "bigint":    LongType(),
    "long":      LongType(),
    "double":    DoubleType(),
    "numeric":   DoubleType(),
    "float":     FloatType(),
    "real":      FloatType(),
    "boolean":   BooleanType(),
    "bool":      BooleanType(),
    "date":      DateType(),
    "timestamp": TimestampType(),
    "datetime":  TimestampType(),
}

_DECIMAL_TYPES = {"decimal"}


def cast_columns(df: DataFrame, metadata_df: DataFrame) -> DataFrame:
    has_length    = "data_length" in metadata_df.columns
    has_precision = "data_precision" in metadata_df.columns

    select_cols = ["column_name", "datatype"]
    if has_length:
        select_cols.append("data_length")
    if has_precision:
        select_cols.append("data_precision")

    meta_rows = metadata_df.select(*select_cols).collect()

    for row in meta_rows:
        col_name = row["column_name"]
        raw_type = str(row["datatype"]).strip().lower()

        if col_name not in df.columns:
            continue

        if raw_type in _DECIMAL_TYPES and has_length and has_precision:
            length    = row["data_length"]
            precision = row["data_precision"]
            target_type = (
                DecimalType(int(length), int(precision))
                if length is not None and precision is not None
                else DoubleType()
            )
        else:
            target_type = _TYPE_MAP.get(raw_type)

        if target_type is None:
            logger.warning("Unknown datatype '%s' for column '%s' — skipping", raw_type, col_name)
            continue

        try:
            df = df.withColumn(col_name, F.col(col_name).cast(target_type))
            logger.debug("Cast '%s' → %s", col_name, type(target_type).__name__)
        except Exception as exc:
            logger.error("Failed to cast '%s' to %s: %s", col_name, raw_type, exc)
            raise

    return df
