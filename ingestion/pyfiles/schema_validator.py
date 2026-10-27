from pyspark.sql import DataFrame
from ingestion.pyfiles.logger import get_logger

logger = get_logger(__name__)


def validate_schema(df: DataFrame, metadata_df: DataFrame) -> None:
    df_cols   = {c.lower() for c in df.columns}
    meta_cols = {row["column_name"].strip().lower() for row in metadata_df.select("column_name").collect()}

    missing_in_df = meta_cols - df_cols
    extra_in_df = df_cols - meta_cols

    if missing_in_df:
        logger.error(
            "Schema mismatch — columns in metadata but ABSENT from dataframe: %s",
            sorted(missing_in_df),
        )
    if extra_in_df:
        logger.error(
            "Schema mismatch — columns in dataframe but NOT defined in metadata: %s",
            sorted(extra_in_df),
        )

    if missing_in_df or extra_in_df:
        raise ValueError(
            f"Schema validation failed. "
            f"Missing from dataframe: {sorted(missing_in_df)}. "
            f"Extra in dataframe (no metadata mapping): {sorted(extra_in_df)}."
        )

    metadata_names = {
        row["column_name"].strip().lower(): row["column_name"].strip()
        for row in metadata_df.select("column_name").collect()
    }
    case_mismatches = sorted(
        column for column in df.columns if metadata_names[column.lower()] != column
    )
    if case_mismatches:
        raise ValueError(
            "Schema validation is case-sensitive for casting and PII rules. "
            f"Rename these dataframe columns to match metadata exactly: {case_mismatches}"
        )

    logger.info("Schema validation passed — %d columns matched", len(df_cols))
