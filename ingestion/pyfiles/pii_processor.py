import pandas as pd
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import concat_ws, lit, col, sha2, md5
from pyspark.sql.types import StringType
from ingestion.env.DE_Ingestion_properties import SALT_KEY, VAULT_PATH
from ingestion.pyfiles.logger import get_logger
from ingestion.pyfiles.vault_client import get_kv_secret, get_transit_encryption_key

logger = get_logger(__name__)

_SALT_BYTES: bytes = SALT_KEY.encode("utf-8")
salt = _SALT_BYTES


def _make_hash_udf():
    _salt = salt  # capture in closure for Arrow batch serialisation

    @F.pandas_udf(StringType())
    def _hash_vec(s: pd.Series) -> pd.Series:
        import hmac, hashlib
        def _h(v):
            if v is None:
                return None
            return hmac.new(_salt, str(v).encode("utf-8"), hashlib.sha256).hexdigest()
        return s.apply(_h)

    return _hash_vec


def _make_mask_udf():
    @F.pandas_udf(StringType())
    def _mask_vec(s: pd.Series) -> pd.Series:
        def _m(v):
            if v is None:
                return None
            t = str(v)
            return "****" if len(t) <= 4 else t[:2] + "*" * (len(t) - 4) + t[-2:]
        return s.apply(_m)

    return _mask_vec


def apply_pii(df: DataFrame, metadata_df: DataFrame) -> DataFrame:
    hash_udf = _make_hash_udf()
    mask_udf = _make_mask_udf()

    pii_rows = (
        metadata_df
        .filter(F.lower(F.col("security_level")).isin("hash", "pii"))
        .select("column_name", "security_level")
        .collect()
    )

    # Most operational/market datasets contain no PII. Avoid making Vault a
    # hidden hard dependency when the metadata asks for no protected columns.
    if not pii_rows:
        logger.info("No PII transformations requested")
        return df
    if not SALT_KEY or SALT_KEY.startswith("REQUIRED_") or SALT_KEY.startswith("change-me"):
        raise ValueError("Set a strong SALT_KEY before processing protected columns")

    needs_encryption = any(
        str(row["security_level"]).strip().lower() == "pii" for row in pii_rows
    )
    salt_2 = None
    encrypt_key = None
    if needs_encryption:
        salt_2 = get_kv_secret(VAULT_PATH, "salt_2", "secret", 2)
        if not salt_2 or str(salt_2).startswith("REQUIRED_"):
            raise ValueError("Set a strong SALT_2 before encrypting PII columns")
        encrypt_key = get_transit_encryption_key()

    for row in pii_rows:
        column = row["column_name"]
        level = str(row["security_level"]).strip().lower()

        if column not in df.columns:
            continue


        if level == "hash":
            df = df.withColumn(column, hash_udf(F.col(column).cast(StringType())))
            logger.info("Hashed column '%s'", column)
        elif level == "pii":
            # Vault exports AES-256 key material as base64. Store Spark's binary
            # AES-GCM result as base64 so downstream text-oriented sinks remain
            # portable and the encrypted value can be decrypted later.
            encrypted = F.base64(
                F.aes_encrypt(
                    concat_ws(
                        "|",
                        sha2(lit(SALT_KEY), 256),
                        md5(lit(salt_2)),
                        col(column).cast(StringType()),
                    ).cast("binary"),
                    F.unbase64(lit(encrypt_key)),
                    lit("GCM"),
                )
            )
            df = df.withColumn(
                column,
                F.when(col(column).isNull(), F.lit(None).cast("string")).otherwise(encrypted),
            )
            logger.info("Encrypted column '%s'", column)

    return df
