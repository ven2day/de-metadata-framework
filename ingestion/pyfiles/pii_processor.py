from pyspark.sql import DataFrame, functions as F
from pyspark.sql.connect.functions import aes_encrypt
from pyspark.sql.functions import concat_ws, lit, col, sha2, md5
from pyspark.sql.types import StringType
from ingestion.env.DE_Ingestion_properties import SALT_KEY, VAULT_PATH
from ingestion.pyfiles.logger import get_logger
from ingestion.pyfiles.vault_client import get_kv_secret, get_transit_encryption_key

logger = get_logger(__name__)

_SALT_BYTES: bytes = SALT_KEY.encode("utf-8")
salt = _SALT_BYTES


def _make_hash_udf():


    def _hash(value):
        if value is None:
            return None
        import hmac, hashlib
        return hmac.new(salt, str(value).encode("utf-8"), hashlib.sha256).hexdigest()

    return F.udf(_hash, StringType())


def _make_mask_udf():
    def mask(value):
        if value is None:
            return None
        s = str(value)
        if len(s) <= 4:
            return "****"
        return s[:2] + "*" * (len(s) - 4) + s[-2:]

    return F.udf(mask, StringType())


def apply_pii(df: DataFrame, metadata_df: DataFrame) -> DataFrame:
    hash_udf = _make_hash_udf()
    mask_udf = _make_mask_udf()

    pii_rows = (
        metadata_df
        .filter(F.lower(F.col("security_level")).isin("hash", "pii"))
        .select("column_name", "security_level")
        .collect()
    )

    salt_2 = get_kv_secret(VAULT_PATH, 'salt_2', 'secret', 2)
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
            df = df.withColumn(
                column,
                aes_encrypt(
                concat_ws('|', sha2(lit(salt)), md5(lit(salt_2)), col(column))
                ),
                encrypt_key
            )
            # df = df.withColumn(column, mask_udf(col(column).cast(StringType())))
            logger.info("Masked column '%s'", column)

    return df
