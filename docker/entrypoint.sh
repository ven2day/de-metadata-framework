#!/bin/sh
set -e

# 1. Substitute MinIO endpoint and credentials in baked Spark config
MINIO_HOST="${MINIO_ENDPOINT:-http://127.0.0.1:9000}"
sed -i "s|http://127.0.0.1:9000|${MINIO_HOST}|g"             /app/conf/spark-defaults.conf
sed -i "s|MINIO_ACCESS_KEY_PLACEHOLDER|${MINIO_ACCESS_KEY}|g" /app/conf/spark-defaults.conf
sed -i "s|MINIO_SECRET_KEY_PLACEHOLDER|${MINIO_SECRET_KEY}|g" /app/conf/spark-defaults.conf

# 2. Load Vault pipeline token — .env wins; vault_secrets volume is fallback only
if [ -z "$VAULT_TOKEN" ] && [ -f "/vault/secrets/pipeline_token" ]; then
    VAULT_TOKEN="$(cat /vault/secrets/pipeline_token)"
    export VAULT_TOKEN
fi

# 3. Load vault-encrypted Supabase password from shared secrets volume (overrides .env)
if [ -f "/vault/secrets/supabase_db_password_ciphertext" ]; then
    SUPABASE_DB_PASSWORD="$(cat /vault/secrets/supabase_db_password_ciphertext)"
    export SUPABASE_DB_PASSWORD
fi

exec spark-submit \
    --py-files /app/ingestion.zip \
    /app/ingestion/main/pipeline.py \
    "$@"
