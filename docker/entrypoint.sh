#!/bin/sh
set -e

# No-op when started by docker compose up with no arguments
if [ $# -eq 0 ]; then
    echo "Pipeline image ready. Use: docker compose run --rm pipeline --application-name <name> ..."
    exit 0
fi

# 1. Substitute MinIO endpoint and credentials in baked Spark config
MINIO_HOST="${MINIO_ENDPOINT:-http://127.0.0.1:9000}"
sed -i "s|http://127.0.0.1:9000|${MINIO_HOST}|g"             /app/conf/spark-defaults.conf
sed -i "s|MINIO_ACCESS_KEY_PLACEHOLDER|${MINIO_ACCESS_KEY}|g" /app/conf/spark-defaults.conf
sed -i "s|MINIO_SECRET_KEY_PLACEHOLDER|${MINIO_SECRET_KEY}|g" /app/conf/spark-defaults.conf

# 2. Load Vault pipeline token — secrets volume always wins over .env
#    vault-init refreshes this file after every unseal; .env may hold a stale token.
if [ -f "/vault/secrets/pipeline_token" ]; then
    VAULT_TOKEN="$(cat /vault/secrets/pipeline_token)"
    export VAULT_TOKEN
fi

# 3. Load vault-encrypted Supabase password from shared secrets volume (overrides .env)
if [ -f "/vault/secrets/supabase_db_password_ciphertext" ]; then
    SUPABASE_DB_PASSWORD="$(cat /vault/secrets/supabase_db_password_ciphertext)"
    export SUPABASE_DB_PASSWORD
fi

if [ "$1" = "shell" ]; then
    exec /bin/sh
fi

if [ "$1" = "bronze" ]; then
    shift
    exec spark-submit \
        --py-files /app/ingestion.zip,/app/bronze_layer.zip \
        /app/bronze_layer/bronze_pipeline.py \
        "$@"
fi

if [ "$1" = "sql" ]; then
    shift
    exec spark-submit /app/docker/sql_runner.py "$@"
fi

if [ "$1" = "spark-server" ]; then
    exec python /app/docker/spark_query_server.py
fi

if [ "$1" = "dbt-run" ]; then
    shift
    APP_NAME=""
    RUN_DATE=""
    DBT_SELECT=""
    while [ $# -gt 0 ]; do
        case "$1" in
            --application-name) APP_NAME="$2"; shift 2 ;;
            --run-date)         RUN_DATE="$2";  shift 2 ;;
            --dbt-select)       DBT_SELECT="$2"; shift 2 ;;
            *) shift ;;
        esac
    done
    cd /app/transformation
    SELECT_ARGS=""
    if [ -n "$DBT_SELECT" ]; then
        SELECT_ARGS="--select $DBT_SELECT"
    fi
    exec dbt run \
        --profiles-dir /app/transformation \
        --target docker \
        --vars "{application_name: '${APP_NAME}', run_date: '${RUN_DATE}'}" \
        $SELECT_ARGS 2>&1
fi

if [ "$1" = "oracle-load" ]; then
    shift
    exec python /app/oracle_layer/oracle_loader.py "$@"
fi

exec spark-submit \
    --py-files /app/ingestion.zip \
    /app/ingestion/main/pipeline.py \
    "$@"
