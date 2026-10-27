#!/bin/sh
set -e

# No-op when started by docker compose up with no arguments
if [ $# -eq 0 ]; then
    echo "Pipeline image ready. Use: docker compose run --rm pipeline --application-name <name> ..."
    exit 0
fi

# 1. Load Vault pipeline token — secrets volume always wins over .env
#    vault-init refreshes this file after every unseal; .env may hold a stale token.
if [ -f "/vault/secrets/pipeline_token" ]; then
    VAULT_TOKEN="$(cat /vault/secrets/pipeline_token)"
    export VAULT_TOKEN
fi

# 2. Load vault-encrypted Supabase password from shared secrets volume (overrides .env)
if [ -f "/vault/secrets/supabase_db_password_ciphertext" ]; then
    SUPABASE_DB_PASSWORD="$(cat /vault/secrets/supabase_db_password_ciphertext)"
    export SUPABASE_DB_PASSWORD
fi

if [ "$1" = "shell" ]; then
    exec /bin/sh
fi

if [ "$1" = "dhan-live" ]; then
    exec python3 -m ingestion.dhan.live_collector
fi

if [ "$1" = "dhan-auth" ]; then
    shift
    exec python3 -m ingestion.dhan.auth_cli "$@"
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
            *) echo "Unknown dbt-run argument: $1" >&2; exit 2 ;;
        esac
    done
    cd /app/transformation
    if [ -z "$DBT_SELECT" ] || [ -z "$RUN_DATE" ]; then
        echo "dbt-run requires --dbt-select <model> and --run-date YYYY-MM-DD" >&2
        exit 2
    fi
    DBT_VARS=$(python3 -c 'import json,sys; from datetime import date; date.fromisoformat(sys.argv[1]); print(json.dumps({"application_name": sys.argv[2], "snapshot_date": sys.argv[1]}))' "$RUN_DATE" "$APP_NAME")
    exec dbt run \
        --profiles-dir /app/transformation \
        --target docker \
        --vars "$DBT_VARS" \
        --select "$DBT_SELECT" 2>&1
fi

if [ "$1" = "gold-load" ]; then
    shift
    exec python -m gold_layer.gold_loader "$@"
fi

if [ "$1" = "oracle-load" ]; then
    if [ "${ENABLE_LEGACY_ORACLE:-0}" != "1" ]; then
        echo "Oracle is disabled. Use gold-load for local Iceberg/Trino Gold tables." >&2
        echo "Legacy Oracle requires explicit ENABLE_LEGACY_ORACLE=1 and separate credentials." >&2
        exit 2
    fi
    shift
    exec python /app/oracle_layer/oracle_loader.py "$@"
fi

exec spark-submit \
    --py-files /app/ingestion.zip \
    /app/ingestion/main/pipeline.py \
    "$@"
