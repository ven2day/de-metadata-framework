#!/bin/sh
# AWS CLI entrypoint pre-wired to the MinIO S3 API.
# Picks up credentials from env vars (set by docker compose or .env).
# Usage:
#   docker compose run --rm awscli s3 ls
#   docker compose run --rm awscli s3 ls s3://de-data-lake/ --recursive
#   docker compose run --rm -it awscli shell   ← interactive terminal

export AWS_ACCESS_KEY_ID="${MINIO_ACCESS_KEY:-minioadmin}"
export AWS_SECRET_ACCESS_KEY="${MINIO_SECRET_KEY:-minioadmin}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
export AWS_ENDPOINT_URL="$ENDPOINT"

if [ "$1" = "shell" ]; then
    exec /bin/sh
fi

exec aws --endpoint-url "$ENDPOINT" --no-verify-ssl "$@"
