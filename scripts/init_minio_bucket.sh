#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Creates the pipeline bucket on the running local MinIO instance.
# Run this once after starting MinIO for the first time.
#
# Usage:
#   ./scripts/init_minio_bucket.sh
#
# Prerequisites (Homebrew):
#   brew install minio/stable/mc
#
# MinIO server must be running first:
#   ./scripts/start_minio.sh
# -----------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: .env not found at $ENV_FILE"
  exit 1
fi

# Source .env — skip blank lines and comments
set -a
while IFS= read -r line || [[ -n "$line" ]]; do
  [[ "$line" =~ ^[[:space:]]*# ]] && continue
  [[ -z "${line// }" ]] && continue
  export "$line"
done < "$ENV_FILE"
set +a

ALIAS="de-pipeline"

echo "Configuring mc alias '$ALIAS' → $MINIO_ENDPOINT"
mc alias set "$ALIAS" "$MINIO_ENDPOINT" "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY"

echo "Creating bucket '$MINIO_BUCKET' (skipped if already exists)..."
mc mb --ignore-existing "$ALIAS/$MINIO_BUCKET"

echo ""
echo "Done. Bucket summary:"
mc ls "$ALIAS"
