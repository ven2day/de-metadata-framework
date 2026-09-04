#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Configures the mc alias and verifies connectivity to the already-running
# local MinIO server using credentials from .env.
#
# Usage:
#   ./scripts/start_minio.sh
# -----------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/../.env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: .env not found at $ENV_FILE"
  exit 1
fi

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

echo "Connection test:"
mc admin info "$ALIAS"
