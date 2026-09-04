# DE Metadata Framework

A metadata-driven data ingestion pipeline built on Apache Spark and Apache Iceberg. Reads from S3-compatible storage or PostgreSQL (Supabase), applies schema validation, type casting, and PII processing, then writes Iceberg tables to MinIO. Ships with a Flask web UI, HashiCorp Vault for secrets management, and full Docker orchestration.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Docker Host                          │
│                                                             │
│  ┌──────────┐   ┌──────────┐   ┌─────────┐  ┌──────────┐  │
│  │  MinIO   │   │  Vault   │   │   UI    │  │ Pipeline │  │
│  │  :9000   │   │  :8200   │   │  :5001  │  │(on demand│  │
│  │  :9001   │   │          │   │         │  │via UI)   │  │
│  └────┬─────┘   └────┬─────┘   └────┬────┘  └────┬─────┘  │
│       │              │               │             │        │
│       └──────────────┴───────────────┴─────────────┘        │
│                          de-net (bridge)                    │
└─────────────────────────────────────────────────────────────┘
```

| Service | Role | Port |
|---|---|---|
| **MinIO** | S3-compatible object store — raw data, Iceberg tables, logs | 9000 (API), 9001 (Console) |
| **Vault** | Secrets store — PII encryption keys, token management | 8200 |
| **UI** | Flask web interface — submit jobs, stream logs, download results | 5001 |
| **Pipeline** | Spark ETL job — spawned by UI via Docker socket, auto-removed | — |

---

## Prerequisites

- **Docker Desktop** (with BuildKit enabled — default on Desktop)
- **Python 3.11** and a virtual environment (for local development only)
- **Java 17** (for running Spark locally outside Docker)

---

## Project Structure

```
de-metadata-framework/
├── Dockerfile                     # Single image for pipeline + UI
├── docker-compose.yml             # Full service orchestration
├── docker/
│   ├── entrypoint.sh             # Pipeline container entry point
│   ├── ui-entrypoint.sh          # UI container entry point
│   ├── vault_init.py             # One-shot Vault bootstrap
│   └── pip_install.sh            # Smart pip installer (skips cached versions)
├── conf/
│   ├── spark-defaults.conf       # Spark + Iceberg + S3A configuration
│   └── log4j2.properties         # Spark logging
├── ingestion/
│   ├── main/pipeline.py          # ETL orchestrator
│   ├── env/DE_Ingestion_properties.py  # All config from environment
│   ├── pyfiles/                  # Core modules (logger, vault client, PII, etc.)
│   ├── source/                   # S3 and Supabase readers
│   └── sink/                     # Iceberg/MinIO writer
├── ui/
│   ├── app.py                    # Flask application
│   └── templates/index.html      # Web interface
├── metadata/
│   └── metadata_sheet_example.csv
├── requirements.txt              # Lightweight deps (boto3, flask, hvac, etc.)
├── requirements-heavy.txt        # Large binaries (pyspark, pyarrow, psycopg2, etc.)
└── scripts/                      # Local helper scripts
```

---

## First-Time Setup

### 1. Clone the repository

```bash
git clone <repo-url>
cd de-metadata-framework
```

### 2. Create your `.env` file

Copy the template below and fill in your values. This file is gitignored and never committed.

```env
# ── MinIO (object store) ──────────────────────────────────────────────────────
MINIO_ACCESS_KEY=your_minio_user
MINIO_SECRET_KEY=your_minio_password
MINIO_BUCKET=de-data-lake
MINIO_ENDPOINT=http://localhost:9000

# ── Source S3 (input data bucket) ────────────────────────────────────────────
SOURCE_S3_ENDPOINT=http://localhost:9000
SOURCE_S3_ACCESS_KEY=your_minio_user
SOURCE_S3_SECRET_KEY=your_minio_password
SOURCE_S3_BUCKET=de-source-data-bucket

# ── Supabase / PostgreSQL ─────────────────────────────────────────────────────
SUPABASE_JDBC_URL=jdbc:postgresql://db.<ref>.supabase.co:5432/postgres
SUPABASE_DB_USER=postgres
SUPABASE_DB_PASSWORD_PLAIN=your_plain_password   # vault-init encrypts this; not used at runtime

# ── HashiCorp Vault ───────────────────────────────────────────────────────────
VAULT_ADDR=http://localhost:8200
VAULT_TOKEN=                        # leave blank; populated by vault-init on first run
VAULT_PATH=encryption/pii

# ── PII Encryption ────────────────────────────────────────────────────────────
SALT_KEY=your_hmac_salt_key         # used for HMAC-SHA256 hashing
SALT_2=your_secondary_salt          # seeded into Vault KV by vault-init

# ── Iceberg ───────────────────────────────────────────────────────────────────
ICEBERG_WAREHOUSE=s3a://de-iceberg-warehouse/
ICEBERG_CATALOG=minio
ICEBERG_DATABASE=default

# ── Spark ─────────────────────────────────────────────────────────────────────
SPARK_APP_NAME=DE-Metadata-Framework
SPARK_MASTER=local[*]

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_S3_BUCKET=de-data-migration-logs
METADATA_S3_BUCKET=de-metadata-bucket

# ── Email notifications (Brevo) ───────────────────────────────────────────────
BREVO_API_KEY=your_brevo_api_key
BREVO_FROM_EMAIL=noreply@yourdomain.com
NOTIFY_EMAIL=you@yourdomain.com
```

### 3. Generate `conf/spark-defaults.conf`

`spark-defaults.conf` is gitignored. Create it from the template below — credentials are injected at runtime by the entrypoint, so use the placeholders exactly as shown:

```properties
spark.sql.extensions  org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

spark.sql.catalog.minio                        org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.minio.type                   hadoop
spark.sql.catalog.minio.warehouse              s3a://de-iceberg-warehouse/
spark.sql.catalog.minio.io-impl                org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.minio.s3.endpoint            http://127.0.0.1:9000
spark.sql.catalog.minio.s3.path-style-access   true
spark.sql.catalog.minio.s3.region              us-east-1
spark.sql.catalog.minio.s3.access-key-id       MINIO_ACCESS_KEY_PLACEHOLDER
spark.sql.catalog.minio.s3.secret-access-key   MINIO_SECRET_KEY_PLACEHOLDER

spark.hadoop.fs.s3a.endpoint                   http://127.0.0.1:9000
spark.hadoop.fs.s3a.path.style.access          true
spark.hadoop.fs.s3a.connection.ssl.enabled     false
spark.hadoop.fs.s3a.endpoint.region            us-east-1
spark.hadoop.fs.s3a.aws.credentials.provider   org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
spark.hadoop.fs.s3a.access.key                 MINIO_ACCESS_KEY_PLACEHOLDER
spark.hadoop.fs.s3a.secret.key                 MINIO_SECRET_KEY_PLACEHOLDER

# spark.jars.packages is commented out in the Docker image (JARs are baked in).
# Uncomment for local development:
# spark.jars.packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0,org.apache.hadoop:hadoop-aws:3.4.2,org.postgresql:postgresql:42.7.8
```

### 4. Build the Docker image

```bash
docker compose build --progress=plain
```

First build takes several minutes — it downloads pip packages, Maven JARs, and caches everything. Subsequent builds are fast.

### 5. Start infrastructure services

```bash
docker compose up -d minio vault
```

### 6. Bootstrap Vault (once only)

```bash
docker compose run --rm vault-init
```

This initialises and unseals Vault, creates encryption keys, seeds KV secrets, encrypts your Supabase password, and writes a scoped pipeline token to the shared `vault_secrets` volume. Run this only once — it is idempotent but skips re-initialisation on subsequent calls.

After it completes, copy the generated `VAULT_TOKEN` from `/vault/secrets/pipeline_token` into your `.env`:

```bash
docker compose run --rm vault cat /vault/secrets/pipeline_token
```

### 7. Provision MinIO buckets

```bash
docker compose run --rm minio-init
```

Creates: `de-data-lake`, `de-source-data-bucket` (or your `SOURCE_S3_BUCKET`), `de-iceberg-warehouse`, `de-metadata-bucket`, `de-data-migration-logs`.

### 8. Start the UI

```bash
docker compose up -d ui
```

Open **http://localhost:5001** in your browser.

---

## Subsequent Starts

Once the initial setup is done, a single command starts everything:

```bash
docker compose up -d
```

To stop:

```bash
docker compose down
```

> Data is persisted in Docker volumes (`minio_data`, `vault_data`). Your ingested tables and logs survive restarts.

---

## Running the Pipeline

### Via the Web UI

1. Open **http://localhost:5001**
2. Fill in the job form:
   - **Application Name** — identifier for this dataset
   - **Source Type** — S3 or Database
   - **Ingest Date** — date partition (`YYYY-MM-DD`)
   - **Metadata Key** — S3 path to the metadata sheet CSV
   - **Output settings** — database, catalog, table name, write mode
3. Click **Run Pipeline**
4. Logs stream in real-time in the right panel
5. Download the log from S3 after the job completes

The UI delegates job execution to a fresh pipeline container via the Docker socket — Spark runs in the pipeline image, not the UI.

### Via CLI (inside the pipeline container)

```bash
docker compose run --rm pipeline \
  --application-name my_dataset \
  --source-type s3 \
  --ingest-date 2026-01-15 \
  --source-bucket de-source-data-bucket \
  --source-key raw/my_dataset/data.parquet \
  --metadata-key metadata/my_dataset_schema.csv \
  --output-database default \
  --output-table-name my_dataset \
  --write-mode overwrite \
  --log-level INFO
```

For a database source:

```bash
docker compose run --rm pipeline \
  --application-name my_table \
  --source-type database \
  --ingest-date 2026-01-15 \
  --source-database public \
  --source-table-name orders \
  --metadata-key metadata/orders_schema.csv \
  --output-database default \
  --write-mode append
```

---

## Pipeline CLI Reference

| Argument | Required | Default | Description |
|---|---|---|---|
| `--application-name` | Yes | — | Dataset / job identifier |
| `--source-type` | Yes | — | `s3` or `database` |
| `--ingest-date` | Yes | — | Partition date (`YYYY-MM-DD`) |
| `--metadata-key` | No | env | S3 key to metadata sheet CSV |
| `--source-bucket` | S3 only | env | Source S3 bucket |
| `--source-key` | S3 only | — | S3 object key (path to file) |
| `--source-database` | DB only | `public` | PostgreSQL schema |
| `--source-table-name` | DB only | — | Table to read |
| `--output-database` | No | env | Iceberg namespace |
| `--output-catalog` | No | `minio` | Iceberg catalog |
| `--output-table-name` | No | application-name | Iceberg table name |
| `--write-mode` | No | `overwrite` | `overwrite`, `append`, or `replace` |
| `--log-folder` | No | `lake` | Top-level folder in log bucket |
| `--log-level` | No | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |

---

## Metadata Sheet Format

The pipeline is driven by a CSV metadata sheet uploaded to MinIO. Each row defines one column:

| Column | Description |
|---|---|
| `column_name` | Exact column name in the source data |
| `data_type` | Target type: `string`, `integer`, `double`, `date`, `timestamp`, `boolean` |
| `nullable` | `true` / `false` |
| `pii_action` | `hash`, `mask`, `encrypt`, or blank (no action) |
| `description` | Human-readable description |

See `metadata/metadata_sheet_example.csv` for a working example.

---

## Vault & Secrets

Vault is used for two things:

1. **PII encryption key export** — `pii-encrypt` transit key (exportable) is fetched by Spark for AES column encryption.
2. **Supabase password decryption** — `supabase-pwd` transit key (non-exportable) decrypts the ciphertext loaded from the shared volume.

The pipeline token written by `vault-init` is scoped to exactly these two operations plus KV read access. The token is short-lived (24h, auto-renewable).

If Vault is restarted (sealed), it unseals automatically on next `docker compose up` using the key stored in `vault_data:/vault/data/init.json`.

---

## Environment Variables Reference

| Variable | Default | Description |
|---|---|---|
| `MINIO_ENDPOINT` | `http://127.0.0.1:9000` | MinIO S3 API URL |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `minioadmin` | MinIO secret key |
| `MINIO_BUCKET` | `de-data-lake` | Sink bucket for Iceberg data |
| `SOURCE_S3_ENDPOINT` | `http://127.0.0.1:9000` | Source S3 endpoint |
| `SOURCE_S3_ACCESS_KEY` | `minioadmin` | Source access key |
| `SOURCE_S3_SECRET_KEY` | `minioadmin` | Source secret key |
| `SOURCE_S3_BUCKET` | `de-source` | Source bucket name |
| `SUPABASE_JDBC_URL` | — | PostgreSQL JDBC connection string |
| `SUPABASE_DB_USER` | `postgres` | Database user |
| `SUPABASE_DB_PASSWORD_PLAIN` | — | Plain password (vault-init only) |
| `VAULT_ADDR` | `http://127.0.0.1:8200` | Vault API address |
| `VAULT_TOKEN` | — | Pipeline-scoped Vault token |
| `VAULT_PATH` | `encryption/pii` | KV path for PII secrets |
| `SALT_KEY` | — | HMAC-SHA256 salt for hashing |
| `SALT_2` | — | Secondary salt seeded into Vault |
| `ICEBERG_WAREHOUSE` | `s3a://de-iceberg-warehouse/` | Iceberg warehouse location |
| `ICEBERG_CATALOG` | `minio` | Iceberg catalog name |
| `ICEBERG_DATABASE` | `default` | Default Iceberg namespace |
| `LOG_S3_BUCKET` | `de-data-migration-logs` | Bucket for pipeline logs |
| `METADATA_S3_BUCKET` | `de-metadata-bucket` | Bucket for metadata sheets |
| `BREVO_API_KEY` | — | Brevo transactional email API key |
| `BREVO_FROM_EMAIL` | — | Sender address for notifications |
| `NOTIFY_EMAIL` | — | Recipient address for notifications |
| `FLASK_DEBUG` | `0` | Set to `1` to enable Flask debug mode |

---

## Local Development (without Docker)

### Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-heavy.txt
pip install -r requirements.txt
```

### Run the pipeline locally

```bash
export $(cat .env | xargs)
export SPARK_CONF_DIR=$(pwd)/conf

spark-submit \
  --py-files ingestion.zip \
  ingestion/main/pipeline.py \
  --application-name test \
  --source-type s3 \
  --ingest-date 2026-01-15 \
  --source-key raw/test.parquet \
  --output-database default
```

### Run the UI locally

```bash
export $(cat .env | xargs)
python ui/app.py
```

Open **http://localhost:5000**.

> When running locally, `COMPOSE_PROJECT_NAME` must be set so the UI can locate the pipeline Docker image and network. If Docker containers are not running, the UI falls back to running `spark-submit` locally.

---

## Rebuilding After Changes

| Changed file | Action needed |
|---|---|
| `requirements.txt` or `requirements-heavy.txt` | `docker compose build` |
| `conf/spark-defaults.conf` | `docker compose build` (conf is baked into image) |
| `ingestion/**` or `ui/**` | `docker compose build` |
| `docker-compose.yml` only | `docker compose up -d` (no rebuild) |
| `.env` only | `docker compose up -d` (no rebuild) |

---

## Troubleshooting

**Pipeline container exits immediately**
Check logs: `docker compose logs pipeline`
Ensure `vault-init` completed successfully and `minio` is healthy.

**Vault sealed after restart**
Run `docker compose run --rm vault-init` — it detects the sealed state and unseals using the saved key.

**MinIO bucket not found**
Run `docker compose run --rm minio-init` to re-provision buckets.

**Spark downloading JARs on every run**
JARs are baked into the image. If you see Maven downloads, rebuild: `docker compose build --no-cache`.

**grpcio-status import error**
Rebuild the image from scratch: `docker compose build --no-cache --progress=plain`
