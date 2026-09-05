# DE Metadata Framework

A metadata-driven data ingestion pipeline built on Apache Spark and Apache Iceberg. Reads from S3-compatible storage or PostgreSQL (Supabase), applies schema validation, type casting, and PII processing, then writes Iceberg tables to MinIO. Ships with a Flask web UI with login-based access control, HashiCorp Vault for secrets management, and full Docker orchestration exposed publicly via Cloudflare Tunnel.

---

## Architecture

### High-Level Architecture

Data flows from external sources through the pipeline into an Iceberg data lake. The web UI is gated behind authentication; consumers query tables directly.

```
                    ┌──────────────────────────────────────────────┐
  Data Sources      │            DE Metadata Framework             │      Consumers
                    │                                              │
  ┌──────────┐      │  ┌────────┐    ┌──────────────────────────┐ │
  │ S3 / MinIO├─────┼─▶│        │    │   Apache Iceberg          │ │  ┌──────────────┐
  │ (raw data)│     │  │        │───▶│   Tables (MinIO)          ├─┼─▶│ BI / Spark   │
  └──────────┘      │  │        │    │   de-iceberg-warehouse/   │ │  │ query engine │
                    │  │  Spark │    └──────────────────────────┘ │  └──────────────┘
  ┌──────────┐      │  │  ETL   │                                  │
  │ Supabase /├─────┼─▶│  Pipeline   ┌──────────────────────────┐ │
  │ PostgreSQL│     │  │        │───▶│   Pipeline Logs           │ │  ┌──────────────┐
  └──────────┘      │  │        │    │   (MinIO log bucket)      ├─┼─▶│ Operators /  │
                    │  └────┬───┘    └──────────────────────────┘ │  │ Monitoring   │
  ┌──────────┐      │       │                                      │  └──────────────┘
  │ Metadata  ├─────┼───────┘        ┌──────────────────────────┐ │
  │ Sheet CSV │     │                │   Email Notification      │ │  ┌──────────────┐
  │ (MinIO)   │     │                │   (Brevo API)             ├─┼─▶│ Data Engineer│
  └──────────┘      │                └──────────────────────────┘ │  └──────────────┘
                    │                                              │
  ┌──────────┐      │  ┌──────────────────────────────────────┐   │
  │ Engineer  ├─────┼─▶│  Flask Web UI (login-protected)       │   │
  │ (browser) │     │  │  Submit jobs · Stream logs · Download │   │
  └──────────┘      │  │  Root user manages team access        │   │
                    │  └──────────────────────────────────────┘   │
                    └──────────────────────────────────────────────┘
                                        ▲
                          Cloudflare Tunnel (cloudflared)
                          app / minio / s3 / vault subdomains
```

**Data flow summary:**

| Step | What happens |
|---|---|
| 1. Login | Engineer authenticates via the login page (Argon2id passwords, account lockout) |
| 2. Submit | Fills in the UI form (source, metadata key, output settings) |
| 3. Spawn | UI launches a fresh pipeline container via Docker socket |
| 4. Read metadata | Pipeline fetches the CSV schema sheet from MinIO |
| 5. Read source | Pipeline reads raw data from S3 or Supabase/PostgreSQL |
| 6. Transform | Schema validation → type casting → PII processing (hash / mask / encrypt) |
| 7. Write | Iceberg table written/appended to MinIO warehouse |
| 8. Log | Full run log uploaded to MinIO log bucket |
| 9. Notify | Success/failure email sent via Brevo |

---

### Low-Level Architecture

#### Docker Services

| Service | Role | Internal Port | Public URL |
|---|---|---|---|
| **minio** | S3-compatible object store — raw data, Iceberg tables, logs | 9000 (API), 9001 (Console) | `https://s3.atestingdomain.info` / `https://minio.atestingdomain.info` |
| **vault** | Secrets — PII keys, pipeline tokens, Supabase password | 8200 | `https://vault.atestingdomain.info` |
| **ui** | Flask web UI — submit jobs, stream logs, user management | 5000 (→ host 5001) | `https://app.atestingdomain.info` |
| **cloudflared** | Outbound Cloudflare Tunnel — routes all public subdomains | — | — |
| **pipeline** | Spark ETL runner — spawned on-demand, auto-removed | — | — |
| **minio-init** | One-shot bucket provisioner (runs once on first up) | — | — |
| **vault-init** | One-shot Vault bootstrapper — unseals + seeds secrets | — | — |

#### Docker Network Topology

```
┌──────────────────────────────────────────────────────────────────────────┐
│  Docker Host                                                             │
│                                                                          │
│  ┌───────────────────────── de-net (bridge) ──────────────────────────┐  │
│  │                                                                    │  │
│  │  ┌─────────────┐   ┌────────────┐   ┌────────────┐  ┌──────────┐  │  │
│  │  │    minio    │   │   vault    │   │    de-ui   │  │cloudflared│  │  │
│  │  │ :9000 API   │   │  :8200     │   │  :5000     │  │ (tunnel) │  │  │
│  │  │ :9001 console   │ Transit    │   │  Flask app │  │ outbound │  │  │
│  │  │             │   │ KV v2      │   │  Auth/CSRF │  │ HTTP/2   │  │  │
│  │  └──────┬──────┘   └─────┬──────┘   └─────┬──────┘  └────┬─────┘  │  │
│  │         │                │                 │              │        │  │
│  │         │         vault_secrets volume      │     ┌────────┘        │  │
│  │         │         (pipeline_token,          │     │ routes:         │  │
│  │         │          supabase_pwd_ciphertext)  │     │ app.→ de-ui     │  │
│  │         │                │                 │     │ s3.→  minio:9000│  │
│  │         │                ▼                 │     │ minio.→:9001    │  │
│  │         │    ┌───────────────────────┐     │     │ vault.→vault    │  │
│  │         └───▶│  pipeline (ephemeral) │◀────┘     └─────────────────│  │
│  │               │  spawned via Docker  │                              │  │
│  │               │  sock; Spark 4.x     │                              │  │
│  │               │  local[*], auto-rm   │                              │  │
│  │               └───────────────────────┘                              │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                                                                          │
│  /var/run/docker.sock  (de-ui mounts host socket to spawn pipeline)      │
└──────────────────────────────────────────────────────────────────────────┘
                                    │
                   Cloudflare Tunnel (HTTPS, HTTP/2)
                                    │
                    ┌───────────────┴───────────────┐
                    │      atestingdomain.info       │
                    │  app.*   minio.*  s3.*  vault.*│
                    └───────────────────────────────┘
```

#### Authentication Architecture

```
  Browser ──HTTPS──▶ Cloudflare ──Tunnel──▶ cloudflared ──▶ de-ui:5000
                                                               │
                                                    Flask-Login + CSRF
                                                               │
                                         ┌─────────────────────┘
                                         │
                              /login  (rate-limited: 5/min per IP)
                                │
                          argon2id verify
                                │
                      ┌─────────┴──────────┐
                      │ Supabase           │
                      │ public.app_users   │
                      │  - id, username    │
                      │  - password_hash   │
                      │  - role (root/user)│
                      │  - failed_attempts │
                      │  - locked_until    │
                      └────────────────────┘
                                │
                    locked after 5 failures (15 min)
                    HaveIBeenPwned check on create
                    root user: only role that can add users
```

#### Secret Resolution Chain

```
  vault-init (one-shot bootstrap)
  ├── Initialises + unseals Vault; saves unseal key to vault_data volume
  ├── Enables Transit engine
  │   ├── Creates pii-encrypt key  (exportable  — AES-256-GCM)
  │   └── Creates supabase-pwd key (non-exportable — encrypt only)
  ├── Enables KV v2 at secret/
  │   └── Writes SALT_2 from .env
  ├── Encrypts SUPABASE_DB_PASSWORD_PLAIN → ciphertext
  │   └── Writes ciphertext to vault_secrets:/vault/secrets/supabase_db_password_ciphertext
  └── Creates scoped pipeline token (24h, renewable)
      └── Writes token to vault_secrets:/vault/secrets/pipeline_token

  seed_root_user.py (runs in ui-entrypoint.sh at every UI startup)
  ├── Calls init_schema() — creates public.app_users table if missing
  ├── Checks for existing root user — skips if found
  └── Creates root user from ROOT_PASSWORD env var (auto-generates if blank)

  ui/db.py (auth database connection)
  ├── AUTH_DATABASE_URL env var (PostgreSQL pooler URL — IPv4 capable)
  │   or falls back to parsing SUPABASE_JDBC_URL
  ├── Decrypts Supabase password via Vault Transit
  └── psycopg2 connection with RealDictCursor

  At pipeline startup (entrypoint.sh)
  ├── VAULT_TOKEN: .env value wins; vault_secrets file is fallback
  ├── SUPABASE_DB_PASSWORD: always loaded from vault_secrets ciphertext file
  └── MINIO credentials: substituted into spark-defaults.conf via sed

  At runtime (Spark job)
  ├── vault_client.py  ──▶  Vault Transit export  ──▶  pii-encrypt AES key
  ├── vault_client.py  ──▶  Vault KV read         ──▶  SALT_2
  └── vault_client.py  ──▶  Vault Transit decrypt  ──▶  Supabase plaintext password
```

#### Spark Execution Flow

```
  pipeline.py (spark-submit entry point)
  │
  ├── 1. Parse CLI args (argparse)
  ├── 2. Load env config  (DE_Ingestion_properties.py)
  ├── 3. Build SparkSession  (spark_session.py)
  │       └── Config loaded from conf/spark-defaults.conf (SPARK_CONF_DIR)
  │           ├── Iceberg extensions
  │           ├── S3A credentials + endpoint
  │           └── Iceberg catalog (minio / hadoop type)
  │
  ├── 4. Read metadata sheet CSV from MinIO
  │       └── Columns: column_name, data_type, nullable, pii_action, description
  │
  ├── 5. Read source data
  │       ├── S3 path  → SparkReader.parquet / csv / json (s3a://)
  │       └── Database → SparkReader.jdbc (Supabase / PostgreSQL)
  │
  ├── 6. Schema validation
  │       └── Assert all metadata columns present in source DataFrame
  │
  ├── 7. Type casting
  │       └── Cast each column to target data_type from metadata sheet
  │
  ├── 8. PII processing  (pii_processor.py + vault_client.py)
  │       ├── hash    → HMAC-SHA256 with SALT_KEY
  │       ├── mask    → replace with "****"
  │       └── encrypt → AES-256-GCM with key from Vault Transit export
  │
  ├── 9. Write Iceberg table
  │       └── spark.sql / DataFrameWriter  →  minio.<database>.<table>
  │           Modes: overwrite | append | replace
  │
  ├── 10. Upload log file to MinIO  (logger.py)
  └── 11. Send email notification   (notifier.py → Brevo API)
```

#### MinIO Bucket Layout

```
  MinIO
  ├── de-source/                      ← raw input files (S3 source)
  │   └── <folder>/<file>
  ├── de-metadata-bucket/             ← metadata CSV sheets
  │   └── <dataset>_schema.csv
  ├── de-iceberg-warehouse/           ← Iceberg table data + metadata
  │   └── <database>/<table>/
  │       ├── data/  (Parquet files)
  │       └── metadata/  (Iceberg manifests + snapshots)
  ├── de-data-lake/                   ← general data lake bucket (sink)
  └── de-data-migration-logs/         ← per-run pipeline logs
      └── lake/<app-name>/<date>/<app-name>_<date>_<ts>.log
```

---

## Domain Setup (Cloudflare Tunnel)

The stack runs locally in Docker and is exposed publicly via a **Cloudflare Tunnel** — no port forwarding or static IP required. `cloudflared` connects outbound using HTTP/2 over TLS, which works reliably behind Docker Desktop's NAT.

| URL | Routes to |
|---|---|
| `https://app.atestingdomain.info` | Flask Web UI (`de-ui:5000`) |
| `https://minio.atestingdomain.info` | MinIO Console (`minio:9001`) |
| `https://s3.atestingdomain.info` | MinIO S3 API (`minio:9000`) |
| `https://vault.atestingdomain.info` | Vault UI + API (`vault:8200`) |

### One-time tunnel setup

1. Log in to [Cloudflare Zero Trust](https://one.dash.cloudflare.com) → **Networks → Tunnels**
2. Click **Create a tunnel** → **Cloudflared** → name it `de-metadata-framework`
3. Copy the **tunnel token** (long string starting with `eyJ...`)
4. Add to your `.env`:
   ```env
   CLOUDFLARE_TUNNEL_TOKEN=eyJ...your_token_here...
   ```
5. In the tunnel's **Public Hostnames** tab, add four entries:

   | Subdomain | Domain | Service URL |
   |---|---|---|
   | `app` | `atestingdomain.info` | `http://de-ui:5000` |
   | `minio` | `atestingdomain.info` | `http://minio:9001` |
   | `s3` | `atestingdomain.info` | `http://minio:9000` |
   | `vault` | `atestingdomain.info` | `http://vault:8200` |

   Cloudflare automatically creates the CNAME DNS records — nothing else to configure.

6. Start the full stack:
   ```bash
   docker compose up -d
   ```

> **Note:** The `cloudflared` service uses `--protocol http2`. Do not change it to `quic` — QUIC over Docker Desktop's NAT causes silent proxy failures.

---

## Prerequisites

- **Docker Desktop** (with BuildKit enabled — default on Desktop)
- **Python 3.11** and a virtual environment (for local development only)
- **Java 17** (for running Spark locally outside Docker)

---

## Project Structure

```
de-metadata-framework/
├── Dockerfile                         # Single image: pipeline + UI + auth
├── docker-compose.yml                 # Full service orchestration
├── docker/
│   ├── entrypoint.sh                 # Pipeline container entry point
│   ├── ui-entrypoint.sh              # UI container entry point (seeds root user)
│   ├── vault_init.py                 # One-shot Vault bootstrap
│   ├── seed_root_user.py             # Creates root user on first UI start
│   └── pip_install.sh                # Smart pip installer (skips cached versions)
├── conf/
│   ├── spark-defaults.conf           # Spark + Iceberg + S3A config (gitignored)
│   └── log4j2.properties             # Spark logging config
├── ingestion/
│   ├── main/pipeline.py              # ETL orchestrator (spark-submit entry point)
│   ├── env/DE_Ingestion_properties.py # All config loaded from environment
│   ├── pyfiles/                      # Core modules
│   │   ├── spark_session.py          # SparkSession builder
│   │   ├── pii_processor.py          # Hash / mask / encrypt columns
│   │   ├── vault_client.py           # Vault Transit + KV client
│   │   ├── logger.py                 # Log to MinIO
│   │   └── notifier.py               # Brevo email notifications
│   ├── source/                       # S3 and Supabase/JDBC readers
│   └── sink/                         # Iceberg/MinIO writer
├── ui/
│   ├── app.py                        # Flask application + pipeline routes
│   ├── auth.py                       # Login / logout / user management routes
│   ├── models.py                     # User model (Argon2id, lockout, HIBP check)
│   ├── db.py                         # Supabase connection + schema init
│   ├── extensions.py                 # Shared Flask extensions (CSRF, Login, Limiter)
│   └── templates/
│       ├── index.html                # Main pipeline submission UI
│       ├── login.html                # Login page
│       └── users.html                # User management (root only)
├── metadata/
│   └── metadata_sheet_example.csv    # Example metadata sheet
├── requirements.txt                  # Lightweight deps (boto3, flask, auth libs…)
├── requirements-heavy.txt            # Large deps (pyspark, pyarrow, psycopg2…)
└── scripts/                          # Local helper scripts
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
MINIO_ENDPOINT=http://minio:9000
MINIO_SERVER_URL=https://s3.atestingdomain.info
MINIO_BROWSER_REDIRECT_URL=https://minio.atestingdomain.info

# ── Source S3 (input data bucket) ────────────────────────────────────────────
SOURCE_S3_ENDPOINT=http://minio:9000
SOURCE_S3_ACCESS_KEY=your_minio_user
SOURCE_S3_SECRET_KEY=your_minio_password
SOURCE_S3_BUCKET=de-source

# ── Supabase / PostgreSQL ─────────────────────────────────────────────────────
SUPABASE_JDBC_URL=jdbc:postgresql://db.<ref>.supabase.co:5432/postgres
SUPABASE_DB_USER=postgres
SUPABASE_DB_PASSWORD_PLAIN=your_plain_password   # vault-init encrypts this
# Use the IPv4 pooler to avoid IPv6 connectivity issues inside Docker:
AUTH_DATABASE_URL=jdbc:postgresql://aws-0-<region>.pooler.supabase.com:6543/postgres?user=postgres.<ref>&password=<password>

# ── HashiCorp Vault ───────────────────────────────────────────────────────────
VAULT_ADDR=http://vault:8200
VAULT_TOKEN=                        # leave blank; populated by vault-init
VAULT_PATH=encryption/pii

# ── PII Encryption ────────────────────────────────────────────────────────────
SALT_KEY=your_hmac_salt_key
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

# ── Flask Auth ────────────────────────────────────────────────────────────────
# Generate once: python3 -c "import secrets; print(secrets.token_hex(32))"
# Never change after first deploy — invalidates all existing sessions.
FLASK_SECRET_KEY=your_64_char_hex_string

# Root user credentials — used only on first startup to seed the DB.
# Remove ROOT_PASSWORD from .env after confirming login works.
ROOT_EMAIL=root@atestingdomain.info
ROOT_PASSWORD=YourStrongPassword@123   # min 12 chars, upper+lower+digit+special

# ── Cloudflare Tunnel ─────────────────────────────────────────────────────────
CLOUDFLARE_TUNNEL_TOKEN=eyJ...your_tunnel_token_here...
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
# spark.jars.packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,...
```

### 4. Build the Docker image

```bash
docker compose build --progress=plain
```

First build takes several minutes — downloads pip packages and Maven JARs, then caches everything. Subsequent builds are fast.

### 5. Start infrastructure

```bash
docker compose up -d minio vault
```

### 6. Bootstrap Vault (once only)

```bash
docker compose run --rm vault-init
```

Initialises and unseals Vault, creates PII encryption keys, seeds KV secrets, encrypts your Supabase password, and writes a scoped pipeline token to the shared `vault_secrets` volume. Idempotent — safe to re-run, skips steps already done.

### 7. Start the UI (seeds root user on first start)

```bash
docker compose up -d ui cloudflared
```

On first startup, `seed_root_user.py` runs automatically and creates the `root` user in Supabase using `ROOT_PASSWORD` from `.env`. The root user's credentials are printed to the UI container log if auto-generated:

```bash
docker logs de-ui 2>&1 | grep -A3 "ROOT USER"
```

Open **https://app.atestingdomain.info** (or **http://localhost:5001**) and log in with `root` / `ROOT_PASSWORD`.

**After confirming login works, remove `ROOT_PASSWORD` from `.env`** — the root account persists in the database.

### 8. Add team users

Log in as `root`, navigate to **Users** in the top-right menu, and create accounts for your team. Only the `root` role can create users. Password requirements:
- Minimum 12 characters
- At least one uppercase, lowercase, digit, and special character
- Must not appear in known data breach databases (HaveIBeenPwned check)

---

## Subsequent Starts

Once the initial setup is done:

```bash
docker compose up -d
```

To stop:

```bash
docker compose down
```

> Data is persisted in Docker volumes (`minio_data`, `vault_data`). Your ingested tables, logs, and user accounts survive restarts.

> **Vault sealed after restart?** Run `docker compose run --rm vault-init` — it detects the sealed state and unseals using the saved key.

---

## Running the Pipeline

### Via the Web UI

1. Open **https://app.atestingdomain.info** and log in
2. Fill in the job form:
   - **Application Name** — identifier for this dataset
   - **Source Type** — S3 or Database
   - **Ingest Date** — date partition (`YYYY-MM-DD`)
   - **Metadata Key** — S3 path to the metadata sheet CSV
   - **Output settings** — database, catalog, table name, write mode
3. Click **Run Pipeline**
4. Logs stream in real-time in the right panel
5. Download the log from S3 after the job completes

The UI delegates execution to a fresh pipeline container via the Docker socket — Spark runs in the pipeline image, not the UI image.

### Via CLI

```bash
docker compose run --rm pipeline \
  --application-name my_dataset \
  --source-type s3 \
  --ingest-date 2026-01-15 \
  --source-bucket de-source \
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

Vault is used for three things:

1. **PII encryption key export** — `pii-encrypt` transit key (exportable AES-256-GCM) is fetched by Spark for column encryption.
2. **Supabase password decryption** — `supabase-pwd` transit key (non-exportable) decrypts the ciphertext loaded from the shared volume at pipeline startup.
3. **Auth DB password** — `ui/db.py` decrypts the Supabase password via Vault Transit before opening the psycopg2 connection for user authentication.

The pipeline token written by `vault-init` is scoped to exactly these operations plus KV read access. Tokens are short-lived (24h, auto-renewable).

---

## Environment Variables Reference

| Variable | Default | Description |
|---|---|---|
| `MINIO_ENDPOINT` | `http://minio:9000` | MinIO S3 API URL (internal) |
| `MINIO_SERVER_URL` | `http://localhost:9000` | MinIO S3 public URL (embedded in redirects) |
| `MINIO_BROWSER_REDIRECT_URL` | `http://localhost:9001` | MinIO Console public URL |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `minioadmin` | MinIO secret key |
| `MINIO_BUCKET` | `de-data-lake` | Sink bucket for Iceberg data |
| `SOURCE_S3_ENDPOINT` | `http://minio:9000` | Source S3 endpoint (internal) |
| `SOURCE_S3_ACCESS_KEY` | `minioadmin` | Source access key |
| `SOURCE_S3_SECRET_KEY` | `minioadmin` | Source secret key |
| `SOURCE_S3_BUCKET` | `de-source` | Source bucket name |
| `SUPABASE_JDBC_URL` | — | PostgreSQL JDBC connection string (direct host) |
| `AUTH_DATABASE_URL` | — | PostgreSQL pooler URL for auth DB (IPv4, preferred over SUPABASE_JDBC_URL) |
| `SUPABASE_DB_USER` | `postgres` | Database user |
| `SUPABASE_DB_PASSWORD_PLAIN` | — | Plain password for vault-init encryption only |
| `VAULT_ADDR` | `http://vault:8200` | Vault API address |
| `VAULT_TOKEN` | — | Pipeline-scoped Vault token |
| `VAULT_PATH` | `encryption/pii` | KV path for PII secrets |
| `SALT_KEY` | — | HMAC-SHA256 salt for hashing |
| `SALT_2` | — | Secondary salt seeded into Vault KV |
| `ICEBERG_WAREHOUSE` | `s3a://de-iceberg-warehouse/` | Iceberg warehouse location |
| `ICEBERG_CATALOG` | `minio` | Iceberg catalog name |
| `ICEBERG_DATABASE` | `default` | Default Iceberg namespace |
| `LOG_S3_BUCKET` | `de-data-migration-logs` | Bucket for pipeline logs |
| `METADATA_S3_BUCKET` | `de-metadata-bucket` | Bucket for metadata sheets |
| `BREVO_API_KEY` | — | Brevo transactional email API key |
| `BREVO_FROM_EMAIL` | — | Sender address for notifications |
| `NOTIFY_EMAIL` | — | Recipient address for notifications |
| `FLASK_SECRET_KEY` | — | Flask session signing key — generate once, never change |
| `ROOT_EMAIL` | — | Email for the root user created on first startup |
| `ROOT_PASSWORD` | — | Password for root user (remove from `.env` after first login) |
| `FLASK_DEBUG` | `0` | Set to `1` to enable Flask debug mode |
| `CLOUDFLARE_TUNNEL_TOKEN` | — | Token from Cloudflare Zero Trust dashboard |

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

**App returns 502 on Cloudflare**
Check cloudflared is running: `docker compose ps cloudflared`. Confirm it uses `--protocol http2` (not quic) — QUIC fails silently under Docker Desktop's NAT.

**Pipeline container exits immediately**
Check logs: `docker compose logs pipeline`. Ensure `vault-init` completed and `minio` is healthy.

**Vault sealed after restart**
Run `docker compose run --rm vault-init` — detects sealed state and unseals using the saved key in `vault_data`.

**MinIO bucket not found**
Run `docker compose run --rm minio-init` to re-provision buckets.

**Login page: "Account temporarily locked"**
Account locks for 15 minutes after 5 failed attempts. Wait, or manually reset in Supabase: `UPDATE public.app_users SET failed_attempts=0, locked_until=NULL WHERE username='...'`

**Lost root password**
Connect to Supabase directly and update the password hash, or delete the root row and let `seed_root_user.py` recreate it (set `ROOT_PASSWORD` in `.env` first, then restart the UI container).

**Spark downloading JARs on every run**
JARs are baked into the image. If you see Maven downloads, rebuild: `docker compose build --no-cache`.

**s3.atestingdomain.info returns 400 or 403 in browser**
This is expected — the MinIO S3 API requires authenticated S3 requests. Use the MinIO Console at `https://minio.atestingdomain.info` for browser access, or the `mc` CLI for S3 operations:
```bash
mc alias set remote https://s3.atestingdomain.info <access-key> <secret-key>
mc ls remote/
```
