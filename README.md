# DE Metadata Framework

A metadata-driven data ingestion pipeline built on Apache Spark and Apache Iceberg. Reads from S3-compatible storage or PostgreSQL (Supabase), applies schema validation, type casting, and PII processing, then writes Iceberg tables to MinIO. Ships with a Flask web UI with login-based access control, HashiCorp Vault for secrets management, and full Docker orchestration exposed publicly via Cloudflare Tunnel.

---

## Architecture

### High-Level Architecture

Data flows from external sources through the pipeline into a Medallion Iceberg data lake (Lake → Bronze). The web UI is gated behind authentication; consumers query tables directly.

```
                        ┌───────────────────────────────────────────────────────────────────────────────────────────┐
  Raw Data Sources      │                              DE Metadata Framework                                        │   Consumers
                        │                                                                                           │
  ┌──────────────────┐  │  ┌─────────────────────────────────────────────────────────────────────────────────────┐  │
  │  S3 Bucket       │  │  │  STEP 1 — INGESTION                                                                 │  │
  │  · .csv          ├──┼─▶│                                                                                     │  │
  │  · .json         │  │  │  ┌─────────────────────────────────────────────────────────────────────────────┐    │  │
  │  · .parquet      │  │  │  │  Spark ETL Pipeline                                                         │    │  │
  └──────────────────┘  │  │  │  1. Read source data (S3 multi-format or Supabase/PostgreSQL JDBC)          │    │  │
                        │  │  │  2. Apply Metadata Sheet: aliasing · type casting · PII hash/mask/encrypt   │    │  │
  ┌──────────────────┐  │  │  └─────────────────────────────────────────────────────────────────────────────┘    │  │
  │  Supabase /      ├──┼─▶│                                    │                                                │  │
  │  PostgreSQL      │  │  │                                    ▼                                                │  │
  └──────────────────┘  │  │  ┌─────────────────────────────────────────────────────────────────────────────┐    │  │
                        │  │  │  Iceberg Table — LAKE LAYER  (minio.de_lake.<app>)                          │    │  │
  ┌──────────────────┐  │  │  │  data → s3a://de-data-lake/<app>/                                           ├─── ┼──┼──▶ BI / Spark
  │  Metadata Sheet  ├──┼─▶┼  │  meta → s3a://de-iceberg-warehouse-bucket/de_lake/<app>/                    │    │  │    query engine
  │  CSV             │  │  │  │  partition: days(ingest_date)                                               │    │  │
  │  (de-metadata-   │  │  │  └─────────────────────────────────────────────────────────────────────────────┘    │  │
  │   bucket)        │  │  └─────────────────────────────────────────────────────────────────────────────────────┘  │
  │                  │  │                            │                                                              │
  │  (shared by      │  │              ┌─────────────┘  ingestion must complete before bronze runs                  │ 
  │  ingestion and   │  │              ▼                                                                            │
  │  bronze)         │  │  ┌─────────────────────────────────────────────────────────────────────────────────────┐  │
  └──────┬───────────┘  │  │  STEP 2 — BRONZE  (reads Metadata Sheet for primary_key + is_timestamp columns)     │  │
         │              │  │                                                                                     │  │
         └─────────────▶┼──┤  ┌──────────────────────────────┐  ┌──────────────────────────────────────────┐     │  │
                        │  │  │  FULL LOAD                   │  │  DELTA                                   │     │  │
                        │  │  │  ──────────────────────────  │  │  ──────────────────────────────────────  │     │  │
                        │  │  │  1. Read lake table filtered │  │  1. Read lake[ingest_date = run_date]    │     │  │
                        │  │  │     by ingest_date=run_date  │  │  2. Read bronze[snapshot_date<run_date]  │     │  │
                        │  │  │  2. Dedup by primary_key +   │  │  3. Dedup lake by primary_key +          │     │  │
                        │  │  │     timestamp/date col       │  │     timestamp/date col                   │     │  │
                        │  │  │     → keep latest per key    │  │  4. Union new (wins) + prev snapshot     │     │  │
                        │  │  │  3. snapshot_date = run_date │  │  5. snapshot_date = run_date             │     │  │
                        │  │  └──────────────────────────────┘  └──────────────────────────────────────────┘     │  │
                        │  │                                    │                                                │  │
                        │  │                                    ▼                                                │  │
                        │  │  ┌─────────────────────────────────────────────────────────────────────────────┐    │  │
                        │  │  │  Iceberg Table — BRONZE LAYER  (minio.de_bronze.<app>)                      │    │  │
                        │  │  │  data → s3a://de-data-bronze/<app>/                                         ├─── ┼──┼──▶ BI / Spark
                        │  │  │  meta → s3a://de-iceberg-warehouse-bucket/de_bronze/<app>/                  │    │  │    query engine
                        │  │  │  partition: days(snapshot_date)                                             │    │  │
                        │  │  └─────────────────────────────────────────────────────────────────────────────┘    │  │
                        │  └─────────────────────────────────────────────────────────────────────────────────────┘  │
                        │                                                                                           │
                        │  ┌─────────────────────────────────────────────────────────────────────────────────────┐  │
  ┌──────────────────┐  │  │  Flask Web UI (login-protected)                                                     │  │
  │  Engineer        ├──┼─▶│  Submit ingestion and bronze jobs · Stream logs · Manage users · Schedule jobs      │  │
  │  (browser)       │  │  └─────────────────────────────────────────────────────────────────────────────────────┘  │
  └──────────────────┘  └───────────────────────────────────────────────────────────────────────────────────────────┘
                                                        ▲
                                          Cloudflare Tunnel (cloudflared)
                                          app / minio / s3 / vault subdomains
```

**Data flow summary (Medallion Architecture):**

**Step 1 — Ingestion** (`--run-type` not applicable — always a full source read)

| Step | What happens |
|---|---|
| 1 | Engineer submits a job via the Web UI or CLI |
| 2 | UI spawns a fresh pipeline container via Docker socket |
| 3 | Reads **Metadata Sheet CSV** from `de-metadata-bucket` — defines columns, types, aliases, and PII actions |
| 4 | Reads raw source data — **S3** (`.csv` / `.json` / `.parquet`) or **Supabase / PostgreSQL** via JDBC |
| 5 | Applies schema: column aliasing, type casting, PII processing (hash / mask / encrypt) |
| 6 | Writes **Iceberg table** `minio.de_lake.<app>` — data path `s3a://de-data-lake/<app>/`, meta path `s3a://de-iceberg-warehouse-bucket/de_lake/<app>/`, partitioned by `days(ingest_date)` |
| 7 | Uploads run log → `s3a://de-data-migration-logs/lake/<app>/<date>/` |
| 8 | Sends success / failure email via Brevo |

**Step 2 — Bronze** (`--run-type full` or `--run-type delta` — runs after ingestion completes)

Both modes read the **same Metadata Sheet CSV** to identify `primary_key` and `is_timestamp` columns.

| Step | Full Load | Delta |
|---|---|---|
| 1 | Read `minio.de_lake.<app>` filtered by `ingest_date = run_date` | Read `minio.de_lake.<app>` filtered by `ingest_date = run_date` |
| 2 | Dedup by primary key + timestamp/date column → keep the latest record per key | Also read existing `minio.de_bronze.<app>` where `snapshot_date < run_date` |
| 3 | Set `snapshot_date = run_date` on all result rows | Dedup the lake batch by primary key + timestamp/date column |
| 4 | Write result to Bronze Iceberg table | Union new batch (`_priority=0`) with prev snapshot (`_priority=1`); dedup by primary_key — new wins on conflict |
| 5 | — | Write result as new partition `snapshot_date = run_date` |
| 6 | Writes **Iceberg table** `minio.de_bronze.<app>` — data path `s3a://de-data-bronze/<app>/`, meta path `s3a://de-iceberg-warehouse-bucket/de_bronze/<app>/`, partitioned by `days(snapshot_date)` | ← same for both modes |
| 7 | Uploads run log → `s3a://de-data-migration-logs/bronze/<app>/<date>/` | ← same for both modes |

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
| **awscli** | Debug helper — AWS CLI pre-wired to MinIO endpoint | — | — |

#### Docker Network Topology

```
┌────────────────────────────────────────────────────────────────────────────────┐
│  Docker Host                                                                   │
│                                                                                │
│  ┌──────────────────────────── de-net (bridge) ──────────────────────────────┐ │
│  │                                                                           │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐       │ │
│  │  │    minio    │  │    vault    │  │    de-ui    │  │ cloudflared │       │ │
│  │  │  :9000 API  │  │    :8200    │  │    :5000    │  │  (tunnel)   │       │ │
│  │  │:9001 console│  │   Transit   │  │  Flask app  │  │  outbound   │       │ │
│  │  │             │  │   KV v2     │  │  Auth/CSRF  │  │  HTTP/2     │       │ │
│  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘       │ │
│  │         │                │                │                │              │ │
│  │         │                │                 │               │              │ │
│  │         │      vault_secrets volume        │     ┌──────┘─────────────┐   │ │         
│  │         │      (pipeline_token,            │     │ routes:            │   │ │
│  │         │       supabase_pwd_ciphertext)   │     │ app.   → de-ui     │   │ │
│  │         │                │                 │     │ s3.    → minio:9000│   │ │
│  │         │                ▼                 │     │ minio. → :9001     │   │ │
│  │         │    ┌───────────────────────┐     │     │ vault. → vault     │   │ │
│  │         └───▶│  pipeline (ephemeral) │◀────┘     └────────────────────┘   │ │
│  │              │  spawned via Docker   │                                    │ │
│  │              │  sock; Spark 4.x      │                                    │ │
│  │              │  local[*], auto-rm    │                                    │ │
│  │              └───────────────────────┘                                    │ │
│  └───────────────────────────────────────────────────────────────────────────┘ │
│                                                                                │
│  /var/run/docker.sock  (de-ui mounts host socket to spawn pipeline)            │
└────────────────────────────────────────────────────────────────────────────────┘
                                    │
                   Cloudflare Tunnel (HTTPS, HTTP/2)
                                    │
                    ┌───────────────┴───────────────┐
                    │      atestingdomain.info       │
                    │  app.*  minio.*  s3.*  vault.* │
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

#### Spark Execution Flow — Lake Pipeline

```
  pipeline.py (spark-submit entry point)
  │
  ├── 1. Parse CLI args (argparse)
  ├── 2. Load env config  (DE_Ingestion_properties.py)
  ├── 3. Build SparkSession  (spark_session.py)
  │       └── Config from conf/spark-defaults.conf
  │           ├── Iceberg extensions + hadoop catalog
  │           ├── S3A credentials + endpoint (injected by entrypoint.sh)
  │           └── Iceberg catalog: minio → s3a://de-iceberg-warehouse-bucket/
  │
  ├── 4. Read metadata sheet CSV  (metadata_reader.py)
  │       └── s3a://de-metadata-bucket/<key>
  │
  ├── 5. Read source data
  │       ├── S3 path  → SparkReader.parquet / csv / json  (s3a://)
  │       └── Database → SparkReader.jdbc  (Supabase / PostgreSQL)
  │
  ├── 6. Schema validation → 7. Type casting → 8. PII processing
  │       ├── hash    → HMAC-SHA256 with SALT_KEY
  │       ├── mask    → replace with "****"
  │       └── encrypt → AES-256-GCM with key from Vault Transit export
  │
  ├── 9. Write Iceberg table  (minio_writer.py)
  │       ├── Table:     minio.de_lake.<app_name>
  │       ├── Data path: s3a://de-data-lake/<app_name>/                       (write.data.path)
  │       ├── Meta path: s3a://de-iceberg-warehouse-bucket/de_lake/<app_name>/  (write.meta.path)
  │       ├── Partition: days(ingest_date)
  │       └── Modes: overwrite | append | replace
  │
  ├── 10. Upload log  → s3a://de-data-migration-logs/lake/<app>/<date>/
  └── 11. Email notification via Brevo
```

#### Spark Execution Flow — Bronze Pipeline

```
  bronze_pipeline.py (spark-submit entry point)
  │
  ├── 1. Parse CLI args (--application-name, --run-date, --run-type)
  ├── 2. Build SparkSession  (same spark-defaults.conf)
  ├── 3. Read metadata sheet → extract primary_key and is_timestamp columns
  │
  ├── 4. run_full OR run_delta  (bronze_processor.py)
  │
  │   run_full (--run-type full)
  │   ├── Read:  minio.de_lake.<app> WHERE ingest_date = run_date
  │   ├── Dedup: Window(partitionBy=primary_keys, orderBy=timestamp desc) → row_number=1
  │   │          (or dropDuplicates if no timestamp column)
  │   ├── Add:   snapshot_date = run_date
  │   └── Write: createOrReplace (first run) or overwritePartitions (subsequent)
  │
  │   run_delta (--run-type delta)
  │   ├── Read:  minio.de_lake.<app> WHERE ingest_date = run_date  → dedup
  │   ├── If bronze table is new → fall back to full write
  │   └── Else →
  │       ├── Read bronze WHERE snapshot_date = MAX(snapshot_date < run_date)
  │       ├── Union: df_new(_priority=0) + df_prev(_priority=1)
  │       ├── Window.partitionBy(*PKs).orderBy(_priority) → keep row_number=1
  │       └── Write new partition: snapshot_date = run_date
  │
  └── 5. Write Iceberg table  (_write_bronze)
          ├── Table:     minio.de_bronze.<app_name>
          ├── Data path: s3a://de-data-bronze/<app_name>/                         (write.data.path)
          ├── Meta path: s3a://de-iceberg-warehouse-bucket/de_bronze/<app_name>/ (write.meta.path)
          └── Partition: days(snapshot_date)
```

#### MinIO Bucket Layout

```
  MinIO
  │
  ├── de-source-data-bucket/              ← raw input files (S3 source type)
  │   └── <folder>/<file>
  │
  ├── de-metadata-bucket/                 ← metadata CSV sheets
  │   └── <dataset>_metadata.csv
  │
  ├── de-data-lake/                       ← LAKE LAYER: Iceberg Parquet data files
  │   └── <app_name>/                     │  (write.data.path)
  │       └── data/                       │
  │           └── ingest_date=YYYY-MM-DD/ │  daily partitions
  │               └── *.parquet           │
  │
  ├── de-data-bronze/                     ← BRONZE LAYER: deduplicated Parquet data
  │   └── <app_name>/                     │  (write.data.path)
  │       └── data/                       │
  │           └── snapshot_date_day=*/    │  daily partitions
  │               └── *.parquet           │
  │
  ├── de-iceberg-warehouse-bucket/        ← Iceberg metadata (manifests + snapshots)
  │   ├── de_lake/                        │  Lake meta (write.meta.path)
  │   │   └── <app_name>/
  │   │       └── metadata/               │  *.avro, *.json, snap-*.avro
  │   └── de_bronze/                      │  Bronze meta (write.meta.path)
  │       └── <app_name>/
  │           └── metadata/               │  *.avro, *.json, snap-*.avro
  │
  └── de-data-migration-logs/             ← pipeline run logs
      ├── lake/<app>/<date>/<app>_<date>_<spark-id>.log
      └── bronze/<app>/<date>/bronze_<app>_<date>_<spark-id>.log
```

#### Iceberg Data Path vs Meta Path

Iceberg splits each table into two storage locations, set via `DataFrameWriter.tableProperty`:

| Property | Purpose | Lake value | Bronze value |
|---|---|---|---|
| `write.data.path` | Parquet data files | `s3a://de-data-lake/<app>/` | `s3a://de-data-bronze/<app>/` |
| `write.meta.path` | Metadata files (manifests, snapshots, schema) | `s3a://de-iceberg-warehouse-bucket/de_lake/<app>/` | `s3a://de-iceberg-warehouse-bucket/de_bronze/<app>/` |

Keeping data and metadata in separate buckets means:
- **Data bucket** can be lifecycle-managed or swapped independently of the catalog metadata
- **Warehouse bucket** holds only lightweight JSON/Avro metadata — easy to back up and inspect
- Both lake and bronze metadata coexist in `de-iceberg-warehouse-bucket` under different prefixes, so a single Iceberg catalog (`minio`) covers all layers

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
│   ├── pip_install.sh                # Smart pip installer (skips cached versions)
│   └── awscli-entrypoint.sh          # AWS CLI entrypoint pre-wired to MinIO credentials
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
├── bronze_layer/
│   ├── bronze_pipeline.py            # Bronze spark-submit entry point
│   └── bronze_processor.py           # Dedup logic: extract_bronze_keys, run_full, run_delta
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
SOURCE_S3_BUCKET=de-source-data-bucket

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
ICEBERG_WAREHOUSE=s3a://de-iceberg-warehouse-bucket/
ICEBERG_DATA_BUCKET=de-data-lake
ICEBERG_METADATA_BUCKET=de-iceberg-warehouse-bucket
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
spark.sql.catalog.minio.warehouse              s3a://de-iceberg-warehouse-bucket/
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

### S3 Source Key Date Resolution

The `--source-key` argument supports automatic date resolution using `__` as a mandatory separator between the static prefix and the date component:

- **Exact match** — key is used as-is if the object exists
- **Token substitution** — if the key contains `__<TOKEN>` (e.g. `file__YYYY-MM-DD.csv`), the token is replaced with `ingest_date` in the matching format
- **Prefix listing** — if the key contains `__` but no known token, S3 objects are listed under the prefix before `__` and matched by date
- **No `__` separator** — if the key has no `__` and the exact object doesn't exist, the job fails immediately (no date substitution is attempted)

Supported tokens (all require `__` prefix in the key): `YYYY-MM-DD`, `DD-MM-YYYY`, `MM-DD-YYYY`, `YYYY_MM_DD`, `DD_MM_YYYY`, `MM_DD_YYYY`, `YYYYMMDD`, `DDMMYYYY`

Example:
```
source_key = data/sales__YYYY-MM-DD.csv   → resolves to data/sales__2026-01-15.csv
source_key = data/sales__20260115.csv     → used as-is (exact match)
source_key = data/sales.csv               → used as-is or fails if not found
```

### Running the Bronze Pipeline

The bronze layer reads from `minio.de_lake.<app>`, deduplicates using primary keys from the metadata sheet, and writes to `minio.de_bronze.<app>` (partitioned by `days(snapshot_date)`).

**Full run** (processes a single `ingest_date` partition from the lake):

```bash
docker compose run --rm pipeline bronze \
  --application-name my_dataset \
  --run-date 2026-01-15 \
  --run-type full \
  --metadata-key metadata/my_dataset_schema.csv \
  --log-level INFO
```

**Delta run** (merges lake data with the previous bronze snapshot (union + priority dedup)):

```bash
docker compose run --rm pipeline bronze \
  --application-name my_dataset \
  --run-date 2026-01-15 \
  --run-type delta \
  --metadata-key metadata/my_dataset_schema.csv
```

> **Typical workflow:** run ingestion first to land data in the lake, then run bronze to produce the deduplicated snapshot.

### Interactive Spark Shell (Debugging)

To open an interactive shell inside the pipeline container with Spark credentials already injected:

```bash
docker compose run --rm -it pipeline shell
```

This runs the entrypoint seds (substituting MinIO credentials into `spark-defaults.conf`) before dropping into `/bin/sh`. From there you can run `spark-sql` or `pyspark` and query Iceberg tables:

```sql
-- inside spark-sql
SHOW TABLES IN minio.de_lake;
SHOW TABLES IN minio.de_bronze;
SELECT * FROM minio.de_lake.my_dataset LIMIT 10;
```

> **Never use** `--entrypoint /bin/sh` directly — that bypasses the entrypoint and leaves `MINIO_ACCESS_KEY_PLACEHOLDER` unreplaced in the Spark config, causing 403 errors.

---

## Scheduled Jobs

The UI supports APScheduler-backed scheduled pipeline jobs stored in PostgreSQL.

**Database tables:**
- `public.scheduled_jobs` — stores all active schedules (application_name is unique — saving again updates the existing schedule)
- `public.scheduled_job_runs` — stores run history including full container stdout/stderr logs per execution

**Scheduling a job:**
1. Fill in the pipeline form as normal
2. Check "Schedule this job" — if the application_name already has a saved schedule, the cron expression is pre-filled and the button shows "Update Schedule"
3. Enter a cron expression (5-part: `min hour day month weekday`). A human-readable description is shown live (e.g. `0 2 * * 1` → "At 02:00 AM on Monday")
4. Click "Save Schedule" / "Update Schedule"

**Scheduled Jobs page:**
Click "📅 Scheduled Jobs" in the top navbar to open a full-page overlay with:
- **Left panel** — all active scheduled jobs with name, cron expression, human-readable schedule, and last-run status badge
- **Right panel** — run history for the selected job: each run shows status badge, run date, start timestamp, and expandable full container logs (stdout + stderr)

**APScheduler behaviour:**
- Scheduler starts with the UI container and loads all active jobs from DB on startup (survives container restarts via DB persistence)
- Each scheduled trigger runs the ingestion pipeline (lake layer only) with `ingest_date = today()`
- After each run, `last_run_at`, `last_run_status`, and full logs are written to `public.scheduled_job_runs`
- Deleting a schedule soft-deletes the DB row (`is_active = FALSE`) and removes the APScheduler trigger

**New Flask routes:**
| Route | Method | Description |
|---|---|---|
| `/scheduled-jobs` | GET | List all active scheduled jobs |
| `/scheduled-jobs` | POST | Create or update a scheduled job (upsert on application_name) |
| `/scheduled-jobs/<id>` | DELETE | Soft-delete and remove from scheduler |
| `/scheduled-job-runs` | GET | List run history (filter by `?job_id=<id>`) |

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

## Bronze Pipeline CLI Reference

| Argument | Required | Default | Description |
|---|---|---|---|
| `--application-name` | Yes | — | Dataset identifier (must match the lake table name) |
| `--run-date` | Yes | — | Date to process (`YYYY-MM-DD`) |
| `--run-type` | No | `full` | `full` = lake run_date partition only; `delta` = snapshot union merge |
| `--catalog` | No | `ICEBERG_CATALOG` env | Iceberg catalog name |
| `--lake-database` | No | `LAKE_DATABASE` env | Source Iceberg namespace (default: `de_lake`) |
| `--bronze-database` | No | `BRONZE_DATABASE` env | Target Iceberg namespace (default: `de_bronze`) |
| `--metadata-key` | No | env | S3 key of the metadata sheet CSV |
| `--log-level` | No | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `--log-folder` | No | `bronze` | Top-level folder in log bucket |

---

## Metadata Sheet Format

The pipeline is driven by a CSV metadata sheet uploaded to MinIO. Each row defines one column:

### Lake ingestion columns

| Column | Description |
|---|---|
| `column_name` | Exact column name in the source data |
| `data_type` | Target type: `string`, `integer`, `double`, `date`, `timestamp`, `boolean` |
| `nullable` | `true` / `false` |
| `pii_action` | `hash`, `mask`, `encrypt`, or blank (no action) |
| `description` | Human-readable description |

### Additional columns for the bronze layer

| Column | Description |
|---|---|
| `primary_key` | `true` / `yes` / `1` — marks columns that together form the unique row key (used for deduplication) |
| `is_timestamp` | `true` / `yes` / `1` — marks the timestamp column used to keep the latest row when duplicates exist on the primary key |

> The bronze layer uses `primary_key` columns to deduplicate. If `is_timestamp` is set, the row with the highest timestamp value is kept; otherwise `dropDuplicates` is used. At least one `primary_key` column is required for `delta` mode.

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
| `LAKE_DATABASE` | `de_lake` | Iceberg namespace for lake layer tables |
| `BRONZE_DATABASE` | `de_bronze` | Iceberg namespace for bronze layer tables |
| `BRONZE_DATA_BUCKET` | `de-data-bronze` | MinIO bucket for bronze Iceberg Parquet data |
| `MINIO_ENDPOINT` | `http://minio:9000` | MinIO S3 API URL (internal) |
| `MINIO_SERVER_URL` | `http://localhost:9000` | MinIO S3 public URL (embedded in redirects) |
| `MINIO_BROWSER_REDIRECT_URL` | `http://localhost:9001` | MinIO Console public URL |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `minioadmin` | MinIO secret key |
| `MINIO_BUCKET` | `de-data-lake` | Sink bucket for Iceberg data |
| `SOURCE_S3_ENDPOINT` | `http://minio:9000` | Source S3 endpoint (internal) |
| `SOURCE_S3_ACCESS_KEY` | `minioadmin` | Source access key |
| `SOURCE_S3_SECRET_KEY` | `minioadmin` | Source secret key |
| `SOURCE_S3_BUCKET` | `de-source-data-bucket` | Source bucket name |
| `SUPABASE_JDBC_URL` | — | PostgreSQL JDBC connection string (direct host) |
| `AUTH_DATABASE_URL` | — | PostgreSQL pooler URL for auth DB (IPv4, preferred over SUPABASE_JDBC_URL) |
| `SUPABASE_DB_USER` | `postgres` | Database user |
| `SUPABASE_DB_PASSWORD_PLAIN` | — | Plain password for vault-init encryption only |
| `VAULT_ADDR` | `http://vault:8200` | Vault API address |
| `VAULT_TOKEN` | — | Pipeline-scoped Vault token |
| `VAULT_PATH` | `encryption/pii` | KV path for PII secrets |
| `SALT_KEY` | — | HMAC-SHA256 salt for hashing |
| `SALT_2` | — | Secondary salt seeded into Vault KV |
| `ICEBERG_WAREHOUSE` | `s3a://de-iceberg-warehouse-bucket/` | Iceberg catalog warehouse root |
| `ICEBERG_DATA_BUCKET` | `de-data-lake` | Bucket for Iceberg Parquet data files |
| `ICEBERG_METADATA_BUCKET` | `de-iceberg-warehouse-bucket` | Bucket for Iceberg table metadata |
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
| `bronze_layer/**` | `docker compose build` (bronze_layer.zip is rebuilt in the image) |
| `docker-compose.yml` only | `docker compose up -d` (no rebuild) |
| `.env` only | `docker compose up -d` (no rebuild) |

---

## Troubleshooting

**App returns 502 on Cloudflare (intermittent — works on some devices, not others)**
You likely have two cloudflared connectors attached to the same tunnel. This happens if a native `cloudflared` daemon (e.g. installed via Homebrew) is running alongside the Docker `cloudflared` container. Cloudflare round-robins between connectors — the native one can't reach `de-ui:5000` inside Docker networking, causing alternating 502s.

Check in Cloudflare Zero Trust → Networks → Tunnels → your tunnel → Connectors. If you see two connectors, disable the native one:
```bash
sudo launchctl stop com.cloudflare.cloudflared
sudo launchctl disable system/com.cloudflare.cloudflared
```

Only the Docker `cloudflared` container should be connected. Verify with `docker compose ps cloudflared` and confirm it uses `--protocol http2` (not quic) — QUIC fails silently under Docker Desktop's NAT.

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

### MinIO CLI Access

The `awscli` service provides an AWS CLI pre-configured to talk to MinIO — no `--endpoint-url` flag needed. The `docker/awscli-entrypoint.sh` entrypoint pre-sets `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_ENDPOINT_URL` from env vars automatically.

```bash
# Open an interactive shell with AWS CLI pre-configured for MinIO
docker compose run --rm awscli shell

# Run AWS CLI commands directly against MinIO
docker compose run --rm awscli s3 ls
docker compose run --rm awscli s3 ls s3://de-source-data-bucket/
```
