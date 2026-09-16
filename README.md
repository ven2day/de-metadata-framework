# DE Metadata Framework

A self-hosted, end-to-end data engineering platform implementing the **Medallion Architecture** (Lake → Bronze → Silver → Gold) with a metadata-driven pipeline, web UI, real-time log streaming, and Oracle ADW integration — fully containerised with Docker Compose.

---
<img width="1672" height="941" alt="image" src="https://github.com/user-attachments/assets/c183341e-a38e-4ef3-bcd4-8378613cac41" />

---

## Table of Contents

1. [Architecture Overview (HLD)](#architecture-overview-hld)
2. [Data Pipeline Flow](#data-pipeline-flow)
3. [Low-Level Design (LLD)](#low-level-design-lld)
4. [Services & Infrastructure](#services--infrastructure)
5. [Folder Structure](#folder-structure)
6. [Layer Functionality](#layer-functionality)
7. [Web UI Features](#web-ui-features)
8. [Scheduling System](#scheduling-system)
9. [Security & Secrets Management](#security--secrets-management)
10. [API Reference](#api-reference)
11. [Environment Variables](#environment-variables)
12. [Setup & Deployment](#setup--deployment)
13. [Tech Stack](#tech-stack)

---

## Architecture Overview (HLD)

```
  ┌───────────────────────────────────────────────────────────┐
  │                      DATA SOURCES                         │
  │   ┌─────────────────────────┐  ┌────────────────────────┐ │
  │   │   Object Storage        │  │  Operational Databases │ │
  │   │   AWS S3  ·  MinIO      │  │  Supabase · PostgreSQL │ │
  │   │   CSV · JSON · Parquet  │  │  (and others)          │ │
  │   └────────────┬────────────┘  └────────────┬───────────┘ │
  └────────────────┼──────────────────────────-─┼─────────────┘
                   └─────────────┬──────────────┘
                                 │  Extract
                                 ▼
  ┌───────────────────────────────────────────────────────────┐
  │  INGESTION LAYER                                          │
  │  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  · │
  │  ①  Schema & column correctness validation               │
  │  ②  Data quality checks                                  │
  │  ③  Metadata-driven PII processing  (Data Governance)    │
  │       Hash    —  HMAC-SHA256 + SALT  (Vault KV)          │
  │       Mask    —  Partial redaction   AB****YZ            │
  │       Encrypt —  AES-256  (Vault Transit engine)         │
  │  ④  Partition by  ingest_date                            │
  │  ⑤  Write Iceberg table  →  de_lake                     │
  └─────────────────────────────┬─────────────────────────────┘
                                │
                                ▼
  ╔═══════════════════════════════════════════════════════════╗
  ║  LAKE  ·  de_lake                                         ║
  ║  Iceberg  ·  Parquet  ·  partitioned by  ingest_date      ║
  ╚═══════════════════════════════════════════════════════════╝
                                │
                                ▼
  ┌───────────────────────────────────────────────────────────┐
  │  BRONZE LAYER                                             │
  │  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  · │
  │  ①  Fetch lake data for current  ingest_date             │
  │  ②  Compare against last available Bronze  snapshot_date │
  │  ③  Dedup by Primary Keys                                │
  │       Full  —  complete table refresh                    │
  │       Delta —  DELETE stale PKs  →  INSERT new rows      │
  │  ④  Partition by  snapshot_date                          │
  │  ⑤  Write Iceberg table  →  de_bronze                   │
  └─────────────────────────────┬─────────────────────────────┘
                                │
                                ▼
  ╔═══════════════════════════════════════════════════════════╗
  ║  BRONZE  ·  de_bronze                                     ║
  ║  Iceberg  ·  Parquet  ·  partitioned by  snapshot_date    ║
  ╚═══════════════════════════════════════════════════════════╝
                                │
                                ▼
  ┌──────────────────────────────────────────── DBT + Trino ──┐
  │  SILVER LAYER                                             │
  │  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  · │
  │  ①  SQL transformations on Bronze Iceberg tables         │
  │  ②  Metadata-driven column mapping & business logic      │
  │  ③  Materialization strategy                             │
  │       append        —  INSERT new rows                   │
  │       overwrite     —  DROP table  →  full reload        │
  │       incremental   —  upsert by primary key             │
  │  ④  Write Iceberg table  →  de_silver                   │
  └─────────────────────────────┬─────────────────────────────┘
                                │
                                ▼
  ╔═══════════════════════════════════════════════════════════╗
  ║  SILVER  ·  de_silver                                     ║
  ║  Iceberg  ·  Parquet  ·  Snappy compressed                ║
  ╚═══════════════════════════════════════════════════════════╝
                                │
                                ▼
  ┌───────────────────────────────────────────────────────────┐
  │  GOLD LAYER                                               │
  │  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  ·  · │
  │  ①  Trino SELECT *  from  de_silver.<table>              │
  │  ②  Load strategy                                        │
  │       append          —  INSERT all rows                 │
  │       truncate         —  TRUNCATE  →  INSERT            │
  │       delete_and_insert —  DELETE date partition → INSERT │
  │  ③  Trino → Oracle type mapping                         │
  │  ④  Batch insert  (5 000 rows / batch)                  │
  │  ⑤  Write  →  Oracle Autonomous AI Data Warehouse       │
  └─────────────────────────────┬─────────────────────────────┘
                                │
                                ▼
  ╔═══════════════════════════════════════════════════════════╗
  ║  GOLD  ·  Oracle Autonomous AI Data Warehouse             ║
  ║  Auto-provisioned tables  ·  Type-mapped columns          ║
  ╚═══════════════════════════════════════════════════════════╝
                                │
                                ▼
  ┌───────────────────────────────────────────────────────────┐
  │  BI / Analytics Consumers                                 │
  │  Dashboards  ·  Reports  ·  Data Products                 │
  └───────────────────────────────────────────────────────────┘

  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
  PLATFORM INFRASTRUCTURE  (Docker Compose)

  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐
  │  MinIO  :9000   │  │ HashiCorp Vault  │  │     Hive     │
  │  S3 Object Store│  │ :8200           │  │  Metastore   │
  │  8 S3 buckets   │  │ PII keys        │  │  :9083       │
  │                 │  │ Oracle ADW creds│  │  Iceberg     │
  │                 │  │ Supabase pwd    │  │  Catalog     │
  └─────────────────┘  └─────────────────┘  └──────────────┘
  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐
  │  Trino  :8081   │  │  Flask Web UI   │  │  Cloudflare  │
  │  SQL Engine     │  │  :5001          │  │  Tunnel      │
  │  DBT adapter    │  │  Pipeline ctrl  │  │  HTTPS proxy │
  │                 │  │  Scheduling     │  │  ·.info      │
  │                 │  │  Log streaming  │  │              │
  └─────────────────┘  └─────────────────┘  └──────────────┘
```

---

## Data Pipeline Flow

```
  ┌────────────────────────────────────────────────────────┐
  │  1. INGESTION LAYER  (Lake)                            │
  │                                                        │
  │  Source S3 Bucket  /  Supabase PostgreSQL              │
  │         │                                              │
  │         ▼                                              │
  │  Schema validation & type casting                      │
  │         │                                              │
  │         ▼                                              │
  │  PII processing  (Hash / Mask / AES-256 via Vault)     │
  │         │                                              │
  │         ▼                                              │
  │  Write Parquet  ──►  de-data-lake                      │
  └──────────────────────────┬─────────────────────────────┘
                             │
  ┌──────────────────────────▼─────────────────────────────┐
  │  2. BRONZE LAYER                                       │
  │                                                        │
  │  Read Parquet from de-data-lake  (Spark)               │
  │         │                                              │
  │         ├── full  ──►  Drop & recreate Iceberg table   │
  │         │                                              │
  │         └── delta ──►  Dedup by PK + timestamp         │
  │                        DELETE matching PKs, INSERT new │
  │         │                                              │
  │         ▼                                              │
  │  Write Iceberg  ──►  de-data-bronze                    │
  └──────────────────────────┬─────────────────────────────┘
                             │
  ┌──────────────────────────▼─────────────────────────────┐
  │  3. SILVER LAYER  (DBT + Trino)                        │
  │                                                        │
  │  UI column mapper  ──►  generate DBT model SQL         │
  │         │                                              │
  │         ▼                                              │
  │  Write  transformation/models/silver/<model>.sql       │
  │         │                                              │
  │         ▼                                              │
  │  dbt run --select <model>  via Trino adapter           │
  │         │                                              │
  │         ▼                                              │
  │  Verify row count  (Trino SELECT COUNT)                │
  │         │                                              │
  │         ▼                                              │
  │  Write Iceberg  ──►  de-data-silver                    │
  └──────────────────────────┬─────────────────────────────┘
                             │
  ┌──────────────────────────▼─────────────────────────────┐
  │  4. GOLD LAYER  (Oracle ADW)                           │
  │                                                        │
  │  Trino SELECT * from minio.de_silver.<table>           │
  │         │                                              │
  │         ├── append          ──►  INSERT 5 000 rows     │
  │         ├── truncate         ──►  TRUNCATE + INSERT    │
  │         └── delete_and_insert ──►  DELETE date+INSERT  │
  │         │                                              │
  │         ▼                                              │
  │  Oracle ADW  target table                              │
  └────────────────────────────────────────────────────────┘
```

---

## Low-Level Design (LLD)

### Ingestion Layer — Component Detail

```
  SOURCE READERS
  ┌────────────────────────────┐  ┌──────────────────────────┐
  │  s3_reader.py              │  │  supabase_reader.py      │
  │  Read CSV/JSON/Parquet     │  │  Read PostgreSQL tables  │
  └──────────────┬─────────────┘  └──────────────┬───────────┘
                 └──────────────┬─────────────────┘
                                │
                                ▼
  ┌────────────────────────────────────────────────────────┐
  │  connectivity_checker.py                               │
  │  Pre-flight S3 + Vault reachability checks             │
  │         │                                              │
  │         ▼                                              │
  │  metadata_reader.py                                    │
  │  Load column definitions CSV from de-metadata-bucket   │
  │         │                                              │
  │         ▼                                              │
  │  schema_validator.py                                   │
  │  Enforce expected columns, reject unexpected fields    │
  │         │                                              │
  │         ▼                                              │
  │  type_caster.py                                        │
  │  Coerce columns to target types per metadata           │
  │         │                                              │
  │         ▼                                              │
  │  pii_processor.py  ──►  vault_client.py (KV v2)       │
  │  Apply per-column security_level:                      │
  │    hash    ── HMAC-SHA256 + SALT_2 from Vault          │
  │    mask    ── Partial redaction  AB****YZ              │
  │    encrypt ── AES-256 via Vault Transit engine         │
  └──────────────────────────┬─────────────────────────────┘
                             │
                             ▼
  ┌────────────────────────────────────────────────────────┐
  │  minio_writer.py                                       │
  │  Write Parquet (Snappy) ──►  de-data-lake/<app>/       │
  │                                                        │
  │  email_notifier.py  ── Success / failure alerts        │
  └────────────────────────────────────────────────────────┘
```

### Bronze Layer — Component Detail

```
  INPUT
  ┌──────────────────────────┐  ┌──────────────────────────┐
  │  de-data-lake            │  │  metadata_reader.py      │
  │  (Parquet files)         │  │  Primary keys, timestamp │
  └────────────┬─────────────┘  └────────────┬─────────────┘
               └──────────────┬──────────────┘
                              │
  bronze_processor.py         ▼
  ┌────────────────────────────────────────────────────────┐
  │  extract_bronze_keys()                                 │
  │  Detect PK columns + optional timestamp column         │
  │         │                                              │
  │         ▼                                              │
  │  _dedup()                                              │
  │  ROW_NUMBER OVER (PARTITION BY pk ORDER BY ts DESC)    │
  │  Keeps latest record per key                           │
  └──────────────────────────┬─────────────────────────────┘
                             │
  bronze_pipeline.py         ▼
  ┌────────────────────────────────────────────────────────┐
  │  Full  ──►  Drop Iceberg table  →  _write_bronze()     │
  │  Delta ──►  DELETE matching PKs  →  INSERT new rows    │
  │             →  _write_bronze()                         │
  └──────────────────────────┬─────────────────────────────┘
                             │
  OUTPUT                     ▼
  ┌────────────────────────────────────────────────────────┐
  │  de-data-bronze/<app>/                                 │
  │  Iceberg data files (Parquet / Snappy)                 │
  │                                                        │
  │  de-iceberg-warehouse-bucket/de_bronze/<app>/          │
  │  Iceberg metadata (JSON manifests, Avro snapshots)     │
  └────────────────────────────────────────────────────────┘
```

### Silver Layer — Component Detail

```
  UI — Silver Panel
  ┌────────────────────────────────────────────────────────┐
  │  Source table selector  ·  Column mapper               │
  │  Materialization  ·  Preview SQL  ·  Flowchart         │
  │  AI SQL assist  ·  DWH Gold Load Strategy              │
  │                                                        │
  │  [ Transform ]           ──►  /silver/run-transform    │
  │  [ Run Transform & Load ] ──►  /run-transform-load     │
  │                                (silver → gold chain)  │
  └──────────────────────────┬─────────────────────────────┘
                             │
  app.py                     ▼
  ┌────────────────────────────────────────────────────────┐
  │  _write_silver_dbt_model()                             │
  │  Generate transformation/models/silver/<model>.sql     │
  │         │                                              │
  │         ▼                                              │
  │  _ensure_silver_sources()                              │
  │  Create / update models/silver/sources.yml             │
  │         │                                              │
  │         ▼  (overwrite strategy only)                   │
  │  _overwrite_silver_cleanup()                           │
  │  Drop Iceberg table + clear S3 prefix                  │
  │         │                                              │
  │         ▼                                              │
  │  dbt run --select <model> --target docker              │
  │  Stream output line-by-line  →  log panel              │
  │  Regex match  →  Silver pipeline progress dots         │
  │         │                                              │
  │         ▼                                              │
  │  Trino SELECT COUNT(*)  ── Row count verification      │
  └──────────────────────────┬─────────────────────────────┘
                             │
  transformation/            ▼
  ┌────────────────────────────────────────────────────────┐
  │  profiles.yml       ── Trino host: trino  port: 8080   │
  │  macros/            ── get_snapshot_date               │
  │                        create_silver_table             │
  │                        append_silver_table             │
  │                        insert_overwrite_silver_table   │
  └──────────────────────────┬─────────────────────────────┘
                             │
  OUTPUT                     ▼
  ┌────────────────────────────────────────────────────────┐
  │  de-data-silver/<table>/                               │
  │  Iceberg data files (Parquet / Snappy)                 │
  │                                                        │
  │  de-iceberg-warehouse-bucket/de_silver/<table>/        │
  │  Iceberg metadata (JSON manifests, Avro snapshots)     │
  └────────────────────────────────────────────────────────┘
```

### Gold Layer — Component Detail

```
  INPUT
  ┌──────────────────────────┐  ┌──────────────────────────┐
  │  de-data-silver          │  │  Vault: secret/oracle/adw│
  │  Iceberg table via Trino │  │  user · password · DSN   │
  └────────────┬─────────────┘  │  wallet ZIP (base64)     │
               │                └────────────┬─────────────┘
               └──────────────┬──────────────┘
                              │
  oracle_loader.py            ▼
  ┌────────────────────────────────────────────────────────┐
  │  _read_oracle_creds()                                  │
  │  Fetch credentials from Vault KV v2                    │
  │         │                                              │
  │         ▼                                              │
  │  _setup_wallet()                                       │
  │  Base64 decode + unzip wallet to temp directory        │
  │         │                                              │
  │         ▼                                              │
  │  _trino_type_to_oracle()                               │
  │  Map Trino column types  →  Oracle column types        │
  │         │                                              │
  │         ▼                                              │
  │  _ensure_table()                                       │
  │  CREATE TABLE IF NOT EXISTS in Oracle ADW              │
  │         │                                              │
  │         ├── append          ──►  INSERT 5 000 rows     │
  │         ├── truncate         ──►  TRUNCATE + INSERT    │
  │         └── delete_and_insert ──►  DELETE date+INSERT  │
  └──────────────────────────┬─────────────────────────────┘
                             │
  OUTPUT                     ▼
  ┌────────────────────────────────────────────────────────┐
  │  Oracle ADW  target table                              │
  │  Auto-created if not exists  ·  Batched (5 000 rows)   │
  └────────────────────────────────────────────────────────┘
```

---

## Services & Infrastructure

### Docker Services

| Service | Container | Image | Port(s) | Purpose |
|---|---|---|---|---|
| MinIO | `minio` | `minio/minio:latest` | `9000` (S3 API), `9001` (Console) | S3-compatible object store for all data lake buckets |
| MinIO Init | `minio-init` | `minio/mc:latest` | — | One-shot bucket provisioner (8 buckets) |
| HashiCorp Vault | `vault` | `hashicorp/vault:latest` | `8200` | Secrets store — PII keys, Oracle ADW credentials |
| Vault Init | `vault-init` | custom | — | Daemon: unseal + seed secrets, monitors for re-seal |
| PostgreSQL | `postgres-meta` | `postgres:15` | — | Hive Metastore backend database |
| Hive Metastore | `hive-metastore` | custom (`Dockerfile.hms`) | `9083` | Iceberg catalog registry (Thrift protocol) |
| Trino | `trino` | `trinodb/trino:482` | `8081` → `8080` | Distributed SQL engine for DBT silver transforms |
| Flask UI | `de-ui` | custom (`Dockerfile`) | `5001` → `5000` | Web UI — pipeline control, scheduling, monitoring |
| Cloudflare Tunnel | `cloudflared` | `cloudflare/cloudflared:latest` | — | HTTPS reverse proxy to `atestingdomain.info` |
| Pipeline Runner | `de-pipeline` | custom (`Dockerfile`) | — | On-demand Spark runner for ingestion and bronze |

### MinIO S3 Buckets

| Bucket | Layer | Contents |
|---|---|---|
| `de-data-lake` | Lake | Raw Parquet files from ingestion (after PII processing) |
| `de-source-data-bucket` | Ingestion | Source CSV/JSON/Parquet uploaded by the user |
| `de-data-bronze` | Bronze | Iceberg data files (Parquet/Snappy) |
| `de-iceberg-warehouse-bucket` | Bronze + Silver | Iceberg metadata (JSON manifests, Avro snapshots) |
| `de-data-silver` | Silver | DBT-produced Iceberg data files |
| `de-data-transformation` | Silver | DBT compilation artefacts |
| `de-metadata-bucket` | All | Metadata CSV sheets (column definitions per app) |
| `de-data-migration-logs` | All | Pipeline execution logs |

### Cloudflare Tunnel Routes

| Domain | Backend Service | Port |
|---|---|---|
| `atestingdomain.info` | Flask UI | 5000 |
| `minio.atestingdomain.info` | MinIO Console | 9001 |
| `vault.atestingdomain.info` | HashiCorp Vault UI | 8200 |

---

## Folder Structure

```
de-metadata-framework/
├── docker-compose.yml              # Full service graph
├── Dockerfile                      # UI + pipeline image
├── docker/
│   ├── Dockerfile.hms              # Hive Metastore image
│   ├── entrypoint.sh               # Pipeline entrypoint (spark-submit)
│   ├── ui-entrypoint.sh            # Flask UI entrypoint (DB migration + gunicorn)
│   ├── vault_init.py               # Vault unseal + secret seeder daemon
│   ├── seed_root_user.py           # Creates root user in Supabase on first boot
│   ├── spark_query_server.py       # Spark SQL query microservice
│   └── sql_runner.py               # SQL execution helper
│
├── conf/
│   ├── spark-defaults.conf         # ALL Spark / S3A / Iceberg config (single source of truth)
│   └── log4j2.properties           # Spark logging config
│
├── ingestion/
│   ├── main/pipeline.py            # Spark pipeline entry point
│   ├── env/DE_Ingestion_properties.py  # Bucket names, catalog, env constants
│   ├── pyfiles/
│   │   ├── spark_session.py        # Spark session builder (reads spark-defaults.conf)
│   │   ├── pii_processor.py        # PII actions: hash, mask, encrypt
│   │   ├── vault_client.py         # Vault KV v2 + Transit engine client
│   │   ├── metadata_reader.py      # Load metadata CSV from MinIO
│   │   ├── schema_validator.py     # Enforce expected schema
│   │   ├── type_caster.py          # Column type coercion
│   │   ├── connectivity_checker.py # Pre-flight S3 / Vault checks
│   │   ├── iceberg_repair.py       # Iceberg table repair utilities
│   │   ├── args_parser.py          # CLI argument parser
│   │   ├── logger.py               # Structured logger
│   │   └── email_notifier.py       # Success / failure email alerts
│   ├── source/
│   │   ├── s3_reader.py            # Read CSV/JSON/Parquet from MinIO
│   │   └── supabase_reader.py      # Read tables from Supabase (PostgreSQL)
│   └── sink/
│       └── minio_writer.py         # Write Parquet to de-data-lake
│
├── bronze_layer/
│   ├── bronze_processor.py         # PK extraction, dedup, Iceberg writer
│   └── bronze_pipeline.py          # Full vs delta strategy orchestration
│
├── transformation/                 # DBT project (live-mounted into de-ui container)
│   ├── dbt_project.yml             # DBT project config
│   ├── profiles.yml                # Trino adapter config (env-driven host/port)
│   ├── packages.yml                # dbt-utils dependency
│   ├── models/
│   │   ├── staging/                # Views over bronze tables (example models)
│   │   ├── silver/                 # Auto-generated silver models (created by UI)
│   │   └── marts/                  # Aggregate / business layer models
│   └── macros/
│       ├── get_snapshot_date.sql   # Returns current run date for filtering
│       ├── generate_schema_name.sql # Schema routing macro
│       ├── create_silver_table.sql  # CREATE TABLE IF NOT EXISTS helper
│       ├── append_silver_table.sql  # INSERT INTO helper
│       └── insert_overwrite_silver_table.sql  # DROP + recreate helper
│
├── oracle_layer/
│   └── oracle_loader.py            # Trino → Oracle ADW loader (3 strategies)
│
├── trino/
│   └── etc/
│       ├── config.properties       # Trino coordinator config
│       ├── jvm.config              # JVM heap settings
│       ├── node.properties         # Node ID and data directory
│       └── catalog/
│           └── minio.properties    # Iceberg connector + native S3 config
│
├── hive-conf/
│   ├── hive-site.xml               # Metastore DB connection + S3 endpoint
│   └── core-site.xml               # Hadoop S3A credentials
│
├── ui/
│   ├── app.py                      # Flask application (all routes + APScheduler)
│   ├── auth.py                     # Login / logout / register blueprints
│   ├── db.py                       # Supabase PostgreSQL connection pool
│   ├── models.py                   # Flask-Login User model
│   ├── extensions.py               # Flask extension singletons
│   └── templates/
│       ├── index.html              # Main SPA (all panels, JS, streaming log)
│       ├── login.html              # Login page
│       └── users.html              # User management page
│
├── metadata/
│   └── metadata_sheet_example.csv  # Example metadata definition template
│
├── tests/
│   ├── unit/                       # Unit tests (preview SQL, silver routes, scheduler)
│   └── regression/                 # Regression tests (Iceberg paths, HMS schema)
│
├── scripts/
│   ├── init_minio_bucket.sh        # Manual bucket setup script
│   └── start_minio.sh              # Local MinIO start helper
│
├── requirements.txt                # Core Python dependencies
├── requirements-heavy.txt          # Spark / PySpark dependencies
├── requirements-dev.txt            # Dev/test dependencies
└── config.py                       # Top-level config constants
```

---

## Layer Functionality

### 1. Ingestion Layer (Lake)

The ingestion layer reads raw data from external sources, applies metadata-driven processing, masks/hashes PII columns, and writes clean Parquet files to the data lake.

**Entry point:** `ingestion/main/pipeline.py` via `docker-compose run pipeline`

**Sources supported:**
- **S3 / MinIO** — CSV, JSON, or Parquet files from `de-source-data-bucket`
- **Supabase (PostgreSQL)** — Direct table reads via `psycopg2`

**Processing steps:**
1. **Connectivity check** — Validates MinIO and Vault reachability before processing begins
2. **Metadata load** — Reads column definitions CSV from `de-metadata-bucket` (column name, data type, security level, primary key flags)
3. **Schema validation** — Enforces expected columns and rejects unexpected fields
4. **Type casting** — Coerces columns to target types per metadata definitions
5. **PII processing** — Three security levels driven by the `security_level` column in the metadata sheet:
   - `hash` — HMAC-SHA256 with SALT_KEY + SALT_2 (fetched from Vault KV)
   - `pii` / `mask` — Partial redaction: first 2 chars + `****` + last 2 chars
   - `encrypt` — AES-256 encryption via Vault Transit engine
6. **Write to lake** — Parquet with Snappy compression written to `s3a://de-data-lake/<app_name>/`

**Naming convention:** Application names are prefixed `Ingestion_` automatically by the UI on field blur.

---

### 2. Bronze Layer

The bronze layer promotes lake Parquet files into structured Iceberg tables, supporting two load strategies.

**Entry point:** `bronze_layer/bronze_pipeline.py` via `/run-bronze` Flask route

**Strategies:**

| Strategy | Behaviour |
|---|---|
| **Full Load** | Drops existing Iceberg table, writes entire lake dataset fresh |
| **Delta Load** | Deduplicates incoming data by primary keys (optionally ordered by timestamp column), then merges: DELETE matching PKs → INSERT new rows |

**Deduplication logic (`bronze_processor.py`):**
- With timestamp column: `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY ts DESC)` — keeps latest record per key
- Without timestamp: `dropDuplicates(primary_keys)`

**Storage layout:**
- Data files → `s3a://de-data-bronze/<app_name>/`
- Iceberg metadata → `s3a://de-iceberg-warehouse-bucket/de_bronze/<app_name>/`

---

### 3. Silver Layer (DBT + Trino)

The silver layer transforms bronze Iceberg tables using DBT models executed against Trino. The UI builds the DBT SQL dynamically from a visual column mapper and supports two run modes.

**Run modes:**

| Button | Endpoint | Behaviour |
|---|---|---|
| **Transform** | `POST /silver/run-transform` | Runs silver DBT transform only; streams logs to the floating log panel and advances the Silver pipeline progress steps |
| **Run Transform & Load** | `POST /run-transform-load` | Two-phase async chain: silver first, then Gold Oracle ADW load if silver succeeds |

**Workflow:**
1. **UI configuration** — User selects source bronze table(s), maps columns (with optional SQL expressions), sets materialization strategy
2. **DBT model generation** (`_write_silver_dbt_model()`) — Generates a `.sql` file in `transformation/models/silver/` with Jinja2 + DBT syntax and Iceberg table properties
3. **Sources manifest** (`_ensure_silver_sources()`) — Auto-creates/updates `sources.yml` to register all referenced bronze tables
4. **Overwrite cleanup** (`_overwrite_silver_cleanup()`) — For overwrite strategy: drops Iceberg table and clears S3 prefix before run
5. **DBT execution** — Runs `dbt run --select <model_name> --target docker` inside the container; output streamed line-by-line to the UI log panel
6. **Verification** — Queries `SELECT COUNT(*)` via Trino to confirm row count

**Streaming log messages and progress step mapping:**

The backend emits structured `[silver]` prefixed log lines. The frontend `processTLLine()` function regex-matches these to advance progress step dots:

| Log line | Progress step |
|---|---|
| `[silver] Ensuring MinIO bucket de-data-silver exists...` | `bucket` — running |
| `[silver] Bucket 'de-data-silver' OK.` | `bucket` — done |
| `[silver] Writing DBT model file...` | `schema` — running |
| `[silver] Materialization: ...` / `[silver] Source tables: ...` | `schema` — running |
| `[silver] Submitting DBT job to Trino...` | `execute` — running |
| `[silver] 1 of 1 START ...` / `[silver] Concurrency: ...` | `data` — running |
| `[silver] DBT run completed successfully.` | `data` — done |
| `[silver] Verifying Iceberg table — querying row count...` | `data` |
| `[silver] Row count verified: N rows in de_silver.<table>.` | `data` |
| `--- Silver Exit 0 ---` | `complete` — done |
| `--- Silver Exit 1 ---` / `[silver] ERROR` | `complete` — fail |

**Oracle target table auto-fill:**

When the user sets the Silver Target Table field, `_tlSyncGoldFields()` fires automatically and:
- Copies the value into the Gold panel's **Silver Table** field
- Derives the Oracle target table as `target_table.toUpperCase()` and writes it to the Gold panel's **Oracle Table** field

This means the Gold layer is pre-populated without manual entry whenever a silver target table is named.

**Materialization options:**

| Strategy | DBT materialization | Behaviour |
|---|---|---|
| Append | `incremental` (append) | Adds new rows without removing existing |
| Overwrite | Custom macro | Drops and recreates table on each run |
| Incremental | `incremental` (merge) | Upsert on primary keys |

**Storage layout:**
- Data files → `s3://de-data-silver/<table_name>/`
- Iceberg metadata → `s3://de-iceberg-warehouse-bucket/de_silver/<table_name>/`

**AI features (via OpenAI):**
- `/silver/convert-logic` — Converts natural-language column descriptions to SQL expressions
- `/lake-ask` — Natural-language SQL query generation against lake data

---

### 4. Gold Layer (Oracle ADW)

The gold layer loads silver Iceberg data into Oracle Autonomous Data Warehouse using three configurable strategies. It is triggered via the **"Run DWH Gold Layer"** button in the Gold panel, or automatically after a successful silver run when using "Run Transform & Load".

**Entry points:**

| Trigger | Endpoint | Description |
|---|---|---|
| Gold panel "Run DWH Gold Layer" button | `POST /run-oracle` | Standalone Oracle load with streaming log output |
| Transform & Load Phase 2 (auto) | `POST /run-transform-load` | Chained after silver success; logs prefixed `[gold]` |
| Scheduled silver job with `oracle_table` | Internal scheduler | Triggered automatically by `_trigger_scheduled_job()` |

**Oracle ADW authentication:**
- Credentials stored in Vault at `secret/data/oracle/adw` (user, password, DSN, wallet password, wallet ZIP as base64)
- Wallet ZIP is base64-decoded and extracted to a temp directory at runtime; `oracledb` connects in thin mode with the wallet path

**Load strategies:**

| Strategy | SQL Executed |
|---|---|
| `append` | Batched `INSERT INTO target` from Silver rows (5,000 rows per batch, configurable via `ORACLE_BATCH_SIZE`) |
| `truncate` | `TRUNCATE TABLE target` followed by batched INSERT |
| `delete_and_insert` | `DELETE FROM target WHERE date_col = run_date` followed by batched INSERT |

**Gold pipeline progress step tracking:**

The Gold progress card advances step dots by matching `[gold]` prefixed log lines:

| Log line pattern | Progress step |
|---|---|
| `[gold] Launching` / `[gold] Starting Oracle load` | `connect` — running |
| `[gold] Table ensured` | `table` — done |
| `[gold] Inserted rows` | `load` — running |
| `[gold] Load complete` | `load` — done |
| `--- Oracle Load Exit 0 ---` | `complete` — done |
| `--- Oracle Load Exit 1 ---` / `[gold] ERROR` | `complete` — fail |

**Type mapping (`_trino_type_to_oracle()`):**

| Trino Type | Oracle Type |
|---|---|
| `varchar(N)` | `VARCHAR2(N)` |
| `bigint`, `integer` | `NUMBER(19)` |
| `decimal(p,s)` | `NUMBER(p,s)` |
| `double`, `float` | `BINARY_DOUBLE` |
| `boolean` | `NUMBER(1)` |
| `timestamp` | `TIMESTAMP` |

**Auto-provisioning:** `_ensure_table()` creates the Oracle target table with correct column types if it does not already exist, derived from `DESCRIBE minio.de_silver.<table>` via Trino.

---

## Web UI Features

The UI is a single-page application (Flask-rendered Jinja2 template with vanilla JavaScript) featuring real-time streaming log panels and visual pipeline progress indicators.

### Panels

| Panel | Description |
|---|---|
| **Ingestion** | Configure and trigger lake ingestion (source, app name, metadata, schedule) |
| **Bronze** | Trigger bronze promotion with full or delta strategy selection |
| **Silver Transform** | Visual column mapper, SQL preview, flowchart diagram, DBT run controls |
| **Gold DWH** | Oracle ADW load — select silver table, load strategy, date column |
| **Transform & Load** | Chained Silver → Gold pipeline with dual progress tracking |
| **Scheduled Jobs** | View, create, and delete scheduled ingestion and silver jobs |
| **Log History** | Browse and download historical pipeline logs from MinIO |
| **Connectivity** | Real-time health check panel for all services |
| **Lake SQL** | Ad-hoc SQL query runner against lake Parquet via Spark |

### Real-Time Streaming

All pipeline runs stream logs line-by-line via HTTP chunked transfer encoding (`stream_with_context`). The floating log panel appends each line as it arrives with no polling. Success or failure is determined by detecting sentinel exit lines in the stream (`--- Silver Exit 0 ---`, `--- Oracle Load Exit 0 ---`) rather than HTTP status codes, since streaming responses always return HTTP 200.

### Transform & Load — Two-Phase Async Flow

The "Run Transform & Load" button executes a two-phase pipeline entirely in the browser using `async/await`:

```
Phase 1 — Silver
  POST /silver/run-transform
  Stream logs → processTLLine() → advance Silver progress dots
  Detect "--- Silver Exit 0 ---" → silver success
  On failure: mark silver complete dot red, abort

  ↓ (silver succeeded)

  Flush log panel, relabel title to "Gold Load Logs"
  Switch progress card tab to Gold
  Set Silver nav dot → done, Gold nav dot → running

Phase 2 — Gold
  POST /run-oracle
  Raw container logs are prefixed [gold] in the browser if not already prefixed
  Stream logs → processTLLine() → advance Gold progress dots
  Detect "--- Oracle Load Exit 0 ---" → gold success
  Set Gold nav dot → done / fail
```

This architecture means the two phases share the same log panel with a clean flush between them, and the pipeline progress card always shows the active phase.

### Pipeline Progress Card

The **Transform & Load** panel includes a visual progress step card with two tabs:

**Silver steps:** `bucket` → `schema` → `execute` → `data` → `complete`

**Gold steps:** `connect` → `table` → `load` → `complete`

Each dot transitions through: `pending` → `running` → `done` / `fail`, driven by regex matching against streamed log lines in `processTLLine()`. When the user switches between Silver and Gold layer views using `setTLLayer()`, the progress card tab syncs automatically.

### Gold Layer Tab Sync

Switching to the Gold layer view (or having the Transform & Load pipeline enter Phase 2) automatically:
- Switches the T&L progress card to the Gold tab
- Relabels the floating log panel to "Gold Load Logs"
- Sets the Silver nav dot to `done` and Gold nav dot to `running`

Switching back to the Silver layer view reverses this, switching the progress card to the Silver tab.

### Active Jobs Panel

A live indicator shows all currently running pipeline containers. Each entry shows the application name, run date, job type (ingestion / bronze / silver / gold), and a link to stream its live logs.

### App Name Normalisation

The UI automatically prefixes application names on field blur:
- Ingestion panel → `Ingestion_<name>`
- Silver panel → `Silver_<name>`
- Gold app name → auto-derived as `Gold_<base>` from the silver name in real time as the user types

---

## Scheduling System

Built on **APScheduler** (`BackgroundScheduler` with `CronTrigger`), persisted in Supabase PostgreSQL (`public.scheduled_jobs` table).

### Supported Job Types

| Type | Behaviour |
|---|---|
| Ingestion | Runs the ingestion pipeline (lake layer) on cron schedule |
| Silver | Runs silver DBT transform; optionally chains Gold Oracle load if `oracle_table` is set |

### Schedule Fields

| Field | Description |
|---|---|
| `app_name` | Application identifier (prefixed `Ingestion_` or `Silver_`) |
| `cron_expression` | Standard cron string (e.g. `0 6 * * *`) |
| `layer_type` | `ingestion` or `silver` |
| `oracle_table` | Oracle target table name (silver jobs only; auto-derived as `target_table.toUpperCase()` when saving) |
| `load_strategy` | `append` / `truncate` / `delete_and_insert` (silver jobs only, defaults to `append`) |
| `date_column` | Date partition column name (required only for `delete_and_insert` strategy) |

### Silver Schedule — DWH Gold Load Strategy

The Silver schedule form includes a **"DWH Gold Load Strategy"** section directly below the cron field. This allows configuring the Gold Oracle load that runs after silver completes:

- **Append** — adds all rows from silver to Oracle without removing existing data
- **Truncate** — clears the Oracle table before inserting silver rows
- **Delete & Insert** — deletes only the date partition matching the run date, then inserts silver rows; selecting this strategy reveals a **Date Column** input field

The `oracle_table` value is automatically derived from the silver target table name (uppercased) when "Save Schedule" is clicked — no manual entry required.

### Silver → Gold Chaining

When a scheduled silver job has `oracle_table` set, the scheduler automatically chains an Oracle ADW load after a successful silver DBT run — the same two-phase flow as the interactive "Transform & Load" button. The Gold Docker container is launched with the stored `load_strategy`, `date_column`, and `oracle_table` values, and its logs are appended to the job run record.

### Scheduled Jobs UI

The Scheduled Jobs panel segregates jobs into two sections:
- **Ingestion** — all jobs with `layer_type = ingestion`
- **Silver Transform** — all jobs with `layer_type = silver`

---

## Security & Secrets Management

### HashiCorp Vault

Vault is the single source of truth for all runtime secrets:

| Vault Path | Contents |
|---|---|
| `encryption/pii` | `key` (AES encryption key), `salt_2` (secondary HMAC salt) |
| `secret/data/oracle/adw` | `user`, `password`, `dsn`, `wallet_password`, `wallet_zip_b64` |
| `secret/data/supabase` | `password` (Supabase DB password) |

**Token management:** `vault_init.py` daemon unseals Vault on startup, writes a pipeline service token to `/vault/secrets/pipeline_token` (shared Docker volume), and monitors for re-seal events to re-unseal automatically.

### Authentication (Flask-Login)

- Users stored in Supabase `app_users` table with Argon2id-hashed passwords
- Session management via Flask-Login with server-side sessions
- Root user seeded at container startup from `ROOT_PASSWORD` environment variable
- User management UI available at `/users` for administrators
- Security headers applied to all responses (X-Frame-Options, X-Content-Type-Options, Referrer-Policy)

### PII Processing

Three security levels selectable per column in the metadata sheet:

| Level | Method | Output example |
|---|---|---|
| `hash` | HMAC-SHA256(value, SALT_KEY \|\| SALT_2) | 64-char hex digest |
| `mask` / `pii` | Partial redaction | `AB****YZ` |
| `encrypt` | AES-256 via Vault Transit engine | Vault ciphertext |

---

## API Reference

### Ingestion & Bronze

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/run` | Trigger ingestion pipeline (lake layer) |
| `POST` | `/run-bronze` | Trigger bronze layer pipeline |
| `GET` | `/list-bucket-keys` | List objects in source bucket |
| `GET` | `/list-metadata-keys` | List metadata CSV files in MinIO |
| `GET` | `/metadata-preview` | Preview metadata CSV contents |

### Silver Layer

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/silver/run-transform` | Run silver DBT transform (streaming response) |
| `POST` | `/silver/check-table` | Check if silver Iceberg table exists |
| `POST` | `/silver/preview-sql` | Preview generated DBT SQL |
| `POST` | `/silver/flowchart-from-sql` | Generate column lineage flowchart from SQL |
| `POST` | `/silver/compile-sql` | Compile DBT model (dry run) |
| `POST` | `/silver/compile-dbt` | Full DBT compile |
| `POST` | `/silver/convert-logic` | Convert natural language to SQL expression (OpenAI) |
| `POST` | `/run-silver` | Run silver via Docker container |

### Gold Layer

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/run-oracle` | Run Oracle ADW load (streaming response) |
| `POST` | `/run-transform-load` | Chained silver + gold run (streaming response) |

### Scheduling

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/scheduled-jobs` | List all scheduled jobs |
| `POST` | `/scheduled-jobs` | Create a new scheduled job |
| `DELETE` | `/scheduled-jobs/<id>` | Delete a scheduled job |
| `GET` | `/scheduled-job-runs` | List recent job run history |

### Monitoring & Logs

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/active-jobs` | List currently running pipeline jobs |
| `GET` | `/job-logs/<label>` | Stream live logs for a running job |
| `GET` | `/log-history` | Browse historical logs from MinIO |
| `GET` | `/view-log` | View a specific log file |
| `GET` | `/download-log` | Download a log file |
| `GET` | `/connectivity` | Health check all services |

### Lake SQL

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/lake-sql-run` | Execute ad-hoc SQL against lake Parquet via Spark |
| `POST` | `/lake-ask` | Natural-language to SQL (OpenAI) then execute |

---

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| `MINIO_ACCESS_KEY` | MinIO root username | `minioadmin` |
| `MINIO_SECRET_KEY` | MinIO root password | `minioadmin` |
| `MINIO_BUCKET` | Lake bucket name | `de-data-lake` |
| `SOURCE_S3_BUCKET` | Source data bucket name | `de-source-data-bucket` |
| `MINIO_ENDPOINT` | MinIO S3 API URL (internal Docker) | `http://minio:9000` |
| `MINIO_SERVER_URL` | MinIO S3 API URL (external) | `http://localhost:9000` |
| `MINIO_BROWSER_REDIRECT_URL` | MinIO Console URL (external) | `http://localhost:9001` |
| `VAULT_ADDR` | Vault API URL | `http://vault:8200` |
| `VAULT_PATH` | Vault KV path for PII keys | `encryption/pii` |
| `SALT_2` | Secondary HMAC salt (seeded to Vault on init) | — |
| `SUPABASE_DB_PASSWORD_PLAIN` | Supabase DB password (seeded to Vault) | — |
| `ROOT_PASSWORD` | Root user password for Flask UI | — |
| `TRINO_HOST` | Trino hostname | `trino` |
| `TRINO_PORT` | Trino port | `8080` |
| `ORACLE_USER` | Oracle ADW username | — |
| `ORACLE_PASSWORD` | Oracle ADW password | — |
| `ORACLE_DSN` | Oracle connection DSN string | — |
| `ORACLE_WALLET_PASSWORD` | Oracle wallet password | — |
| `ORACLE_WALLET_ZIP_B64` | Base64-encoded wallet ZIP file | — |
| `ORACLE_BATCH_SIZE` | Number of rows per Oracle INSERT batch | `5000` |
| `OPENAI_API_KEY` | OpenAI API key (for SQL assist features) | — |
| `OPENAI_MODEL` | OpenAI model ID | `gpt-4.1-nano` |
| `CLOUDFLARE_TUNNEL_TOKEN` | Cloudflare Zero Trust tunnel token | — |
| `BEHIND_HTTPS_PROXY` | Set to `1` when deployed behind an HTTPS proxy | — |

---

## Setup & Deployment

### Prerequisites

- Docker 24+ and Docker Compose v2
- A Cloudflare account with a Zero Trust tunnel (optional, for external HTTPS access)
- Oracle ADW wallet ZIP and credentials (for Gold layer)
- OpenAI API key (optional, for AI SQL assist)

### First-Time Setup

```bash
# 1. Clone the repository
git clone <repo-url> de-metadata-framework
cd de-metadata-framework

# 2. Create the .env file and fill in required secrets
cp .env.example .env
# Required: SALT_2, SUPABASE_DB_PASSWORD_PLAIN, ROOT_PASSWORD,
#           ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN,
#           ORACLE_WALLET_PASSWORD, ORACLE_WALLET_ZIP_B64,
#           CLOUDFLARE_TUNNEL_TOKEN, OPENAI_API_KEY

# 3. Start infrastructure first (MinIO + Vault)
docker compose up -d minio vault

# 4. Start Vault daemon (auto-unseals and seeds all secrets)
docker compose up -d vault-init

# 5. Start remaining services
docker compose up -d

# 6. Open the UI
open http://localhost:5001
# Login with: root / <ROOT_PASSWORD from .env>
```

### Subsequent Starts

```bash
docker compose up -d
```

### Running a Pipeline Manually

```bash
# Ingestion (lake layer) — via Docker Compose
docker compose run --rm pipeline \
  --application-name Ingestion_MyApp \
  --source-type s3 \
  --run-date 2024-01-15

# Bronze and Silver layers are triggered from the UI
# or via direct POST requests to the Flask API
```

### DBT Development

The `transformation/` directory is volume-mounted live into the `de-ui` container, so DBT models written by the UI are immediately available on the host filesystem:

```bash
# Run DBT manually inside the container
docker exec -it de-ui bash
cd /app/transformation
dbt run --target docker --select my_silver_model
dbt compile --target docker
```

### Health Checks

```bash
# Check all services at once
curl http://localhost:5001/connectivity

# Individual service health endpoints
curl http://localhost:9000/minio/health/live     # MinIO
curl http://localhost:8200/v1/sys/health         # Vault
curl http://localhost:8081/v1/info               # Trino
```

---

## Tech Stack

| Category | Technology |
|---|---|
| **Container Orchestration** | Docker Compose |
| **Object Storage** | MinIO (S3-compatible) |
| **Secrets Management** | HashiCorp Vault (KV v2, Transit engine) |
| **Iceberg Catalog** | Apache Hive Metastore 3.x |
| **Table Format** | Apache Iceberg |
| **SQL Engine** | Trino 482 |
| **Transformation** | dbt-core + dbt-trino adapter |
| **Batch Processing** | Apache Spark 3.x (PySpark) |
| **Web Framework** | Flask 3.x |
| **Authentication** | Flask-Login + Argon2id password hashing |
| **Job Scheduling** | APScheduler (BackgroundScheduler + CronTrigger) |
| **App Database** | PostgreSQL 15 (Supabase) |
| **DWH Target** | Oracle Autonomous Data Warehouse |
| **Oracle Driver** | python-oracledb (thin mode + wallet ZIP) |
| **AI Assist** | OpenAI API |
| **External Access** | Cloudflare Tunnel (Zero Trust) |
| **File Format** | Apache Parquet + Snappy compression |
| **Language** | Python 3.11+ |
