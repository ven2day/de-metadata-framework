# DE Metadata Framework — Codebase Reference

> **For AI assistants:** Read this file first before answering any question about this project. Do not scan source files unless the user asks about something not covered here or asks you to verify a specific implementation detail. Update this file whenever the structure changes.

---

## Project Purpose

A PySpark-based metadata-driven ETL pipeline that:
1. Reads raw data from **MinIO S3-compatible source** (CSV / JSON / Parquet) or **Supabase** (PostgreSQL via JDBC)
2. Loads a **metadata sheet** from a dedicated MinIO metadata bucket
3. Validates that the dataframe columns exactly match the metadata — **fails the job** if they don't
4. **Casts** each column to the declared type (supports `DecimalType` with precision/scale)
5. **Hashes** (`security_level=hash` → HMAC-SHA256) or **masks** (`security_level=pii` → character mask) sensitive columns
6. Writes the processed dataframe as a **month-partitioned Apache Iceberg table** to MinIO sink

---

## Directory Layout

Entry point is `ingestion/main/pipeline.py`. There is no `pipeline.py` at the project root.

```
de-metadata-framework/
├── .env                                      ← secrets (gitignored)
├── conf/
│   └── spark-defaults.conf                   ← all Spark/Iceberg/S3A config (SPARK_CONF_DIR)
├── CODEBASE.md                               ← this file
├── requirements.txt
│
└── ingestion/
    ├── env/
    │   └── DE_Ingestion_properties.py        ← single source of truth for all config properties
    │
    ├── pyfiles/
    │   ├── logger.py
    │   ├── args_parser.py
    │   ├── connectivity_checker.py
    │   ├── spark_session.py
    │   ├── metadata_reader.py
    │   ├── schema_validator.py
    │   ├── type_caster.py
    │   └── pii_processor.py
    │
    ├── source/
    │   ├── s3_reader.py
    │   └── supabase_reader.py
    │
    ├── sink/
    │   └── minio_writer.py
    │
    └── main/
        └── pipeline.py                       ← CLI entry point and orchestrator
```

---

## Import Paths (canonical)

All imports use explicit module paths — no `__init__.py` re-exports.

| What you need | Import path |
|---|---|
| Config properties | `from ingestion.env.DE_Ingestion_properties import <VAR>` |
| Logger | `from ingestion.pyfiles.logger import get_logger` |
| Arg parser | `from ingestion.pyfiles.args_parser import parse_args` |
| Connectivity checks | `from ingestion.pyfiles.connectivity_checker import run_all_checks` |
| SparkSession | `from ingestion.pyfiles.spark_session import get_spark_session` |
| Metadata reader | `from ingestion.pyfiles.metadata_reader import read_metadata` |
| Schema validator | `from ingestion.pyfiles.schema_validator import validate_schema` |
| Type caster | `from ingestion.pyfiles.type_caster import cast_columns` |
| PII processor | `from ingestion.pyfiles.pii_processor import apply_pii` |
| S3 reader | `from ingestion.source.s3_reader import read_s3_file` |
| Supabase reader | `from ingestion.source.supabase_reader import read_supabase_table` |
| MinIO writer | `from ingestion.sink.minio_writer import write_to_minio` |

---

## File-by-File Reference

### `ingestion/env/DE_Ingestion_properties.py`

| Variable | Default | Purpose |
|---|---|---|
| `SALT_KEY` | `change-me-...` | HMAC-SHA256 salt for hashing |
| `SOURCE_S3_ENDPOINT` | `http://127.0.0.1:9000` | Source MinIO endpoint |
| `SOURCE_S3_ACCESS_KEY` | `minioadmin` | Source MinIO credentials |
| `SOURCE_S3_SECRET_KEY` | `minioadmin` | Source MinIO credentials |
| `SOURCE_S3_BUCKET` | `de-source` | Source data bucket |
| `METADATA_S3_BUCKET` | *(from .env)* | Separate bucket holding the metadata sheet |
| `MINIO_ENDPOINT` | `http://127.0.0.1:9000` | Sink MinIO endpoint |
| `MINIO_ACCESS_KEY` | `minioadmin` | Sink MinIO credentials |
| `MINIO_SECRET_KEY` | `minioadmin` | Sink MinIO credentials |
| `MINIO_BUCKET` | `de-data-lake` | Sink data bucket |
| `SUPABASE_URL` | placeholder | Supabase REST endpoint |
| `SUPABASE_KEY` | placeholder | Supabase anon/service-role key |
| `SUPABASE_JDBC_URL` | placeholder | PostgreSQL JDBC URL for Spark reads |
| `SUPABASE_DB_USER` | `postgres` | DB user for JDBC |
| `SUPABASE_DB_PASSWORD` | placeholder | DB password for JDBC |
| `SPARK_APP_NAME` | `DE-Metadata-Framework` | Spark application name |
| `SPARK_MASTER` | `local[*]` | Spark master URL |
| `ICEBERG_WAREHOUSE` | `s3a://de-iceberg-warehouse/` | Iceberg warehouse root |
| `ICEBERG_CATALOG` | `minio` | Iceberg catalog name |
| `ICEBERG_DATABASE` | `default` | Iceberg database / namespace |

---

### `conf/spark-defaults.conf`

Loaded via `SPARK_CONF_DIR=conf` at spark-submit time. Contains **all** Spark/Iceberg/S3A config. Python code never sets Spark config — it all lives here.

| Section | Key settings |
|---|---|
| Iceberg extensions | `spark.sql.extensions = IcebergSparkSessionExtensions` |
| Iceberg catalog | `spark.sql.catalog.minio` = `SparkCatalog` (Hadoop type, S3FileIO, warehouse `s3a://de-iceberg-warehouse/`) |
| Iceberg S3 creds | `spark.sql.catalog.minio.s3.access-key-id` / `.secret-access-key` |
| Global S3A | endpoint `http://127.0.0.1:9000`, `SimpleAWSCredentialsProvider`, access/secret keys |
| Source per-bucket | Commented-out block for `de-source` bucket overrides (uncomment if source differs from sink) |
| JAR packages | `iceberg-spark-runtime-4.1_2.13:1.11.0`, `iceberg-aws-bundle:1.11.0`, `hadoop-aws:3.4.2` |

---

### `ingestion/pyfiles/logger.py`

`get_logger(name, level=INFO) → logging.Logger`

Returns a project-scoped logger. Only records from `_PROJECT_MODULES = {source, ingestion, sink, pipeline, config, pyfiles, __main__}` pass through. boto3/botocore/urllib3/s3transfer/httpx/httpcore/supabase are set to `CRITICAL`.

---

### `ingestion/pyfiles/args_parser.py`

`build_parser()` / `parse_args(argv=None) → Namespace`

| Group | Flag | dest | Required? |
|---|---|---|---|
| — | `--application-name` | `application_name` | Yes |
| — | `--ingest-date` | `ingest_date` | Yes |
| Source | `--source-type {s3,database}` | `source_type` | Yes |
| Source | `--source-bucket` | `source_bucket` | No |
| Source | `--source-key` | `source_key` | If s3 |
| Source | `--source-database` | `source_database` | No (PostgreSQL schema) |
| Source | `--source-table-name` | `source_table_name` | If database |
| Metadata | `--metadata-key` | `metadata_key` | No |
| Sink | `--output-table-name` | `output_table_name` | No (derived from app name) |
| Sink | `--output-database` | `output_database` | No |
| Sink | `--output-catalog` | `output_catalog` | No |
| Sink | `--output-bucket` | `output_bucket` | No |
| Sink | `--write-mode {overwrite,append}` | `write_mode` | No (default: overwrite) |
| Runtime | `--log-level` | `log_level` | No (default: INFO) |

`output_table_name` is derived from `application_name` (lower, spaces/hyphens → underscores) if not provided.

---

### `ingestion/pyfiles/connectivity_checker.py`

`run_all_checks(args, source_bucket=None, minio_bucket=None)`

Routes checks based on `args.source_type`:
- `s3` → `check_source_s3()` (credentialed boto3 `HeadBucket` against source MinIO)
- `database` → `check_supabase()` (Supabase REST `pg_sleep` RPC)
- Always → `check_minio()` (sink MinIO `HeadBucket`; auto-creates bucket on 404)

---

### `ingestion/pyfiles/spark_session.py`

`get_spark_session() → SparkSession` (singleton)

Sets only `appName`, `master`, `spark.ui.showConsoleProgress=false`, and `setLogLevel("WARN")`. All other config comes from `spark-defaults.conf`.

---

### `ingestion/pyfiles/metadata_reader.py`

`read_metadata(spark, bucket=None, key=None) → DataFrame`

- Bucket defaults to `METADATA_S3_BUCKET`; key must be supplied via `--metadata-key`
- Required columns: `column_name`, `datatype`
- Optional columns handled: `security_level` (normalised to lower; defaults to `"none"`), `data_length` (cast to int), `data_precision` (cast to int)
- Uses `inferSchema=true`

**Metadata sheet format:**

| column_name | datatype | data_length | data_precision | security_level |
|---|---|---|---|---|
| state_fips | BIGINT | | | |
| lat | DECIMAL | 18 | 4 | |
| state_name | STRING | | | pii |
| ssn | STRING | | | hash |

---

### `ingestion/pyfiles/schema_validator.py`

`validate_schema(df, metadata_df) → None`

Compares `set(df.columns)` vs `{row["column_name"]}` from metadata. Raises `ValueError` on any mismatch — job fails.

---

### `ingestion/pyfiles/type_caster.py`

`cast_columns(df, metadata_df) → DataFrame`

Uses `datatype` column. For `decimal` type, reads `data_length` (precision) and `data_precision` (scale) to build `DecimalType(length, scale)`. Falls back to `DoubleType()` if either is null.

| Metadata datatype | Spark type |
|---|---|
| string / str / varchar / text / char | `StringType` |
| integer / int / smallint | `IntegerType` |
| bigint / long | `LongType` |
| decimal (with length+precision) | `DecimalType(length, scale)` |
| decimal / numeric (without) | `DoubleType` |
| float / real | `FloatType` |
| boolean / bool | `BooleanType` |
| date | `DateType` |
| timestamp / datetime | `TimestampType` |

---

### `ingestion/pyfiles/pii_processor.py`

`apply_pii(df, metadata_df) → DataFrame`

Filters metadata on `security_level IN ('hash', 'pii')`:

| security_level | Action |
|---|---|
| `hash` | HMAC-SHA256 deterministic hash (64-char hex) using `SALT_KEY` |
| `pii` | Character mask: first 2 + `****` + last 2 chars; `****` if ≤ 4 chars |

---

### `ingestion/source/s3_reader.py`

`read_s3_file(spark, key, bucket=None) → DataFrame`

Detects format from extension (csv / json / parquet). Defaults bucket to `SOURCE_S3_BUCKET`. CSV uses `header=true, inferSchema=true`.

---

### `ingestion/source/supabase_reader.py`

`read_supabase_table(spark, source_table_name, schema=None, columns=None, filters=None) → DataFrame`

Reads via JDBC (`org.postgresql.Driver`). If `schema` provided, qualifies table as `schema.table_name`. `columns` and `filters` are optional extras (not wired to CLI args).

---

### `ingestion/sink/minio_writer.py`

`write_to_minio(df, args, table_name, database=None, catalog=None, mode="overwrite") → None`

1. Adds `ingest_date` column from `args.ingest_date` (`to_date(lit(...))`)
2. Writes as Iceberg table partitioned by `months("ingest_date")`
3. `mode="overwrite"` → `createOrReplace()`; `mode="append"` → `append()`

Full table ref: `{catalog}.{database}.{table_name}`

---

### `ingestion/main/pipeline.py`

CLI entry point. Run with `SPARK_CONF_DIR=conf .venv/bin/spark-submit ingestion/main/pipeline.py <args>`.

**Functions:**
- `run_s3_pipeline(args, source_key, source_bucket, output_table_name, output_database, output_catalog, output_bucket, metadata_key, write_mode)`
- `run_database_pipeline(args, source_table_name, source_database, output_table_name, output_database, output_catalog, output_bucket, metadata_key, write_mode)`

Both take `args: Namespace` as first parameter (passed from `__main__`). `args.application_name` and `args.ingest_date` are read directly from it inside the function.

**Execution order (both pipelines):**
1. `run_all_checks(args, ...)` — pre-flight connectivity
2. `get_spark_session()` — get/create SparkSession
3. `read_metadata(spark, key=metadata_key)` — load metadata sheet
4. `read_s3_file()` / `read_supabase_table()` — load raw data
5. `validate_schema()` — fail-fast on column mismatch
6. `cast_columns()` — apply declared types
7. `apply_pii()` — hash / mask sensitive columns
8. `write_to_minio(df, args, ...)` — persist as Iceberg table

---

## spark-submit Command

```bash
SPARK_CONF_DIR=conf \
.venv/bin/spark-submit \
  ingestion/main/pipeline.py \
  --application-name "my-app" \
  --ingest-date "2026-09-02" \
  --source-type s3 \
  --source-bucket de-source \
  --source-key "path/to/file.csv" \
  --metadata-key "metadata/metadata_sheet.csv" \
  --output-table-name my_app \
  --output-database default \
  --output-catalog minio \
  --output-bucket de-data-lake \
  --write-mode overwrite \
  --log-level INFO
```

---

## Key Design Rules

- **Schema mismatch = job failure.** `validate_schema` raises on any column discrepancy.
- **All Spark config lives in `conf/spark-defaults.conf`.** `spark_session.py` sets only `appName`, `master`, and UI/log settings — nothing else.
- **All imports are explicit module paths.** No `from ingestion.pyfiles import *` or `__init__.py` re-exports.
- **`args: Namespace` is threaded into pipeline functions** so `write_to_minio` and `run_all_checks` can access `args.ingest_date` and `args.source_type`.
- **Metadata sheet drives everything** — types, partitioning date, and security handling all come from the CSV in `METADATA_S3_BUCKET`.
- **SparkSession is a singleton.** `get_spark_session()` caches in `_SESSION`.
- **MinIO runs as a local binary** (not Docker) at `http://127.0.0.1:9000`.
