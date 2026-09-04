import argparse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="etl-pipeline",
        description="Metadata-driven ETL pipeline: reads from S3 or Supabase, "
                    "applies PII processing, and writes as an Iceberg table to MinIO.",
    )

    parser.add_argument(
        "--application-name",
        dest="application_name",
        required=True,
        help="Name of the application to be ingested.",
    )

    parser.add_argument(
        "--ingest-date",
        dest="ingest_date",
        required=True,
        help="Date of the ingestion (YYYY-MM-DD).",
    )

    # ── Source selection ──────────────────────────────────────────────────────
    source_group = parser.add_argument_group("Source")
    source_group.add_argument(
        "--source-type",
        dest="source_type",
        choices=["s3", "database"],
        required=True,
        help="Origin of the raw data: 's3' for file-based, 'database' for Supabase/PostgreSQL.",
    )

    # S3 source options
    source_group.add_argument(
        "--source-bucket",
        dest="source_bucket",
        default=None,
        help="[s3] S3 bucket containing the source file. "
             "Defaults to SOURCE_S3_BUCKET from .env.",
    )
    source_group.add_argument(
        "--source-key",
        dest="source_key",
        default=None,
        help="[s3] S3 object key (path) of the source file. "
             "Required when --source-type=s3.",
    )

    # Database (Supabase) source options
    source_group.add_argument(
        "--source-database",
        dest="source_database",
        default='public',
        help="[database] PostgreSQL schema to qualify the table read "
             "(e.g. 'public'). Optional — omit to use the default schema.",
    )
    source_group.add_argument(
        "--source-table-name",
        dest="source_table_name",
        default=None,
        help="[database] Supabase / PostgreSQL table name to read. "
             "Required when --source-type=database.",
    )

    # ── Metadata sheet ────────────────────────────────────────────────────────
    meta_group = parser.add_argument_group("Metadata sheet")

    meta_group.add_argument(
        "--metadata-key",
        dest="metadata_key",
        default=None,
        help="S3 key of the metadata sheet CSV. "
             "Defaults to METADATA_S3_KEY from .env.",
    )

    # ── Output / sink (Iceberg on MinIO) ──────────────────────────────────────
    sink_group = parser.add_argument_group("Sink (Iceberg on MinIO)")

    sink_group.add_argument(
        "--output-table-name",
        dest="output_table_name",
        default=None,
        help="Iceberg table name to write to. "
             "Defaults to --application-name lowercased with spaces/hyphens replaced by underscores.",
    )
    sink_group.add_argument(
        "--output-database",
        dest="output_database",
        default=None,
        help="Iceberg database / namespace. Defaults to ICEBERG_DATABASE from .env.",
    )
    sink_group.add_argument(
        "--output-catalog",
        dest="output_catalog",
        default=None,
        help="Iceberg catalog name. Defaults to ICEBERG_CATALOG from .env.",
    )
    sink_group.add_argument(
        "--output-bucket",
        dest="output_bucket",
        default=None,
        help="MinIO bucket used for the connectivity pre-flight check. "
             "Defaults to MINIO_BUCKET from .env.",
    )
    sink_group.add_argument(
        "--write-mode",
        dest="write_mode",
        choices=["overwrite", "append", "replace"],
        default="overwrite",
        help="Iceberg write mode: overwrite (createOrReplace) or append. Default: overwrite.",
    )

    # ── Runtime ───────────────────────────────────────────────────────────────
    runtime_group = parser.add_argument_group("Runtime")
    runtime_group.add_argument(
        "--log-level",
        dest="log_level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Log verbosity (default: INFO).",
    )
    runtime_group.add_argument(
        "--log-folder",
        dest="log_folder",
        required=False,
        default="lake",
        help="Top-level folder inside the log S3 bucket. "
             "Log key: s3://<LOG_S3_BUCKET>/<log_folder>/<application_name>/<ingest_date>/<application_name>_<ingest_date>.log "
             "(default: lake).",
    )

    runtime_group.add_argument(
        "--partition",
        dest="partition",
        default=None,
        required=False,
    )
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)

    # Cross-argument validation
    if args.source_type == "s3" and not args.source_key:
        parser.error("--source-key is required when --source-type=s3")
    if args.source_type == "database" and not args.source_table_name:
        parser.error("--source-table-name is required when --source-type=database")

    # Derive output table name from application name if not explicitly provided
    if not args.output_table_name:
        args.output_table_name = (
            args.application_name.lower().replace("-", "_").replace(" ", "_")
        )

    return args
