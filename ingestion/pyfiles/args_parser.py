import argparse
from datetime import datetime
from zoneinfo import ZoneInfo

from ingestion.env.DE_Ingestion_properties import (
    DHAN_RAW_BUCKET,
    DHAN_INSTRUMENTS,
    DHAN_SYMBOLS,
    DHAN_SYMBOLS_FILE,
    DHAN_SNAPSHOT_MODE,
    LAKE_DATABASE,
    METADATA_S3_KEY,
)
from ingestion.dhan.client import DhanConfigError
from ingestion.dhan.instruments import configured_selection


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="etl-pipeline",
        description="Metadata-driven ETL pipeline: reads from S3, PostgreSQL, or Dhan, "
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
        choices=["s3", "database", "dhan", "dhan-live"],
        required=True,
        help="Origin: s3, database, dhan (current REST snapshot), or dhan-live (landed JSONL micro-batch).",
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

    # DhanHQ Market Quote source options
    source_group.add_argument(
        "--dhan-symbols",
        dest="dhan_symbols",
        default=None,
        help="[dhan] Comma-separated exact symbols, e.g. RELIANCE,TCS. "
             "Bare symbols select NSE cash EQ shares; indices require IDX_I:. "
             "Defaults to DHAN_SYMBOLS when no selection flag is supplied.",
    )
    source_group.add_argument(
        "--dhan-symbols-file",
        dest="dhan_symbols_file",
        default=None,
        help="[dhan] Local CSV with a symbol header. Defaults to DHAN_SYMBOLS_FILE "
             "when no selection flag is supplied; cannot be combined with symbols or IDs.",
    )
    source_group.add_argument(
        "--dhan-instruments",
        dest="dhan_instruments",
        default=None,
        help="[dhan] Comma-separated SEGMENT:SECURITY_ID values, for example "
             "NSE_EQ:1333,NSE_FNO:49081. Defaults to DHAN_INSTRUMENTS.",
    )
    source_group.add_argument(
        "--dhan-mode",
        dest="dhan_mode",
        choices=["ltp", "ohlc", "quote"],
        default=DHAN_SNAPSHOT_MODE,
        help="[dhan] REST snapshot detail level (default: DHAN_SNAPSHOT_MODE or quote).",
    )

    # ── Metadata sheet ────────────────────────────────────────────────────────
    meta_group = parser.add_argument_group("Metadata sheet")

    meta_group.add_argument(
        "--metadata-key",
        dest="metadata_key",
        default=METADATA_S3_KEY or None,
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
        default=LAKE_DATABASE,
        help="Iceberg lake database / namespace. Defaults to LAKE_DATABASE from .env.",
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
        help="MinIO bucket used for Iceberg data and connectivity pre-flight. "
             "Defaults to ICEBERG_DATA_BUCKET from .env.",
    )
    sink_group.add_argument(
        "--write-mode",
        dest="write_mode",
        choices=["overwrite", "append", "replace"],
        default=None,
        help="Iceberg write mode. Defaults to append for Dhan and overwrite for other sources.",
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

    try:
        datetime.strptime(args.ingest_date, "%Y-%m-%d")
    except ValueError:
        parser.error("--ingest-date must use YYYY-MM-DD")

    # Normalise application name to lowercase immediately
    args.application_name = args.application_name.lower().replace("-", "_").replace(" ", "_")

    # Cross-argument validation
    if args.source_type in {"s3", "dhan-live"} and not args.source_key:
        parser.error("--source-key is required for s3 and dhan-live sources")
    if args.source_type == "dhan-live" and not args.source_bucket:
        args.source_bucket = DHAN_RAW_BUCKET
    if args.source_type == "database" and not args.source_table_name:
        parser.error("--source-table-name is required when --source-type=database")
    if args.source_type == "dhan":
        if args.dhan_symbols is None and args.dhan_instruments is None and args.dhan_symbols_file is None:
            args.dhan_symbols = DHAN_SYMBOLS or None
            args.dhan_instruments = DHAN_INSTRUMENTS or None
            args.dhan_symbols_file = DHAN_SYMBOLS_FILE or None
        configured = sum(bool((value or "").strip()) for value in (
            args.dhan_symbols, args.dhan_instruments, args.dhan_symbols_file,
        ))
        if configured > 1:
            parser.error("Use only one Dhan selection: symbols, explicit instruments, or a symbols CSV file")
        if configured == 0:
            parser.error("Configure Dhan symbols, explicit instruments, or a symbols CSV file")
        if args.dhan_symbols_file:
            try:
                configured_selection(
                    symbols=args.dhan_symbols or "", instruments=args.dhan_instruments or "",
                    symbols_file=args.dhan_symbols_file, max_instruments=1000,
                )
            except DhanConfigError as exc:
                parser.error(str(exc))
        nse_today = datetime.now(ZoneInfo("Asia/Kolkata")).date().isoformat()
        if args.ingest_date != nse_today:
            parser.error(
                f"Dhan snapshots are current-only; --ingest-date must be {nse_today} (Asia/Kolkata)"
            )
    if not args.metadata_key:
        parser.error("--metadata-key (or METADATA_S3_KEY) is required")

    if args.write_mode is None:
        args.write_mode = (
            "append" if args.source_type in {"dhan", "dhan-live"} else "overwrite"
        )

    # Derive output table name from application name if not explicitly provided

    if args.output_table_name:
        args.output_table_name = (
            args.output_table_name.lower().replace("-", "_").replace(" ", "_")
        )
    else:
        args.output_table_name = args.application_name

    return args
