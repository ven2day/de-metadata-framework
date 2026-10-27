import io
import json
import os
import sys
from flask import Flask, render_template, request, Response, stream_with_context, send_file, jsonify
from flask_login import login_required, current_user
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from werkzeug.middleware.proxy_fix import ProxyFix

app = Flask(__name__)
_flask_secret = os.environ.get("FLASK_SECRET_KEY", "").strip()
if not _flask_secret or _flask_secret.startswith("REQUIRED_"):
    raise RuntimeError("FLASK_SECRET_KEY is required; generate one and set it in .env")
app.secret_key = _flask_secret

# ── Proxy trust (Cloudflare Tunnel sends X-Forwarded-Proto / X-Forwarded-For) ──
_BEHIND_PROXY = os.environ.get("BEHIND_HTTPS_PROXY", "0") == "1"
if _BEHIND_PROXY:
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

# ── Cookie security ─────────────────────────────────────────────────────────────
app.config.update(
    SESSION_COOKIE_HTTPONLY  = True,
    SESSION_COOKIE_SAMESITE  = "Lax",
    SESSION_COOKIE_SECURE    = _BEHIND_PROXY,   # True only when HTTPS is guaranteed
    REMEMBER_COOKIE_HTTPONLY = True,
    REMEMBER_COOKIE_SECURE   = _BEHIND_PROXY,
    REMEMBER_COOKIE_DURATION = 86400 * 7,       # 7 days
)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

from ingestion.env.DE_Ingestion_properties import (
    BRONZE_DATABASE, ICEBERG_CATALOG, ICEBERG_METADATA_BUCKET, LAKE_DATABASE, MINIO_ENDPOINT, MINIO_REGION,
    MINIO_ACCESS_KEY, MINIO_SECRET_KEY, LOG_S3_BUCKET, SILVER_DATABASE,
    SILVER_DATA_BUCKET, VAULT_ADDR,
    SOURCE_S3_ENDPOINT, SOURCE_S3_ACCESS_KEY, SOURCE_S3_REGION, SOURCE_S3_SECRET_KEY,
    METADATA_S3_BUCKET,
)
from gold_layer.gold_loader import (
    GoldConfig, identifier as validate_sql_identifier, resolve_tables,
    run_date as validate_gold_run_date,
)

GOLD_DATABASE = os.environ.get("GOLD_DATABASE", "de_gold")

# ── Extensions ─────────────────────────────────────────────────────────────────
from ui.extensions import csrf, login_manager, limiter
from ui.job_runtime import background_job_stream, scheduled_run_date
from ui.dhan_setup import india_date, inspect_setup as inspect_dhan_setup, snapshot_form as dhan_snapshot_form
from ui.dhan_setup import reject_credentials as reject_dhan_credentials
from ingestion.dhan.instruments import (
    DhanInstrumentError, configured_selection as configured_dhan_selection,
    parse_symbols as parse_dhan_symbols,
    resolve_selection as resolve_dhan_selection,
)
from ui.db import close_connection

csrf.init_app(app)
login_manager.init_app(app)
limiter.init_app(app)
app.teardown_appcontext(close_connection)

@login_manager.user_loader
def load_user(user_id: str):
    from ui.models import User
    try:
        user = User.get_by_id(int(user_id))
    except (TypeError, ValueError):
        return None
    return user if user and user.is_active else None

# ── Auth blueprint ─────────────────────────────────────────────────────────────
from ui.auth import auth_bp
app.register_blueprint(auth_bp)

# ── Security headers ────────────────────────────────────────────────────────────
@app.after_request
def set_security_headers(response):
    # Prevent MIME-type sniffing
    response.headers["X-Content-Type-Options"] = "nosniff"
    # Block the app from being embedded in iframes on other origins
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    # Legacy XSS filter (belt-and-suspenders for older browsers)
    response.headers["X-XSS-Protection"] = "1; mode=block"
    # Don't leak the full URL in the Referer header when navigating away
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    # Restrict browser feature APIs
    response.headers["Permissions-Policy"] = (
        "geolocation=(), microphone=(), camera=(), payment=(), usb=()"
    )
    # Content Security Policy — self-only; unsafe-inline required for existing
    # inline <script>/<style> blocks in index.html (tighten with nonces later)
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data:; "
        "font-src 'self' data:; "
        "connect-src 'self'; "
        "object-src 'none'; "
        "base-uri 'self'; "
        "form-action 'self';"
    )
    # HSTS — tell browsers to always use HTTPS for the next year (only meaningful behind HTTPS)
    if _BEHIND_PROXY:
        response.headers["Strict-Transport-Security"] = (
            "max-age=31536000; includeSubDomains; preload"
        )
    return response

# ── Docker resource names ──────────────────────────────────────────────────────
_PROJECT          = os.environ.get("COMPOSE_PROJECT_NAME", "de-metadata-framework")
_PIPELINE_IMAGE   = f"{_PROJECT}-pipeline"
_COMPOSE_NETWORK  = f"{_PROJECT}_de-net"
_VAULT_VOL        = f"{_PROJECT}_vault_secrets"
_DHAN_VOL         = f"{_PROJECT}_dhan_secrets"
_DHAN_INSTRUMENT_VOL = f"{_PROJECT}_dhan_instruments"

_COMMON_JOB_ENV_KEYS = frozenset({
    "MINIO_ENDPOINT", "MINIO_REGION", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_BUCKET",
    "SOURCE_S3_ENDPOINT", "SOURCE_S3_ACCESS_KEY", "SOURCE_S3_SECRET_KEY",
    "SOURCE_S3_BUCKET", "SOURCE_S3_REGION",
    "METADATA_S3_BUCKET", "METADATA_S3_KEY",
    "SPARK_APP_NAME", "SPARK_MASTER", "SPARK_PACKAGES", "HIVE_METASTORE_URI",
    "ICEBERG_WAREHOUSE", "ICEBERG_DATA_BUCKET", "ICEBERG_METADATA_BUCKET",
    "ICEBERG_CATALOG", "ICEBERG_DATABASE",
    "LAKE_DATABASE", "BRONZE_DATABASE", "BRONZE_DATA_BUCKET",
    "TRANSFORMATION_DATABASE", "TRANSFORMATION_DATA_BUCKET",
    "SILVER_DATABASE", "SILVER_DATA_BUCKET", "GOLD_DATABASE", "GOLD_DATA_BUCKET", "LOG_S3_BUCKET",
    "SALT_KEY", "VAULT_ADDR", "VAULT_NAMESPACE", "VAULT_PATH",
    "BREVO_API_KEY", "BREVO_FROM_EMAIL", "NOTIFY_EMAIL",
    "AWS_REGION", "AWS_DEFAULT_REGION", "TRINO_HOST", "TRINO_PORT",
})
_DATABASE_JOB_ENV_KEYS = frozenset({
    "SUPABASE_URL", "SUPABASE_KEY", "SUPABASE_JDBC_URL",
    "SUPABASE_DB_USER", "SUPABASE_DB_PASSWORD",
})
_DHAN_JOB_ENV_KEYS = frozenset({
    "DHAN_CLIENT_ID", "DHAN_ACCESS_TOKEN", "DHAN_ACCESS_TOKEN_FILE",
    "DHAN_PIN",
    "DHAN_TOTP_SECRET", "DHAN_INSTRUMENTS", "DHAN_SYMBOLS", "DHAN_SYMBOLS_FILE", "DHAN_INSTRUMENT_CACHE_FILE", "DHAN_SNAPSHOT_MODE",
    "DHAN_HTTP_TIMEOUT_SECONDS",
})


def _job_environment(source_type: str = "") -> dict[str, str]:
    """Return only the settings needed by a child pipeline container.

    Copying the UI process environment wholesale would expose unrelated secrets
    such as the Flask signing key, root password, OpenAI key, and tunnel token to
    every transient Spark job.
    """
    if source_type == "gold":
        keys = {
            "ICEBERG_CATALOG", "ICEBERG_METADATA_BUCKET", "SILVER_DATABASE",
            "GOLD_DATABASE", "GOLD_DATA_BUCKET", "LAKE_DATABASE", "BRONZE_DATABASE",
            "TRINO_HOST", "TRINO_PORT",
        }
        return {key: os.environ[key] for key in keys if key in os.environ}
    keys = set(_COMMON_JOB_ENV_KEYS)
    if source_type == "database":
        keys.update(_DATABASE_JOB_ENV_KEYS)
    elif source_type == "dhan":
        keys.update(_DHAN_JOB_ENV_KEYS)
    return {key: os.environ[key] for key in keys if key in os.environ}


def _job_volumes(source_type: str = "") -> dict[str, dict[str, str]]:
    volumes = {_VAULT_VOL: {"bind": "/vault/secrets", "mode": "ro"}}
    if source_type == "dhan":
        volumes[_DHAN_VOL] = {"bind": "/run/secrets", "mode": "rw"}
        volumes[_DHAN_INSTRUMENT_VOL] = {"bind": "/var/cache/dhan", "mode": "rw"}
    return volumes

# ── Active jobs tracking ───────────────────────────────────────────────────────
import threading as _threading
_active_jobs_lock = _threading.Lock()
_active_jobs: dict = {}  # label -> {label, app_name, date, type, started_at}


def _register_active_job(app_name: str, run_date: str, job_type: str) -> str:
    import datetime
    label = f"{app_name}_{run_date}_{job_type}"
    with _active_jobs_lock:
        _active_jobs[label] = {
            "label":        label,
            "app_name":     app_name,
            "date":         run_date,
            "type":         job_type,
            "started_at":   datetime.datetime.utcnow().isoformat(),
            "container_id": None,
        }
    return label


def _set_job_container(label: str, container_id: str) -> None:
    with _active_jobs_lock:
        if label in _active_jobs:
            _active_jobs[label]["container_id"] = container_id


def _remove_active_job(label: str) -> None:
    with _active_jobs_lock:
        _active_jobs.pop(label, None)


# ── Background job scheduler ───────────────────────────────────────────────────
_scheduler = BackgroundScheduler(timezone="UTC", daemon=True)


class _SilverJobDone(Exception):
    """Sentinel raised inside _trigger_scheduled_job to short-circuit ingestion phases."""
    def __init__(self, status: str, logs: str):
        self.status = status
        self.logs   = logs


_SILVER_SCHED_MIGRATION = """
DO $$ BEGIN
    -- Add layer_type + silver_sql columns (original migration)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='scheduled_jobs' AND column_name='layer_type'
    ) THEN
        ALTER TABLE public.scheduled_jobs ADD COLUMN layer_type VARCHAR(20) DEFAULT 'ingestion';
        ALTER TABLE public.scheduled_jobs ADD COLUMN silver_sql TEXT;
    END IF;

    -- Add silver_payload column for full form reconstruction
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='scheduled_jobs' AND column_name='silver_payload'
    ) THEN
        ALTER TABLE public.scheduled_jobs ADD COLUMN silver_payload TEXT;
    END IF;

    -- Replace single-column unique constraint with compound (application_name, layer_type)
    -- so ingestion and silver jobs with the same name coexist independently.
    IF EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'scheduled_jobs_application_name_key'
    ) THEN
        ALTER TABLE public.scheduled_jobs DROP CONSTRAINT scheduled_jobs_application_name_key;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'scheduled_jobs_app_layer_key'
    ) THEN
        ALTER TABLE public.scheduled_jobs
            ADD CONSTRAINT scheduled_jobs_app_layer_key UNIQUE (application_name, layer_type);
    END IF;

    -- Gold/Oracle fields for silver scheduled jobs (chain silver → gold)
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='scheduled_jobs' AND column_name='oracle_table'
    ) THEN
        ALTER TABLE public.scheduled_jobs ADD COLUMN oracle_table   TEXT;
        ALTER TABLE public.scheduled_jobs ADD COLUMN IF NOT EXISTS load_strategy VARCHAR(50) DEFAULT 'replace';
        ALTER TABLE public.scheduled_jobs ADD COLUMN IF NOT EXISTS date_column VARCHAR(100);
    END IF;
    -- Retain Oracle data for review; local Gold has its own destination.
    ALTER TABLE public.scheduled_jobs ADD COLUMN IF NOT EXISTS gold_table TEXT;
    ALTER TABLE public.scheduled_jobs ADD COLUMN IF NOT EXISTS load_strategy VARCHAR(50) DEFAULT 'replace';
    ALTER TABLE public.scheduled_jobs ADD COLUMN IF NOT EXISTS date_column VARCHAR(100);
    ALTER TABLE public.scheduled_jobs ALTER COLUMN load_strategy SET DEFAULT 'replace';
END $$;
"""


def _trigger_scheduled_job(job_id: int) -> None:
    import datetime
    import docker as _docker
    from ui.db import get_fresh_conn
    from psycopg2.extras import RealDictCursor
    run_date = scheduled_run_date(os.environ.get("SCHEDULER_TIMEZONE", "Asia/Kolkata"))
    conn   = None
    status = "error"
    logs   = ""
    run_id = None
    try:
        conn = get_fresh_conn()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM public.scheduled_jobs WHERE id = %s AND is_active = TRUE",
                (job_id,),
            )
            row = cur.fetchone()
            job = dict(row) if row else None
        if not job:
            return

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "INSERT INTO public.scheduled_job_runs (job_id, run_date) VALUES (%s, %s) RETURNING id",
                (job_id, run_date),
            )
            run_id = cur.fetchone()["id"]
        conn.commit()

        layer_type = (job.get("layer_type") or "ingestion").strip()
        app_name   = job["application_name"]

        # ── Silver layer: write DBT model then run via dbt run ────────────────
        if layer_type == "silver":
            silver_payload = (job.get("silver_payload") or "").strip()
            if not silver_payload:
                raise ValueError("No silver_payload stored for this scheduled job")
            key = _register_active_job(app_name, run_date, "silver")
            try:
                import subprocess as _sp
                import boto3 as _boto3
                from botocore.exceptions import ClientError as _CE

                logs += f"[silver] Ensuring MinIO bucket {SILVER_DATA_BUCKET} exists...\n"
                _s3 = _boto3.client(
                    "s3",
                    endpoint_url=MINIO_ENDPOINT,
                    aws_access_key_id=MINIO_ACCESS_KEY,
                    aws_secret_access_key=MINIO_SECRET_KEY,
                    region_name=MINIO_REGION,
                )
                try:
                    _s3.head_bucket(Bucket=SILVER_DATA_BUCKET)
                except _CE:
                    _s3.create_bucket(Bucket=SILVER_DATA_BUCKET)
                    logs += "[silver] Bucket created.\n"

                logs += "[silver] Writing DBT model file...\n"
                target_table = _write_silver_dbt_model(silver_payload, app_name)
                logs += f"[silver] Model: transformation/models/silver/{target_table}.sql\n"

                project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
                dbt_dir      = os.path.join(project_root, "transformation")
                dbt_cmd      = [
                    "dbt", "run",
                    "--select",       target_table,
                    "--profiles-dir", dbt_dir,
                    "--project-dir",  dbt_dir,
                    "--target",       "docker",
                ]
                if run_date:
                    dbt_cmd += ["--vars", f'{{"snapshot_date": "{run_date}"}}']

                logs += f"[silver] Running: {' '.join(dbt_cmd)}\n"
                result = _sp.run(
                    dbt_cmd, cwd=dbt_dir,
                    capture_output=True, text=True,
                )
                logs += result.stdout
                if result.stderr:
                    logs += result.stderr
                if result.returncode == 0:
                    logs += "[silver] DBT run completed successfully.\n"
                    logs += "--- Transform Exit 0 ---\n"
                    status = "success"

                    # ── Chain Gold layer if oracle_table is configured ────────
                    _gold_table = (job.get("gold_table") or "").strip()
                    if not _gold_table and (job.get("oracle_table") or "").strip():
                        logs += "[gold] Legacy Oracle destination was not run. Edit this schedule and choose a local Gold table to enable publishing.\n"
                    if _gold_table:
                        _gold_key  = _register_active_job(app_name, run_date, "gold")
                        _gcont     = None
                        try:
                            _load_strat  = (job.get("load_strategy") or "replace").strip()
                            _date_col    = (job.get("date_column") or "").strip()
                            gold_args    = [
                                "gold-load",
                                "--application-name", app_name,
                                "--silver-table",     target_table,
                                "--gold-table",       _gold_table,
                                "--load-strategy",    _load_strat,
                                "--run-date",         run_date,
                            ]
                            if _date_col:
                                gold_args += ["--date-column", _date_col]
                            logs += f"[gold] Publishing local Gold: {target_table} -> {_gold_table}\n"
                            _gclient = _docker.DockerClient(
                                base_url="unix:///var/run/docker.sock"
                            )
                            _genv   = _job_environment("gold")
                            _gcont  = _gclient.containers.run(
                                _PIPELINE_IMAGE,
                                command=gold_args,
                                environment=_genv,
                                network=_COMPOSE_NETWORK,
                                detach=True,
                                remove=False,
                            )
                            _set_job_container(_gold_key, _gcont.id)
                            for _chunk in _gcont.logs(stream=True, follow=True):
                                logs += _chunk.decode("utf-8", errors="replace")
                            _gresult = _gcont.wait()
                            if _gresult["StatusCode"] == 0:
                                logs += "[gold] Publish completed successfully.\n"
                                logs += "--- Gold Exit 0 ---\n"
                            else:
                                logs   += f"[gold] Publish failed (exit {_gresult['StatusCode']}).\n"
                                logs   += "--- Gold Exit 1 ---\n"
                                status  = "failed"
                        except Exception as _ge:
                            logs   += f"[gold] ERROR: {_ge}\n--- Gold Exit 1 ---\n"
                            status  = "failed"
                        finally:
                            _remove_active_job(_gold_key)
                            if _gcont:
                                try:
                                    _gcont.remove(force=True)
                                except Exception:
                                    pass
                else:
                    logs += f"[silver] DBT run failed (exit {result.returncode}).\n"
                    logs += "--- Transform Exit 1 ---\n"
                    status = "failed"
            except Exception as exc:
                logs  += f"\nERROR: {exc}\n--- Transform Exit 1 ---\n"
                status = "failed"
            finally:
                _remove_active_job(key)
            raise _SilverJobDone(status, logs)

        write_mode = job["write_mode"] or "overwrite"
        client     = _docker.DockerClient(base_url="unix:///var/run/docker.sock")
        env        = _job_environment(job["source_type"] or "s3")

        # ── Phase 1: Ingestion ─────────────────────────────────────────────────
        form_data = {
            "application_name":  app_name,
            "ingest_date":       run_date,
            "source_type":       job["source_type"] or "s3",
            "source_bucket":     job["source_bucket"] or "",
            "source_key":        job["source_key"] or "",
            "source_database":   job["source_database"] or "",
            "source_table_name": job["source_table_name"] or "",
            "metadata_key":      job["metadata_key"] or "",
            "write_mode":        write_mode,
            "log_folder":        "lake",
            "log_level":         job["log_level"] or "INFO",
            "output_catalog":    ICEBERG_CATALOG,
            "output_database":   LAKE_DATABASE,
        }
        pipeline_args = _build_pipeline_args(form_data)
        ingest_key    = _register_active_job(app_name, run_date, "ingestion")
        ingestion_exit = -1
        container = None
        try:
            container = client.containers.run(
                _PIPELINE_IMAGE,
                command=pipeline_args,
                environment=env,
                network=_COMPOSE_NETWORK,
                volumes=_job_volumes(job["source_type"] or "s3"),
                detach=True,
                remove=False,
            )
            _set_job_container(ingest_key, container.id)
            result = container.wait()
            ingestion_exit = result["StatusCode"]
            status = "success" if ingestion_exit == 0 else "failed"
            try:
                logs = container.logs(stdout=True, stderr=True).decode("utf-8", errors="replace")
            except Exception:
                pass
        finally:
            _remove_active_job(ingest_key)
            if container:
                try:
                    container.remove(force=True)
                except Exception:
                    pass

        # ── Phase 2: Bronze (runs only when ingestion succeeded) ───────────────
        if ingestion_exit == 0:
            bronze_run_type = "delta" if write_mode.strip() != "replace" else "full"
            bronze_form = {
                "application_name": app_name,
                "run_date":         run_date,
                "run_type":         bronze_run_type,
                "metadata_key":     job["metadata_key"] or "",
                "log_folder":       "bronze",
                "log_level":        job["log_level"] or "INFO",
            }
            bronze_args = _build_bronze_args(bronze_form)
            bronze_key  = _register_active_job(app_name, run_date, "bronze")
            bronze_container = None
            try:
                bronze_container = client.containers.run(
                    _PIPELINE_IMAGE,
                    command=bronze_args,
                    environment=env,
                    network=_COMPOSE_NETWORK,
                    volumes=_job_volumes(),
                    detach=True,
                    remove=False,
                )
                _set_job_container(bronze_key, bronze_container.id)
                bronze_result  = bronze_container.wait()
                bronze_exit    = bronze_result["StatusCode"]
                status         = "success" if bronze_exit == 0 else "failed"
                try:
                    bronze_logs = bronze_container.logs(stdout=True, stderr=True).decode("utf-8", errors="replace")
                except Exception:
                    bronze_logs = ""
                logs += f"\n\n--- BRONZE PHASE (run_type={bronze_run_type}) ---\n{bronze_logs}"
            except Exception as exc:
                status = "error"
                logs  += f"\n\n--- BRONZE PHASE ERROR ---\n{exc}"
            finally:
                _remove_active_job(bronze_key)
                if bronze_container:
                    try:
                        bronze_container.remove(force=True)
                    except Exception:
                        pass

    except _SilverJobDone as done:
        status = done.status
        logs   = done.logs
    except Exception as exc:
        status = "error"
        logs   = str(exc)
        print(f"[scheduler] job {job_id} error: {exc}")
    finally:
        if conn:
            try:
                if run_id:
                    with conn.cursor() as cur:
                        cur.execute(
                            """UPDATE public.scheduled_job_runs
                               SET finished_at = NOW(), status = %s, logs = %s
                               WHERE id = %s""",
                            (status, logs, run_id),
                        )
                        cur.execute(
                            "UPDATE public.scheduled_jobs SET last_run_at = NOW(), last_run_status = %s WHERE id = %s",
                            (status, job_id),
                        )
                    conn.commit()
            except Exception:
                pass
            finally:
                try:
                    conn.close()
                except Exception:
                    pass


def _cron_trigger(cron_expr: str):
    parts = cron_expr.strip().split()
    if len(parts) != 5:
        raise ValueError("Cron expression must contain five fields")
    return CronTrigger(
        minute=parts[0], hour=parts[1], day=parts[2], month=parts[3],
        day_of_week=parts[4],
        timezone=os.environ.get("SCHEDULER_TIMEZONE", "Asia/Kolkata"),
    )


def _schedule_job(job_id: int, cron_expr: str) -> None:
    _scheduler.add_job(
        _trigger_scheduled_job,
        _cron_trigger(cron_expr),
        args=[job_id],
        id=f"sjob_{job_id}",
        replace_existing=True,
    )


def _resolve_silver_macros(sql: str, snapshot_date: str = "") -> str:
    """Resolve DBT macros in silver SQL before sending to Trino.

    Strategies:
      {{ create_silver_table('t') }}          → CREATE OR REPLACE TABLE … WITH (…)
      {{ append_silver_table('t') }}          → INSERT INTO the configured Silver namespace
      {{ insert_overwrite_silver_table('t') }} → DELETE FROM … WHERE …;\n\nINSERT INTO …
      {{ get_snapshot_date() }}               → DATE 'YYYY-MM-DD' or CURRENT_DATE
    """
    import re as _re

    resolved_date = f"DATE '{snapshot_date}'" if snapshot_date else "CURRENT_DATE"

    def _expand_create(m):
        t = m.group(1)
        return (
            f"CREATE OR REPLACE TABLE {ICEBERG_CATALOG}.{SILVER_DATABASE}.{t}\n"
            f"WITH (\n"
            f"    format        = 'PARQUET',\n"
            f"    location      = 's3://{ICEBERG_METADATA_BUCKET}/{SILVER_DATABASE}/{t}/',\n"
            f"    data_location = 's3://{SILVER_DATA_BUCKET}/{t}/'\n"
            f")"
        )

    def _expand_append(m):
        t = m.group(1)
        return f"INSERT INTO {ICEBERG_CATALOG}.{SILVER_DATABASE}.{t}"

    def _expand_insert_overwrite(m):
        t = m.group(1)
        return (
            f"DELETE FROM {ICEBERG_CATALOG}.{SILVER_DATABASE}.{t}\n"
            f"WHERE snapshot_date = {resolved_date};\n\n"
            f"INSERT INTO {ICEBERG_CATALOG}.{SILVER_DATABASE}.{t}"
        )

    sql = _re.sub(r"\{\{\s*create_silver_table\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}", _expand_create, sql)
    sql = _re.sub(r"\{\{\s*append_silver_table\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}", _expand_append, sql)
    sql = _re.sub(r"\{\{\s*insert_overwrite_silver_table\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}", _expand_insert_overwrite, sql)
    sql = _re.sub(r"\{\{\s*get_snapshot_date\(\)\s*\}\}", resolved_date, sql)
    return sql


def _build_bronze_args(form) -> list[str]:
    args = [
        "bronze",
        "--application-name", form["application_name"],
        "--run-date",         form["run_date"],
        "--run-type",         form.get("run_type", "full"),
        "--log-folder",       form.get("log_folder", "bronze"),
        "--log-level",        form.get("log_level", "INFO"),
    ]
    if form.get("metadata_key"):
        args += ["--metadata-key", form["metadata_key"]]
    return args


def _build_pipeline_args(form) -> list[str]:
    source_type = form.get("source_type", "s3")
    if source_type == "dhan":
        form = dhan_snapshot_form(form)
    elif source_type not in {"s3", "database"}:
        raise ValueError("Choose a supported ingestion source.")
    args = [
        "--application-name", form["application_name"],
        "--source-type",      source_type,
        "--ingest-date",      form["ingest_date"],
        "--metadata-key",     form["metadata_key"],
        "--output-database",  form["output_database"],
        "--output-catalog",   form.get("output_catalog", ICEBERG_CATALOG),
        "--write-mode",       form.get("write_mode", "overwrite"),
        "--log-folder",       form.get("log_folder", "lake"),
        "--log-level",        form.get("log_level", "INFO"),
    ]
    args += ["--output-table-name", form["application_name"]]
    if source_type == "s3":
        args += ["--source-bucket", form["source_bucket"],
                 "--source-key",    form["source_key"]]
    elif source_type == "database":
        args += ["--source-database",   form["source_database"],
                 "--source-table-name", form["source_table_name"]]
    elif source_type == "dhan":
        args += ["--dhan-mode", form["dhan_snapshot_mode"],
                 "--dhan-instruments", form["dhan_instruments"]]
    return args


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/")
@login_required
def index():
    return render_template(
        "index.html", iceberg_catalog=ICEBERG_CATALOG, lake_database=LAKE_DATABASE,
        bronze_database=BRONZE_DATABASE, silver_database=SILVER_DATABASE,
        gold_database=GOLD_DATABASE, trading_date=india_date(), trading_timezone="Asia/Kolkata",
        dhan_symbols="",
    )


@app.route("/dhan/status")
@login_required
def dhan_status():
    try:
        result = inspect_dhan_setup()
    except Exception:
        # Neither local parsing errors nor token-file exceptions may expose secrets.
        result = inspect_dhan_setup({})
        result["message"] = "Local Dhan settings could not be inspected. Review the server configuration."
    response = jsonify(result)
    response.headers["Cache-Control"] = "no-store, private"
    response.headers["Pragma"] = "no-cache"
    return response


@app.route("/dhan/resolve", methods=["POST"])
@login_required
@limiter.limit("20 per minute")
def dhan_resolve():
    """Resolve an explicit symbol request using public data, never brokerage auth."""
    status = 200
    try:
        payload = request.get_json(silent=True) if request.is_json else request.form
        if payload is None or (request.is_json and not isinstance(payload, dict)):
            raise ValueError("Invalid symbol request")
        reject_dhan_credentials(payload)
        symbols = payload.get("symbols", "")
        if not isinstance(symbols, str):
            raise ValueError("Invalid symbols")
        symbols = symbols.strip()
        if not symbols:
            symbols, _ = configured_dhan_selection(
                symbols=os.environ.get("DHAN_SYMBOLS", ""),
                instruments=os.environ.get("DHAN_INSTRUMENTS", ""),
                symbols_file=os.environ.get("DHAN_SYMBOLS_FILE", ""),
                max_instruments=1000,
            )
        requested = parse_dhan_symbols(symbols, max_instruments=1000)
        resolved = resolve_dhan_selection(symbols=symbols, max_instruments=1000)
        if len(requested) != len(resolved):
            raise ValueError("Invalid resolution count")
        result = {
            "count": len(resolved),
            "instruments": [
                {"symbol": item.symbol, "exchange_segment": instrument.exchange_segment,
                 "security_id": instrument.security_id}
                for item, instrument in zip(requested, resolved)
            ],
            "message": "Symbols resolved using the official Dhan instrument list. Market-data access has not been verified.",
        }
    except DhanInstrumentError as exc:
        status = 503 if exc.code == "master_unavailable" else 400
        result = {"code": exc.code, "error": str(exc)}
    except ValueError:
        status = 400
        result = {"code": "invalid_symbols", "error": "Enter valid symbols only. Credentials must remain on the server."}
    except Exception:
        status = 503
        result = {"code": "master_unavailable", "error": "The official Dhan instrument list is unavailable. Try again later."}
    response = jsonify(result)
    response.status_code = status
    response.headers["Cache-Control"] = "no-store, private"
    response.headers["Pragma"] = "no-cache"
    return response


@app.route("/active-jobs")
@login_required
def active_jobs_list():
    with _active_jobs_lock:
        jobs = list(_active_jobs.values())
    return jsonify(jobs)


@app.route("/job-logs/<label>")
@login_required
def job_logs(label):
    with _active_jobs_lock:
        job = _active_jobs.get(label)
    container_id = job.get("container_id") if job else None
    if not container_id:
        return Response("No container attached to this job.\n", mimetype="text/plain")

    def generate():
        import docker as _docker
        try:
            client = _docker.DockerClient(base_url="unix:///var/run/docker.sock")
            container = client.containers.get(container_id)
            for chunk in container.logs(stream=True, follow=True, stdout=True, stderr=True):
                yield chunk.decode("utf-8", errors="replace")
        except Exception as exc:
            yield f"\nERROR attaching to container logs: {exc}\n"

    return Response(stream_with_context(generate()), mimetype="text/plain")


@app.route("/connectivity")
@login_required
def connectivity():
    import boto3
    import requests as _req
    from botocore.exceptions import BotoCoreError, ClientError

    results = {}

    try:
        client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name=MINIO_REGION,
        )
        client.list_buckets()
        results["minio"] = "ok"
    except (BotoCoreError, ClientError, Exception) as exc:
        results["minio"] = f"error: {exc}"

    try:
        r = _req.get(f"{VAULT_ADDR}/v1/sys/health", timeout=3)
        results["vault"] = "ok" if r.status_code in (200, 429) else f"error: HTTP {r.status_code}"
    except Exception as exc:
        results["vault"] = f"error: {exc}"

    return jsonify(results)


@app.route("/run", methods=["POST"])
@login_required
def run():
    try:
        form_data = dhan_snapshot_form(request.form) if request.form.get("source_type") == "dhan" else request.form
        pipeline_args = _build_pipeline_args(form_data)
    except DhanInstrumentError as exc:
        return jsonify({"code": exc.code, "error": str(exc)}), (503 if exc.code == "master_unavailable" else 400)
    except (ValueError, KeyError):
        message = "Invalid or incomplete ingestion settings."
        if request.form.get("source_type") == "dhan":
            message = "Dhan snapshot settings are incomplete or invalid. Check setup status, today's India date, and the selected instruments/mode. Credentials must remain on the server."
        return jsonify({"error": message}), 400
    app_name      = form_data.get("application_name", "")
    run_date      = form_data.get("ingest_date", "")
    write_mode    = form_data.get("write_mode", "overwrite")
    metadata_key  = form_data.get("metadata_key", "")
    log_level     = form_data.get("log_level", "INFO")
    skip_bronze   = form_data.get("skip_bronze", "0") == "1"

    def generate():
        import docker as _docker
        ingest_key     = _register_active_job(app_name, run_date, "ingestion")
        container      = None
        ingestion_exit = -1
        try:
            client = _docker.DockerClient(base_url="unix:///var/run/docker.sock")
            env = _job_environment(form_data.get("source_type", "s3"))
            if form_data.get("source_type") == "dhan":
                # The web request already resolved symbols. Canonical IDs must
                # not conflict with the UI's inherited symbol defaults in a job.
                env.update(DHAN_SYMBOLS="", DHAN_SYMBOLS_FILE="",
                           DHAN_INSTRUMENTS=form_data["dhan_instruments"])
            container = client.containers.run(
                _PIPELINE_IMAGE,
                command=pipeline_args,
                environment=env,
                network=_COMPOSE_NETWORK,
                volumes=_job_volumes(form_data.get("source_type", "s3")),
                detach=True,
                remove=False,
            )
            _set_job_container(ingest_key, container.id)
            for chunk in container.logs(stream=True, follow=True):
                yield chunk.decode("utf-8", errors="replace")
            result = container.wait()
            ingestion_exit = result["StatusCode"]
            yield f"\n--- Exit {ingestion_exit} ---\n"
        except Exception as exc:
            yield f"\nERROR launching pipeline container: {exc}\n"
        finally:
            _remove_active_job(ingest_key)
            if container:
                try:
                    container.remove(force=True)
                except Exception:
                    pass

        # ── Auto-trigger bronze when ingestion succeeds ────────────────────────
        if ingestion_exit == 0 and not skip_bronze:
            bronze_run_type  = "delta" if write_mode.strip() != "replace" else "full"
            bronze_form      = {
                "application_name": app_name,
                "run_date":         run_date,
                "run_type":         bronze_run_type,
                "metadata_key":     metadata_key,
                "log_folder":       "bronze",
                "log_level":        log_level,
            }
            bronze_args     = _build_bronze_args(bronze_form)
            bronze_key      = _register_active_job(app_name, run_date, "bronze")
            bronze_container = None
            try:
                yield f"\n--- BRONZE AUTO-TRIGGER run_type={bronze_run_type} ---\n"
                client = _docker.DockerClient(base_url="unix:///var/run/docker.sock")
                env = _job_environment()
                bronze_container = client.containers.run(
                    _PIPELINE_IMAGE,
                    command=bronze_args,
                    environment=env,
                    network=_COMPOSE_NETWORK,
                    volumes=_job_volumes(),
                    detach=True,
                    remove=False,
                )
                _set_job_container(bronze_key, bronze_container.id)
                for chunk in bronze_container.logs(stream=True, follow=True):
                    yield chunk.decode("utf-8", errors="replace")
                bronze_result = bronze_container.wait()
                yield f"\n--- Bronze Exit {bronze_result['StatusCode']} ---\n"
            except Exception as exc:
                yield f"\nERROR launching bronze container: {exc}\n"
            finally:
                if bronze_container:
                    try:
                        bronze_container.reload()
                        still_running = bronze_container.status in ("running", "created")
                    except Exception:
                        still_running = False

                    if still_running:
                        # Client disconnected mid-stream but container is still running.
                        # Hand off to a watcher thread so Active Jobs stays accurate.
                        import threading as _th
                        def _bronze_watcher(c=bronze_container, key=bronze_key):
                            try:
                                c.wait()
                            except Exception:
                                pass
                            finally:
                                _remove_active_job(key)
                                try:
                                    c.remove(force=True)
                                except Exception:
                                    pass
                        _th.Thread(target=_bronze_watcher, daemon=True).start()
                    else:
                        _remove_active_job(bronze_key)
                        try:
                            bronze_container.remove(force=True)
                        except Exception:
                            pass
                else:
                    _remove_active_job(bronze_key)

    return Response(background_job_stream(generate), mimetype="text/plain")


@app.route("/run-bronze", methods=["POST"])
@login_required
def run_bronze():
    bronze_args = _build_bronze_args(request.form)

    def generate():
        import docker as _docker
        container = None
        try:
            client = _docker.DockerClient(base_url="unix:///var/run/docker.sock")
            env = _job_environment()

            container = client.containers.run(
                _PIPELINE_IMAGE,
                command=bronze_args,
                environment=env,
                network=_COMPOSE_NETWORK,
                volumes=_job_volumes(),
                detach=True,
                remove=False,
            )

            for chunk in container.logs(stream=True, follow=True):
                yield chunk.decode("utf-8", errors="replace")

            result = container.wait()
            yield f"\n--- Exit {result['StatusCode']} ---\n"

        except Exception as exc:
            yield f"\nERROR launching bronze container: {exc}\n"
        finally:
            if container:
                try:
                    container.remove(force=True)
                except Exception:
                    pass

    return Response(background_job_stream(generate), mimetype="text/plain")


@app.route("/list-bucket-keys")
@login_required
def list_bucket_keys():
    import boto3
    bucket = request.args.get("bucket", "").strip()
    prefix = request.args.get("prefix", "")
    if not bucket:
        return jsonify([])
    client = boto3.client(
        "s3",
        endpoint_url=(SOURCE_S3_ENDPOINT or "").strip(),
        aws_access_key_id=SOURCE_S3_ACCESS_KEY,
        aws_secret_access_key=SOURCE_S3_SECRET_KEY,
        region_name=SOURCE_S3_REGION,
    )
    try:
        resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=500)
        keys = [obj["Key"] for obj in resp.get("Contents", [])]
    except Exception:
        return jsonify([])
    return jsonify(keys)


@app.route("/list-metadata-keys")
@login_required
def list_metadata_keys():
    import boto3
    meta_bucket = (METADATA_S3_BUCKET or "de-metadata-bucket").strip()
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION,
    )
    try:
        resp = client.list_objects_v2(Bucket=meta_bucket, MaxKeys=500)
        keys = [obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].lower().endswith(".csv")]
    except Exception:
        return jsonify([])
    return jsonify(keys)


@app.route("/metadata-preview")
@login_required
def metadata_preview():
    import boto3, csv, io
    key = request.args.get("key", "").strip()
    if not key:
        return jsonify({"headers": [], "rows": []})
    meta_bucket = (METADATA_S3_BUCKET or "de-metadata-bucket").strip()
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION,
    )
    try:
        obj     = client.get_object(Bucket=meta_bucket, Key=key)
        content = obj["Body"].read().decode("utf-8", errors="replace")
        reader  = csv.DictReader(io.StringIO(content))
        rows    = [dict(r) for r in reader]
        headers = list(reader.fieldnames or [])
        return jsonify({"headers": headers, "rows": rows})
    except Exception as exc:
        return jsonify({"error": str(exc), "headers": [], "rows": []})


@app.route("/log-history")
@login_required
def log_history():
    import boto3
    from collections import defaultdict

    app_name   = request.args.get("app_name", "").strip()
    log_folder = request.args.get("log_folder", "lake")
    if not app_name:
        return jsonify([])

    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION,
    )
    try:
        resp    = client.list_objects_v2(Bucket=LOG_S3_BUCKET, Prefix=f"{log_folder}/{app_name}/")
        objects = resp.get("Contents", [])
    except Exception:
        return jsonify([])

    by_date = defaultdict(list)
    for obj in objects:
        parts = obj["Key"].split("/")
        if len(parts) >= 3:
            by_date[parts[2]].append(obj)

    runs = []
    for date in sorted(by_date.keys(), reverse=True):
        entries = sorted(by_date[date], key=lambda o: o["LastModified"])
        date_runs = [
            {
                "label":   f"{app_name}_{date}__attempt_{i}",
                "key":     obj["Key"],
                "date":    date,
                "attempt": i,
                "size":    obj["Size"],
            }
            for i, obj in enumerate(entries, 1)
        ]
        runs.extend(reversed(date_runs))
    return jsonify(runs)


@app.route("/view-log")
@login_required
def view_log():
    import boto3

    key = request.args.get("key", "")
    if not key:
        return ("", 400)
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION,
    )
    buf = io.BytesIO()
    client.download_fileobj(LOG_S3_BUCKET, key, buf)
    buf.seek(0)
    return Response(buf.read().decode("utf-8", errors="replace"), mimetype="text/plain")


def _find_log_key(client, app_name: str, ingest_date: str, log_folder: str) -> str:
    prefix = f"{log_folder}/{app_name}/{ingest_date}/{app_name}_{ingest_date}"
    resp = client.list_objects_v2(Bucket=LOG_S3_BUCKET, Prefix=prefix)
    objects = resp.get("Contents", [])
    if not objects:
        raise FileNotFoundError(f"No log found for prefix {prefix}")
    return sorted(objects, key=lambda o: o["LastModified"], reverse=True)[0]["Key"]


@app.route("/download-log")
@login_required
def download_log():
    import boto3

    app_name    = request.args.get("app_name", "")
    ingest_date = request.args.get("ingest_date", "")
    log_folder  = request.args.get("log_folder", "lake")

    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name=MINIO_REGION,
    )

    s3_key   = _find_log_key(client, app_name, ingest_date, log_folder)
    filename = os.path.basename(s3_key)

    buf = io.BytesIO()
    client.download_fileobj(LOG_S3_BUCKET, s3_key, buf)
    buf.seek(0)

    return send_file(buf, mimetype="text/plain", as_attachment=True, download_name=filename)


@app.route("/scheduled-jobs", methods=["GET"])
@login_required
def list_scheduled_jobs():
    from ui.db import get_conn
    from psycopg2.extras import RealDictCursor
    conn = get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM public.scheduled_jobs WHERE is_active = TRUE ORDER BY created_at DESC"
        )
        jobs = [dict(r) for r in cur.fetchall()]
    for j in jobs:
        if j.get("created_at"):
            j["created_at"] = j["created_at"].isoformat()
        if j.get("last_run_at"):
            j["last_run_at"] = j["last_run_at"].isoformat()
    return jsonify(jobs)


def _fqn_to_dbt_ref(fqn: str) -> str:
    """Quote a runtime-selected Trino table without requiring a dbt registration.

    The browser can select tables created outside this dbt project. A ref/source
    would incorrectly require a matching model or YAML declaration at parse time.
    """
    if not isinstance(fqn, str):
        raise ValueError("Source table must be a three-part catalog.schema.table name")
    parts = fqn.strip().split(".")
    if len(parts) != 3:
        raise ValueError("Source table must be a three-part catalog.schema.table name")
    return ".".join(f'"{validate_sql_identifier(part)}"' for part in parts)


def _overwrite_silver_cleanup(target_table: str) -> list:
    """Preserve snapshots; dbt performs atomic Iceberg replacement."""
    return [f"[silver] Existing {target_table} data is retained until replacement commits.\n"]


def _ensure_silver_sources(model_sql: str, project_root: str) -> None:
    """Parse {{ source('x', 'y') }} refs from model SQL and upsert models/silver/_sources.yml."""
    import re   as _re
    import yaml as _yaml

    SOURCE_RE = _re.compile(
        r"\{\{\s*source\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}",
        _re.IGNORECASE,
    )
    refs = set(SOURCE_RE.findall(model_sql))
    if not refs:
        return

    sources_path = os.path.join(
        project_root, "transformation", "models", "silver", "_sources.yml"
    )

    if os.path.isfile(sources_path):
        with open(sources_path, encoding="utf-8") as fh:
            doc = _yaml.safe_load(fh) or {}
    else:
        doc = {}

    doc.setdefault("version", 2)
    doc.setdefault("sources", [])

    src_by_name = {s["name"]: s for s in doc["sources"]}

    changed = False
    for src_name, table_name in sorted(refs):
        if src_name not in src_by_name:
            # Source name is the schema name; database is the configured catalog.
            entry = {
                "name":     src_name,
                "database": ICEBERG_CATALOG,
                "schema":   src_name,
                "tables":   [],
            }
            src_by_name[src_name] = entry
            doc["sources"].append(entry)
            changed = True

        existing_tables = {t["name"] for t in src_by_name[src_name].get("tables", [])}
        if table_name not in existing_tables:
            src_by_name[src_name].setdefault("tables", []).append({"name": table_name})
            changed = True

    if changed:
        with open(sources_path, "w", encoding="utf-8") as fh:
            _yaml.dump(doc, fh, default_flow_style=False, sort_keys=False, allow_unicode=True)
        app.logger.info("Updated silver _sources.yml: %s", sources_path)


def _write_silver_dbt_model(silver_payload_json: str, app_name: str) -> str:
    """Parse a saved silver_payload, write a proper DBT model .sql file, return target_table."""
    import json as _json
    import re as _re

    payload       = _json.loads(silver_payload_json)
    target_table  = (payload.get("target_table") or app_name).strip()
    sources       = payload.get("sources", [])
    col_mappings  = payload.get("columns", [])
    extra_filters = payload.get("filters", [])
    strategy      = (payload.get("strategy") or "overwrite").strip()

    if not sources or not target_table:
        raise ValueError("silver_payload missing sources or target_table")
    if not _re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", target_table):
        raise ValueError("target_table must be a simple SQL identifier")

    # Map silver strategy to DBT materialization
    _MAT = {
        "overwrite":        ("table",       {"on_table_exists": "'replace'"}),
        "append":           ("incremental", {"incremental_strategy": "'append'"}),
        "insert_overwrite": ("incremental", {"incremental_strategy": "'delete+insert'",
                                             "unique_key":           "'snapshot_date'"}),
    }
    if strategy not in _MAT:
        raise ValueError(f"Unsupported Silver strategy: {strategy}")
    mat, extra_cfg = _MAT[strategy]

    # Iceberg table properties: data and metadata in separate buckets.
    iceberg_props = (
        f"    properties={{\n"
        f"        \"format\":        \"'PARQUET'\",\n"
        f"        \"location\":      \"'s3://{ICEBERG_METADATA_BUCKET}/{SILVER_DATABASE}/{target_table}/'\",\n"
        f"        \"data_location\": \"'s3://{SILVER_DATA_BUCKET}/{target_table}/'\"\n"
        f"    }},\n"
    )
    extra_cfg_str = "".join(f"    {k}={v},\n" for k, v in extra_cfg.items())

    config_block = (
        f"{{{{\n  config(\n"
        f"    materialized='{mat}',\n"
        f"{iceberg_props}"
        f"{extra_cfg_str}"
        f"  )\n}}}}"
    )

    # Build SELECT list
    select_parts = []
    for col in col_mappings:
        expr   = (col.get("source_expr") or "").strip()
        target = (col.get("target") or "").strip() or expr.split(".")[-1]
        cast   = (col.get("cast") or "").strip()
        if cast:
            expr = f"CAST({expr} AS {cast})"
        if expr:
            select_parts.append(f"    {expr} AS {target}")
    select_clause = ",\n".join(select_parts) if select_parts else "    *"

    # Runtime-selected tables are quoted directly, without dbt model dependencies.
    first     = sources[0]
    alias     = (first.get("alias") or "src").strip()
    from_ref  = _fqn_to_dbt_ref(first.get("fqn", ""))
    from_clause = f"{from_ref} {alias}"
    where_parts = []
    if first.get("snapshot_date"):
        where_parts.append(f"{alias}.snapshot_date = {{{{ get_snapshot_date() }}}}")

    for i, s in enumerate(sources[1:], 2):
        a         = (s.get("alias") or f"src{i}").strip()
        join_type = (s.get("join_type") or "INNER JOIN").strip().upper()
        join_conds = [c for c in (s.get("join_conds") or [])
                      if (c.get("lh") or "").strip() and (c.get("rh") or "").strip()]
        ref_str   = _fqn_to_dbt_ref(s.get("fqn", ""))
        if join_type == "CROSS JOIN" or not join_conds:
            from_clause += f"\n    {join_type} {ref_str} {a}"
        else:
            on_parts = "\n        AND ".join(
                f"{c['lh'].strip()} = {c['rh'].strip()}" for c in join_conds
            )
            from_clause += f"\n    {join_type} {ref_str} {a}\n    ON {on_parts}"
        if s.get("snapshot_date"):
            where_parts.append(f"{a}.snapshot_date = {{{{ get_snapshot_date() }}}}")

    for f in extra_filters:
        col = (f.get("col") or "").strip()
        op  = (f.get("op") or "=").strip().upper()
        val = (f.get("val") or "").strip()
        if not col:
            continue
        if op in ("IS NULL", "IS NOT NULL"):
            where_parts.append(f"{col} {op}")
        elif val:
            quoted = val if (val.startswith("'") or val.lstrip("-").replace(".", "", 1).isdigit()) else f"'{val}'"
            where_parts.append(f"{col} {op} {quoted}")

    where_clause = ("\nWHERE " + "\n  AND ".join(where_parts)) if where_parts else ""

    model_sql = (
        f"{config_block}\n\n"
        f"SELECT\n{select_clause}\n"
        f"FROM {from_clause}"
        f"{where_clause}\n"
    )

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    silver_dir   = os.path.join(project_root, "transformation", "models", "silver")
    os.makedirs(silver_dir, exist_ok=True)
    model_path   = os.path.join(silver_dir, f"{target_table}.sql")
    with open(model_path, "w", encoding="utf-8") as fh:
        fh.write(model_sql)
    app.logger.info("DBT model written: %s", model_path)

    _ensure_silver_sources(model_sql, project_root)
    return target_table


@app.route("/scheduled-jobs", methods=["POST"])
@login_required
def create_scheduled_job():
    from ui.db import get_conn
    from psycopg2.extras import RealDictCursor
    data = request.get_json(force=True)
    app_name       = data.get("application_name", "").strip()
    layer_type     = data.get("layer_type", "ingestion").strip() or "ingestion"
    silver_sql     = data.get("silver_sql") or None
    silver_payload = data.get("silver_payload") or None
    gold_table     = (data.get("gold_table") or "").strip() or None
    load_strategy  = (data.get("load_strategy") or "replace").strip()
    date_column    = (data.get("date_column") or "").strip() or None

    if data.get("source_type") == "dhan":
        return jsonify({"error": "Dhan snapshots are currently manual UI runs. Use the CLI/Compose guide for unattended scheduling."}), 400

    if not app_name:
        return jsonify({"error": "application_name is required"}), 400
    if layer_type == "silver" and not silver_sql:
        return jsonify({"error": "silver_sql is required for silver layer jobs"}), 400
    if data.get("oracle_table"):
        return jsonify({"error": "Oracle schedules are retired. Choose gold_table for local Iceberg Gold."}), 400
    try:
        if gold_table:
            resolve_tables("validation_source", gold_table, GoldConfig.from_env())
        if load_strategy not in {"replace", "append", "delete_and_insert"}:
            raise ValueError("Invalid local Gold load strategy")
        if gold_table and load_strategy == "delete_and_insert" and not date_column:
            raise ValueError("date_column is required for date replacement")
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    # Validate field values before saving a row that startup will later load.
    cron = (data.get("cron_expression") or "").strip()
    try:
        _cron_trigger(cron)
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid cron expression (min hour day month weekday)"}), 400

    conn = get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Run all schema migrations idempotently
        cur.execute(_SILVER_SCHED_MIGRATION)
        # Upsert keyed on (application_name, layer_type) — ingestion and silver are independent rows
        cur.execute(
            """INSERT INTO public.scheduled_jobs
               (application_name, cron_expression, source_type, source_bucket, source_key,
                source_database, source_table_name, metadata_key, write_mode, log_level,
                layer_type, silver_sql, silver_payload,
                gold_table, load_strategy, date_column, is_active)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, TRUE)
               ON CONFLICT (application_name, layer_type) DO UPDATE SET
                   cron_expression   = EXCLUDED.cron_expression,
                   source_type       = EXCLUDED.source_type,
                   source_bucket     = EXCLUDED.source_bucket,
                   source_key        = EXCLUDED.source_key,
                   source_database   = EXCLUDED.source_database,
                   source_table_name = EXCLUDED.source_table_name,
                   metadata_key      = EXCLUDED.metadata_key,
                   write_mode        = EXCLUDED.write_mode,
                   log_level         = EXCLUDED.log_level,
                   silver_sql        = EXCLUDED.silver_sql,
                   silver_payload    = EXCLUDED.silver_payload,
                   gold_table        = EXCLUDED.gold_table,
                   load_strategy     = EXCLUDED.load_strategy,
                   date_column       = EXCLUDED.date_column,
                   is_active         = TRUE
               RETURNING id""",
            (
                app_name, cron,
                data.get("source_type", "s3"),
                data.get("source_bucket") or None, data.get("source_key") or None,
                data.get("source_database") or None, data.get("source_table_name") or None,
                data.get("metadata_key") or None,
                data.get("write_mode", "overwrite"), data.get("log_level", "INFO"),
                layer_type, silver_sql, silver_payload,
                gold_table, load_strategy, date_column,
            ),
        )
        job_id = cur.fetchone()["id"]
    conn.commit()
    # _schedule_job uses replace_existing=True, so re-registering same id is safe
    _schedule_job(job_id, data["cron_expression"])

    # ── Write DBT model file for silver schedules ──────────────────────────────
    if layer_type == "silver" and silver_payload:
        try:
            _write_silver_dbt_model(silver_payload, app_name)
        except Exception as _e:
            app.logger.warning("Could not write DBT model file: %s", _e)

    return jsonify({"id": job_id, "status": "scheduled"}), 201


@app.route("/scheduled-job-runs")
@login_required
def list_scheduled_job_runs():
    from ui.db import get_conn
    from psycopg2.extras import RealDictCursor
    job_id = request.args.get("job_id", type=int)
    conn = get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if job_id:
            cur.execute(
                """SELECT r.id, r.job_id, r.run_date, r.started_at, r.finished_at, r.status, r.logs
                   FROM public.scheduled_job_runs r
                   WHERE r.job_id = %s
                   ORDER BY r.started_at DESC LIMIT 50""",
                (job_id,),
            )
        else:
            cur.execute(
                """SELECT r.id, r.job_id, r.run_date, r.started_at, r.finished_at, r.status, r.logs,
                          j.application_name
                   FROM public.scheduled_job_runs r
                   JOIN public.scheduled_jobs j ON j.id = r.job_id
                   ORDER BY r.started_at DESC LIMIT 100"""
            )
        runs = [dict(row) for row in cur.fetchall()]
    for r in runs:
        for col in ("started_at", "finished_at"):
            if r.get(col):
                r[col] = r[col].isoformat()
        if r.get("run_date"):
            r["run_date"] = str(r["run_date"])
    return jsonify(runs)


@app.route("/scheduled-jobs/<int:job_id>", methods=["DELETE"])
@login_required
def delete_scheduled_job(job_id):
    from ui.db import get_conn
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE public.scheduled_jobs SET is_active = FALSE WHERE id = %s", (job_id,)
        )
    conn.commit()
    try:
        _scheduler.remove_job(f"sjob_{job_id}")
    except Exception:
        pass
    return jsonify({"status": "deleted"})


_SPARK_SQL_URL = os.environ.get("SPARK_SQL_URL", "http://spark-sql:5002")


def _build_silver_args(form) -> list[str]:
    args = [
        "dbt-run",
        "--application-name", form["application_name"],
        "--run-date",         form["run_date"],
    ]
    if form.get("dbt_select"):
        args += ["--dbt-select", form["dbt_select"]]
    return args


def _build_gold_args(form) -> list[str]:
    resolve_tables(form["silver_table"], form["gold_table"], GoldConfig.from_env())
    validate_gold_run_date(form["run_date"])
    strategy = form.get("load_strategy", "replace")
    if strategy not in {"replace", "append", "delete_and_insert"}:
        raise ValueError("Invalid local Gold load strategy")
    if strategy == "delete_and_insert" and not form.get("date_column"):
        raise ValueError("date_column is required for date replacement")
    args = [
        "gold-load",
        "--application-name", form["application_name"],
        "--silver-table",     form["silver_table"],
        "--gold-table",       form["gold_table"],
        "--load-strategy",    strategy,
        "--run-date",         form["run_date"],
    ]
    if form.get("date_column"):
        args += ["--date-column", form["date_column"]]
    return args


@app.route("/silver/check-table", methods=["POST"])
@login_required
def silver_check_table():
    data = request.get_json(force=True)
    table_input = (data.get("table") or "").strip()
    if not table_input:
        return jsonify({"exists": False, "error": "Table name required"}), 400

    parts = table_input.split(".")
    if len(parts) == 1:
        catalog, schema, tbl = ICEBERG_CATALOG, BRONZE_DATABASE, parts[0]
    elif len(parts) == 2:
        catalog, schema, tbl = ICEBERG_CATALOG, parts[0], parts[1]
    else:
        catalog, schema, tbl = parts[0], parts[1], parts[2]
    fqn = f"{catalog}.{schema}.{tbl}"

    try:
        import trino.dbapi as _trino
        conn = _trino.connect(
            host=os.environ.get("TRINO_HOST", "trino"),
            port=int(os.environ.get("TRINO_PORT", "8080")),
            user="de-ui", catalog=catalog, schema=schema,
        )
        cur = conn.cursor()
        cur.execute(
            f"SELECT column_name, data_type FROM {catalog}.information_schema.columns "
            f"WHERE table_schema = '{schema}' AND table_name = '{tbl}' "
            f"ORDER BY ordinal_position"
        )
        rows = cur.fetchall()
        conn.close()
        if not rows:
            return jsonify({"exists": False, "error": "Table not found in catalog"})
        columns = [{"name": r[0], "type": r[1]} for r in rows]
        return jsonify({"exists": True, "columns": columns, "fqn": fqn})
    except Exception as exc:
        return jsonify({"exists": False, "error": str(exc)})


@app.route("/silver/convert-logic", methods=["POST"])
@login_required
def silver_convert_logic():
    data = request.get_json(force=True)
    logic   = (data.get("logic") or "").strip()
    columns = data.get("columns", [])
    if not logic:
        return jsonify({"error": "Logic is required"}), 400
    openai_key = os.environ.get("OPENAI_API_KEY", "")
    if not openai_key:
        return jsonify({"error": "OPENAI_API_KEY not configured"}), 503
    try:
        import openai as _openai
        client = _openai.OpenAI(api_key=openai_key)
        system = (
            "You are a Trino SQL expert specialising in Apache Iceberg tables. "
            "Convert natural language transformation logic into a single SQL expression "
            "suitable for a Trino SELECT clause on Iceberg tables.\n\n"
            "STRICT RULES:\n"
            "- Use ONLY Trino-compatible SQL functions that work on Iceberg tables "
            "(e.g. date_diff, date_add, date_trunc, date_format, from_unixtime, "
            "concat, regexp_replace, split_part, try_cast, coalesce, nullif, "
            "if, case/when, array_join, element_at, cardinality, etc.).\n"
            "- Do NOT use Python syntax, Python functions, f-strings, variable assignments, "
            "lambda expressions, or any non-SQL constructs.\n"
            "- Return ONLY the raw SQL expression — no SELECT keyword, no AS alias, "
            "no semicolon, no explanation, no markdown code fences, no comments.\n"
            "- The output must be pasteable directly after SELECT and before AS column_name."
        )
        user_msg = f"Available columns: {', '.join(columns)}\n\nLogic: {logic}"
        resp = client.chat.completions.create(
            model=os.environ.get("OPENAI_MODEL", "gpt-4.1-nano"),
            messages=[{"role": "system", "content": system}, {"role": "user", "content": user_msg}],
            max_tokens=200,
        )
        sql = resp.choices[0].message.content.strip().strip("`").strip()
        if sql.lower().startswith("sql"):
            sql = sql[3:].strip()
        return jsonify({"sql": sql})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/silver/preview-sql", methods=["POST"])
@login_required
def silver_preview_sql():
    data = request.get_json(force=True)
    sources      = data.get("sources", [])
    col_mappings = data.get("columns", [])
    target_table = (data.get("target_table") or "").strip()
    extra_filters = data.get("filters", [])
    if not sources or not target_table:
        return jsonify({"error": "sources and target_table are required"}), 400

    # Build SELECT list
    select_parts = []
    for col in col_mappings:
        expr   = col.get("source_expr", "").strip()
        target = col.get("target", "").strip() or expr.split(".")[-1]
        cast   = col.get("cast", "").strip()
        if cast:
            expr = f"CAST({expr} AS {cast})"
        select_parts.append(f"    {expr} AS {target}")
    select_clause = ",\n".join(select_parts) if select_parts else "    *"

    # Build FROM clause + per-table snapshot_date WHERE conditions
    first = sources[0]
    alias = first.get("alias") or "src"
    from_clause = f"{first['fqn']} {alias}"
    where_parts = []
    if first.get("snapshot_date"):
        where_parts.append(f"{alias}.snapshot_date = {{{{ get_snapshot_date() }}}}")
    for i, s in enumerate(sources[1:], 2):
        a = s.get("alias") or f"src{i}"
        join_type  = (s.get("join_type") or "INNER JOIN").strip().upper()
        join_conds = [c for c in (s.get("join_conds") or [])
                      if (c.get("lh") or "").strip() and (c.get("rh") or "").strip()]
        if join_type == "CROSS JOIN" or not join_conds:
            from_clause += f"\n    {join_type} {s['fqn']} {a}"
        else:
            on_parts = "\n      AND ".join(
                f"{c['lh'].strip()} = {c['rh'].strip()}" for c in join_conds
            )
            from_clause += f"\n    {join_type} {s['fqn']} {a}\n    ON {on_parts}"
        if s.get("snapshot_date"):
            where_parts.append(f"{a}.snapshot_date = {{{{ get_snapshot_date() }}}}")

    # Append extra user-defined filters
    for f in extra_filters:
        col = (f.get("col") or "").strip()
        op  = (f.get("op") or "=").strip().upper()
        val = (f.get("val") or "").strip()
        if not col:
            continue
        if op in ("IS NULL", "IS NOT NULL"):
            where_parts.append(f"{col} {op}")
        elif val:
            # Quote string-looking values; leave numbers/expressions bare
            quoted = val if (val.startswith("'") or val.lstrip("-").replace(".","",1).isdigit()) else f"'{val}'"
            where_parts.append(f"{col} {op} {quoted}")

    where_clause = ("\nWHERE " + "\n  AND ".join(where_parts)) if where_parts else ""

    strategy = (data.get("strategy") or "overwrite").strip()
    _STRATEGY_MACRO = {
        "overwrite":        ("create_silver_table",          True),
        "append":           ("append_silver_table",          False),
        "insert_overwrite": ("insert_overwrite_silver_table", False),
    }
    macro_name, needs_as = _STRATEGY_MACRO.get(strategy, ("create_silver_table", True))

    header = f"{{{{ {macro_name}('{target_table}') }}}}"
    joiner = "\nAS\n" if needs_as else "\n"
    sql = (
        f"{header}{joiner}"
        f"SELECT\n{select_clause}\n"
        f"FROM {from_clause}"
        f"{where_clause}"
    )
    return jsonify({"sql": sql})


@app.route("/silver/flowchart-from-sql", methods=["POST"])
@login_required
def silver_flowchart_from_sql():
    data = request.get_json(force=True)
    sql  = (data.get("sql") or "").strip()
    if not sql:
        return jsonify({"error": "SQL is required"}), 400
    # Expand macros so the AI sees fully-resolved SQL, not DBT template syntax.
    sql = _resolve_silver_macros(sql, snapshot_date="")

    openai_key = os.environ.get("OPENAI_API_KEY", "")
    if not openai_key:
        return jsonify({"error": "OPENAI_API_KEY not configured"}), 503

    system = (
        "You are a SQL pipeline analyser. "
        "Given a Trino CTAS statement, return a pipeline graph as JSON.\n\n"
        "Return ONLY valid JSON with this exact schema — no explanation, no markdown:\n"
        "{\n"
        '  "nodes": [\n'
        '    {"id":"<unique string>","type":"source|join|transform|target",'
        '"label":"<short name>","sublabel":"<secondary line>","lines":["<detail line>",...]}\n'
        "  ],\n"
        '  "edges": [{"from":"<node id>","to":"<node id>"}]\n'
        "}\n\n"
        "Rules:\n"
        "- One 'source' node per FROM/JOIN table: label=alias, sublabel=schema.table, "
        "lines=[snapshot_date filter string if present, otherwise empty array].\n"
        "- One 'join' node per JOIN clause: label=join type (e.g. 'INNER JOIN'), sublabel='', "
        "lines=[each ON condition as a separate string, e.g. 'a.id = b.id'].\n"
        "- One 'transform' node if SELECT has explicit column expressions (not SELECT *): "
        "label='Transform', sublabel='N column mappings' (replace N with the actual count), "
        "lines=[each column as 'sql_expression → target_column'; show up to 8, "
        "then add '+ N more' as the final entry if truncated]. "
        "Omit this node only if the SELECT clause is literally SELECT *.\n"
        f"- One 'target' node: label=target table name, sublabel='{ICEBERG_CATALOG}.{SILVER_DATABASE}', lines=[].\n"
        "- Edge direction: each source feeds its join node; join nodes chain left-to-right; "
        "the last join (or sources if no joins) feeds transform (if present), then target.\n"
        "- 'lines' must always be a JSON array (use [] when empty).\n"
        "- All node ids must be unique strings with no spaces."
    )

    try:
        import openai as _openai
        client = _openai.OpenAI(api_key=openai_key)
        resp = client.chat.completions.create(
            model=os.environ.get("OPENAI_MODEL", "gpt-4.1-nano"),
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": f"SQL:\n{sql}"},
            ],
            response_format={"type": "json_object"},
            max_tokens=1800,
            temperature=0,
        )
        graph = json.loads(resp.choices[0].message.content)
        return jsonify(graph)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/silver/compile-sql", methods=["POST"])
@login_required
def silver_compile_sql():
    data         = request.get_json(force=True)
    sql          = (data.get("sql") or "").strip()
    snapshot_date = (data.get("run_date") or "").strip()
    if not sql:
        return jsonify({"error": "sql is required"}), 400
    return jsonify({"sql": _resolve_silver_macros(sql, snapshot_date=snapshot_date)})


@app.route("/silver/run-transform", methods=["POST"])
@login_required
def silver_run_transform():
    data            = request.get_json(force=True)
    silver_payload  = (data.get("silver_payload") or "").strip()
    app_name        = (data.get("app_name") or "transform").strip()
    run_date        = (data.get("run_date") or "").strip()
    if not silver_payload:
        return jsonify({"error": "silver_payload is required"}), 400

    def generate():
        import subprocess as _sp
        import boto3 as _boto3
        import json   as _json
        from botocore.exceptions import ClientError as _CE

        key = _register_active_job(app_name, run_date, "silver")
        try:
            # ── Step 1: ensure MinIO buckets exist ───────────────
            yield f"[silver] Ensuring MinIO bucket {SILVER_DATA_BUCKET} exists...\n"
            _s3 = _boto3.client(
                "s3",
                endpoint_url=MINIO_ENDPOINT,
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY,
                region_name=MINIO_REGION,
            )
            for _bkt in (SILVER_DATA_BUCKET, ICEBERG_METADATA_BUCKET):
                try:
                    _s3.head_bucket(Bucket=_bkt)
                    yield f"[silver] Bucket '{_bkt}' OK.\n"
                except _CE:
                    _s3.create_bucket(Bucket=_bkt)
                    yield f"[silver] Bucket '{_bkt}' created.\n"

            # ── Step 2: parse payload metadata ───────────────────
            _payload       = _json.loads(silver_payload)
            _strategy      = (_payload.get("strategy") or "overwrite").strip()
            _sources_count = len(_payload.get("sources", []))
            _cols_count    = len(_payload.get("columns", []))
            _MAT_LABEL     = {
                "overwrite":        "table (full overwrite)",
                "append":           "incremental (append)",
                "insert_overwrite": "incremental (delete+insert)",
            }
            _mat_label = _MAT_LABEL.get(_strategy, _strategy)

            # ── Step 3: write DBT model file ─────────────────────
            yield "[silver] Writing DBT model file...\n"
            target_table = _write_silver_dbt_model(silver_payload, app_name)
            yield f"[silver] Model written: transformation/models/silver/{target_table}.sql\n"
            yield f"[silver] Materialization : {_mat_label}\n"
            yield f"[silver] Source tables   : {_sources_count}\n"
            yield f"[silver] Mapped columns  : {_cols_count}\n"
            yield f"[silver] Target table    : {ICEBERG_CATALOG}.{SILVER_DATABASE}.{target_table}\n"
            yield f"[silver] Data location   : s3://{SILVER_DATA_BUCKET}/{target_table}/\n"
            yield f"[silver] Meta location   : s3://{ICEBERG_METADATA_BUCKET}/{SILVER_DATABASE}/{target_table}/\n"

            # Preserve the previous snapshot until replacement commits.
            if _strategy == "overwrite":
                for _log_line in _overwrite_silver_cleanup(target_table):
                    yield _log_line

            # ── Step 5: run dbt ──────────────────────────────────
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
            dbt_dir      = os.path.join(project_root, "transformation")
            dbt_cmd      = [
                "dbt", "run",
                "--select",       target_table,
                "--profiles-dir", dbt_dir,
                "--project-dir",  dbt_dir,
                "--target",       "docker",
            ]
            if run_date:
                dbt_cmd += ["--vars", f'{{"snapshot_date": "{run_date}"}}']

            yield f"[silver] Running: {' '.join(dbt_cmd)}\n"
            yield "[silver] Submitting DBT job to Trino — streaming output below...\n"
            proc = _sp.Popen(
                dbt_cmd,
                cwd=dbt_dir,
                stdout=_sp.PIPE,
                stderr=_sp.STDOUT,
                text=True,
                bufsize=1,
            )
            for line in proc.stdout:
                yield f"[silver] {line}"
            proc.wait()

            if proc.returncode != 0:
                yield f"[silver] DBT run failed (exit {proc.returncode}).\n"
                yield "\n--- Silver Exit 1 ---\n"
                return

            yield "[silver] DBT run completed successfully.\n"

            # ── Step 6: verify row count via Trino ───────────────
            yield "[silver] Verifying Iceberg table — querying row count...\n"
            try:
                import trino as _trino
                _conn = _trino.dbapi.connect(
                    host    = os.environ.get("TRINO_HOST", "trino"),
                    port    = int(os.environ.get("TRINO_PORT", 8080)),
                    user    = "admin",
                    catalog = ICEBERG_CATALOG,
                    schema  = SILVER_DATABASE,
                )
                _cur = _conn.cursor()
                _cur.execute(
                    f"SELECT COUNT(*) FROM {ICEBERG_CATALOG}.{SILVER_DATABASE}.{target_table}"
                )
                _row = _cur.fetchone()
                _conn.close()
                _count = _row[0] if _row else 0
                yield f"[silver] Row count verified: {_count:,} rows in {SILVER_DATABASE}.{target_table}.\n"
            except Exception as _ve:
                yield f"[silver] Warning: row count query failed — {_ve}\n"

            yield f"[silver] Silver layer complete for '{target_table}'.\n"
            yield "\n--- Silver Exit 0 ---\n"

        except Exception as exc:
            yield f"\n[silver] ERROR: {exc}\n--- Silver Exit 1 ---\n"
        finally:
            _remove_active_job(key)

    return Response(background_job_stream(generate), mimetype="text/plain")


@app.route("/run-silver", methods=["POST"])
@login_required
def run_silver():
    silver_args = _build_silver_args(request.form)
    app_name    = request.form.get("application_name", "")
    run_date    = request.form.get("run_date", "")

    def generate():
        import docker as _docker
        silver_key = _register_active_job(app_name, run_date, "silver")
        container  = None
        try:
            client    = _docker.DockerClient(base_url="unix:///var/run/docker.sock")
            env       = _job_environment()
            container = client.containers.run(
                _PIPELINE_IMAGE,
                command=silver_args,
                environment=env,
                network=_COMPOSE_NETWORK,
                volumes=_job_volumes(),
                detach=True,
                remove=False,
            )
            _set_job_container(silver_key, container.id)
            for chunk in container.logs(stream=True, follow=True):
                yield chunk.decode("utf-8", errors="replace")
            result = container.wait()
            yield f"\n--- DBT Exit {result['StatusCode']} ---\n"
        except Exception as exc:
            yield f"\nERROR launching dbt container: {exc}\n"
        finally:
            _remove_active_job(silver_key)
            if container:
                try:
                    container.remove(force=True)
                except Exception:
                    pass

    return Response(background_job_stream(generate), mimetype="text/plain")


@app.route("/run-oracle", methods=["POST"])
@login_required
def run_oracle():
    return jsonify({"error": "Oracle publishing is retired. Use /run-gold with a local Iceberg Gold table."}), 410


@app.route("/run-gold", methods=["POST"])
@login_required
def run_gold():
    try:
        gold_args = _build_gold_args(request.form)
    except (ValueError, KeyError) as exc:
        return jsonify({"error": str(exc)}), 400
    app_name    = request.form.get("application_name", "")
    run_date    = request.form.get("run_date", "")

    def generate():
        import docker as _docker
        gold_key = _register_active_job(app_name, run_date, "gold")
        container  = None
        try:
            client    = _docker.DockerClient(base_url="unix:///var/run/docker.sock")
            env       = _job_environment("gold")
            container = client.containers.run(
                _PIPELINE_IMAGE,
                command=gold_args,
                environment=env,
                network=_COMPOSE_NETWORK,
                detach=True,
                remove=False,
            )
            _set_job_container(gold_key, container.id)
            for chunk in container.logs(stream=True, follow=True):
                yield chunk.decode("utf-8", errors="replace")
            result = container.wait()
            yield f"\n--- Gold Exit {result['StatusCode']} ---\n"
        except Exception as exc:
            yield f"\n[gold] ERROR launching local Gold container: {exc}\n--- Gold Exit 1 ---\n"
        finally:
            _remove_active_job(gold_key)
            if container:
                try:
                    container.remove(force=True)
                except Exception:
                    pass

    return Response(background_job_stream(generate), mimetype="text/plain")


@app.route("/run-transform-load", methods=["POST"])
@login_required
def run_transform_load():
    data            = request.get_json(force=True)
    silver_payload  = (data.get("silver_payload") or "").strip()
    app_name        = (data.get("app_name") or "transform_load").strip()
    run_date        = (data.get("run_date") or "").strip()
    silver_table    = (data.get("silver_table") or "").strip()
    gold_table      = (data.get("gold_table") or "").strip()
    load_strategy   = (data.get("load_strategy") or "replace").strip()
    date_column     = (data.get("date_column") or "").strip()

    if not silver_payload:
        return jsonify({"error": "silver_payload is required"}), 400
    if not silver_table:
        return jsonify({"error": "silver_table is required"}), 400
    if not gold_table:
        return jsonify({"error": "gold_table is required"}), 400

    effective_date = run_date or scheduled_run_date(os.environ.get("SCHEDULER_TIMEZONE", "Asia/Kolkata"))
    run_date = effective_date
    try:
        gold_args = _build_gold_args({
            "application_name": app_name, "silver_table": silver_table,
            "gold_table": gold_table, "load_strategy": load_strategy,
            "run_date": effective_date, "date_column": date_column,
        })
        payload_target = (json.loads(silver_payload).get("target_table") or app_name).strip()
        if payload_target != silver_table.split(".")[-1]:
            raise ValueError("silver_table must match the Silver payload target_table")
    except (ValueError, TypeError, AttributeError) as exc:
        return jsonify({"error": str(exc)}), 400

    def generate_with_cleanup():
        import subprocess as _sp
        import boto3 as _boto3
        from botocore.exceptions import ClientError as _CE
        import docker as _docker

        key           = _register_active_job(app_name, run_date, "transform_load")
        container_ref = [None]
        phase1_ok     = [False]
        try:
            # ── Phase 1: Silver via DBT ────────────────────────────────
            yield f"[silver] Ensuring MinIO bucket {SILVER_DATA_BUCKET} exists...\n"
            _s3 = _boto3.client(
                "s3",
                endpoint_url=MINIO_ENDPOINT,
                aws_access_key_id=MINIO_ACCESS_KEY,
                aws_secret_access_key=MINIO_SECRET_KEY,
                region_name=MINIO_REGION,
            )
            try:
                _s3.head_bucket(Bucket=SILVER_DATA_BUCKET)
            except _CE:
                _s3.create_bucket(Bucket=SILVER_DATA_BUCKET)
                yield "[silver] Bucket created.\n"

            yield "[silver] Writing DBT model file...\n"
            target_table = _write_silver_dbt_model(silver_payload, app_name)
            yield f"[silver] Model: transformation/models/silver/{target_table}.sql\n"

            import json as _json
            _strategy = (_json.loads(silver_payload).get("strategy") or "overwrite").strip()
            if _strategy == "overwrite":
                yield "[silver] Preparing atomic replacement; existing data remains available until commit.\n"
                for _log_line in _overwrite_silver_cleanup(target_table):
                    yield _log_line

            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
            dbt_dir      = os.path.join(project_root, "transformation")
            dbt_cmd      = [
                "dbt", "run",
                "--select",       target_table,
                "--profiles-dir", dbt_dir,
                "--project-dir",  dbt_dir,
                "--target",       "docker",
            ]
            if run_date:
                dbt_cmd += ["--vars", f'{{"snapshot_date": "{run_date}"}}']

            yield f"[silver] Running: {' '.join(dbt_cmd)}\n"
            proc = _sp.Popen(
                dbt_cmd, cwd=dbt_dir,
                stdout=_sp.PIPE, stderr=_sp.STDOUT,
                text=True, bufsize=1,
            )
            for line in proc.stdout:
                yield f"[silver] {line}"
            proc.wait()

            if proc.returncode != 0:
                yield f"[silver] DBT run failed (exit {proc.returncode}).\n"
                yield "\n--- Silver Exit 1 ---\n"
                _remove_active_job(key)
                return

            yield "[silver] DBT run completed successfully.\n"
            yield "\n--- Silver Exit 0 ---\n"
            phase1_ok[0] = True

        except Exception as exc:
            yield f"\n[silver] ERROR: {exc}\n--- Silver Exit 1 ---\n"

        # Phase 2: publish local Iceberg Gold only after Silver succeeds.
        if phase1_ok[0]:
            try:
                yield "[gold] Launching local Iceberg Gold container...\n"
                client = _docker.DockerClient(base_url="unix:///var/run/docker.sock")
                env    = _job_environment("gold")
                container_ref[0] = client.containers.run(
                    _PIPELINE_IMAGE,
                    command=gold_args,
                    environment=env,
                    network=_COMPOSE_NETWORK,
                    detach=True,
                    remove=False,
                )
                _set_job_container(key, container_ref[0].id)
                for chunk in container_ref[0].logs(stream=True, follow=True):
                    yield f"[gold] {chunk.decode('utf-8', errors='replace')}"
                result = container_ref[0].wait()
                yield f"\n--- Gold Exit {result['StatusCode']} ---\n"
            except Exception as exc:
                yield f"\n[gold] ERROR launching local Gold container: {exc}\n--- Gold Exit 1 ---\n"

        _remove_active_job(key)
        if container_ref[0]:
            try:
                container_ref[0].remove(force=True)
            except Exception:
                pass

    return Response(background_job_stream(generate_with_cleanup), mimetype="text/plain")


@app.route("/silver/compile-dbt", methods=["POST"])
@login_required
def silver_compile_dbt():
    """Write DBT model, run dbt compile, return compiled SQL + manifest graph."""
    import subprocess as _sp
    import json as _json

    data           = request.get_json(force=True)
    silver_payload = (data.get("silver_payload") or "").strip()
    run_date       = (data.get("run_date") or "").strip()

    if not silver_payload:
        return jsonify({"error": "silver_payload is required"}), 400

    try:
        app_name = "preview"
        target_table = _write_silver_dbt_model(silver_payload, app_name)
    except Exception as exc:
        return jsonify({"error": f"Failed to write model: {exc}"}), 500

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    dbt_dir      = os.path.join(project_root, "transformation")

    dbt_cmd = [
        "dbt", "compile",
        "--select",       target_table,
        "--profiles-dir", dbt_dir,
        "--project-dir",  dbt_dir,
        "--target",       "docker",
    ]
    if run_date:
        dbt_cmd += ["--vars", f'{{"snapshot_date": "{run_date}"}}']

    try:
        result = _sp.run(
            dbt_cmd, cwd=dbt_dir,
            stdout=_sp.PIPE, stderr=_sp.STDOUT,
            text=True, timeout=120,
        )
    except Exception as exc:
        return jsonify({"error": f"dbt compile failed: {exc}"}), 500

    if result.returncode != 0:
        return jsonify({"error": result.stdout[-3000:]}), 500

    # Read compiled SQL from target/compiled/
    compiled_sql = None
    compiled_path = os.path.join(
        dbt_dir, "target", "compiled", "de_transformation",
        "models", "silver", f"{target_table}.sql",
    )
    if os.path.isfile(compiled_path):
        with open(compiled_path, encoding="utf-8", errors="replace") as fh:
            compiled_sql = fh.read().strip()

    return jsonify({
        "target_table": target_table,
        "compiled_sql": compiled_sql,
        "dbt_output":   result.stdout[-3000:],
    })




@app.route("/lake-sql-run", methods=["POST"])
@login_required
def lake_sql_run():
    import urllib.request as _ur
    import urllib.error   as _ue
    body      = request.get_json(silent=True) or {}
    sql_query = (body.get("sql") or "").strip()
    if not sql_query:
        return jsonify({"error": "sql is required"}), 400

    def _stream():
        data = json.dumps({"sql": sql_query}).encode()
        req  = _ur.Request(
            f"{_SPARK_SQL_URL}/query",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with _ur.urlopen(req, timeout=300) as resp:
                while True:
                    chunk = resp.read(4096)
                    if not chunk:
                        break
                    yield chunk.decode("utf-8", errors="replace")
        except _ue.HTTPError as exc:
            yield f"ERROR (HTTP {exc.code}): {exc.read().decode(errors='replace')}\n"
        except Exception as exc:
            yield f"ERROR: {exc}\n"

    return Response(stream_with_context(_stream()), mimetype="text/plain")


_OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
_OPENAI_MODEL   = os.environ.get("OPENAI_MODEL", "gpt-4.1-nano")
_TRINO_URL      = os.environ.get("TRINO_URL", "http://trino:8080")


def _trino_schema_context() -> str:
    import urllib.request, json as _json
    headers = {"X-Trino-User": "lake-assistant", "Content-Type": "text/plain"}
    catalog = ICEBERG_CATALOG
    lines: list[str] = []
    try:
        def _q(sql: str) -> list:
            req = urllib.request.Request(
                f"{_TRINO_URL}/v1/statement",
                data=sql.encode(),
                headers={**headers, "X-Trino-Catalog": catalog, "X-Trino-Schema": "default"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=8) as r:
                page = _json.loads(r.read())
            rows = list(page.get("data") or [])
            nxt = page.get("nextUri")
            while nxt:
                with urllib.request.urlopen(urllib.request.Request(nxt, headers={"X-Trino-User": "lake-assistant"}), timeout=8) as r:
                    page = _json.loads(r.read())
                rows.extend(page.get("data") or [])
                nxt = page.get("nextUri")
            return rows

        for schema in [r[0] for r in _q(f"SHOW SCHEMAS IN {catalog}") if r[0] != "information_schema"]:
            for table in [r[0] for r in _q(f"SHOW TABLES IN {catalog}.{schema}")]:
                try:
                    cols = ", ".join(f"{c[0]} {c[1]}" for c in _q(f"DESCRIBE {catalog}.{schema}.{table}"))
                    lines.append(f"TABLE {catalog}.{schema}.{table} ({cols})")
                except Exception:
                    pass
    except Exception as exc:
        lines.append(f"-- schema fetch failed: {exc}")
    return "\n".join(lines) if lines else "-- no tables found"


_LAKE_SYSTEM = """You are a data lake assistant for a Trino/Iceberg lake backed by MinIO.

Schema context (current catalog snapshot):
{schema}

Rules:
1. Always respond with a Trino SQL query in ```sql ... ``` plus a one-line explanation — for every request, including:
   - Data queries (SELECT, aggregations, filters)
   - Catalog discovery (SHOW SCHEMAS IN {catalog}, SHOW TABLES IN {catalog}.<schema>)
   - Table inspection (DESCRIBE {catalog}.<schema>.<table>, SHOW COLUMNS FROM ...)
2. Use fully-qualified names (catalog.schema.table). Never invent columns or tables not in the schema above.
3. If the question is genuinely unanswerable with SQL (e.g. a greeting), reply in plain text with no code block.
"""


@app.route("/lake-ask", methods=["POST"])
@login_required
def lake_ask():
    import urllib.request, json as _json
    body = request.get_json(silent=True) or {}
    question  = (body.get("question") or "").strip()
    run_sql   = body.get("run_sql", False)
    sql_query = (body.get("sql") or "").strip()

    if not _OPENAI_API_KEY:
        return jsonify({"error": "OPENAI_API_KEY not configured"}), 503

    if run_sql and sql_query:
        def _exec():
            try:
                req = urllib.request.Request(
                    f"{_TRINO_URL}/v1/statement",
                    data=sql_query.encode(),
                    headers={"X-Trino-User": "lake-assistant", "Content-Type": "text/plain",
                             "X-Trino-Catalog": ICEBERG_CATALOG, "X-Trino-Schema": "default"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=15) as r:
                    page = _json.loads(r.read())
                columns = [c["name"] for c in (page.get("columns") or [])]
                rows = list(page.get("data") or [])
                nxt = page.get("nextUri")
                while nxt:
                    with urllib.request.urlopen(urllib.request.Request(nxt, headers={"X-Trino-User": "lake-assistant"}), timeout=15) as r:
                        page = _json.loads(r.read())
                    rows.extend(page.get("data") or [])
                    nxt = page.get("nextUri")
                yield _json.dumps({"type": "result", "columns": columns, "rows": rows[:500]}) + "\n"
            except Exception as exc:
                yield _json.dumps({"type": "error", "message": str(exc)}) + "\n"
        return Response(stream_with_context(_exec()), mimetype="application/x-ndjson")

    if not question:
        return jsonify({"error": "question is required"}), 400

    schema  = _trino_schema_context()
    payload = _json.dumps({
        "model":  _OPENAI_MODEL,
        "stream": True,
        "messages": [
            {"role": "system", "content": _LAKE_SYSTEM.format(schema=schema, catalog=ICEBERG_CATALOG)},
            {"role": "user",   "content": question},
        ],
    }).encode()

    def _stream():
        try:
            req = urllib.request.Request(
                "https://api.openai.com/v1/chat/completions",
                data=payload,
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {_OPENAI_API_KEY}"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=60) as resp:
                for raw in resp:
                    line = raw.decode("utf-8", errors="replace").strip()
                    if line.startswith("data: "):
                        chunk = line[6:]
                        if chunk == "[DONE]":
                            break
                        yield chunk + "\n"
        except Exception as exc:
            yield _json.dumps({"error": str(exc)}) + "\n"

    return Response(stream_with_context(_stream()), mimetype="application/x-ndjson")


def _cleanup_orphan_pipeline_containers() -> None:
    """Remove any stopped pipeline-run containers left over from crashed previous runs."""
    try:
        import docker as _docker
        client = _docker.DockerClient(base_url="unix:///var/run/docker.sock")
        stopped = client.containers.list(
            all=True,
            filters={"name": f"{_PROJECT}-pipeline-run", "status": "exited"},
        )
        for c in stopped:
            try:
                c.remove(force=True)
                print(f"[startup] removed orphan container {c.name}")
            except Exception:
                pass
    except Exception as exc:
        print(f"[startup] container cleanup skipped: {exc}")


if __name__ == "__main__":
    from ui.db import init_schema, get_conn
    from psycopg2.extras import RealDictCursor
    init_schema()
    _cleanup_orphan_pipeline_containers()

    # Run schema migrations then load active scheduled jobs into APScheduler
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(_SILVER_SCHED_MIGRATION)
            conn.commit()
            cur.execute("SELECT id, cron_expression FROM public.scheduled_jobs WHERE is_active = TRUE")
            for row in cur.fetchall():
                try:
                    _schedule_job(row["id"], row["cron_expression"])
                except (TypeError, ValueError) as exc:
                    print(f"[scheduler] Skipping invalid job {row['id']}: {exc}")
    except Exception as exc:
        print(f"[scheduler] Failed to load jobs on startup: {exc}")

    _scheduler.start()
    close_connection()

    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(debug=debug, host="0.0.0.0", port=5000, threaded=True)
