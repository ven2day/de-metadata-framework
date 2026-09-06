import io
import os
import sys
from flask import Flask, render_template, request, Response, stream_with_context, send_file, jsonify
from flask_login import login_required, current_user
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

app = Flask(__name__)
app.secret_key = os.environ["FLASK_SECRET_KEY"]

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

from ingestion.env.DE_Ingestion_properties import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, LOG_S3_BUCKET, VAULT_ADDR,
    SOURCE_S3_ENDPOINT, SOURCE_S3_ACCESS_KEY, SOURCE_S3_SECRET_KEY, METADATA_S3_BUCKET,
)

# ── Extensions ─────────────────────────────────────────────────────────────────
from ui.extensions import csrf, login_manager, limiter

csrf.init_app(app)
login_manager.init_app(app)
limiter.init_app(app)

@login_manager.user_loader
def load_user(user_id: str):
    from ui.models import User
    return User.get_by_id(int(user_id))

# ── Auth blueprint ─────────────────────────────────────────────────────────────
from ui.auth import auth_bp
app.register_blueprint(auth_bp)

# ── Docker resource names ──────────────────────────────────────────────────────
_PROJECT          = os.environ.get("COMPOSE_PROJECT_NAME", "de-metadata-framework")
_PIPELINE_IMAGE   = f"{_PROJECT}-pipeline"
_COMPOSE_NETWORK  = f"{_PROJECT}_de-net"
_VAULT_VOL        = f"{_PROJECT}_vault_secrets"

_SKIP_ENV = {"FLASK_DEBUG", "FLASK_ENV", "WERKZEUG_RUN_MAIN", "HOSTNAME"}

# ── Background job scheduler ───────────────────────────────────────────────────
_scheduler = BackgroundScheduler(timezone="UTC", daemon=True)


def _trigger_scheduled_job(job_id: int) -> None:
    import datetime
    import docker as _docker
    from ui.db import get_conn
    run_date = datetime.date.today().isoformat()
    conn   = None
    status = "error"
    logs   = ""
    run_id = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM public.scheduled_jobs WHERE id = %s AND is_active = TRUE",
                (job_id,),
            )
            job = cur.fetchone()
        if not job:
            return

        # Insert run record so we can track it even if container crashes
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO public.scheduled_job_runs (job_id, run_date) VALUES (%s, %s) RETURNING id",
                (job_id, run_date),
            )
            run_id = cur.fetchone()[0]
        conn.commit()

        form_data = {
            "application_name":  job["application_name"],
            "ingest_date":       run_date,
            "source_type":       job["source_type"] or "s3",
            "source_bucket":     job["source_bucket"] or "",
            "source_key":        job["source_key"] or "",
            "source_database":   job["source_database"] or "",
            "source_table_name": job["source_table_name"] or "",
            "metadata_key":      job["metadata_key"] or "",
            "write_mode":        job["write_mode"] or "overwrite",
            "log_folder":        "lake",
            "log_level":         job["log_level"] or "INFO",
            "output_catalog":    "minio",
            "output_database":   "de_lake",
        }
        pipeline_args = _build_pipeline_args(form_data)
        client = _docker.DockerClient(base_url="unix:///var/run/docker.sock")
        env = {k: v for k, v in os.environ.items() if k not in _SKIP_ENV}
        container = client.containers.run(
            _PIPELINE_IMAGE,
            command=pipeline_args,
            environment=env,
            network=_COMPOSE_NETWORK,
            volumes={_VAULT_VOL: {"bind": "/vault/secrets", "mode": "ro"}},
            detach=True,
            remove=False,
        )
        result = container.wait()
        status = "success" if result["StatusCode"] == 0 else "failed"
        try:
            logs = container.logs(stdout=True, stderr=True).decode("utf-8", errors="replace")
        except Exception:
            pass
        try:
            container.remove(force=True)
        except Exception:
            pass
    except Exception as exc:
        status = "error"
        logs   = str(exc)
        print(f"[scheduler] job {job_id} error: {exc}")
    finally:
        if conn and run_id:
            try:
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


def _schedule_job(job_id: int, cron_expr: str) -> None:
    parts = cron_expr.strip().split()
    if len(parts) != 5:
        return
    _scheduler.add_job(
        _trigger_scheduled_job,
        CronTrigger(minute=parts[0], hour=parts[1], day=parts[2], month=parts[3], day_of_week=parts[4], timezone="UTC"),
        args=[job_id],
        id=f"sjob_{job_id}",
        replace_existing=True,
    )


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
    args = [
        "--application-name", form["application_name"],
        "--source-type",      source_type,
        "--ingest-date",      form["ingest_date"],
        "--metadata-key",     form["metadata_key"],
        "--output-database",  form["output_database"],
        "--output-catalog",   form.get("output_catalog", "minio"),
        "--write-mode",       form.get("write_mode", "overwrite"),
        "--log-folder",       form.get("log_folder", "lake"),
        "--log-level",        form.get("log_level", "INFO"),
    ]
    args += ["--output-table-name", form["application_name"]]
    if source_type == "s3":
        args += ["--source-bucket", form["source_bucket"],
                 "--source-key",    form["source_key"]]
    else:
        args += ["--source-database",   form["source_database"],
                 "--source-table-name", form["source_table_name"]]
    return args


@app.route("/")
@login_required
def index():
    return render_template("index.html")


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
        )
        client.list_buckets()
        results["minio"] = "ok"
    except (BotoCoreError, ClientError, Exception) as exc:
        results["minio"] = f"error: {exc}"

    try:
        r = _req.get(f"{VAULT_ADDR}/v1/sys/health", timeout=3)
        results["vault"] = "ok" if r.status_code in (200, 429, 503) else f"error: HTTP {r.status_code}"
    except Exception as exc:
        results["vault"] = f"error: {exc}"

    return jsonify(results)


@app.route("/run", methods=["POST"])
@login_required
def run():
    pipeline_args = _build_pipeline_args(request.form)

    def generate():
        import docker as _docker
        container = None
        try:
            client = _docker.DockerClient(base_url="unix:///var/run/docker.sock")
            env = {k: v for k, v in os.environ.items() if k not in _SKIP_ENV}

            container = client.containers.run(
                _PIPELINE_IMAGE,
                command=pipeline_args,
                environment=env,
                network=_COMPOSE_NETWORK,
                volumes={_VAULT_VOL: {"bind": "/vault/secrets", "mode": "ro"}},
                detach=True,
                remove=False,
            )

            for chunk in container.logs(stream=True, follow=True):
                yield chunk.decode("utf-8", errors="replace")

            result = container.wait()
            yield f"\n--- Exit {result['StatusCode']} ---\n"

        except Exception as exc:
            yield f"\nERROR launching pipeline container: {exc}\n"
        finally:
            if container:
                try:
                    container.remove(force=True)
                except Exception:
                    pass

    return Response(stream_with_context(generate()), mimetype="text/plain")


@app.route("/run-bronze", methods=["POST"])
@login_required
def run_bronze():
    bronze_args = _build_bronze_args(request.form)

    def generate():
        import docker as _docker
        container = None
        try:
            client = _docker.DockerClient(base_url="unix:///var/run/docker.sock")
            env = {k: v for k, v in os.environ.items() if k not in _SKIP_ENV}

            container = client.containers.run(
                _PIPELINE_IMAGE,
                command=bronze_args,
                environment=env,
                network=_COMPOSE_NETWORK,
                volumes={_VAULT_VOL: {"bind": "/vault/secrets", "mode": "ro"}},
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

    return Response(stream_with_context(generate()), mimetype="text/plain")


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
    )
    try:
        resp = client.list_objects_v2(Bucket=meta_bucket, MaxKeys=500)
        keys = [obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].lower().endswith(".csv")]
    except Exception:
        return jsonify([])
    return jsonify(keys)


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


@app.route("/scheduled-jobs", methods=["POST"])
@login_required
def create_scheduled_job():
    from ui.db import get_conn
    from psycopg2.extras import RealDictCursor
    data = request.get_json(force=True)
    app_name = data["application_name"]
    conn = get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Ensure uniqueness constraint exists (idempotent — safe to run every time)
        cur.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'scheduled_jobs_application_name_key'
                ) THEN
                    ALTER TABLE public.scheduled_jobs
                        ADD CONSTRAINT scheduled_jobs_application_name_key
                        UNIQUE (application_name);
                END IF;
            END $$;
        """)
        # Upsert: update existing active job or insert new one
        cur.execute(
            """INSERT INTO public.scheduled_jobs
               (application_name, cron_expression, source_type, source_bucket, source_key,
                source_database, source_table_name, metadata_key, write_mode, log_level, is_active)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, TRUE)
               ON CONFLICT (application_name) DO UPDATE SET
                   cron_expression   = EXCLUDED.cron_expression,
                   source_type       = EXCLUDED.source_type,
                   source_bucket     = EXCLUDED.source_bucket,
                   source_key        = EXCLUDED.source_key,
                   source_database   = EXCLUDED.source_database,
                   source_table_name = EXCLUDED.source_table_name,
                   metadata_key      = EXCLUDED.metadata_key,
                   write_mode        = EXCLUDED.write_mode,
                   log_level         = EXCLUDED.log_level,
                   is_active         = TRUE
               RETURNING id""",
            (
                app_name, data["cron_expression"],
                data.get("source_type", "s3"),
                data.get("source_bucket") or None, data.get("source_key") or None,
                data.get("source_database") or None, data.get("source_table_name") or None,
                data.get("metadata_key") or None,
                data.get("write_mode", "overwrite"), data.get("log_level", "INFO"),
            ),
        )
        job_id = cur.fetchone()["id"]
    conn.commit()
    # _schedule_job uses replace_existing=True, so re-registering same id is safe
    _schedule_job(job_id, data["cron_expression"])
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


if __name__ == "__main__":
    from ui.db import init_schema, get_conn
    from psycopg2.extras import RealDictCursor
    init_schema()

    # Load existing active scheduled jobs into APScheduler
    try:
        conn = get_conn()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, cron_expression FROM public.scheduled_jobs WHERE is_active = TRUE")
            for row in cur.fetchall():
                _schedule_job(row["id"], row["cron_expression"])
    except Exception as exc:
        print(f"[scheduler] Failed to load jobs on startup: {exc}")

    _scheduler.start()

    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(debug=debug, host="0.0.0.0", port=5000, threaded=True)
