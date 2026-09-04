import io
import os
import sys
from flask import Flask, render_template, request, Response, stream_with_context, send_file, jsonify

app = Flask(__name__)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

sys.path.insert(0, PROJECT_ROOT)
from ingestion.env.DE_Ingestion_properties import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, LOG_S3_BUCKET, VAULT_ADDR,
)

# Derive Docker resource names from the compose project name.
# docker compose names images/networks/volumes as {project}-{service} / {project}_{resource}.
_PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "de-metadata-framework")
_PIPELINE_IMAGE   = f"{_PROJECT}-pipeline"
_COMPOSE_NETWORK  = f"{_PROJECT}_de-net"
_VAULT_VOL        = f"{_PROJECT}_vault_secrets"

# Env vars forwarded from the UI container into the pipeline container.
# MINIO_ENDPOINT / SOURCE_S3_ENDPOINT stay as Docker-internal addresses.
_SKIP_ENV = {"FLASK_DEBUG", "FLASK_ENV", "WERKZEUG_RUN_MAIN", "HOSTNAME"}


def _build_pipeline_args(form) -> list[str]:
    """Return the pipeline.py CLI arguments (entrypoint.sh prepends spark-submit)."""
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
    if form.get("output_table_name"):
        args += ["--output-table-name", form["output_table_name"]]
    if source_type == "s3":
        args += ["--source-bucket", form["source_bucket"],
                 "--source-key",    form["source_key"]]
    else:
        args += ["--source-database",   form["source_database"],
                 "--source-table-name", form["source_table_name"]]
    return args


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/connectivity")
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
        # 200=unsealed, 503=sealed (still reachable), 429=standby
        results["vault"] = "ok" if r.status_code in (200, 429, 503) else f"error: HTTP {r.status_code}"
    except Exception as exc:
        results["vault"] = f"error: {exc}"

    return jsonify(results)


@app.route("/run", methods=["POST"])
def run():
    pipeline_args = _build_pipeline_args(request.form)

    def generate():
        import docker as _docker
        container = None
        try:
            client = _docker.DockerClient(base_url="unix:///var/run/docker.sock")

            # Forward all env vars from this container into the pipeline container,
            # excluding Flask-specific noise. MINIO/SOURCE endpoints keep their
            # Docker-internal values set by docker-compose.
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


def _find_log_key(client, app_name: str, ingest_date: str, log_folder: str) -> str:
    prefix = f"{log_folder}/{app_name}/{ingest_date}/{app_name}_{ingest_date}"
    resp = client.list_objects_v2(Bucket=LOG_S3_BUCKET, Prefix=prefix)
    objects = resp.get("Contents", [])
    if not objects:
        raise FileNotFoundError(f"No log found for prefix {prefix}")
    return sorted(objects, key=lambda o: o["LastModified"], reverse=True)[0]["Key"]


@app.route("/download-log")
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


if __name__ == "__main__":
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(debug=debug, host="0.0.0.0", port=5000, threaded=True)
