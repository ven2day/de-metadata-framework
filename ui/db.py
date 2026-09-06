import re
import os
import sys

import psycopg2
from psycopg2.extras import RealDictCursor

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)

from ingestion.env.DE_Ingestion_properties import (
    SUPABASE_JDBC_URL, SUPABASE_DB_USER, SUPABASE_DB_PASSWORD,
)

_conn: "psycopg2.extensions.connection | None" = None
_JDBC_RE = re.compile(r"jdbc:postgresql://([^:/]+):(\d+)/(.+)")

# AUTH_DATABASE_URL overrides SUPABASE_JDBC_URL for the auth DB connection.
# Use the Supabase IPv4-capable pooler URL to avoid IPv6 routing issues in Docker.
# Format: postgresql://postgres.[ref]:[password]@aws-0-[region].pooler.supabase.com:6543/postgres
# Strip jdbc: prefix if user copied the JDBC URL format instead of the libpq format.
_AUTH_DB_URL = os.environ.get("AUTH_DATABASE_URL", "").strip().removeprefix("jdbc:")


def _pg_dsn() -> "dict | str":
    """Return either a DSN dict (JDBC fallback) or a libpq connection string (AUTH_DATABASE_URL)."""
    if _AUTH_DB_URL:
        return _AUTH_DB_URL

    m = _JDBC_RE.match(SUPABASE_JDBC_URL)
    if not m:
        raise ValueError(f"Cannot parse SUPABASE_JDBC_URL: {SUPABASE_JDBC_URL}")
    from ingestion.pyfiles.vault_client import get_encrypt_value
    password = get_encrypt_value(
        SUPABASE_DB_PASSWORD,
        key_name="supabase-pwd",
        key_type="encryption-key",
        mount_path="transit",
    )
    return {
        "host":     m.group(1),
        "port":     int(m.group(2)),
        "dbname":   m.group(3),
        "user":     SUPABASE_DB_USER,
        "password": password,
        "sslmode":  "require",
    }


def get_conn() -> "psycopg2.extensions.connection":
    """Return a live psycopg2 connection, reconnecting if closed or broken."""
    global _conn
    try:
        if _conn and not _conn.closed:
            _conn.cursor().execute("SELECT 1")
            return _conn
    except Exception:
        pass
    dsn = _pg_dsn()
    _conn = psycopg2.connect(dsn) if isinstance(dsn, str) else psycopg2.connect(**dsn)
    _conn.autocommit = False
    return _conn


def cursor() -> RealDictCursor:
    return get_conn().cursor(cursor_factory=RealDictCursor)


_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS public.app_users (
    id              SERIAL       PRIMARY KEY,
    username        VARCHAR(50)  UNIQUE NOT NULL,
    email           VARCHAR(255) UNIQUE NOT NULL,
    password_hash   TEXT         NOT NULL,
    role            VARCHAR(20)  NOT NULL DEFAULT 'user'
                    CHECK (role IN ('root', 'user')),
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_by      INTEGER      REFERENCES public.app_users(id),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_login      TIMESTAMPTZ,
    failed_attempts INTEGER      NOT NULL DEFAULT 0,
    locked_until    TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS public.scheduled_jobs (
    id                SERIAL        PRIMARY KEY,
    application_name  VARCHAR(255)  NOT NULL,
    cron_expression   VARCHAR(100)  NOT NULL,
    source_type       VARCHAR(20)   NOT NULL DEFAULT 's3',
    source_bucket     VARCHAR(500),
    source_key        VARCHAR(500),
    source_database   VARCHAR(255),
    source_table_name VARCHAR(255),
    metadata_key      VARCHAR(500),
    write_mode        VARCHAR(20)   NOT NULL DEFAULT 'overwrite',
    log_level         VARCHAR(10)   NOT NULL DEFAULT 'INFO',
    is_active         BOOLEAN       NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    last_run_at       TIMESTAMPTZ,
    last_run_status   VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS public.scheduled_job_runs (
    id          SERIAL      PRIMARY KEY,
    job_id      INTEGER     NOT NULL REFERENCES public.scheduled_jobs(id),
    run_date    DATE        NOT NULL,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status      VARCHAR(20),
    logs        TEXT
);
"""


def init_schema():
    """Create app_users, scheduled_jobs, and scheduled_job_runs tables if they do not exist."""
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(_SCHEMA_SQL)
    conn.commit()
