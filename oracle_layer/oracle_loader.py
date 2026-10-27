#!/usr/bin/env python3
"""
Silver → Gold loader: reads a Silver Iceberg table via Trino,
writes to Oracle Autonomous Data Warehouse using one of three strategies:
  append            — insert all rows from Silver
  truncate          — replace all rows in one transaction (using DELETE)
  delete_and_insert — DELETE WHERE date_col = run_date, then insert that partition
"""
import argparse
import base64
from datetime import date, time, timedelta
import io
import logging
import os
import re
import sys
import tempfile
import zipfile
from uuid import UUID

import requests
import oracledb
import trino.dbapi

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [oracle-loader] %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

VAULT_ADDR  = os.environ.get("VAULT_ADDR", "http://vault:8200")
TRINO_HOST  = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT  = int(os.environ.get("TRINO_PORT", "8080"))
BATCH_SIZE  = int(os.environ.get("ORACLE_BATCH_SIZE", "5000"))
SILVER_CATALOG = os.environ.get("ICEBERG_CATALOG", "minio")
SILVER_SCHEMA = os.environ.get("SILVER_DATABASE", "de_silver")
ORACLE_SCHEMA = os.environ.get("ORACLE_SCHEMA", "dw_enigma")
_IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,127}$")


def _identifier(value: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"Invalid SQL identifier: {value!r}")
    return value


def _oracle_name(value: str) -> str:
    parts = value.split(".")
    if len(parts) not in {1, 2}:
        raise ValueError("Oracle table must be TABLE or SCHEMA.TABLE")
    return ".".join(_identifier(part) for part in parts)


def _column_names(columns: list[str]) -> list[str]:
    names = [_identifier(column).upper() for column in columns]
    if not names or len(set(names)) != len(names):
        raise ValueError("Column names must be nonempty and unique ignoring case")
    return [f'"{name}"' for name in names]


def _run_date(value: str | None) -> date:
    try:
        parsed = date.fromisoformat(value or "")
    except (TypeError, ValueError) as exc:
        raise ValueError("run_date must use YYYY-MM-DD") from exc
    if parsed.isoformat() != value:
        raise ValueError("run_date must use YYYY-MM-DD")
    return parsed


# ── Vault helpers ─────────────────────────────────────────────────────────────

def _vault_token() -> str:
    token_file = "/vault/secrets/pipeline_token"
    if os.path.exists(token_file):
        return open(token_file).read().strip()
    return os.environ.get("VAULT_TOKEN", "")


def _read_oracle_creds() -> dict:
    token = _vault_token()
    resp = requests.get(
        f"{VAULT_ADDR}/v1/secret/data/oracle/adw",
        headers={"X-Vault-Token": token},
        timeout=5,
    )
    resp.raise_for_status()
    return resp.json()["data"]["data"]


# ── Trino reader ──────────────────────────────────────────────────────────────

def _trino_type_to_oracle(trino_type: str) -> str:
    t = trino_type.lower().strip()
    if t.startswith("varchar"):
        m = re.match(r"varchar\((\d+)\)", t)
        n = int(m.group(1)) if m else None
        return f"VARCHAR2({n} CHAR)" if n is not None and 0 < n <= 4000 else "CLOB"
    if t.startswith("char"):
        m = re.match(r"char\((\d+)\)", t)
        n = int(m.group(1)) if m else 255
        return f"CHAR({n} CHAR)" if 0 < n <= 2000 else "CLOB"
    if t in ("bigint", "integer", "int", "smallint", "tinyint"):
        return "NUMBER(19)"
    if t.startswith("decimal") or t.startswith("numeric"):
        m = re.match(r"(?:decimal|numeric)\((\d+),\s*(\d+)\)", t)
        if not m:
            raise ValueError(f"Unsupported decimal type: {trino_type}")
        precision, scale = map(int, m.groups())
        if not 1 <= precision <= 38 or not 0 <= scale <= precision:
            raise ValueError(f"Unsupported decimal type: {trino_type}")
        return f"NUMBER({precision},{scale})"
    if t in ("double", "real", "float"):
        return "BINARY_DOUBLE"
    if t == "boolean":
        return "NUMBER(1)"
    if t == "date":
        return "DATE"
    if t.startswith("timestamp"):
        match = re.fullmatch(r"timestamp(?:\(([0-9])\))?( with time zone)?", t)
        if not match:
            raise ValueError(f"Unsupported timestamp type: {trino_type}")
        precision = match.group(1) or "6"
        return f"TIMESTAMP({precision})" + (" WITH TIME ZONE" if match.group(2) else "")
    if t.startswith("time(") or t in {"time", "time with time zone"}:
        return "VARCHAR2(64)"
    if t == "uuid":
        return "VARCHAR2(36)"
    if t == "varbinary":
        return "BLOB"
    if t == "json":
        return "CLOB"
    raise ValueError(
        f"Unsupported Silver type {trino_type!r}; cast complex values to JSON/VARCHAR in the Silver model"
    )


def _ensure_table_exists(oracle_conn: oracledb.Connection, oracle_table: str,
                         columns: list[str], col_types: list[str]) -> None:
    oracle_table = _oracle_name(oracle_table)
    quoted_columns = _column_names(columns)
    if len(columns) != len(col_types):
        raise ValueError("Silver columns and type definitions have different lengths")
    col_ddl = ",\n  ".join(
        f"{col} {_trino_type_to_oracle(typ)}"
        for col, typ in zip(quoted_columns, col_types)
    )
    # ORA-00955 = name already used by an existing object → safe to ignore
    plsql = f"""
BEGIN
  EXECUTE IMMEDIATE 'CREATE TABLE {oracle_table} (
  {col_ddl}
)';
EXCEPTION
  WHEN OTHERS THEN
    IF SQLCODE = -955 THEN NULL;
    ELSE RAISE;
    END IF;
END;"""
    cur = oracle_conn.cursor()
    try:
        cur.execute(plsql)
        oracle_conn.commit()
    finally:
        cur.close()
    log.info("Table %s ensured.", oracle_table)


def _read_silver(silver_schema: str, silver_table: str, date_column: str | None,
                 run_date: str | None, strategy: str,
                 silver_catalog: str | None = None) -> tuple[list[str], list[list], list[str]]:
    catalog = _identifier(silver_catalog or SILVER_CATALOG)
    schema = _identifier(silver_schema)
    table = _identifier(silver_table)
    if strategy == "delete_and_insert" and not date_column:
        raise ValueError("date_column is required for delete_and_insert")
    if date_column:
        _identifier(date_column)
    selected_date = _run_date(run_date) if strategy == "delete_and_insert" else None
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="oracle-loader",
        catalog=catalog,
        schema=schema,
    )
    cur = conn.cursor()
    fqn = f'"{catalog}"."{schema}"."{table}"'

    if strategy == "delete_and_insert":
        sql = f'SELECT * FROM {fqn} WHERE CAST("{date_column}" AS DATE) = ?'
    else:
        sql = f"SELECT * FROM {fqn}"

    log.info("Reading Silver: %s", sql)
    try:
        if selected_date is None:
            cur.execute(sql)
        else:
            cur.execute(sql, [selected_date])
        columns   = [d[0] for d in cur.description]
        col_types = [d[1] for d in cur.description]
        rows      = cur.fetchall()
    finally:
        cur.close()
        conn.close()
    log.info("Fetched %d rows, %d columns", len(rows), len(columns))
    return columns, rows, col_types


# ── Oracle writer ─────────────────────────────────────────────────────────────

def _extract_wallet(creds: dict) -> str | None:
    """Base64-decode the wallet zip from Vault and extract to a temp directory."""
    zip_b64 = creds.get("oracle_wallet_zip_b64", "")
    if not zip_b64:
        return None
    wallet_dir = tempfile.mkdtemp(prefix="oracle_wallet_")
    with zipfile.ZipFile(io.BytesIO(base64.b64decode(zip_b64))) as zf:
        zf.extractall(wallet_dir)
    log.info("Wallet extracted to %s", wallet_dir)
    return wallet_dir


def _oracle_connect(creds: dict, wallet_dir: str | None) -> oracledb.Connection:
    kwargs = dict(
        user=creds["oracle_user"],
        password=creds["oracle_password"],
        dsn=creds["oracle_dsn"],
    )
    if wallet_dir:
        kwargs["config_dir"]       = wallet_dir
        kwargs["wallet_location"]  = wallet_dir
        kwargs["wallet_password"]  = creds.get("oracle_wallet_password", "")
    return oracledb.connect(**kwargs)


def _insert_batch(cursor, oracle_table: str, columns: list[str], batch: list) -> None:
    oracle_table = _oracle_name(oracle_table)
    col_list   = ", ".join(_column_names(columns))
    bind_list  = ", ".join(f":{i+1}" for i in range(len(columns)))
    sql        = f"INSERT INTO {oracle_table} ({col_list}) VALUES ({bind_list})"
    # Oracle SQL BOOLEAN is unavailable in older ADW versions; preserve it as 0/1.
    converted = [
        [int(value) if isinstance(value, bool)
         else value.isoformat() if isinstance(value, time)
         else str(value) if isinstance(value, UUID) else value
         for value in row]
        for row in batch
    ]
    cursor.executemany(sql, converted)


def _load(oracle_conn: oracledb.Connection, oracle_table: str, strategy: str,
          date_column: str | None, run_date: str | None,
          columns: list[str], rows: list, col_types: list[str] | None = None) -> None:
    oracle_table = _oracle_name(oracle_table)
    _column_names(columns)
    if BATCH_SIZE <= 0:
        raise ValueError("ORACLE_BATCH_SIZE must be positive")
    if strategy not in {"append", "truncate", "delete_and_insert"}:
        raise ValueError(f"Unsupported load strategy: {strategy!r}")
    if strategy == "delete_and_insert":
        if not date_column:
            raise ValueError("date_column is required for delete_and_insert")
        _identifier(date_column)
        selected_date = _run_date(run_date)
    oracle_conn.autocommit = False
    cur = oracle_conn.cursor()
    try:
        if col_types:
            # Explicit bindings retain timezone offsets and permit unbounded text.
            sizes = []
            for typ in col_types:
                mapped = _trino_type_to_oracle(typ)
                if mapped == "CLOB":
                    sizes.append(oracledb.DB_TYPE_CLOB)
                elif mapped == "BLOB":
                    sizes.append(oracledb.DB_TYPE_BLOB)
                elif mapped.endswith("WITH TIME ZONE"):
                    sizes.append(oracledb.DB_TYPE_TIMESTAMP_TZ)
                else:
                    sizes.append(None)
            cur.setinputsizes(*sizes)

        if strategy == "truncate":
            # TRUNCATE is DDL and commits immediately; DELETE keeps replacement atomic.
            log.info("Replacing all rows in %s in one transaction", oracle_table)
            cur.execute(f"DELETE FROM {oracle_table}")
        elif strategy == "delete_and_insert":
            column = f'"{date_column.upper()}"'
            cur.execute(
                f"DELETE FROM {oracle_table} WHERE {column} >= :1 AND {column} < :2",
                [selected_date, selected_date + timedelta(days=1)],
            )

        total = len(rows)
        for start in range(0, total, BATCH_SIZE):
            batch = rows[start : start + BATCH_SIZE]
            _insert_batch(cur, oracle_table, columns, batch)
            log.info("Prepared rows %d–%d of %d", start + 1, min(start + BATCH_SIZE, total), total)
        oracle_conn.commit()
    except BaseException:
        oracle_conn.rollback()
        raise
    finally:
        cur.close()

    log.info("Load complete: %d rows → %s [%s]", total, oracle_table, strategy)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Silver → Oracle ADW Gold loader")
    parser.add_argument("--application-name", required=True)
    parser.add_argument("--silver-catalog",   default=SILVER_CATALOG)
    parser.add_argument("--silver-schema",    default=SILVER_SCHEMA)
    parser.add_argument("--silver-table",     required=True, help="Table name inside de_silver")
    parser.add_argument("--oracle-table",     required=True, help="Fully-qualified Oracle target table")
    parser.add_argument("--load-strategy",    required=True, choices=["append", "truncate", "delete_and_insert"])
    parser.add_argument("--date-column",      default=None,  help="Required for delete_and_insert")
    parser.add_argument("--run-date",         required=True, help="ISO date, e.g. 2026-09-08")
    args = parser.parse_args()
    _run_date(args.run_date)
    _identifier(args.silver_catalog)
    _identifier(args.silver_schema)

    # Strip any accidental catalog/schema prefix so the user can pass either
    # "my_table" or "de_silver.my_table" or "minio.de_silver.my_table".
    silver_table = args.silver_table.split(".")[-1]

    # Keep the default compatible with existing jobs; ORACLE_SCHEMA can change it.
    oracle_table = args.oracle_table if "." in args.oracle_table or not ORACLE_SCHEMA else f"{ORACLE_SCHEMA}.{args.oracle_table}"
    _oracle_name(oracle_table)

    log.info(
        "Starting Oracle load | app=%s | table=%s → %s | strategy=%s | date=%s",
        args.application_name, silver_table, oracle_table,
        args.load_strategy, args.run_date,
    )

    creds      = _read_oracle_creds()
    wallet_dir = _extract_wallet(creds)

    columns, rows, col_types = _read_silver(
        silver_schema=args.silver_schema,
        silver_table=silver_table,
        date_column=args.date_column,
        run_date=args.run_date,
        strategy=args.load_strategy,
        silver_catalog=args.silver_catalog,
    )

    if not rows and args.load_strategy == "append":
        log.warning("No rows returned from Silver — nothing to load.")
        sys.exit(0)

    oracle_conn = _oracle_connect(creds, wallet_dir)
    try:
        _ensure_table_exists(oracle_conn, oracle_table, columns, col_types)
        _load(
            oracle_conn=oracle_conn,
            oracle_table=oracle_table,
            strategy=args.load_strategy,
            date_column=args.date_column,
            run_date=args.run_date,
            columns=columns,
            rows=rows,
            col_types=col_types,
        )
    finally:
        oracle_conn.close()

    log.info("Oracle load finished successfully.")


if __name__ == "__main__":
    main()
