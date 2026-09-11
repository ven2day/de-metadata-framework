#!/usr/bin/env python3
"""
Silver → Gold loader: reads a Silver Iceberg table via Trino,
writes to Oracle Autonomous Data Warehouse using one of three strategies:
  append            — insert all rows from Silver
  truncate          — truncate target table, then insert all rows
  delete_and_insert — DELETE WHERE date_col = run_date, then insert that partition
"""
import argparse
import base64
import io
import logging
import os
import re
import sys
import tempfile
import zipfile

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
        n = int(m.group(1)) if m else 4000
        return f"VARCHAR2({min(n, 4000)})"
    if t.startswith("char"):
        m = re.match(r"char\((\d+)\)", t)
        n = int(m.group(1)) if m else 255
        return f"CHAR({n})"
    if t in ("bigint", "integer", "int", "smallint", "tinyint"):
        return "NUMBER(19)"
    if t.startswith("decimal") or t.startswith("numeric"):
        m = re.match(r"(?:decimal|numeric)\((\d+),\s*(\d+)\)", t)
        return f"NUMBER({m.group(1)},{m.group(2)})" if m else "NUMBER"
    if t in ("double", "real", "float"):
        return "BINARY_DOUBLE"
    if t == "boolean":
        return "NUMBER(1)"
    if t == "date":
        return "DATE"
    if t.startswith("timestamp"):
        return "TIMESTAMP"
    return "VARCHAR2(4000)"


def _ensure_table_exists(oracle_conn: oracledb.Connection, oracle_table: str,
                         columns: list[str], col_types: list[str]) -> None:
    col_ddl = ",\n  ".join(
        f"{col.upper()} {_trino_type_to_oracle(typ)}"
        for col, typ in zip(columns, col_types)
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
    cur.execute(plsql)
    oracle_conn.commit()
    log.info("Table %s ensured.", oracle_table)


def _read_silver(silver_schema: str, silver_table: str, date_column: str | None,
                 run_date: str | None, strategy: str) -> tuple[list[str], list[list], list[str]]:
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="oracle-loader",
        catalog="minio",
        schema=silver_schema,
    )
    cur = conn.cursor()
    fqn = f"minio.{silver_schema}.{silver_table}"

    if strategy == "delete_and_insert" and date_column and run_date:
        sql = f"SELECT * FROM {fqn} WHERE {date_column} = DATE '{run_date}'"
    else:
        sql = f"SELECT * FROM {fqn}"

    log.info("Reading Silver: %s", sql)
    cur.execute(sql)
    columns   = [d[0] for d in cur.description]
    col_types = [d[1] for d in cur.description]
    rows      = cur.fetchall()
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
    col_list   = ", ".join(columns)
    bind_list  = ", ".join(f":{i+1}" for i in range(len(columns)))
    sql        = f"INSERT INTO {oracle_table} ({col_list}) VALUES ({bind_list})"
    cursor.executemany(sql, batch)


def _load(oracle_conn: oracledb.Connection, oracle_table: str, strategy: str,
          date_column: str | None, run_date: str | None,
          columns: list[str], rows: list) -> None:
    cur = oracle_conn.cursor()

    if strategy == "truncate":
        log.info("Truncating %s", oracle_table)
        cur.execute(f"TRUNCATE TABLE {oracle_table}")
        oracle_conn.commit()

    elif strategy == "delete_and_insert":
        if not date_column or not run_date:
            raise ValueError("date_column and run_date are required for delete_and_insert")
        log.info("Deleting from %s WHERE %s = '%s'", oracle_table, date_column, run_date)
        cur.execute(
            f"DELETE FROM {oracle_table} WHERE {date_column} = :1",
            [run_date],
        )
        oracle_conn.commit()

    total = len(rows)
    for start in range(0, total, BATCH_SIZE):
        batch = rows[start : start + BATCH_SIZE]
        _insert_batch(cur, oracle_table, columns, batch)
        oracle_conn.commit()
        log.info("Inserted rows %d–%d of %d", start + 1, min(start + BATCH_SIZE, total), total)

    log.info("Load complete: %d rows → %s [%s]", total, oracle_table, strategy)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Silver → Oracle ADW Gold loader")
    parser.add_argument("--application-name", required=True)
    parser.add_argument("--silver-schema",    default="de_silver")
    parser.add_argument("--silver-table",     required=True, help="Table name inside de_silver")
    parser.add_argument("--oracle-table",     required=True, help="Fully-qualified Oracle target table")
    parser.add_argument("--load-strategy",    required=True, choices=["append", "truncate", "delete_and_insert"])
    parser.add_argument("--date-column",      default=None,  help="Required for delete_and_insert")
    parser.add_argument("--run-date",         required=True, help="ISO date, e.g. 2026-09-08")
    args = parser.parse_args()

    # Strip any accidental catalog/schema prefix so the user can pass either
    # "my_table" or "de_silver.my_table" or "minio.de_silver.my_table".
    silver_table = args.silver_table.split(".")[-1]

    # Default Oracle target schema is dw_enigma; qualify bare table names automatically.
    oracle_table = args.oracle_table if "." in args.oracle_table else f"dw_enigma.{args.oracle_table}"

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
    )

    if not rows:
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
        )
    finally:
        oracle_conn.close()

    log.info("Oracle load finished successfully.")


if __name__ == "__main__":
    main()
