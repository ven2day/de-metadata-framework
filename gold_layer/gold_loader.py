"""Publish curated Silver tables to a local Iceberg Gold namespace atomically."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
import logging
import os
import re

import trino.dbapi
from trino.exceptions import TrinoUserError


log = logging.getLogger("gold-loader")
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_BUCKET = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")


def identifier(value: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"Invalid SQL identifier: {value!r}")
    return value


def run_date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except (ValueError, TypeError) as exc:
        raise ValueError("run_date must use YYYY-MM-DD") from exc
    if parsed.isoformat() != value:
        raise ValueError("run_date must use YYYY-MM-DD")
    return parsed


@dataclass(frozen=True)
class GoldConfig:
    catalog: str = "minio"
    silver_schema: str = "de_silver"
    gold_schema: str = "de_gold"
    data_bucket: str = "de-data-gold"
    metadata_bucket: str = "de-iceberg-warehouse-bucket"
    lake_schema: str = "de_lake"
    bronze_schema: str = "de_bronze"

    @classmethod
    def from_env(cls) -> "GoldConfig":
        return cls(
            catalog=os.environ.get("ICEBERG_CATALOG", "minio"),
            silver_schema=os.environ.get("SILVER_DATABASE", "de_silver"),
            gold_schema=os.environ.get("GOLD_DATABASE", "de_gold"),
            data_bucket=os.environ.get("GOLD_DATA_BUCKET", "de-data-gold"),
            metadata_bucket=os.environ.get("ICEBERG_METADATA_BUCKET", "de-iceberg-warehouse-bucket"),
            lake_schema=os.environ.get("LAKE_DATABASE", "de_lake"),
            bronze_schema=os.environ.get("BRONZE_DATABASE", "de_bronze"),
        )

    def validate(self) -> None:
        for name in (self.catalog, self.silver_schema, self.gold_schema,
                     self.lake_schema, self.bronze_schema):
            identifier(name)
        if self.gold_schema.lower() in {
            self.silver_schema.lower(), self.lake_schema.lower(), self.bronze_schema.lower(),
        }:
            raise ValueError("GOLD_DATABASE must differ from Lake, Bronze, and Silver namespaces")
        for bucket in (self.data_bucket, self.metadata_bucket):
            if not _BUCKET.fullmatch(bucket):
                raise ValueError(f"Invalid S3 bucket name: {bucket!r}")


@dataclass(frozen=True)
class TableName:
    catalog: str
    schema: str
    table: str

    @property
    def sql(self) -> str:
        return ".".join(f'"{identifier(value)}"' for value in (self.catalog, self.schema, self.table))

    @property
    def label(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


def resolve_tables(silver_table: str, gold_table: str, config: GoldConfig) -> tuple[TableName, TableName]:
    config.validate()
    source_parts = silver_table.split(".")
    if len(source_parts) == 1:
        source_parts = [config.catalog, config.silver_schema, source_parts[0]]
    elif len(source_parts) == 2:
        source_parts = [config.catalog, *source_parts]
    if len(source_parts) != 3:
        raise ValueError("Silver table must be TABLE, SCHEMA.TABLE, or CATALOG.SCHEMA.TABLE")
    for part in source_parts:
        identifier(part)
    source = TableName(*source_parts)
    if source.catalog.lower() != config.catalog.lower() or source.schema.lower() != config.silver_schema.lower():
        raise ValueError("The source must belong to the configured Silver namespace")

    target_parts = gold_table.split(".")
    if len(target_parts) == 1:
        target_parts = [config.gold_schema, target_parts[0]]
    if len(target_parts) != 2:
        raise ValueError("Gold table must be TABLE or SCHEMA.TABLE")
    for part in target_parts:
        identifier(part)
    if target_parts[0].lower() != config.gold_schema.lower():
        raise ValueError("The target must belong to GOLD_DATABASE; publishing over another layer is forbidden")
    target = TableName(config.catalog, *target_parts)
    if source.label.lower() == target.label.lower():
        raise ValueError("Silver source and Gold target cannot be the same table")
    return source, target


def _query(connection, sql: str, parameters: list | None = None) -> list:
    cursor = connection.cursor()
    try:
        if parameters:
            cursor.execute(sql, parameters)
        else:
            cursor.execute(sql)
        return cursor.fetchall()
    finally:
        cursor.close()


def _schema(connection, table: TableName, *, missing_ok: bool = False) -> list[tuple[str, str]] | None:
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT * FROM {table.sql} LIMIT 0")
        result = [(item[0], re.sub(r"\s+", " ", item[1].lower().strip())) for item in cursor.description]
        if not result:
            raise ValueError(f"No columns found for {table.label}")
        for name, _ in result:
            identifier(name)
        return result
    except TrinoUserError as exc:
        if missing_ok and exc.error_name == "TABLE_NOT_FOUND":
            return None
        raise
    finally:
        cursor.close()


def _compatible(source: list[tuple[str, str]], target: list[tuple[str, str]]) -> None:
    if [(name.lower(), typ) for name, typ in source] != [(name.lower(), typ) for name, typ in target]:
        raise ValueError("Silver and Gold column names, order, and types must match for append/date replacement")


def publish(connection, *, silver_table: str, gold_table: str,
            strategy: str = "replace", date_column: str | None = None,
            selected_date: str | None = None, config: GoldConfig | None = None) -> int:
    config = config or GoldConfig.from_env()
    source, target = resolve_tables(silver_table, gold_table, config)
    day = run_date(selected_date) if selected_date is not None else None
    strategy = "replace" if strategy == "truncate" else strategy
    if strategy not in {"replace", "append", "delete_and_insert"}:
        raise ValueError(f"Unknown Gold strategy: {strategy!r}")
    if strategy == "delete_and_insert" and day is None:
        raise ValueError("run_date is required for delete_and_insert")
    columns = _schema(connection, source)
    assert columns is not None
    projection = ", ".join(f'"{name}"' for name, _ in columns)
    predicate = ""
    parameters = []
    if strategy == "delete_and_insert":
        if not date_column:
            raise ValueError("date_column is required for delete_and_insert")
        identifier(date_column)
        matching = [(name, typ) for name, typ in columns if name.lower() == date_column.lower()]
        if not matching or not (matching[0][1] == "date" or matching[0][1].startswith("timestamp")):
            raise ValueError("date_column must be a DATE or TIMESTAMP column in Silver")
        date_column = matching[0][0]
        predicate = f' WHERE CAST("{date_column}" AS DATE) = ?'
        parameters = [day]
    log.info("Reading Silver %s", source.label)
    count = int(_query(connection, f"SELECT COUNT(*) FROM {source.sql}{predicate}", parameters)[0][0])
    if count == 0:
        if strategy != "append":
            raise ValueError("Refusing an empty Gold replacement; existing Gold data has been preserved")
        log.info("Publish completed: 0 rows (empty append; Gold unchanged)")
        return 0

    _query(connection, f'CREATE SCHEMA IF NOT EXISTS "{config.catalog}"."{config.gold_schema}"')
    target_schema = _schema(connection, target, missing_ok=True)
    if target_schema is not None and strategy in {"append", "delete_and_insert"}:
        _compatible(columns, target_schema)
    log.info("Table %s ensured for atomic publish", target.label)

    source_select = f"SELECT {projection} FROM {source.sql}{predicate}"
    if strategy == "append" and target_schema is not None:
        _query(connection, f"INSERT INTO {target.sql} ({projection}) {source_select}", parameters)
    else:
        physical = f"{config.catalog}/{config.gold_schema}/{target.table}"
        properties = (
            "format = 'PARQUET', format_version = 2, "
            f"location = 's3://{config.metadata_bucket}/iceberg/{physical}/', "
            f"data_location = 's3://{config.data_bucket}/{physical}/'"
        )
        if strategy == "delete_and_insert" and target_schema is not None:
            # Both branches read a consistent query snapshot; the final Iceberg
            # metadata swap publishes the complete result in one operation.
            existing = (
                f"SELECT {projection} FROM {target.sql} "
                f'WHERE "{date_column}" IS NULL OR CAST("{date_column}" AS DATE) <> ?'
            )
            source_select = f"{existing} UNION ALL {source_select}"
            parameters = [day, day]
        _query(connection,
               f"CREATE OR REPLACE TABLE {target.sql} WITH ({properties}) AS {source_select}",
               parameters)
    log.info("Publish completed: %d Silver rows published to %s [%s]", count, target.label, strategy)
    return count


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish Silver to local Iceberg Gold through Trino")
    parser.add_argument("--application-name", required=True)
    parser.add_argument("--silver-table", required=True)
    parser.add_argument("--gold-table", required=True, help="TABLE or configured GOLD_DATABASE.TABLE")
    parser.add_argument("--load-strategy", choices=["replace", "append", "delete_and_insert", "truncate"], default="replace")
    parser.add_argument("--date-column")
    parser.add_argument("--run-date", help="Required for delete_and_insert; optional for whole-table publish")
    return parser


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [gold] %(message)s")
    args = build_parser().parse_args(argv)
    config = GoldConfig.from_env()
    resolve_tables(args.silver_table, args.gold_table, config)
    if args.run_date is not None:
        run_date(args.run_date)
    elif args.load_strategy == "delete_and_insert":
        raise ValueError("--run-date is required for delete_and_insert")
    connection = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user="gold-loader", catalog=config.catalog, schema=config.silver_schema,
    )
    try:
        publish(connection, silver_table=args.silver_table, gold_table=args.gold_table,
                strategy=args.load_strategy, date_column=args.date_column,
                selected_date=args.run_date, config=config)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
