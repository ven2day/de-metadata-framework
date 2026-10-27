"""Spark source adapter for Dhan Market Quote snapshots."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ingestion.dhan.client import DhanRestClient
from ingestion.dhan.instruments import configured_selection, resolve_selection
from ingestion.dhan.normalization import MARKET_COLUMNS, normalize_market_snapshot
from ingestion.env.DE_Ingestion_properties import DHAN_INSTRUMENTS, DHAN_SYMBOLS, DHAN_SYMBOLS_FILE, DHAN_SNAPSHOT_MODE
from ingestion.pyfiles.logger import get_logger


logger = get_logger(__name__)

_STRING_FIELDS = {
    "source",
    "batch_id",
    "event_id",
    "feed_type",
    "exchange_segment",
    "security_id",
    "depth_json",
}
_LONG_FIELDS = {
    "sequence",
    "last_quantity",
    "volume",
    "buy_quantity",
    "sell_quantity",
    "open_interest",
    "previous_open_interest",
    "oi_day_high",
    "oi_day_low",
}
_DOUBLE_FIELDS = {
    "last_price",
    "average_price",
    "open",
    "high",
    "low",
    "close",
    "net_change",
    "upper_circuit_limit",
    "lower_circuit_limit",
}


def market_schema() -> StructType:
    fields = []
    for column in MARKET_COLUMNS:
        if column in _STRING_FIELDS:
            data_type = StringType()
        elif column in _LONG_FIELDS:
            data_type = LongType()
        elif column in _DOUBLE_FIELDS:
            data_type = DoubleType()
        elif column in {"received_at", "exchange_time"}:
            data_type = TimestampType()
        elif column == "trading_date":
            data_type = DateType()
        else:  # pragma: no cover - MARKET_COLUMNS and sets are kept in lockstep
            raise AssertionError(f"No Spark type configured for {column}")
        fields.append(StructField(column, data_type, nullable=True))
    return StructType(fields)


def read_dhan_snapshot(
    spark: SparkSession,
    instruments: str | None = None,
    mode: str | None = None,
    *,
    symbols: str | None = None,
    symbols_file: str | None = None,
    client: DhanRestClient | None = None,
) -> DataFrame:
    selected_mode = (mode or DHAN_SNAPSHOT_MODE).strip().lower()
    # Explicit arguments select one mode without inheriting the other from env.
    if instruments is None and symbols is None and symbols_file is None:
        instruments, symbols, symbols_file = DHAN_INSTRUMENTS, DHAN_SYMBOLS, DHAN_SYMBOLS_FILE
    if symbols_file:
        symbols, instruments = configured_selection(
            symbols=symbols or "", instruments=instruments or "",
            symbols_file=symbols_file, max_instruments=1000,
        )
    parsed = resolve_selection(symbols=symbols or "", instruments=instruments or "", max_instruments=1000)
    api = client or DhanRestClient.from_env()
    api.validate_market_data_access()
    logger.info(
        "Reading Dhan %s snapshot for %d NSE instruments",
        selected_mode,
        len(parsed),
    )
    payload = api.market_snapshot(parsed, selected_mode)
    rows = normalize_market_snapshot(payload, selected_mode)
    requested = {
        (item.exchange_segment, item.security_id) for item in parsed
    }
    returned = {
        (str(row["exchange_segment"]), str(row["security_id"])) for row in rows
    }
    missing = requested - returned
    if missing:
        sample = ", ".join(f"{segment}:{security_id}" for segment, security_id in sorted(missing)[:10])
        raise ValueError(
            f"Dhan snapshot omitted {len(missing)} requested instrument(s): {sample}"
        )
    logger.info("Loaded %d Dhan market-data rows", len(rows))
    return spark.createDataFrame(rows, schema=market_schema())
