"""Convert Dhan REST and SDK records into one stable lake schema."""

from __future__ import annotations

from datetime import date, datetime, time as clock_time, timezone
import json
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo


MARKET_COLUMNS = (
    "source",
    "batch_id",
    "sequence",
    "event_id",
    "feed_type",
    "exchange_segment",
    "security_id",
    "received_at",
    "trading_date",
    "exchange_time",
    "last_price",
    "last_quantity",
    "average_price",
    "volume",
    "buy_quantity",
    "sell_quantity",
    "open",
    "high",
    "low",
    "close",
    "net_change",
    "open_interest",
    "previous_open_interest",
    "oi_day_high",
    "oi_day_low",
    "upper_circuit_limit",
    "lower_circuit_limit",
    "depth_json",
)

_SEGMENT_CODES = {0: "IDX_I", 1: "NSE_EQ", 2: "NSE_FNO", 3: "NSE_CURRENCY"}


def _float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _now(value: datetime | None = None) -> datetime:
    current = value or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc)


def _exchange_timestamp(
    value: Any,
    received_at: datetime,
    *,
    local_ist: bool,
) -> datetime | None:
    if value in (None, "", "01/01/1980 00:00:00"):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        parsed = None
        for pattern in ("%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                parsed = datetime.strptime(text, pattern)
                break
            except ValueError:
                pass
        if parsed is None:
            try:
                parsed_time = clock_time.fromisoformat(text)
            except ValueError:
                return None
            parsed = datetime.combine(received_at.date(), parsed_time)
            local_ist = False  # The official SDK formats LTT from UTC epoch.
    if parsed.tzinfo is None:
        parsed = parsed.replace(
            tzinfo=ZoneInfo("Asia/Kolkata") if local_ist else timezone.utc
        )
    return parsed.astimezone(timezone.utc)


def _base_record(
    *,
    feed_type: str,
    exchange_segment: str | None,
    security_id: str | int | None,
    received_at: datetime,
    batch_id: str,
    sequence: int,
) -> dict[str, Any]:
    record = {column: None for column in MARKET_COLUMNS}
    record.update(
        {
            "source": "dhan",
            "batch_id": batch_id,
            "sequence": sequence,
            "event_id": str(uuid4()),
            "feed_type": feed_type,
            "exchange_segment": exchange_segment,
            "security_id": None if security_id is None else str(security_id),
            "received_at": received_at,
            "trading_date": received_at.astimezone(
                ZoneInfo("Asia/Kolkata")
            ).date(),
        }
    )
    return record


def _stable_depth(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def normalize_market_snapshot(
    payload: dict,
    mode: str,
    *,
    received_at: datetime | None = None,
    batch_id: str | None = None,
) -> list[dict[str, Any]]:
    """Flatten a successful Dhan Market Quote response to fixed-schema rows."""
    if str(payload.get("status") or "").lower() != "success":
        raise ValueError("Dhan snapshot payload is not successful")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise ValueError("Dhan snapshot payload has no data object")

    timestamp = _now(received_at)
    batch = batch_id or str(uuid4())
    rows: list[dict[str, Any]] = []
    sequence = 0
    for segment in sorted(data):
        instruments = data[segment]
        if not isinstance(instruments, dict):
            continue
        for security_id in sorted(instruments, key=lambda item: (len(str(item)), str(item))):
            quote = instruments[security_id]
            if not isinstance(quote, dict):
                continue
            sequence += 1
            ohlc = quote.get("ohlc") if isinstance(quote.get("ohlc"), dict) else {}
            row = _base_record(
                feed_type=mode,
                exchange_segment=str(segment),
                security_id=security_id,
                received_at=timestamp,
                batch_id=batch,
                sequence=sequence,
            )
            exchange_time = _exchange_timestamp(
                quote.get("last_trade_time"), timestamp, local_ist=True
            )
            row.update(
                {
                    "exchange_time": exchange_time,
                    "last_price": _float(quote.get("last_price")),
                    "last_quantity": _int(quote.get("last_quantity")),
                    "average_price": _float(quote.get("average_price")),
                    "volume": _int(quote.get("volume")),
                    "buy_quantity": _int(quote.get("buy_quantity")),
                    "sell_quantity": _int(quote.get("sell_quantity")),
                    "open": _float(ohlc.get("open")),
                    "high": _float(ohlc.get("high")),
                    "low": _float(ohlc.get("low")),
                    "close": _float(ohlc.get("close")),
                    "net_change": _float(quote.get("net_change")),
                    "open_interest": _int(quote.get("oi")),
                    "oi_day_high": _int(quote.get("oi_day_high")),
                    "oi_day_low": _int(quote.get("oi_day_low")),
                    "upper_circuit_limit": _float(quote.get("upper_circuit_limit")),
                    "lower_circuit_limit": _float(quote.get("lower_circuit_limit")),
                    "depth_json": _stable_depth(quote.get("depth")),
                }
            )
            if exchange_time is not None:
                row["trading_date"] = exchange_time.astimezone(
                    ZoneInfo("Asia/Kolkata")
                ).date()
            rows.append(row)
    if not rows:
        raise ValueError("Dhan snapshot response contained no instrument rows")
    return rows


def normalize_live_record(
    payload: Any,
    *,
    sequence: int,
    received_at: datetime | None = None,
) -> dict[str, Any] | None:
    """Normalize a decoded official-SDK packet; ignore control packets."""
    if not isinstance(payload, dict):
        return None
    packet_type = str(payload.get("type") or "").strip().lower()
    feed_types = {
        "ticker data": "ticker",
        "quote data": "quote",
        "full data": "full",
        "oi data": "open_interest",
        "previous close": "previous_close",
    }
    if packet_type not in feed_types:
        return None
    segment_code = _int(payload.get("exchange_segment"))
    segment = _SEGMENT_CODES.get(segment_code) if segment_code is not None else None
    if segment is None:
        raise ValueError(
            f"Recognized Dhan {packet_type!r} packet has an invalid exchange_segment"
        )
    security_id = _int(payload.get("security_id"))
    if security_id is None or security_id <= 0:
        raise ValueError(
            f"Recognized Dhan {packet_type!r} packet has an invalid security_id"
        )

    timestamp = _now(received_at)
    exchange_time = _exchange_timestamp(
        payload.get("LTT"), timestamp, local_ist=False
    )
    row = _base_record(
        feed_type=feed_types[packet_type],
        exchange_segment=segment,
        security_id=security_id,
        received_at=timestamp,
        batch_id="",
        sequence=sequence,
    )
    depth = payload.get("depth")
    row.update(
        {
            "exchange_time": exchange_time,
            "last_price": _float(payload.get("LTP")),
            "last_quantity": _int(payload.get("LTQ")),
            "average_price": _float(payload.get("avg_price")),
            "volume": _int(payload.get("volume")),
            "buy_quantity": _int(payload.get("total_buy_quantity")),
            "sell_quantity": _int(payload.get("total_sell_quantity")),
            "open": _float(payload.get("open")),
            "high": _float(payload.get("high")),
            "low": _float(payload.get("low")),
            "close": _float(payload.get("close") or payload.get("prev_close")),
            "open_interest": _int(payload.get("OI")),
            "previous_open_interest": _int(payload.get("prev_OI")),
            "oi_day_high": _int(payload.get("oi_day_high")),
            "oi_day_low": _int(payload.get("oi_day_low")),
            "depth_json": _stable_depth(depth),
        }
    )
    if exchange_time is not None:
        row["trading_date"] = exchange_time.astimezone(
            ZoneInfo("Asia/Kolkata")
        ).date()
    return row


def json_ready(record: dict[str, Any]) -> dict[str, Any]:
    result = dict(record)
    for key, value in result.items():
        if isinstance(value, datetime):
            result[key] = value.isoformat().replace("+00:00", "Z")
        elif isinstance(value, date):
            result[key] = value.isoformat()
    return result
