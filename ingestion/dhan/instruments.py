"""Exact NSE symbol resolution against Dhan's public compact instrument master.

Only the fixed unauthenticated HTTPS master is downloaded. Local status
inspection may read a configured symbols CSV, but never fetches or writes data.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import io
import os
from pathlib import Path
import re
import tempfile
from typing import Callable, Mapping
from zoneinfo import ZoneInfo

import requests

from ingestion.dhan.client import DhanConfigError, DhanInstrument, parse_instruments


MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
DEFAULT_CACHE_PATH = "/var/cache/dhan/instruments.csv"
CACHE_TTL = timedelta(hours=24)
MAX_MASTER_BYTES = 64 * 1024 * 1024
MAX_SYMBOLS_FILE_BYTES = 1024 * 1024
MAX_SYMBOL_LENGTH = 128
_SEGMENTS = frozenset({"NSE_EQ", "NSE_FNO", "NSE_CURRENCY", "IDX_I"})
_SYMBOL = re.compile(r"^[A-Z0-9&_. /()\-]+$")
_REQUIRED_COLUMNS = frozenset({
    "SEM_EXM_EXCH_ID", "SEM_SEGMENT", "SEM_SMST_SECURITY_ID",
    "SEM_INSTRUMENT_NAME", "SEM_TRADING_SYMBOL", "SEM_SERIES", "SEM_EXPIRY_DATE",
})
_MESSAGES = {
    "invalid_symbols": "Use comma-separated exact symbols with supported NSE segment prefixes and the configured instrument limit.",
    "selection_conflict": "Configure only one of DHAN_SYMBOLS, DHAN_SYMBOLS_FILE, or DHAN_INSTRUMENTS.",
    "symbols_file_unavailable": "The configured symbols CSV could not be read. Check DHAN_SYMBOLS_FILE and the read-only Docker file mount.",
    "symbols_file_invalid": "The symbols CSV must be UTF-8 with one symbol column, a symbol header, and at least one valid exact NSE symbol within the configured limit.",
    "unknown_symbol": "A symbol does not match an active exact NSE instrument. Derivatives require the full trading contract; indices require IDX_I.",
    "ambiguous_symbol": "A symbol maps to multiple instruments. Use an explicit segment and SecurityId instead.",
    "master_unavailable": "The official Dhan instrument master could not be read or refreshed. Check network and cache availability, then retry.",
}


class DhanInstrumentError(DhanConfigError):
    """A static, non-secret instrument-selection error for API/UI callers."""

    def __init__(self, code: str):
        if code not in _MESSAGES:
            code = "master_unavailable"
        self.code = code
        super().__init__(_MESSAGES[code])


@dataclass(frozen=True)
class DhanSymbol:
    exchange_segment: str
    symbol: str


def parse_symbols(value: str, *, max_instruments: int = 5000) -> list[DhanSymbol]:
    if not isinstance(value, str) or not value.strip() or max_instruments < 1:
        raise DhanInstrumentError("invalid_symbols")
    if len(value) > max_instruments * (MAX_SYMBOL_LENGTH + 16):
        raise DhanInstrumentError("invalid_symbols")
    selected = []
    seen = set()
    for item in value.split(","):
        item = item.strip().upper()
        if not item or item.count(":") > 1:
            raise DhanInstrumentError("invalid_symbols")
        if ":" in item:
            segment, symbol = (part.strip() for part in item.split(":", 1))
        else:
            segment, symbol = "NSE_EQ", item
        if segment not in _SEGMENTS or not symbol or len(symbol) > MAX_SYMBOL_LENGTH or not _SYMBOL.fullmatch(symbol):
            raise DhanInstrumentError("invalid_symbols")
        key = (segment, symbol)
        if key not in seen:
            selected.append(DhanSymbol(*key))
            seen.add(key)
        if len(selected) > max_instruments:
            raise DhanInstrumentError("invalid_symbols")
    return selected


def read_symbols_file(path: str | Path, *, max_instruments: int = 1000) -> str:
    """Read a bounded local watchlist, never returning file contents on error.

    The path is server configuration, not an accepted UI form parameter. A CSV
    contains exactly one column named ``symbol`` and one exact symbol per row.
    No instrument-master download or authentication happens here.
    """
    try:
        with Path(path).open("rb") as handle:
            body = handle.read(MAX_SYMBOLS_FILE_BYTES + 1)
    except (OSError, TypeError, ValueError):
        raise DhanInstrumentError("symbols_file_unavailable") from None
    if not body or len(body) > MAX_SYMBOLS_FILE_BYTES:
        raise DhanInstrumentError("symbols_file_invalid")
    try:
        reader = csv.reader(io.StringIO(body.decode("utf-8-sig"), newline=""), strict=True)
        header = next(reader, None)
        if header is None or len(header) != 1 or header[0].strip().lower() != "symbol":
            raise DhanInstrumentError("symbols_file_invalid")
        symbols = []
        for row in reader:
            if not row:  # Ordinary blank lines are harmless.
                continue
            if len(row) != 1 or not row[0].strip() or "," in row[0]:
                raise DhanInstrumentError("symbols_file_invalid")
            symbols.append(row[0].strip())
        selected = parse_symbols(",".join(symbols), max_instruments=max_instruments)
        return ",".join(
            item.symbol if item.exchange_segment == "NSE_EQ"
            else f"{item.exchange_segment}:{item.symbol}"
            for item in selected
        )
    except (UnicodeError, csv.Error, ValueError, TypeError, DhanInstrumentError):
        raise DhanInstrumentError("symbols_file_invalid") from None


def configured_selection(symbols: str = "", instruments: str = "", symbols_file: str = "", *,
                         max_instruments: int = 1000) -> tuple[str, str]:
    """Resolve local selection configuration only, before any provider request."""
    if not all(isinstance(value, str) for value in (symbols, instruments, symbols_file)):
        raise DhanInstrumentError("invalid_symbols")
    symbols, instruments, symbols_file = (value.strip() for value in (symbols, instruments, symbols_file))
    if sum(bool(value) for value in (symbols, instruments, symbols_file)) > 1:
        raise DhanInstrumentError("selection_conflict")
    if symbols_file:
        symbols = read_symbols_file(symbols_file, max_instruments=max_instruments)
    return symbols, instruments


def _now(now: datetime | None = None, clock: Callable[[], datetime] | None = None) -> datetime:
    instant = now if now is not None else clock() if clock is not None else datetime.now(timezone.utc)
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=timezone.utc)
    return instant.astimezone(timezone.utc)


def _csv_rows(body: bytes):
    if not body or len(body) > MAX_MASTER_BYTES:
        raise DhanInstrumentError("master_unavailable")
    try:
        text = body.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(text, newline=""), strict=True)
        names = reader.fieldnames
        if names is None or len(set(names)) != len(names) or not _REQUIRED_COLUMNS.issubset(names):
            raise DhanInstrumentError("master_unavailable")
        found = False
        for row in reader:
            if None in row or any(row.get(name) is None for name in _REQUIRED_COLUMNS):
                raise DhanInstrumentError("master_unavailable")
            found = True
            yield row
        if not found:
            raise DhanInstrumentError("master_unavailable")
    except (UnicodeError, csv.Error, ValueError, TypeError):
        raise DhanInstrumentError("master_unavailable") from None


def _download(session=None) -> bytes:
    owned_session = session is None
    transport = session if session is not None else requests.Session()
    response = None
    try:
        # Preparing the Request directly bypasses any session auth, default
        # headers, and cookies. Brokerage credentials never reach this host.
        request = requests.Request("GET", MASTER_URL, headers={"Accept": "text/csv"}).prepare()
        response = transport.send(request, stream=True, timeout=(5, 30), allow_redirects=False)
        if response.status_code != 200 or (getattr(response, "url", MASTER_URL) or MASTER_URL) != MASTER_URL:
            raise DhanInstrumentError("master_unavailable")
        length = response.headers.get("Content-Length")
        if length is not None and (not str(length).isdigit() or int(length) > MAX_MASTER_BYTES):
            raise DhanInstrumentError("master_unavailable")
        chunks = []
        size = 0
        for chunk in response.iter_content(chunk_size=64 * 1024):
            if not chunk:
                continue
            size += len(chunk)
            if size > MAX_MASTER_BYTES:
                raise DhanInstrumentError("master_unavailable")
            chunks.append(chunk)
        body = b"".join(chunks)
        for _ in _csv_rows(body):
            pass
        return body
    except Exception:
        raise DhanInstrumentError("master_unavailable") from None
    finally:
        if response is not None:
            try:
                response.close()
            except Exception:
                pass
        if owned_session:
            try:
                transport.close()
            except Exception:
                pass


def _write_cache(path: Path, body: bytes, instant: datetime) -> None:
    temporary = None
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.utime(temporary, (instant.timestamp(), instant.timestamp()))
        os.replace(temporary, path)
    except (OSError, ValueError, TypeError):
        raise DhanInstrumentError("master_unavailable") from None
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except OSError:
                pass


def _master(cache_path: str | Path | None, *, session=None,
            instant: datetime, allow_refresh: bool) -> bytes:
    try:
        path = Path(cache_path or os.environ.get("DHAN_INSTRUMENT_CACHE_FILE") or DEFAULT_CACHE_PATH)
    except (ValueError, TypeError):
        raise DhanInstrumentError("master_unavailable") from None
    try:
        metadata = path.stat()
        age = instant - datetime.fromtimestamp(metadata.st_mtime, tz=timezone.utc)
        if timedelta(0) <= age < CACHE_TTL and 0 < metadata.st_size <= MAX_MASTER_BYTES:
            body = path.read_bytes()
            for _ in _csv_rows(body):
                pass
            return body
    except (OSError, DhanInstrumentError, OverflowError, ValueError):
        pass
    if not allow_refresh:
        raise DhanInstrumentError("master_unavailable")
    body = _download(session)
    _write_cache(path, body, instant)
    return body


def _row_segment(row: dict, trading_day) -> str | None:
    if row["SEM_EXM_EXCH_ID"].strip().upper() != "NSE":
        return None
    segment = row["SEM_SEGMENT"].strip().upper()
    instrument = row["SEM_INSTRUMENT_NAME"].strip().upper()
    if segment == "E" and instrument == "EQUITY" and row["SEM_SERIES"].strip().upper() == "EQ":
        return "NSE_EQ"
    if segment == "I" and instrument == "INDEX":
        return "IDX_I"
    if segment in {"D", "C"}:
        try:
            expiry = datetime.fromisoformat(row["SEM_EXPIRY_DATE"].strip()).date()
        except (TypeError, ValueError):
            return None
        if expiry < trading_day:
            return None
        if segment == "D" and instrument in {"FUTSTK", "FUTIDX", "OPTSTK", "OPTIDX"}:
            return "NSE_FNO"
        if segment == "C" and instrument in {"FUTCUR", "OPTCUR"}:
            return "NSE_CURRENCY"
    return None


def resolve_selection(symbols: str = "", instruments: str = "", *, max_instruments: int = 1000,
                      cache_path: str | Path | None = None, session=None,
                      now: datetime | None = None, clock: Callable[[], datetime] | None = None,
                      allow_refresh: bool = True) -> list[DhanInstrument]:
    if not isinstance(symbols, str) or not isinstance(instruments, str):
        raise DhanInstrumentError("invalid_symbols")
    if symbols.strip() and instruments.strip():
        raise DhanInstrumentError("selection_conflict")
    if instruments.strip():
        try:
            return parse_instruments(instruments, max_instruments=max_instruments)
        except DhanConfigError:
            raise DhanInstrumentError("invalid_symbols") from None
    requested = parse_symbols(symbols, max_instruments=max_instruments)
    instant = _now(now, clock)
    body = _master(cache_path, session=session, instant=instant, allow_refresh=allow_refresh)
    matches = {(item.exchange_segment, item.symbol): set() for item in requested}
    requested_symbols = {item.symbol for item in requested}
    trading_day = instant.astimezone(ZoneInfo("Asia/Kolkata")).date()
    for row in _csv_rows(body):
        symbol = row["SEM_TRADING_SYMBOL"].strip().upper()
        index_display = (row.get("SEM_CUSTOM_SYMBOL") or "").strip().upper()
        if symbol not in requested_symbols and index_display not in requested_symbols:
            continue
        segment = _row_segment(row, trading_day)
        names = {symbol}
        if segment == "IDX_I" and index_display:
            # The official compact master calls index 13's trading symbol
            # NIFTY and its display symbol Nifty 50. Accept that exact provider
            # name only for explicitly selected indices, never company aliases.
            names.add(index_display)
        keys = [(segment, name) for name in names if (segment, name) in matches]
        if not keys:
            continue
        security_id = row["SEM_SMST_SECURITY_ID"].strip()
        if security_id.isascii() and security_id.isdigit() and int(security_id) > 0:
            for key in keys:
                matches[key].add(str(int(security_id)))
    resolved = []
    seen_ids = set()
    for item in requested:
        candidates = matches[(item.exchange_segment, item.symbol)]
        if not candidates:
            raise DhanInstrumentError("unknown_symbol")
        if len(candidates) != 1:
            raise DhanInstrumentError("ambiguous_symbol")
        security_id = next(iter(candidates))
        key = (item.exchange_segment, security_id)
        if key in seen_ids:
            raise DhanInstrumentError("ambiguous_symbol")
        seen_ids.add(key)
        resolved.append(DhanInstrument(*key))
    return resolved


def resolve_from_env(environment: Mapping[str, str] | None = None, **kwargs) -> list[DhanInstrument]:
    environment = os.environ if environment is None else environment
    if "cache_path" not in kwargs:
        kwargs["cache_path"] = environment.get("DHAN_INSTRUMENT_CACHE_FILE") or DEFAULT_CACHE_PATH
    symbols, instruments = configured_selection(
        symbols=environment.get("DHAN_SYMBOLS", ""),
        instruments=environment.get("DHAN_INSTRUMENTS", ""),
        symbols_file=environment.get("DHAN_SYMBOLS_FILE", ""),
        max_instruments=kwargs.get("max_instruments", 1000),
    )
    return resolve_selection(symbols=symbols, instruments=instruments, **kwargs)


def inspect_selection(symbols: str = "", instruments: str = "", *, symbols_file: str = "", max_instruments: int = 1000,
                      environment: Mapping[str, str] | None = None) -> dict:
    """Inspect local selection/CSV only; mapping requires an explicit runtime call."""
    if environment is not None:
        symbols = environment.get("DHAN_SYMBOLS", "")
        instruments = environment.get("DHAN_INSTRUMENTS", "")
        symbols_file = environment.get("DHAN_SYMBOLS_FILE", "")
    mode = "conflict" if sum(bool(value.strip()) for value in (symbols, instruments, symbols_file)) > 1 else "symbols" if symbols.strip() else "symbols_file" if symbols_file.strip() else "security_ids" if instruments.strip() else "missing"
    result = {"configured": mode != "missing", "valid": False, "count": 0,
              "mode": mode, "configured_validated": False,
              "resolution_required": mode in {"symbols", "symbols_file"}, "code": None, "message": "No Dhan instruments configured."}
    try:
        if mode == "conflict":
            raise DhanInstrumentError("selection_conflict")
        if mode == "missing":
            return result
        symbols, instruments = configured_selection(symbols, instruments, symbols_file, max_instruments=max_instruments)
        selected = parse_symbols(symbols, max_instruments=max_instruments) if mode in {"symbols", "symbols_file"} else resolve_selection(instruments=instruments, max_instruments=max_instruments)
        result.update(valid=True, count=len(selected), configured_validated=mode == "security_ids", message="Symbols will be resolved against the official master on explicit request." if mode in {"symbols", "symbols_file"} else "Explicit SecurityId selection is configured.")
    except DhanInstrumentError as exc:
        result.update(code=exc.code, message=str(exc))
    return result
