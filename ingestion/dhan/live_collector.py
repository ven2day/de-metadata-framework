"""Long-running Dhan WebSocket collector writing immutable JSONL batches to S3.

Spark remains a replayable batch consumer. This service only decodes Dhan's
binary WebSocket packets through the official SDK, normalizes them, and lands
micro-batches in the source bucket.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from queue import Empty, Full, Queue
import re
import signal
import threading
import time
from typing import Any
from uuid import uuid4

import boto3
from botocore.config import Config

from ingestion.dhan.client import (
    DhanApiError,
    DhanConfigError,
    DhanRestClient,
    DhanTokenProvider,
    _is_retryable_api_error,
)
from ingestion.dhan.instruments import configured_selection, resolve_selection
from ingestion.dhan.normalization import json_ready, normalize_live_record
from ingestion.env.DE_Ingestion_properties import (
    DHAN_BATCH_SIZE,
    DHAN_FEED_MODE,
    DHAN_FLUSH_SECONDS,
    DHAN_INSTRUMENTS,
    DHAN_SYMBOLS,
    DHAN_SYMBOLS_FILE,
    DHAN_RAW_BUCKET,
    DHAN_RAW_PREFIX,
    DHAN_SPOOL_DIR,
    SOURCE_S3_ACCESS_KEY,
    SOURCE_S3_ENDPOINT,
    SOURCE_S3_REGION,
    SOURCE_S3_SECRET_KEY,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [dhan-live] %(levelname)s %(message)s",
)
logger = logging.getLogger("dhan-live")

_WRITER_SHUTDOWN_TIMEOUT_SECONDS = 90


class JsonlS3BatchWriter(threading.Thread):
    def __init__(self, queue: Queue, stop_event: threading.Event):
        super().__init__(name="dhan-jsonl-writer", daemon=True)
        self.queue = queue
        self.stop_event = stop_event
        self.ready = threading.Event()
        self.error: Exception | None = None
        self._spool_lock = threading.Lock()
        self.spool_dir = Path(DHAN_SPOOL_DIR)
        self.raw_prefix = DHAN_RAW_PREFIX.strip("/")
        if (
            not self.raw_prefix
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]*", self.raw_prefix)
            or any(part in {"", ".", ".."} for part in self.raw_prefix.split("/"))
        ):
            raise DhanConfigError(
                "DHAN_RAW_PREFIX must be a safe relative S3 prefix without empty, '.' or '..' segments"
            )
        if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", DHAN_RAW_BUCKET):
            raise DhanConfigError(f"Invalid DHAN_RAW_BUCKET: {DHAN_RAW_BUCKET!r}")
        self.client = boto3.client(
            "s3",
            endpoint_url=SOURCE_S3_ENDPOINT,
            aws_access_key_id=SOURCE_S3_ACCESS_KEY,
            aws_secret_access_key=SOURCE_S3_SECRET_KEY,
            region_name=SOURCE_S3_REGION,
            config=Config(
                connect_timeout=5,
                read_timeout=15,
                retries={"total_max_attempts": 2, "mode": "standard"},
            ),
        )

    def _spool_group(self, records: list[dict[str, Any]]) -> Path:
        with self._spool_lock:
            batch_id = str(uuid4())
            for record in records:
                record["batch_id"] = batch_id
            first = records[0]["received_at"]
            key = (
                f"{self.raw_prefix}/date={first:%Y-%m-%d}/hour={first:%H}/"
                f"minute={first:%M}/part-{batch_id}.json"
            )
            body = "".join(
                json.dumps(json_ready(record), separators=(",", ":"), allow_nan=False) + "\n"
                for record in records
            ).encode("utf-8")
            target = self.spool_dir.joinpath(*key.split("/"))
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_name(f".{target.name}.{uuid4().hex}.tmp")
            with temporary.open("xb") as handle:
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
            if os.name == "posix":
                directory_fd = os.open(
                    target.parent,
                    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
                )
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            return target

    def _upload_spooled(self, path: Path) -> None:
        key = path.relative_to(self.spool_dir).as_posix()
        body = path.read_bytes()
        batch_id = path.stem.removeprefix("part-")
        self.client.put_object(
            Bucket=DHAN_RAW_BUCKET,
            Key=key,
            Body=body,
            ContentType="application/x-ndjson",
            Metadata={
                "batch-id": batch_id,
                "record-count": str(body.count(b"\n")),
            },
        )
        path.unlink()
        logger.info("Committed spooled Dhan batch to s3://%s/%s", DHAN_RAW_BUCKET, key)

    def _flush(self, records: list[dict[str, Any]]) -> None:
        if not records:
            return
        # Partition by each record's UTC event-receipt minute. Spool every group
        # before attempting network I/O so a partial S3 outage loses no later group.
        groups: dict[str, list[dict[str, Any]]] = {}
        for record in records:
            minute = record["received_at"].strftime("%Y-%m-%dT%H:%M")
            groups.setdefault(minute, []).append(record)
        paths = [self._spool_group(groups[key]) for key in sorted(groups)]
        # Every current record is durable now. Mutate the caller's list before
        # network I/O so an upload exception cannot cause an accidental second
        # raw copy during emergency queue draining.
        records.clear()
        for path in paths:
            self._upload_spooled(path)

    def _replay_spool(self) -> None:
        if not self.spool_dir.exists():
            return
        for path in sorted(self.spool_dir.rglob("part-*.json")):
            self._upload_spooled(path)

    def preserve_queued(self) -> int:
        """Synchronously move any remaining queue entries into the local spool."""
        pending: list[dict[str, Any]] = []
        while True:
            try:
                pending.append(self.queue.get_nowait())
                self.queue.task_done()
            except Empty:
                break
        groups: dict[str, list[dict[str, Any]]] = {}
        for record in pending:
            minute = record["received_at"].strftime("%Y-%m-%dT%H:%M")
            groups.setdefault(minute, []).append(record)
        for key in sorted(groups):
            self._spool_group(groups[key])
        return len(pending)

    def run(self) -> None:
        records: list[dict[str, Any]] = []
        deadline = time.monotonic() + DHAN_FLUSH_SECONDS
        try:
            self._replay_spool()
            self.ready.set()
            while not self.stop_event.is_set() or not self.queue.empty():
                # Poll frequently enough that shutdown does not inherit an
                # arbitrarily large configured flush interval.
                timeout = max(0.1, min(0.5, deadline - time.monotonic()))
                try:
                    item = self.queue.get(timeout=timeout)
                    records.append(item)
                    self.queue.task_done()
                except Empty:
                    pass
                if len(records) >= DHAN_BATCH_SIZE or (
                    records and time.monotonic() >= deadline
                ):
                    # Only discard after a durable put succeeds. A failure exits
                    # the writer and causes the process to fail rather than lose data.
                    self._flush(records)
                    deadline = time.monotonic() + DHAN_FLUSH_SECONDS
            self._flush(records)
            # A queue-saturation callback can spool its current decoded event
            # directly before stopping the collector. Commit that file too.
            self._replay_spool()
        except Exception as exc:  # surfaced to the main loop
            self.error = exc
            self.stop_event.set()
            self.ready.set()
            # Stop callbacks first, then preserve ticks that reached memory but
            # had not yet entered the durable spool. If the spool itself is the
            # failing component, the process still exits non-zero and surfaces
            # the original failure.
            while True:
                try:
                    records.append(self.queue.get_nowait())
                    self.queue.task_done()
                except Empty:
                    break
            if records:
                try:
                    groups: dict[str, list[dict[str, Any]]] = {}
                    for record in records:
                        minute = record["received_at"].strftime("%Y-%m-%dT%H:%M")
                        groups.setdefault(minute, []).append(record)
                    for key in sorted(groups):
                        self._spool_group(groups[key])
                    logger.info(
                        "Preserved %d queued Dhan records in the local spool after writer failure",
                        len(records),
                    )
                except Exception:
                    logger.error("Could not preserve queued Dhan records after writer failure")


def _sdk_instruments(spec: str, feed_mode: str, *, symbols: str = "", symbols_file: str = ""):
    from dhanhq import MarketFeed

    segment_codes = {
        "IDX_I": MarketFeed.IDX,
        "NSE_EQ": MarketFeed.NSE,
        "NSE_FNO": MarketFeed.NSE_FNO,
        "NSE_CURRENCY": MarketFeed.NSE_CURR,
    }
    mode_codes = {
        "ticker": MarketFeed.Ticker,
        "quote": MarketFeed.Quote,
        "full": MarketFeed.Full,
    }
    if feed_mode not in mode_codes:
        raise DhanConfigError("DHAN_FEED_MODE must be ticker, quote, or full")
    if symbols_file:
        symbols, spec = configured_selection(
            symbols=symbols, instruments=spec, symbols_file=symbols_file, max_instruments=5000,
        )
    return [
        (segment_codes[item.exchange_segment], item.security_id, mode_codes[feed_mode])
        for item in resolve_selection(symbols=symbols, instruments=spec, max_instruments=5000)
    ]


def main() -> int:
    exit_code = 0
    try:
        from dhanhq import DhanContext, MarketFeed
    except ImportError as exc:
        raise SystemExit("dhanhq package is required for the live collector") from exc

    selection_options = {"symbols": DHAN_SYMBOLS}
    if DHAN_SYMBOLS_FILE:
        selection_options["symbols_file"] = DHAN_SYMBOLS_FILE
    instruments = _sdk_instruments(DHAN_INSTRUMENTS, DHAN_FEED_MODE, **selection_options)
    stop_event = threading.Event()
    writer_stop_event = threading.Event()
    record_queue: Queue = Queue(maxsize=max(DHAN_BATCH_SIZE * 4, 1000))
    writer = JsonlS3BatchWriter(record_queue, writer_stop_event)
    sequence = 0
    collector_failed = threading.Event()

    def stop_handler(_signum, _frame):
        stop_event.set()

    def safe_thread_exception(args):
        # The SDK may include its token-bearing WebSocket URL in a background
        # thread traceback. This dedicated process logs only safe metadata.
        thread_name = args.thread.name if args.thread is not None else "unknown"
        exception_name = args.exc_type.__name__ if args.exc_type is not None else "Exception"
        logger.error(
            "Background thread %s failed with %s; traceback suppressed to protect credentials",
            thread_name,
            exception_name,
        )

    signal.signal(signal.SIGTERM, stop_handler)
    signal.signal(signal.SIGINT, stop_handler)
    threading.excepthook = safe_thread_exception

    writer.start()
    while not writer.ready.wait(0.5):
        if stop_event.is_set():
            break
    if writer.error:
        logger.error("Dhan batch writer could not initialize: %s", type(writer.error).__name__)
        writer.join(timeout=5)
        return 1
    if stop_event.is_set():
        writer_stop_event.set()
        writer.join(timeout=_WRITER_SHUTDOWN_TIMEOUT_SECONDS)
        return 0

    provider = DhanTokenProvider()
    backoff = 1
    try:
        while not stop_event.is_set():
            feed = None
            feed_thread = None
            disconnected = threading.Event()

            def on_message(_feed, decoded):
                nonlocal sequence
                if stop_event.is_set():
                    return
                try:
                    sequence += 1
                    record = normalize_live_record(decoded, sequence=sequence)
                    if record is not None:
                        record_queue.put_nowait(record)
                except Full:
                    try:
                        writer._spool_group([record])
                        logger.error(
                            "Dhan collector queue capacity was exhausted; preserved the current tick and stopped"
                        )
                    except Exception:
                        logger.error(
                            "Dhan collector queue and emergency spool were both unavailable; stopping"
                        )
                    collector_failed.set()
                    stop_event.set()
                except Exception:
                    logger.error("Dhan packet normalization failed; stopping collector")
                    collector_failed.set()
                    stop_event.set()

            def on_disconnect(*_args):
                disconnected.set()

            try:
                token = provider.get_token()
                rest = DhanRestClient(
                    token,
                    session=provider.session,
                    provider=provider,
                )
                rest.validate_market_data_access()
                context = DhanContext(token.client_id, token.value)
                feed = MarketFeed(
                    context,
                    instruments,
                    "v2",
                    on_message=on_message,
                    on_close=on_disconnect,
                    on_error=on_disconnect,
                )
                logger.info("Connecting to Dhan live feed for %d instruments", len(instruments))
                connected_at = time.monotonic()
                feed_thread = feed.start()
                while (
                    not stop_event.is_set()
                    and feed_thread.is_alive()
                    and not disconnected.wait(0.5)
                ):
                    if writer.error:
                        collector_failed.set()
                        stop_event.set()
                        break
                if not stop_event.is_set():
                    if time.monotonic() - connected_at >= 60:
                        backoff = 1
                    raise RuntimeError("Dhan live feed disconnected")
            except DhanConfigError as exc:
                logger.error("Dhan live configuration rejected: %s", exc)
                collector_failed.set()
                stop_event.set()
            except DhanApiError as exc:
                if exc.status_code in {401, 403}:
                    logger.error(
                        "Dhan authentication was rejected after its one allowed refresh; update the token or credentials"
                    )
                    collector_failed.set()
                    stop_event.set()
                elif _is_retryable_api_error(exc):
                    logger.warning(
                        "Dhan API preflight is temporarily unavailable (HTTP %s); retrying",
                        exc.status_code or "network",
                    )
                    stop_event.wait(backoff)
                    backoff = min(backoff * 2, 60)
                else:
                    logger.error(
                        "Dhan API preflight rejected the request (HTTP %s); check account entitlements and credentials",
                        exc.status_code,
                    )
                    collector_failed.set()
                    stop_event.set()
            except Exception:
                if stop_event.is_set():
                    break
                # The official SDK's exception can contain its WebSocket URL,
                # including the token, so never interpolate it into logs.
                logger.error("Dhan live connection interrupted; validating token before retry")
                try:
                    token = provider.get_token()
                    DhanRestClient(
                        token,
                        session=provider.session,
                        provider=provider,
                    ).profile()
                except DhanConfigError as exc:
                    logger.error("Dhan token refresh is not configured: %s", exc)
                    collector_failed.set()
                    stop_event.set()
                except DhanApiError as exc:
                    if exc.status_code in {401, 403}:
                        logger.error(
                            "Dhan rejected the access token after refresh; update the token file"
                        )
                        collector_failed.set()
                        stop_event.set()
                    elif _is_retryable_api_error(exc):
                        logger.warning(
                            "Dhan token validation is temporarily unavailable (HTTP %s)",
                            exc.status_code or "network",
                        )
                    else:
                        logger.error(
                            "Dhan token validation request was rejected (HTTP %s); stopping",
                            exc.status_code,
                        )
                        collector_failed.set()
                        stop_event.set()
                except Exception:
                    # profile() already refreshes exactly once for 401/403.
                    # Network/5xx failures must not rotate a still-valid token.
                    logger.warning(
                        "Dhan token validation was unavailable; retaining the current token for the next retry"
                    )
                stop_event.wait(backoff)
                backoff = min(backoff * 2, 60)
            finally:
                unsafe_to_reconnect = False
                if feed is not None:
                    # The SDK's callback runner owns its asyncio loop. Close it
                    # from a helper so SIGTERM cannot hang the main thread.
                    def close_feed():
                        try:
                            feed.close_connection()
                        except Exception:
                            pass

                    closer = threading.Thread(target=close_feed, daemon=True)
                    closer.start()
                    closer.join(timeout=10)
                    if closer.is_alive():
                        logger.error(
                            "Dhan SDK close timed out; restarting the process to avoid a stale connection"
                        )
                        unsafe_to_reconnect = True
                if feed_thread is not None:
                    feed_thread.join(timeout=5)
                    if feed_thread.is_alive():
                        logger.error(
                            "Dhan SDK feed thread did not stop; restarting the process to avoid exceeding the connection quota"
                        )
                        unsafe_to_reconnect = True
                if unsafe_to_reconnect:
                    collector_failed.set()
                    stop_event.set()
    finally:
        stop_event.set()
        # Producers have been stopped/joined by the feed cleanup above. Only
        # now may the writer observe its stop event and finish draining.
        writer_stop_event.set()
        writer.join(timeout=_WRITER_SHUTDOWN_TIMEOUT_SECONDS)
        try:
            preserved = writer.preserve_queued()
            if preserved:
                logger.warning(
                    "Preserved %d late Dhan records in the spool during final shutdown",
                    preserved,
                )
                exit_code = 1
        except Exception:
            logger.error("Failed to preserve the final Dhan queue during shutdown")
            exit_code = 1
        if writer.is_alive():
            logger.error("Timed out while flushing the final Dhan batch")
            exit_code = 1
        elif writer.error:
            logger.error("Dhan batch writer failed: %s", type(writer.error).__name__)
            exit_code = 1
        if collector_failed.is_set():
            exit_code = 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
