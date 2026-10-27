"""Job execution helpers independent of an HTTP connection."""

from datetime import datetime, timezone
from queue import Empty, Full, Queue
from threading import Event, Thread
from zoneinfo import ZoneInfo


def scheduled_run_date(timezone_name: str, now: datetime | None = None) -> str:
    instant = now if now is not None else datetime.now(timezone.utc)
    return instant.astimezone(ZoneInfo(timezone_name)).date().isoformat()


def background_job_stream(generate):
    """Continue the complete job chain when its browser log stream closes.

    A bounded queue provides backpressure while connected. After disconnect,
    output is discarded and the worker continues consuming the job generator,
    including its downstream phases and final container cleanup.
    """
    messages = Queue(maxsize=128)
    disconnected = Event()
    finished = Event()
    errors = []

    def produce():
        try:
            for chunk in generate():
                while not disconnected.is_set():
                    try:
                        messages.put(chunk, timeout=0.1)
                        break
                    except Full:
                        continue
        except BaseException as exc:
            errors.append(exc)
        finally:
            finished.set()

    Thread(target=produce, name="pipeline-job", daemon=True).start()
    try:
        while not finished.is_set() or not messages.empty():
            try:
                yield messages.get(timeout=0.1)
            except Empty:
                continue
        if errors:
            raise errors[0]
    finally:
        disconnected.set()
