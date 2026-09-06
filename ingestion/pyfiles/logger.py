import logging
import sys

_PROJECT_MODULES = frozenset({
    "source",
    "ingestion",
    "sink",
    "pipeline",
    "config",
    "pyfiles",
    "bronze_layer",
    "__main__",
})

_registered_loggers: list[logging.Logger] = []
_file_handler: logging.FileHandler | None = None

_LOG_FMT = logging.Formatter(
    fmt="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class _ProjectFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.name.split(".")[0] in _PROJECT_MODULES


def setup_log_file(log_path: str) -> None:
    """Attach a FileHandler to all current and future project loggers."""
    global _file_handler
    handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    handler.setFormatter(_LOG_FMT)
    handler.addFilter(_ProjectFilter())
    _file_handler = handler
    for lgr in _registered_loggers:
        if handler not in lgr.handlers:
            lgr.addHandler(handler)


def close_log_file() -> None:
    """Flush and close the S3-bound FileHandler before upload."""
    global _file_handler
    if _file_handler:
        _file_handler.flush()
        _file_handler.close()
        _file_handler = None


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        if _file_handler and _file_handler not in logger.handlers:
            logger.addHandler(_file_handler)
        return logger

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_LOG_FMT)
    handler.addFilter(_ProjectFilter())

    logger.setLevel(level)
    logger.addHandler(handler)
    logger.propagate = False

    if _file_handler:
        logger.addHandler(_file_handler)

    for noisy in ("boto3", "botocore", "urllib3", "s3transfer", "httpx", "httpcore", "supabase"):
        logging.getLogger(noisy).setLevel(logging.CRITICAL)

    _registered_loggers.append(logger)
    return logger
