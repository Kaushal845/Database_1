"""Shared logging configuration for the project."""

import logging
import os
import sys


DEFAULT_LOG_LEVEL = "INFO"
_LOGGING_CONFIGURED = False


def setup_logging(level: str | None = None) -> int:
    """Configure root logging with a consistent format."""
    global _LOGGING_CONFIGURED

    desired = (level or os.getenv("DB1_LOG_LEVEL", DEFAULT_LOG_LEVEL)).upper()
    resolved_level = getattr(logging, desired, logging.INFO)
    root_logger = logging.getLogger()

    if not _LOGGING_CONFIGURED:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
            datefmt="%H:%M:%S",
        )
        handler.setFormatter(formatter)
        root_logger.handlers.clear()
        root_logger.addHandler(handler)
        _LOGGING_CONFIGURED = True

    root_logger.setLevel(resolved_level)

    # Keep third-party libraries quieter unless debugging is explicitly enabled.
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("pymongo").setLevel(logging.WARNING)
    return resolved_level


def get_logger(name: str) -> logging.Logger:
    """Get a logger while ensuring global logging is configured."""
    setup_logging()
    return logging.getLogger(name)
