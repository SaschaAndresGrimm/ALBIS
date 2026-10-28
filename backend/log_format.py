"""The one layout of ALBIS's log lines, on the console and in the log files:
the backend's, uvicorn's and the launcher's alike.

A module of its own, light to import: the launcher sets up its logging
before the backend app, which takes a second or two to import.
"""

from __future__ import annotations

import logging
from typing import Any

# One layout for every line on the console and in albis.log, ALBIS's and
# uvicorn's alike, the level padded so the messages start in one column:
# "2026-10-28 16:47:52,830 DEBUG   albis: GET /api/health -> 200 (3.6ms)".
LOG_FORMAT = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"


class _UvicornName(logging.Filter):
    """Uvicorn logs its startup and shutdown as "uvicorn.error", which reads
    as an error on every start; they are shown as "uvicorn"."""

    def filter(self, record: logging.LogRecord) -> bool:
        if record.name == "uvicorn.error":
            record.name = "uvicorn"
        return True


def uvicorn_log_config(level: str) -> dict[str, Any]:
    """Uvicorn's logging in ALBIS's layout. Its access log is left out: the
    request middleware in app.py already logs every request, with its status and
    duration, and raises it to a warning when it fails or is slow -- uvicorn's
    line was the same request a second time, in another layout."""
    level = str(level or "INFO").upper()
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"albis": {"format": LOG_FORMAT}},
        "filters": {"name": {"()": _UvicornName}},
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "albis",
                "filters": ["name"],
                "stream": "ext://sys.stderr",
            }
        },
        "loggers": {
            "uvicorn": {"handlers": ["console"], "level": level, "propagate": False},
            "uvicorn.error": {"level": level},
            "uvicorn.access": {"handlers": [], "level": "WARNING", "propagate": False},
        },
    }
