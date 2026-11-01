"""What the `albis` logger says about requests and the listening address.

The request log middleware (`log_requests`), with its quieter handling of the
interface's routine polls, and the startup line that says what a non-loopback
bind accepts. `app.py` registers the middleware and calls
`_log_network_exposure` from its lifespan; the logic lives here so the
composition root stays wiring.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from .lifecycle import idle_watch
from .request_guard import is_loopback_host, local_host_names, strip_port

# The same logger app.py configures in `_init_logging`.
logger = logging.getLogger("albis")


def _log_network_exposure(pid: int, runtime_state: Any) -> None:
    """Say what a non-loopback bind accepts, because the operator chose it blind.

    Typing `0.0.0.0` is a decision to serve other machines, but nothing told the
    person who typed it which names ALBIS will answer to -- and the answer
    decides whether a page in their browser can reach it by pointing its own
    domain at this address. A deployment reached under a name ALBIS cannot
    derive needs `server.allowed_hosts`, and finding that out from a 403 in a
    colleague's browser is worse than reading it at startup.
    """
    bind_host = str(runtime_state.bind_host or "").strip()
    if is_loopback_host(strip_port(bind_host)):
        return

    configured = [
        str(entry).strip().lower() for entry in runtime_state.allowed_hosts if str(entry).strip()
    ]
    if "*" in configured:
        logger.warning(
            'ALBIS is listening on %s and server.allowed_hosts is ["*"] (pid=%s): every Host '
            "header is accepted, which turns off the DNS-rebinding check. Use it only where a "
            "proxy or container makes the name unpredictable.",
            bind_host,
            pid,
        )
        return
    if configured:
        logger.info(
            "ALBIS is listening on %s (pid=%s), answering to %s plus this machine's addresses.",
            bind_host,
            pid,
            ", ".join(configured),
        )
        return

    logger.warning(
        "ALBIS is listening on %s with no authentication (pid=%s). It answers to IP addresses "
        "and to %s; a client reaching it under any other name -- a reverse proxy, a container "
        "alias -- must be added to server.allowed_hosts.",
        bind_host,
        pid,
        ", ".join(sorted(local_host_names())) or "no resolvable name",
    )


# Asked by the interface on a timer -- a job, an export's progress, the
# detector's state, the live monitor -- up to several times a second. Their
# routine answers say nothing new and buried everything else at DEBUG, so
# they are left out; one that fails or is slow is logged like any other.
_POLLED_PATHS = frozenset({"/api/health", "/api/log-tail", "/api/simplon/monitor"})


def _is_poll(path: str) -> bool:
    return path in _POLLED_PATHS or path.endswith(("/status", "/latest"))


# A polled endpoint that keeps failing -- a detector switched off, a monitor
# out of reach -- failed once a second, and logged a warning each time. The
# first failure is logged as before, the rest at DEBUG, and the recovery once:
# path -> (failed polls, since when).
_poll_failures: dict[str, tuple[int, float]] = {}


def _log_poll(request, status: int, duration_ms: float) -> None:
    path = request.url.path
    line = f"{request.method} {path} -> {status} ({duration_ms:.1f}ms)"
    if status >= 400:
        count, since = _poll_failures.get(path, (0, time.monotonic()))
        _poll_failures[path] = (count + 1, since)
        if count:
            logger.debug(line)
            return
        query = f" [{request.url.query}]" if request.url.query else ""
        level = logging.ERROR if status >= 500 else logging.WARNING
        logger.log(level, "%s%s; until it answers again, repeats are DEBUG", line, query)
        return
    failed = _poll_failures.pop(path, None)
    if failed:
        logger.info(
            "%s %s answers again, after %d failed poll(s) over %.0f s",
            request.method,
            path,
            failed[0],
            time.monotonic() - failed[1],
        )
    elif duration_ms >= 1000:
        logger.info(line)


async def log_requests(request, call_next):
    """Emit request/response logs with severity based on status code and latency."""
    start = time.perf_counter()
    if request.url.path.startswith("/api/"):
        # A tab is in touch: see lifecycle.IdleWatch.
        idle_watch.touch()
    try:
        response = await call_next(request)
    except Exception:
        logger.exception("Unhandled error: %s %s", request.method, request.url.path)
        raise
    duration_ms = (time.perf_counter() - start) * 1000
    status = response.status_code
    if _is_poll(request.url.path):
        _log_poll(request, status, duration_ms)
        return response
    if status >= 500:
        logger.error("%s %s -> %s (%.1fms)", request.method, request.url.path, status, duration_ms)
    elif status >= 400:
        logger.warning(
            "%s %s -> %s (%.1fms)", request.method, request.url.path, status, duration_ms
        )
    else:
        if duration_ms >= 1000:
            logger.info(
                "%s %s -> %s (%.1fms)", request.method, request.url.path, status, duration_ms
            )
        else:
            logger.debug(
                "%s %s -> %s (%.1fms)", request.method, request.url.path, status, duration_ms
            )
    return response
