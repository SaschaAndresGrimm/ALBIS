"""Rejects requests a browser on another site made on the user's behalf.

ALBIS is a local-first viewer with no authentication, which is reasonable for a
program that only ever answers the person sitting in front of it. What makes it
reachable by anyone else is the browser: while ALBIS is running, every page the
user visits can send requests to it, and they arrive carrying whatever local
access the user has. Two consequences need closing.

DNS rebinding. A page on `attacker.example` whose DNS answer is `127.0.0.1` is,
to the browser, same-origin with whatever is listening there. The same-origin
policy stops protecting anything, and the API becomes readable -- including
`/api/browse`, which lists directories, and `/api/image`, which returns file
contents. The defence is to check the `Host` header, because the rebound request
still names the attacker's domain in it.

Cross-site writes. CORS blocks a page from *reading* a cross-origin response,
which is often mistaken for blocking the request. `POST /api/upload` is
`multipart/form-data`, a form encoding that predates CORS and is therefore sent
with no preflight at all -- so any page can write a file into the data directory
and simply not look at the reply. The defence is to notice the browser's own
account of where the request came from.

Both checks let a request through when it does not look like it came from a
browser. That is deliberate: `POST /api/remote/v1/frame` exists to be called by
detector-side scripts, and a non-browser client can set any header it likes, so
demanding one would break the documented workflow while stopping nobody.
"""

from __future__ import annotations

import ipaddress
from typing import Any

from starlette.datastructures import Headers
from starlette.responses import JSONResponse

# Hostnames that always mean this machine. Any address that parses as a loopback
# IP is accepted too, so `127.0.0.2` and `::1` need no special case.
LOOPBACK_NAMES = frozenset({"localhost", "localhost.localdomain", ""})

# A bind address that accepts connections from anywhere. Choosing one is an
# explicit decision to serve clients other than this machine, and those clients
# reach ALBIS under a name it cannot predict -- a container hostname, a LAN
# address, a reverse proxy's `server_name`. Guessing would break working
# deployments, so the allowlist stands aside unless it was configured.
WILDCARD_BINDS = frozenset({"0.0.0.0", "::", "*", ""})

# `Sec-Fetch-Site` values that mean the request did not originate from ALBIS's
# own pages. `none` is a user-initiated navigation (a typed URL or a bookmark)
# and `same-origin` is the frontend itself; everything else is another site.
CROSS_SITE_FETCH_VALUES = frozenset({"cross-site", "same-site"})

# Requests that change state or act on the desktop. The two GETs are here
# because they open a native file dialog on the user's screen -- harmless to the
# attacker, who cannot read the answer, but the user gets a picker they did not
# ask for and may click through.
GUARDED_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
GUARDED_GET_PATHS = frozenset({"/api/choose-file", "/api/choose-folder"})


def strip_port(host_header: str) -> str:
    """Return the host part of a `Host` header, without its port.

    IPv6 literals are bracketed (`[::1]:8000`), so the last colon is only a port
    separator when it comes after the closing bracket -- or when there is no
    bracket and only one colon, since a bare `::1` is an address, not a port.
    """
    value = host_header.strip()
    if value.startswith("["):
        closing = value.find("]")
        if closing != -1:
            return value[1:closing].lower()
        return value.lower()
    if value.count(":") == 1:
        value = value.rsplit(":", 1)[0]
    return value.lower()


def is_loopback_host(host: str) -> bool:
    if host in LOOPBACK_NAMES:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def is_host_allowed(host_header: str, *, bind_host: str, allowed_hosts: list[str]) -> bool:
    """Decide whether a `Host` header may be served.

    An explicit `server.allowed_hosts` wins outright. Otherwise the rule follows
    the bind address: a loopback bind can only legitimately be addressed as this
    machine, so anything else is a rebound request, while a wildcard bind has no
    predictable name and is left alone.
    """
    configured = [entry.strip().lower() for entry in allowed_hosts if str(entry).strip()]
    host = strip_port(host_header)
    if configured:
        if "*" in configured:
            return True
        return host in configured or is_loopback_host(host)

    if str(bind_host).strip().lower() in WILDCARD_BINDS:
        return True
    if is_loopback_host(host):
        return True
    return host == strip_port(str(bind_host))


def is_cross_site_request(headers: Headers, host_header: str) -> bool:
    """Report whether a browser told us this request came from another site.

    `Sec-Fetch-Site` is the browser's own statement and cannot be set by page
    script, so it is preferred. `Origin` is the fallback for browsers that do not
    send it; it is compared against the requested host rather than a configured
    origin, so the check keeps working behind a proxy or on any port.
    """
    fetch_site = headers.get("sec-fetch-site", "").strip().lower()
    if fetch_site:
        return fetch_site in CROSS_SITE_FETCH_VALUES

    origin = headers.get("origin", "").strip()
    if not origin or origin.lower() == "null":
        # No browser context at all -- a script, curl, or a detector-side
        # producer. Nothing to verify, and nothing CSRF can exploit.
        return False
    _, _, origin_host = origin.partition("://")
    return strip_port(origin_host) != strip_port(host_header)


def is_guarded(method: str, path: str) -> bool:
    return method.upper() in GUARDED_METHODS or (
        method.upper() == "GET" and path in GUARDED_GET_PATHS
    )


class RequestGuardMiddleware:
    """Reject rebound and cross-site browser requests before they do any work."""

    def __init__(
        self,
        app: Any,
        *,
        get_bind_host: Any,
        get_allowed_hosts: Any,
        logger: Any = None,
    ) -> None:
        self.app = app
        self.get_bind_host = get_bind_host
        self.get_allowed_hosts = get_allowed_hosts
        self.logger = logger

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = Headers(scope=scope)
        host_header = headers.get("host", "")
        path = scope.get("path", "")
        method = scope.get("method", "GET")

        if not is_host_allowed(
            host_header,
            bind_host=str(self.get_bind_host() or ""),
            allowed_hosts=list(self.get_allowed_hosts() or []),
        ):
            await self._reject(
                scope,
                receive,
                send,
                reason=f"Host header {host_header!r} is not allowed",
                detail=(
                    "This address is not served by ALBIS. Open it as localhost, or add "
                    "the name to server.allowed_hosts."
                ),
            )
            return

        if is_guarded(method, path) and is_cross_site_request(headers, host_header):
            await self._reject(
                scope,
                receive,
                send,
                reason=f"cross-site {method} {path} from origin {headers.get('origin', '')!r}",
                detail="Cross-site requests are not accepted for this endpoint.",
            )
            return

        await self.app(scope, receive, send)

    async def _reject(
        self, scope: Any, receive: Any, send: Any, *, reason: str, detail: str
    ) -> None:
        if self.logger is not None:
            client = scope.get("client")
            self.logger.warning(
                "Blocked request: %s (client=%s)", reason, client[0] if client else "unknown"
            )
        await JSONResponse({"detail": detail}, status_code=403)(scope, receive, send)
