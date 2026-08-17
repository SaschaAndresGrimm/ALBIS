"""Client-aware gzip compression for responses.

ALBIS is a local-first viewer, but it also runs on a server with the browser
elsewhere. Frames leave the backend as raw little-endian pixel bytes, so a single
large detector frame is tens of megabytes on the wire — painful over a remote
link, irrelevant over loopback where the transfer was already instant.

Compressing unconditionally would tax the common local case to help the rare
remote one. This middleware decides per request instead, delegating to Starlette's
GZipMiddleware only for clients that actually benefit.
"""

from __future__ import annotations

import ipaddress
from typing import Any

from starlette.middleware.gzip import GZipMiddleware

# Level 1 rather than the library default of 9. Diffraction frames are sparse
# enough that the cheapest level captures most of the achievable ratio, and it
# compresses several times faster — the point is to beat the link, not the clock.
COMPRESS_LEVEL = 1

# Below this, the gzip header and the round trip through the compressor cost more
# than they save. Small JSON replies pass through untouched.
MINIMUM_SIZE = 1024

COMPRESSION_MODES = ("auto", "on", "off")


def is_loopback_client(host: str | None) -> bool:
    """Report whether a client address is loopback.

    An address we cannot parse counts as remote. Unknown clients are more likely
    to be proxied or non-local than to be this machine, and compressing an
    already-fast local response is a smaller mistake than shipping a frame
    uncompressed across a slow one.
    """
    if not host:
        return False
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class RemoteGZipMiddleware:
    """Apply gzip based on `mode` and, for "auto", on who is asking.

    - ``auto`` (default): compress for every client except loopback.
    - ``on``: always compress. Required behind a reverse proxy, where the proxy
      is the client and so every request appears to come from loopback.
    - ``off``: never compress.
    """

    def __init__(
        self,
        app: Any,
        *,
        mode: str = "auto",
        minimum_size: int = MINIMUM_SIZE,
        compresslevel: int = COMPRESS_LEVEL,
    ) -> None:
        self.app = app
        self.mode = str(mode or "auto").strip().lower()
        if self.mode not in COMPRESSION_MODES:
            self.mode = "auto"
        self.gzip = GZipMiddleware(app, minimum_size=minimum_size, compresslevel=compresslevel)

    def _should_compress(self, scope: dict[str, Any]) -> bool:
        if self.mode == "off":
            return False
        if self.mode == "on":
            return True
        client = scope.get("client")
        host = client[0] if client else None
        return not is_loopback_client(host)

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] == "http" and self._should_compress(scope):
            await self.gzip(scope, receive, send)
            return
        await self.app(scope, receive, send)
