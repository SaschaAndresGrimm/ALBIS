"""Client-aware response compression.

ALBIS is a local-first viewer, but it also runs on a server with the browser
elsewhere. Frames leave the backend as raw little-endian pixel bytes, so a single
large detector frame is tens of megabytes on the wire — painful over a remote
link, irrelevant over loopback where the transfer was already instant.

Compressing unconditionally would tax the common local case to help the rare
remote one, so the decision is made per request: who is asking, and what can they
decode. Measured on a real EIGER 1M `uint32` frame (4.38 MB):

    gzip -1     2.10x    47 ms
    zstd -3     2.33x    18 ms     <- what we send when the client accepts it
    gzip -9     2.35x  1902 ms

zstd wins on both axes, so it is preferred whenever the client advertises it and
gzip remains the fallback. A client that accepts neither gets raw bytes. This is
plain HTTP content negotiation, so no browser is ever sent something it cannot
read — an old browser simply does not offer `zstd` and receives gzip instead.

Implementation note: the responders subclass Starlette's `IdentityResponder`,
which reduces zstd support to one `apply_compression` hook and reuses Starlette's
handling of the minimum-size threshold, already-encoded responses, excluded
content types, partial responses, streaming bodies and the `Vary` header. Those
names are not part of Starlette's documented API, so `test_response_compression.py`
asserts the assumptions this relies on -- including whether the hook is a
coroutine, which changed in Starlette 1.6 and must fail loudly in CI rather than
at request time.
"""

from __future__ import annotations

import ipaddress
from typing import Any

import anyio
from starlette.datastructures import Headers
from starlette.middleware.gzip import GZipResponder, IdentityResponder

try:  # pragma: no cover - exercised by whichever branch the environment provides
    import zstandard
except ImportError:  # pragma: no cover - zstd is preferred but never required
    zstandard = None  # type: ignore[assignment]

# Level 3 is libzstd's own default and the balance point for this data: it beats
# gzip at *any* level on ratio while staying ~2.7x faster than the gzip level 1
# it replaces. Higher levels keep gaining slowly (2.42x at 5, 2.49x at 7) but the
# CPU is better spent serving the next frame.
ZSTD_LEVEL = 3

# Level 1 rather than the library default of 9. Diffraction frames are sparse
# enough that the cheapest level captures most of the achievable ratio, and it
# compresses ~40x faster — the point is to beat the link, not the clock.
GZIP_LEVEL = 1

# Below this, the framing and the round trip through the compressor cost more
# than they save. Small JSON replies pass through untouched.
MINIMUM_SIZE = 1024

# Above this, compression runs on a worker thread. Frames are megabytes, and
# compressing one inline would block the event loop for long enough to stall
# every other request on the server. Mirrors what Starlette does for gzip.
THREAD_MINIMUM_SIZE = 128 * 1024

COMPRESSION_MODES = ("auto", "on", "off")


def available_encodings() -> tuple[str, ...]:
    """Return the encodings this build can produce, best first."""
    if zstandard is None:
        return ("gzip",)
    return ("zstd", "gzip")


def parse_accept_encoding(value: str | None) -> dict[str, float]:
    """Parse an `Accept-Encoding` header into `{token: quality}`.

    A malformed quality is treated as `q=0` (unacceptable) rather than ignored, so
    a garbled header can never trick us into sending an encoding the client did
    not ask for.
    """
    parsed: dict[str, float] = {}
    for part in (value or "").split(","):
        token, _, params = part.strip().partition(";")
        token = token.strip().lower()
        if not token:
            continue
        quality = 1.0
        for param in params.split(";"):
            name, _, raw = param.partition("=")
            if name.strip().lower() == "q":
                try:
                    quality = float(raw.strip())
                except ValueError:
                    quality = 0.0
        parsed[token] = quality
    return parsed


def negotiate_encoding(accept_encoding: str | None) -> str | None:
    """Pick an encoding for a client, or None to send the response unchanged.

    Highest advertised quality wins. On a tie our own preference decides, which
    puts zstd ahead of gzip — browsers send `gzip, deflate, br, zstd` without
    qualities, so the tie is the normal case.
    """
    accepted = parse_accept_encoding(accept_encoding)
    wildcard = accepted.get("*", 0.0)
    offers = [
        (accepted.get(name, wildcard), name)
        for name in available_encodings()
        if accepted.get(name, wildcard) > 0
    ]
    if not offers:
        return None
    best = max(quality for quality, _ in offers)
    return next(name for quality, name in offers if quality == best)


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


class ZstdResponder(IdentityResponder):
    """Compress a response body as a single zstd frame."""

    content_encoding = "zstd"

    def __init__(
        self,
        app: Any,
        minimum_size: int,
        level: int = ZSTD_LEVEL,
        *,
        thread_minimum_size: int = THREAD_MINIMUM_SIZE,
    ) -> None:
        super().__init__(app, minimum_size)
        self.level = level
        self.thread_minimum_size = thread_minimum_size
        self._compressor: Any = None

    @property
    def compressor(self) -> Any:
        # Built on first use so constructing a responder for a response that
        # turns out to be too small to compress costs nothing.
        if self._compressor is None:
            self._compressor = zstandard.ZstdCompressor(level=self.level).compressobj()
        return self._compressor

    async def apply_compression(self, body: bytes, *, more_body: bool) -> bytes:
        if len(body) >= self.thread_minimum_size:
            return await anyio.to_thread.run_sync(self._compress_body, body, more_body)
        return self._compress_body(body, more_body)

    def _compress_body(self, body: bytes, more_body: bool) -> bytes:
        chunk = self.compressor.compress(body)
        if not more_body:
            return chunk + self.compressor.flush(zstandard.COMPRESSOBJ_FLUSH_FINISH)
        if not chunk:
            # zstd buffers small writes and can legitimately return nothing here.
            # Starlette decides whether to set Content-Encoding by checking that
            # the first streamed chunk differs from the input, so yielding b"" on
            # that chunk would leave the header unset for the whole stream. Flush
            # a block to guarantee output.
            chunk = self.compressor.flush(zstandard.COMPRESSOBJ_FLUSH_BLOCK)
        return chunk


class ResponseCompressionMiddleware:
    """Compress responses based on `mode` and, for "auto", on who is asking.

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
        zstd_level: int = ZSTD_LEVEL,
        gzip_level: int = GZIP_LEVEL,
    ) -> None:
        self.app = app
        self.mode = str(mode or "auto").strip().lower()
        if self.mode not in COMPRESSION_MODES:
            self.mode = "auto"
        self.minimum_size = minimum_size
        self.zstd_level = zstd_level
        self.gzip_level = gzip_level

    def _should_compress(self, scope: dict[str, Any]) -> bool:
        if self.mode == "off":
            return False
        if self.mode == "on":
            return True
        client = scope.get("client")
        host = client[0] if client else None
        return not is_loopback_client(host)

    def _responder(self, encoding: str) -> Any:
        if encoding == "zstd":
            return ZstdResponder(self.app, self.minimum_size, level=self.zstd_level)
        return GZipResponder(self.app, self.minimum_size, compresslevel=self.gzip_level)

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] != "http" or not self._should_compress(scope):
            await self.app(scope, receive, send)
            return
        encoding = negotiate_encoding(Headers(scope=scope).get("accept-encoding"))
        if encoding is None:
            await self.app(scope, receive, send)
            return
        await self._responder(encoding)(scope, receive, send)
