"""Client-aware response compression.

Frames leave the backend as raw little-endian pixel bytes, so a large detector
frame is tens of megabytes on the wire. Compressing helps a remote browser and
does nothing for a local one, so the decision is made per client, and the codec is
negotiated: zstd when the client accepts it, gzip otherwise.

The claim that has to hold above all others: compression must be invisible to the
frontend. Decoded bytes are identical and the decode headers survive.

The zstd path is driven through a small ASGI harness rather than TestClient,
because whether httpx decodes zstd depends on its version — the harness sees the
raw bytes either way and keeps these tests deterministic.
"""

from __future__ import annotations

import asyncio
import gzip
from pathlib import Path

import h5py
import numpy as np
import pytest
import zstandard
from fastapi.testclient import TestClient
from starlette.middleware.gzip import GZipResponder, IdentityResponder

from backend.app import app
from backend.response_compression import (
    MINIMUM_SIZE,
    ResponseCompressionMiddleware,
    ZstdResponder,
    available_encodings,
    is_loopback_client,
    negotiate_encoding,
    parse_accept_encoding,
)

LOCAL_CLIENT = ("127.0.0.1", 5555)
REMOTE_CLIENT = ("192.168.1.50", 5555)

# Sparse counts over a run of zeros: the shape of real diffraction data, and large
# enough to clear the minimum-size threshold.
FRAME_SHAPE = (64, 64)
FRAME_BYTES = int(np.prod(FRAME_SHAPE)) * 4

# Compressible and comfortably over MINIMUM_SIZE.
BIG_BODY = b"albis" * 4000


def unzstd(data: bytes) -> bytes:
    return zstandard.ZstdDecompressor().decompressobj().decompress(data)


# --------------------------------------------------------------------------
# ASGI harness
# --------------------------------------------------------------------------


def make_app(
    chunks: tuple[bytes, ...],
    *,
    status: int = 200,
    headers: list[tuple[str, str]] | None = None,
):
    """An ASGI app that emits `chunks` as a body, streaming when there are several."""

    async def sample_app(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [(k.encode(), v.encode()) for k, v in (headers or [])],
            }
        )
        for index, chunk in enumerate(chunks):
            await send(
                {
                    "type": "http.response.body",
                    "body": chunk,
                    "more_body": index < len(chunks) - 1,
                }
            )

    return sample_app


def drive(
    middleware,
    *,
    accept_encoding: str = "zstd",
    client: tuple[str, int] = REMOTE_CLIENT,
) -> tuple[dict[str, str], bytes]:
    """Run a middleware and return its response headers and raw body bytes."""
    sent: list[dict] = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "client": client,
        "headers": [(b"accept-encoding", accept_encoding.encode())] if accept_encoding else [],
    }
    asyncio.run(middleware(scope, receive, send))

    start = next(m for m in sent if m["type"] == "http.response.start")
    headers = {k.decode().lower(): v.decode() for k, v in start["headers"]}
    body = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    return headers, body


def compressed(chunks: tuple[bytes, ...], *, accept_encoding: str = "zstd", **kwargs):
    middleware = ResponseCompressionMiddleware(make_app(chunks, **kwargs), mode="auto")
    return drive(middleware, accept_encoding=accept_encoding)


# --------------------------------------------------------------------------
# Negotiation
# --------------------------------------------------------------------------


def test_zstd_is_available_in_this_build() -> None:
    """The rest of this module assumes the optional dependency is installed."""
    assert available_encodings() == ("zstd", "gzip")


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        # Browsers send this shape: no qualities, so our own preference decides.
        ("gzip, deflate, br, zstd", "zstd"),
        ("gzip, deflate, br", "gzip"),
        ("zstd", "zstd"),
        ("gzip", "gzip"),
        # A client that prefers gzip explicitly gets gzip, not our favourite.
        ("zstd;q=0.5, gzip;q=1.0", "gzip"),
        ("zstd;q=1.0, gzip;q=0.5", "zstd"),
        # q=0 means "not acceptable".
        ("zstd;q=0, gzip", "gzip"),
        ("zstd;q=0, gzip;q=0", None),
        # Wildcards stand in for anything unlisted.
        ("*", "zstd"),
        ("gzip, *;q=0", "gzip"),
        # Nothing we can produce.
        ("br", None),
        ("identity", None),
        ("", None),
        (None, None),
    ],
)
def test_negotiation_picks_the_best_mutually_supported_encoding(
    header: str | None, expected: str | None
) -> None:
    assert negotiate_encoding(header) == expected


def test_accept_encoding_parsing_handles_whitespace_and_case() -> None:
    assert parse_accept_encoding("  GZIP ;q=0.8 ,  Zstd  ") == {"gzip": 0.8, "zstd": 1.0}


def test_malformed_quality_is_treated_as_unacceptable() -> None:
    """A garbled header must never trick us into sending an unwanted encoding."""
    assert parse_accept_encoding("zstd;q=bogus") == {"zstd": 0.0}
    assert negotiate_encoding("zstd;q=bogus") is None


# --------------------------------------------------------------------------
# zstd payloads
# --------------------------------------------------------------------------


def test_zstd_response_is_a_valid_frame_that_round_trips() -> None:
    headers, body = compressed((BIG_BODY,))

    assert headers["content-encoding"] == "zstd"
    assert headers["vary"] == "Accept-Encoding"
    assert headers["content-length"] == str(len(body))
    assert len(body) < len(BIG_BODY)
    assert unzstd(body) == BIG_BODY


def test_zstd_streaming_response_round_trips_and_drops_content_length() -> None:
    chunks = (b"first-" * 500, b"second-" * 500, b"third-" * 500)

    headers, body = compressed(chunks)

    assert headers["content-encoding"] == "zstd"
    # A streamed body has no length to declare up front.
    assert "content-length" not in headers
    assert unzstd(body) == b"".join(chunks)


def test_zstd_streaming_sets_the_header_even_when_the_first_chunk_buffers() -> None:
    """zstd can return nothing for a small first write.

    Starlette decides whether to set Content-Encoding from whether that first
    chunk changed, so a responder that yielded b"" there would emit an unmarked
    compressed stream that no client could read.
    """
    chunks = (b"a", b"b" * 5000, b"c" * 5000)

    headers, body = compressed(chunks)

    assert headers["content-encoding"] == "zstd"
    assert unzstd(body) == b"".join(chunks)


def test_small_bodies_are_not_compressed() -> None:
    small = b"tiny"
    assert len(small) < MINIMUM_SIZE

    headers, body = compressed((small,))

    assert "content-encoding" not in headers
    assert body == small


def test_an_already_encoded_response_is_left_alone() -> None:
    """Re-compressing something the route already encoded would corrupt it."""
    headers, body = compressed((BIG_BODY,), headers=[("content-encoding", "br")])

    assert headers["content-encoding"] == "br"
    assert body == BIG_BODY


def test_event_streams_are_not_compressed() -> None:
    """Buffering an event stream through a compressor would defeat its purpose."""
    headers, body = compressed((BIG_BODY,), headers=[("content-type", "text/event-stream")])

    assert "content-encoding" not in headers
    assert body == BIG_BODY


def test_gzip_is_used_when_the_client_cannot_take_zstd() -> None:
    headers, body = compressed((BIG_BODY,), accept_encoding="gzip, deflate, br")

    assert headers["content-encoding"] == "gzip"
    assert gzip.decompress(body) == BIG_BODY


def test_a_client_accepting_nothing_we_produce_gets_raw_bytes() -> None:
    headers, body = compressed((BIG_BODY,), accept_encoding="br")

    assert "content-encoding" not in headers
    assert body == BIG_BODY


def test_zstd_compresses_better_than_gzip_on_the_same_body() -> None:
    """The premise for taking on a native dependency at all."""
    _, as_zstd = compressed((BIG_BODY,), accept_encoding="zstd")
    _, as_gzip = compressed((BIG_BODY,), accept_encoding="gzip")

    assert len(as_zstd) < len(as_gzip)


# --------------------------------------------------------------------------
# Client policy
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("mode", "client", "expected"),
    [
        ("auto", LOCAL_CLIENT, False),
        ("auto", REMOTE_CLIENT, True),
        ("on", LOCAL_CLIENT, True),
        ("on", REMOTE_CLIENT, True),
        ("off", LOCAL_CLIENT, False),
        ("off", REMOTE_CLIENT, False),
    ],
)
def test_mode_decides_when_compression_engages(mode: str, client: tuple, expected: bool) -> None:
    middleware = ResponseCompressionMiddleware(None, mode=mode)

    assert middleware._should_compress({"type": "http", "client": client}) is expected


def test_loopback_client_is_not_compressed_end_to_end() -> None:
    """Local desktop use is the common case and must pay nothing for this."""
    middleware = ResponseCompressionMiddleware(make_app((BIG_BODY,)), mode="auto")

    headers, body = drive(middleware, client=LOCAL_CLIENT)

    assert "content-encoding" not in headers
    assert body == BIG_BODY


def test_on_mode_covers_the_reverse_proxy_case() -> None:
    """Behind a proxy every request appears to come from loopback."""
    auto = ResponseCompressionMiddleware(None, mode="auto")
    forced = ResponseCompressionMiddleware(None, mode="on")
    proxied = {"type": "http", "client": LOCAL_CLIENT}

    assert auto._should_compress(proxied) is False
    assert forced._should_compress(proxied) is True


def test_off_mode_sends_raw_bytes_to_a_remote_client() -> None:
    middleware = ResponseCompressionMiddleware(make_app((BIG_BODY,)), mode="off")

    headers, body = drive(middleware, client=REMOTE_CLIENT)

    assert "content-encoding" not in headers
    assert body == BIG_BODY


def test_unrecognized_mode_falls_back_to_auto() -> None:
    assert ResponseCompressionMiddleware(None, mode="yes please").mode == "auto"
    assert ResponseCompressionMiddleware(None, mode="").mode == "auto"


@pytest.mark.parametrize(
    ("host", "expected"),
    [
        ("127.0.0.1", True),
        ("127.0.0.5", True),
        ("::1", True),
        ("192.168.1.50", False),
        ("10.0.0.1", False),
        # An address we cannot parse counts as remote: shipping a frame
        # uncompressed over a slow link is the costlier mistake.
        ("testclient", False),
        ("", False),
        (None, False),
    ],
)
def test_loopback_detection(host: str | None, expected: bool) -> None:
    assert is_loopback_client(host) is expected


def test_a_client_that_is_absent_from_the_scope_is_treated_as_remote() -> None:
    middleware = ResponseCompressionMiddleware(None, mode="auto")

    assert middleware._should_compress({"type": "http", "client": None}) is True
    assert middleware._should_compress({"type": "http"}) is True


def test_non_http_scopes_pass_straight_through() -> None:
    seen: list[str] = []

    async def sample_app(scope, receive, send):
        seen.append(scope["type"])

    middleware = ResponseCompressionMiddleware(sample_app, mode="on")

    async def noop(*_args):
        return {}

    asyncio.run(middleware({"type": "lifespan"}, noop, noop))

    assert seen == ["lifespan"]


# --------------------------------------------------------------------------
# Through the real app
# --------------------------------------------------------------------------


@pytest.fixture
def frame_file(tmp_path: Path) -> Path:
    h5_path = tmp_path / "frames.h5"
    frames = np.zeros((3, *FRAME_SHAPE), dtype=np.uint32)
    frames[:, ::8, ::8] = 1200
    with h5py.File(h5_path, "w") as h5:
        h5.require_group("/entry/data").create_dataset("data", data=frames)
    return h5_path


def _frame(client: TestClient, h5_path: Path, **kwargs: object):
    return client.get(
        "/api/frame",
        params={"file": str(h5_path), "dataset": "/entry/data/data", "index": 0},
        **kwargs,
    )


def test_remote_client_receives_a_compressed_frame(frame_file: Path) -> None:
    response = _frame(TestClient(app, client=REMOTE_CLIENT), frame_file)

    assert response.status_code == 200
    # Which codec depends on what the HTTP client advertises; both are correct.
    assert response.headers["content-encoding"] in {"zstd", "gzip"}


def test_loopback_client_is_not_compressed(frame_file: Path) -> None:
    response = _frame(TestClient(app, client=LOCAL_CLIENT), frame_file)

    assert response.status_code == 200
    assert response.headers.get("content-encoding") is None
    assert len(response.content) == FRAME_BYTES


def test_a_zstd_frame_from_the_real_app_decodes_to_the_raw_frame(frame_file: Path) -> None:
    local = _frame(TestClient(app, client=LOCAL_CLIENT), frame_file)
    remote = _frame(
        TestClient(app, client=REMOTE_CLIENT),
        frame_file,
        headers={"Accept-Encoding": "zstd"},
    )

    assert remote.headers["content-encoding"] == "zstd"
    # httpx decodes zstd only in newer versions; accept either and compare content.
    try:
        decoded = unzstd(remote.content)
    except zstandard.ZstdError:
        decoded = remote.content

    assert decoded == local.content
    assert len(decoded) == FRAME_BYTES
    frame = np.frombuffer(decoded, dtype="<u4").reshape(FRAME_SHAPE)
    assert frame[0, 0] == 1200
    assert frame[1, 1] == 0


def test_decode_headers_survive_compression(frame_file: Path) -> None:
    """The frontend reads shape and dtype from headers to interpret the payload."""
    response = _frame(TestClient(app, client=REMOTE_CLIENT), frame_file)

    assert response.headers["x-dtype"] == "<u4"
    assert response.headers["x-shape"] == "64,64"
    assert response.headers["x-frame"] == "0"


def test_client_without_compression_support_still_gets_a_readable_frame(frame_file: Path) -> None:
    response = _frame(
        TestClient(app, client=REMOTE_CLIENT),
        frame_file,
        headers={"Accept-Encoding": "identity"},
    )

    assert response.status_code == 200
    assert response.headers.get("content-encoding") is None
    assert len(response.content) == FRAME_BYTES


def test_small_responses_are_left_alone() -> None:
    """Below the threshold, compression framing costs more than it saves."""
    response = TestClient(app, client=REMOTE_CLIENT).get("/api/health")

    assert response.status_code == 200
    assert response.headers.get("content-encoding") is None


def test_health_reports_which_encodings_this_build_can_produce() -> None:
    """A packaged build that fails to bundle zstd degrades silently otherwise."""
    response = TestClient(app, client=LOCAL_CLIENT).get("/api/health")

    assert response.json()["compression_encodings"] == list(available_encodings())


# --------------------------------------------------------------------------
# Assumptions about Starlette internals
# --------------------------------------------------------------------------


def test_starlette_responder_extension_point_still_exists() -> None:
    """ZstdResponder subclasses an undocumented Starlette class.

    That reuse is deliberate — it inherits the minimum-size threshold, the
    already-encoded and event-stream skips, the streaming protocol and the Vary
    header. If a Starlette upgrade moves any of it, fail here with a clear reason
    rather than by silently sending broken responses.
    """
    assert ZstdResponder.__mro__[1] is IdentityResponder
    assert callable(IdentityResponder.apply_compression)
    assert GZipResponder.content_encoding == "gzip"
    assert ZstdResponder.content_encoding == "zstd"
    assert IdentityResponder.__init__ is not object.__init__
