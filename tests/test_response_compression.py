"""Client-aware gzip compression of responses.

Frames leave the backend as raw little-endian pixel bytes, so a large detector
frame is tens of megabytes on the wire. Compressing helps a remote browser and
does nothing for a local one, so the decision is made per client.

The claim that has to hold above all others: compression must be invisible to the
frontend. Decompressed bytes are identical, and the decode headers survive.
"""

from __future__ import annotations

import gzip
from pathlib import Path

import h5py
import numpy as np
import pytest
from fastapi.testclient import TestClient

from backend.app import app
from backend.response_compression import RemoteGZipMiddleware, is_loopback_client

LOCAL_CLIENT = ("127.0.0.1", 5555)
REMOTE_CLIENT = ("192.168.1.50", 5555)

# Sparse counts over a run of zeros: the shape of real diffraction data, and large
# enough to clear the minimum-size threshold.
FRAME_SHAPE = (64, 64)


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


def test_remote_client_receives_compressed_frames(frame_file: Path) -> None:
    response = _frame(TestClient(app, client=REMOTE_CLIENT), frame_file)

    assert response.status_code == 200
    assert response.headers["content-encoding"] == "gzip"


def test_loopback_client_is_not_compressed(frame_file: Path) -> None:
    """Local desktop use is the common case and must pay nothing for this."""
    response = _frame(TestClient(app, client=LOCAL_CLIENT), frame_file)

    assert response.status_code == 200
    assert response.headers.get("content-encoding") is None


def test_compression_does_not_alter_frame_bytes(frame_file: Path) -> None:
    local = _frame(TestClient(app, client=LOCAL_CLIENT), frame_file)
    remote = _frame(TestClient(app, client=REMOTE_CLIENT), frame_file)

    assert remote.headers["content-encoding"] == "gzip"
    assert remote.content == local.content
    assert len(local.content) == np.prod(FRAME_SHAPE) * 4

    decoded = np.frombuffer(remote.content, dtype="<u4").reshape(FRAME_SHAPE)
    assert decoded[0, 0] == 1200
    assert decoded[1, 1] == 0


def test_decode_headers_survive_compression(frame_file: Path) -> None:
    """The frontend reads shape and dtype from headers to interpret the payload."""
    response = _frame(TestClient(app, client=REMOTE_CLIENT), frame_file)

    assert response.headers["x-dtype"] == "<u4"
    assert response.headers["x-shape"] == "64,64"
    assert response.headers["x-frame"] == "0"


def test_client_without_gzip_support_still_gets_a_readable_frame(frame_file: Path) -> None:
    response = _frame(
        TestClient(app, client=REMOTE_CLIENT),
        frame_file,
        headers={"Accept-Encoding": "identity"},
    )

    assert response.status_code == 200
    assert response.headers.get("content-encoding") is None
    assert len(response.content) == np.prod(FRAME_SHAPE) * 4


def test_small_responses_are_left_alone() -> None:
    """Below the threshold, gzip framing costs more than it saves."""
    response = TestClient(app, client=REMOTE_CLIENT).get("/api/health")

    assert response.status_code == 200
    assert response.headers.get("content-encoding") is None


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
    middleware = RemoteGZipMiddleware(None, mode=mode)

    assert middleware._should_compress({"type": "http", "client": client}) is expected


def test_on_mode_covers_the_reverse_proxy_case() -> None:
    """Behind a proxy every request appears to come from loopback."""
    auto = RemoteGZipMiddleware(None, mode="auto")
    forced = RemoteGZipMiddleware(None, mode="on")
    proxied = {"type": "http", "client": LOCAL_CLIENT}

    assert auto._should_compress(proxied) is False
    assert forced._should_compress(proxied) is True


def test_unrecognized_mode_falls_back_to_auto() -> None:
    assert RemoteGZipMiddleware(None, mode="yes please").mode == "auto"
    assert RemoteGZipMiddleware(None, mode="").mode == "auto"


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
    middleware = RemoteGZipMiddleware(None, mode="auto")

    assert middleware._should_compress({"type": "http", "client": None}) is True
    assert middleware._should_compress({"type": "http"}) is True


def test_compressed_payload_is_actually_smaller(frame_file: Path) -> None:
    """Guard the premise: sparse frame data has to compress to be worth the CPU."""
    raw = _frame(TestClient(app, client=LOCAL_CLIENT), frame_file).content

    assert len(gzip.compress(raw, compresslevel=1)) < len(raw) / 2
