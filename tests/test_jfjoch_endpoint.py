from __future__ import annotations

import socket
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from backend.app import app
from backend.services.jungfraujoch_preview import (
    jfjoch_probe_endpoint,
    normalize_jfjoch_endpoint,
    validate_jfjoch_endpoint,
)


@pytest.fixture
def listening_port() -> Iterator[int]:
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    try:
        yield int(server.getsockname()[1])
    finally:
        server.close()


def _closed_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


# A closed local port refuses the connection on Linux and macOS, but Windows
# may silently drop the SYN instead, which surfaces as a timeout. Both are
# truthful diagnoses of "nothing is listening there", so assert the pair rather
# than pinning platform-specific behaviour.
_NOTHING_LISTENING = {"refused", "timeout"}


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("", ""),
        ("   ", ""),
        (None, ""),
        ("tcp://", ""),
        # The common mistake: no transport prefix.
        ("192.168.1.5:31003", "tcp://192.168.1.5:31003"),
        ("jfjoch.example.com:31003", "tcp://jfjoch.example.com:31003"),
        # Mangled separators.
        ("tcp//192.168.1.5:31003", "tcp://192.168.1.5:31003"),
        ("tcp:/192.168.1.5:31003", "tcp://192.168.1.5:31003"),
        ("tcp:192.168.1.5:31003", "tcp://192.168.1.5:31003"),
        ("TCP://192.168.1.5:31003", "tcp://192.168.1.5:31003"),
        ("tcp:///192.168.1.5:31003", "tcp://192.168.1.5:31003"),
        # Whitespace and trailing slashes.
        ("  tcp://host:31003/  ", "tcp://host:31003"),
        ("tcp://host:31003///", "tcp://host:31003"),
        # Path transports: the slashes are the address, not separators.
        ("ipc:///tmp/jf.sock", "ipc:///tmp/jf.sock"),
        ("inproc://preview", "inproc://preview"),
        # Non-ZeroMQ transport passes through for validation to reject by name.
        ("http://host:31003", "http://host:31003"),
    ],
)
def test_normalize_jfjoch_endpoint(raw: str | None, expected: str) -> None:
    assert normalize_jfjoch_endpoint(raw) == expected


def test_normalize_jfjoch_endpoint_is_idempotent() -> None:
    once = normalize_jfjoch_endpoint("192.168.1.5:31003")
    assert normalize_jfjoch_endpoint(once) == once


def test_validate_accepts_usable_endpoints() -> None:
    assert validate_jfjoch_endpoint("192.168.1.5:31003") == "tcp://192.168.1.5:31003"
    assert validate_jfjoch_endpoint("ipc:///tmp/jf.sock") == "ipc:///tmp/jf.sock"


@pytest.mark.parametrize(
    ("raw", "expected_text"),
    [
        ("", "required"),
        ("   ", "required"),
        # No default preview port exists, so a bare host cannot be guessed at.
        ("192.168.1.5", "explicit port"),
        ("tcp://192.168.1.5", "explicit port"),
        ("tcp://:31003", "missing a host"),
        ("http://host:31003", "Unsupported preview transport"),
    ],
)
def test_validate_rejects_unusable_endpoints(raw: str, expected_text: str) -> None:
    with pytest.raises(ValueError) as excinfo:
        validate_jfjoch_endpoint(raw)
    assert expected_text in str(excinfo.value)


def test_probe_reports_an_open_port(listening_port: int) -> None:
    result = jfjoch_probe_endpoint(f"127.0.0.1:{listening_port}")

    assert result["status"] == "ok"
    assert result["code"] == "ok"
    assert result["endpoint"] == f"tcp://127.0.0.1:{listening_port}"
    assert result["port"] == listening_port


def test_probe_reports_a_closed_port() -> None:
    port = _closed_port()
    result = jfjoch_probe_endpoint(f"127.0.0.1:{port}")

    assert result["status"] == "error"
    assert result["code"] in _NOTHING_LISTENING
    assert result["port"] == port
    assert result["endpoint"] == f"tcp://127.0.0.1:{port}"


def test_probe_reports_an_unknown_host() -> None:
    result = jfjoch_probe_endpoint("no-such-preview.invalid:31003")

    assert result["status"] == "error"
    assert result["code"] == "dns"


def test_probe_skips_transports_without_a_host() -> None:
    """ipc/inproc have nothing to connect to, so they are accepted unprobed."""
    result = jfjoch_probe_endpoint("ipc:///tmp/jf.sock")

    assert result["status"] == "ok"
    assert result["code"] == "not_probed"


def test_probe_route_returns_a_diagnosis(listening_port: int) -> None:
    client = TestClient(app)

    ok = client.get("/api/jfjoch/probe", params={"endpoint": f"127.0.0.1:{listening_port}"})
    assert ok.status_code == 200
    assert ok.json()["code"] == "ok"

    port = _closed_port()
    refused = client.get("/api/jfjoch/probe", params={"endpoint": f"127.0.0.1:{port}"})
    assert refused.status_code == 200
    body = refused.json()
    assert body["status"] == "error"
    assert body["code"] in _NOTHING_LISTENING
    assert body["port"] == port


def test_probe_route_rejects_an_endpoint_without_a_port() -> None:
    response = TestClient(app).get("/api/jfjoch/probe", params={"endpoint": "192.168.1.5"})

    assert response.status_code == 400
    assert "explicit port" in response.json()["detail"]


def test_preview_start_rejects_an_endpoint_without_a_port() -> None:
    """The bridge used to accept this and fail later inside ZeroMQ."""
    response = TestClient(app).post(
        "/api/jfjoch/preview/start", json={"endpoint": "192.168.1.5", "source_id": "jfjoch"}
    )

    assert response.status_code == 400
    assert "explicit port" in response.json()["detail"]
