from __future__ import annotations

import json
import socket
import threading
import urllib.error
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from fastapi.testclient import TestClient

from backend.app import app
from backend.services import simplon as simplon_module
from backend.services.simplon import classify_simplon_failure, simplon_probe

DESCRIPTION = "Dectris EIGER2 CdTe 4M"
SERIAL = "E-32-0123"


class _FakeSimplonHandler(BaseHTTPRequestHandler):
    """Minimal stand-in for the detector's SIMPLON API."""

    api_version = "1.8.0"

    def log_message(self, *args: object) -> None:  # pragma: no cover - silence test noise
        pass

    def _json(self, status: int, payload: object) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        version = self.api_version
        routes = {
            f"/monitor/api/{version}/config/mode": {"value": "enabled"},
            f"/detector/api/{version}/config/description": {"value": DESCRIPTION},
            f"/detector/api/{version}/config/detector_number": {"value": SERIAL},
        }
        if self.path in routes:
            self._json(200, routes[self.path])
            return
        if self.path.startswith("/monitor/api/") or self.path.startswith("/detector/api/"):
            self._json(404, {"error": "unknown api version"})
            return
        self._json(500, {"error": "boom"})


class _Simplon16Handler(_FakeSimplonHandler):
    """A detector that only serves API 1.6.0."""

    api_version = "1.6.0"


class _SimplonExoticHandler(_FakeSimplonHandler):
    """A detector on an API version ALBIS does not know to try."""

    api_version = "2.5.0"


def _serve(handler: type[BaseHTTPRequestHandler]) -> Iterator[str]:
    server = HTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture
def fake_simplon_16() -> Iterator[str]:
    yield from _serve(_Simplon16Handler)


@pytest.fixture
def fake_simplon_exotic() -> Iterator[str]:
    yield from _serve(_SimplonExoticHandler)


@pytest.fixture
def fake_simplon() -> Iterator[str]:
    server = HTTPServer(("127.0.0.1", 0), _FakeSimplonHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _closed_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def test_probe_reports_detector_identity(fake_simplon: str) -> None:
    result = simplon_probe(fake_simplon, "1.8.0")

    assert result["status"] == "ok"
    assert result["code"] == "ok"
    assert result["detector"] == DESCRIPTION
    assert result["serial"] == SERIAL
    assert result["url"] == f"http://{fake_simplon}"
    assert result["api_version"] == "1.8.0"


def test_probe_accepts_a_bare_host_and_pasted_api_path(fake_simplon: str) -> None:
    pasted = f"http://{fake_simplon}/monitor/api/1.8.0/images/monitor"
    assert simplon_probe(pasted, "1.8.0")["status"] == "ok"


def test_probe_falls_back_to_a_known_version(fake_simplon: str) -> None:
    """An unknown configured version resolves to the one the detector serves."""
    result = simplon_probe(fake_simplon, "9.9.9")

    assert result["status"] == "ok"
    assert result["code"] == "ok_other_version"
    assert result["api_version"] == "1.8.0"
    assert result["requested_version"] == "9.9.9"


def test_probe_reports_api_missing_when_no_known_version_answers(
    fake_simplon_exotic: str,
) -> None:
    """A detector on an unrecognized API version still reports the mismatch."""
    result = simplon_probe(fake_simplon_exotic, "1.8.0")

    assert result["status"] == "error"
    assert result["code"] == "api_missing"
    assert result["http_status"] == 404
    assert result["api_version"] == "1.8.0"


def test_probe_adopts_a_version_the_detector_serves(fake_simplon_16: str) -> None:
    """A detector on 1.6.0 probed with the 1.8.0 default reports the working one."""
    result = simplon_probe(fake_simplon_16, "1.8.0")

    assert result["status"] == "ok"
    assert result["code"] == "ok_other_version"
    assert result["api_version"] == "1.6.0"
    assert result["requested_version"] == "1.8.0"
    assert result["detector"] == DESCRIPTION


def test_probe_does_not_flag_a_switch_when_the_requested_version_works(
    fake_simplon_16: str,
) -> None:
    result = simplon_probe(fake_simplon_16, "1.6.0")

    assert result["code"] == "ok"
    assert result["api_version"] == "1.6.0"
    assert result["requested_version"] == ""


def test_probe_does_not_retry_versions_for_a_dead_host() -> None:
    """A refused port fails identically for every version, so retrying is waste."""
    port = _closed_port()
    calls: list[str] = []
    original = simplon_module._simplon_probe_version

    def counting(url: str, version: str) -> tuple[bool, Exception | None]:
        calls.append(version)
        return original(url, version)

    simplon_module._simplon_probe_version = counting
    try:
        result = simplon_probe(f"127.0.0.1:{port}", "1.8.0")
    finally:
        simplon_module._simplon_probe_version = original

    assert result["code"] == "refused"
    assert calls == ["1.8.0"]


def test_probe_reports_the_refused_port() -> None:
    port = _closed_port()
    result = simplon_probe(f"127.0.0.1:{port}", "1.8.0")

    assert result["status"] == "error"
    # Windows may drop the SYN to a closed local port rather than refusing it,
    # which reads as a timeout; both mean nothing is listening.
    assert result["code"] in {"refused", "timeout"}
    if result["code"] == "refused":
        assert result["port"] == port


def test_probe_reports_an_unknown_host() -> None:
    result = simplon_probe("no-such-detector.invalid", "1.8.0")

    assert result["status"] == "error"
    assert result["code"] == "dns"


def test_probe_route_returns_a_diagnosis_not_an_error(fake_simplon: str) -> None:
    client = TestClient(app)

    ok = client.get("/api/simplon/probe", params={"url": fake_simplon})
    assert ok.status_code == 200
    assert ok.json()["status"] == "ok"
    assert ok.json()["detector"] == DESCRIPTION

    port = _closed_port()
    refused = client.get("/api/simplon/probe", params={"url": f"127.0.0.1:{port}"})
    assert refused.status_code == 200
    body = refused.json()
    assert body["status"] == "error"
    assert body["code"] in {"refused", "timeout"}


def test_probe_route_rejects_an_unusable_address() -> None:
    response = TestClient(app).get("/api/simplon/probe", params={"url": "tcp://det.local"})

    assert response.status_code == 400
    assert "hostname or IP address" in response.json()["detail"]


def test_monitor_route_returns_a_classified_failure() -> None:
    response = TestClient(app).get(
        "/api/simplon/monitor", params={"url": "no-such-detector.invalid"}
    )

    assert response.status_code == 502
    detail = response.json()["detail"]
    assert detail["code"] == "dns"
    assert detail["summary"]


def test_mask_route_returns_a_classified_failure() -> None:
    port = _closed_port()
    response = TestClient(app).get("/api/simplon/mask", params={"url": f"127.0.0.1:{port}"})

    assert response.status_code == 502
    detail = response.json()["detail"]
    assert detail["code"] in {"refused", "timeout"}
    assert detail["summary"]


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (socket.gaierror(-2, "Name or service not known"), {"code": "dns"}),
        (urllib.error.URLError(socket.gaierror(-2, "nope")), {"code": "dns"}),
        (ConnectionRefusedError(61, "Connection refused"), {"code": "refused", "port": 5000}),
        (
            urllib.error.URLError(ConnectionRefusedError(61, "Connection refused")),
            {"code": "refused", "port": 5000},
        ),
        (TimeoutError("timed out"), {"code": "timeout"}),
        (urllib.error.URLError(TimeoutError("timed out")), {"code": "timeout"}),
        (OSError("network is down"), {"code": "unreachable"}),
    ],
)
def test_classify_transport_failures(exc: BaseException, expected: dict[str, object]) -> None:
    diagnosis = classify_simplon_failure(exc, "http://det.local:5000")

    for key, value in expected.items():
        assert diagnosis[key] == value
    assert diagnosis["message"]


@pytest.mark.parametrize(
    ("status", "code"),
    [(404, "api_missing"), (401, "http_error"), (500, "http_error")],
)
def test_classify_http_failures(status: int, code: str) -> None:
    exc = urllib.error.HTTPError("http://det.local", status, "boom", {}, None)  # type: ignore[arg-type]

    diagnosis = classify_simplon_failure(exc, "http://det.local")

    assert diagnosis["code"] == code
    assert diagnosis["http_status"] == status


def test_classify_uses_the_default_port_when_none_is_given() -> None:
    diagnosis = classify_simplon_failure(
        ConnectionRefusedError(61, "Connection refused"), "http://det.local"
    )

    assert diagnosis["port"] == 80
