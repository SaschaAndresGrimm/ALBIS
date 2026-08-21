"""Cover for the two ways a browser on another site can reach a local ALBIS.

The threat is not that someone attacks the port directly -- it is that while
ALBIS runs, every page the user visits can send requests to it that arrive with
the user's own local access.
"""

from __future__ import annotations

import json
import uuid

import pytest
from fastapi.testclient import TestClient
from starlette.datastructures import Headers

from backend.app import app, runtime_state
from backend.request_guard import (
    is_cross_site_request,
    is_host_allowed,
    strip_port,
)

client = TestClient(app)

# What a page on another origin can send to a local port without a CORS
# preflight, and what the browser attaches when it does.
CROSS_SITE_HEADERS = {"Origin": "https://evil.example", "Sec-Fetch-Site": "cross-site"}


def _headers(**pairs: str) -> Headers:
    return Headers({k.replace("_", "-"): v for k, v in pairs.items()})


# --------------------------------------------------------------------------
# Host allowlist -- the DNS-rebinding defence
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("127.0.0.1:8000", "127.0.0.1"),
        ("localhost", "localhost"),
        ("LocalHost:9", "localhost"),
        ("[::1]:8000", "::1"),
        ("[::1]", "::1"),
        ("::1", "::1"),  # unbracketed IPv6 is an address, not host:port
        ("albis.lab:8000", "albis.lab"),
        ("", ""),
    ],
)
def test_strip_port_handles_ipv6_and_ports(raw: str, expected: str) -> None:
    assert strip_port(raw) == expected


@pytest.mark.parametrize("host", ["127.0.0.1:8000", "localhost:8000", "[::1]:8000", "127.0.0.2"])
def test_loopback_bind_accepts_this_machine(host: str) -> None:
    assert is_host_allowed(host, bind_host="127.0.0.1", allowed_hosts=[])


@pytest.mark.parametrize("host", ["evil.example", "attacker.test:8000", "192.168.1.5"])
def test_loopback_bind_rejects_any_other_name(host: str) -> None:
    """A rebound request reaches loopback while still naming the attacker's domain."""
    assert not is_host_allowed(host, bind_host="127.0.0.1", allowed_hosts=[])


@pytest.mark.parametrize("bind", ["0.0.0.0", "::", ""])
def test_wildcard_bind_accepts_any_host(bind: str) -> None:
    """Binding to a wildcard is an explicit choice to serve clients ALBIS cannot name.

    Docker, a LAN address and a reverse proxy all arrive under a hostname that
    cannot be predicted, so guessing one would break working deployments.
    """
    assert is_host_allowed("anything.example", bind_host=bind, allowed_hosts=[])


def test_configured_hosts_allow_a_reverse_proxy_in_front_of_a_loopback_bind() -> None:
    """The documented fix for the one legitimate case the auto-rule cannot detect."""
    assert is_host_allowed("albis.lab:443", bind_host="127.0.0.1", allowed_hosts=["albis.lab"])
    assert not is_host_allowed("evil.example", bind_host="127.0.0.1", allowed_hosts=["albis.lab"])
    # Local access must keep working alongside the proxy name.
    assert is_host_allowed("127.0.0.1:8000", bind_host="127.0.0.1", allowed_hosts=["albis.lab"])


def test_wildcard_entry_disables_the_check() -> None:
    assert is_host_allowed("anything.example", bind_host="127.0.0.1", allowed_hosts=["*"])


def test_a_concrete_bind_address_allows_itself() -> None:
    assert is_host_allowed("192.168.1.5:8000", bind_host="192.168.1.5", allowed_hosts=[])
    assert not is_host_allowed("evil.example", bind_host="192.168.1.5", allowed_hosts=[])


# --------------------------------------------------------------------------
# Cross-site writes -- the CSRF defence
# --------------------------------------------------------------------------


@pytest.mark.parametrize("value", ["cross-site", "same-site"])
def test_browser_reporting_another_site_is_cross_site(value: str) -> None:
    assert is_cross_site_request(_headers(sec_fetch_site=value), "127.0.0.1:8000")


@pytest.mark.parametrize("value", ["same-origin", "none"])
def test_frontend_and_user_navigation_are_not_cross_site(value: str) -> None:
    """`same-origin` is ALBIS's own page; `none` is a typed URL or a bookmark."""
    assert not is_cross_site_request(_headers(sec_fetch_site=value), "127.0.0.1:8000")


def test_origin_is_the_fallback_when_sec_fetch_site_is_absent() -> None:
    assert is_cross_site_request(_headers(origin="https://evil.example"), "127.0.0.1:8000")
    assert not is_cross_site_request(_headers(origin="http://127.0.0.1:8000"), "127.0.0.1:8000")


def test_sec_fetch_site_is_trusted_over_a_spoofable_origin() -> None:
    """Page script cannot set `Sec-Fetch-Site`, so it decides when both are present."""
    headers = _headers(sec_fetch_site="cross-site", origin="http://127.0.0.1:8000")
    assert is_cross_site_request(headers, "127.0.0.1:8000")


def test_a_client_that_is_not_a_browser_is_left_alone() -> None:
    """Detector-side producers post frames with neither header.

    Requiring one would break the documented Remote Stream workflow and stop
    nobody: a non-browser client can send any header it likes.
    """
    assert not is_cross_site_request(_headers(), "127.0.0.1:8000")
    assert not is_cross_site_request(_headers(origin="null"), "127.0.0.1:8000")


# --------------------------------------------------------------------------
# End to end
# --------------------------------------------------------------------------


def test_cross_site_upload_cannot_write_a_file() -> None:
    """`multipart/form-data` predates CORS and is sent with no preflight.

    Before the guard this wrote the file and the attacking page simply ignored
    the response it was not allowed to read.
    """
    # The upload route resolved its root at registration time, so the only
    # meaningful place to look is the real data dir -- pointing a fixture
    # elsewhere would assert against a directory nothing writes to.
    written = runtime_state.data_dir.resolve() / "pwn.tif"
    assert not written.exists(), "stale artefact from an earlier run"
    boundary = uuid.uuid4().hex
    body = (
        (
            f"--{boundary}\r\n"
            'Content-Disposition: form-data; name="file"; filename="pwn.tif"\r\n'
            "Content-Type: application/octet-stream\r\n\r\n"
        ).encode()
        + b"II*\x00"
        + f"\r\n--{boundary}--\r\n".encode()
    )

    response = client.post(
        "/api/upload",
        content=body,
        headers={**CROSS_SITE_HEADERS, "Content-Type": f"multipart/form-data; boundary={boundary}"},
    )

    assert response.status_code == 403
    assert not written.exists()


@pytest.fixture
def loopback_bind(monkeypatch: pytest.MonkeyPatch):
    """Pin the bind address the guard reasons about.

    `runtime_state.bind_host` comes from whatever albis.config.json the machine
    has, and that file is gitignored -- a developer who binds 0.0.0.0 locally
    would otherwise see these fail while CI, running on defaults, passed.
    """
    monkeypatch.setattr(runtime_state, "bind_host", "127.0.0.1")
    monkeypatch.setattr(runtime_state, "allowed_hosts", [])


def test_rebound_host_cannot_read_the_filesystem(loopback_bind) -> None:
    response = client.get("/api/browse", params={"path": "/etc"}, headers={"Host": "evil.example"})

    assert response.status_code == 403
    assert "allowed_hosts" in response.json()["detail"]


def test_a_wildcard_bind_still_answers_any_host(monkeypatch: pytest.MonkeyPatch) -> None:
    """Docker and LAN deployments must keep working unchanged."""
    monkeypatch.setattr(runtime_state, "bind_host", "0.0.0.0")
    monkeypatch.setattr(runtime_state, "allowed_hosts", [])

    assert client.get("/api/health", headers={"Host": "albis-container"}).status_code == 200


def test_configured_allowed_host_is_served(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runtime_state, "bind_host", "127.0.0.1")
    monkeypatch.setattr(runtime_state, "allowed_hosts", ["albis.lab"])

    assert client.get("/api/health", headers={"Host": "albis.lab"}).status_code == 200
    assert client.get("/api/health", headers={"Host": "evil.example"}).status_code == 403


@pytest.mark.parametrize("path", ["/api/choose-file", "/api/choose-folder"])
def test_cross_site_cannot_open_a_native_picker(path: str) -> None:
    """Guarded despite being GETs: they put a dialog on the user's screen."""
    response = client.get(path, headers=CROSS_SITE_HEADERS)
    assert response.status_code == 403


def test_cross_site_reads_are_still_served() -> None:
    """The guard is about writes and rebinding, not about blocking every read.

    A genuinely cross-origin GET is already unreadable to the calling page under
    CORS, so refusing it would add nothing and could break embedding.
    """
    assert client.get("/api/health", headers=CROSS_SITE_HEADERS).status_code == 200


def test_remote_stream_ingest_still_works_for_a_script() -> None:
    """The documented external-producer workflow must be untouched."""
    response = client.post(
        "/api/remote/v1/frame",
        params={"source_id": "guard-test"},
        data={"meta": json.dumps({"format": "raw", "dtype": "<u2", "shape": [2, 2]})},
        files={"image": ("f.raw", b"\x01\x00\x02\x00\x03\x00\x04\x00", "application/octet-stream")},
    )
    assert response.status_code == 200


def test_the_frontend_own_requests_are_allowed() -> None:
    """Same-origin is what the browser reports for ALBIS's own fetches."""
    response = client.post(
        "/api/client-log",
        json={"level": "info", "message": "hello"},
        headers={"Origin": "http://127.0.0.1:8000", "Sec-Fetch-Site": "same-origin"},
    )
    assert response.status_code == 200
