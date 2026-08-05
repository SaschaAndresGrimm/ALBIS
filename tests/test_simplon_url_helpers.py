from __future__ import annotations

import pytest
from fastapi import HTTPException

from backend.services.simplon import (
    normalize_simplon_base_url,
    simplon_base,
    simplon_detector_base,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("", ""),
        ("   ", ""),
        (None, ""),
        ("http://", ""),
        # Bare hosts get the scheme the detector actually serves.
        ("192.168.1.10", "http://192.168.1.10"),
        ("detector.example.com", "http://detector.example.com"),
        ("det", "http://det"),
        # An explicit port is preserved; omitting one means the default (80).
        ("192.168.1.10:5000", "http://192.168.1.10:5000"),
        ("det.local:80", "http://det.local:80"),
        ("http://det.local:", "http://det.local"),
        # Mistyped scheme separators.
        ("http//192.168.1.10", "http://192.168.1.10"),
        ("http:/192.168.1.10", "http://192.168.1.10"),
        ("http:192.168.1.10", "http://192.168.1.10"),
        ("https:///192.168.1.10", "https://192.168.1.10"),
        ("HTTP://192.168.1.10", "http://192.168.1.10"),
        # Hostnames that merely start with "http" are not scheme-mangled.
        ("http-gw.local", "http://http-gw.local"),
        ("https-det", "http://https-det"),
        # API paths pasted from the SIMPLON docs or a browser address bar.
        ("http://192.168.1.10/monitor/api/1.8.0", "http://192.168.1.10"),
        ("http://192.168.1.10/monitor/api/1.8.0/images/monitor", "http://192.168.1.10"),
        ("http://det.local/detector/api/1.8.0/config/description", "http://det.local"),
        ("192.168.1.10/monitor/api/1.8.0", "http://192.168.1.10"),
        ("http://det.local/monitor", "http://det.local"),
        # A reverse-proxy prefix ahead of the API path survives.
        ("http://gw.local/det1/monitor/api/1.8.0", "http://gw.local/det1"),
        # Whitespace and trailing slashes.
        ("  http://192.168.1.10/  ", "http://192.168.1.10"),
        ("http://192.168.1.10///", "http://192.168.1.10"),
        ("http:// 192.168.1.10", "http://192.168.1.10"),
        # Non-HTTP schemes pass through so callers can reject them.
        ("tcp://192.168.1.10:31003", "tcp://192.168.1.10:31003"),
    ],
)
def test_normalize_simplon_base_url(raw: str | None, expected: str) -> None:
    assert normalize_simplon_base_url(raw) == expected


def test_normalize_simplon_base_url_is_idempotent() -> None:
    once = normalize_simplon_base_url("192.168.1.10/monitor/api/1.8.0")
    assert normalize_simplon_base_url(once) == once


def test_simplon_base_accepts_bare_host() -> None:
    assert simplon_base("192.168.1.10", "1.8.0") == "http://192.168.1.10/monitor/api/1.8.0"
    assert simplon_detector_base("192.168.1.10", "1.8.0") == (
        "http://192.168.1.10/detector/api/1.8.0"
    )


def test_simplon_base_strips_pasted_api_path() -> None:
    assert simplon_base("http://det.local/monitor/api/1.8.0/images/monitor", "1.8.0") == (
        "http://det.local/monitor/api/1.8.0"
    )


def test_simplon_base_defaults_missing_version() -> None:
    assert simplon_base("det.local", "") == "http://det.local/monitor/api/1.8.0"
    assert simplon_base("det.local", None) == "http://det.local/monitor/api/1.8.0"
    assert simplon_base("det.local", "/1.6.0/") == "http://det.local/monitor/api/1.6.0"


@pytest.mark.parametrize("raw", ["", "   ", "http://", "tcp://192.168.1.10:31003", "///"])
def test_simplon_base_rejects_unusable_input(raw: str) -> None:
    for helper in (simplon_base, simplon_detector_base):
        with pytest.raises(HTTPException) as excinfo:
            helper(raw, "1.8.0")
        assert excinfo.value.status_code == 400
        assert "hostname or IP address" in excinfo.value.detail
