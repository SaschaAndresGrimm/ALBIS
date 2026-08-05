"""SIMPLON monitor client helpers."""

from __future__ import annotations

import base64
import json
import re
import socket
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, NoReturn

import numpy as np
from fastapi import HTTPException

from ..image_formats import _normalize_image_array

_SCHEME_RE = re.compile(r"^[a-z][a-z0-9+.\-]*://", re.IGNORECASE)
# `http//host`, `http:/host`, `http:host` — a scheme keyword with a mangled
# separator. The trailing `:`/`/` requirement keeps hostnames that merely start
# with "http" (e.g. `http-gw.local`) untouched.
_MALFORMED_SCHEME_RE = re.compile(r"^(https?)(?::/*|/+)", re.IGNORECASE)
# SIMPLON sub-API roots, e.g. `/monitor/api/1.8.0/images/monitor`.
_API_PATH_RE = re.compile(r"/(monitor|detector|stream|filewriter|system)/api(/|$)", re.IGNORECASE)
# A dangling sub-API segment without the `/api` part, e.g. `http://host/monitor`.
_TRAILING_API_ROOT_RE = re.compile(
    r"/(monitor|detector|stream|filewriter|system)/*$", re.IGNORECASE
)

_DEFAULT_API_VERSION = "1.8.0"
# Short enough that a wrong address fails the connection test while the user
# is still looking at the field.
_PROBE_TIMEOUT_S = 3.0


def normalize_simplon_base_url(url: str) -> str:
    """Fold user-supplied detector addresses into a canonical base URL.

    Accepts what operators actually enter — a bare hostname or IP, a URL copied
    from the SIMPLON docs (carrying the `/monitor/api/<version>` path), or a
    mistyped scheme separator — and returns `http(s)://host[:port][/prefix]`.
    The port is never rewritten: omitting it means the detector default (80),
    and an explicit port is kept as given.

    Mirrors ``frontend/modules/simplon_url_utils.js`` so presets and settings
    persisted outside the UI normalize identically. Returns "" for empty input;
    a non-HTTP scheme is passed through unchanged for the caller to reject.
    """
    text = re.sub(r"\s+", "", str(url or ""))
    if not text:
        return ""

    if _SCHEME_RE.match(text):
        scheme, _, remainder = text.partition("://")
        if scheme.lower() not in {"http", "https"}:
            return text
        text = f"{scheme.lower()}://{remainder}"
    else:
        malformed = _MALFORMED_SCHEME_RE.match(text)
        if malformed:
            text = f"{malformed.group(1).lower()}://{text[malformed.end():]}"
        else:
            text = f"http://{text}"

    scheme, _, rest = text.partition("://")
    rest = rest.lstrip("/")
    if not rest:
        return ""

    api_path = _API_PATH_RE.search(rest)
    rest = rest[: api_path.start()] if api_path else _TRAILING_API_ROOT_RE.sub("", rest)

    rest = rest.rstrip("/").rstrip(":")
    if not rest:
        return ""
    return f"{scheme}://{rest}"


def _simplon_api_base(url: str, version: str, section: str) -> str:
    base = normalize_simplon_base_url(url)
    parsed = urllib.parse.urlparse(base)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid SIMPLON base URL. Enter the detector hostname or IP address "
                "(for example 192.168.1.10), optionally as a full http:// URL."
            ),
        )
    ver = (version or _DEFAULT_API_VERSION).strip().strip("/")
    if not ver:
        ver = _DEFAULT_API_VERSION
    return f"{base}/{section}/api/{ver}"


def simplon_base(url: str, version: str) -> str:
    return _simplon_api_base(url, version, "monitor")


def simplon_detector_base(url: str, version: str) -> str:
    return _simplon_api_base(url, version, "detector")


def classify_simplon_failure(exc: BaseException, base_url: str) -> dict[str, Any]:
    """Turn a transport/HTTP error into a diagnosis the UI can act on.

    Returns a ``code`` from a fixed vocabulary — ``dns``, ``refused``,
    ``timeout``, ``api_missing``, ``http_error``, ``unreachable`` — plus the
    parameters that make the message specific (the port that refused, the HTTP
    status). The frontend localizes from ``code``; ``message`` is the English
    fallback used in logs and by non-UI clients.
    """
    parsed = urllib.parse.urlparse(base_url)
    host = parsed.hostname or base_url
    port = parsed.port or (443 if parsed.scheme == "https" else 80)

    if isinstance(exc, urllib.error.HTTPError):
        if exc.code == 404:
            return {
                "code": "api_missing",
                "http_status": 404,
                "message": f"{host} answered, but no SIMPLON API was found at this path",
            }
        return {
            "code": "http_error",
            "http_status": int(exc.code),
            "message": f"{host} answered with HTTP {exc.code}",
        }

    reason = getattr(exc, "reason", None)
    causes = [exc] + ([reason] if isinstance(reason, BaseException) else [])
    if any(isinstance(cause, socket.gaierror) for cause in causes):
        return {"code": "dns", "message": f"Host not found: {host}"}
    if any(isinstance(cause, ConnectionRefusedError) for cause in causes):
        return {
            "code": "refused",
            "port": port,
            "message": f"Connection refused by {host} on port {port}",
        }
    # socket.timeout is an alias of TimeoutError on Python 3.10+.
    if any(isinstance(cause, TimeoutError) for cause in causes):
        return {"code": "timeout", "message": f"No response from {host}"}
    return {"code": "unreachable", "message": f"Cannot reach {host} on port {port}"}


def _raise_simplon_failure(exc: BaseException, base_url: str, summary: str) -> NoReturn:
    diagnosis = classify_simplon_failure(exc, base_url)
    raise HTTPException(status_code=502, detail={"summary": summary, **diagnosis}) from exc


def simplon_set_mode(base: str, mode: str) -> None:
    payload = json.dumps({"value": mode}).encode("utf-8")
    req = urllib.request.Request(
        f"{base}/config/mode",
        data=payload,
        method="PUT",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            resp.read()
    except Exception as exc:
        _raise_simplon_failure(exc, base, "Failed to update SIMPLON monitor mode")


def simplon_fetch_monitor(base: str, timeout_ms: int) -> bytes | None:
    query = urllib.parse.urlencode({"timeout": max(0, int(timeout_ms))}) if timeout_ms else ""
    url = f"{base}/images/monitor"
    if query:
        url = f"{url}?{query}"
    try:
        with urllib.request.urlopen(url, timeout=max(timeout_ms / 1000 + 1, 2)) as resp:
            return resp.read()
    except urllib.error.HTTPError as exc:
        if exc.code in {204, 408}:
            return None
        _raise_simplon_failure(exc, base, "Failed to fetch SIMPLON monitor image")
    except Exception as exc:
        _raise_simplon_failure(exc, base, "Failed to fetch SIMPLON monitor image")


def simplon_fetch_pixel_mask(base_url: str, version: str) -> np.ndarray | None:
    base = simplon_detector_base(base_url, version)
    candidates = ("pixel_mask", "threshold/1/pixel_mask")
    last_error: Exception | None = None
    for key in candidates:
        try:
            with urllib.request.urlopen(f"{base}/config/{key}", timeout=5) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            last_error = exc
            continue
        value = payload.get("value")
        if not isinstance(value, dict) or "__darray__" not in value:
            last_error = ValueError("Invalid pixel mask response")
            continue
        data_b64 = value.get("data")
        dtype_str = value.get("type")
        shape = value.get("shape")
        if not data_b64 or not dtype_str or not shape:
            last_error = ValueError("Incomplete pixel mask response")
            continue
        try:
            raw = base64.b64decode(data_b64)
            dtype = np.dtype(dtype_str)
            arr = np.frombuffer(raw, dtype=dtype)
            height = int(shape[0])
            width = int(shape[1])
            arr = arr.reshape((height, width))
        except Exception as exc:
            last_error = exc
            continue
        return _normalize_image_array(arr)
    if last_error:
        _raise_simplon_failure(last_error, base, "Failed to fetch SIMPLON pixel mask")
    return None


def _simplon_get_json(url: str, timeout: float) -> Any:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _simplon_config_value(base: str, key: str, timeout: float) -> str:
    """Best-effort read of one SIMPLON config value as display text."""
    try:
        payload = _simplon_get_json(f"{base}/config/{key}", timeout)
    except Exception:
        return ""
    value = payload.get("value") if isinstance(payload, dict) else None
    if isinstance(value, (str, int, float)):
        return str(value).strip()
    return ""


def simplon_probe(url: str, version: str) -> dict[str, Any]:
    """Diagnose whether a SIMPLON monitor API answers at the given address.

    Always returns a payload rather than raising for a dead detector: a failed
    probe is a successful diagnosis. ``status`` is ``ok`` or ``error``, and on
    error ``code`` says which failure it was so the UI can name the fix (wrong
    port, unknown host, wrong API version). Only unusable input raises (400).
    """
    monitor_base = simplon_base(url, version)
    detector_base = simplon_detector_base(url, version)
    base = normalize_simplon_base_url(url)
    api_version = monitor_base.rsplit("/", 1)[-1]
    result: dict[str, Any] = {
        "url": base,
        "api_version": api_version,
        "timeout_s": _PROBE_TIMEOUT_S,
    }

    # The monitor API is what live polling needs, so probe that rather than
    # settling for "the host answers something".
    try:
        _simplon_get_json(f"{monitor_base}/config/mode", _PROBE_TIMEOUT_S)
    except Exception as exc:
        return {"status": "error", **result, **classify_simplon_failure(exc, base)}

    detector = _simplon_config_value(detector_base, "description", _PROBE_TIMEOUT_S)
    serial = _simplon_config_value(detector_base, "detector_number", _PROBE_TIMEOUT_S)
    return {
        "status": "ok",
        "code": "ok",
        **result,
        "detector": detector,
        "serial": serial,
        "message": f"SIMPLON monitor API {api_version} answered at {base}",
    }
