"""SIMPLON monitor client helpers."""

from __future__ import annotations

import base64
import json
import re
import urllib.error
import urllib.parse
import urllib.request

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
    if api_path:
        rest = rest[: api_path.start()]
    else:
        rest = _TRAILING_API_ROOT_RE.sub("", rest)

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
        raise HTTPException(
            status_code=502, detail="Failed to update SIMPLON monitor mode"
        ) from exc


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
        raise HTTPException(status_code=502, detail=f"SIMPLON monitor error {exc.code}") from exc
    except Exception as exc:
        raise HTTPException(
            status_code=502, detail="Failed to fetch SIMPLON monitor image"
        ) from exc


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
        raise HTTPException(
            status_code=502, detail="Failed to fetch SIMPLON pixel mask"
        ) from last_error
    return None
