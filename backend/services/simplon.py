from __future__ import annotations

"""SIMPLON monitor client helpers."""

import base64
import json
import urllib.error
import urllib.parse
import urllib.request

import numpy as np
from fastapi import HTTPException

from ..image_formats import _normalize_image_array


def simplon_base(url: str, version: str) -> str:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise HTTPException(status_code=400, detail="Invalid SIMPLON base URL")
    base = url.rstrip("/")
    ver = (version or "1.8.0").strip().strip("/")
    if not ver:
        ver = "1.8.0"
    return f"{base}/monitor/api/{ver}"


def simplon_detector_base(url: str, version: str) -> str:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise HTTPException(status_code=400, detail="Invalid SIMPLON base URL")
    base = url.rstrip("/")
    ver = (version or "1.8.0").strip().strip("/")
    if not ver:
        ver = "1.8.0"
    return f"{base}/detector/api/{ver}"


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
