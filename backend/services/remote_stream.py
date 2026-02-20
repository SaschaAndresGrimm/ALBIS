from __future__ import annotations

"""Remote stream helper logic.

This module owns parsing and in-memory frame buffering for /api/remote/v1/*.
"""

import json
import math
import re
import threading
import time
from pathlib import Path
from typing import Any

import numpy as np
from fastapi import HTTPException

from ..image_formats import (
    _as_int,
    _first_number,
    _image_ext_name,
    _normalize_image_array,
    _read_cbf,
    _read_cbf_gz,
    _read_edf,
    _read_tiff_bytes,
)

_remote_frames: dict[str, dict[str, Any]] = {}
_remote_frames_lock = threading.Lock()
_REMOTE_SOURCES_MAX = 64


def remote_safe_source_id(source_id: str | None) -> str:
    source = (source_id or "default").strip()
    if not source:
        source = "default"
    source = source[:64]
    if not re.fullmatch(r"[A-Za-z0-9_.:-]+", source):
        raise HTTPException(status_code=400, detail="Invalid source_id")
    return source


def remote_parse_meta(meta_raw: str | None) -> dict[str, Any]:
    if not meta_raw:
        return {}
    try:
        payload = json.loads(meta_raw)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON in meta") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Meta payload must be a JSON object")
    return payload


def remote_parse_shape(value: Any) -> tuple[int, ...] | None:
    dims: list[int] = []
    if isinstance(value, str):
        tokens = [token.strip() for token in value.split(",") if token.strip()]
        for token in tokens:
            if not token.isdigit():
                return None
            dims.append(int(token))
    elif isinstance(value, (list, tuple)):
        for item in value:
            try:
                dims.append(int(item))
            except (TypeError, ValueError):
                return None
    else:
        return None
    if not dims:
        return None
    if any(d <= 0 for d in dims):
        return None
    return tuple(dims)


def remote_read_image_bytes(
    raw: bytes, *, meta: dict[str, Any], filename: str | None
) -> np.ndarray:
    fmt = str(meta.get("format") or "").strip().lower()
    if not fmt and filename:
        fmt = _image_ext_name(filename).lower()
    if fmt in {"raw", "array", "binary", ""}:
        dtype_raw = meta.get("dtype") or meta.get("type")
        if not dtype_raw:
            raise HTTPException(status_code=400, detail="Remote raw image requires metadata dtype")
        shape = remote_parse_shape(meta.get("shape"))
        if not shape:
            raise HTTPException(status_code=400, detail="Remote raw image requires metadata shape")
        try:
            dtype = np.dtype(str(dtype_raw))
        except Exception as exc:
            raise HTTPException(status_code=400, detail="Invalid remote dtype") from exc
        expected = int(np.prod(shape)) * dtype.itemsize
        if expected != len(raw):
            raise HTTPException(
                status_code=400,
                detail=f"Remote raw image size mismatch (expected {expected} bytes, got {len(raw)})",
            )
        arr = np.frombuffer(raw, dtype=dtype).reshape(shape)
        return _normalize_image_array(np.asarray(arr))

    if fmt in {".tif", ".tiff", "tif", "tiff"}:
        return _read_tiff_bytes(raw)

    suffix_map = {
        ".cbf": ".cbf",
        "cbf": ".cbf",
        ".cbf.gz": ".cbf.gz",
        "cbf.gz": ".cbf.gz",
        ".edf": ".edf",
        "edf": ".edf",
    }
    if fmt in suffix_map:
        suffix = suffix_map[fmt]
        import tempfile

        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp.write(raw)
            tmp_path = Path(tmp.name)
        try:
            if suffix == ".cbf":
                return _read_cbf(tmp_path)
            if suffix == ".cbf.gz":
                return _read_cbf_gz(tmp_path)
            return _read_edf(tmp_path)
        finally:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass

    raise HTTPException(status_code=400, detail=f"Unsupported remote image format: {fmt}")


def _remote_pick_float(meta: dict[str, Any], key: str) -> float | None:
    value = meta.get(key)
    if isinstance(value, dict):
        return None
    return _first_number(value)


def _remote_parse_peak_sets(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    cleaned_sets: list[dict[str, Any]] = []
    for idx, item in enumerate(value[:32]):
        if not isinstance(item, dict):
            continue
        raw_points = item.get("points")
        if not isinstance(raw_points, list):
            continue
        points: list[list[float]] = []
        for point in raw_points[:10000]:
            if not isinstance(point, (list, tuple)) or len(point) < 2:
                continue
            x = _first_number(point[0])
            y = _first_number(point[1])
            if x is None or y is None or not math.isfinite(x) or not math.isfinite(y):
                continue
            intensity = _first_number(point[2]) if len(point) >= 3 else None
            row = [float(x), float(y)]
            if intensity is not None and math.isfinite(intensity):
                row.append(float(intensity))
            points.append(row)
        if not points:
            continue
        name = str(item.get("name") or f"Set {idx + 1}")[:64]
        color = str(item.get("color") or "#4aa3ff").strip()
        if not re.fullmatch(r"#?[0-9A-Fa-f]{6}", color):
            color = "#4aa3ff"
        if not color.startswith("#"):
            color = f"#{color}"
        cleaned_sets.append({"name": name, "color": color, "points": points})
    return cleaned_sets


def remote_extract_metadata(meta: dict[str, Any]) -> dict[str, Any]:
    display = meta.get("display")
    if not isinstance(display, dict):
        display = {}
    resolution = meta.get("resolution")
    if not isinstance(resolution, dict):
        resolution = {}

    distance_mm = _first_number(resolution.get("distance_mm"))
    if distance_mm is None:
        distance_mm = _remote_pick_float(meta, "distance_mm")
    pixel_size_um = _first_number(resolution.get("pixel_size_um"))
    if pixel_size_um is None:
        pixel_size_um = _remote_pick_float(meta, "pixel_size_um")
    energy_ev = _first_number(resolution.get("energy_ev"))
    if energy_ev is None:
        energy_ev = _remote_pick_float(meta, "energy_ev")
    wavelength_a = _first_number(resolution.get("wavelength_a"))
    if wavelength_a is None:
        wavelength_a = _remote_pick_float(meta, "wavelength_a")

    beam_center = resolution.get("beam_center_px")
    center_x = center_y = None
    if isinstance(beam_center, (list, tuple)) and len(beam_center) >= 2:
        center_x = _first_number(beam_center[0])
        center_y = _first_number(beam_center[1])
    if center_x is None:
        center_x = _remote_pick_float(meta, "beam_center_x")
    if center_y is None:
        center_y = _remote_pick_float(meta, "beam_center_y")

    return {
        "display_name": str(meta.get("display_name") or "").strip(),
        "series_number": _as_int(
            display.get("series_number")
            if "series_number" in display
            else meta.get("series_number")
        ),
        "image_number": _as_int(
            display.get("image_number") if "image_number" in display else meta.get("image_number")
        ),
        "image_datetime": str(
            display.get("image_datetime")
            if display.get("image_datetime") is not None
            else meta.get("image_datetime") or ""
        ).strip(),
        "resolution": {
            "distance_mm": (
                distance_mm if distance_mm is not None and math.isfinite(distance_mm) else None
            ),
            "pixel_size_um": (
                pixel_size_um
                if pixel_size_um is not None and math.isfinite(pixel_size_um)
                else None
            ),
            "energy_ev": energy_ev if energy_ev is not None and math.isfinite(energy_ev) else None,
            "wavelength_a": (
                wavelength_a if wavelength_a is not None and math.isfinite(wavelength_a) else None
            ),
            "beam_center_px": (
                [center_x, center_y]
                if center_x is not None
                and center_y is not None
                and math.isfinite(center_x)
                and math.isfinite(center_y)
                else None
            ),
        },
        "peak_sets": _remote_parse_peak_sets(meta.get("peak_sets")),
        "extra": meta.get("extra") if isinstance(meta.get("extra"), dict) else {},
    }


def remote_store_frame(
    *, source_id: str, frame: np.ndarray, meta: dict[str, Any], seq: int | None
) -> int:
    now = time.time()
    with _remote_frames_lock:
        previous = _remote_frames.get(source_id)
        next_seq = (
            int(seq) if seq is not None else int(previous.get("seq", 0) + 1 if previous else 1)
        )
        _remote_frames[source_id] = {
            "source_id": source_id,
            "seq": next_seq,
            "updated_at": now,
            "dtype": frame.dtype.str,
            "shape": tuple(int(v) for v in frame.shape),
            "bytes": frame.tobytes(order="C"),
            "meta": meta,
        }
        if len(_remote_frames) > _REMOTE_SOURCES_MAX:
            oldest = sorted(
                _remote_frames.items(),
                key=lambda item: float(item[1].get("updated_at", 0.0)),
            )
            for old_source, _entry in oldest[: len(_remote_frames) - _REMOTE_SOURCES_MAX]:
                _remote_frames.pop(old_source, None)
        return next_seq


def remote_snapshot(source_id: str) -> dict[str, Any] | None:
    with _remote_frames_lock:
        frame = _remote_frames.get(source_id)
        if not frame:
            return None
        return dict(frame)
