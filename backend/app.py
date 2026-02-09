from __future__ import annotations

import base64
import csv
import math
import fnmatch
import io
import json
import os
import platform
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import logging
from logging.handlers import RotatingFileHandler
import tempfile
import hdf5plugin  # noqa: F401
import h5py
import numpy as np
import tifffile
import fabio
from fastapi import Body, FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
import shutil

DATA_DIR = Path(os.environ.get("VIEWER_DATA_DIR", "")).expanduser()
if not DATA_DIR:
    DATA_DIR = Path(__file__).resolve().parents[1]

ALBIS_VERSION = "0.1"

app = FastAPI(title="ALBIS — ALBIS WEB VIEW", version=ALBIS_VERSION)

AUTOLOAD_EXTS = {".h5", ".hdf5", ".tif", ".tiff", ".cbf"}
ALLOW_ABS_PATHS = os.environ.get("VIEWER_ALLOW_ABS", "1").lower() in {"1", "true", "yes", "on"}
SCAN_CACHE_SEC = float(os.environ.get("ALBIS_SCAN_CACHE_SEC", "2.0") or 0.0)
MAX_SCAN_DEPTH = int(os.environ.get("ALBIS_MAX_SCAN_DEPTH", "-1") or -1)
MAX_UPLOAD_MB = int(os.environ.get("ALBIS_MAX_UPLOAD_MB", "0") or 0)
MAX_UPLOAD_BYTES = MAX_UPLOAD_MB * 1024 * 1024 if MAX_UPLOAD_MB > 0 else 0
_files_cache: tuple[float, list[str]] = (0.0, [])
_folders_cache: tuple[float, list[str]] = (0.0, [])
LOG_DIR: Path | None = None
LOG_PATH: Path | None = None


def _init_logging() -> logging.Logger:
    global LOG_DIR, LOG_PATH
    level_name = os.environ.get("ALBIS_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logger = logging.getLogger("albis")
    if logger.handlers:
        return logger
    logger.setLevel(level)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    logger.addHandler(stream)

    log_dir_env = os.environ.get("ALBIS_LOG_DIR", "").strip()
    log_dir = Path(log_dir_env).expanduser() if log_dir_env else (DATA_DIR / "logs")
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        log_dir = Path(tempfile.gettempdir()) / "albis-logs"
        log_dir.mkdir(parents=True, exist_ok=True)
    LOG_DIR = log_dir
    LOG_PATH = log_dir / "albis.log"
    try:
        file_handler = RotatingFileHandler(
            LOG_PATH,
            maxBytes=5 * 1024 * 1024,
            backupCount=5,
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except OSError:
        logger.warning("Failed to initialize file logging in %s", log_dir)
    return logger


logger = _init_logging()
logger.info("ALBIS data dir: %s", DATA_DIR)


@app.middleware("http")
async def log_requests(request, call_next):
    start = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        logger.exception("Unhandled error: %s %s", request.method, request.url.path)
        raise
    duration_ms = (time.perf_counter() - start) * 1000
    status = response.status_code
    if status >= 500:
        logger.error("%s %s -> %s (%.1fms)", request.method, request.url.path, status, duration_ms)
    elif status >= 400:
        logger.warning("%s %s -> %s (%.1fms)", request.method, request.url.path, status, duration_ms)
    else:
        if duration_ms >= 1000:
            logger.info("%s %s -> %s (%.1fms)", request.method, request.url.path, status, duration_ms)
        else:
            logger.debug("%s %s -> %s (%.1fms)", request.method, request.url.path, status, duration_ms)
    return response


def _safe_rel_path(name: str) -> Path:
    if not name:
        raise HTTPException(status_code=400, detail="Invalid file name")
    if name.startswith(("/", "\\")):
        raise HTTPException(status_code=400, detail="Invalid file name")
    raw = Path(name)
    if raw.is_absolute():
        raise HTTPException(status_code=400, detail="Invalid file name")
    if any(part == ".." or part.startswith(".") for part in raw.parts):
        raise HTTPException(status_code=400, detail="Invalid file name")
    return raw


def _resolve_file(name: str) -> Path:
    raw = Path(name)
    if raw.is_absolute():
        if not ALLOW_ABS_PATHS:
            raise HTTPException(status_code=400, detail="Absolute paths are disabled")
        path = raw.expanduser().resolve()
        if not path.exists() or path.suffix.lower() not in {".h5", ".hdf5"}:
            raise HTTPException(status_code=404, detail="File not found")
        return path
    safe = _safe_rel_path(name)
    path = (DATA_DIR / safe).resolve()
    if not _is_within(path, DATA_DIR.resolve()):
        raise HTTPException(status_code=400, detail="Invalid file name")
    if not path.exists() or path.suffix.lower() not in {".h5", ".hdf5"}:
        raise HTTPException(status_code=404, detail="File not found")
    return path


def _resolve_dir(name: str | None) -> Path:
    if name is None:
        return DATA_DIR.resolve()
    trimmed = name.strip()
    if trimmed in ("", ".", "./"):
        return DATA_DIR.resolve()
    raw = Path(trimmed)
    if raw.is_absolute():
        if not ALLOW_ABS_PATHS:
            raise HTTPException(status_code=400, detail="Absolute paths are disabled")
        path = raw.expanduser().resolve()
        if not path.exists() or not path.is_dir():
            raise HTTPException(status_code=404, detail="Directory not found")
        return path
    safe = _safe_rel_path(trimmed)
    path = (DATA_DIR / safe).resolve()
    if not _is_within(path, DATA_DIR.resolve()):
        raise HTTPException(status_code=400, detail="Invalid directory")
    if not path.exists() or not path.is_dir():
        raise HTTPException(status_code=404, detail="Directory not found")
    return path


def _resolve_image_file(name: str) -> Path:
    raw = Path(name)
    if raw.is_absolute():
        if not ALLOW_ABS_PATHS:
            raise HTTPException(status_code=400, detail="Absolute paths are disabled")
        path = raw.expanduser().resolve()
        if not path.exists() or path.suffix.lower() not in AUTOLOAD_EXTS:
            raise HTTPException(status_code=404, detail="File not found")
        return path
    safe = _safe_rel_path(name)
    path = (DATA_DIR / safe).resolve()
    if not _is_within(path, DATA_DIR.resolve()):
        raise HTTPException(status_code=400, detail="Invalid file name")
    if not path.exists() or path.suffix.lower() not in AUTOLOAD_EXTS:
        raise HTTPException(status_code=404, detail="File not found")
    return path


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _normalize_image_array(arr: np.ndarray, index: int = 0) -> np.ndarray:
    if arr.ndim == 2:
        frame = arr
    elif arr.ndim == 3:
        if arr.shape[-1] in (3, 4):
            frame = arr[..., 0]
        else:
            idx = max(0, min(index, arr.shape[0] - 1))
            frame = arr[idx]
    else:
        raise HTTPException(status_code=400, detail="Unsupported image shape")
    frame = np.ascontiguousarray(frame)
    if frame.dtype.byteorder == ">" or (frame.dtype.byteorder == "=" and sys.byteorder == "big"):
        frame = frame.byteswap().newbyteorder("<")
    return frame


def _read_tiff(path: Path, index: int = 0) -> np.ndarray:
    arr = tifffile.imread(path)
    return _normalize_image_array(np.asarray(arr), index=index)


def _read_cbf(path: Path) -> np.ndarray:
    image = fabio.open(str(path))
    arr = np.asarray(image.data)
    return _normalize_image_array(arr)


def _parse_ext_filter(exts: str | None) -> set[str]:
    if not exts:
        return set(AUTOLOAD_EXTS)
    cleaned: set[str] = set()
    for raw in exts.split(","):
        token = raw.strip().lower()
        if not token:
            continue
        if not token.startswith("."):
            token = f".{token}"
        cleaned.add(token)
    allowed = cleaned.intersection(AUTOLOAD_EXTS)
    return allowed or set(AUTOLOAD_EXTS)


def _iter_entries(root: Path, max_depth: int | None):
    stack: list[tuple[Path, int]] = [(root, 0)]
    while stack:
        base, depth = stack.pop()
        try:
            with os.scandir(base) as it:
                for entry in it:
                    name = entry.name
                    if name.startswith("."):
                        continue
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            if max_depth is None or depth < max_depth:
                                stack.append((Path(entry.path), depth + 1))
                        elif entry.is_file(follow_symlinks=False):
                            yield entry.path, name
                    except OSError:
                        continue
        except OSError:
            continue


def _scan_files(root: Path) -> list[str]:
    items: list[str] = []
    max_depth = None if MAX_SCAN_DEPTH < 0 else MAX_SCAN_DEPTH
    root = root.resolve()
    for path_str, name in _iter_entries(root, max_depth):
        ext = os.path.splitext(name)[1].lower()
        if ext not in {".h5", ".hdf5"}:
            continue
        try:
            rel = os.path.relpath(path_str, root)
        except ValueError:
            continue
        if rel.startswith(".."):
            continue
        items.append(rel.replace(os.sep, "/"))
    return sorted(set(items))


def _scan_folders(root: Path) -> list[str]:
    dirs: set[str] = set()
    max_depth = None if MAX_SCAN_DEPTH < 0 else MAX_SCAN_DEPTH
    stack: list[tuple[Path, int]] = [(root.resolve(), 0)]
    while stack:
        base, depth = stack.pop()
        try:
            with os.scandir(base) as it:
                for entry in it:
                    name = entry.name
                    if name.startswith("."):
                        continue
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            if max_depth is None or depth < max_depth:
                                stack.append((Path(entry.path), depth + 1))
                            try:
                                rel = os.path.relpath(entry.path, root)
                            except ValueError:
                                continue
                            if not rel or rel.startswith(".."):
                                continue
                            dirs.add(rel.replace(os.sep, "/"))
                    except OSError:
                        continue
        except OSError:
            continue
    return sorted(dirs)


def _latest_image_file(root: Path, allowed_exts: set[str], pattern: str | None) -> Path | None:
    latest_path: Path | None = None
    latest_mtime = -1.0
    max_depth = None if MAX_SCAN_DEPTH < 0 else MAX_SCAN_DEPTH
    root = root.resolve()
    pattern_norm = (pattern or "").strip()
    needs_rel = "/" in pattern_norm or "\\" in pattern_norm
    for path_str, name in _iter_entries(root, max_depth):
        ext = os.path.splitext(name)[1].lower()
        if ext not in allowed_exts:
            continue
        if pattern_norm:
            if needs_rel:
                try:
                    rel = os.path.relpath(path_str, root).replace(os.sep, "/")
                except ValueError:
                    continue
                target = rel
            else:
                target = name
            if not fnmatch.fnmatch(target, pattern_norm):
                continue
        try:
            mtime = os.stat(path_str).st_mtime
        except OSError:
            continue
        if mtime > latest_mtime:
            latest_mtime = mtime
            latest_path = Path(path_str)
    return latest_path


def _simplon_base(url: str, version: str) -> str:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise HTTPException(status_code=400, detail="Invalid SIMPLON base URL")
    base = url.rstrip("/")
    ver = (version or "1.8.0").strip().strip("/")
    if not ver:
        ver = "1.8.0"
    return f"{base}/monitor/api/{ver}"


def _simplon_detector_base(url: str, version: str) -> str:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise HTTPException(status_code=400, detail="Invalid SIMPLON base URL")
    base = url.rstrip("/")
    ver = (version or "1.8.0").strip().strip("/")
    if not ver:
        ver = "1.8.0"
    return f"{base}/detector/api/{ver}"


def _simplon_set_mode(base: str, mode: str) -> None:
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
        raise HTTPException(status_code=502, detail="Failed to update SIMPLON monitor mode") from exc


def _simplon_fetch_monitor(base: str, timeout_ms: int) -> bytes | None:
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
        raise HTTPException(status_code=502, detail="Failed to fetch SIMPLON monitor image") from exc


def _simplon_fetch_pixel_mask(base_url: str, version: str) -> np.ndarray | None:
    base = _simplon_detector_base(base_url, version)
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
        raise HTTPException(status_code=502, detail="Failed to fetch SIMPLON pixel mask") from last_error
    return None


def _resolve_external_path(base_file: Path, filename: str | None) -> Path | None:
    if not filename:
        return None
    target = Path(str(filename))
    if not target.is_absolute():
        target = base_file.parent / target
    target = target.expanduser().resolve()
    if not ALLOW_ABS_PATHS:
        allowed_root = DATA_DIR.resolve() if DATA_DIR else base_file.parent.resolve()
        if allowed_root and not _is_within(target, allowed_root):
            return None
    if not target.exists() or target.suffix.lower() not in {".h5", ".hdf5"}:
        return None
    return target


def _dataset_info(name: str, obj: Any) -> dict[str, Any] | None:
    if not isinstance(obj, h5py.Dataset):
        return None
    shape = tuple(int(x) for x in obj.shape)
    dtype = str(obj.dtype)
    ndim = obj.ndim
    size = int(np.prod(shape)) if shape else 0
    return {
        "path": f"/{name}" if not name.startswith("/") else name,
        "shape": shape,
        "dtype": dtype,
        "ndim": ndim,
        "size": size,
        "chunks": obj.chunks,
        "maxshape": obj.maxshape,
    }


def _is_image_dataset(info: dict[str, Any]) -> bool:
    if info["ndim"] not in (2, 3, 4):
        return False
    dtype = info["dtype"]
    return any(token in dtype for token in ("int", "uint", "float"))


MASK_PATHS = (
    "/entry/instrument/detector/detectorSpecific/pixel_mask",
    "/entry/instrument/detector/pixel_mask",
    "/entry/instrument/detector/detectorSpecific/bad_pixel_mask",
    "/entry/instrument/detector/bad_pixel_mask",
    "/entry/instrument/detector/pixel_mask_applied",
    "/entry/instrument/detector/detectorSpecific/pixel_mask_applied",
)


def _serialize_h5_value(value: Any) -> Any:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception:
            return value.decode("utf-8", "replace")
    if isinstance(value, np.generic):
        try:
            return value.item()
        except Exception:
            return str(value)
    if isinstance(value, np.ndarray):
        if value.size == 1:
            return _serialize_h5_value(value.reshape(-1)[0])
        if value.dtype.kind == "S":
            try:
                return [v.decode("utf-8", "replace") for v in value.reshape(-1)[:16]]
            except Exception:
                return value.reshape(-1)[:16].tolist()
        if value.size <= 16:
            return value.tolist()
        return {
            "shape": value.shape,
            "dtype": str(value.dtype),
            "preview": value.reshape(-1)[:16].tolist(),
            "truncated": True,
        }
    if isinstance(value, (list, tuple)):
        return [_serialize_h5_value(v) for v in value]
    return value


def _collect_h5_attrs(obj: Any) -> list[dict[str, Any]]:
    attrs: list[dict[str, Any]] = []
    try:
        for key in obj.attrs.keys():
            try:
                attrs.append({"name": str(key), "value": _serialize_h5_value(obj.attrs[key])})
            except Exception:
                attrs.append({"name": str(key), "value": "<unreadable>"})
    except Exception:
        return []
    return attrs


def _array_preview_to_list(arr: np.ndarray) -> Any:
    if arr.ndim == 0:
        return _serialize_h5_value(arr.item())
    if arr.ndim == 1:
        return [_serialize_h5_value(v) for v in arr.tolist()]
    if arr.ndim == 2:
        return [[_serialize_h5_value(v) for v in row] for row in arr.tolist()]
    return _serialize_h5_value(arr)


def _dataset_value_preview(
    dset: h5py.Dataset,
    max_cells: int = 2048,
    max_rows: int = 128,
    max_cols: int = 128,
) -> tuple[Any | None, tuple[int, ...] | None, bool, dict[str, Any] | None]:
    shape = tuple(int(x) for x in dset.shape) if dset.shape else ()
    total = int(np.prod(shape)) if shape else 1
    if total <= 0:
        return None, None, False, None
    try:
        if dset.ndim == 0:
            value = np.asarray(dset[()])
            return _array_preview_to_list(value), shape, False, None
        if dset.ndim == 1:
            count = max(1, min(shape[0], max_cells))
            data = np.asarray(dset[:count])
            truncated = count < shape[0]
            return _array_preview_to_list(data), (count,), truncated, None
        rows = max(1, min(shape[-2], max_rows))
        cols = max(1, min(shape[-1], max_cols))
        if rows * cols > max_cells:
            scale = math.sqrt(max_cells / max(rows * cols, 1))
            rows = max(1, int(rows * scale))
            cols = max(1, int(cols * scale))
        lead = (0,) * max(0, dset.ndim - 2)
        data = np.asarray(dset[lead + (slice(0, rows), slice(0, cols))])
        preview_shape = tuple(int(x) for x in data.shape)
        truncated = rows < shape[-2] or cols < shape[-1] or dset.ndim > 2
        slice_info = {"lead": list(lead), "rows": rows, "cols": cols} if dset.ndim > 2 else None
        return _array_preview_to_list(data), preview_shape, truncated, slice_info
    except Exception:
        return None, None, False, None


def _dataset_preview_array(
    dset: h5py.Dataset,
    max_cells: int = 65536,
    max_rows: int = 1024,
    max_cols: int = 1024,
) -> tuple[np.ndarray | None, bool, dict[str, Any] | None]:
    shape = tuple(int(x) for x in dset.shape) if dset.shape else ()
    total = int(np.prod(shape)) if shape else 1
    if total <= 0:
        return None, False, None
    try:
        if dset.ndim == 0:
            return np.asarray(dset[()]), False, None
        if dset.ndim == 1:
            count = max(1, min(shape[0], max_cells))
            data = np.asarray(dset[:count])
            return data, count < shape[0], None
        rows = max(1, min(shape[-2], max_rows))
        cols = max(1, min(shape[-1], max_cols))
        if rows * cols > max_cells:
            scale = math.sqrt(max_cells / max(rows * cols, 1))
            rows = max(1, int(rows * scale))
            cols = max(1, int(cols * scale))
        lead = (0,) * max(0, dset.ndim - 2)
        data = np.asarray(dset[lead + (slice(0, rows), slice(0, cols))])
        truncated = rows < shape[-2] or cols < shape[-1] or dset.ndim > 2
        slice_info = {"lead": list(lead), "rows": rows, "cols": cols} if dset.ndim > 2 else None
        return data, truncated, slice_info
    except Exception:
        return None, False, None


def _find_pixel_mask(h5: h5py.File, threshold: int | None = None) -> h5py.Dataset | None:
    if threshold is not None:
        key = f"/entry/instrument/detector/threshold_{threshold + 1}_channel/pixel_mask"
        if key in h5:
            obj = h5[key]
            if isinstance(obj, h5py.Dataset) and obj.ndim == 2:
                return obj
    for path in MASK_PATHS:
        if path in h5:
            obj = h5[path]
            if isinstance(obj, h5py.Dataset) and obj.ndim == 2:
                return obj
    return None


def _coerce_scalar(value: Any) -> float | None:
    try:
        if isinstance(value, np.ndarray):
            if value.size == 0:
                return None
            value = value.reshape(-1)[0]
        if isinstance(value, np.generic):
            value = value.item()
        return float(value)
    except Exception:
        return None


def _get_units(obj: Any) -> str | None:
    try:
        units = obj.attrs.get("units") or obj.attrs.get("unit")
        if isinstance(units, bytes):
            return units.decode("utf-8", "replace")
        if isinstance(units, np.ndarray) and units.size == 1:
            units = units.reshape(-1)[0]
        if isinstance(units, np.generic):
            units = units.item()
        if isinstance(units, bytes):
            return units.decode("utf-8", "replace")
        return str(units) if units is not None else None
    except Exception:
        return None


def _read_scalar(h5: h5py.File, paths: list[str]) -> tuple[float | None, str | None]:
    for path in paths:
        if path in h5:
            obj = h5[path]
            if not isinstance(obj, h5py.Dataset):
                continue
            try:
                value = _coerce_scalar(obj[()])
            except Exception:
                value = None
            units = _get_units(obj)
            if value is not None:
                return value, units
    return None, None


def _norm_unit(unit: str | None) -> str:
    return (unit or "").strip().lower().replace("µ", "u")


def _to_mm(value: float, unit: str | None) -> float:
    u = _norm_unit(unit)
    if u in {"m", "meter", "metre", "meters", "metres"}:
        return value * 1000
    if u in {"cm", "centimeter", "centimetre", "centimeters", "centimetres"}:
        return value * 10
    if u in {"mm", "millimeter", "millimetre", "millimeters", "millimetres"}:
        return value
    if u in {"um", "micrometer", "micrometre", "micrometers", "micrometres"}:
        return value / 1000
    if u in {"nm", "nanometer", "nanometre", "nanometers", "nanometres"}:
        return value / 1e6
    if value < 0.5:
        return value * 1000
    return value


def _to_um(value: float, unit: str | None) -> float:
    u = _norm_unit(unit)
    if u in {"m", "meter", "metre", "meters", "metres"}:
        return value * 1e6
    if u in {"cm", "centimeter", "centimetre", "centimeters", "centimetres"}:
        return value * 1e4
    if u in {"mm", "millimeter", "millimetre", "millimeters", "millimetres"}:
        return value * 1000
    if u in {"um", "micrometer", "micrometre", "micrometers", "micrometres"}:
        return value
    if u in {"nm", "nanometer", "nanometre", "nanometers", "nanometres"}:
        return value / 1000
    if value < 1e-2:
        return value * 1e6
    if value < 1:
        return value * 1000
    return value


def _to_ev(value: float, unit: str | None) -> float:
    u = _norm_unit(unit)
    if u in {"kev", "kiloelectronvolt", "kiloelectronvolts"}:
        return value * 1000
    if u in {"ev", "electronvolt", "electronvolts"}:
        return value
    if value < 1000:
        return value * 1000
    return value


def _wavelength_to_ev(value: float, unit: str | None) -> float | None:
    u = _norm_unit(unit)
    wavelength_m = None
    if u in {"m", "meter", "metre", "meters", "metres"}:
        wavelength_m = value
    elif u in {"nm", "nanometer", "nanometre", "nanometers", "nanometres"}:
        wavelength_m = value * 1e-9
    elif u in {"um", "micrometer", "micrometre", "micrometers", "micrometres"}:
        wavelength_m = value * 1e-6
    elif u in {"a", "ang", "angstrom", "angstroms"}:
        wavelength_m = value * 1e-10
    elif value < 1e-6:
        wavelength_m = value
    if wavelength_m is None or wavelength_m <= 0:
        return None
    return 12398.4193 / (wavelength_m * 1e10)

def _read_threshold_energies(h5: h5py.File, count: int) -> list[float | None]:
    energies: list[float | None] = []
    for idx in range(count):
        energy = None
        key = f"/entry/instrument/detector/threshold_{idx + 1}_channel/threshold_energy"
        if key in h5:
            try:
                data = h5[key][()]
                arr = np.asarray(data)
                if arr.size:
                    energy = float(arr.reshape(-1)[0])
            except Exception:
                energy = None
        energies.append(energy)
    return energies


def _walk_datasets(
    obj: Any,
    base_path: str,
    file_path: Path,
    results: list[dict[str, Any]],
    ancestors: set[tuple[Path, bytes]],
    file_cache: dict[Path, h5py.File],
) -> None:
    if isinstance(obj, h5py.Dataset):
        info = _dataset_info(base_path, obj)
        if info:
            info["image"] = _is_image_dataset(info)
            results.append(info)
        return
    if not isinstance(obj, h5py.Group):
        return
    obj_ref = (file_path, obj.id)
    if obj_ref in ancestors:
        return
    next_ancestors = set(ancestors)
    next_ancestors.add(obj_ref)

    for name in obj.keys():
        try:
            link = obj.get(name, getlink=True)
        except Exception:
            continue
        child_path = f"{base_path}/{name}" if base_path != "/" else f"/{name}"
        if isinstance(link, h5py.ExternalLink):
            target_path = _resolve_external_path(file_path, link.filename)
            if not target_path:
                continue
            target_file = file_cache.get(target_path)
            if target_file is None:
                try:
                    target_file = h5py.File(target_path, "r")
                except OSError:
                    continue
                file_cache[target_path] = target_file
            try:
                target_obj = target_file[link.path]
            except Exception:
                continue
            _walk_datasets(target_obj, child_path, target_path, results, next_ancestors, file_cache)
            continue
        if isinstance(link, h5py.SoftLink):
            try:
                target_obj = obj[link.path]
            except Exception:
                continue
            _walk_datasets(target_obj, child_path, file_path, results, next_ancestors, file_cache)
            continue
        try:
            target_obj = obj[name]
        except Exception:
            continue
        _walk_datasets(target_obj, child_path, file_path, results, next_ancestors, file_cache)


def _resolve_dataset(
    h5: h5py.File, base_file: Path, dataset: str
) -> tuple[h5py.Dataset, list[h5py.File]]:
    parts = [p for p in dataset.strip("/").split("/") if p]
    if not parts:
        raise KeyError("Dataset not found")
    current: Any = h5["/"]
    current_file = base_file
    opened: list[h5py.File] = []

    for idx, part in enumerate(parts):
        if not isinstance(current, h5py.Group):
            raise KeyError("Dataset not found")
        link = current.get(part, getlink=True)
        if link is None:
            raise KeyError("Dataset not found")
        if isinstance(link, h5py.ExternalLink):
            target_path = _resolve_external_path(current_file, link.filename)
            if not target_path:
                raise KeyError("Dataset not found")
            try:
                target_file = h5py.File(target_path, "r")
            except OSError as exc:
                raise KeyError("Dataset not found") from exc
            opened.append(target_file)
            current_file = target_path
            try:
                current = target_file[link.path]
            except Exception as exc:
                raise KeyError("Dataset not found") from exc
        elif isinstance(link, h5py.SoftLink):
            try:
                current = current[link.path]
            except Exception as exc:
                raise KeyError("Dataset not found") from exc
        else:
            try:
                current = current[part]
            except Exception as exc:
                raise KeyError("Dataset not found") from exc

        if idx < len(parts) - 1 and isinstance(current, h5py.Dataset):
            raise KeyError("Dataset not found")

    if not isinstance(current, h5py.Dataset):
        raise KeyError("Dataset not found")
    return current, opened


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok", "version": ALBIS_VERSION}


@app.post("/api/client-log")
def client_log(payload: dict[str, Any] = Body(...)) -> dict[str, str]:
    try:
        level = str(payload.get("level", "info")).lower()
        message = str(payload.get("message", "")).strip()
        context = payload.get("context")
        meta = {
            "url": payload.get("url"),
            "userAgent": payload.get("userAgent"),
            "extra": payload.get("extra"),
        }
        if not message:
            return {"status": "ignored"}
        if len(message) > 2000:
            message = message[:2000] + "…"
        if isinstance(context, str) and len(context) > 4000:
            context = context[:4000] + "…"
        try:
            meta_json = json.dumps(meta, default=str)
        except Exception:
            meta_json = "{}"
        if isinstance(context, (dict, list)):
            try:
                context = json.dumps(context, default=str)
            except Exception:
                context = str(context)
        level_map = {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warning": logging.WARNING,
            "error": logging.ERROR,
            "critical": logging.CRITICAL,
        }
        log_level = level_map.get(level, logging.INFO)
        if context:
            logger.log(log_level, "CLIENT %s | %s | %s", message, context, meta_json)
        else:
            logger.log(log_level, "CLIENT %s | %s", message, meta_json)
        return {"status": "ok"}
    except Exception as exc:
        logger.exception("Failed to record client log: %s", exc)
        raise HTTPException(status_code=400, detail="Invalid log payload")


@app.post("/api/open-log")
def open_log() -> dict[str, str]:
    if LOG_PATH is None:
        raise HTTPException(status_code=500, detail="Log file unavailable")
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        LOG_PATH.touch(exist_ok=True)
    except OSError as exc:
        raise HTTPException(status_code=500, detail="Failed to access log file") from exc

    system = platform.system()
    try:
        if system == "Windows":
            os.startfile(str(LOG_PATH))  # type: ignore[attr-defined]
        elif system == "Darwin":
            subprocess.run(["open", str(LOG_PATH)], check=False)
        else:
            subprocess.run(["xdg-open", str(LOG_PATH)], check=False)
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Failed to open log file") from exc
    return {"status": "ok", "path": str(LOG_PATH)}


def _prefix_paths(root: Path, items: list[str]) -> list[str]:
    root = root.resolve()
    data_root = DATA_DIR.resolve()
    try:
        rel_root = root.relative_to(data_root)
        prefix = rel_root.as_posix()
    except ValueError:
        return [str((root / Path(item)).resolve()) for item in items]
    if prefix in ("", "."):
        return items
    return [f"{prefix}/{item}" for item in items]


@app.get("/api/files")
def files(folder: str | None = Query(None)) -> dict[str, list[str]]:
    global _files_cache
    trimmed = (folder or "").strip()
    use_cache = trimmed in ("", ".", "./")
    if use_cache:
        now = time.monotonic()
        if SCAN_CACHE_SEC > 0 and now - _files_cache[0] < SCAN_CACHE_SEC:
            return {"files": _files_cache[1]}
        items = _scan_files(DATA_DIR)
        _files_cache = (now, items)
        return {"files": items}
    root = _resolve_dir(trimmed)
    items = _scan_files(root)
    return {"files": _prefix_paths(root, items)}


@app.get("/api/folders")
def folders() -> dict[str, list[str]]:
    global _folders_cache
    now = time.monotonic()
    if SCAN_CACHE_SEC > 0 and now - _folders_cache[0] < SCAN_CACHE_SEC:
        return {"folders": _folders_cache[1]}
    items = _scan_folders(DATA_DIR)
    _folders_cache = (now, items)
    return {"folders": items}


@app.get("/api/choose-folder")
def choose_folder() -> Response:
    if not ALLOW_ABS_PATHS:
        raise HTTPException(status_code=403, detail="Absolute paths are disabled")
    system = platform.system()
    logger.debug("Folder picker requested (os=%s)", system)
    if system == "Darwin":
        script = 'POSIX path of (choose folder with prompt "Select Auto Load folder")'
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").lower()
            if "user canceled" in stderr:
                return Response(status_code=204)
            raise HTTPException(status_code=500, detail="Folder picker failed") from exc
        path = result.stdout.strip()
        if not path:
            return Response(status_code=204)
        return JSONResponse({"path": path})

    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Folder picker unavailable") from exc

    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    try:
        path = filedialog.askdirectory(title="Select Auto Load folder")
    finally:
        root.destroy()

    if not path:
        return Response(status_code=204)
    logger.info("Folder picker selected: %s", path)
    return JSONResponse({"path": path})


@app.get("/api/choose-file")
def choose_file() -> Response:
    if not ALLOW_ABS_PATHS:
        raise HTTPException(status_code=403, detail="Absolute paths are disabled")
    system = platform.system()
    logger.debug("File picker requested (os=%s)", system)
    if system == "Darwin":
        script = 'POSIX path of (choose file with prompt "Select HDF5 file")'
        try:
            result = subprocess.run(
                ["osascript", "-e", script],
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").lower()
            if "user canceled" in stderr:
                return Response(status_code=204)
            raise HTTPException(status_code=500, detail="File picker failed") from exc
        path = result.stdout.strip()
        if not path:
            return Response(status_code=204)
    else:
        try:
            import tkinter as tk
            from tkinter import filedialog
        except Exception as exc:
            raise HTTPException(status_code=500, detail="File picker unavailable") from exc

        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        try:
            path = filedialog.askopenfilename(
                title="Select HDF5 file",
                filetypes=[("HDF5 files", "*.h5 *.hdf5"), ("All files", "*.*")],
            )
        finally:
            root.destroy()

        if not path:
            return Response(status_code=204)
    picked = Path(path).expanduser().resolve()
    if picked.suffix.lower() not in {".h5", ".hdf5"}:
        raise HTTPException(status_code=400, detail="Unsupported file type")
    if not picked.exists():
        raise HTTPException(status_code=404, detail="File not found")
    logger.info("File picker selected: %s", picked)
    return JSONResponse({"path": str(picked)})


@app.get("/api/autoload/latest")
def autoload_latest(
    folder: str | None = Query(None),
    exts: str | None = Query(None),
    pattern: str | None = Query(None),
) -> Response:
    root = _resolve_dir(folder)
    allowed = _parse_ext_filter(exts)
    latest = _latest_image_file(root, allowed, pattern)
    if not latest:
        logger.debug("Autoload scan: no file found (folder=%s pattern=%s)", root, pattern or "")
        return Response(status_code=204)
    try:
        rel = latest.resolve().relative_to(DATA_DIR.resolve()).as_posix()
        absolute = False
        file_label = rel
    except ValueError:
        if not ALLOW_ABS_PATHS:
            raise HTTPException(status_code=400, detail="Invalid file location")
        absolute = True
        file_label = str(latest.resolve())
    logger.debug("Autoload scan: latest=%s absolute=%s", file_label, absolute)
    return JSONResponse(
        {
            "file": file_label,
            "ext": latest.suffix.lower(),
            "mtime": latest.stat().st_mtime,
            "absolute": absolute,
        }
    )


@app.get("/api/image")
def image(
    file: str = Query(..., min_length=1),
    index: int = Query(0, ge=0),
) -> Response:
    path = _resolve_image_file(file)
    ext = path.suffix.lower()
    if ext in {".h5", ".hdf5"}:
        raise HTTPException(status_code=400, detail="Use /api/frame for HDF5 datasets")
    if ext in {".tif", ".tiff"}:
        arr = _read_tiff(path, index=index)
    elif ext == ".cbf":
        arr = _read_cbf(path)
    else:
        raise HTTPException(status_code=400, detail="Unsupported image format")

    data = arr.tobytes(order="C")
    headers = {
        "X-Dtype": arr.dtype.str,
        "X-Shape": ",".join(str(x) for x in arr.shape),
        "X-Frame": "0",
    }
    return Response(content=data, media_type="application/octet-stream", headers=headers)


@app.get("/api/simplon/monitor")
def simplon_monitor(
    url: str = Query(..., min_length=4),
    version: str = Query("1.8.0"),
    timeout: int = Query(500, ge=0),
    enable: bool = Query(True),
) -> Response:
    base = _simplon_base(url, version)
    if enable:
        _simplon_set_mode(base, "enabled")
    data = _simplon_fetch_monitor(base, timeout)
    if data is None:
        logger.debug("SIMPLON monitor: no data (url=%s)", url)
        return Response(status_code=204)
    arr = _normalize_image_array(np.asarray(tifffile.imread(io.BytesIO(data))))
    data_bytes = arr.tobytes(order="C")
    headers = {
        "X-Dtype": arr.dtype.str,
        "X-Shape": ",".join(str(x) for x in arr.shape),
        "X-Frame": "0",
    }
    return Response(content=data_bytes, media_type="application/octet-stream", headers=headers)


@app.post("/api/simplon/mode")
def simplon_mode(
    url: str = Query(..., min_length=4),
    version: str = Query("1.8.0"),
    mode: str = Query("enabled"),
) -> dict[str, str]:
    mode_value = mode.lower()
    if mode_value not in {"enabled", "disabled"}:
        raise HTTPException(status_code=400, detail="Invalid monitor mode")
    base = _simplon_base(url, version)
    _simplon_set_mode(base, mode_value)
    logger.info("SIMPLON monitor mode: %s (url=%s)", mode_value, url)
    return {"status": "ok", "mode": mode_value}


@app.get("/api/simplon/mask")
def simplon_mask(
    url: str = Query(..., min_length=4),
    version: str = Query("1.8.0"),
) -> Response:
    arr = _simplon_fetch_pixel_mask(url, version)
    if arr is None:
        logger.debug("SIMPLON mask: not available (url=%s)", url)
        return Response(status_code=204)
    logger.info("SIMPLON mask fetched (url=%s)", url)
    data = arr.tobytes(order="C")
    headers = {
        "X-Dtype": arr.dtype.str,
        "X-Shape": ",".join(str(x) for x in arr.shape),
    }
    return Response(content=data, media_type="application/octet-stream", headers=headers)


@app.post("/api/upload")
async def upload(file: UploadFile = File(...), folder: str | None = Query(None)) -> dict[str, str]:
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename")
    safe_path = _safe_rel_path(Path(file.filename).name)
    safe = safe_path.as_posix()
    if not safe.lower().endswith((".h5", ".hdf5")):
        raise HTTPException(status_code=400, detail="Only .h5/.hdf5 files are supported")
    root = _resolve_dir(folder) if folder else DATA_DIR.resolve()
    dest = (root / safe).resolve()
    if not _is_within(dest, root):
        raise HTTPException(status_code=400, detail="Invalid file name")
    logger.info("Upload start: %s -> %s", safe, dest)
    written = 0
    chunk_size = 1024 * 1024 * 4
    try:
        with dest.open("wb") as fh:
            while True:
                chunk = file.file.read(chunk_size)
                if not chunk:
                    break
                written += len(chunk)
                if MAX_UPLOAD_BYTES and written > MAX_UPLOAD_BYTES:
                    raise HTTPException(status_code=413, detail="Upload too large")
                fh.write(chunk)
    except HTTPException:
        try:
            if dest.exists():
                dest.unlink()
        except OSError:
            pass
        raise
    logger.info("Upload complete: %s (%d bytes)", dest, written)
    return {"filename": safe}


@app.get("/api/datasets")
def datasets(file: str = Query(..., min_length=1)) -> dict[str, Any]:
    path = _resolve_file(file)
    results: list[dict[str, Any]] = []
    with h5py.File(path, "r") as h5:
        file_cache: dict[Path, h5py.File] = {path: h5}
        try:
            _walk_datasets(h5["/"], "/", path, results, set(), file_cache)
        finally:
            for cache_path, handle in file_cache.items():
                if cache_path == path:
                    continue
                try:
                    handle.close()
                except Exception:
                    pass
    return {"datasets": results}


@app.get("/api/hdf5/tree")
def hdf5_tree(file: str = Query(..., min_length=1), path: str = Query("/")) -> dict[str, Any]:
    file_path = _resolve_file(file)
    with h5py.File(file_path, "r") as h5:
        if path not in h5:
            raise HTTPException(status_code=404, detail="Path not found")
        obj = h5[path]
        if not isinstance(obj, h5py.Group):
            return {"path": path, "children": []}
        children: list[dict[str, Any]] = []
        for name in obj.keys():
            child_path = f"{path}/{name}" if path != "/" else f"/{name}"
            try:
                link = obj.get(name, getlink=True)
            except Exception:
                link = None
            if isinstance(link, h5py.ExternalLink):
                children.append(
                    {
                        "name": name,
                        "path": child_path,
                        "type": "link",
                        "link": "external",
                        "target": f"{link.filename}:{link.path}",
                    }
                )
                continue
            if isinstance(link, h5py.SoftLink):
                children.append(
                    {
                        "name": name,
                        "path": child_path,
                        "type": "link",
                        "link": "soft",
                        "target": str(link.path),
                    }
                )
                continue
            try:
                child = obj[name]
            except Exception:
                continue
            if isinstance(child, h5py.Group):
                children.append(
                    {
                        "name": name,
                        "path": child_path,
                        "type": "group",
                        "hasChildren": len(child.keys()) > 0,
                    }
                )
            elif isinstance(child, h5py.Dataset):
                children.append(
                    {
                        "name": name,
                        "path": child_path,
                        "type": "dataset",
                        "shape": tuple(int(x) for x in child.shape),
                        "dtype": str(child.dtype),
                    }
                )
        children.sort(key=lambda item: (item.get("type") != "group", item.get("name", "")))
        return {"path": path, "children": children}


@app.get("/api/hdf5/node")
def hdf5_node(file: str = Query(..., min_length=1), path: str = Query(..., min_length=1)) -> dict[str, Any]:
    file_path = _resolve_file(file)
    with h5py.File(file_path, "r") as h5:
        if path not in h5:
            raise HTTPException(status_code=404, detail="Path not found")
        obj = h5[path]
        if isinstance(obj, h5py.Group):
            return {"path": path, "type": "group", "attrs": _collect_h5_attrs(obj)}
        if isinstance(obj, h5py.Dataset):
            preview = None
            try:
                if obj.size <= 64 or obj.ndim == 0:
                    preview = _serialize_h5_value(obj[()])
            except Exception:
                preview = None
            return {
                "path": path,
                "type": "dataset",
                "shape": tuple(int(x) for x in obj.shape),
                "dtype": str(obj.dtype),
                "attrs": _collect_h5_attrs(obj),
                "preview": preview,
            }
        raise HTTPException(status_code=400, detail="Unsupported node type")


@app.get("/api/hdf5/value")
def hdf5_value(
    file: str = Query(..., min_length=1),
    path: str = Query(..., min_length=1),
    max_cells: int = Query(2048, ge=16, le=65536),
) -> dict[str, Any]:
    file_path = _resolve_file(file)
    with h5py.File(file_path, "r") as h5:
        if path not in h5:
            raise HTTPException(status_code=404, detail="Path not found")
        obj = h5[path]
        if not isinstance(obj, h5py.Dataset):
            raise HTTPException(status_code=400, detail="Not a dataset")
        preview, preview_shape, truncated, slice_info = _dataset_value_preview(obj, max_cells=max_cells)
        return {
            "path": path,
            "type": "dataset",
            "shape": tuple(int(x) for x in obj.shape),
            "dtype": str(obj.dtype),
            "preview": preview,
            "preview_shape": preview_shape,
            "truncated": truncated,
            "slice": slice_info,
        }


@app.get("/api/hdf5/search")
def hdf5_search(
    file: str = Query(..., min_length=1),
    query: str = Query(..., min_length=1),
    limit: int = Query(200, ge=1, le=1000),
) -> dict[str, Any]:
    needle = query.strip().lower()
    if not needle:
        return {"matches": []}
    file_path = _resolve_file(file)
    matches: list[dict[str, Any]] = []
    with h5py.File(file_path, "r") as h5:
        stack: list[tuple[str, h5py.Group]] = [("/", h5["/"])]
        while stack and len(matches) < limit:
            base_path, group = stack.pop()
            try:
                names = sorted(group.keys())
            except Exception:
                continue
            for name in names:
                child_path = f"{base_path}/{name}" if base_path != "/" else f"/{name}"
                is_match = needle in name.lower() or needle in child_path.lower()
                try:
                    link = group.get(name, getlink=True)
                except Exception:
                    link = None
                if isinstance(link, h5py.ExternalLink):
                    if is_match:
                        matches.append(
                            {
                                "name": name,
                                "path": child_path,
                                "type": "link",
                                "link": "external",
                                "target": f"{link.filename}:{link.path}",
                            }
                        )
                    continue
                if isinstance(link, h5py.SoftLink):
                    if is_match:
                        matches.append(
                            {
                                "name": name,
                                "path": child_path,
                                "type": "link",
                                "link": "soft",
                                "target": str(link.path),
                            }
                        )
                    continue
                try:
                    child = group[name]
                except Exception:
                    continue
                if isinstance(child, h5py.Group):
                    if is_match:
                        matches.append(
                            {
                                "name": name,
                                "path": child_path,
                                "type": "group",
                                "hasChildren": len(child.keys()) > 0,
                            }
                        )
                    stack.append((child_path, child))
                elif isinstance(child, h5py.Dataset) and is_match:
                    matches.append(
                        {
                            "name": name,
                            "path": child_path,
                            "type": "dataset",
                            "shape": tuple(int(x) for x in child.shape),
                            "dtype": str(child.dtype),
                        }
                    )
                if len(matches) >= limit:
                    break
    return {"matches": matches}


@app.get("/api/hdf5/csv")
def hdf5_csv(
    file: str = Query(..., min_length=1),
    path: str = Query(..., min_length=1),
    max_cells: int = Query(65536, ge=64, le=262144),
) -> Response:
    file_path = _resolve_file(file)
    with h5py.File(file_path, "r") as h5:
        if path not in h5:
            raise HTTPException(status_code=404, detail="Path not found")
        obj = h5[path]
        if not isinstance(obj, h5py.Dataset):
            raise HTTPException(status_code=400, detail="Not a dataset")
        data, truncated, slice_info = _dataset_preview_array(obj, max_cells=max_cells)
        if data is None:
            raise HTTPException(status_code=500, detail="Unable to read dataset")
        output = io.StringIO()
        if slice_info:
            output.write(
                f"# slice={slice_info.get('lead')} rows={slice_info.get('rows')} cols={slice_info.get('cols')}\n"
            )
        writer = csv.writer(output)
        if data.ndim == 0:
            writer.writerow([_serialize_h5_value(data.item())])
        elif data.ndim == 1:
            writer.writerow(["index", "value"])
            for idx, value in enumerate(data.tolist()):
                writer.writerow([idx, _serialize_h5_value(value)])
        else:
            for row in data.tolist():
                writer.writerow([_serialize_h5_value(v) for v in row])
        if truncated:
            output.write("# truncated\n")
        filename = path.strip("/").replace("/", "_") or "dataset"
        headers = {"Content-Disposition": f'attachment; filename="{filename}.csv"'}
        return Response(content=output.getvalue(), media_type="text/csv", headers=headers)


@app.get("/api/analysis/params")
def analysis_params(
    file: str = Query(..., min_length=1),
    dataset: str | None = Query(None),
) -> dict[str, Any]:
    file_path = _resolve_file(file)
    distance_val, distance_unit = None, None
    pixel_x_val, pixel_x_unit = None, None
    pixel_y_val, pixel_y_unit = None, None
    energy_val, energy_unit = None, None
    wavelength_val, wavelength_unit = None, None
    center_x_val, center_x_unit = None, None
    center_y_val, center_y_unit = None, None
    shape = None

    with h5py.File(file_path, "r") as h5:
        if dataset:
            try:
                dset, extra_files = _resolve_dataset(h5, file_path, dataset)
                try:
                    shape = tuple(int(x) for x in dset.shape)
                finally:
                    for handle in extra_files:
                        handle.close()
            except Exception:
                shape = None

        distance_val, distance_unit = _read_scalar(
            h5,
            [
                "/entry/instrument/detector/detector_distance",
                "/entry/instrument/detector/distance",
                "/entry/instrument/detector/detectorSpecific/detector_distance",
            ],
        )
        pixel_x_val, pixel_x_unit = _read_scalar(
            h5,
            [
                "/entry/instrument/detector/x_pixel_size",
                "/entry/instrument/detector/detectorSpecific/x_pixel_size",
                "/entry/instrument/detector/pixel_size",
            ],
        )
        pixel_y_val, pixel_y_unit = _read_scalar(
            h5,
            [
                "/entry/instrument/detector/y_pixel_size",
                "/entry/instrument/detector/detectorSpecific/y_pixel_size",
                "/entry/instrument/detector/pixel_size",
            ],
        )
        energy_val, energy_unit = _read_scalar(
            h5,
            [
                "/entry/instrument/beam/incident_energy",
                "/entry/instrument/beam/energy",
                "/entry/instrument/beam/photon_energy",
                "/entry/instrument/source/energy",
            ],
        )
        wavelength_val, wavelength_unit = _read_scalar(
            h5,
            [
                "/entry/instrument/beam/incident_wavelength",
                "/entry/instrument/beam/wavelength",
                "/entry/instrument/beam/photon_wavelength",
            ],
        )
        center_x_val, center_x_unit = _read_scalar(
            h5,
            [
                "/entry/instrument/detector/beam_center_x",
                "/entry/instrument/detector/beam_center_x_mm",
                "/entry/instrument/detector/detectorSpecific/beam_center_x",
            ],
        )
        center_y_val, center_y_unit = _read_scalar(
            h5,
            [
                "/entry/instrument/detector/beam_center_y",
                "/entry/instrument/detector/beam_center_y_mm",
                "/entry/instrument/detector/detectorSpecific/beam_center_y",
            ],
        )

    distance_mm = _to_mm(distance_val, distance_unit) if distance_val is not None else None

    pixel_size_um = None
    if pixel_x_val is not None:
        pixel_size_um = _to_um(pixel_x_val, pixel_x_unit)
    if pixel_y_val is not None:
        pixel_y_um = _to_um(pixel_y_val, pixel_y_unit)
        pixel_size_um = (
            (pixel_size_um + pixel_y_um) / 2 if pixel_size_um is not None else pixel_y_um
        )

    energy_ev = None
    if energy_val is not None:
        energy_ev = _to_ev(energy_val, energy_unit)
    elif wavelength_val is not None:
        energy_ev = _wavelength_to_ev(wavelength_val, wavelength_unit)

    center_x_px = None
    center_y_px = None
    if center_x_val is not None:
        unit = _norm_unit(center_x_unit)
        if unit in {"mm", "m", "cm", "um", "nm"}:
            if pixel_size_um:
                center_x_px = _to_mm(center_x_val, center_x_unit) / (pixel_size_um / 1000)
        else:
            center_x_px = center_x_val
    if center_y_val is not None:
        unit = _norm_unit(center_y_unit)
        if unit in {"mm", "m", "cm", "um", "nm"}:
            if pixel_size_um:
                center_y_px = _to_mm(center_y_val, center_y_unit) / (pixel_size_um / 1000)
        else:
            center_y_px = center_y_val

    return {
        "distance_mm": distance_mm,
        "pixel_size_um": pixel_size_um,
        "energy_ev": energy_ev,
        "center_x_px": center_x_px,
        "center_y_px": center_y_px,
        "shape": shape,
    }


@app.get("/api/metadata")
def metadata(file: str = Query(..., min_length=1), dataset: str = Query(..., min_length=1)) -> dict[str, Any]:
    path = _resolve_file(file)
    with h5py.File(path, "r") as h5:
        try:
            dset, extra_files = _resolve_dataset(h5, path, dataset)
            try:
                shape = tuple(int(x) for x in dset.shape)
                response = {
                    "path": dataset,
                    "shape": shape,
                    "dtype": str(dset.dtype),
                    "ndim": dset.ndim,
                    "chunks": dset.chunks,
                    "maxshape": dset.maxshape,
                }
                if dset.ndim == 4:
                    response["threshold_energies"] = _read_threshold_energies(h5, shape[1])
                return response
            finally:
                for handle in extra_files:
                    handle.close()
        except KeyError:
            raise HTTPException(status_code=404, detail="Dataset not found")


@app.get("/api/frame")
def frame(
    file: str = Query(..., min_length=1),
    dataset: str = Query(..., min_length=1),
    index: int = Query(0, ge=0),
    threshold: int = Query(0, ge=0),
) -> Response:
    path = _resolve_file(file)
    with h5py.File(path, "r") as h5:
        try:
            dset, extra_files = _resolve_dataset(h5, path, dataset)
        except KeyError:
            raise HTTPException(status_code=404, detail="Dataset not found")
        try:
            if dset.ndim == 4:
                if index >= dset.shape[0]:
                    raise HTTPException(status_code=416, detail="Frame index out of range")
                if threshold >= dset.shape[1]:
                    raise HTTPException(status_code=416, detail="Threshold index out of range")
                frame_data = dset[index, threshold, :, :]
            elif dset.ndim == 3:
                if index >= dset.shape[0]:
                    raise HTTPException(status_code=416, detail="Frame index out of range")
                frame_data = dset[index, :, :]
            elif dset.ndim == 2:
                frame_data = dset[:, :]
            else:
                raise HTTPException(status_code=400, detail="Dataset is not 2D, 3D, or 4D")
        finally:
            for handle in extra_files:
                handle.close()

        arr = np.asarray(frame_data)
        if arr.dtype.byteorder == ">" or (arr.dtype.byteorder == "=" and sys.byteorder == "big"):
            arr = arr.byteswap().newbyteorder("<")

        data = arr.tobytes(order="C")
        headers = {
            "X-Dtype": arr.dtype.str,
            "X-Shape": ",".join(str(x) for x in arr.shape),
            "X-Frame": str(index),
        }
        return Response(content=data, media_type="application/octet-stream", headers=headers)


@app.get("/api/preview")
def preview(
    file: str = Query(..., min_length=1),
    dataset: str = Query(..., min_length=1),
    index: int = Query(0, ge=0),
    max_size: int = Query(1024, ge=64, le=4096),
    threshold: int = Query(0, ge=0),
) -> Response:
    path = _resolve_file(file)
    with h5py.File(path, "r") as h5:
        try:
            dset, extra_files = _resolve_dataset(h5, path, dataset)
        except KeyError:
            raise HTTPException(status_code=404, detail="Dataset not found")
        try:
            if dset.ndim == 4:
                if index >= dset.shape[0]:
                    raise HTTPException(status_code=416, detail="Frame index out of range")
                if threshold >= dset.shape[1]:
                    raise HTTPException(status_code=416, detail="Threshold index out of range")
                frame_data = dset[index, threshold, :, :]
            elif dset.ndim == 3:
                if index >= dset.shape[0]:
                    raise HTTPException(status_code=416, detail="Frame index out of range")
                frame_data = dset[index, :, :]
            elif dset.ndim == 2:
                frame_data = dset[:, :]
            else:
                raise HTTPException(status_code=400, detail="Dataset is not 2D, 3D, or 4D")
        finally:
            for handle in extra_files:
                handle.close()

        arr = np.asarray(frame_data)
        height, width = arr.shape
        scale = max(height / max_size, width / max_size, 1.0)
        if scale > 1:
            step = int(np.ceil(scale))
            arr = arr[::step, ::step]

        if arr.dtype.byteorder == ">" or (arr.dtype.byteorder == "=" and sys.byteorder == "big"):
            arr = arr.byteswap().newbyteorder("<")

        data = arr.tobytes(order="C")
        headers = {
            "X-Dtype": arr.dtype.str,
            "X-Shape": ",".join(str(x) for x in arr.shape),
            "X-Frame": str(index),
            "X-Preview": "1",
        }
        return Response(content=data, media_type="application/octet-stream", headers=headers)


@app.get("/api/mask")
def mask(
    file: str = Query(..., min_length=1),
    threshold: int | None = Query(None, ge=0),
) -> Response:
    path = _resolve_file(file)
    with h5py.File(path, "r") as h5:
        dset = _find_pixel_mask(h5, threshold=threshold)
        if not dset:
            raise HTTPException(status_code=404, detail="Pixel mask not found")
        if dset.ndim != 2:
            raise HTTPException(status_code=400, detail="Pixel mask has invalid shape")
        arr = np.asarray(dset)
        if arr.dtype.byteorder == ">" or (arr.dtype.byteorder == "=" and sys.byteorder == "big"):
            arr = arr.byteswap().newbyteorder("<")
        data = arr.tobytes(order="C")
        headers = {
            "X-Dtype": arr.dtype.str,
            "X-Shape": ",".join(str(x) for x in arr.shape),
            "X-Mask-Path": dset.name,
        }
        return Response(content=data, media_type="application/octet-stream", headers=headers)


def _resource_root() -> Path:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)
    return Path(__file__).resolve().parents[1]


STATIC_DIR = _resource_root() / "frontend"
app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")


if __name__ == "__main__":
    import uvicorn

    host = os.environ.get("ALBIS_HOST", "127.0.0.1")
    port = int(os.environ.get("ALBIS_PORT", "8000") or 8000)
    reload = os.environ.get("ALBIS_RELOAD", "0").lower() in {"1", "true", "yes", "on"}
    uvicorn.run("app:app", host=host, port=port, reload=reload)
