from __future__ import annotations

"""ALBIS backend API service.

This module keeps backend runtime logic centralized for simple packaged
execution. It handles path safety, image IO, metadata extraction, analysis
endpoints, monitor integration, and long-running series jobs.
"""

import math
import fnmatch
import os
import re
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import logging
from logging.handlers import RotatingFileHandler
import tempfile
import numpy as np
from fastapi import Body, FastAPI, HTTPException, Query, Request
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles

try:
    from .config import (
        DEFAULT_CONFIG,
        get_bool,
        get_float,
        get_int,
        get_str,
        load_config,
        normalize_config,
        resolve_path,
        save_config,
    )
    from .image_formats import (
        _image_ext_name,
        _pilatus_header_text,
        _pilatus_meta_from_fabio,
        _pilatus_meta_from_tiff,
        _read_tiff_bytes_with_simplon_meta,
        _read_cbf,
        _read_cbf_gz,
        _read_edf,
        _read_tiff,
        _resolve_series_files,
        _split_series_name,
        _strip_image_ext,
        _write_tiff,
    )
    from .services.remote_stream import (
        remote_extract_metadata as _remote_extract_metadata,
        remote_parse_meta as _remote_parse_meta,
        remote_read_image_bytes as _remote_read_image_bytes,
        remote_safe_source_id as _remote_safe_source_id,
        remote_snapshot as _remote_snapshot,
        remote_store_frame as _remote_store_frame,
    )
    from .services.series_ops import (
        iter_sum_groups as _iter_sum_groups,
        mask_flag_value as _mask_flag_value,
        mask_slices as _mask_slices,
    )
    from .services.simplon import (
        simplon_base as _simplon_base,
        simplon_fetch_monitor as _simplon_fetch_monitor,
        simplon_fetch_pixel_mask as _simplon_fetch_pixel_mask,
        simplon_set_mode as _simplon_set_mode,
    )
    from .routes.hdf5 import HDF5RouteDeps, register_hdf5_routes
    from .routes.files import FileRouteDeps, register_file_routes
    from .routes.system import SystemRouteDeps, register_system_routes
    from .routes.stream import StreamRouteDeps, register_stream_routes
except ImportError:  # pragma: no cover - supports `python backend/app.py`
    from config import (
        DEFAULT_CONFIG,
        get_bool,
        get_float,
        get_int,
        get_str,
        load_config,
        normalize_config,
        resolve_path,
        save_config,
    )
    from image_formats import (
        _image_ext_name,
        _pilatus_header_text,
        _pilatus_meta_from_fabio,
        _pilatus_meta_from_tiff,
        _read_tiff_bytes_with_simplon_meta,
        _read_cbf,
        _read_cbf_gz,
        _read_edf,
        _read_tiff,
        _resolve_series_files,
        _split_series_name,
        _strip_image_ext,
        _write_tiff,
    )
    from services.remote_stream import (
        remote_extract_metadata as _remote_extract_metadata,
        remote_parse_meta as _remote_parse_meta,
        remote_read_image_bytes as _remote_read_image_bytes,
        remote_safe_source_id as _remote_safe_source_id,
        remote_snapshot as _remote_snapshot,
        remote_store_frame as _remote_store_frame,
    )
    from services.series_ops import (
        iter_sum_groups as _iter_sum_groups,
        mask_flag_value as _mask_flag_value,
        mask_slices as _mask_slices,
    )
    from services.simplon import (
        simplon_base as _simplon_base,
        simplon_fetch_monitor as _simplon_fetch_monitor,
        simplon_fetch_pixel_mask as _simplon_fetch_pixel_mask,
        simplon_set_mode as _simplon_set_mode,
    )
    from routes.hdf5 import HDF5RouteDeps, register_hdf5_routes
    from routes.files import FileRouteDeps, register_file_routes
    from routes.system import SystemRouteDeps, register_system_routes
    from routes.stream import StreamRouteDeps, register_stream_routes

CONFIG, CONFIG_PATH = load_config()
CONFIG_BASE_DIR = CONFIG_PATH.parent

data_root_cfg = get_str(CONFIG, ("data", "root"), "").strip()
if data_root_cfg:
    DATA_DIR = resolve_path(data_root_cfg, base_dir=CONFIG_BASE_DIR)
else:
    if getattr(sys, "frozen", False):
        DATA_DIR = Path.home() / "ALBIS-data"
    else:
        DATA_DIR = Path(__file__).resolve().parents[1]

ALBIS_VERSION = "0.6"

app = FastAPI(title="ALBIS — ALBIS WEB VIEW", version=ALBIS_VERSION)

AUTOLOAD_EXTS = {".h5", ".hdf5", ".tif", ".tiff", ".cbf", ".cbf.gz", ".edf"}
ALLOW_ABS_PATHS = get_bool(CONFIG, ("data", "allow_abs_paths"), True)
SCAN_CACHE_SEC = get_float(CONFIG, ("data", "scan_cache_sec"), 2.0)
MAX_SCAN_DEPTH = get_int(CONFIG, ("data", "max_scan_depth"), -1)
MAX_UPLOAD_MB = get_int(CONFIG, ("data", "max_upload_mb"), 0)
MAX_UPLOAD_BYTES = MAX_UPLOAD_MB * 1024 * 1024 if MAX_UPLOAD_MB > 0 else 0
LOG_DIR: Path | None = None
LOG_PATH: Path | None = None
_series_jobs: dict[str, dict[str, Any]] = {}
_series_jobs_lock = threading.Lock()
h5py = None
_hdf5_stack_ready = False


def _ensure_hdf5_stack() -> None:
    """Lazy-load HDF5 modules and compression plugins on first use."""
    global h5py, _hdf5_stack_ready
    if not _hdf5_stack_ready:
        import hdf5plugin as _hdf5plugin  # noqa: F401

        _ = _hdf5plugin
        _hdf5_stack_ready = True
    if h5py is None:
        import h5py as _h5py  # type: ignore[import-not-found]

        h5py = _h5py


def _init_logging() -> logging.Logger:
    global LOG_DIR, LOG_PATH
    level_name = get_str(CONFIG, ("logging", "level"), "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logger = logging.getLogger("albis")
    if logger.handlers:
        return logger
    logger.setLevel(level)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    logger.addHandler(stream)

    log_dir_cfg = get_str(CONFIG, ("logging", "dir"), "").strip()
    log_dir = (
        resolve_path(log_dir_cfg, base_dir=CONFIG_BASE_DIR) if log_dir_cfg else (DATA_DIR / "logs")
    )
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
_startup_banner_logged = False


@app.on_event("startup")
async def _log_startup_banner() -> None:
    """Log startup paths once per serving process.

    Keep this out of module import time so uvicorn reload supervisors do not
    spam repeated startup lines.
    """
    global _startup_banner_logged
    if _startup_banner_logged:
        return
    _startup_banner_logged = True
    pid = os.getpid()
    logger.info("ALBIS data dir (pid=%s): %s", pid, DATA_DIR)
    logger.info("ALBIS config (pid=%s): %s", pid, CONFIG_PATH)


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
        logger.warning(
            "%s %s -> %s (%.1fms)", request.method, request.url.path, status, duration_ms
        )
    else:
        if duration_ms >= 1000:
            logger.info(
                "%s %s -> %s (%.1fms)", request.method, request.url.path, status, duration_ms
            )
        else:
            logger.debug(
                "%s %s -> %s (%.1fms)", request.method, request.url.path, status, duration_ms
            )
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
        if not path.exists() or _image_ext_name(path.name) not in AUTOLOAD_EXTS:
            raise HTTPException(status_code=404, detail="File not found")
        return path
    safe = _safe_rel_path(name)
    path = (DATA_DIR / safe).resolve()
    if not _is_within(path, DATA_DIR.resolve()):
        raise HTTPException(status_code=400, detail="Invalid file name")
    if not path.exists() or _image_ext_name(path.name) not in AUTOLOAD_EXTS:
        raise HTTPException(status_code=404, detail="File not found")
    return path


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


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
        if token == ".cbf":
            cleaned.add(".cbf.gz")
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
        ext = _image_ext_name(name)
        if ext not in AUTOLOAD_EXTS:
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
        ext = _image_ext_name(name)
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


def _copy_h5_metadata(src_h5: h5py.File, dst_h5: h5py.File, threshold_count: int) -> None:
    """Copy metadata from source H5 to destination, including channel info and threshold energies."""
    # Copy root-level attributes
    for key, val in src_h5.attrs.items():
        if key not in dst_h5.attrs:
            try:
                dst_h5.attrs[key] = val
            except Exception:
                pass

    # Copy instrument/detector metadata, especially channel and threshold info
    if "/entry/instrument/detector" in src_h5:
        src_detector = src_h5["/entry/instrument/detector"]
        dst_detector = dst_h5.require_group("/entry/instrument/detector")

        # Copy detector group attributes
        for key, val in src_detector.attrs.items():
            if key not in dst_detector.attrs:
                try:
                    dst_detector.attrs[key] = val
                except Exception:
                    pass

        # Copy channel information (threshold channels)
        for thr in range(threshold_count):
            channel_path = f"threshold_{thr + 1}_channel"
            if channel_path in src_detector:
                src_channel = src_detector[channel_path]
                dst_channel = dst_detector.require_group(channel_path)

                # Copy channel attributes
                for key, val in src_channel.attrs.items():
                    if key not in dst_channel.attrs:
                        try:
                            dst_channel.attrs[key] = val
                        except Exception:
                            pass

                # Copy important datasets from channel (threshold_energy, etc.)
                for item_name in src_channel:
                    if item_name in ("pixel_mask",):  # Skip pixel_mask, handled separately
                        continue
                    src_item = src_channel[item_name]
                    if isinstance(src_item, h5py.Dataset) and item_name not in dst_channel:
                        try:
                            dst_channel.create_dataset(item_name, data=src_item[()])
                        except Exception:
                            pass

    # Copy sample/beamline metadata if present
    for group_path in ("/entry/instrument", "/entry/sample", "/entry/data"):
        if group_path in src_h5 and group_path not in dst_h5:
            try:
                src_group = src_h5[group_path]
                dst_group = dst_h5.require_group(group_path)
                # Copy group attributes
                for key, val in src_group.attrs.items():
                    if key not in dst_group.attrs:
                        try:
                            dst_group.attrs[key] = val
                        except Exception:
                            pass
            except Exception:
                pass


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


_LINKED_DATA_NAME_RE = re.compile(r"^data(?:[_-]?(\d+))?$")


def _is_linked_data_member(name: str) -> bool:
    return _LINKED_DATA_NAME_RE.match(name) is not None


def _linked_member_sort_key(path_or_name: str) -> tuple[int, int, str]:
    name = path_or_name.rsplit("/", 1)[-1]
    match = _LINKED_DATA_NAME_RE.match(name)
    if not match:
        return (1, 0, name)
    suffix = match.group(1)
    return (0, int(suffix) if suffix else 0, name)


def _aggregate_linked_stack_datasets(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Replace segmented `data_*` datasets with one synthetic stack entry.

    Many master files split one logical stack into sibling datasets such as
    `/entry/data/data_000001`, `/entry/data/data_000002`, etc. The frontend
    expects one continuous frame axis, so we merge compatible members into one
    synthetic descriptor while retaining segment paths in `members`.
    """
    grouped: dict[str, list[dict[str, Any]]] = {}
    for info in results:
        if not info.get("image"):
            continue
        path = str(info.get("path", ""))
        if "/" not in path.strip("/"):
            continue
        parent, name = path.rsplit("/", 1)
        if not _is_linked_data_member(name):
            continue
        grouped.setdefault(parent, []).append(info)

    remove_paths: set[str] = set()
    synthetic: list[dict[str, Any]] = []
    for parent, members in grouped.items():
        if len(members) < 2:
            continue
        ordered = sorted(
            members, key=lambda item: _linked_member_sort_key(str(item.get("path", "")))
        )
        first = ordered[0]
        ndim = int(first.get("ndim") or 0)
        if ndim not in (3, 4):
            continue
        dtype = str(first.get("dtype", ""))
        shape = tuple(int(x) for x in (first.get("shape") or ()))
        if len(shape) != ndim:
            continue
        tail = shape[1:]
        total_frames = 0
        valid: list[dict[str, Any]] = []
        for item in ordered:
            item_shape = tuple(int(x) for x in (item.get("shape") or ()))
            if (
                int(item.get("ndim") or 0) != ndim
                or str(item.get("dtype", "")) != dtype
                or len(item_shape) != ndim
                or tuple(item_shape[1:]) != tail
                or int(item_shape[0]) <= 0
            ):
                continue
            total_frames += int(item_shape[0])
            valid.append(item)
        if len(valid) < 2 or total_frames <= 0:
            continue
        agg_shape = (int(total_frames),) + tail
        synthetic.append(
            {
                "path": parent,
                "shape": agg_shape,
                "dtype": dtype,
                "ndim": ndim,
                "size": int(np.prod(agg_shape)),
                "chunks": None,
                "maxshape": None,
                "image": True,
                "linked_stack": True,
                "members": [str(item.get("path")) for item in valid],
            }
        )
        for item in valid:
            remove_paths.add(str(item.get("path", "")))

    if not synthetic:
        return results
    filtered = [item for item in results if str(item.get("path", "")) not in remove_paths]
    filtered.extend(synthetic)
    return filtered


def _resolve_node(h5: h5py.File, base_file: Path, path: str) -> tuple[Any, Path, list[h5py.File]]:
    """Resolve HDF5 object path while following soft/external links safely."""
    parts = [p for p in path.strip("/").split("/") if p]
    if not parts:
        return h5["/"], base_file, []
    current: Any = h5["/"]
    current_file = base_file
    opened: list[h5py.File] = []

    try:
        for idx, part in enumerate(parts):
            if not isinstance(current, h5py.Group):
                raise KeyError("Path not found")
            link = current.get(part, getlink=True)
            if link is None:
                raise KeyError("Path not found")
            if isinstance(link, h5py.ExternalLink):
                target_path = _resolve_external_path(current_file, link.filename)
                if not target_path:
                    raise KeyError("Path not found")
                try:
                    target_file = h5py.File(target_path, "r")
                except OSError as exc:
                    raise KeyError("Path not found") from exc
                opened.append(target_file)
                current_file = target_path
                try:
                    current = target_file[link.path]
                except Exception as exc:
                    raise KeyError("Path not found") from exc
            elif isinstance(link, h5py.SoftLink):
                try:
                    current = current[link.path]
                except Exception as exc:
                    raise KeyError("Path not found") from exc
            else:
                try:
                    current = current[part]
                except Exception as exc:
                    raise KeyError("Path not found") from exc
            if idx < len(parts) - 1 and isinstance(current, h5py.Dataset):
                raise KeyError("Path not found")
    except Exception:
        for handle in opened:
            try:
                handle.close()
            except Exception:
                pass
        raise
    return current, current_file, opened


def _resolve_group_linked_stack(
    group: h5py.Group,
    group_path: str,
    group_file: Path,
    opened: list[h5py.File],
) -> dict[str, Any] | None:
    """Interpret a group as segmented stack if members are compatible."""
    segments: list[dict[str, Any]] = []
    ndim: int | None = None
    dtype: str | None = None
    tail: tuple[int, ...] | None = None

    for name in sorted(group.keys(), key=_linked_member_sort_key):
        if not _is_linked_data_member(name):
            continue
        try:
            link = group.get(name, getlink=True)
        except Exception:
            continue
        if isinstance(link, h5py.ExternalLink):
            target_path = _resolve_external_path(group_file, link.filename)
            if not target_path:
                continue
            try:
                target_file = h5py.File(target_path, "r")
            except OSError:
                continue
            opened.append(target_file)
            try:
                child = target_file[link.path]
            except Exception:
                continue
        elif isinstance(link, h5py.SoftLink):
            try:
                child = group[link.path]
            except Exception:
                continue
        else:
            try:
                child = group[name]
            except Exception:
                continue
        if not isinstance(child, h5py.Dataset):
            continue
        child_shape = tuple(int(x) for x in child.shape)
        child_ndim = int(child.ndim)
        if child_ndim not in (3, 4) or len(child_shape) != child_ndim or child_shape[0] <= 0:
            continue
        child_dtype = str(child.dtype)
        child_tail = child_shape[1:]
        if ndim is None:
            ndim = child_ndim
            dtype = child_dtype
            tail = child_tail
        elif child_ndim != ndim or child_dtype != dtype or child_tail != tail:
            continue
        child_path = f"{group_path.rstrip('/')}/{name}" if group_path != "/" else f"/{name}"
        segments.append(
            {
                "path": child_path,
                "dataset": child,
                "frames": int(child_shape[0]),
                "shape": child_shape,
            }
        )

    if not segments or ndim is None or tail is None or dtype is None:
        return None
    total_frames = sum(int(seg["frames"]) for seg in segments)
    if total_frames <= 0:
        return None
    shape = (int(total_frames),) + tail
    return {
        "kind": "linked_stack",
        "path": group_path,
        "shape": shape,
        "dtype": dtype,
        "ndim": ndim,
        "segments": segments,
    }


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


def _resolve_dataset_view(
    h5: h5py.File, base_file: Path, dataset: str
) -> tuple[dict[str, Any], list[h5py.File]]:
    """Normalize target into either direct dataset view or linked-stack view."""
    node, current_file, opened = _resolve_node(h5, base_file, dataset)
    if isinstance(node, h5py.Dataset):
        return (
            {
                "kind": "dataset",
                "path": dataset,
                "shape": tuple(int(x) for x in node.shape),
                "dtype": str(node.dtype),
                "ndim": int(node.ndim),
                "dataset": node,
            },
            opened,
        )
    if isinstance(node, h5py.Group):
        stack = _resolve_group_linked_stack(node, dataset, current_file, opened)
        if stack is not None:
            return stack, opened
    for handle in opened:
        try:
            handle.close()
        except Exception:
            pass
    raise KeyError("Dataset not found")


def _extract_frame(view: dict[str, Any], index: int, threshold: int) -> np.ndarray:
    """Extract one 2D frame from a normalized dataset view."""
    if view["kind"] == "dataset":
        dset = view["dataset"]
        if dset.ndim == 4:
            if index >= dset.shape[0]:
                raise HTTPException(status_code=416, detail="Frame index out of range")
            if threshold >= dset.shape[1]:
                raise HTTPException(status_code=416, detail="Threshold index out of range")
            return np.asarray(dset[index, threshold, :, :])
        if dset.ndim == 3:
            if index >= dset.shape[0]:
                raise HTTPException(status_code=416, detail="Frame index out of range")
            return np.asarray(dset[index, :, :])
        if dset.ndim == 2:
            return np.asarray(dset[:, :])
        raise HTTPException(status_code=400, detail="Dataset is not 2D, 3D, or 4D")

    if view["kind"] == "linked_stack":
        shape = tuple(int(x) for x in view["shape"])
        if not shape:
            raise HTTPException(status_code=400, detail="Dataset has invalid shape")
        total_frames = int(shape[0])
        if index >= total_frames:
            raise HTTPException(status_code=416, detail="Frame index out of range")
        ndim = int(view["ndim"])
        if ndim == 4 and threshold >= int(shape[1]):
            raise HTTPException(status_code=416, detail="Threshold index out of range")
        local = int(index)
        selected: dict[str, Any] | None = None
        for segment in view["segments"]:
            frames = int(segment["frames"])
            if local < frames:
                selected = segment
                break
            local -= frames
        if selected is None:
            raise HTTPException(status_code=416, detail="Frame index out of range")
        dset = selected["dataset"]
        if ndim == 4:
            return np.asarray(dset[local, threshold, :, :])
        if ndim == 3:
            return np.asarray(dset[local, :, :])
        raise HTTPException(status_code=400, detail="Dataset is not 3D or 4D")

    raise HTTPException(status_code=400, detail="Unsupported dataset view")


def _series_job_update(job_id: str, **changes: Any) -> None:
    with _series_jobs_lock:
        job = _series_jobs.get(job_id)
        if not job:
            return
        job.update(changes)
        job["updated_at"] = time.time()


def _resolve_series_output_base(output_path: str | None) -> Path:
    raw = (output_path or "").strip()
    if raw:
        target = Path(raw).expanduser()
        if not target.is_absolute():
            target = (DATA_DIR / target).resolve()
        else:
            target = target.resolve()
    else:
        target = (DATA_DIR / "output" / "series_sum").resolve()

    if target.exists() and target.is_dir():
        target = (target / "series_sum").resolve()

    allowed_root = DATA_DIR.resolve()
    if not ALLOW_ABS_PATHS and not _is_within(target, allowed_root):
        raise HTTPException(status_code=400, detail="Output path is outside data directory")
    target.parent.mkdir(parents=True, exist_ok=True)
    return target


def _next_available_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    for idx in range(1, 10000):
        candidate = path.with_name(f"{stem}_{idx:03d}{suffix}")
        if not candidate.exists():
            return candidate
    raise HTTPException(status_code=500, detail="Unable to allocate output file name")


def _run_series_summing_job(
    job_id: str,
    file: str,
    dataset: str,
    mode: str,
    step: int,
    operation: str,
    normalize_frame: int | None,
    range_start: int | None,
    range_end: int | None,
    output_path: str | None,
    output_format: str,
    apply_mask: bool,
) -> None:
    """Background worker for series summing output generation."""
    try:
        source_path = _resolve_image_file(file)
        ext = _image_ext_name(source_path.name)
        base_target = _resolve_series_output_base(output_path)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        output_format = output_format.lower()
        mode = mode.lower()
        operation = operation.lower()
        step = max(1, int(step))
        normalize_frame_idx = int(normalize_frame) - 1 if normalize_frame is not None else None

        _series_job_update(job_id, status="running", message="Preparing datasets…", progress=0.01)

        if ext in {".h5", ".hdf5"}:
            _ensure_hdf5_stack()
            with h5py.File(source_path, "r") as h5:
                view, extra_files = _resolve_dataset_view(h5, source_path, dataset)
                try:
                    shape = tuple(int(x) for x in view["shape"])
                    ndim = int(view["ndim"])
                    if ndim not in (3, 4):
                        raise HTTPException(
                            status_code=400, detail="Series summing requires 3D or 4D image stacks"
                        )
                    frame_count = int(shape[0])
                    threshold_count = int(shape[1]) if ndim == 4 else 1
                    if normalize_frame_idx is not None and (
                        normalize_frame_idx < 0 or normalize_frame_idx >= frame_count
                    ):
                        raise HTTPException(
                            status_code=400, detail="Normalize frame is out of range"
                        )
                    groups = _iter_sum_groups(frame_count, mode, step, range_start, range_end)
                    if not groups:
                        raise HTTPException(
                            status_code=400, detail="No frames available for summing"
                        )

                    source_dtype = np.dtype(view["dtype"])
                    flag_value = _mask_flag_value(source_dtype)
                    mask_bits_by_thr: list[np.ndarray | None] = []
                    for thr in range(threshold_count):
                        if not apply_mask:
                            mask_bits_by_thr.append(None)
                            continue
                        mask_dset = _find_pixel_mask(
                            h5, threshold=thr if threshold_count > 1 else None
                        )
                        if mask_dset is None:
                            mask_bits_by_thr.append(None)
                            continue
                        mask_bits_by_thr.append(np.asarray(mask_dset, dtype=np.uint32))

                    total_input_frames = sum(int(group["count"]) for group in groups)
                    total_steps = max(1, total_input_frames * threshold_count)
                    processed = 0
                    sums: list[tuple[int, int, int, int, int, np.ndarray, np.ndarray | None]] = []

                    for thr in range(threshold_count):
                        mask_bits = mask_bits_by_thr[thr]
                        _, _, any_mask = (
                            _mask_slices(mask_bits) if mask_bits is not None else (None, None, None)
                        )
                        norm_ref: np.ndarray | None = None
                        norm_ref_valid: np.ndarray | None = None
                        if normalize_frame_idx is not None:
                            norm_ref = np.asarray(
                                _extract_frame(view, normalize_frame_idx, thr), dtype=np.float64
                            )
                            if any_mask is not None:
                                norm_ref = norm_ref.copy()
                                norm_ref[any_mask] = np.nan
                            norm_ref_valid = np.isfinite(norm_ref) & (np.abs(norm_ref) > 1e-12)

                        for chunk_idx, group in enumerate(groups):
                            start_idx = int(group["start"])
                            end_idx = int(group["end"])
                            frame_indices = list(group["indices"])
                            acc: np.ndarray | None = None
                            median_stack: list[np.ndarray] | None = (
                                [] if operation == "median" else None
                            )
                            for frame_idx in frame_indices:
                                arr = _extract_frame(view, frame_idx, thr)
                                arr = np.asarray(arr, dtype=np.float64)
                                if norm_ref is not None and norm_ref_valid is not None:
                                    arr = np.divide(
                                        arr,
                                        norm_ref,
                                        out=np.zeros_like(arr, dtype=np.float64),
                                        where=norm_ref_valid,
                                    )
                                if any_mask is not None:
                                    arr = arr.copy()
                                    arr[any_mask] = 0.0
                                if operation == "median":
                                    if median_stack is not None:
                                        median_stack.append(arr)
                                else:
                                    if acc is None:
                                        acc = np.zeros_like(arr, dtype=np.float64)
                                    acc += arr
                                processed += 1
                                progress = min(0.95, processed / total_steps)
                                _series_job_update(
                                    job_id,
                                    progress=progress,
                                    message=(
                                        f"{operation.capitalize()} threshold {thr + 1}/{threshold_count}, "
                                        f"frame {frame_idx + 1}/{frame_count}"
                                    ),
                                )
                            if operation == "median":
                                if not median_stack:
                                    continue
                                reduced = np.median(np.stack(median_stack, axis=0), axis=0)
                            else:
                                if acc is None:
                                    continue
                                if operation == "mean":
                                    reduced = acc / float(max(1, len(frame_indices)))
                                else:
                                    reduced = acc
                            sums.append(
                                (
                                    thr,
                                    chunk_idx,
                                    start_idx,
                                    end_idx,
                                    len(frame_indices),
                                    reduced,
                                    mask_bits,
                                )
                            )

                finally:
                    for handle in extra_files:
                        try:
                            handle.close()
                        except Exception:
                            pass
        else:
            series_files, _ = _resolve_series_files(source_path)
            frame_count = len(series_files)
            threshold_count = 1
            if normalize_frame_idx is not None and (
                normalize_frame_idx < 0 or normalize_frame_idx >= frame_count
            ):
                raise HTTPException(status_code=400, detail="Normalize frame is out of range")
            groups = _iter_sum_groups(frame_count, mode, step, range_start, range_end)
            if not groups:
                raise HTTPException(status_code=400, detail="No frames available for summing")

            def read_image(path: Path) -> np.ndarray:
                ext_name = _image_ext_name(path.name)
                if ext_name in {".tif", ".tiff"}:
                    return _read_tiff(path, index=0)
                if ext_name == ".cbf":
                    return _read_cbf(path)
                if ext_name == ".cbf.gz":
                    return _read_cbf_gz(path)
                if ext_name == ".edf":
                    return _read_edf(path)
                raise HTTPException(status_code=400, detail="Unsupported image format")

            sample = read_image(series_files[0])
            image_h = int(sample.shape[-2])
            image_w = int(sample.shape[-1])
            shape = (int(frame_count), image_h, image_w)
            source_dtype = np.dtype(sample.dtype)
            flag_value = _mask_flag_value(source_dtype)
            mask_bits = np.zeros((image_h, image_w), dtype=np.uint32) if apply_mask else None
            mask_bits_by_thr = [mask_bits]

            norm_ref: np.ndarray | None = None
            norm_ref_valid: np.ndarray | None = None
            if normalize_frame_idx is not None:
                ref_arr = np.asarray(
                    read_image(series_files[normalize_frame_idx]), dtype=np.float64
                )
                if apply_mask:
                    neg = ref_arr < 0
                    if neg.any():
                        gaps = ref_arr == -1
                        if mask_bits is not None:
                            mask_bits[gaps] |= 1
                            mask_bits[neg & ~gaps] |= 0x1E
                        ref_arr = ref_arr.copy()
                        ref_arr[neg] = np.nan
                norm_ref = ref_arr
                norm_ref_valid = np.isfinite(norm_ref) & (np.abs(norm_ref) > 1e-12)

            total_input_frames = sum(int(group["count"]) for group in groups)
            total_steps = max(1, total_input_frames)
            processed = 0
            sums = []

            for chunk_idx, group in enumerate(groups):
                start_idx = int(group["start"])
                end_idx = int(group["end"])
                frame_indices = list(group["indices"])
                acc: np.ndarray | None = None
                median_stack: list[np.ndarray] | None = [] if operation == "median" else None
                for frame_idx in frame_indices:
                    arr = np.asarray(read_image(series_files[frame_idx]), dtype=np.float64)
                    if apply_mask:
                        neg = arr < 0
                        if neg.any():
                            gaps = arr == -1
                            if mask_bits is not None:
                                mask_bits[gaps] |= 1
                                mask_bits[neg & ~gaps] |= 0x1E
                            arr = arr.copy()
                            arr[neg] = 0.0
                        if mask_bits is not None and np.any(mask_bits):
                            arr = arr.copy()
                            arr[mask_bits != 0] = 0.0
                    if norm_ref is not None and norm_ref_valid is not None:
                        arr = np.divide(
                            arr,
                            norm_ref,
                            out=np.zeros_like(arr, dtype=np.float64),
                            where=norm_ref_valid,
                        )
                    if operation == "median":
                        if median_stack is not None:
                            median_stack.append(arr)
                    else:
                        if acc is None:
                            acc = np.zeros_like(arr, dtype=np.float64)
                        acc += arr
                    processed += 1
                    progress = min(0.95, processed / total_steps)
                    _series_job_update(
                        job_id,
                        progress=progress,
                        message=f"{operation.capitalize()} frame {frame_idx + 1}/{frame_count}",
                    )
                if operation == "median":
                    if not median_stack:
                        continue
                    reduced = np.median(np.stack(median_stack, axis=0), axis=0)
                else:
                    if acc is None:
                        continue
                    if operation == "mean":
                        reduced = acc / float(max(1, len(frame_indices)))
                    else:
                        reduced = acc
                sums.append(
                    (0, chunk_idx, start_idx, end_idx, len(frame_indices), reduced, mask_bits)
                )

        outputs: list[str] = []
        _series_job_update(job_id, progress=0.97, message="Writing outputs…")
        if output_format in {"hdf5", "h5"}:
            _ensure_hdf5_stack()
            if base_target.suffix.lower() in {".h5", ".hdf5"}:
                out_file = base_target
            else:
                out_file = base_target.parent / f"{base_target.name}_{timestamp}.h5"
            out_file = _next_available_path(out_file)
            with h5py.File(out_file, "w") as out_h5:
                out_h5.attrs["source_file"] = str(source_path)
                out_h5.attrs["source_dataset"] = str(dataset)
                out_h5.attrs["series_mode"] = mode
                out_h5.attrs["operation"] = operation
                out_h5.attrs["frame_count"] = int(frame_count)
                out_h5.attrs["threshold_count"] = int(threshold_count)
                out_h5.attrs["mask_applied"] = bool(apply_mask)
                if normalize_frame is not None:
                    out_h5.attrs["normalize_frame"] = int(normalize_frame)

                out_frame_count = len(groups)
                image_h = int(shape[-2])
                image_w = int(shape[-1])
                if threshold_count > 1:
                    data_shape = (out_frame_count, threshold_count, image_h, image_w)
                    data_chunks = (1, 1, image_h, image_w)
                else:
                    data_shape = (out_frame_count, image_h, image_w)
                    data_chunks = (1, image_h, image_w)

                entry_group = out_h5.require_group("/entry")
                data_group = entry_group.require_group("data")

                # Copy metadata from source H5 file when available
                if ext in {".h5", ".hdf5"}:
                    try:
                        with h5py.File(source_path, "r") as src_h5:
                            _copy_h5_metadata(src_h5, out_h5, threshold_count)
                    except Exception:
                        pass  # If metadata copy fails, continue with data writing

                data_dset = data_group.create_dataset(
                    "data",
                    shape=data_shape,
                    dtype=np.float64,
                    chunks=data_chunks,
                    compression="gzip",
                    compression_opts=4,
                    shuffle=True,
                )
                data_dset.attrs["sum_mode"] = mode
                data_dset.attrs["sum_step"] = int(step)
                data_dset.attrs["sum_operation"] = operation
                if range_start is not None:
                    data_dset.attrs["sum_range_start"] = int(range_start)
                if range_end is not None:
                    data_dset.attrs["sum_range_end"] = int(range_end)
                if normalize_frame is not None:
                    data_dset.attrs["sum_normalize_frame"] = int(normalize_frame)
                data_dset.attrs["source_dataset"] = str(dataset)
                data_dset.attrs["frame_count_in"] = int(frame_count)
                data_dset.attrs["frame_count_out"] = int(out_frame_count)
                data_dset.attrs["threshold_count"] = int(threshold_count)
                data_dset.attrs["signal"] = "data"

                chunk_start = np.asarray([int(group["start"]) for group in groups], dtype=np.int64)
                chunk_end = np.asarray([int(group["end"]) for group in groups], dtype=np.int64)
                chunk_count = np.asarray([int(group["count"]) for group in groups], dtype=np.int64)
                data_group.create_dataset("sum_start_frame", data=chunk_start)
                data_group.create_dataset("sum_end_frame", data=chunk_end)
                data_group.create_dataset("sum_frame_count", data=chunk_count)

                for thr, chunk_idx, _start_idx, _end_idx, _count, arr, mask_bits in sums:
                    arr_out = np.asarray(arr, dtype=np.float64)
                    if mask_bits is not None:
                        _, _, any_mask = _mask_slices(mask_bits)
                        arr_out = arr_out.copy()
                        arr_out[any_mask] = flag_value
                    if threshold_count > 1:
                        data_dset[chunk_idx, thr, :, :] = arr_out
                    else:
                        data_dset[chunk_idx, :, :] = arr_out

                if apply_mask:
                    base_mask_bits = mask_bits_by_thr[0] if mask_bits_by_thr else None
                    if base_mask_bits is not None:
                        mask_group = out_h5.require_group(
                            "/entry/instrument/detector/detectorSpecific"
                        )
                        mask_group.create_dataset(
                            "pixel_mask",
                            data=base_mask_bits.astype(np.uint32),
                            compression="gzip",
                        )
                    if threshold_count > 1:
                        detector_group = out_h5.require_group("/entry/instrument/detector")
                        for thr, mask_bits in enumerate(mask_bits_by_thr):
                            if mask_bits is None:
                                continue
                            thr_group = detector_group.require_group(f"threshold_{thr + 1}_channel")
                            thr_group.create_dataset(
                                "pixel_mask",
                                data=mask_bits.astype(np.uint32),
                                compression="gzip",
                            )
                outputs.append(str(out_file))
        elif output_format in {"tiff", "tif"}:
            base_name = base_target.stem or base_target.name or "series_sum"
            out_dir = base_target.parent
            use_float_tiff = operation in {"mean", "median"} or normalize_frame_idx is not None
            for thr, chunk_idx, start_idx, end_idx, frame_count_in_sum, arr, mask_bits in sums:
                thr_tag = f"_thr{thr + 1:02d}" if threshold_count > 1 else ""
                if mode == "all":
                    chunk_tag = "_all"
                elif mode == "nth":
                    chunk_tag = f"_every{step:03d}_n{frame_count_in_sum:05d}"
                else:
                    chunk_tag = f"_chunk{chunk_idx + 1:04d}_f{start_idx + 1:06d}-{end_idx + 1:06d}"
                out_file = out_dir / f"{base_name}{thr_tag}{chunk_tag}_{timestamp}.tiff"
                out_file = _next_available_path(out_file)
                if use_float_tiff:
                    arr_tiff = np.asarray(arr, dtype=np.float32)
                else:
                    arr_out = np.rint(np.asarray(arr, dtype=np.float64))
                    tiff_dtype: np.dtype = np.int32
                    max_val = float(np.nanmax(arr_out)) if arr_out.size else 0.0
                    if max_val > float(np.iinfo(np.int32).max):
                        tiff_dtype = np.int64
                    arr_tiff = arr_out.astype(tiff_dtype, casting="unsafe")
                if mask_bits is not None:
                    gap_mask, bad_mask, _ = _mask_slices(mask_bits)
                    arr_tiff = arr_tiff.copy()
                    arr_tiff[gap_mask] = -1
                    arr_tiff[bad_mask] = -2
                _write_tiff(out_file, np.asarray(arr_tiff))
                outputs.append(str(out_file))
        else:
            raise HTTPException(status_code=400, detail="Unsupported output format")

        _series_job_update(
            job_id,
            status="done",
            progress=1.0,
            message=f"Completed: wrote {len(outputs)} file(s)",
            outputs=outputs,
            done_at=time.time(),
        )
    except Exception as exc:
        logger.exception("Series summing failed: %s", exc)
        detail = exc.detail if isinstance(exc, HTTPException) else str(exc)
        _series_job_update(
            job_id,
            status="error",
            progress=1.0,
            message=f"Failed: {detail}",
            error=str(detail),
            done_at=time.time(),
        )


def _settings_payload() -> dict[str, Any]:
    return {
        "config": CONFIG,
        "defaults": DEFAULT_CONFIG,
        "path": str(CONFIG_PATH),
        "restart_required": True,
    }


def _apply_runtime_config(payload: dict[str, Any]) -> None:
    global CONFIG, ALLOW_ABS_PATHS, SCAN_CACHE_SEC, MAX_SCAN_DEPTH, MAX_UPLOAD_MB, MAX_UPLOAD_BYTES
    CONFIG = payload
    ALLOW_ABS_PATHS = get_bool(CONFIG, ("data", "allow_abs_paths"), True)
    SCAN_CACHE_SEC = get_float(CONFIG, ("data", "scan_cache_sec"), 2.0)
    MAX_SCAN_DEPTH = get_int(CONFIG, ("data", "max_scan_depth"), -1)
    MAX_UPLOAD_MB = max(0, get_int(CONFIG, ("data", "max_upload_mb"), 0))
    MAX_UPLOAD_BYTES = MAX_UPLOAD_MB * 1024 * 1024 if MAX_UPLOAD_MB > 0 else 0


def _get_log_path() -> Path | None:
    return LOG_PATH


def _get_allow_abs_paths() -> bool:
    return ALLOW_ABS_PATHS


def _get_scan_cache_sec() -> float:
    return SCAN_CACHE_SEC


def _get_max_upload_bytes() -> int:
    return MAX_UPLOAD_BYTES


register_system_routes(
    app,
    SystemRouteDeps(
        version=ALBIS_VERSION,
        logger=logger,
        default_config=DEFAULT_CONFIG,
        config_path=CONFIG_PATH,
        settings_payload=_settings_payload,
        normalize_config=normalize_config,
        save_config=save_config,
        apply_runtime_config=_apply_runtime_config,
        get_log_path=_get_log_path,
        data_dir=DATA_DIR,
        get_allow_abs_paths=_get_allow_abs_paths,
        is_within=_is_within,
    ),
)


register_file_routes(
    app,
    FileRouteDeps(
        data_dir=DATA_DIR,
        autoload_exts=AUTOLOAD_EXTS,
        logger=logger,
        get_allow_abs_paths=_get_allow_abs_paths,
        get_scan_cache_sec=_get_scan_cache_sec,
        get_max_upload_bytes=_get_max_upload_bytes,
        resolve_dir=_resolve_dir,
        resolve_image_file=_resolve_image_file,
        is_within=_is_within,
        parse_ext_filter=_parse_ext_filter,
        latest_image_file=_latest_image_file,
        safe_rel_path=_safe_rel_path,
        scan_files=_scan_files,
        scan_folders=_scan_folders,
        image_ext_name=_image_ext_name,
        split_series_name=_split_series_name,
        strip_image_ext=_strip_image_ext,
    ),
)


register_stream_routes(
    app,
    StreamRouteDeps(
        logger=logger,
        resolve_image_file=_resolve_image_file,
        image_ext_name=_image_ext_name,
        read_tiff=_read_tiff,
        read_cbf=_read_cbf,
        read_cbf_gz=_read_cbf_gz,
        read_edf=_read_edf,
        pilatus_meta_from_tiff=_pilatus_meta_from_tiff,
        pilatus_meta_from_fabio=_pilatus_meta_from_fabio,
        pilatus_header_text=_pilatus_header_text,
        simplon_base=_simplon_base,
        simplon_set_mode=_simplon_set_mode,
        simplon_fetch_monitor=_simplon_fetch_monitor,
        simplon_fetch_pixel_mask=_simplon_fetch_pixel_mask,
        read_tiff_bytes_with_simplon_meta=_read_tiff_bytes_with_simplon_meta,
        remote_parse_meta=_remote_parse_meta,
        remote_safe_source_id=_remote_safe_source_id,
        remote_read_image_bytes=_remote_read_image_bytes,
        remote_extract_metadata=_remote_extract_metadata,
        remote_store_frame=_remote_store_frame,
        remote_snapshot=_remote_snapshot,
    ),
)


def _get_h5py() -> Any:
    _ensure_hdf5_stack()
    return h5py


register_hdf5_routes(
    app,
    HDF5RouteDeps(
        ensure_hdf5_stack=_ensure_hdf5_stack,
        get_h5py=_get_h5py,
        resolve_file=_resolve_file,
        walk_datasets=_walk_datasets,
        aggregate_linked_stack_datasets=_aggregate_linked_stack_datasets,
        collect_h5_attrs=_collect_h5_attrs,
        serialize_h5_value=_serialize_h5_value,
        dataset_value_preview=_dataset_value_preview,
        dataset_preview_array=_dataset_preview_array,
    ),
)


@app.get("/api/analysis/params")
def analysis_params(
    file: str = Query(..., min_length=1),
    dataset: str | None = Query(None),
) -> dict[str, Any]:
    _ensure_hdf5_stack()
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
                view, extra_files = _resolve_dataset_view(h5, file_path, dataset)
                try:
                    shape = tuple(int(x) for x in view["shape"])
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


@app.post("/api/analysis/series-sum/start")
def analysis_series_sum_start(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    """Start asynchronous series summing and return pollable job metadata."""
    file = str(payload.get("file", "")).strip()
    dataset = str(payload.get("dataset", "")).strip()
    mode = str(payload.get("mode", "all")).strip().lower()
    step = int(payload.get("step", 10) or 10)
    operation = str(payload.get("operation", "sum")).strip().lower()
    normalize_frame = payload.get("normalize_frame")
    normalize_frame = (
        int(normalize_frame)
        if normalize_frame is not None and str(normalize_frame).strip() != ""
        else None
    )
    range_start = payload.get("range_start")
    range_end = payload.get("range_end")
    range_start = (
        int(range_start) if range_start is not None and str(range_start).strip() != "" else None
    )
    range_end = int(range_end) if range_end is not None and str(range_end).strip() != "" else None
    output_path = payload.get("output_path")
    output_format = str(payload.get("format", "hdf5")).strip().lower()
    apply_mask = bool(payload.get("apply_mask", True))

    if not file:
        raise HTTPException(status_code=400, detail="Missing file")
    ext = _image_ext_name(Path(file).name)
    if ext in {".h5", ".hdf5"} and not dataset:
        raise HTTPException(status_code=400, detail="Missing dataset")
    if mode not in {"all", "step", "nth", "range"}:
        raise HTTPException(status_code=400, detail="Invalid mode")
    if operation not in {"sum", "mean", "median"}:
        raise HTTPException(status_code=400, detail="Invalid operation")
    if output_format not in {"hdf5", "h5", "tiff", "tif"}:
        raise HTTPException(status_code=400, detail="Invalid format")
    if step < 1:
        raise HTTPException(status_code=400, detail="Step must be >= 1")
    if range_start is not None and range_start < 1:
        raise HTTPException(status_code=400, detail="Range start must be >= 1")
    if range_end is not None and range_end < 1:
        raise HTTPException(status_code=400, detail="Range end must be >= 1")
    if range_start is not None and range_end is not None and range_start > range_end:
        raise HTTPException(status_code=400, detail="Range start must be <= range end")
    if normalize_frame is not None and normalize_frame < 1:
        raise HTTPException(status_code=400, detail="Normalize frame must be >= 1")

    job_id = uuid.uuid4().hex
    job_data = {
        "id": job_id,
        "status": "queued",
        "progress": 0.0,
        "message": "Queued",
        "created_at": time.time(),
        "updated_at": time.time(),
        "outputs": [],
        "error": None,
        "config": {
            "file": file,
            "dataset": dataset,
            "mode": mode,
            "step": step,
            "operation": operation,
            "normalize_frame": normalize_frame,
            "range_start": range_start,
            "range_end": range_end,
            "format": output_format,
            "apply_mask": apply_mask,
            "output_path": output_path,
        },
    }
    with _series_jobs_lock:
        _series_jobs[job_id] = job_data
        # keep memory bounded
        done_jobs = [
            jid for jid, info in _series_jobs.items() if info.get("status") in {"done", "error"}
        ]
        if len(done_jobs) > 200:
            done_jobs.sort(key=lambda jid: float(_series_jobs[jid].get("updated_at", 0.0)))
            for old_id in done_jobs[: len(done_jobs) - 200]:
                _series_jobs.pop(old_id, None)

    worker = threading.Thread(
        target=_run_series_summing_job,
        kwargs={
            "job_id": job_id,
            "file": file,
            "dataset": dataset,
            "mode": mode,
            "step": step,
            "operation": operation,
            "normalize_frame": normalize_frame,
            "range_start": range_start,
            "range_end": range_end,
            "output_path": str(output_path or ""),
            "output_format": output_format,
            "apply_mask": apply_mask,
        },
        daemon=True,
    )
    worker.start()
    return {"job_id": job_id, "status": "queued"}


@app.get("/api/analysis/series-sum/status")
def analysis_series_sum_status(job_id: str = Query(..., min_length=1)) -> dict[str, Any]:
    with _series_jobs_lock:
        job = _series_jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return dict(job)


@app.get("/api/metadata")
def metadata(
    file: str = Query(..., min_length=1), dataset: str = Query(..., min_length=1)
) -> dict[str, Any]:
    _ensure_hdf5_stack()
    path = _resolve_file(file)
    with h5py.File(path, "r") as h5:
        try:
            view, extra_files = _resolve_dataset_view(h5, path, dataset)
            try:
                shape = tuple(int(x) for x in view["shape"])
                response = {
                    "path": dataset,
                    "shape": shape,
                    "dtype": str(view["dtype"]),
                    "ndim": int(view["ndim"]),
                    "chunks": view["dataset"].chunks if view["kind"] == "dataset" else None,
                    "maxshape": view["dataset"].maxshape if view["kind"] == "dataset" else None,
                    "linked_stack": view["kind"] == "linked_stack",
                }
                if int(view["ndim"]) == 4:
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
    _ensure_hdf5_stack()
    path = _resolve_file(file)
    with h5py.File(path, "r") as h5:
        try:
            view, extra_files = _resolve_dataset_view(h5, path, dataset)
        except KeyError:
            raise HTTPException(status_code=404, detail="Dataset not found")
        try:
            frame_data = _extract_frame(view, index=index, threshold=threshold)
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
    _ensure_hdf5_stack()
    path = _resolve_file(file)
    with h5py.File(path, "r") as h5:
        try:
            view, extra_files = _resolve_dataset_view(h5, path, dataset)
        except KeyError:
            raise HTTPException(status_code=404, detail="Dataset not found")
        try:
            frame_data = _extract_frame(view, index=index, threshold=threshold)
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
    _ensure_hdf5_stack()
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


@app.middleware("http")
async def _no_cache_static(request: "Request", call_next):
    response = await call_next(request)
    if request.method == "GET":
        path = request.url.path
        if path in {"/", "/index.html", "/app.js", "/style.css", "/docs.html"}:
            response.headers["Cache-Control"] = "no-store"
    return response


STATIC_DIR = _resource_root() / "frontend"
app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")


if __name__ == "__main__":
    import uvicorn

    host = get_str(CONFIG, ("server", "host"), "127.0.0.1")
    port = max(1, get_int(CONFIG, ("server", "port"), 8000))
    reload = get_bool(CONFIG, ("server", "reload"), False)
    uvicorn.run("app:app", host=host, port=port, reload=reload)
