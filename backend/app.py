from __future__ import annotations

"""ALBIS backend API service.

This module keeps backend runtime logic centralized for simple packaged
execution. It handles path safety, image IO, metadata extraction, analysis
endpoints, monitor integration, and long-running series jobs.
"""

import fnmatch
import os
import sys
import threading
import time
from pathlib import Path
from typing import Any

import logging
from logging.handlers import RotatingFileHandler
import tempfile
import numpy as np
from fastapi import FastAPI, HTTPException, Request
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
    from .services.hdf5_stack import HDF5StackService
    from .services.simplon import (
        simplon_base as _simplon_base,
        simplon_fetch_monitor as _simplon_fetch_monitor,
        simplon_fetch_pixel_mask as _simplon_fetch_pixel_mask,
        simplon_set_mode as _simplon_set_mode,
    )
    from .routes.hdf5 import HDF5RouteDeps, register_hdf5_routes
    from .routes.analysis import AnalysisRouteDeps, register_analysis_routes
    from .routes.frames import FrameRouteDeps, register_frame_routes
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
    from services.hdf5_stack import HDF5StackService
    from services.simplon import (
        simplon_base as _simplon_base,
        simplon_fetch_monitor as _simplon_fetch_monitor,
        simplon_fetch_pixel_mask as _simplon_fetch_pixel_mask,
        simplon_set_mode as _simplon_set_mode,
    )
    from routes.hdf5 import HDF5RouteDeps, register_hdf5_routes
    from routes.analysis import AnalysisRouteDeps, register_analysis_routes
    from routes.frames import FrameRouteDeps, register_frame_routes
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


def _get_h5py() -> Any:
    _ensure_hdf5_stack()
    return h5py


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


hdf5_stack = HDF5StackService(
    data_dir=DATA_DIR,
    get_allow_abs_paths=lambda: ALLOW_ABS_PATHS,
    is_within=_is_within,
    get_h5py=_get_h5py,
)

_resolve_external_path = hdf5_stack.resolve_external_path
_dataset_info = hdf5_stack.dataset_info
_is_image_dataset = hdf5_stack.is_image_dataset
_serialize_h5_value = hdf5_stack.serialize_h5_value
_collect_h5_attrs = hdf5_stack.collect_h5_attrs
_dataset_value_preview = hdf5_stack.dataset_value_preview
_dataset_preview_array = hdf5_stack.dataset_preview_array
_find_pixel_mask = hdf5_stack.find_pixel_mask
_read_scalar = hdf5_stack.read_scalar
_norm_unit = hdf5_stack.norm_unit
_to_mm = hdf5_stack.to_mm
_to_um = hdf5_stack.to_um
_to_ev = hdf5_stack.to_ev
_wavelength_to_ev = hdf5_stack.wavelength_to_ev
_read_threshold_energies = hdf5_stack.read_threshold_energies


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


_walk_datasets = hdf5_stack.walk_datasets
_linked_member_sort_key = hdf5_stack.linked_member_sort_key
_aggregate_linked_stack_datasets = hdf5_stack.aggregate_linked_stack_datasets
_resolve_node = hdf5_stack.resolve_node
_resolve_dataset = hdf5_stack.resolve_dataset
_resolve_dataset_view = hdf5_stack.resolve_dataset_view
_extract_frame = hdf5_stack.extract_frame


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

register_analysis_routes(
    app,
    AnalysisRouteDeps(
        ensure_hdf5_stack=_ensure_hdf5_stack,
        get_h5py=_get_h5py,
        resolve_file=_resolve_file,
        resolve_dataset_view=_resolve_dataset_view,
        read_scalar=_read_scalar,
        to_mm=_to_mm,
        to_um=_to_um,
        to_ev=_to_ev,
        wavelength_to_ev=_wavelength_to_ev,
        norm_unit=_norm_unit,
        read_threshold_energies=_read_threshold_energies,
        run_series_summing_job=_run_series_summing_job,
        series_jobs=_series_jobs,
        series_jobs_lock=_series_jobs_lock,
    ),
)


register_frame_routes(
    app,
    FrameRouteDeps(
        ensure_hdf5_stack=_ensure_hdf5_stack,
        get_h5py=_get_h5py,
        resolve_file=_resolve_file,
        resolve_dataset_view=_resolve_dataset_view,
        extract_frame=_extract_frame,
        find_pixel_mask=_find_pixel_mask,
        read_threshold_energies=_read_threshold_energies,
    ),
)


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
