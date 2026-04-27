"""ALBIS backend API service.

This module keeps backend runtime logic centralized for simple packaged
execution. It handles path safety, image IO, metadata extraction, analysis
endpoints, monitor integration, and long-running series jobs.
"""

from __future__ import annotations

import fnmatch
import logging
import mimetypes
import os
import sys
import tempfile
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

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
        resolve_data_dir,
        resolve_log_dir,
        save_config,
    )
    from .image_formats import (
        _image_ext_name,
        _pilatus_header_text,
        _pilatus_image_geometry,
        _pilatus_meta_from_fabio,
        _pilatus_meta_from_image,
        _pilatus_meta_from_tiff,
        _read_cbf,
        _read_cbf_gz,
        _read_edf,
        _read_tiff,
        _read_tiff_bytes_with_simplon_meta,
        _resolve_series_files,
        _split_series_name,
        _strip_image_ext,
        _write_tiff,
    )
    from .routes.analysis import AnalysisRouteDeps, register_analysis_routes
    from .routes.files import FileRouteDeps, register_file_routes
    from .routes.frames import FrameRouteDeps, register_frame_routes
    from .routes.handoff import HandoffRouteDeps, register_handoff_routes
    from .routes.hdf5 import HDF5RouteDeps, register_hdf5_routes
    from .routes.stream import StreamRouteDeps, register_stream_routes
    from .routes.system import SystemRouteDeps, register_system_routes
    from .services.handoff_queue import HandoffQueueService
    from .services.hdf5_stack import HDF5StackService
    from .services.jungfraujoch_preview import JungfraujochPreviewBridge
    from .services.path_policy import PathPolicy
    from .services.remote_stream import (
        remote_extract_metadata as _remote_extract_metadata,
    )
    from .services.remote_stream import (
        remote_parse_meta as _remote_parse_meta,
    )
    from .services.remote_stream import (
        remote_read_image_bytes as _remote_read_image_bytes,
    )
    from .services.remote_stream import (
        remote_safe_source_id as _remote_safe_source_id,
    )
    from .services.remote_stream import (
        remote_snapshot as _remote_snapshot,
    )
    from .services.remote_stream import (
        remote_store_frame as _remote_store_frame,
    )
    from .services.root_scan_cache import RootScanCacheService
    from .services.series_ops import (
        iter_sum_groups as _iter_sum_groups,
    )
    from .services.series_ops import (
        mask_flag_value as _mask_flag_value,
    )
    from .services.series_ops import (
        mask_slices as _mask_slices,
    )
    from .services.series_summing import SeriesSummingDeps, SeriesSummingService
    from .services.simplon import (
        simplon_base as _simplon_base,
    )
    from .services.simplon import (
        simplon_fetch_monitor as _simplon_fetch_monitor,
    )
    from .services.simplon import (
        simplon_fetch_pixel_mask as _simplon_fetch_pixel_mask,
    )
    from .services.simplon import (
        simplon_set_mode as _simplon_set_mode,
    )
    from .services.update_check import ReleaseCheckService
    from .version import ALBIS_VERSION
except ImportError:  # pragma: no cover - supports `python backend/app.py`
    from config import (
        DEFAULT_CONFIG,
        get_bool,
        get_float,
        get_int,
        get_str,
        load_config,
        normalize_config,
        resolve_data_dir,
        resolve_log_dir,
        save_config,
    )
    from image_formats import (
        _image_ext_name,
        _pilatus_header_text,
        _pilatus_image_geometry,
        _pilatus_meta_from_fabio,
        _pilatus_meta_from_image,
        _pilatus_meta_from_tiff,
        _read_cbf,
        _read_cbf_gz,
        _read_edf,
        _read_tiff,
        _read_tiff_bytes_with_simplon_meta,
        _resolve_series_files,
        _split_series_name,
        _strip_image_ext,
        _write_tiff,
    )
    from routes.analysis import AnalysisRouteDeps, register_analysis_routes
    from routes.files import FileRouteDeps, register_file_routes
    from routes.frames import FrameRouteDeps, register_frame_routes
    from routes.handoff import HandoffRouteDeps, register_handoff_routes
    from routes.hdf5 import HDF5RouteDeps, register_hdf5_routes
    from routes.stream import StreamRouteDeps, register_stream_routes
    from routes.system import SystemRouteDeps, register_system_routes
    from services.handoff_queue import HandoffQueueService
    from services.hdf5_stack import HDF5StackService
    from services.jungfraujoch_preview import JungfraujochPreviewBridge
    from services.path_policy import PathPolicy
    from services.remote_stream import (
        remote_extract_metadata as _remote_extract_metadata,
    )
    from services.remote_stream import (
        remote_parse_meta as _remote_parse_meta,
    )
    from services.remote_stream import (
        remote_read_image_bytes as _remote_read_image_bytes,
    )
    from services.remote_stream import (
        remote_safe_source_id as _remote_safe_source_id,
    )
    from services.remote_stream import (
        remote_snapshot as _remote_snapshot,
    )
    from services.remote_stream import (
        remote_store_frame as _remote_store_frame,
    )
    from services.root_scan_cache import RootScanCacheService
    from services.series_ops import (
        iter_sum_groups as _iter_sum_groups,
    )
    from services.series_ops import (
        mask_flag_value as _mask_flag_value,
    )
    from services.series_ops import (
        mask_slices as _mask_slices,
    )
    from services.series_summing import SeriesSummingDeps, SeriesSummingService
    from services.simplon import (
        simplon_base as _simplon_base,
    )
    from services.simplon import (
        simplon_fetch_monitor as _simplon_fetch_monitor,
    )
    from services.simplon import (
        simplon_fetch_pixel_mask as _simplon_fetch_pixel_mask,
    )
    from services.simplon import (
        simplon_set_mode as _simplon_set_mode,
    )
    from services.update_check import ReleaseCheckService
    from version import ALBIS_VERSION

CONFIG, CONFIG_PATH = load_config()

DATA_DIR = resolve_data_dir(CONFIG, CONFIG_PATH)


@dataclass
class RuntimeState:
    config: dict[str, Any]
    config_path: Path
    data_dir: Path
    allow_abs_paths: bool = True
    scan_cache_sec: float = 2.0
    max_scan_depth: int = -1
    max_upload_mb: int = 0
    max_upload_bytes: int = 0

    def apply_config(self, payload: dict[str, Any]) -> None:
        self.config = payload
        self.allow_abs_paths = get_bool(self.config, ("data", "allow_abs_paths"), True)
        self.scan_cache_sec = get_float(self.config, ("data", "scan_cache_sec"), 2.0)
        self.max_scan_depth = get_int(self.config, ("data", "max_scan_depth"), -1)
        self.max_upload_mb = max(0, get_int(self.config, ("data", "max_upload_mb"), 0))
        self.max_upload_bytes = self.max_upload_mb * 1024 * 1024 if self.max_upload_mb > 0 else 0


runtime_state = RuntimeState(config=CONFIG, config_path=CONFIG_PATH, data_dir=DATA_DIR)
runtime_state.apply_config(CONFIG)


def _register_static_mime_types() -> None:
    """Pin frontend MIME types so static serving stays stable across platforms."""
    mimetypes.add_type("application/javascript", ".js", strict=True)
    mimetypes.add_type("application/javascript", ".mjs", strict=True)
    mimetypes.add_type("text/css", ".css", strict=True)


_register_static_mime_types()

app = FastAPI(title="ALBIS — ALBIS WEB VIEW", version=ALBIS_VERSION)

AUTOLOAD_EXTS = {".h5", ".hdf5", ".tif", ".tiff", ".cbf", ".cbf.gz", ".edf"}
LOG_DIR: Path | None = None
LOG_PATH: Path | None = None
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
    level_name = get_str(runtime_state.config, ("logging", "level"), "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logger = logging.getLogger("albis")
    if logger.handlers:
        return logger
    logger.setLevel(level)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    logger.addHandler(stream)

    log_dir = resolve_log_dir(runtime_state.config, runtime_state.config_path)
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        try:
            log_dir = Path(tempfile.gettempdir()) / "albis-logs"
            log_dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            print("WARNING: Could not create log directory; file logging disabled", file=sys.stderr)
            return logger
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
update_check_service = ReleaseCheckService(current_version=ALBIS_VERSION, logger=logger)
_startup_banner_logged = False
handoff_queue = HandoffQueueService(max_jobs=1024)
root_scan_cache = RootScanCacheService()


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    """Log startup details and cleanly stop background workers on shutdown."""
    global _startup_banner_logged
    if not _startup_banner_logged:
        _startup_banner_logged = True
        pid = os.getpid()
        logger.info("ALBIS data dir (pid=%s): %s", pid, runtime_state.data_dir)
        logger.info("ALBIS config (pid=%s): %s", pid, runtime_state.config_path)
        logger.info(
            "ALBIS frontend root (pid=%s): %s [index=%s app.js=%s js-mime=%s]",
            pid,
            STATIC_DIR,
            (STATIC_DIR / "index.html").exists(),
            (STATIC_DIR / "app.js").exists(),
            mimetypes.guess_type("app.js")[0],
        )
    try:
        yield
    finally:
        try:
            jfjoch_preview.stop()
        except Exception:
            logger.exception("Failed to stop JUNGFRAUJOCH preview bridge on shutdown")


app.router.lifespan_context = _lifespan


@app.middleware("http")
async def log_requests(request, call_next):
    """Emit request/response logs with severity based on status code and latency."""
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


path_policy = PathPolicy(
    data_dir=runtime_state.data_dir,
    autoload_exts=AUTOLOAD_EXTS,
    image_ext_name=_image_ext_name,
    allow_abs_paths=lambda: runtime_state.allow_abs_paths,
)


def _safe_rel_path(name: str) -> Path:
    """Validate user-provided relative paths and block traversal/dot-prefix segments."""
    return path_policy.safe_rel_path(name)


def _resolve_file(name: str) -> Path:
    """Resolve an HDF5 file from relative data-root paths or approved absolute paths."""
    return path_policy.resolve_hdf5_file(name)


def _resolve_dir(name: str | None) -> Path:
    """Resolve a browse/autoload directory with the same safety rules as file resolution."""
    return path_policy.resolve_dir(name)


def _resolve_optional_path(name: str) -> Path:
    """Resolve a relative data-root path or approved absolute path without requiring it to exist."""
    raw = Path(name)
    if raw.is_absolute():
        if not runtime_state.allow_abs_paths:
            raise HTTPException(status_code=400, detail="Absolute paths are disabled")
        return raw.expanduser().resolve()
    safe = path_policy.safe_rel_path(name)
    root = runtime_state.data_dir.resolve()
    path = (runtime_state.data_dir / safe).resolve()
    if not path_policy.is_within(path, root):
        raise HTTPException(status_code=400, detail="Invalid file name")
    return path


def _resolve_image_file(name: str) -> Path:
    """Resolve any supported image file path constrained by runtime path policy."""
    return path_policy.resolve_image_file(name)


def _is_within(path: Path, root: Path) -> bool:
    """Return True when `path` is inside `root` after normalization."""
    return PathPolicy.is_within(path, root)


def _parse_ext_filter(exts: str | None) -> set[str]:
    """Normalize comma-separated extension filters to the supported autoload set."""
    return path_policy.parse_ext_filter(exts)


def _iter_entries(root: Path, max_depth: int | None):
    """Depth-limited directory walk that skips hidden entries and symlink traversal."""
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
    """Collect unique relative image-file paths under `root`."""
    items: list[str] = []
    max_depth = None if runtime_state.max_scan_depth < 0 else runtime_state.max_scan_depth
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
    """Collect unique relative subfolders under `root` respecting scan depth limits."""
    dirs: set[str] = set()
    max_depth = None if runtime_state.max_scan_depth < 0 else runtime_state.max_scan_depth
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
    """Find the newest image file under `root` after extension/pattern filtering."""
    latest_path: Path | None = None
    latest_mtime = -1.0
    max_depth = None if runtime_state.max_scan_depth < 0 else runtime_state.max_scan_depth
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
    data_dir=runtime_state.data_dir,
    get_allow_abs_paths=lambda: runtime_state.allow_abs_paths,
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
_walk_datasets = hdf5_stack.walk_datasets
_linked_member_sort_key = hdf5_stack.linked_member_sort_key
_aggregate_linked_stack_datasets = hdf5_stack.aggregate_linked_stack_datasets
_resolve_node = hdf5_stack.resolve_node
_resolve_dataset = hdf5_stack.resolve_dataset
_resolve_dataset_view = hdf5_stack.resolve_dataset_view
_extract_frame = hdf5_stack.extract_frame

series_summing = SeriesSummingService(
    SeriesSummingDeps(
        data_dir=runtime_state.data_dir,
        get_allow_abs_paths=lambda: runtime_state.allow_abs_paths,
        is_within=_is_within,
        logger=logger,
        ensure_hdf5_stack=_ensure_hdf5_stack,
        get_h5py=_get_h5py,
        resolve_image_file=_resolve_image_file,
        image_ext_name=_image_ext_name,
        resolve_series_files=_resolve_series_files,
        read_tiff=_read_tiff,
        read_cbf=_read_cbf,
        read_cbf_gz=_read_cbf_gz,
        read_edf=_read_edf,
        write_tiff=_write_tiff,
        iter_sum_groups=_iter_sum_groups,
        mask_flag_value=_mask_flag_value,
        mask_slices=_mask_slices,
        resolve_dataset_view=_resolve_dataset_view,
        extract_frame=_extract_frame,
        find_pixel_mask=_find_pixel_mask,
    )
)

jfjoch_preview = JungfraujochPreviewBridge(
    logger=logger,
    remote_store_frame=_remote_store_frame,
)


def _settings_payload() -> dict[str, Any]:
    """Build the API payload used by settings GET/POST handlers."""
    return {
        "config": runtime_state.config,
        "defaults": DEFAULT_CONFIG,
        "path": str(runtime_state.config_path),
        "restart_required": True,
    }


def _apply_runtime_config(payload: dict[str, Any]) -> None:
    runtime_state.apply_config(payload)


def _get_log_path() -> Path | None:
    return LOG_PATH


def _check_update():
    return update_check_service.check_for_update()


def _get_allow_abs_paths() -> bool:
    return runtime_state.allow_abs_paths


def _get_scan_cache_sec() -> float:
    return runtime_state.scan_cache_sec


def _get_max_upload_bytes() -> int:
    return runtime_state.max_upload_bytes


register_system_routes(
    app,
    SystemRouteDeps(
        version=ALBIS_VERSION,
        logger=logger,
        default_config=DEFAULT_CONFIG,
        config_path=runtime_state.config_path,
        settings_payload=_settings_payload,
        normalize_config=normalize_config,
        save_config=save_config,
        apply_runtime_config=_apply_runtime_config,
        get_log_path=_get_log_path,
        check_update=_check_update,
    ),
)

register_handoff_routes(
    app,
    HandoffRouteDeps(
        logger=logger,
        queue_job=handoff_queue.queue_job,
        latest_job=handoff_queue.latest_job,
    ),
)


register_file_routes(
    app,
    FileRouteDeps(
        data_dir=runtime_state.data_dir,
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
        get_cached_root_files=root_scan_cache.get_root_files,
        get_cached_root_folders=root_scan_cache.get_root_folders,
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
        resolve_optional_path=_resolve_optional_path,
        image_ext_name=_image_ext_name,
        read_tiff=_read_tiff,
        read_cbf=_read_cbf,
        read_cbf_gz=_read_cbf_gz,
        read_edf=_read_edf,
        pilatus_meta_from_tiff=_pilatus_meta_from_tiff,
        pilatus_meta_from_fabio=_pilatus_meta_from_fabio,
        pilatus_header_text=_pilatus_header_text,
        pilatus_image_geometry=_pilatus_image_geometry,
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
        jfjoch_preview_start=jfjoch_preview.start,
        jfjoch_preview_stop=jfjoch_preview.stop,
        jfjoch_preview_status=jfjoch_preview.status,
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
        resolve_optional_path=_resolve_optional_path,
        resolve_dataset_view=_resolve_dataset_view,
        read_scalar=_read_scalar,
        image_ext_name=_image_ext_name,
        pilatus_meta_from_image=_pilatus_meta_from_image,
        to_mm=_to_mm,
        to_um=_to_um,
        to_ev=_to_ev,
        wavelength_to_ev=_wavelength_to_ev,
        norm_unit=_norm_unit,
        read_threshold_energies=_read_threshold_energies,
        start_series_sum_job=series_summing.start_job,
        get_series_sum_job=series_summing.get_job,
        cancel_series_sum_job=series_summing.cancel_job,
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
    """Resolve static asset root for both source and bundled (PyInstaller) runs."""
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)
    return Path(__file__).resolve().parents[1]


@app.middleware("http")
async def _no_cache_static(request: Request, call_next):
    """Disable browser caching for core frontend entry assets during development/updates."""
    response = await call_next(request)
    if request.method == "GET":
        path = request.url.path
        if path == "/" or (
            not path.startswith(("/api/", "/assets/"))
            and path.endswith((".html", ".js", ".css", ".json"))
        ):
            response.headers["Cache-Control"] = "no-store"
    return response


STATIC_DIR = _resource_root() / "frontend"
ASSET_DIR = _resource_root() / "albis_assets"
if ASSET_DIR.exists():
    app.mount("/assets", StaticFiles(directory=ASSET_DIR), name="assets")
app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")


if __name__ == "__main__":
    import uvicorn

    host = get_str(runtime_state.config, ("server", "host"), "127.0.0.1")
    port = max(0, min(65535, get_int(runtime_state.config, ("server", "port"), 0)))
    reload = get_bool(runtime_state.config, ("server", "reload"), False)
    uvicorn.run("app:app", host=host, port=port, reload=reload)
