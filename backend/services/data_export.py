"""Background data-format export/conversion service."""

from __future__ import annotations

import contextlib
import os
import shutil
import sys
import threading
import time
import uuid
from collections import deque
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any

import numpy as np
from fastapi import HTTPException

from .data_export_frames import UNSIGNED_TYPES, DataExportFrameMixin
from .data_export_live import DataExportLiveMixin
from .data_export_metadata import DataExportMetadataMixin
from .data_export_names import DataExportNamesMixin
from .flatfield import Flat, prepare_flat


class _DataExportCancelledError(Exception):
    """Internal control-flow exception for user requested cancellation."""


@dataclass(frozen=True)
class DataExportDeps:
    data_dir: Path
    get_allow_abs_paths: Callable[[], bool]
    is_within: Callable[[Path, Path], bool]
    logger: Any
    ensure_hdf5_stack: Callable[[], None]
    get_h5py: Callable[[], Any]
    resolve_image_file: Callable[[str], Path]
    image_ext_name: Callable[[str], str]
    resolve_series_files: Callable[[Path], tuple[list[Path], int]]
    read_tiff: Callable[[Path, int], np.ndarray]
    read_cbf: Callable[[Path], np.ndarray]
    read_cbf_gz: Callable[[Path], np.ndarray]
    read_edf: Callable[[Path], np.ndarray]
    write_tiff: Callable[[Path, np.ndarray, dict[str, Any] | None], None]
    write_cbf: Callable[[Path, np.ndarray, dict[str, Any] | None], None]
    resolve_dataset_view: Callable[[Any, Path, str], tuple[dict[str, Any], list[Any]]]
    extract_frame: Callable[[dict[str, Any], int, int], np.ndarray]
    find_pixel_mask: Callable[[Any, int | None], Any | None]
    mask_slices: Callable[[np.ndarray], tuple[np.ndarray, np.ndarray, np.ndarray]]
    read_scalar: Callable[[Any, list[str]], tuple[float | None, str | None]]
    to_mm: Callable[[float, str | None], float]
    to_um: Callable[[float, str | None], float]
    to_ev: Callable[[float, str | None], float]
    wavelength_to_ev: Callable[[float, str | None], float | None]
    pilatus_meta_from_image: Callable[[Path], dict[str, Any]]
    pilatus_header_text: Callable[[Path], str]
    # Pixels, parsed header and header text of a single-image file, from one
    # read of it.
    read_image_with_header: Callable[[Path], tuple[np.ndarray, dict[str, Any], str]]


def _performance_cores() -> int | None:
    """Apple silicon's performance cores, or None where there is no such
    split (Intel Macs, Windows, Linux) or it cannot be read."""
    if sys.platform != "darwin":
        return None
    try:
        import ctypes
        import ctypes.util

        libc = ctypes.CDLL(ctypes.util.find_library("c"))
        value = ctypes.c_int(0)
        size = ctypes.c_size_t(ctypes.sizeof(value))
        if libc.sysctlbyname(
            b"hw.perflevel0.physicalcpu", ctypes.byref(value), ctypes.byref(size), None, 0
        ):
            return None
        return value.value if value.value > 0 else None
    except (OSError, AttributeError, TypeError):
        return None


def default_export_workers(cpus: int | None, performance_cores: int | None) -> int:
    """Frames converted at once, at most. The work is numpy and file I/O, which
    run outside the GIL, so threads share the cores without copying frames
    between processes. Two cores are left to the rest of ALBIS -- the viewer,
    the live monitor, detector control -- which an export would otherwise slow
    down. On Apple silicon no more than its performance cores: a worker on an
    efficiency core holds up the others (on a 4 + 4 core Mac, processing took
    9.2 ms a frame on four threads and 12.1 ms on six)."""
    workers = min(8, (cpus or 2) - 2)
    if performance_cores:
        workers = min(workers, performance_cores)
    return max(1, workers)


EXPORT_WORKERS = default_export_workers(os.cpu_count(), _performance_cores())
# Working memory a frame takes while it is converted, per pixel: flatfield and
# inpainting hold float64 copies and full-size index arrays (0.84 GB for an
# 18 Mpx EIGER2 16M frame), plus the frames read and waiting.
BYTES_PER_PIXEL_IN_FLIGHT = 48


def _physical_memory() -> int | None:
    try:
        return int(os.sysconf("SC_PAGE_SIZE")) * int(os.sysconf("SC_PHYS_PAGES"))
    except (AttributeError, OSError, ValueError):
        return None  # Windows: the fixed budget below


def export_workers(pixels: int, memory: int | None = None) -> int:
    """How many frames of `pixels` to convert at once: as many as the cores
    allow, as long as they fit a working-memory budget of 2 GB (a quarter of
    the machine's memory, where that is less). A 1 Mpx frame gets every
    worker; an EIGER2 16M frame two, where seven would have needed 7 GB."""
    memory = _physical_memory() if memory is None else memory
    budget = min(2 * 1024**3, memory // 4) if memory else 2 * 1024**3
    # Its own working memory, and the two raw frames it has waiting.
    per_frame = max(1, int(pixels)) * BYTES_PER_PIXEL_IN_FLIGHT
    return max(1, min(EXPORT_WORKERS, budget // per_frame))


class DataExportService(
    DataExportNamesMixin,
    DataExportFrameMixin,
    DataExportMetadataMixin,
    DataExportLiveMixin,
):
    """Threaded job manager for 2D detector data conversion.

    The parts that own one thing each live in mixins of their own: file names
    (data_export_names), the frame as written (data_export_frames), the source's
    header facts (data_export_metadata) and frames the browser sends
    (data_export_live). This class runs the jobs and writes the files."""

    def __init__(self, deps: DataExportDeps, max_finished_jobs: int = 200) -> None:
        self._deps = deps
        self._jobs: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()
        # The last flat prepared, by path: (path, mtime) -> Flat.
        self._flat_cache: dict[str, tuple[tuple[str, int], Flat]] = {}
        self._max_finished_jobs = max(10, int(max_finished_jobs))

    def start_job(
        self,
        *,
        file: str,
        dataset: str,
        output_format: str,
        output_dir: str | None,
        output_prefix: str | None,
        frame_mode: str,
        frame_start: int | None,
        frame_end: int | None,
        threshold_mode: str,
        threshold_index: int | None,
        overwrite: bool,
        inpaint: str = "off",
        inpaint_margin: int = 0,
        inpaint_method: str = "interpolate",
        inpaint_value: float = 0.0,
        inpaint_noise: bool = False,
        flatfield: str = "",
        flatfield_scale: str = "counts",
        data_type: str = "auto",
    ) -> str:
        job_id = uuid.uuid4().hex
        job_data = {
            "id": job_id,
            "status": "queued",
            "progress": 0.0,
            "message": "Queued",
            "created_at": time.time(),
            "updated_at": time.time(),
            "outputs": [],
            # Pixels clipped to fit the data type asked for, and in how many files.
            "clipped_pixels": 0,
            "clipped_files": 0,
            "error": None,
            "cancel_requested": False,
            "config": {
                "file": file,
                "dataset": dataset,
                "format": output_format,
                "output_dir": output_dir or "",
                "output_prefix": output_prefix or "",
                "frame_mode": frame_mode,
                "frame_start": frame_start,
                "frame_end": frame_end,
                "threshold_mode": threshold_mode,
                "threshold_index": threshold_index,
                "overwrite": bool(overwrite),
                "inpaint": inpaint,
                "inpaint_margin": inpaint_margin,
                "inpaint_method": inpaint_method,
                "inpaint_value": inpaint_value,
                "inpaint_noise": inpaint_noise,
                "flatfield": flatfield,
                "flatfield_scale": flatfield_scale,
                "data_type": data_type,
            },
        }
        with self._lock:
            self._jobs[job_id] = job_data
            self._trim_finished_jobs()

        worker = threading.Thread(
            target=self._run_job,
            kwargs={
                "job_id": job_id,
                "file": file,
                "dataset": dataset,
                "output_format": output_format,
                "output_dir": output_dir or "",
                "output_prefix": output_prefix or "",
                "frame_mode": frame_mode,
                "frame_start": frame_start,
                "frame_end": frame_end,
                "threshold_mode": threshold_mode,
                "threshold_index": threshold_index,
                "overwrite": bool(overwrite),
                "inpaint": inpaint,
                "inpaint_margin": inpaint_margin,
                "inpaint_method": inpaint_method,
                "inpaint_value": inpaint_value,
                "inpaint_noise": inpaint_noise,
                "flatfield": flatfield,
                "flatfield_scale": flatfield_scale,
                "data_type": data_type,
            },
            daemon=True,
        )
        worker.start()
        return job_id

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return None
            return dict(job)

    def cancel_job(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return False
            status = str(job.get("status") or "")
            if status in {"done", "error", "cancelled"}:
                return False
            job["cancel_requested"] = True
            job["message"] = "Cancellation requested..."
            job["updated_at"] = time.time()
            return True

    def _is_cancel_requested(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return False
            return bool(job.get("cancel_requested"))

    def _raise_if_cancelled(self, job_id: str) -> None:
        if self._is_cancel_requested(job_id):
            raise _DataExportCancelledError("Cancelled by user")

    def has_running_job(self) -> bool:
        """Whether a conversion is still writing files."""
        with self._lock:
            return any(
                str(job.get("status") or "") in {"queued", "running"} for job in self._jobs.values()
            )

    def _trim_finished_jobs(self) -> None:
        done_jobs = [
            jid
            for jid, info in self._jobs.items()
            if info.get("status") in {"done", "error", "cancelled"}
        ]
        if len(done_jobs) <= self._max_finished_jobs:
            return
        done_jobs.sort(key=lambda jid: float(self._jobs[jid].get("updated_at", 0.0)))
        for old_id in done_jobs[: len(done_jobs) - self._max_finished_jobs]:
            self._jobs.pop(old_id, None)

    def _update_job(self, job_id: str, **changes: Any) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            job.update(changes)
            job["updated_at"] = time.time()

    def _clipped_note(self, job_id: str) -> str:
        with self._lock:
            job = self._jobs.get(job_id) or {}
            pixels = int(job.get("clipped_pixels") or 0)
            files = int(job.get("clipped_files") or 0)
        return f"; {pixels} pixel(s) clipped in {files} file(s)" if pixels else ""

    @staticmethod
    def _check_room(
        out_dir: Path, files: int, shape: tuple[int, ...], output_format: str, data_type: str
    ) -> None:
        """Refuse a TIFF export the disk cannot hold, before its first file
        rather than part-way through a long series. A TIFF here is
        uncompressed, so its size is known; a CBF's depends on the data."""
        if output_format == "cbf" or len(shape) < 2:
            return
        try:
            free = shutil.disk_usage(out_dir).free
        except OSError:
            return
        per_pixel = 2 if data_type == "uint16" else 4
        # A header of a few kB per file on top of the pixels.
        needed = files * (int(shape[-2]) * int(shape[-1]) * per_pixel + 16_384)
        if needed > free:
            raise HTTPException(
                status_code=507,
                detail=f"Not enough disk space: about {needed / 1e9:.1f} GB needed, "
                f"{free / 1e9:.1f} GB free in {out_dir}",
            )

    def _count_clipped(self, job_id: str, clipped: int) -> None:
        if not clipped:
            return
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job["clipped_pixels"] = int(job.get("clipped_pixels") or 0) + clipped
                job["clipped_files"] = int(job.get("clipped_files") or 0) + 1

    def _resolve_output_dir(self, output_dir: str | None, source_path: Path) -> Path:
        raw = (output_dir or "").strip()
        if raw:
            target = Path(raw).expanduser()
            if not target.is_absolute():
                target = (self._deps.data_dir / target).resolve()
            else:
                target = target.resolve()
        else:
            target = (source_path.parent / f"{source_path.stem}_export").resolve()

        allowed_root = self._deps.data_dir.resolve()
        if not self._deps.get_allow_abs_paths() and not self._deps.is_within(target, allowed_root):
            raise HTTPException(
                status_code=400, detail="Output directory is outside data directory"
            )
        if target.exists() and not target.is_dir():
            raise HTTPException(status_code=400, detail="Output path is not a directory")
        target.mkdir(parents=True, exist_ok=True)
        return target

    def _write_frames(
        self,
        job_id: str,
        tasks: Iterator[Callable[[], tuple[Path, int]]],
        total: int,
        pixels: int,
    ) -> list[str]:
        """Run the frame `tasks` (each writes one file) on worker threads, as
        many as frames of `pixels` fit (export_workers); returns the files in
        the order of the tasks. The tasks are drawn as there is room, so only a
        few frames are in memory at once, and the job's progress, clipped pixels
        and cancellation are handled here.

        Stopped -- cancelled, or a frame failed -- the frames already being
        converted finish, and their files are removed again: the folder ends
        where the export says it did, not a few files further on."""
        workers = export_workers(pixels)
        outputs: list[str] = []
        pending: deque[Future[tuple[Path, int]]] = deque()

        def finish_oldest() -> None:
            path, clipped = pending.popleft().result()
            outputs.append(str(path))
            self._count_clipped(job_id, clipped)
            self._update_job(
                job_id,
                progress=len(outputs) / total,
                message=f"Exported {len(outputs)}/{total} frame(s)",
            )

        pool = ThreadPoolExecutor(workers, thread_name_prefix="albis-export")
        try:
            for task in tasks:
                self._raise_if_cancelled(job_id)
                pending.append(pool.submit(task))
                if len(pending) >= 2 * workers:
                    finish_oldest()
            while pending:
                self._raise_if_cancelled(job_id)
                finish_oldest()
        except BaseException:
            for future in pending:
                future.cancel()
            pool.shutdown(wait=True)
            for future in pending:
                if future.cancelled() or future.exception() is not None:
                    continue
                with contextlib.suppress(OSError):
                    future.result()[0].unlink()
            raise
        finally:
            pool.shutdown(wait=True)
        return outputs

    def _write_frame(
        self,
        *,
        out_dir: Path,
        name: str,
        output_format: str,
        arr: np.ndarray,
        mask_bits: np.ndarray | None = None,
        metadata: dict[str, Any] | None = None,
        overwrite: bool,
        inpaint: str = "off",
        inpaint_margin: int = 0,
        inpaint_method: str = "interpolate",
        inpaint_value: float = 0.0,
        inpaint_noise: bool = False,
        flatfield: str = "",
        flatfield_scale: str = "counts",
        data_type: str = "auto",
    ) -> tuple[Path, int]:
        """Write one frame; returns the file and how many pixels were clipped
        to fit `data_type`."""
        suffix = ".cbf" if output_format == "cbf" else ".tiff"
        path = out_dir / f"{name}{suffix}"
        if path.exists() and not overwrite:
            path = self._next_available_path(path)
        saturated_value = self._finite_int(metadata.get("saturation_value")) if metadata else None
        flat = self._load_flat(flatfield) if flatfield else None
        if flat is not None and flatfield_scale == "transmission" and output_format == "cbf":
            raise HTTPException(
                status_code=400,
                detail="Transmission values need TIFF: CBF holds whole numbers only",
            )
        if data_type not in {"auto", *UNSIGNED_TYPES}:
            raise HTTPException(status_code=400, detail="Invalid data type")
        if data_type != "auto" and output_format == "cbf":
            raise HTTPException(
                status_code=400, detail="Unsigned data types need TIFF: CBF is signed 32-bit"
            )
        if data_type != "auto" and flat is not None and flatfield_scale == "transmission":
            raise HTTPException(
                status_code=400,
                detail="Transmission values need floating point: choose Automatic",
            )
        frame, filled, clipped = self._normalize_frame(
            arr,
            mask_bits=mask_bits,
            saturated_value=saturated_value,
            inpaint=inpaint,
            inpaint_margin=inpaint_margin,
            inpaint_method=inpaint_method,
            inpaint_value=inpaint_value,
            inpaint_noise=inpaint_noise,
            flat=flat,
            flatfield_scale=flatfield_scale,
            data_type=data_type,
            # A corrected value is rarely a whole number: TIFF keeps it.
            keep_float=flat is not None and output_format != "cbf",
        )
        if flat is not None:
            metadata = {
                **(metadata or {}),
                "flatfield": Path(flatfield).name,
                "flatfield_scale": flatfield_scale,
            }
        if filled:
            # The header says which pixels hold values that were not measured.
            metadata = {
                **(metadata or {}),
                "inpainted": inpaint,
                "inpainted_margin": int(inpaint_margin),
                "inpainted_method": inpaint_method,
                "inpainted_value": float(inpaint_value),
                "inpainted_noise": bool(inpaint_noise),
            }
        if data_type != "auto":
            # The header says what the type did to the values.
            marker = int(np.iinfo(frame.dtype).max)
            metadata = {
                **(metadata or {}),
                "data_type": data_type,
                "marker_value": marker,
                "clipped_pixels": clipped,
            }
            # A clipped count sits at the top value: from there on the header's
            # count cutoff must call it overloaded, not measured.
            cutoff = self._finite_int(metadata.get("count_cutoff"))
            if (cutoff is not None and cutoff > marker - 1) or (cutoff is None and clipped):
                metadata["count_cutoff"] = marker - 1
        if output_format == "cbf":
            self._deps.write_cbf(path, frame, metadata)
        else:
            self._deps.write_tiff(path, frame, metadata)
        return path, clipped

    def _load_flat(self, flatfield: str) -> Flat:
        """The flat at `flatfield`, prepared once and kept while the file is
        unchanged: a series export divides every frame by the same one."""
        path = self._deps.resolve_image_file(flatfield)
        stamp = (str(path), path.stat().st_mtime_ns)
        with self._lock:
            cached = self._flat_cache.get(stamp[0])
            if cached and cached[0] == stamp:
                return cached[1]
        flat = prepare_flat(self._read_non_h5_image(path))
        if flat is None:
            raise HTTPException(status_code=400, detail="The flatfield has no usable pixels")
        with self._lock:
            self._flat_cache = {stamp[0]: (stamp, flat)}
        return flat

    @staticmethod
    def _frame_indices(
        frame_count: int,
        mode: str,
        frame_start: int | None,
        frame_end: int | None,
        current_index: int = 0,
    ) -> list[int]:
        if frame_count <= 0:
            raise HTTPException(status_code=400, detail="No frames available")
        if mode == "all":
            return list(range(frame_count))
        if mode == "current":
            idx = int(frame_start - 1) if frame_start is not None else int(current_index)
            if idx < 0 or idx >= frame_count:
                raise HTTPException(status_code=416, detail="Frame index out of range")
            return [idx]
        if mode == "range":
            start = int(frame_start or 1)
            end = int(frame_end or frame_count)
            if start < 1 or end < 1 or start > end:
                raise HTTPException(status_code=400, detail="Invalid frame range")
            start_idx = max(0, start - 1)
            end_idx = min(frame_count - 1, end - 1)
            if start_idx > end_idx:
                raise HTTPException(status_code=416, detail="Frame range is out of range")
            return list(range(start_idx, end_idx + 1))
        raise HTTPException(status_code=400, detail="Invalid frame mode")

    @staticmethod
    def _threshold_indices(
        threshold_count: int,
        mode: str,
        threshold_index: int | None,
    ) -> list[int]:
        if threshold_count <= 0:
            return [0]
        if mode == "all":
            return list(range(threshold_count))
        if mode == "current":
            idx = int(threshold_index or 1) - 1
            if idx < 0 or idx >= threshold_count:
                raise HTTPException(status_code=416, detail="Threshold index out of range")
            return [idx]
        raise HTTPException(status_code=400, detail="Invalid threshold mode")

    def _read_non_h5_image(self, path: Path) -> np.ndarray:
        ext_name = self._deps.image_ext_name(path.name)
        if ext_name in {".tif", ".tiff"}:
            return self._deps.read_tiff(path, index=0)
        if ext_name == ".cbf":
            return self._deps.read_cbf(path)
        if ext_name == ".cbf.gz":
            return self._deps.read_cbf_gz(path)
        if ext_name == ".edf":
            return self._deps.read_edf(path)
        raise HTTPException(status_code=400, detail="Unsupported source format")

    def _run_hdf5_export(
        self,
        *,
        job_id: str,
        source_path: Path,
        dataset: str,
        output_format: str,
        out_dir: Path,
        output_prefix: str,
        frame_mode: str,
        frame_start: int | None,
        frame_end: int | None,
        threshold_mode: str,
        threshold_index: int | None,
        overwrite: bool,
        inpaint: str = "off",
        inpaint_margin: int = 0,
        inpaint_method: str = "interpolate",
        inpaint_value: float = 0.0,
        inpaint_noise: bool = False,
        flatfield: str = "",
        flatfield_scale: str = "counts",
        data_type: str = "auto",
    ) -> list[str]:
        if not dataset:
            raise HTTPException(status_code=400, detail="Missing dataset")
        self._deps.ensure_hdf5_stack()
        h5py = self._deps.get_h5py()
        outputs: list[str] = []
        with h5py.File(source_path, "r") as h5:
            view, extra_files = self._deps.resolve_dataset_view(h5, source_path, dataset)
            try:
                shape = tuple(int(v) for v in view["shape"])
                ndim = int(view["ndim"])
                if ndim == 2:
                    frame_count = 1
                    threshold_count = 1
                elif ndim == 3:
                    frame_count = int(shape[0])
                    threshold_count = 1
                elif ndim == 4:
                    frame_count = int(shape[0])
                    threshold_count = int(shape[1])
                else:
                    raise HTTPException(status_code=400, detail="Dataset is not 2D, 3D, or 4D")
                frames = self._frame_indices(frame_count, frame_mode, frame_start, frame_end)
                thresholds = self._threshold_indices(
                    threshold_count, threshold_mode, threshold_index
                )
                total = max(1, len(frames) * len(thresholds))
                mask_bits_by_thr: dict[int, np.ndarray | None] = {}
                for thr_idx in thresholds:
                    mask_dset = self._deps.find_pixel_mask(
                        h5, threshold=thr_idx if threshold_count > 1 else None
                    )
                    mask_bits_by_thr[thr_idx] = (
                        np.asarray(mask_dset, dtype=np.uint32) if mask_dset is not None else None
                    )
                base = (
                    self._safe_name(output_prefix)
                    if output_prefix
                    else self.default_export_base(source_path, dataset)
                )
                base_metadata = self._hdf5_base_export_metadata(h5, source_path, dataset)

                # The frame's number in the dataset, 1-based; the threshold
                # whenever the detector has more than one, even if only the
                # current one is exported, so the name says which.
                def thr_tag(index: int) -> int | None:
                    return index + 1 if threshold_count > 1 else None

                planned = [(f + 1, thr_tag(t)) for f in frames for t in thresholds]
                suffix = ".cbf" if output_format == "cbf" else ".tiff"
                base = self._free_base(out_dir, base, planned, suffix, overwrite)
                self._check_room(out_dir, len(planned), shape, output_format, data_type)
                options = {
                    "out_dir": out_dir,
                    "output_format": output_format,
                    "overwrite": overwrite,
                    "inpaint": inpaint,
                    "inpaint_margin": inpaint_margin,
                    "inpaint_method": inpaint_method,
                    "inpaint_value": inpaint_value,
                    "inpaint_noise": inpaint_noise,
                    "flatfield": flatfield,
                    "flatfield_scale": flatfield_scale,
                    "data_type": data_type,
                }

                # Read here, one frame after the other: h5py reads one at a
                # time anyway. The conversion and writing run on the workers.
                def tasks() -> Iterator[Callable[[], tuple[Path, int]]]:
                    for frame_idx in frames:
                        for thr_idx in thresholds:
                            arr = self._deps.extract_frame(view, frame_idx, thr_idx)
                            metadata = self._hdf5_frame_export_metadata(
                                h5,
                                base_metadata,
                                frame_index=frame_idx,
                                threshold_index=thr_idx,
                                frame_count=frame_count,
                                threshold_count=threshold_count,
                            )
                            yield partial(
                                self._write_frame,
                                name=self.frame_name(base, frame_idx + 1, thr_tag(thr_idx)),
                                arr=arr,
                                mask_bits=mask_bits_by_thr.get(thr_idx),
                                metadata=metadata,
                                **options,
                            )

                pixels = int(shape[-2]) * int(shape[-1])
                outputs = self._write_frames(job_id, tasks(), total, pixels)
            finally:
                for handle in extra_files:
                    with contextlib.suppress(Exception):
                        handle.close()
        return outputs

    def _run_image_series_export(
        self,
        *,
        job_id: str,
        source_path: Path,
        output_format: str,
        out_dir: Path,
        output_prefix: str,
        frame_mode: str,
        frame_start: int | None,
        frame_end: int | None,
        overwrite: bool,
        inpaint: str = "off",
        inpaint_margin: int = 0,
        inpaint_method: str = "interpolate",
        inpaint_value: float = 0.0,
        inpaint_noise: bool = False,
        flatfield: str = "",
        flatfield_scale: str = "counts",
        data_type: str = "auto",
    ) -> list[str]:
        series_files, current_index = self._deps.resolve_series_files(source_path)
        if not series_files:
            raise HTTPException(status_code=400, detail="No frames available")
        frames = self._frame_indices(
            len(series_files), frame_mode, frame_start, frame_end, current_index=current_index
        )
        total = max(1, len(frames))
        base = (
            self._safe_name(output_prefix)
            if output_prefix
            else self.default_export_base(source_path)
        )
        # Each frame keeps its source's own number: image_00007.cbf -> _00007.
        numbers = {idx: self.source_frame_number(series_files[idx], idx + 1) for idx in frames}
        suffix = ".cbf" if output_format == "cbf" else ".tiff"
        base = self._free_base(
            out_dir, base, [(n, None) for n in numbers.values()], suffix, overwrite
        )
        options = {
            "out_dir": out_dir,
            "output_format": output_format,
            "overwrite": overwrite,
            "inpaint": inpaint,
            "inpaint_margin": inpaint_margin,
            "inpaint_method": inpaint_method,
            "inpaint_value": inpaint_value,
            "inpaint_noise": inpaint_noise,
            "flatfield": flatfield,
            "flatfield_scale": flatfield_scale,
            "data_type": data_type,
        }

        def convert(frame_idx: int, first: tuple | None = None) -> tuple[Path, int]:
            # Reading on the worker too: files are separate, so their reads
            # overlap, which is what a network drive needs.
            src = series_files[frame_idx]
            arr, parsed, text = first or self._deps.read_image_with_header(src)
            metadata = self._image_source_export_metadata(
                src, frame_idx, len(series_files), header=(parsed, text)
            )
            return self._write_frame(
                name=self.frame_name(base, numbers[frame_idx]),
                arr=arr,
                metadata=metadata,
                **options,
            )

        # The first frame read here, for its size: whether the disk has room
        # for them all, before anything is written, and how many fit in memory.
        first = self._deps.read_image_with_header(series_files[frames[0]])
        shape = np.shape(first[0])
        self._check_room(out_dir, total, shape, output_format, data_type)

        def tasks() -> Iterator[Callable[[], tuple[Path, int]]]:
            yield partial(convert, frames[0], first)
            for frame_idx in frames[1:]:
                yield partial(convert, frame_idx)

        return self._write_frames(job_id, tasks(), total, int(np.prod(shape[-2:])))

    def _run_job(
        self,
        *,
        job_id: str,
        file: str,
        dataset: str,
        output_format: str,
        output_dir: str,
        output_prefix: str,
        frame_mode: str,
        frame_start: int | None,
        frame_end: int | None,
        threshold_mode: str,
        threshold_index: int | None,
        overwrite: bool,
        inpaint: str = "off",
        inpaint_margin: int = 0,
        inpaint_method: str = "interpolate",
        inpaint_value: float = 0.0,
        inpaint_noise: bool = False,
        flatfield: str = "",
        flatfield_scale: str = "counts",
        data_type: str = "auto",
    ) -> None:
        started = time.perf_counter()
        try:
            self._update_job(job_id, status="running", progress=0.0, message="Resolving source")
            source_path = self._deps.resolve_image_file(file)
            ext = self._deps.image_ext_name(source_path.name)
            if output_format not in {"tiff", "tif", "cbf"}:
                raise HTTPException(status_code=400, detail="Unsupported output format")
            normalized_format = "tiff" if output_format in {"tiff", "tif"} else "cbf"
            out_dir = self._resolve_output_dir(output_dir, source_path)
            # What was converted, how and where to: the line to find when a
            # file turns out not to be what was expected.
            frames = "all frames"
            if frame_mode == "range":
                frames = f"frames {frame_start}-{frame_end}"
            elif frame_mode == "current":
                frames = f"frame {frame_start}"
            options = [f"{normalized_format.upper()} {data_type}", frames]
            if threshold_mode == "all":
                options.append("all thresholds")
            if flatfield:
                options.append(f"flatfield {Path(flatfield).name} ({flatfield_scale})")
            if inpaint != "off":
                options.append(
                    f"inpaint {inpaint} +{inpaint_margin} px {inpaint_method}"
                    + (" with noise" if inpaint_noise else "")
                )
            self._deps.logger.info(
                "Export started: %s%s -> %s (%s)",
                source_path,
                f" {dataset}" if dataset else "",
                out_dir,
                ", ".join(options),
            )
            self._update_job(job_id, progress=0.0, message="Starting export")
            if ext in {".h5", ".hdf5"}:
                outputs = self._run_hdf5_export(
                    job_id=job_id,
                    source_path=source_path,
                    dataset=dataset,
                    output_format=normalized_format,
                    out_dir=out_dir,
                    output_prefix=output_prefix,
                    frame_mode=frame_mode,
                    frame_start=frame_start,
                    frame_end=frame_end,
                    threshold_mode=threshold_mode,
                    threshold_index=threshold_index,
                    overwrite=overwrite,
                    inpaint=inpaint,
                    inpaint_margin=inpaint_margin,
                    inpaint_method=inpaint_method,
                    inpaint_value=inpaint_value,
                    inpaint_noise=inpaint_noise,
                    flatfield=flatfield,
                    flatfield_scale=flatfield_scale,
                    data_type=data_type,
                )
            else:
                outputs = self._run_image_series_export(
                    job_id=job_id,
                    source_path=source_path,
                    output_format=normalized_format,
                    out_dir=out_dir,
                    output_prefix=output_prefix,
                    frame_mode=frame_mode,
                    frame_start=frame_start,
                    frame_end=frame_end,
                    overwrite=overwrite,
                    inpaint=inpaint,
                    inpaint_margin=inpaint_margin,
                    inpaint_method=inpaint_method,
                    inpaint_value=inpaint_value,
                    inpaint_noise=inpaint_noise,
                    flatfield=flatfield,
                    flatfield_scale=flatfield_scale,
                    data_type=data_type,
                )
            self._deps.logger.info(
                "Export done: %d file(s) in %.1f s -> %s%s",
                len(outputs),
                time.perf_counter() - started,
                out_dir,
                self._clipped_note(job_id),
            )
            self._update_job(
                job_id,
                status="done",
                progress=1.0,
                message=f"Completed: wrote {len(outputs)} file(s)" + self._clipped_note(job_id),
                outputs=outputs,
                cancel_requested=False,
                done_at=time.time(),
            )
        except _DataExportCancelledError:
            self._deps.logger.info("Export cancelled after %.1f s", time.perf_counter() - started)
            self._update_job(
                job_id,
                status="cancelled",
                progress=1.0,
                message="Cancelled",
                cancel_requested=False,
                done_at=time.time(),
            )
        except Exception as exc:
            self._deps.logger.exception("Data export failed: %s", exc)
            detail = exc.detail if isinstance(exc, HTTPException) else str(exc)
            self._update_job(
                job_id,
                status="error",
                progress=1.0,
                message=f"Failed: {detail}",
                error=str(detail),
                cancel_requested=False,
                done_at=time.time(),
            )
