"""Background data-format export/conversion service."""

from __future__ import annotations

import contextlib
import re
import sys
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from fastapi import HTTPException


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
    write_tiff: Callable[[Path, np.ndarray], None]
    write_cbf: Callable[[Path, np.ndarray], None]
    resolve_dataset_view: Callable[[Any, Path, str], tuple[dict[str, Any], list[Any]]]
    extract_frame: Callable[[dict[str, Any], int, int], np.ndarray]
    find_pixel_mask: Callable[[Any, int | None], Any | None]
    mask_slices: Callable[[np.ndarray], tuple[np.ndarray, np.ndarray, np.ndarray]]


class DataExportService:
    """Threaded job manager for 2D detector data conversion."""

    def __init__(self, deps: DataExportDeps, max_finished_jobs: int = 200) -> None:
        self._deps = deps
        self._jobs: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()
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

    @staticmethod
    def _safe_name(value: str, fallback: str = "dataset") -> str:
        text = str(value or "").strip().replace("\\", "/")
        text = text.strip("/")
        text = re.sub(r"[^A-Za-z0-9_.-]+", "_", text)
        text = re.sub(r"_+", "_", text).strip("._-")
        return text or fallback

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

    @staticmethod
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

    @staticmethod
    def _cast_frame_to_int(frame: np.ndarray) -> np.ndarray:
        if np.issubdtype(frame.dtype, np.integer):
            min_value = int(np.min(frame, initial=0))
            max_value = int(np.max(frame, initial=0))
        else:
            finite = np.isfinite(frame)
            finite_values = frame[finite]
            if finite_values.size:
                min_value = int(np.rint(np.min(finite_values)))
                max_value = int(np.rint(np.max(finite_values)))
            else:
                min_value = 0
                max_value = 0
            frame = np.rint(np.where(finite, frame, 0))

        dtype = np.int32
        if min_value < np.iinfo(np.int32).min or max_value > np.iinfo(np.int32).max:
            dtype = np.int64
        return np.asarray(frame, dtype=dtype)

    def _normalize_frame(self, arr: np.ndarray, mask_bits: np.ndarray | None = None) -> np.ndarray:
        frame = np.asarray(arr)
        if frame.ndim != 2:
            raise HTTPException(status_code=400, detail="Only 2D frames can be exported")
        frame = np.ascontiguousarray(frame)
        if frame.dtype.byteorder == ">" or (
            frame.dtype.byteorder == "=" and sys.byteorder == "big"
        ):
            frame = frame.byteswap().view(frame.dtype.newbyteorder("<"))

        gap_mask: np.ndarray | None = None
        bad_mask: np.ndarray | None = None
        if mask_bits is not None:
            mask = np.asarray(mask_bits, dtype=np.uint32)
            if mask.shape != frame.shape:
                raise HTTPException(status_code=400, detail="Pixel mask shape does not match frame")
            gap_mask, bad_mask, _ = self._deps.mask_slices(mask)

        if not np.issubdtype(frame.dtype, np.integer):
            bad_values = ~np.isfinite(frame)
            if np.any(bad_values):
                bad_mask = bad_values if bad_mask is None else (bad_mask | bad_values)

        output = self._cast_frame_to_int(frame)
        if gap_mask is not None or bad_mask is not None:
            output = output.copy()
            if gap_mask is not None:
                output[gap_mask] = -1
            if bad_mask is not None:
                output[bad_mask] = -2
        return output

    def _write_frame(
        self,
        *,
        out_dir: Path,
        name: str,
        output_format: str,
        arr: np.ndarray,
        mask_bits: np.ndarray | None = None,
        overwrite: bool,
    ) -> Path:
        suffix = ".cbf" if output_format == "cbf" else ".tiff"
        path = out_dir / f"{name}{suffix}"
        if path.exists() and not overwrite:
            path = self._next_available_path(path)
        frame = self._normalize_frame(arr, mask_bits=mask_bits)
        if output_format == "cbf":
            self._deps.write_cbf(path, frame)
        else:
            self._deps.write_tiff(path, frame)
        return path

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
                completed = 0
                mask_bits_by_thr: dict[int, np.ndarray | None] = {}
                for thr_idx in thresholds:
                    mask_dset = self._deps.find_pixel_mask(
                        h5, threshold=thr_idx if threshold_count > 1 else None
                    )
                    mask_bits_by_thr[thr_idx] = (
                        np.asarray(mask_dset, dtype=np.uint32) if mask_dset is not None else None
                    )
                prefix = (
                    self._safe_name(output_prefix)
                    if output_prefix
                    else self._safe_name(
                        f"{source_path.stem}_{dataset}", fallback=source_path.stem or "dataset"
                    )
                )
                include_frame_tag = frame_count > 1
                include_threshold_tag = threshold_count > 1
                for frame_idx in frames:
                    for thr_idx in thresholds:
                        self._raise_if_cancelled(job_id)
                        parts = [prefix]
                        if include_frame_tag:
                            parts.append(f"f{frame_idx + 1:06d}")
                        if include_threshold_tag:
                            parts.append(f"thr{thr_idx + 1:02d}")
                        name = "_".join(parts)
                        arr = self._deps.extract_frame(view, frame_idx, thr_idx)
                        out_file = self._write_frame(
                            out_dir=out_dir,
                            name=name,
                            output_format=output_format,
                            arr=arr,
                            mask_bits=mask_bits_by_thr.get(thr_idx),
                            overwrite=overwrite,
                        )
                        outputs.append(str(out_file))
                        completed += 1
                        self._update_job(
                            job_id,
                            progress=completed / total,
                            message=f"Exported {completed}/{total} frame(s)",
                        )
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
    ) -> list[str]:
        series_files, current_index = self._deps.resolve_series_files(source_path)
        if not series_files:
            raise HTTPException(status_code=400, detail="No frames available")
        frames = self._frame_indices(
            len(series_files), frame_mode, frame_start, frame_end, current_index=current_index
        )
        total = max(1, len(frames))
        outputs: list[str] = []
        prefix = (
            self._safe_name(output_prefix) if output_prefix else self._safe_name(source_path.stem)
        )
        include_frame_tag = len(series_files) > 1
        for completed, frame_idx in enumerate(frames, start=1):
            self._raise_if_cancelled(job_id)
            src = series_files[frame_idx]
            name = prefix
            if include_frame_tag:
                name = f"{prefix}_f{frame_idx + 1:06d}"
            arr = self._read_non_h5_image(src)
            out_file = self._write_frame(
                out_dir=out_dir,
                name=name,
                output_format=output_format,
                arr=arr,
                overwrite=overwrite,
            )
            outputs.append(str(out_file))
            self._update_job(
                job_id,
                progress=completed / total,
                message=f"Exported {completed}/{total} frame(s)",
            )
        return outputs

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
    ) -> None:
        try:
            self._update_job(job_id, status="running", progress=0.0, message="Resolving source")
            source_path = self._deps.resolve_image_file(file)
            ext = self._deps.image_ext_name(source_path.name)
            if output_format not in {"tiff", "tif", "cbf"}:
                raise HTTPException(status_code=400, detail="Unsupported output format")
            normalized_format = "tiff" if output_format in {"tiff", "tif"} else "cbf"
            out_dir = self._resolve_output_dir(output_dir, source_path)
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
                )
            self._update_job(
                job_id,
                status="done",
                progress=1.0,
                message=f"Completed: wrote {len(outputs)} file(s)",
                outputs=outputs,
                cancel_requested=False,
                done_at=time.time(),
            )
        except _DataExportCancelledError:
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
