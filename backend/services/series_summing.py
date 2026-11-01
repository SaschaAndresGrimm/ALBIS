"""Background series operations service."""

from __future__ import annotations

import contextlib
import os
import tempfile
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from fastapi import HTTPException

from .series_output_files import SeriesOutputFilesMixin
from .series_reduction import SeriesReductionMixin


class _SeriesSummingCancelledError(Exception):
    """Internal control-flow exception for user requested cancellation."""


@dataclass(frozen=True)
class SeriesSummingDeps:
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
    write_tiff: Callable[..., None]
    iter_sum_groups: Callable[..., list[dict[str, Any]]]
    mask_flag_value: Callable[[np.dtype], float]
    mask_slices: Callable[[np.ndarray], tuple[np.ndarray, np.ndarray, np.ndarray]]
    resolve_dataset_view: Callable[[Any, Path, str], tuple[dict[str, Any], list[Any]]]
    extract_frame: Callable[[dict[str, Any], int, int], np.ndarray]
    find_pixel_mask: Callable[[Any, int | None], Any | None]
    # Instrument facts for a combined frame's header. Optional so a caller that
    # only needs pixels -- several tests -- can leave it out and get today's
    # provenance-only header rather than an error.
    combined_frame_metadata: Callable[..., dict[str, Any]] | None = None


class SeriesSummingService(SeriesOutputFilesMixin, SeriesReductionMixin):
    """Threaded job manager for long-running series operations."""

    def __init__(self, deps: SeriesSummingDeps, max_finished_jobs: int = 200) -> None:
        self._deps = deps
        self._jobs: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._max_finished_jobs = max(10, int(max_finished_jobs))

    def start_job(
        self,
        *,
        file: str,
        dataset: str,
        mode: str,
        step: int,
        operation: str,
        normalize_method: str = "none",
        normalize_frame: int | None = None,
        normalize_scalar: float | None = None,
        normalize_image: str | None = None,
        range_start: int | None,
        range_end: int | None,
        output_path: str | None,
        output_format: str,
        apply_mask: bool,
        frame_step: int = 1,
        group_size: int | None = None,
        geometry: dict[str, Any] | None = None,
        distance_mm: float | None = None,
        pixel_size_um: float | None = None,
        energy_ev: float | None = None,
        center_x_px: float | None = None,
        center_y_px: float | None = None,
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
                "mode": mode,
                "step": step,
                "operation": operation,
                "normalize_method": normalize_method,
                "normalize_frame": normalize_frame,
                "normalize_scalar": normalize_scalar,
                "normalize_image": normalize_image,
                "range_start": range_start,
                "range_end": range_end,
                "frame_step": frame_step,
                "group_size": group_size,
                "format": output_format,
                "apply_mask": apply_mask,
                "output_path": output_path,
                "geometry_embedded": bool(geometry and geometry.get("mode") == "geometry"),
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
                "mode": mode,
                "step": step,
                "operation": operation,
                "normalize_method": normalize_method,
                "normalize_frame": normalize_frame,
                "normalize_scalar": normalize_scalar,
                "normalize_image": normalize_image,
                "range_start": range_start,
                "range_end": range_end,
                "output_path": str(output_path or ""),
                "output_format": output_format,
                "apply_mask": apply_mask,
                "frame_step": frame_step,
                "group_size": group_size,
                "geometry": geometry,
                "distance_mm": distance_mm,
                "pixel_size_um": pixel_size_um,
                "energy_ev": energy_ev,
                "center_x_px": center_x_px,
                "center_y_px": center_y_px,
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

    def has_running_job(self) -> bool:
        """Whether any job is still working.

        Asked by the update-apply flow, which closes ALBIS: a series sum writes
        an output file over minutes, and ending the process midway leaves a
        truncated one behind.
        """
        with self._lock:
            return any(
                str(job.get("status") or "") in {"queued", "running"} for job in self._jobs.values()
            )

    def cancel_job(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return False
            status = str(job.get("status") or "")
            if status in {"done", "error", "cancelled"}:
                return False
            job["cancel_requested"] = True
            job["message"] = "Cancellation requested…"
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
            raise _SeriesSummingCancelledError("Cancelled by user")

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

    def _run_job(
        self,
        job_id: str,
        file: str,
        dataset: str,
        mode: str,
        step: int,
        operation: str,
        normalize_method: str,
        normalize_frame: int | None,
        normalize_scalar: float | None,
        normalize_image: str | None,
        range_start: int | None,
        range_end: int | None,
        output_path: str | None,
        output_format: str,
        apply_mask: bool,
        frame_step: int,
        group_size: int | None,
        geometry: dict[str, Any] | None,
        distance_mm: float | None,
        pixel_size_um: float | None,
        energy_ev: float | None,
        center_x_px: float | None,
        center_y_px: float | None,
    ) -> None:
        started = time.perf_counter()
        try:
            source_path = self._deps.resolve_image_file(file)
            ext = self._deps.image_ext_name(source_path.name)
            base_target = self._resolve_output_base(output_path)
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            output_format = output_format.lower()
            mode = mode.lower()
            operation = operation.lower()
            step = max(1, int(step))
            frame_step = max(1, int(frame_step))
            normalize_method = str(normalize_method or "none").strip().lower()
            if normalize_method == "none" and normalize_frame is not None:
                normalize_method = "frame"
            normalize_frame_idx = int(normalize_frame) - 1 if normalize_frame is not None else None
            normalize_scalar_value = (
                float(normalize_scalar) if normalize_scalar is not None else None
            )
            geometry_payload = self._normalize_geometry_payload(geometry)
            distance_mm_value = self._finite_float(distance_mm)
            pixel_size_um_value = self._finite_float(pixel_size_um)
            energy_ev_value = self._finite_float(energy_ev)
            center_x_px_value = self._finite_float(center_x_px)
            center_y_px_value = self._finite_float(center_y_px)
            if pixel_size_um_value is None:
                pixel_size_um_value = self._geometry_pixel_size_um(geometry_payload)
            normalize_image_path = str(normalize_image or "").strip()
            # What was combined, how and where to.
            frames = f"frames {range_start or 1}-{range_end or 'end'}"
            if frame_step > 1:
                frames += f" every {frame_step}"
            grouping = f"groups of {group_size}" if group_size else f"{mode} step {step}"
            self._deps.logger.info(
                "Combine Frames started: %s%s -> %s (%s %s, %s, %s, normalize %s%s)",
                source_path,
                f" {dataset}" if dataset else "",
                base_target,
                operation,
                output_format.upper(),
                frames,
                grouping,
                normalize_method,
                ", mask applied" if apply_mask else "",
            )
            if normalize_method not in {"none", "frame", "scalar", "image"}:
                raise HTTPException(status_code=400, detail="Invalid normalization method")
            if normalize_method == "frame" and normalize_frame_idx is None:
                raise HTTPException(status_code=400, detail="Normalize frame is required")
            if normalize_method == "scalar":
                if normalize_scalar_value is None:
                    raise HTTPException(status_code=400, detail="Normalize scalar is required")
                if not np.isfinite(normalize_scalar_value) or abs(normalize_scalar_value) <= 1e-12:
                    raise HTTPException(status_code=400, detail="Normalize scalar must be non-zero")
            if normalize_method == "image" and not normalize_image_path:
                raise HTTPException(status_code=400, detail="Normalize image is required")

            self._update_job(job_id, status="running", message="Preparing datasets…", progress=0.01)
            self._raise_if_cancelled(job_id)

            normalize_image_ref: np.ndarray | None = None
            normalize_image_source: Path | None = None
            if normalize_method == "image":
                normalize_image_source = self._deps.resolve_image_file(normalize_image_path)
                normalize_image_ext = self._deps.image_ext_name(normalize_image_source.name)
                if normalize_image_ext not in {".tif", ".tiff"}:
                    raise HTTPException(
                        status_code=400,
                        detail="Normalize image must be a TIFF file in v1",
                    )
                normalize_image_ref = np.asarray(
                    self._read_non_h5_image(normalize_image_source),
                    dtype=np.float64,
                )
                if normalize_image_ref.ndim != 2:
                    raise HTTPException(status_code=400, detail="Normalize image must be 2D")

            sums: list[tuple[int, int, int, int, int, np.ndarray, np.ndarray | None]] = []
            mask_bits_by_thr: list[np.ndarray | None] = []
            result_min: float | None = None
            result_max: float | None = None
            has_fractional_values = False
            max_group_count = 1

            if ext in {".h5", ".hdf5"}:
                self._deps.ensure_hdf5_stack()
                h5py = self._deps.get_h5py()
                with h5py.File(source_path, "r") as h5:
                    view, extra_files = self._deps.resolve_dataset_view(h5, source_path, dataset)
                    try:
                        shape = tuple(int(x) for x in view["shape"])
                        ndim = int(view["ndim"])
                        if ndim not in (3, 4):
                            raise HTTPException(
                                status_code=400,
                                detail="Series summing requires 3D or 4D image stacks",
                            )
                        frame_count = int(shape[0])
                        threshold_count = int(shape[1]) if ndim == 4 else 1
                        if (
                            normalize_method == "frame"
                            and normalize_frame_idx is not None
                            and (normalize_frame_idx < 0 or normalize_frame_idx >= frame_count)
                        ):
                            raise HTTPException(
                                status_code=400, detail="Normalize frame is out of range"
                            )
                        if (
                            normalize_image_ref is not None
                            and normalize_image_ref.shape != shape[-2:]
                        ):
                            raise HTTPException(
                                status_code=400,
                                detail=(
                                    "Normalize image shape mismatch: expected "
                                    f"{shape[-2]}x{shape[-1]}, got "
                                    f"{normalize_image_ref.shape[0]}x{normalize_image_ref.shape[1]}"
                                ),
                            )
                        groups = self._deps.iter_sum_groups(
                            frame_count,
                            mode,
                            step,
                            range_start,
                            range_end,
                            frame_step=frame_step,
                            group_size=group_size,
                        )
                        if not groups:
                            raise HTTPException(
                                status_code=400, detail="No frames available for summing"
                            )

                        source_dtype = np.dtype(view["dtype"])
                        for thr in range(threshold_count):
                            if not apply_mask:
                                mask_bits_by_thr.append(None)
                                continue
                            mask_dset = self._deps.find_pixel_mask(
                                h5, threshold=thr if threshold_count > 1 else None
                            )
                            if mask_dset is None:
                                mask_bits_by_thr.append(None)
                                continue
                            mask_bits_by_thr.append(np.asarray(mask_dset, dtype=np.uint32))

                        total_input_frames = sum(int(group["count"]) for group in groups)
                        max_group_count = max(1, max(int(group["count"]) for group in groups))
                        total_steps = max(1, total_input_frames * threshold_count)
                        processed_ref = [0]

                        for thr in range(threshold_count):
                            self._raise_if_cancelled(job_id)
                            mask_bits = mask_bits_by_thr[thr]
                            _, _, any_mask = (
                                self._deps.mask_slices(mask_bits)
                                if mask_bits is not None
                                else (None, None, None)
                            )
                            norm_ref: np.ndarray | None = None
                            norm_ref_valid: np.ndarray | None = None
                            if normalize_method == "frame" and normalize_frame_idx is not None:
                                norm_ref = np.asarray(
                                    self._deps.extract_frame(view, normalize_frame_idx, thr),
                                    dtype=np.float64,
                                )
                            elif normalize_method == "image" and normalize_image_ref is not None:
                                norm_ref = np.asarray(normalize_image_ref, dtype=np.float64)

                            if norm_ref is not None:
                                norm_ref = norm_ref.copy()
                                existing_mask = any_mask.copy() if any_mask is not None else None
                                if any_mask is not None:
                                    norm_ref[any_mask] = np.nan
                                norm_ref_valid = np.isfinite(norm_ref) & (np.abs(norm_ref) > 1e-12)
                                if apply_mask:
                                    invalid_ref = ~norm_ref_valid
                                    if existing_mask is not None:
                                        invalid_ref = invalid_ref & ~existing_mask
                                    if np.any(invalid_ref):
                                        if mask_bits is None:
                                            mask_bits = np.zeros(norm_ref.shape, dtype=np.uint32)
                                            mask_bits_by_thr[thr] = mask_bits
                                        mask_bits[invalid_ref] |= np.uint32(0x1E)
                                        _, _, any_mask = self._deps.mask_slices(mask_bits)
                                        norm_ref[any_mask] = np.nan
                                        norm_ref_valid = np.isfinite(norm_ref) & (
                                            np.abs(norm_ref) > 1e-12
                                        )

                            for chunk_idx, group in enumerate(groups):
                                self._raise_if_cancelled(job_id)
                                start_idx = int(group["start"])
                                end_idx = int(group["end"])
                                frame_indices = list(group["indices"])
                                reduced_arr = self._reduce_group_values(
                                    job_id=job_id,
                                    frame_indices=frame_indices,
                                    operation=operation,
                                    load_frame=lambda frame_idx, thr_idx=thr: self._deps.extract_frame(
                                        view, frame_idx, thr_idx
                                    ),
                                    processed_ref=processed_ref,
                                    total_steps=total_steps,
                                    progress_message=(
                                        lambda frame_idx, thr_idx=thr: (
                                            f"{operation.capitalize()} threshold {thr_idx + 1}/{threshold_count}, "
                                            f"frame {frame_idx + 1}/{frame_count}"
                                        )
                                    ),
                                    reduce_message=(
                                        lambda thr_idx=thr, start=start_idx, end=end_idx: (
                                            f"Reducing median threshold {thr_idx + 1}/{threshold_count}, "
                                            f"frames {start + 1}-{end + 1}"
                                        )
                                    ),
                                    normalize_scalar=(
                                        normalize_scalar_value
                                        if normalize_method == "scalar"
                                        else None
                                    ),
                                    normalize_ref=norm_ref,
                                    normalize_ref_valid=norm_ref_valid,
                                    any_mask=any_mask,
                                )
                                if reduced_arr is None:
                                    continue
                                result_min, result_max = self._update_result_range(
                                    reduced_arr, result_min, result_max
                                )
                                if (
                                    operation == "median"
                                    and normalize_method == "none"
                                    and (
                                        np.issubdtype(source_dtype, np.integer)
                                        or np.issubdtype(source_dtype, np.unsignedinteger)
                                    )
                                    and not has_fractional_values
                                ):
                                    rounded = np.rint(reduced_arr)
                                    has_fractional_values = bool(
                                        np.any(np.abs(reduced_arr - rounded) > 1e-6)
                                    )
                                sums.append(
                                    (
                                        thr,
                                        chunk_idx,
                                        start_idx,
                                        end_idx,
                                        len(frame_indices),
                                        reduced_arr,
                                        mask_bits,
                                    )
                                )
                    finally:
                        for handle in extra_files:
                            with contextlib.suppress(Exception):
                                handle.close()

            else:
                series_files, _ = self._deps.resolve_series_files(source_path)
                frame_count = len(series_files)
                threshold_count = 1
                if (
                    normalize_method == "frame"
                    and normalize_frame_idx is not None
                    and (normalize_frame_idx < 0 or normalize_frame_idx >= frame_count)
                ):
                    raise HTTPException(status_code=400, detail="Normalize frame is out of range")
                groups = self._deps.iter_sum_groups(
                    frame_count,
                    mode,
                    step,
                    range_start,
                    range_end,
                    frame_step=frame_step,
                    group_size=group_size,
                )
                if not groups:
                    raise HTTPException(status_code=400, detail="No frames available for summing")
                max_group_count = max(1, max(int(group["count"]) for group in groups))

                sample = self._read_non_h5_image(series_files[0])
                image_h = int(sample.shape[-2])
                image_w = int(sample.shape[-1])
                shape = (int(frame_count), image_h, image_w)
                source_dtype = np.dtype(sample.dtype)
                unsigned_flag = (
                    float(np.iinfo(source_dtype).max)
                    if np.issubdtype(source_dtype, np.unsignedinteger)
                    else None
                )
                mask_bits = np.zeros((image_h, image_w), dtype=np.uint32) if apply_mask else None
                mask_bits_by_thr = [mask_bits]
                if normalize_image_ref is not None and normalize_image_ref.shape != (
                    image_h,
                    image_w,
                ):
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            "Normalize image shape mismatch: expected "
                            f"{image_h}x{image_w}, got "
                            f"{normalize_image_ref.shape[0]}x{normalize_image_ref.shape[1]}"
                        ),
                    )

                norm_ref: np.ndarray | None = None
                norm_ref_valid: np.ndarray | None = None
                if normalize_method == "frame" and normalize_frame_idx is not None:
                    ref_arr = np.asarray(
                        self._read_non_h5_image(series_files[normalize_frame_idx]),
                        dtype=np.float64,
                    )
                elif normalize_method == "image" and normalize_image_ref is not None:
                    ref_arr = np.asarray(normalize_image_ref, dtype=np.float64)
                else:
                    ref_arr = None

                if ref_arr is not None:
                    ref_arr = ref_arr.copy()
                    existing_mask = (mask_bits != 0) if mask_bits is not None else None
                    if apply_mask:
                        neg = ref_arr < 0
                        if neg.any():
                            gaps = ref_arr == -1
                            if mask_bits is not None:
                                mask_bits[gaps] |= 1
                                mask_bits[neg & ~gaps] |= 0x1E
                            ref_arr[neg] = np.nan
                        invalid_ref = ~np.isfinite(ref_arr) | (np.abs(ref_arr) <= 1e-12)
                        if existing_mask is not None:
                            invalid_ref = invalid_ref & ~existing_mask
                        if invalid_ref.any() and mask_bits is not None:
                            mask_bits[invalid_ref] |= 0x1E
                            ref_arr[invalid_ref] = np.nan
                    norm_ref = ref_arr
                    norm_ref_valid = np.isfinite(norm_ref) & (np.abs(norm_ref) > 1e-12)

                total_input_frames = sum(int(group["count"]) for group in groups)
                total_steps = max(1, total_input_frames)
                processed_ref = [0]

                for chunk_idx, group in enumerate(groups):
                    self._raise_if_cancelled(job_id)
                    start_idx = int(group["start"])
                    end_idx = int(group["end"])
                    frame_indices = list(group["indices"])

                    def _preprocess_non_h5_frame(arr: np.ndarray, _frame_idx: int) -> np.ndarray:
                        if not apply_mask:
                            return arr
                        neg = arr < 0
                        if neg.any():
                            gaps = arr == -1
                            if mask_bits is not None:
                                mask_bits[gaps] |= 1
                                mask_bits[neg & ~gaps] |= 0x1E
                            arr = arr.copy()
                            arr[neg] = 0.0
                        if unsigned_flag is not None:
                            # An unsigned frame cannot hold -1: its gaps are the
                            # type's maximum, which is no count either. Summed or
                            # averaged as one, 65535 came back as an unmarked pixel.
                            flagged = arr == unsigned_flag
                            if flagged.any():
                                if mask_bits is not None:
                                    mask_bits[flagged] |= 1
                                arr = arr.copy()
                                arr[flagged] = 0.0
                        if mask_bits is not None and np.any(mask_bits):
                            arr = arr.copy()
                            arr[mask_bits != 0] = 0.0
                        return arr

                    reduced_arr = self._reduce_group_values(
                        job_id=job_id,
                        frame_indices=frame_indices,
                        operation=operation,
                        load_frame=lambda frame_idx: self._read_non_h5_image(
                            series_files[frame_idx]
                        ),
                        processed_ref=processed_ref,
                        total_steps=total_steps,
                        progress_message=(
                            lambda frame_idx: f"{operation.capitalize()} frame {frame_idx + 1}/{frame_count}"
                        ),
                        reduce_message=(
                            lambda start=start_idx, end=end_idx: (
                                f"Reducing median frames {start + 1}-{end + 1}"
                            )
                        ),
                        normalize_scalar=(
                            normalize_scalar_value if normalize_method == "scalar" else None
                        ),
                        normalize_ref=norm_ref,
                        normalize_ref_valid=norm_ref_valid,
                        preprocess_frame=_preprocess_non_h5_frame,
                    )
                    if reduced_arr is None:
                        continue
                    result_min, result_max = self._update_result_range(
                        reduced_arr, result_min, result_max
                    )
                    if (
                        operation == "median"
                        and normalize_method == "none"
                        and (
                            np.issubdtype(source_dtype, np.integer)
                            or np.issubdtype(source_dtype, np.unsignedinteger)
                        )
                        and not has_fractional_values
                    ):
                        rounded = np.rint(reduced_arr)
                        has_fractional_values = bool(np.any(np.abs(reduced_arr - rounded) > 1e-6))
                    sums.append(
                        (
                            0,
                            chunk_idx,
                            start_idx,
                            end_idx,
                            len(frame_indices),
                            reduced_arr,
                            mask_bits,
                        )
                    )

            outputs: list[str] = []
            output_dtype = self._select_output_dtype(
                source_dtype=source_dtype,
                operation=operation,
                normalize_enabled=normalize_method != "none",
                max_group_count=max_group_count,
                result_min=result_min,
                result_max=result_max,
                has_fractional_values=has_fractional_values,
            )
            flag_value = self._deps.mask_flag_value(output_dtype)
            self._raise_if_cancelled(job_id)
            self._update_job(job_id, progress=0.97, message="Writing outputs…")
            operation_base_name = self._operation_base_name(
                base_target.stem or base_target.name or "series_sum",
                operation,
            )
            _out_frames = len(groups)
            _out_h = int(shape[-2])
            _out_w = int(shape[-1])
            _bytes_per_elem = int(np.dtype(output_dtype).itemsize)
            _estimated_bytes = _out_frames * threshold_count * _out_h * _out_w * _bytes_per_elem
            self._check_disk_space(base_target.parent, _estimated_bytes)
            if output_format in {"hdf5", "h5"}:
                self._deps.ensure_hdf5_stack()
                h5py = self._deps.get_h5py()
                if base_target.suffix.lower() in {".h5", ".hdf5"}:
                    out_file = base_target
                else:
                    out_file = base_target.parent / f"{operation_base_name}_{timestamp}.h5"
                out_file = self._next_available_path(out_file)
                # Written beside the target and renamed on success. The write
                # loop below calls `_raise_if_cancelled`, so pressing Cancel --
                # or any error part-way -- used to leave a half-written .h5 at
                # the final name, indistinguishable from a finished one and
                # recorded nowhere, because `outputs` is discarded on the way
                # out. Same reasoning and same directory as the atomic config
                # save in backend/config.py: os.replace is only atomic within a
                # filesystem.
                tmp_handle, tmp_name = tempfile.mkstemp(
                    dir=out_file.parent, prefix=f".{out_file.name}.", suffix=".partial"
                )
                os.close(tmp_handle)
                tmp_path = Path(tmp_name)
                try:
                    with h5py.File(tmp_path, "w") as out_h5:
                        out_h5.attrs["source_file"] = str(source_path)
                        out_h5.attrs["source_dataset"] = str(dataset)
                        out_h5.attrs["series_mode"] = mode
                        if mode == "frames":
                            out_h5.attrs["frame_step"] = int(frame_step)
                            out_h5.attrs["group_size"] = int(group_size or 0)
                        out_h5.attrs["operation"] = operation
                        out_h5.attrs["frame_count"] = int(frame_count)
                        out_h5.attrs["threshold_count"] = int(threshold_count)
                        out_h5.attrs["mask_applied"] = bool(apply_mask)
                        out_h5.attrs["normalize_method"] = normalize_method
                        if normalize_method == "frame" and normalize_frame is not None:
                            out_h5.attrs["normalize_frame"] = int(normalize_frame)
                        if normalize_method == "scalar" and normalize_scalar_value is not None:
                            out_h5.attrs["normalize_scalar"] = float(normalize_scalar_value)
                        if normalize_method == "image" and normalize_image_source is not None:
                            out_h5.attrs["normalize_image"] = str(normalize_image_source)

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

                        if ext in {".h5", ".hdf5"}:
                            try:
                                with h5py.File(source_path, "r") as src_h5:
                                    self._copy_h5_metadata(src_h5, out_h5, threshold_count)
                            except Exception:
                                pass
                        embedded_geometry = self._write_embedded_geometry(out_h5, geometry_payload)
                        self._write_analysis_geometry_metadata(
                            out_h5,
                            distance_mm=distance_mm_value,
                            pixel_size_um=pixel_size_um_value,
                            energy_ev=energy_ev_value,
                            center_x_px=center_x_px_value,
                            center_y_px=center_y_px_value,
                        )
                        if embedded_geometry:
                            out_h5.attrs["albis_geometry_embedded"] = True

                        data_dset = data_group.create_dataset(
                            "data",
                            shape=data_shape,
                            dtype=output_dtype,
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
                        data_dset.attrs["sum_normalize_method"] = normalize_method
                        if normalize_method == "frame" and normalize_frame is not None:
                            data_dset.attrs["sum_normalize_frame"] = int(normalize_frame)
                        if normalize_method == "scalar" and normalize_scalar_value is not None:
                            data_dset.attrs["sum_normalize_scalar"] = float(normalize_scalar_value)
                        if normalize_method == "image" and normalize_image_source is not None:
                            data_dset.attrs["sum_normalize_image"] = str(normalize_image_source)
                        data_dset.attrs["source_dataset"] = str(dataset)
                        data_dset.attrs["frame_count_in"] = int(frame_count)
                        data_dset.attrs["frame_count_out"] = int(out_frame_count)
                        data_dset.attrs["threshold_count"] = int(threshold_count)
                        data_dset.attrs["signal"] = "data"

                        chunk_start = np.asarray(
                            [int(group["start"]) for group in groups], dtype=np.int64
                        )
                        chunk_end = np.asarray(
                            [int(group["end"]) for group in groups], dtype=np.int64
                        )
                        chunk_count = np.asarray(
                            [int(group["count"]) for group in groups], dtype=np.int64
                        )
                        # These are indices into the source dataset, so they
                        # are 0-based like the array they index. The exported
                        # TIFF filenames and the progress messages count from
                        # 1, because that is what a person reading "frames
                        # 3-7" expects -- so the same run reports its range
                        # two different ways and nothing said which was which.
                        # Machine-readable stays 0-based; the attribute is
                        # what makes it checkable instead of guessable.
                        start_dset = data_group.create_dataset("sum_start_frame", data=chunk_start)
                        end_dset = data_group.create_dataset("sum_end_frame", data=chunk_end)
                        for _dset in (start_dset, end_dset):
                            _dset.attrs["index_base"] = 0
                            _dset.attrs["description"] = (
                                "0-based frame index into the source dataset. Exported "
                                "TIFF filenames and the interface show the same range "
                                "counting from 1."
                            )
                        data_group.create_dataset("sum_frame_count", data=chunk_count)

                        for thr, chunk_idx, _start_idx, _end_idx, _count, arr, mask_bits in sums:
                            self._raise_if_cancelled(job_id)
                            arr_out = self._cast_result_to_dtype(arr, output_dtype)
                            if mask_bits is not None:
                                _, _, any_mask = self._deps.mask_slices(mask_bits)
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
                                    thr_group = detector_group.require_group(
                                        f"threshold_{thr + 1}_channel"
                                    )
                                    thr_group.create_dataset(
                                        "pixel_mask",
                                        data=mask_bits.astype(np.uint32),
                                        compression="gzip",
                                    )
                    os.replace(tmp_path, out_file)
                except BaseException:
                    # Cancellation included: a job the user stopped must not
                    # leave a file that looks like a finished result.
                    with contextlib.suppress(OSError):
                        tmp_path.unlink()
                    raise
                outputs.append(str(out_file))
            elif output_format in {"tiff", "tif"}:
                base_name = operation_base_name
                out_dir = base_target.parent
                for thr, chunk_idx, start_idx, end_idx, frame_count_in_sum, arr, mask_bits in sums:
                    self._raise_if_cancelled(job_id)
                    thr_tag = f"_thr{thr + 1:02d}" if threshold_count > 1 else ""
                    if mode == "frames":
                        chunk_tag, combined_what = self._frames_output_label(
                            operation=operation,
                            group_total=len(groups),
                            chunk_idx=chunk_idx,
                            start_idx=start_idx,
                            end_idx=end_idx,
                            count=frame_count_in_sum,
                            frame_count=frame_count,
                            frame_step=frame_step,
                        )
                    elif mode == "all":
                        chunk_tag = "_all"
                    elif mode == "nth":
                        chunk_tag = f"_every{step:03d}_n{frame_count_in_sum:05d}"
                    else:
                        chunk_tag = (
                            f"_chunk{chunk_idx + 1:04d}_f{start_idx + 1:06d}-{end_idx + 1:06d}"
                        )
                    out_file = out_dir / f"{base_name}{thr_tag}{chunk_tag}_{timestamp}.tiff"
                    out_file = self._next_available_path(out_file)
                    arr_tiff = self._cast_result_to_dtype(arr, output_dtype)
                    if mask_bits is not None:
                        gap_mask, bad_mask, _ = self._deps.mask_slices(mask_bits)
                        arr_tiff = arr_tiff.copy()
                        if np.issubdtype(arr_tiff.dtype, np.unsignedinteger):
                            info = np.iinfo(arr_tiff.dtype)
                            arr_tiff[gap_mask] = info.max
                            arr_tiff[bad_mask] = max(info.min, info.max - 1)
                        elif np.issubdtype(arr_tiff.dtype, np.integer):
                            arr_tiff[gap_mask] = -1
                            arr_tiff[bad_mask] = -2
                        else:
                            arr_tiff[gap_mask] = -1.0
                            arr_tiff[bad_mask] = -2.0
                    if mode == "frames":
                        pass  # labelled together with the file name above
                    elif mode == "all":
                        combined_what = f"{operation} of {frame_count_in_sum} frames"
                    elif mode == "nth":
                        combined_what = (
                            f"{operation} of every {step}th frame " f"({frame_count_in_sum} frames)"
                        )
                    else:
                        combined_what = (
                            f"{operation} of {frame_count_in_sum} frames "
                            f"{start_idx + 1}-{end_idx + 1}"
                        )
                    if threshold_count > 1:
                        combined_what += f", threshold {thr + 1}/{threshold_count}"
                    tiff_metadata = None
                    if self._deps.combined_frame_metadata is not None:
                        with contextlib.suppress(Exception):
                            tiff_metadata = self._deps.combined_frame_metadata(
                                source_path, dataset, description=combined_what
                            )
                    self._deps.write_tiff(out_file, np.asarray(arr_tiff), tiff_metadata)
                    outputs.append(str(out_file))
            else:
                raise HTTPException(status_code=400, detail="Unsupported output format")

            self._deps.logger.info(
                "Combine Frames done: %d file(s) in %.1f s%s",
                len(outputs),
                time.perf_counter() - started,
                f" -> {outputs[0]}" if len(outputs) == 1 else "",
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
        except _SeriesSummingCancelledError:
            self._deps.logger.info(
                "Combine Frames cancelled after %.1f s", time.perf_counter() - started
            )
            self._update_job(
                job_id,
                status="cancelled",
                progress=1.0,
                message="Cancelled",
                cancel_requested=False,
                done_at=time.time(),
            )
        except Exception as exc:
            self._deps.logger.exception("Series summing failed: %s", exc)
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
