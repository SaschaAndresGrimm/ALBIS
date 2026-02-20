from __future__ import annotations

"""Background series operations service."""

import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import numpy as np
from fastapi import HTTPException


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
    write_tiff: Callable[[Path, np.ndarray], None]
    iter_sum_groups: Callable[[int, str, int, int | None, int | None], list[dict[str, Any]]]
    mask_flag_value: Callable[[np.dtype], float]
    mask_slices: Callable[[np.ndarray], tuple[np.ndarray, np.ndarray, np.ndarray]]
    resolve_dataset_view: Callable[[Any, Path, str], tuple[dict[str, Any], list[Any]]]
    extract_frame: Callable[[dict[str, Any], int, int], np.ndarray]
    find_pixel_mask: Callable[[Any, int | None], Any | None]


class SeriesSummingService:
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
        normalize_frame: int | None,
        range_start: int | None,
        range_end: int | None,
        output_path: str | None,
        output_format: str,
        apply_mask: bool,
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
        return job_id

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return None
            return dict(job)

    def _trim_finished_jobs(self) -> None:
        done_jobs = [
            jid for jid, info in self._jobs.items() if info.get("status") in {"done", "error"}
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

    def _resolve_output_base(self, output_path: str | None) -> Path:
        raw = (output_path or "").strip()
        if raw:
            target = Path(raw).expanduser()
            if not target.is_absolute():
                target = (self._deps.data_dir / target).resolve()
            else:
                target = target.resolve()
        else:
            target = (self._deps.data_dir / "output" / "series_sum").resolve()

        if target.exists() and target.is_dir():
            target = (target / "series_sum").resolve()

        allowed_root = self._deps.data_dir.resolve()
        if not self._deps.get_allow_abs_paths() and not self._deps.is_within(target, allowed_root):
            raise HTTPException(status_code=400, detail="Output path is outside data directory")
        target.parent.mkdir(parents=True, exist_ok=True)
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

    def _copy_h5_metadata(self, src_h5: Any, dst_h5: Any, threshold_count: int) -> None:
        h5py = self._deps.get_h5py()
        for key, val in src_h5.attrs.items():
            if key not in dst_h5.attrs:
                try:
                    dst_h5.attrs[key] = val
                except Exception:
                    pass

        if "/entry/instrument/detector" in src_h5:
            src_detector = src_h5["/entry/instrument/detector"]
            dst_detector = dst_h5.require_group("/entry/instrument/detector")
            for key, val in src_detector.attrs.items():
                if key not in dst_detector.attrs:
                    try:
                        dst_detector.attrs[key] = val
                    except Exception:
                        pass

            for thr in range(threshold_count):
                channel_path = f"threshold_{thr + 1}_channel"
                if channel_path in src_detector:
                    src_channel = src_detector[channel_path]
                    dst_channel = dst_detector.require_group(channel_path)
                    for key, val in src_channel.attrs.items():
                        if key not in dst_channel.attrs:
                            try:
                                dst_channel.attrs[key] = val
                            except Exception:
                                pass
                    for item_name in src_channel:
                        if item_name in ("pixel_mask",):
                            continue
                        src_item = src_channel[item_name]
                        if isinstance(src_item, h5py.Dataset) and item_name not in dst_channel:
                            try:
                                dst_channel.create_dataset(item_name, data=src_item[()])
                            except Exception:
                                pass

        for group_path in ("/entry/instrument", "/entry/sample", "/entry/data"):
            if group_path in src_h5 and group_path not in dst_h5:
                try:
                    src_group = src_h5[group_path]
                    dst_group = dst_h5.require_group(group_path)
                    for key, val in src_group.attrs.items():
                        if key not in dst_group.attrs:
                            try:
                                dst_group.attrs[key] = val
                            except Exception:
                                pass
                except Exception:
                    pass

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
        raise HTTPException(status_code=400, detail="Unsupported image format")

    def _run_job(
        self,
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
        try:
            source_path = self._deps.resolve_image_file(file)
            ext = self._deps.image_ext_name(source_path.name)
            base_target = self._resolve_output_base(output_path)
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            output_format = output_format.lower()
            mode = mode.lower()
            operation = operation.lower()
            step = max(1, int(step))
            normalize_frame_idx = int(normalize_frame) - 1 if normalize_frame is not None else None

            self._update_job(job_id, status="running", message="Preparing datasets…", progress=0.01)

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
                        if normalize_frame_idx is not None and (
                            normalize_frame_idx < 0 or normalize_frame_idx >= frame_count
                        ):
                            raise HTTPException(
                                status_code=400, detail="Normalize frame is out of range"
                            )
                        groups = self._deps.iter_sum_groups(
                            frame_count, mode, step, range_start, range_end
                        )
                        if not groups:
                            raise HTTPException(
                                status_code=400, detail="No frames available for summing"
                            )

                        source_dtype = np.dtype(view["dtype"])
                        flag_value = self._deps.mask_flag_value(source_dtype)
                        mask_bits_by_thr: list[np.ndarray | None] = []
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
                        total_steps = max(1, total_input_frames * threshold_count)
                        processed = 0
                        sums: list[
                            tuple[int, int, int, int, int, np.ndarray, np.ndarray | None]
                        ] = []

                        for thr in range(threshold_count):
                            mask_bits = mask_bits_by_thr[thr]
                            _, _, any_mask = (
                                self._deps.mask_slices(mask_bits)
                                if mask_bits is not None
                                else (None, None, None)
                            )
                            norm_ref: np.ndarray | None = None
                            norm_ref_valid: np.ndarray | None = None
                            if normalize_frame_idx is not None:
                                norm_ref = np.asarray(
                                    self._deps.extract_frame(view, normalize_frame_idx, thr),
                                    dtype=np.float64,
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
                                    arr = self._deps.extract_frame(view, frame_idx, thr)
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
                                    self._update_job(
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
                series_files, _ = self._deps.resolve_series_files(source_path)
                frame_count = len(series_files)
                threshold_count = 1
                if normalize_frame_idx is not None and (
                    normalize_frame_idx < 0 or normalize_frame_idx >= frame_count
                ):
                    raise HTTPException(status_code=400, detail="Normalize frame is out of range")
                groups = self._deps.iter_sum_groups(frame_count, mode, step, range_start, range_end)
                if not groups:
                    raise HTTPException(status_code=400, detail="No frames available for summing")

                sample = self._read_non_h5_image(series_files[0])
                image_h = int(sample.shape[-2])
                image_w = int(sample.shape[-1])
                shape = (int(frame_count), image_h, image_w)
                source_dtype = np.dtype(sample.dtype)
                flag_value = self._deps.mask_flag_value(source_dtype)
                mask_bits = np.zeros((image_h, image_w), dtype=np.uint32) if apply_mask else None
                mask_bits_by_thr = [mask_bits]

                norm_ref: np.ndarray | None = None
                norm_ref_valid: np.ndarray | None = None
                if normalize_frame_idx is not None:
                    ref_arr = np.asarray(
                        self._read_non_h5_image(series_files[normalize_frame_idx]), dtype=np.float64
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
                        arr = np.asarray(
                            self._read_non_h5_image(series_files[frame_idx]), dtype=np.float64
                        )
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
                        self._update_job(
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
            self._update_job(job_id, progress=0.97, message="Writing outputs…")
            if output_format in {"hdf5", "h5"}:
                self._deps.ensure_hdf5_stack()
                h5py = self._deps.get_h5py()
                if base_target.suffix.lower() in {".h5", ".hdf5"}:
                    out_file = base_target
                else:
                    out_file = base_target.parent / f"{base_target.name}_{timestamp}.h5"
                out_file = self._next_available_path(out_file)
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

                    if ext in {".h5", ".hdf5"}:
                        try:
                            with h5py.File(source_path, "r") as src_h5:
                                self._copy_h5_metadata(src_h5, out_h5, threshold_count)
                        except Exception:
                            pass

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

                    chunk_start = np.asarray(
                        [int(group["start"]) for group in groups], dtype=np.int64
                    )
                    chunk_end = np.asarray([int(group["end"]) for group in groups], dtype=np.int64)
                    chunk_count = np.asarray(
                        [int(group["count"]) for group in groups], dtype=np.int64
                    )
                    data_group.create_dataset("sum_start_frame", data=chunk_start)
                    data_group.create_dataset("sum_end_frame", data=chunk_end)
                    data_group.create_dataset("sum_frame_count", data=chunk_count)

                    for thr, chunk_idx, _start_idx, _end_idx, _count, arr, mask_bits in sums:
                        arr_out = np.asarray(arr, dtype=np.float64)
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
                        chunk_tag = (
                            f"_chunk{chunk_idx + 1:04d}_f{start_idx + 1:06d}-{end_idx + 1:06d}"
                        )
                    out_file = out_dir / f"{base_name}{thr_tag}{chunk_tag}_{timestamp}.tiff"
                    out_file = self._next_available_path(out_file)
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
                        gap_mask, bad_mask, _ = self._deps.mask_slices(mask_bits)
                        arr_tiff = arr_tiff.copy()
                        arr_tiff[gap_mask] = -1
                        arr_tiff[bad_mask] = -2
                    self._deps.write_tiff(out_file, np.asarray(arr_tiff))
                    outputs.append(str(out_file))
            else:
                raise HTTPException(status_code=400, detail="Unsupported output format")

            self._update_job(
                job_id,
                status="done",
                progress=1.0,
                message=f"Completed: wrote {len(outputs)} file(s)",
                outputs=outputs,
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
                done_at=time.time(),
            )
