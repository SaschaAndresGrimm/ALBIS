"""Background data-format export/conversion service."""

from __future__ import annotations

import contextlib
import math
import re
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from fastapi import HTTPException

from ..image_formats import _to_little_endian


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

    def _normalize_frame(
        self,
        arr: np.ndarray,
        mask_bits: np.ndarray | None = None,
        saturated_value: int | None = None,
    ) -> np.ndarray:
        frame = np.asarray(arr)
        if frame.ndim != 2:
            raise HTTPException(status_code=400, detail="Only 2D frames can be exported")
        frame = np.ascontiguousarray(frame)
        frame = _to_little_endian(frame)

        gap_mask: np.ndarray | None = None
        bad_mask: np.ndarray | None = None
        if mask_bits is not None:
            mask = np.asarray(mask_bits, dtype=np.uint32)
            if mask.shape != frame.shape:
                raise HTTPException(status_code=400, detail="Pixel mask shape does not match frame")
            gap_mask, bad_mask, _ = self._deps.mask_slices(mask)

        if np.issubdtype(frame.dtype, np.integer):
            saturated_masks: list[np.ndarray] = []
            if np.issubdtype(frame.dtype, np.unsignedinteger):
                saturated_masks.append(frame == np.iinfo(frame.dtype).max)
            if saturated_value is not None:
                saturated_masks.append(frame >= saturated_value)
            for saturated_mask in saturated_masks:
                if np.any(saturated_mask):
                    bad_mask = saturated_mask if bad_mask is None else (bad_mask | saturated_mask)
        else:
            bad_values = ~np.isfinite(frame)
            if np.any(bad_values):
                bad_mask = bad_values if bad_mask is None else (bad_mask | bad_values)

        cast_source = frame
        cast_mask = None
        if gap_mask is not None:
            cast_mask = gap_mask
        if bad_mask is not None:
            cast_mask = bad_mask if cast_mask is None else (cast_mask | bad_mask)
        if cast_mask is not None and np.any(cast_mask):
            cast_source = frame.copy()
            cast_source[cast_mask] = 0

        output = self._cast_frame_to_int(cast_source)
        if gap_mask is not None or bad_mask is not None:
            output = output.copy()
            if bad_mask is not None:
                output[bad_mask] = -2
            if gap_mask is not None:
                output[gap_mask] = -1
        return output

    @staticmethod
    def _finite_float(value: Any) -> float | None:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if math.isfinite(number) else None

    @classmethod
    def _finite_int(cls, value: Any) -> int | None:
        number = cls._finite_float(value)
        if number is None:
            return None
        return int(round(number))

    @staticmethod
    def _decode_h5_text(value: Any) -> str:
        if isinstance(value, np.ndarray):
            if value.size == 0:
                return ""
            value = value.reshape(-1)[0]
        if isinstance(value, np.generic):
            value = value.item()
        if isinstance(value, bytes | np.bytes_):
            return bytes(value).decode("utf-8", errors="replace").strip("\x00").strip()
        return str(value or "").strip()

    @staticmethod
    def _h5_units(obj: Any) -> str | None:
        try:
            units = obj.attrs.get("units") or obj.attrs.get("unit")
        except Exception:
            return None
        if isinstance(units, np.ndarray):
            if units.size == 0:
                return None
            units = units.reshape(-1)[0]
        if isinstance(units, np.generic):
            units = units.item()
        if isinstance(units, bytes | np.bytes_):
            return bytes(units).decode("utf-8", errors="replace")
        return str(units) if units is not None else None

    @staticmethod
    def _to_seconds(value: float, units: str | None) -> float:
        unit = (units or "").strip().lower().replace("µ", "u")
        if unit in {"ms", "millisecond", "milliseconds"}:
            return value / 1000.0
        if unit in {"us", "microsecond", "microseconds"}:
            return value / 1_000_000.0
        if unit in {"ns", "nanosecond", "nanoseconds"}:
            return value / 1_000_000_000.0
        return value

    @staticmethod
    def _to_degrees(value: float, units: str | None) -> float:
        unit = (units or "").strip().lower()
        if unit in {"rad", "radian", "radians"}:
            return math.degrees(value)
        return value

    @staticmethod
    def _to_angstrom(value: float, units: str | None) -> float:
        unit = (units or "").strip().lower().replace("å", "a").replace("µ", "u")
        if unit in {"m", "meter", "metre", "meters", "metres"}:
            return value * 1e10
        if unit in {"mm", "millimeter", "millimetre", "millimeters", "millimetres"}:
            return value * 1e7
        if unit in {"um", "micrometer", "micrometre", "micrometers", "micrometres"}:
            return value * 1e4
        if unit in {"nm", "nanometer", "nanometre", "nanometers", "nanometres"}:
            return value * 10.0
        if unit in {"a", "ang", "angstrom", "angstroms"}:
            return value
        if value < 1e-6:
            return value * 1e10
        if value < 0.2:
            return value * 10.0
        return value

    def _read_h5_text(self, h5: Any, paths: list[str]) -> str | None:
        h5py = self._deps.get_h5py()
        for path in paths:
            if path not in h5:
                continue
            obj = h5[path]
            if not isinstance(obj, h5py.Dataset):
                continue
            try:
                text = self._decode_h5_text(obj[()])
            except Exception:
                continue
            if text:
                return text
        return None

    def _read_h5_number(self, h5: Any, paths: list[str]) -> tuple[float | None, str | None]:
        value, units = self._deps.read_scalar(h5, paths)
        number = self._finite_float(value)
        if number is None:
            return None, units
        return number, units

    def _read_h5_indexed_number(
        self, h5: Any, path: str, index: int
    ) -> tuple[float | None, str | None]:
        h5py = self._deps.get_h5py()
        if path not in h5:
            return None, None
        obj = h5[path]
        if not isinstance(obj, h5py.Dataset):
            return None, None
        try:
            arr = np.asarray(obj[()])
        except Exception:
            return None, self._h5_units(obj)
        units = self._h5_units(obj)
        if arr.size == 0:
            return None, units
        if arr.ndim == 0:
            value = arr.item()
        else:
            flat = arr.reshape(-1)
            value = flat[min(max(0, int(index)), flat.size - 1)]
        return self._finite_float(value), units

    def _read_h5_length_m(self, h5: Any, paths: list[str]) -> float | None:
        value, units = self._read_h5_number(h5, paths)
        if value is None:
            return None
        try:
            return self._deps.to_um(value, units) / 1e6
        except Exception:
            return None

    def _read_h5_sensor_thickness_m(self, h5: Any, paths: list[str]) -> float | None:
        value, units = self._read_h5_number(h5, paths)
        if value is None:
            return None
        thickness_m = self._read_h5_length_m(h5, paths)
        unit = (units or "").strip().lower()
        if thickness_m is not None and thickness_m > 0.1 and unit in {"m", "meter", "metre"}:
            # Some Dectris/Jungfrau master files report micrometer values with a meter unit.
            return value / 1e6
        return thickness_m

    def _read_h5_distance_m(self, h5: Any, paths: list[str]) -> float | None:
        value, units = self._read_h5_number(h5, paths)
        if value is None:
            return None
        try:
            return self._deps.to_mm(value, units) / 1000.0
        except Exception:
            return None

    def _read_h5_energy_ev(self, h5: Any, paths: list[str]) -> float | None:
        value, units = self._read_h5_number(h5, paths)
        if value is None:
            return None
        try:
            return self._deps.to_ev(value, units)
        except Exception:
            return None

    def _read_h5_seconds(self, h5: Any, paths: list[str]) -> float | None:
        value, units = self._read_h5_number(h5, paths)
        if value is None:
            return None
        return self._to_seconds(value, units)

    def _read_h5_angle_deg(self, h5: Any, paths: list[str]) -> float | None:
        value, units = self._read_h5_number(h5, paths)
        if value is None:
            return None
        return self._to_degrees(value, units)

    def _read_h5_threshold_energy_ev(self, h5: Any, threshold_index: int) -> float | None:
        return self._read_h5_energy_ev(
            h5,
            [
                f"/entry/instrument/detector/threshold_{threshold_index + 1}_channel/threshold_energy",
                f"/entry/instrument/detector/detectorSpecific/threshold_{threshold_index + 1}_energy",
                "/entry/instrument/detector/threshold_energy",
            ],
        )

    def _hdf5_base_export_metadata(
        self, h5: Any, source_path: Path, dataset: str
    ) -> dict[str, Any]:
        meta: dict[str, Any] = {
            "source_path": str(source_path),
            "source_dataset": dataset,
        }
        text_fields = {
            "detector_description": [
                "/entry/instrument/detector/description",
                "/entry/instrument/detector/name",
            ],
            "detector_serial_number": [
                "/entry/instrument/detector/detector_number",
                "/entry/instrument/detector/serial_number",
                "/entry/instrument/detector/detectorSpecific/detector_number",
            ],
            "series_unique_id": [
                "/entry/instrument/detector/detectorSpecific/series_unique_id",
                "/entry/instrument/detector/detectorSpecific/series_id",
                "/entry/experiment_identifier",
            ],
            "image_datetime": [
                "/entry/start_time",
                "/entry/instrument/detector/detectorSpecific/image_datetime",
            ],
        }
        for key, paths in text_fields.items():
            value = self._read_h5_text(h5, paths)
            if value:
                meta[key] = value

        series_number, _ = self._read_h5_number(
            h5,
            [
                "/entry/instrument/detector/detectorSpecific/series_number",
                "/entry/instrument/detector/detectorSpecific/series",
            ],
        )
        if series_number is not None:
            meta["series_number"] = int(round(series_number))

        image_start, _ = self._read_h5_number(
            h5,
            [
                "/entry/instrument/detector/detectorSpecific/image_nr_start",
                "/entry/instrument/detector/detectorSpecific/image_nr_low",
            ],
        )
        if image_start is not None:
            meta["image_number_start"] = int(round(image_start))

        x_pixel = self._read_h5_length_m(
            h5,
            [
                "/entry/instrument/detector/x_pixel_size",
                "/entry/instrument/detector/detectorSpecific/x_pixel_size",
            ],
        )
        y_pixel = self._read_h5_length_m(
            h5,
            [
                "/entry/instrument/detector/y_pixel_size",
                "/entry/instrument/detector/detectorSpecific/y_pixel_size",
            ],
        )
        if x_pixel is not None:
            meta["pixel_size_x_m"] = x_pixel
        if y_pixel is not None:
            meta["pixel_size_y_m"] = y_pixel
        if x_pixel is not None and y_pixel is None:
            meta["pixel_size_y_m"] = x_pixel
        if y_pixel is not None and x_pixel is None:
            meta["pixel_size_x_m"] = y_pixel

        sensor_thickness = self._read_h5_sensor_thickness_m(
            h5, ["/entry/instrument/detector/sensor_thickness"]
        )
        if sensor_thickness is not None:
            meta["sensor_thickness_m"] = sensor_thickness

        exposure_time = self._read_h5_seconds(
            h5,
            [
                "/entry/instrument/detector/count_time",
                "/entry/instrument/detector/exposure_time",
            ],
        )
        if exposure_time is not None:
            meta["exposure_time_s"] = exposure_time
        exposure_period = self._read_h5_seconds(
            h5,
            [
                "/entry/instrument/detector/frame_time",
                "/entry/instrument/detector/exposure_period",
            ],
        )
        if exposure_period is not None:
            meta["exposure_period_s"] = exposure_period

        for path, increment in (
            ("/entry/instrument/detector/detectorSpecific/saturation_value", 0),
            ("/entry/instrument/detector/saturation_value", 0),
            (
                "/entry/instrument/detector/detectorSpecific/countrate_correction_count_cutoff",
                1,
            ),
            (
                "/entry/instrument/detector/detectorSpecific/detectorModule_000/countrate_correction_count_cutoff",
                1,
            ),
        ):
            value, _units = self._read_h5_number(h5, [path])
            if value is not None:
                if increment == 0:
                    meta["saturation_value"] = int(round(value))
                meta["count_cutoff"] = int(round(value)) + increment
                break

        wavelength_value, wavelength_units = self._read_h5_number(
            h5,
            [
                "/entry/sample/beam/incident_wavelength",
                "/entry/instrument/beam/wavelength",
                "/entry/instrument/monochromator/wavelength",
                "/entry/instrument/beam/incident_wavelength",
            ],
        )
        if wavelength_value is not None:
            wavelength_a = self._to_angstrom(wavelength_value, wavelength_units)
            meta["wavelength_a"] = wavelength_a
            energy_from_wavelength = self._deps.wavelength_to_ev(
                wavelength_value, wavelength_units
            )
            if energy_from_wavelength is not None:
                meta["incident_energy_ev"] = energy_from_wavelength

        incident_energy = self._read_h5_energy_ev(
            h5,
            [
                "/entry/instrument/beam/incident_energy",
                "/entry/instrument/beam/energy",
                "/entry/instrument/source/energy",
                "/entry/sample/beam/incident_energy",
                "/entry/instrument/detector/detectorSpecific/photon_energy",
            ],
        )
        if incident_energy is not None:
            meta["incident_energy_ev"] = incident_energy

        distance = self._read_h5_distance_m(
            h5,
            [
                "/entry/instrument/detector/distance",
                "/entry/instrument/detector/detector_distance",
            ],
        )
        if distance is not None:
            meta["detector_distance_m"] = distance

        beam_x, _ = self._read_h5_number(h5, ["/entry/instrument/detector/beam_center_x"])
        beam_y, _ = self._read_h5_number(h5, ["/entry/instrument/detector/beam_center_y"])
        if beam_x is not None and beam_y is not None:
            meta["beam_center_x_px"] = beam_x
            meta["beam_center_y_px"] = beam_y

        angle_increment = self._read_h5_angle_deg(
            h5,
            [
                "/entry/sample/goniometer/omega_range_average",
                "/entry/sample/goniometer/omega_increment",
            ],
        )
        if angle_increment is not None:
            meta["angle_increment_deg"] = angle_increment

        return meta

    def _hdf5_frame_export_metadata(
        self,
        h5: Any,
        base: dict[str, Any],
        *,
        frame_index: int,
        threshold_index: int,
    ) -> dict[str, Any]:
        meta = dict(base)
        image_start = self._finite_int(meta.get("image_number_start"))
        meta["image_number"] = (image_start + frame_index) if image_start is not None else frame_index + 1
        meta["threshold_ids"] = [threshold_index + 1]
        threshold_energy = self._read_h5_threshold_energy_ev(h5, threshold_index)
        if threshold_energy is not None:
            meta["threshold_energies_ev"] = [threshold_energy]

        start_angle, start_units = self._read_h5_indexed_number(
            h5, "/entry/sample/goniometer/omega", frame_index
        )
        if start_angle is not None:
            meta["start_angle_deg"] = self._to_degrees(start_angle, start_units)
        elif self._finite_float(meta.get("angle_increment_deg")) is not None:
            meta["start_angle_deg"] = float(meta["angle_increment_deg"]) * frame_index
        else:
            omega_start = self._read_h5_angle_deg(
                h5,
                [
                    "/entry/sample/goniometer/omega_start",
                    "/entry/sample/goniometer/start_angle",
                ],
            )
            if omega_start is not None:
                meta["start_angle_deg"] = omega_start

        return meta

    def _image_source_export_metadata(self, path: Path, frame_index: int) -> dict[str, Any]:
        meta: dict[str, Any] = {
            "source_path": str(path),
            "image_number": frame_index + 1,
        }
        parsed: dict[str, Any] = {}
        with contextlib.suppress(Exception):
            parsed = self._deps.pilatus_meta_from_image(path)
        if parsed.get("series_unique_id"):
            meta["series_unique_id"] = parsed.get("series_unique_id")
        if parsed.get("series_number") is not None:
            meta["series_number"] = parsed.get("series_number")
        if parsed.get("image_number") is not None:
            meta["image_number"] = parsed.get("image_number")
        if parsed.get("image_datetime"):
            meta["image_datetime"] = parsed.get("image_datetime")
        if parsed.get("threshold_ids"):
            meta["threshold_ids"] = parsed.get("threshold_ids")
        if parsed.get("threshold_energies_ev"):
            meta["threshold_energies_ev"] = parsed.get("threshold_energies_ev")
        elif parsed.get("threshold_energy_ev") is not None:
            meta["threshold_energies_ev"] = [parsed.get("threshold_energy_ev")]
        if parsed.get("exposure_time_s") is not None:
            meta["exposure_time_s"] = parsed.get("exposure_time_s")
        if parsed.get("energy_ev") is not None:
            meta["incident_energy_ev"] = parsed.get("energy_ev")
        if parsed.get("wavelength_a") is not None:
            meta["wavelength_a"] = parsed.get("wavelength_a")
        if parsed.get("lost_pixel_count") is not None:
            meta["lost_pixel_count"] = parsed.get("lost_pixel_count")
        if parsed.get("distance_mm") is not None:
            meta["detector_distance_m"] = float(parsed["distance_mm"]) / 1000.0
        if parsed.get("pixel_size_um") is not None:
            pixel_m = float(parsed["pixel_size_um"]) / 1e6
            meta["pixel_size_x_m"] = pixel_m
            meta["pixel_size_y_m"] = pixel_m
        beam_center = parsed.get("beam_center_px")
        if isinstance(beam_center, list | tuple) and len(beam_center) >= 2:
            meta["beam_center_x_px"] = beam_center[0]
            meta["beam_center_y_px"] = beam_center[1]
        with contextlib.suppress(Exception):
            header_text = self._deps.pilatus_header_text(path)
            if header_text:
                meta["source_header_text"] = header_text
        return meta

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
    ) -> Path:
        suffix = ".cbf" if output_format == "cbf" else ".tiff"
        path = out_dir / f"{name}{suffix}"
        if path.exists() and not overwrite:
            path = self._next_available_path(path)
        saturated_value = self._finite_int(metadata.get("saturation_value")) if metadata else None
        frame = self._normalize_frame(
            arr,
            mask_bits=mask_bits,
            saturated_value=saturated_value,
        )
        if output_format == "cbf":
            self._deps.write_cbf(path, frame, metadata)
        else:
            self._deps.write_tiff(path, frame, metadata)
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
                base_metadata = self._hdf5_base_export_metadata(h5, source_path, dataset)
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
                        metadata = self._hdf5_frame_export_metadata(
                            h5,
                            base_metadata,
                            frame_index=frame_idx,
                            threshold_index=thr_idx,
                        )
                        out_file = self._write_frame(
                            out_dir=out_dir,
                            name=name,
                            output_format=output_format,
                            arr=arr,
                            mask_bits=mask_bits_by_thr.get(thr_idx),
                            metadata=metadata,
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
            metadata = self._image_source_export_metadata(src, frame_idx)
            out_file = self._write_frame(
                out_dir=out_dir,
                name=name,
                output_format=output_format,
                arr=arr,
                metadata=metadata,
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
