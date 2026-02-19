from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from fastapi import Body, FastAPI, HTTPException, Query


@dataclass(frozen=True)
class AnalysisRouteDeps:
    ensure_hdf5_stack: Callable[[], None]
    get_h5py: Callable[[], Any]
    resolve_file: Callable[[str], Path]
    resolve_dataset_view: Callable[[Any, Path, str], tuple[dict[str, Any], list[Any]]]
    read_scalar: Callable[[Any, list[str]], tuple[float | None, str | None]]
    to_mm: Callable[[float, str | None], float]
    to_um: Callable[[float, str | None], float]
    to_ev: Callable[[float, str | None], float]
    wavelength_to_ev: Callable[[float, str | None], float | None]
    norm_unit: Callable[[str | None], str]
    read_threshold_energies: Callable[[Any, int], list[float | None]]
    run_series_summing_job: Callable[..., None]
    series_jobs: dict[str, dict[str, Any]]
    series_jobs_lock: threading.Lock


def register_analysis_routes(app: FastAPI, deps: AnalysisRouteDeps) -> None:
    @app.get("/api/analysis/params")
    def analysis_params(
        file: str = Query(..., min_length=1),
        dataset: str | None = Query(None),
    ) -> dict[str, Any]:
        deps.ensure_hdf5_stack()
        h5py = deps.get_h5py()
        file_path = deps.resolve_file(file)
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
                    view, extra_files = deps.resolve_dataset_view(h5, file_path, dataset)
                    try:
                        shape = tuple(int(x) for x in view["shape"])
                    finally:
                        for handle in extra_files:
                            handle.close()
                except Exception:
                    shape = None

            distance_val, distance_unit = deps.read_scalar(
                h5,
                [
                    "/entry/instrument/detector/detector_distance",
                    "/entry/instrument/detector/distance",
                    "/entry/instrument/detector/detectorSpecific/detector_distance",
                ],
            )
            pixel_x_val, pixel_x_unit = deps.read_scalar(
                h5,
                [
                    "/entry/instrument/detector/x_pixel_size",
                    "/entry/instrument/detector/detectorSpecific/x_pixel_size",
                    "/entry/instrument/detector/pixel_size",
                ],
            )
            pixel_y_val, pixel_y_unit = deps.read_scalar(
                h5,
                [
                    "/entry/instrument/detector/y_pixel_size",
                    "/entry/instrument/detector/detectorSpecific/y_pixel_size",
                    "/entry/instrument/detector/pixel_size",
                ],
            )
            energy_val, energy_unit = deps.read_scalar(
                h5,
                [
                    "/entry/instrument/beam/incident_energy",
                    "/entry/instrument/beam/energy",
                    "/entry/instrument/beam/photon_energy",
                    "/entry/instrument/source/energy",
                ],
            )
            wavelength_val, wavelength_unit = deps.read_scalar(
                h5,
                [
                    "/entry/instrument/beam/incident_wavelength",
                    "/entry/instrument/beam/wavelength",
                    "/entry/instrument/beam/photon_wavelength",
                ],
            )
            center_x_val, center_x_unit = deps.read_scalar(
                h5,
                [
                    "/entry/instrument/detector/beam_center_x",
                    "/entry/instrument/detector/beam_center_x_mm",
                    "/entry/instrument/detector/detectorSpecific/beam_center_x",
                ],
            )
            center_y_val, center_y_unit = deps.read_scalar(
                h5,
                [
                    "/entry/instrument/detector/beam_center_y",
                    "/entry/instrument/detector/beam_center_y_mm",
                    "/entry/instrument/detector/detectorSpecific/beam_center_y",
                ],
            )

        distance_mm = deps.to_mm(distance_val, distance_unit) if distance_val is not None else None

        pixel_size_um = None
        if pixel_x_val is not None:
            pixel_size_um = deps.to_um(pixel_x_val, pixel_x_unit)
        if pixel_y_val is not None:
            pixel_y_um = deps.to_um(pixel_y_val, pixel_y_unit)
            pixel_size_um = (
                (pixel_size_um + pixel_y_um) / 2 if pixel_size_um is not None else pixel_y_um
            )

        energy_ev = None
        if energy_val is not None:
            energy_ev = deps.to_ev(energy_val, energy_unit)
        elif wavelength_val is not None:
            energy_ev = deps.wavelength_to_ev(wavelength_val, wavelength_unit)

        center_x_px = None
        center_y_px = None
        if center_x_val is not None:
            unit = deps.norm_unit(center_x_unit)
            if unit in {"mm", "m", "cm", "um", "nm"}:
                if pixel_size_um:
                    center_x_px = deps.to_mm(center_x_val, center_x_unit) / (pixel_size_um / 1000)
            else:
                center_x_px = center_x_val
        if center_y_val is not None:
            unit = deps.norm_unit(center_y_unit)
            if unit in {"mm", "m", "cm", "um", "nm"}:
                if pixel_size_um:
                    center_y_px = deps.to_mm(center_y_val, center_y_unit) / (pixel_size_um / 1000)
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
        range_end = (
            int(range_end) if range_end is not None and str(range_end).strip() != "" else None
        )
        output_path = payload.get("output_path")
        output_format = str(payload.get("format", "hdf5")).strip().lower()
        apply_mask = bool(payload.get("apply_mask", True))

        if not file:
            raise HTTPException(status_code=400, detail="Missing file")
        ext = Path(file).suffix.lower()
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
        with deps.series_jobs_lock:
            deps.series_jobs[job_id] = job_data
            # keep memory bounded
            done_jobs = [
                jid
                for jid, info in deps.series_jobs.items()
                if info.get("status") in {"done", "error"}
            ]
            if len(done_jobs) > 200:
                done_jobs.sort(key=lambda jid: float(deps.series_jobs[jid].get("updated_at", 0.0)))
                for old_id in done_jobs[: len(done_jobs) - 200]:
                    deps.series_jobs.pop(old_id, None)

        worker = threading.Thread(
            target=deps.run_series_summing_job,
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
        with deps.series_jobs_lock:
            job = deps.series_jobs.get(job_id)
            if not job:
                raise HTTPException(status_code=404, detail="Job not found")
            return dict(job)
