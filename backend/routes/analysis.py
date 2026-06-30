from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query

try:
    from ..api_models import (
        AnalysisParamsResponse,
        SeriesSumCancelRequest,
        SeriesSumCancelResponse,
        SeriesSumStartRequest,
        SeriesSumStartResponse,
    )
except ImportError:  # pragma: no cover - supports `python backend/app.py`
    from api_models import (  # type: ignore[no-redef]
        AnalysisParamsResponse,
        SeriesSumCancelRequest,
        SeriesSumCancelResponse,
        SeriesSumStartRequest,
        SeriesSumStartResponse,
    )


@dataclass(frozen=True)
class AnalysisRouteDeps:
    ensure_hdf5_stack: Callable[[], None]
    get_h5py: Callable[[], Any]
    resolve_file: Callable[[str], Path]
    resolve_optional_path: Callable[[str], Path]
    resolve_dataset_view: Callable[[Any, Path, str], tuple[dict[str, Any], list[Any]]]
    read_scalar: Callable[[Any, list[str]], tuple[float | None, str | None]]
    image_ext_name: Callable[[str], str]
    pilatus_meta_from_image: Callable[[Path], dict[str, Any]]
    to_mm: Callable[[float, str | None], float]
    to_um: Callable[[float, str | None], float]
    to_ev: Callable[[float, str | None], float]
    wavelength_to_ev: Callable[[float, str | None], float | None]
    norm_unit: Callable[[str | None], str]
    read_threshold_energies: Callable[[Any, int], list[float | None]]
    start_series_sum_job: Callable[..., str]
    get_series_sum_job: Callable[[str], dict[str, Any] | None]
    cancel_series_sum_job: Callable[[str], bool]


def register_analysis_routes(app: FastAPI, deps: AnalysisRouteDeps) -> None:
    def _clean_text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="ignore").strip()
        item = value.item() if hasattr(value, "item") else value
        return str(item).strip()

    def _analysis_payload_from_h5(h5: Any, file_path: Path, dataset: str | None) -> dict[str, Any]:
        distance_val, distance_unit = None, None
        pixel_x_val, pixel_x_unit = None, None
        pixel_y_val, pixel_y_unit = None, None
        energy_val, energy_unit = None, None
        wavelength_val, wavelength_unit = None, None
        center_x_val, center_x_unit = None, None
        center_y_val, center_y_unit = None, None
        shape = None

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

        # Keep the X and Y pixel sizes distinct so anisotropic ("strixel")
        # detectors such as DECTRIS POLLUX can be displayed with the correct
        # aspect ratio. pixel_size_um stays as the reference (X / fast axis)
        # value for backwards compatibility; for square detectors X == Y so
        # nothing downstream changes.
        pixel_x_um = deps.to_um(pixel_x_val, pixel_x_unit) if pixel_x_val is not None else None
        pixel_y_um = deps.to_um(pixel_y_val, pixel_y_unit) if pixel_y_val is not None else None
        # Fall back across axes when only one is present so the aspect ratio
        # defaults to 1 (square) rather than going undefined.
        if pixel_x_um is None:
            pixel_x_um = pixel_y_um
        if pixel_y_um is None:
            pixel_y_um = pixel_x_um
        pixel_size_um = pixel_x_um

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
                if pixel_x_um:
                    center_x_px = deps.to_mm(center_x_val, center_x_unit) / (pixel_x_um / 1000)
            else:
                center_x_px = center_x_val
        if center_y_val is not None:
            unit = deps.norm_unit(center_y_unit)
            if unit in {"mm", "m", "cm", "um", "nm"}:
                if pixel_y_um:
                    center_y_px = deps.to_mm(center_y_val, center_y_unit) / (pixel_y_um / 1000)
            else:
                center_y_px = center_y_val

        return {
            "distance_mm": distance_mm,
            "pixel_size_um": pixel_size_um,
            "pixel_size_x_um": pixel_x_um,
            "pixel_size_y_um": pixel_y_um,
            "energy_ev": energy_ev,
            "center_x_px": center_x_px,
            "center_y_px": center_y_px,
            "shape": [int(v) for v in shape] if shape else None,
        }

    def _analysis_payload_from_source_image(path: Path) -> dict[str, Any]:
        meta = deps.pilatus_meta_from_image(path)
        beam_center = meta.get("beam_center_px")
        center_x_px = beam_center[0] if isinstance(beam_center, tuple | list) and len(beam_center) >= 2 else None
        center_y_px = beam_center[1] if isinstance(beam_center, tuple | list) and len(beam_center) >= 2 else None
        pixel_size_um = meta.get("pixel_size_um")
        return {
            "distance_mm": meta.get("distance_mm"),
            "pixel_size_um": pixel_size_um,
            "pixel_size_x_um": pixel_size_um,
            "pixel_size_y_um": pixel_size_um,
            "energy_ev": meta.get("energy_ev"),
            "center_x_px": center_x_px,
            "center_y_px": center_y_px,
        }

    @app.get("/api/analysis/params", response_model=AnalysisParamsResponse)
    def analysis_params(
        file: str = Query(..., min_length=1),
        dataset: str | None = Query(None),
    ) -> AnalysisParamsResponse:
        deps.ensure_hdf5_stack()
        h5py = deps.get_h5py()
        file_path = deps.resolve_file(file)
        payload: dict[str, Any]
        source_file_ref = ""
        source_dataset_ref = ""

        with h5py.File(file_path, "r") as h5:
            payload = _analysis_payload_from_h5(h5, file_path, dataset)
            source_file_ref = _clean_text(h5.attrs.get("source_file"))
            source_dataset_ref = _clean_text(h5.attrs.get("source_dataset"))

        if source_file_ref and any(payload.get(key) is None for key in (
            "distance_mm",
            "pixel_size_um",
            "energy_ev",
            "center_x_px",
            "center_y_px",
        )):
            try:
                source_path = deps.resolve_optional_path(source_file_ref)
            except HTTPException:
                source_path = None
            if source_path and source_path.exists():
                ext = deps.image_ext_name(source_path.name)
                source_payload: dict[str, Any] = {}
                try:
                    if ext in {".h5", ".hdf5"} and source_path.resolve() != file_path.resolve():
                        with h5py.File(source_path, "r") as source_h5:
                            source_payload = _analysis_payload_from_h5(
                                source_h5,
                                source_path,
                                source_dataset_ref or dataset,
                            )
                    elif ext in {".tif", ".tiff", ".cbf", ".cbf.gz", ".edf"}:
                        source_payload = _analysis_payload_from_source_image(source_path)
                except Exception:
                    source_payload = {}
                for key in (
                    "distance_mm",
                    "pixel_size_um",
                    "pixel_size_x_um",
                    "pixel_size_y_um",
                    "energy_ev",
                    "center_x_px",
                    "center_y_px",
                ):
                    if payload.get(key) is None and source_payload.get(key) is not None:
                        payload[key] = source_payload[key]

        return AnalysisParamsResponse(**payload)

    @app.post("/api/analysis/series-sum/start", response_model=SeriesSumStartResponse)
    def analysis_series_sum_start(payload: SeriesSumStartRequest) -> SeriesSumStartResponse:
        """Start asynchronous series summing and return pollable job metadata."""
        file = str(payload.file).strip()
        dataset = str(payload.dataset).strip()
        mode = str(payload.mode).strip().lower()
        if mode == "step":
            mode = "chunks"
        step = int(payload.step or 10)
        operation = str(payload.operation).strip().lower()
        normalize_method = str(payload.normalize_method or "none").strip().lower()
        normalize_frame = payload.normalize_frame
        normalize_scalar = payload.normalize_scalar
        normalize_image = str(payload.normalize_image or "").strip()
        range_start = payload.range_start
        range_end = payload.range_end
        output_path = payload.output_path
        output_format = str(payload.format or "hdf5").strip().lower()
        apply_mask = bool(payload.apply_mask)
        geometry = payload.geometry.model_dump() if payload.geometry else None
        distance_mm = payload.distance_mm
        pixel_size_um = payload.pixel_size_um
        energy_ev = payload.energy_ev
        center_x_px = payload.center_x_px
        center_y_px = payload.center_y_px

        if not file:
            raise HTTPException(status_code=400, detail="Missing file")
        ext = Path(file).suffix.lower()
        if ext in {".h5", ".hdf5"} and not dataset:
            raise HTTPException(status_code=400, detail="Missing dataset")
        if mode not in {"all", "chunks", "nth", "range"}:
            raise HTTPException(status_code=400, detail="Invalid mode")
        if operation not in {"sum", "mean", "median"}:
            raise HTTPException(status_code=400, detail="Invalid operation")
        if normalize_method not in {"none", "frame", "scalar", "image"}:
            raise HTTPException(status_code=400, detail="Invalid normalization method")
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
        if normalize_method == "none" and normalize_frame is not None:
            # Backward compatibility for older clients that only send normalize_frame.
            normalize_method = "frame"
        if normalize_method == "frame":
            if normalize_frame is None:
                raise HTTPException(status_code=400, detail="Normalize frame is required")
            if normalize_frame < 1:
                raise HTTPException(status_code=400, detail="Normalize frame must be >= 1")
        if normalize_method == "scalar":
            if normalize_scalar is None:
                raise HTTPException(status_code=400, detail="Normalize scalar is required")
            if not isinstance(normalize_scalar, (int, float)):
                raise HTTPException(status_code=400, detail="Normalize scalar must be numeric")
            if abs(float(normalize_scalar)) <= 1e-12:
                raise HTTPException(status_code=400, detail="Normalize scalar must be non-zero")
        if normalize_method == "image" and not normalize_image:
            raise HTTPException(status_code=400, detail="Normalize image is required")

        job_id = deps.start_series_sum_job(
            file=file,
            dataset=dataset,
            mode=mode,
            step=step,
            operation=operation,
            normalize_method=normalize_method,
            normalize_frame=normalize_frame,
            normalize_scalar=normalize_scalar,
            normalize_image=normalize_image,
            range_start=range_start,
            range_end=range_end,
            output_path=str(output_path or ""),
            output_format=output_format,
            apply_mask=apply_mask,
            geometry=geometry,
            distance_mm=distance_mm,
            pixel_size_um=pixel_size_um,
            energy_ev=energy_ev,
            center_x_px=center_x_px,
            center_y_px=center_y_px,
        )
        return SeriesSumStartResponse(job_id=job_id, status="queued")

    @app.get("/api/analysis/series-sum/status")
    def analysis_series_sum_status(job_id: str = Query(..., min_length=1)) -> dict[str, Any]:
        """Return current progress/terminal state for a series-summing background job."""
        job = deps.get_series_sum_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return dict(job)

    @app.post("/api/analysis/series-sum/cancel", response_model=SeriesSumCancelResponse)
    def analysis_series_sum_cancel(payload: SeriesSumCancelRequest) -> SeriesSumCancelResponse:
        job_id = str(payload.job_id).strip()
        if not job_id:
            raise HTTPException(status_code=400, detail="Missing job_id")
        job = deps.get_series_sum_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        status = str(job.get("status") or "")
        if status in {"done", "error", "cancelled"}:
            return SeriesSumCancelResponse(job_id=job_id, status=status, accepted=False)
        accepted = deps.cancel_series_sum_job(job_id)
        current = deps.get_series_sum_job(job_id) or job
        return SeriesSumCancelResponse(
            job_id=job_id,
            status=str(current.get("status") or status or "running"),
            accepted=bool(accepted),
        )
