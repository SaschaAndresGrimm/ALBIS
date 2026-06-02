from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query

try:
    from ..api_models import (
        DataExportCancelRequest,
        DataExportCancelResponse,
        DataExportStartRequest,
        DataExportStartResponse,
    )
except ImportError:  # pragma: no cover - supports `python backend/app.py`
    from api_models import (  # type: ignore[no-redef]
        DataExportCancelRequest,
        DataExportCancelResponse,
        DataExportStartRequest,
        DataExportStartResponse,
    )


@dataclass(frozen=True)
class DataExportRouteDeps:
    start_data_export_job: Callable[..., str]
    get_data_export_job: Callable[[str], dict[str, Any] | None]
    cancel_data_export_job: Callable[[str], bool]


def register_data_export_routes(app: FastAPI, deps: DataExportRouteDeps) -> None:
    @app.post("/api/export/data/start", response_model=DataExportStartResponse)
    def data_export_start(payload: DataExportStartRequest) -> DataExportStartResponse:
        """Start asynchronous conversion of detector frames to 2D image files."""
        file = str(payload.file).strip()
        dataset = str(payload.dataset or "").strip()
        output_format = str(payload.format or "tiff").strip().lower()
        output_dir = str(payload.output_dir or "").strip()
        output_prefix = str(payload.output_prefix or "").strip()
        frame_mode = str(payload.frame_mode or "all").strip().lower()
        frame_start = payload.frame_start
        frame_end = payload.frame_end
        threshold_mode = str(payload.threshold_mode or "current").strip().lower()
        threshold_index = payload.threshold_index
        overwrite = bool(payload.overwrite)

        if not file:
            raise HTTPException(status_code=400, detail="Missing file")
        if Path(file).suffix.lower() in {".h5", ".hdf5"} and not dataset:
            raise HTTPException(status_code=400, detail="Missing dataset")
        if output_format not in {"tiff", "tif", "cbf"}:
            raise HTTPException(status_code=400, detail="Invalid format")
        if frame_mode not in {"all", "current", "range"}:
            raise HTTPException(status_code=400, detail="Invalid frame mode")
        if threshold_mode not in {"all", "current"}:
            raise HTTPException(status_code=400, detail="Invalid threshold mode")
        if frame_start is not None and frame_start < 1:
            raise HTTPException(status_code=400, detail="Frame start must be >= 1")
        if frame_end is not None and frame_end < 1:
            raise HTTPException(status_code=400, detail="Frame end must be >= 1")
        if frame_start is not None and frame_end is not None and frame_start > frame_end:
            raise HTTPException(status_code=400, detail="Frame start must be <= frame end")
        if threshold_index is not None and threshold_index < 1:
            raise HTTPException(status_code=400, detail="Threshold index must be >= 1")

        job_id = deps.start_data_export_job(
            file=file,
            dataset=dataset,
            output_format=output_format,
            output_dir=output_dir,
            output_prefix=output_prefix,
            frame_mode=frame_mode,
            frame_start=frame_start,
            frame_end=frame_end,
            threshold_mode=threshold_mode,
            threshold_index=threshold_index,
            overwrite=overwrite,
        )
        return DataExportStartResponse(job_id=job_id, status="queued")

    @app.get("/api/export/data/status")
    def data_export_status(job_id: str = Query(..., min_length=1)) -> dict[str, Any]:
        """Return current progress/terminal state for a data export background job."""
        job = deps.get_data_export_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return dict(job)

    @app.post("/api/export/data/cancel", response_model=DataExportCancelResponse)
    def data_export_cancel(payload: DataExportCancelRequest) -> DataExportCancelResponse:
        job_id = str(payload.job_id).strip()
        if not job_id:
            raise HTTPException(status_code=400, detail="Missing job_id")
        job = deps.get_data_export_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        status = str(job.get("status") or "")
        if status in {"done", "error", "cancelled"}:
            return DataExportCancelResponse(job_id=job_id, status=status, accepted=False)
        accepted = deps.cancel_data_export_job(job_id)
        current = deps.get_data_export_job(job_id) or job
        return DataExportCancelResponse(
            job_id=job_id,
            status=str(current.get("status") or status or "running"),
            accepted=bool(accepted),
        )
