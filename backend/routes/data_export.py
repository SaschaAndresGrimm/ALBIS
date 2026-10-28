from __future__ import annotations

import json
import math
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile

from ..api_models import (
    DataExportCancelRequest,
    DataExportCancelResponse,
    DataExportFrameResponse,
    DataExportStartRequest,
    DataExportStartResponse,
)


@dataclass(frozen=True)
class DataExportRouteDeps:
    start_data_export_job: Callable[..., str]
    get_data_export_job: Callable[[str], dict[str, Any] | None]
    cancel_data_export_job: Callable[[str], bool]
    write_live_frame: Callable[..., tuple[Path, int]]
    save_flatfield: Callable[..., Path]


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
        if payload.data_type != "auto" and output_format == "cbf":
            raise HTTPException(
                status_code=400, detail="Unsigned data types need TIFF: CBF is signed 32-bit"
            )
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
            inpaint=payload.inpaint,
            inpaint_margin=payload.inpaint_margin,
            inpaint_method=payload.inpaint_method,
            inpaint_value=payload.inpaint_value,
            flatfield=payload.flatfield.strip(),
            flatfield_scale=payload.flatfield_scale,
            data_type=payload.data_type,
        )
        return DataExportStartResponse(job_id=job_id, status="queued")

    # Plain `def`: writing a frame is file I/O and numpy work, kept off the
    # event loop as `/api/upload` and the remote ingest are.
    @app.post("/api/export/data/frame", response_model=DataExportFrameResponse)
    def data_export_frame(
        meta: str = Form("{}"),
        pixels: UploadFile = File(...),
        mask: UploadFile | None = File(None),
    ) -> DataExportFrameResponse:
        """Write one frame of a live source, sent by the browser, as TIFF or CBF.

        A live source has no file to convert; its frames are in the browser's
        live history. `meta` is JSON: `dtype`, `shape`, `name`, `format`,
        `output_dir`, `overwrite`, `metadata` for the header, and `inpaint`
        ("off", "defects" or "saturated") and `inpaint_margin` (0-5 pixels
        around each flagged one), `inpaint_method` ("interpolate" or "fixed") and
        `inpaint_value` (the fixed value), with the source's pixel `mask`
        (uint32) sent alongside to say which pixels to fill, and `data_type`
        ("auto", "uint16" or "uint32"; the unsigned ones TIFF only).
        """
        try:
            config = json.loads(meta or "{}")
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="Invalid meta JSON") from exc
        if not isinstance(config, dict):
            raise HTTPException(status_code=400, detail="Invalid meta JSON")
        payload = pixels.file.read()
        if not payload:
            raise HTTPException(status_code=400, detail="Empty pixel data")
        inpaint = str(config.get("inpaint") or "off")
        if inpaint not in {"off", "defects", "saturated"}:
            raise HTTPException(status_code=400, detail="Invalid inpaint option")
        try:
            inpaint_margin = int(config.get("inpaint_margin") or 0)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="Invalid inpaint margin") from exc
        if not 0 <= inpaint_margin <= 5:
            raise HTTPException(status_code=400, detail="Invalid inpaint margin")
        inpaint_method = str(config.get("inpaint_method") or "interpolate")
        if inpaint_method not in {"interpolate", "fixed"}:
            raise HTTPException(status_code=400, detail="Invalid inpaint method")
        try:
            inpaint_value = float(config.get("inpaint_value") or 0)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="Invalid inpaint value") from exc
        if not math.isfinite(inpaint_value):
            raise HTTPException(status_code=400, detail="Invalid inpaint value")
        flatfield = str(config.get("flatfield") or "").strip()
        flatfield_scale = str(config.get("flatfield_scale") or "counts")
        if flatfield_scale not in {"counts", "transmission"}:
            raise HTTPException(status_code=400, detail="Invalid flatfield scale")
        data_type = str(config.get("data_type") or "auto")
        if data_type not in {"auto", "uint16", "uint32"}:
            raise HTTPException(status_code=400, detail="Invalid data type")
        path, clipped = deps.write_live_frame(
            payload=payload,
            dtype=str(config.get("dtype") or ""),
            shape=config.get("shape") or [],
            name=str(config.get("name") or ""),
            output_format=str(config.get("format") or "tiff").strip().lower(),
            output_dir=str(config.get("output_dir") or "").strip() or None,
            overwrite=bool(config.get("overwrite")),
            metadata=config.get("metadata") if isinstance(config.get("metadata"), dict) else {},
            mask=mask.file.read() if mask is not None else None,
            inpaint=inpaint,
            inpaint_margin=inpaint_margin,
            inpaint_method=inpaint_method,
            inpaint_value=inpaint_value,
            flatfield=flatfield,
            flatfield_scale=flatfield_scale,
            data_type=data_type,
        )
        return DataExportFrameResponse(path=str(path), clipped=clipped)

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

    # Plain `def`, like the live frames: writing a file is blocking I/O.
    @app.post("/api/flatfield/save", response_model=DataExportFrameResponse)
    def flatfield_save(
        meta: str = Form("{}"),
        pixels: UploadFile = File(...),
    ) -> DataExportFrameResponse:
        """Keep a frame sent by the browser as a flatfield: a TIFF in the data
        folder's `flatfields/`, which the viewer and every export divide by.
        `meta` is JSON: `dtype` and `shape`."""
        try:
            config = json.loads(meta or "{}")
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="Invalid meta JSON") from exc
        if not isinstance(config, dict):
            raise HTTPException(status_code=400, detail="Invalid meta JSON")
        payload = pixels.file.read()
        if not payload:
            raise HTTPException(status_code=400, detail="Empty pixel data")
        path = deps.save_flatfield(
            payload=payload,
            dtype=str(config.get("dtype") or ""),
            shape=config.get("shape") or [],
        )
        return DataExportFrameResponse(path=str(path))
