from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import JSONResponse, Response

try:
    from ..api_models import (
        ImageGeometryResponse,
        ImageHeaderResponse,
        JungfraujochPreviewControlResponse,
        JungfraujochPreviewStartRequest,
        JungfraujochPreviewStatusResponse,
        RemoteFrameIngestResponse,
        RemoteMetaConflictResponse,
        RemoteMetaResponse,
        SimplonModeResponse,
    )
    from .binary_response_utils import (
        add_optional_header,
        build_binary_headers,
        octet_stream_responses,
    )
except ImportError:  # pragma: no cover - supports `python backend/app.py`
    from api_models import (  # type: ignore[no-redef]
        ImageGeometryResponse,
        ImageHeaderResponse,
        JungfraujochPreviewControlResponse,
        JungfraujochPreviewStartRequest,
        JungfraujochPreviewStatusResponse,
        RemoteFrameIngestResponse,
        RemoteMetaConflictResponse,
        RemoteMetaResponse,
        SimplonModeResponse,
    )
    from binary_response_utils import (  # type: ignore[no-redef]
        add_optional_header,
        build_binary_headers,
        octet_stream_responses,
    )


IMAGE_RESPONSE_DOCS = octet_stream_responses(
    "Raw detector image bytes in little-endian C-order layout.",
    {
        "X-Dtype": "NumPy dtype string for decoding the payload.",
        "X-Shape": "Comma-separated image dimensions.",
        "X-Frame": "0-based frame index returned by this request.",
        "X-Image-DetectorDistance-MM": "Optional detector distance in millimeters.",
        "X-Image-PixelSize-UM": "Optional detector pixel size in micrometers.",
        "X-Image-Energy-Ev": "Optional beam energy in electron volts.",
        "X-Image-Wavelength-A": "Optional wavelength in Angstrom.",
        "X-Image-BeamCenter-X": "Optional beam center X coordinate (pixels).",
        "X-Image-BeamCenter-Y": "Optional beam center Y coordinate (pixels).",
    },
)

SIMPLON_MONITOR_RESPONSE_DOCS = octet_stream_responses(
    "Raw SIMPLON monitor frame bytes in little-endian C-order layout.",
    {
        "X-Dtype": "NumPy dtype string for decoding the payload.",
        "X-Shape": "Comma-separated image dimensions.",
        "X-Frame": "0-based frame index returned by this request.",
        "X-Simplon-Series": "Optional SIMPLON series number.",
        "X-Simplon-Image": "Optional SIMPLON image number.",
        "X-Simplon-Date": "Optional SIMPLON image timestamp.",
        "X-Simplon-Threshold-Ev": "Optional threshold energy in electron volts.",
        "X-Simplon-Energy-Ev": "Optional beam energy in electron volts.",
        "X-Simplon-Wavelength-A": "Optional wavelength in Angstrom.",
        "X-Simplon-DetectorDistance-MM": "Optional detector distance in millimeters.",
        "X-Simplon-BeamCenter-X": "Optional beam center X coordinate (pixels).",
        "X-Simplon-BeamCenter-Y": "Optional beam center Y coordinate (pixels).",
    },
    include_no_content=True,
)

SIMPLON_MASK_RESPONSE_DOCS = octet_stream_responses(
    "Raw SIMPLON pixel-mask bytes in little-endian C-order layout.",
    {
        "X-Dtype": "NumPy dtype string for decoding the payload.",
        "X-Shape": "Comma-separated mask dimensions.",
    },
    include_no_content=True,
)

REMOTE_LATEST_RESPONSE_DOCS = octet_stream_responses(
    "Raw latest remote frame bytes in little-endian C-order layout.",
    {
        "X-Dtype": "NumPy dtype string for decoding the payload.",
        "X-Shape": "Comma-separated image dimensions.",
        "X-Frame": "0-based frame index within the returned payload.",
        "X-Remote-Source": "Normalized source identifier.",
        "X-Remote-Seq": "Monotonic sequence number of the latest frame.",
        "X-Remote-Display": "Human-readable display label for the frame.",
        "X-Remote-Series": "Optional series number from metadata.",
        "X-Remote-Image": "Optional image number from metadata.",
        "X-Remote-Date": "Optional image timestamp from metadata.",
        "X-Remote-DetectorDistance-MM": "Optional detector distance in millimeters.",
        "X-Remote-PixelSize-UM": "Optional detector pixel size in micrometers.",
        "X-Remote-Energy-Ev": "Optional beam energy in electron volts.",
        "X-Remote-Wavelength-A": "Optional wavelength in Angstrom.",
        "X-Remote-BeamCenter-X": "Optional beam center X coordinate (pixels).",
        "X-Remote-BeamCenter-Y": "Optional beam center Y coordinate (pixels).",
        "X-Remote-PeakSets": "Count of metadata peak sets attached to this frame.",
    },
    include_no_content=True,
)


@dataclass(frozen=True)
class StreamRouteDeps:
    logger: Any
    resolve_image_file: Callable[[str], Path]
    resolve_optional_path: Callable[[str], Path]
    image_ext_name: Callable[[str], str]
    read_tiff: Callable[[Path, int], Any]
    read_cbf: Callable[[Path], Any]
    read_cbf_gz: Callable[[Path], Any]
    read_edf: Callable[[Path], Any]
    read_mythen_acquisition: Callable[[Path], tuple[Any, dict[str, Any]]]
    mythen_header_text: Callable[[Path], str]
    pilatus_meta_from_tiff: Callable[[Path], dict[str, Any]]
    pilatus_meta_from_fabio: Callable[[Path], dict[str, Any]]
    pilatus_header_text: Callable[[Path], str]
    pilatus_image_geometry: Callable[[Path, Path | None], dict[str, Any]]
    simplon_base: Callable[[str, str], str]
    simplon_set_mode: Callable[[str, str], None]
    simplon_fetch_monitor: Callable[[str, int], bytes | None]
    simplon_fetch_pixel_mask: Callable[[str, str], Any | None]
    read_tiff_bytes_with_simplon_meta: Callable[[bytes], tuple[Any, dict[str, Any]]]
    remote_parse_meta: Callable[[str], dict[str, Any]]
    remote_safe_source_id: Callable[[str], str]
    remote_read_image_bytes: Callable[..., Any]
    remote_extract_metadata: Callable[[dict[str, Any]], dict[str, Any]]
    remote_store_frame: Callable[..., int]
    remote_snapshot: Callable[[str], dict[str, Any] | None]
    jfjoch_preview_start: Callable[..., dict[str, Any]]
    jfjoch_preview_stop: Callable[[], dict[str, Any]]
    jfjoch_preview_status: Callable[[], dict[str, Any]]


def register_stream_routes(app: FastAPI, deps: StreamRouteDeps) -> None:
    @app.get("/api/image", responses=IMAGE_RESPONSE_DOCS)
    def image(
        file: str = Query(..., min_length=1),
        index: int = Query(0, ge=0),
    ) -> Response:
        """Read one detector image and return raw bytes plus acquisition headers."""
        path = deps.resolve_image_file(file)
        ext = deps.image_ext_name(path.name)
        meta: dict[str, Any] = {}
        if ext in {".h5", ".hdf5"}:
            raise HTTPException(status_code=400, detail="Use /api/frame for HDF5 datasets")
        if ext in {".cfg", ".dat"}:
            arr, meta = deps.read_mythen_acquisition(path)
        elif ext in {".tif", ".tiff"}:
            arr = deps.read_tiff(path, index=index)
            meta = deps.pilatus_meta_from_tiff(path)
        elif ext == ".cbf":
            arr = deps.read_cbf(path)
            meta = deps.pilatus_meta_from_fabio(path)
        elif ext == ".cbf.gz":
            arr = deps.read_cbf_gz(path)
            meta = deps.pilatus_meta_from_fabio(path)
        elif ext == ".edf":
            arr = deps.read_edf(path)
            meta = deps.pilatus_meta_from_fabio(path)
        else:
            raise HTTPException(status_code=400, detail="Unsupported image format")

        data = arr.tobytes(order="C")
        headers = build_binary_headers(dtype=arr.dtype.str, shape=arr.shape, frame=0)
        if meta:
            deps.logger.debug("Image meta (%s): %s", path.name, meta)
            add_optional_header(headers, "X-Image-DetectorDistance-MM", meta.get("distance_mm"))
            add_optional_header(headers, "X-Image-PixelSize-UM", meta.get("pixel_size_um"))
            add_optional_header(headers, "X-Image-Energy-Ev", meta.get("energy_ev"))
            add_optional_header(headers, "X-Image-Wavelength-A", meta.get("wavelength_a"))
            if meta.get("beam_center_px"):
                center = meta["beam_center_px"]
                add_optional_header(headers, "X-Image-BeamCenter-X", center[0])
                add_optional_header(headers, "X-Image-BeamCenter-Y", center[1])
            bad_channels = meta.get("bad_channels")
            if bad_channels:
                # Cap to keep the header well under typical 8 KiB limits.
                capped = [int(c) for c in bad_channels[:1000]]
                add_optional_header(
                    headers, "X-Image-Bad-Channels", ",".join(str(c) for c in capped)
                )
        return Response(content=data, media_type="application/octet-stream", headers=headers)

    @app.get("/api/image/header", response_model=ImageHeaderResponse)
    def image_header(file: str = Query(..., min_length=1)) -> ImageHeaderResponse:
        """Return decoded Pilatus textual header for non-HDF image formats."""
        path = deps.resolve_image_file(file)
        ext = deps.image_ext_name(path.name)
        if ext in {".h5", ".hdf5"}:
            raise HTTPException(
                status_code=400, detail="Header is only available for non-HDF images"
            )
        if ext in {".cfg", ".dat"}:
            header_text = deps.mythen_header_text(path)
            return ImageHeaderResponse(header=header_text or "")
        header_text = deps.pilatus_header_text(path)
        deps.logger.debug("Image header (%s): %d chars", path.name, len(header_text))
        return ImageHeaderResponse(header=header_text or "")

    @app.get("/api/image/geometry", response_model=ImageGeometryResponse)
    def image_geometry(
        file: str = Query(..., min_length=1),
        geometry_file: str | None = Query(None),
    ) -> ImageGeometryResponse:
        """Resolve optional detector geometry metadata for supported image files."""
        path = deps.resolve_image_file(file)
        geometry_path = None
        if geometry_file:
            geometry_path = deps.resolve_optional_path(geometry_file)
            if geometry_path.suffix.lower() != ".expt":
                raise HTTPException(
                    status_code=400, detail="Geometry override must be a DIALS .expt file"
                )
        payload = deps.pilatus_image_geometry(path, geometry_path)
        deps.logger.debug("Image geometry (%s): %s", path.name, payload.get("mode"))
        return ImageGeometryResponse(**payload)

    @app.get("/api/simplon/monitor", responses=SIMPLON_MONITOR_RESPONSE_DOCS)
    def simplon_monitor(
        url: str = Query(..., min_length=1),
        version: str = Query("1.8.0"),
        timeout: int = Query(500, ge=0),
        enable: bool = Query(True),
    ) -> Response:
        """Fetch one live SIMPLON monitor frame and expose parsed metadata headers."""
        base = deps.simplon_base(url, version)
        if enable:
            deps.simplon_set_mode(base, "enabled")
        data = deps.simplon_fetch_monitor(base, timeout)
        if data is None:
            deps.logger.debug("SIMPLON monitor: no data (url=%s)", url)
            return Response(status_code=204)
        arr, meta = deps.read_tiff_bytes_with_simplon_meta(data)
        if meta:
            deps.logger.debug("SIMPLON meta (url=%s): %s", url, meta)
        data_bytes = arr.tobytes(order="C")
        headers = build_binary_headers(dtype=arr.dtype.str, shape=arr.shape, frame=0)
        if meta:
            add_optional_header(headers, "X-Simplon-Series", meta.get("series_number"))
            add_optional_header(headers, "X-Simplon-Image", meta.get("image_number"))
            add_optional_header(headers, "X-Simplon-Date", meta.get("image_datetime"))
            add_optional_header(headers, "X-Simplon-Threshold-Ev", meta.get("threshold_energy_ev"))
            add_optional_header(headers, "X-Simplon-Energy-Ev", meta.get("energy_ev"))
            add_optional_header(headers, "X-Simplon-Wavelength-A", meta.get("wavelength_a"))
            add_optional_header(headers, "X-Simplon-DetectorDistance-MM", meta.get("distance_mm"))
            if meta.get("beam_center_px"):
                center = meta["beam_center_px"]
                add_optional_header(headers, "X-Simplon-BeamCenter-X", center[0])
                add_optional_header(headers, "X-Simplon-BeamCenter-Y", center[1])
        return Response(content=data_bytes, media_type="application/octet-stream", headers=headers)

    @app.post("/api/simplon/mode", response_model=SimplonModeResponse)
    def simplon_mode(
        url: str = Query(..., min_length=1),
        version: str = Query("1.8.0"),
        mode: str = Query("enabled"),
    ) -> SimplonModeResponse:
        """Set SIMPLON monitor state to enabled/disabled for the target detector."""
        mode_value = mode.lower()
        if mode_value not in {"enabled", "disabled"}:
            raise HTTPException(status_code=400, detail="Invalid monitor mode")
        base = deps.simplon_base(url, version)
        deps.simplon_set_mode(base, mode_value)
        deps.logger.info("SIMPLON monitor mode: %s (url=%s)", mode_value, url)
        return SimplonModeResponse(status="ok", mode=mode_value)

    @app.get("/api/simplon/mask", responses=SIMPLON_MASK_RESPONSE_DOCS)
    def simplon_mask(
        url: str = Query(..., min_length=1),
        version: str = Query("1.8.0"),
    ) -> Response:
        """Fetch detector pixel mask from SIMPLON and return it as raw bytes."""
        arr = deps.simplon_fetch_pixel_mask(url, version)
        if arr is None:
            deps.logger.debug("SIMPLON mask: not available (url=%s)", url)
            return Response(status_code=204)
        deps.logger.info("SIMPLON mask fetched (url=%s)", url)
        data = arr.tobytes(order="C")
        headers = build_binary_headers(dtype=arr.dtype.str, shape=arr.shape)
        return Response(content=data, media_type="application/octet-stream", headers=headers)

    @app.post(
        "/api/jfjoch/preview/start",
        response_model=JungfraujochPreviewControlResponse,
    )
    def jfjoch_preview_start(
        payload: JungfraujochPreviewStartRequest,
    ) -> JungfraujochPreviewControlResponse:
        """Start (or reconfigure) JUNGFRAUJOCH preview bridge subscription."""
        endpoint = str(payload.endpoint or "").strip()
        if not endpoint:
            raise HTTPException(status_code=400, detail="Missing preview endpoint")
        safe_source = deps.remote_safe_source_id(payload.source_id or "jungfraujoch")
        topic = str(payload.topic or "")
        channel = str(payload.channel or "")
        try:
            status = deps.jfjoch_preview_start(
                endpoint=endpoint,
                source_id=safe_source,
                topic=topic,
                channel=channel,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        deps.logger.info(
            "JUNGFRAUJOCH preview configured: endpoint=%s source=%s topic=%s channel=%s",
            endpoint,
            safe_source,
            topic or "<all>",
            channel or "<first>",
        )
        return JungfraujochPreviewControlResponse(
            status="ok",
            running=bool(status.get("running")),
            source_id=str(status.get("source_id") or safe_source),
        )

    @app.post(
        "/api/jfjoch/preview/stop",
        response_model=JungfraujochPreviewControlResponse,
    )
    def jfjoch_preview_stop() -> JungfraujochPreviewControlResponse:
        """Stop JUNGFRAUJOCH preview bridge subscription."""
        status = deps.jfjoch_preview_stop()
        return JungfraujochPreviewControlResponse(
            status="ok",
            running=bool(status.get("running")),
            source_id=str(status.get("source_id") or "jungfraujoch"),
        )

    @app.get(
        "/api/jfjoch/preview/status",
        response_model=JungfraujochPreviewStatusResponse,
    )
    def jfjoch_preview_status() -> JungfraujochPreviewStatusResponse:
        """Return current preview bridge status and most recent ingest state."""
        payload = deps.jfjoch_preview_status() or {}
        source_id = deps.remote_safe_source_id(str(payload.get("source_id") or "jungfraujoch"))
        return JungfraujochPreviewStatusResponse(
            running=bool(payload.get("running")),
            endpoint=str(payload.get("endpoint") or ""),
            source_id=source_id,
            topic=str(payload.get("topic") or ""),
            channel=str(payload.get("channel") or ""),
            started_at=payload.get("started_at"),
            last_message_at=payload.get("last_message_at"),
            last_frame_at=payload.get("last_frame_at"),
            last_frame_seq=int(payload.get("last_frame_seq") or 0),
            ingested_frames=int(payload.get("ingested_frames") or 0),
            series_number=payload.get("series_number"),
            image_number=payload.get("image_number"),
            display_name=str(payload.get("display_name") or ""),
            last_error=str(payload.get("last_error") or ""),
        )

    @app.post("/api/remote/v1/frame", response_model=RemoteFrameIngestResponse)
    async def remote_frame_ingest(
        source_id: str = Query("default", min_length=1),
        seq: int | None = Query(None, ge=0),
        meta: str = Form("{}"),
        image: UploadFile = File(...),
    ) -> RemoteFrameIngestResponse:
        """Ingest one remotely pushed frame and store it in the in-memory snapshot cache."""
        if not image.filename:
            raise HTTPException(status_code=400, detail="Missing image filename")
        payload = await image.read()
        if not payload:
            raise HTTPException(status_code=400, detail="Empty image payload")
        meta_dict = deps.remote_parse_meta(meta)
        safe_source = deps.remote_safe_source_id(
            source_id or str(meta_dict.get("source_id") or "default")
        )
        frame = deps.remote_read_image_bytes(payload, meta=meta_dict, filename=image.filename)
        extracted_meta = deps.remote_extract_metadata(meta_dict)
        seq_value = deps.remote_store_frame(
            source_id=safe_source, frame=frame, meta=extracted_meta, seq=seq
        )
        deps.logger.debug(
            "Remote frame ingested: source=%s seq=%s shape=%s dtype=%s peak_sets=%d",
            safe_source,
            seq_value,
            tuple(int(v) for v in frame.shape),
            frame.dtype.str,
            len(extracted_meta.get("peak_sets") or []),
        )
        return RemoteFrameIngestResponse(status="ok", source_id=safe_source, seq=seq_value)

    @app.get("/api/remote/v1/latest", responses=REMOTE_LATEST_RESPONSE_DOCS)
    def remote_frame_latest(
        source_id: str = Query("default", min_length=1),
        after_seq: int | None = Query(None, ge=0),
    ) -> Response:
        """Return the latest cached remote frame, optionally only when newer than `after_seq`."""
        safe_source = deps.remote_safe_source_id(source_id)
        frame = deps.remote_snapshot(safe_source)
        if not frame:
            return Response(status_code=204)
        seq = int(frame.get("seq", 0))
        if after_seq is not None and seq <= int(after_seq):
            return Response(status_code=204)

        meta = frame.get("meta") or {}
        resolution = meta.get("resolution") or {}
        display_name = str(meta.get("display_name") or "").strip()
        if not display_name:
            # Keep a stable, human-readable fallback label when upstream meta is sparse.
            parts: list[str] = [f"Remote stream ({safe_source})"]
            if meta.get("series_number") is not None:
                parts.append(f"S{meta.get('series_number')}")
            if meta.get("image_number") is not None:
                parts.append(f"Img{meta.get('image_number')}")
            if meta.get("image_datetime"):
                parts.append(str(meta.get("image_datetime")))
            display_name = " ".join(parts)
        headers = build_binary_headers(
            dtype=str(frame.get("dtype") or ""),
            shape=frame.get("shape") or (),
            frame=0,
            extra={
                "X-Remote-Source": safe_source,
                "X-Remote-Seq": seq,
                "X-Remote-Display": display_name,
            },
        )
        add_optional_header(headers, "X-Remote-Series", meta.get("series_number"))
        add_optional_header(headers, "X-Remote-Image", meta.get("image_number"))
        add_optional_header(headers, "X-Remote-Date", meta.get("image_datetime"))
        add_optional_header(headers, "X-Remote-DetectorDistance-MM", resolution.get("distance_mm"))
        add_optional_header(headers, "X-Remote-PixelSize-UM", resolution.get("pixel_size_um"))
        add_optional_header(headers, "X-Remote-Energy-Ev", resolution.get("energy_ev"))
        add_optional_header(headers, "X-Remote-Wavelength-A", resolution.get("wavelength_a"))
        center = resolution.get("beam_center_px")
        if isinstance(center, list) and len(center) >= 2:
            add_optional_header(headers, "X-Remote-BeamCenter-X", center[0])
            add_optional_header(headers, "X-Remote-BeamCenter-Y", center[1])
        peak_sets = meta.get("peak_sets") if isinstance(meta, dict) else []
        headers["X-Remote-PeakSets"] = str(len(peak_sets) if isinstance(peak_sets, list) else 0)
        return Response(
            content=frame.get("bytes") or b"",
            media_type="application/octet-stream",
            headers=headers,
        )

    @app.get(
        "/api/remote/v1/meta",
        response_model=RemoteMetaResponse,
        responses={409: {"model": RemoteMetaConflictResponse}},
    )
    def remote_frame_meta(
        source_id: str = Query("default", min_length=1),
        seq: int | None = Query(None, ge=0),
    ) -> Response:
        """Return typed metadata for the latest cached remote frame."""
        safe_source = deps.remote_safe_source_id(source_id)
        frame = deps.remote_snapshot(safe_source)
        if not frame:
            return Response(status_code=204)
        current_seq = int(frame.get("seq", 0))
        if seq is not None and int(seq) != current_seq:
            conflict = RemoteMetaConflictResponse(
                detail="Requested sequence is no longer current",
                current_seq=current_seq,
            )
            return JSONResponse(
                status_code=409,
                content=conflict.model_dump(),
            )
        meta = frame.get("meta") if isinstance(frame.get("meta"), dict) else {}
        payload = RemoteMetaResponse(
            source_id=safe_source,
            seq=current_seq,
            updated_at=frame.get("updated_at"),
            display_name=meta.get("display_name") or "",
            series_number=meta.get("series_number"),
            image_number=meta.get("image_number"),
            image_datetime=meta.get("image_datetime") or "",
            resolution=meta.get("resolution") or {},
            peak_sets=meta.get("peak_sets") or [],
            extra=meta.get("extra") or {},
        )
        return JSONResponse(payload.model_dump())
