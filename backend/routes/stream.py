from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import JSONResponse, Response


@dataclass(frozen=True)
class StreamRouteDeps:
    logger: Any
    resolve_image_file: Callable[[str], Path]
    image_ext_name: Callable[[str], str]
    read_tiff: Callable[[Path, int], Any]
    read_cbf: Callable[[Path], Any]
    read_cbf_gz: Callable[[Path], Any]
    read_edf: Callable[[Path], Any]
    pilatus_meta_from_tiff: Callable[[Path], dict[str, Any]]
    pilatus_meta_from_fabio: Callable[[Path], dict[str, Any]]
    pilatus_header_text: Callable[[Path], str]
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


def register_stream_routes(app: FastAPI, deps: StreamRouteDeps) -> None:
    @app.get("/api/image")
    def image(
        file: str = Query(..., min_length=1),
        index: int = Query(0, ge=0),
    ) -> Response:
        path = deps.resolve_image_file(file)
        ext = deps.image_ext_name(path.name)
        meta: dict[str, Any] = {}
        if ext in {".h5", ".hdf5"}:
            raise HTTPException(status_code=400, detail="Use /api/frame for HDF5 datasets")
        if ext in {".tif", ".tiff"}:
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
        headers = {
            "X-Dtype": arr.dtype.str,
            "X-Shape": ",".join(str(x) for x in arr.shape),
            "X-Frame": "0",
        }
        if meta:
            deps.logger.debug("Image meta (%s): %s", path.name, meta)
            if meta.get("distance_mm") is not None:
                headers["X-Image-DetectorDistance-MM"] = str(meta["distance_mm"])
            if meta.get("pixel_size_um") is not None:
                headers["X-Image-PixelSize-UM"] = str(meta["pixel_size_um"])
            if meta.get("energy_ev") is not None:
                headers["X-Image-Energy-Ev"] = str(meta["energy_ev"])
            if meta.get("wavelength_a") is not None:
                headers["X-Image-Wavelength-A"] = str(meta["wavelength_a"])
            if meta.get("beam_center_px"):
                center = meta["beam_center_px"]
                headers["X-Image-BeamCenter-X"] = str(center[0])
                headers["X-Image-BeamCenter-Y"] = str(center[1])
        return Response(content=data, media_type="application/octet-stream", headers=headers)

    @app.get("/api/image/header")
    def image_header(file: str = Query(..., min_length=1)) -> dict[str, str]:
        path = deps.resolve_image_file(file)
        ext = deps.image_ext_name(path.name)
        if ext in {".h5", ".hdf5"}:
            raise HTTPException(
                status_code=400, detail="Header is only available for non-HDF images"
            )
        header_text = deps.pilatus_header_text(path)
        deps.logger.debug("Image header (%s): %d chars", path.name, len(header_text))
        return {"header": header_text or ""}

    @app.get("/api/simplon/monitor")
    def simplon_monitor(
        url: str = Query(..., min_length=4),
        version: str = Query("1.8.0"),
        timeout: int = Query(500, ge=0),
        enable: bool = Query(True),
    ) -> Response:
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
        headers = {
            "X-Dtype": arr.dtype.str,
            "X-Shape": ",".join(str(x) for x in arr.shape),
            "X-Frame": "0",
        }
        if meta:
            if meta.get("series_number") is not None:
                headers["X-Simplon-Series"] = str(meta["series_number"])
            if meta.get("image_number") is not None:
                headers["X-Simplon-Image"] = str(meta["image_number"])
            if meta.get("image_datetime"):
                headers["X-Simplon-Date"] = str(meta["image_datetime"])
            if meta.get("threshold_energy_ev") is not None:
                headers["X-Simplon-Threshold-Ev"] = str(meta["threshold_energy_ev"])
            if meta.get("energy_ev") is not None:
                headers["X-Simplon-Energy-Ev"] = str(meta["energy_ev"])
            if meta.get("wavelength_a") is not None:
                headers["X-Simplon-Wavelength-A"] = str(meta["wavelength_a"])
            if meta.get("distance_mm") is not None:
                headers["X-Simplon-DetectorDistance-MM"] = str(meta["distance_mm"])
            if meta.get("beam_center_px"):
                center = meta["beam_center_px"]
                headers["X-Simplon-BeamCenter-X"] = str(center[0])
                headers["X-Simplon-BeamCenter-Y"] = str(center[1])
        return Response(content=data_bytes, media_type="application/octet-stream", headers=headers)

    @app.post("/api/simplon/mode")
    def simplon_mode(
        url: str = Query(..., min_length=4),
        version: str = Query("1.8.0"),
        mode: str = Query("enabled"),
    ) -> dict[str, str]:
        mode_value = mode.lower()
        if mode_value not in {"enabled", "disabled"}:
            raise HTTPException(status_code=400, detail="Invalid monitor mode")
        base = deps.simplon_base(url, version)
        deps.simplon_set_mode(base, mode_value)
        deps.logger.info("SIMPLON monitor mode: %s (url=%s)", mode_value, url)
        return {"status": "ok", "mode": mode_value}

    @app.get("/api/simplon/mask")
    def simplon_mask(
        url: str = Query(..., min_length=4),
        version: str = Query("1.8.0"),
    ) -> Response:
        arr = deps.simplon_fetch_pixel_mask(url, version)
        if arr is None:
            deps.logger.debug("SIMPLON mask: not available (url=%s)", url)
            return Response(status_code=204)
        deps.logger.info("SIMPLON mask fetched (url=%s)", url)
        data = arr.tobytes(order="C")
        headers = {
            "X-Dtype": arr.dtype.str,
            "X-Shape": ",".join(str(x) for x in arr.shape),
        }
        return Response(content=data, media_type="application/octet-stream", headers=headers)

    @app.post("/api/remote/v1/frame")
    async def remote_frame_ingest(
        source_id: str = Query("default", min_length=1),
        seq: int | None = Query(None, ge=0),
        meta: str = Form("{}"),
        image: UploadFile = File(...),
    ) -> dict[str, Any]:
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
        return {"status": "ok", "source_id": safe_source, "seq": seq_value}

    @app.get("/api/remote/v1/latest")
    def remote_frame_latest(
        source_id: str = Query("default", min_length=1),
        after_seq: int | None = Query(None, ge=0),
    ) -> Response:
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
            parts: list[str] = [f"Remote stream ({safe_source})"]
            if meta.get("series_number") is not None:
                parts.append(f"S{meta.get('series_number')}")
            if meta.get("image_number") is not None:
                parts.append(f"Img{meta.get('image_number')}")
            if meta.get("image_datetime"):
                parts.append(str(meta.get("image_datetime")))
            display_name = " ".join(parts)
        headers = {
            "X-Dtype": str(frame.get("dtype") or ""),
            "X-Shape": ",".join(str(v) for v in frame.get("shape") or ()),
            "X-Frame": "0",
            "X-Remote-Source": safe_source,
            "X-Remote-Seq": str(seq),
            "X-Remote-Display": display_name,
        }
        if meta.get("series_number") is not None:
            headers["X-Remote-Series"] = str(meta.get("series_number"))
        if meta.get("image_number") is not None:
            headers["X-Remote-Image"] = str(meta.get("image_number"))
        if meta.get("image_datetime"):
            headers["X-Remote-Date"] = str(meta.get("image_datetime"))
        if resolution.get("distance_mm") is not None:
            headers["X-Remote-DetectorDistance-MM"] = str(resolution.get("distance_mm"))
        if resolution.get("pixel_size_um") is not None:
            headers["X-Remote-PixelSize-UM"] = str(resolution.get("pixel_size_um"))
        if resolution.get("energy_ev") is not None:
            headers["X-Remote-Energy-Ev"] = str(resolution.get("energy_ev"))
        if resolution.get("wavelength_a") is not None:
            headers["X-Remote-Wavelength-A"] = str(resolution.get("wavelength_a"))
        center = resolution.get("beam_center_px")
        if isinstance(center, list) and len(center) >= 2:
            headers["X-Remote-BeamCenter-X"] = str(center[0])
            headers["X-Remote-BeamCenter-Y"] = str(center[1])
        peak_sets = meta.get("peak_sets") if isinstance(meta, dict) else []
        headers["X-Remote-PeakSets"] = str(len(peak_sets) if isinstance(peak_sets, list) else 0)
        return Response(
            content=frame.get("bytes") or b"",
            media_type="application/octet-stream",
            headers=headers,
        )

    @app.get("/api/remote/v1/meta")
    def remote_frame_meta(
        source_id: str = Query("default", min_length=1),
        seq: int | None = Query(None, ge=0),
    ) -> Response:
        safe_source = deps.remote_safe_source_id(source_id)
        frame = deps.remote_snapshot(safe_source)
        if not frame:
            return Response(status_code=204)
        current_seq = int(frame.get("seq", 0))
        if seq is not None and int(seq) != current_seq:
            return JSONResponse(
                status_code=409,
                content={
                    "detail": "Requested sequence is no longer current",
                    "current_seq": current_seq,
                },
            )
        meta = frame.get("meta") if isinstance(frame.get("meta"), dict) else {}
        return JSONResponse(
            {
                "source_id": safe_source,
                "seq": current_seq,
                "updated_at": frame.get("updated_at"),
                "display_name": meta.get("display_name") or "",
                "series_number": meta.get("series_number"),
                "image_number": meta.get("image_number"),
                "image_datetime": meta.get("image_datetime") or "",
                "resolution": meta.get("resolution") or {},
                "peak_sets": meta.get("peak_sets") or [],
                "extra": meta.get("extra") or {},
            }
        )
