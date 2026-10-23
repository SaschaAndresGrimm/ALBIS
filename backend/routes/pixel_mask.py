"""Pixel mask routes (beta): load, save, and read or replace a detector's mask.

Everything here answers 404 unless `ui.pixel_mask` is switched on; the two
detector routes also need `ui.detector_control`, as the rest of the detector's
endpoints do. Sending a mask is a POST and so passes the request guard's
cross-site check.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import Response

from ..api_models import PixelMaskDetectorWriteResponse
from ..services import pixel_mask, simplon_control
from ..services.hdf5_stack import open_hdf5_for_read, read_hdf5_array
from .binary_response_utils import (
    add_optional_header,
    build_binary_headers,
    octet_stream_responses,
)
from .detector_control import OFF_MESSAGE as DETECTOR_OFF_MESSAGE

OFF_MESSAGE = "Pixel masks are switched off. Turn them on under Settings -> Tools."

MASK_DOCS = octet_stream_responses(
    "The pixel mask as raw little-endian uint32 bytes in C order.",
    {
        "X-Dtype": "NumPy dtype string for decoding the payload.",
        "X-Shape": "Comma-separated array dimensions (rows,cols).",
        "X-Mask-Path": "Where in the file, or under which detector key, the mask was found.",
    },
)
# The detector refuses a new mask while it works; so does this route, rather
# than leave the write to fail half way through a series.
_BUSY_STATES = frozenset({"initialize", "configure", "acquire", "test"})
_FORMATS = {
    "h5": ("application/x-hdf5", ".h5"),
    "tiff": ("image/tiff", ".tiff"),
}


@dataclass(frozen=True)
class PixelMaskRouteDeps:
    logger: Any
    # Both read per request: the settings can change while ALBIS runs.
    enabled: Callable[[], bool]
    detector_enabled: Callable[[], bool]
    get_h5py: Callable[[], Any]
    resolve_image_file: Callable[[str], Path]
    find_pixel_mask: Callable[..., Any]
    data_dir: Callable[[], Path]
    invalidate_scans: Callable[[], None]


def _mask_response(arr: np.ndarray, where: str) -> Response:
    headers = build_binary_headers(dtype=arr.dtype.str, shape=arr.shape)
    add_optional_header(headers, "X-Mask-Path", where)
    return Response(
        content=arr.tobytes(order="C"), media_type="application/octet-stream", headers=headers
    )


def _read_meta(raw: str) -> dict[str, Any]:
    try:
        meta = json.loads(raw or "{}")
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="Invalid meta JSON") from exc
    if not isinstance(meta, dict):
        raise HTTPException(status_code=400, detail="Invalid meta JSON")
    return meta


def _safe_stem(name: Any) -> str:
    stem = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in str(name or "").strip())
    return stem.strip("._")[:120] or "pixel_mask"


def register_pixel_mask_routes(app: FastAPI, deps: PixelMaskRouteDeps) -> None:
    def require_enabled() -> None:
        if not deps.enabled():
            raise HTTPException(status_code=404, detail=OFF_MESSAGE)

    def require_detector() -> None:
        require_enabled()
        if not deps.detector_enabled():
            raise HTTPException(status_code=404, detail=DETECTOR_OFF_MESSAGE)

    @app.get("/api/pixel-mask/file", responses=MASK_DOCS)
    def pixel_mask_file(
        file: str = Query(..., min_length=1),
        threshold: int | None = Query(None, ge=0),
    ) -> Response:
        """The pixel mask in an HDF5 file (where a DECTRIS or NeXus file keeps
        it) or a TIFF (the whole image).

        A detector with several energy thresholds writes one mask per threshold
        and none for the whole detector. `threshold` (0-based) picks one; a file
        without it, or a request without one, gets the first threshold's.
        """
        require_enabled()
        path = deps.resolve_image_file(file)
        suffix = path.suffix.lower()
        if suffix in {".tif", ".tiff"}:
            return _mask_response(pixel_mask.read_tiff_mask(path), "")
        if suffix not in {".h5", ".hdf5"}:
            raise HTTPException(status_code=400, detail="A pixel mask is an HDF5 or TIFF file.")
        h5py = deps.get_h5py()
        with open_hdf5_for_read(h5py, path) as h5:
            dset = deps.find_pixel_mask(h5, threshold=threshold)
            if dset is None:
                dset = deps.find_pixel_mask(h5, threshold=0)
            if dset is None:
                raise HTTPException(status_code=404, detail="Pixel mask not found")
            arr = pixel_mask.as_mask(read_hdf5_array(path, lambda: np.asarray(dset)))
            return _mask_response(arr, str(dset.name))

    @app.post("/api/pixel-mask/encode")
    def pixel_mask_encode(meta: str = Form("{}"), pixels: UploadFile = File(...)) -> Response:
        """The mask the browser holds as a file to download. `meta` is JSON:
        `format` (`h5` or `tiff`), `shape` (rows, cols) and a `name`."""
        require_enabled()
        config = _read_meta(meta)
        fmt = str(config.get("format") or "").lower()
        if fmt not in _FORMATS:
            raise HTTPException(status_code=400, detail="Save a pixel mask as h5 or tiff.")
        arr = pixel_mask.from_bytes(pixels.file.read(), config.get("shape"))
        body = (
            pixel_mask.encode_hdf5(arr, deps.get_h5py())
            if fmt == "h5"
            else pixel_mask.encode_tiff(arr)
        )
        media_type, suffix = _FORMATS[fmt]
        filename = f"{_safe_stem(config.get('name'))}{suffix}"
        return Response(
            content=body,
            media_type=media_type,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @app.get("/api/pixel-mask/detector", responses=MASK_DOCS)
    def pixel_mask_detector(
        url: str = Query(..., min_length=1), version: str = Query("1.8.0")
    ) -> Response:
        """The pixel mask the detector applies now."""
        require_detector()
        arr, key = pixel_mask.read_detector_mask(url, version)
        return _mask_response(arr, key)

    @app.post("/api/pixel-mask/detector", response_model=PixelMaskDetectorWriteResponse)
    def pixel_mask_detector_write(
        meta: str = Form("{}"), pixels: UploadFile = File(...)
    ) -> PixelMaskDetectorWriteResponse:
        """Replace the detector's pixel mask. `meta` is JSON: `url`, `version`,
        `shape` (rows, cols) and the detector's `serial`, for the backup's name.

        The mask there is read first, kept as a TIFF in the data root's
        `masks/backups/`, and must have the new one's shape; the new one is read
        back afterwards to confirm the detector holds what was sent.
        """
        require_detector()
        config = _read_meta(meta)
        url = str(config.get("url") or "").strip()
        version = str(config.get("version") or "1.8.0").strip()
        if not url:
            raise HTTPException(status_code=400, detail="No detector address")
        arr = pixel_mask.from_bytes(pixels.file.read(), config.get("shape"))
        if simplon_control.detector_state(url, version) in _BUSY_STATES:
            raise HTTPException(
                status_code=409, detail="The detector is busy; send the mask when it is idle."
            )
        previous, key = pixel_mask.read_detector_mask(url, version)
        if previous.shape != arr.shape:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"The detector's mask is {previous.shape[0]} x {previous.shape[1]} pixels, "
                    f"this one {arr.shape[0]} x {arr.shape[1]}."
                ),
            )
        root = deps.data_dir().resolve()
        backup_dir = root / "masks" / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        backup = backup_dir / pixel_mask.backup_name(str(config.get("serial") or ""))
        backup.write_bytes(pixel_mask.encode_tiff(previous))
        deps.invalidate_scans()
        pixel_mask.write_detector_mask(url, version, key, arr)
        try:
            after, _ = pixel_mask.read_detector_mask(url, version)
            verified = after.shape == arr.shape and bool(np.array_equal(after, arr))
        except HTTPException:
            verified = False
        deps.logger.info(
            "Pixel mask: sent to %s (%s, %d masked, was %d; verified: %s; backup %s)",
            url,
            key,
            pixel_mask.count_masked(arr),
            pixel_mask.count_masked(previous),
            verified,
            backup.name,
        )
        return PixelMaskDetectorWriteResponse(
            key=key,
            masked=pixel_mask.count_masked(arr),
            previous_masked=pixel_mask.count_masked(previous),
            backup=backup.relative_to(root).as_posix(),
            verified=verified,
        )
