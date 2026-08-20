from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import Response

try:
    from ..api_models import FrameMetadataResponse
    from ..image_formats import _to_little_endian
    from ..services.hdf5_stack import open_hdf5_for_read
    from .binary_response_utils import (
        add_optional_header,
        build_binary_headers,
        octet_stream_responses,
    )
except ImportError:  # pragma: no cover - supports `python backend/app.py`
    from api_models import FrameMetadataResponse  # type: ignore[no-redef]
    from binary_response_utils import (  # type: ignore[no-redef]
        add_optional_header,
        build_binary_headers,
        octet_stream_responses,
    )
    from image_formats import _to_little_endian  # type: ignore[no-redef]
    from services.hdf5_stack import open_hdf5_for_read  # type: ignore[no-redef]


FRAME_RESPONSE_DOCS = octet_stream_responses(
    "Raw frame bytes in little-endian C-order layout.",
    {
        "X-Dtype": "NumPy dtype string for decoding the payload.",
        "X-Shape": "Comma-separated array dimensions (rows,cols).",
        "X-Frame": "0-based frame index returned by this request.",
    },
)

MASK_RESPONSE_DOCS = octet_stream_responses(
    "Raw pixel-mask bytes in little-endian C-order layout.",
    {
        "X-Dtype": "NumPy dtype string for decoding the payload.",
        "X-Shape": "Comma-separated mask dimensions (rows,cols).",
        "X-Mask-Path": "Resolved HDF5 dataset path of the pixel mask.",
    },
)


@dataclass(frozen=True)
class FrameRouteDeps:
    ensure_hdf5_stack: Callable[[], None]
    get_h5py: Callable[[], Any]
    resolve_file: Callable[[str], Path]
    resolve_dataset_view: Callable[[Any, Path, str], tuple[dict[str, Any], list[Any]]]
    extract_frame: Callable[[dict[str, Any], int, int], np.ndarray]
    find_pixel_mask: Callable[[Any, int | None], Any | None]
    read_threshold_energies: Callable[[Any, int], list[float | None]]


def register_frame_routes(app: FastAPI, deps: FrameRouteDeps) -> None:
    @app.get("/api/metadata", response_model=FrameMetadataResponse)
    def metadata(
        file: str = Query(..., min_length=1), dataset: str = Query(..., min_length=1)
    ) -> FrameMetadataResponse:
        """Return typed metadata required to decode and navigate frame payloads."""
        deps.ensure_hdf5_stack()
        h5py = deps.get_h5py()
        path = deps.resolve_file(file)
        with open_hdf5_for_read(h5py, path) as h5:
            try:
                view, extra_files = deps.resolve_dataset_view(h5, path, dataset)
                try:
                    shape = tuple(int(x) for x in view["shape"])
                    response: dict[str, Any] = {
                        "path": dataset,
                        "shape": [int(x) for x in shape],
                        "dtype": str(view["dtype"]),
                        "ndim": int(view["ndim"]),
                        "chunks": view["dataset"].chunks if view["kind"] == "dataset" else None,
                        "maxshape": view["dataset"].maxshape if view["kind"] == "dataset" else None,
                        "linked_stack": view["kind"] == "linked_stack",
                    }
                    if int(view["ndim"]) == 4:
                        response["threshold_energies"] = deps.read_threshold_energies(h5, shape[1])
                    return FrameMetadataResponse(**response)
                finally:
                    for handle in extra_files:
                        handle.close()
            except KeyError as exc:
                raise HTTPException(status_code=404, detail="Dataset not found") from exc

    @app.get("/api/frame", responses=FRAME_RESPONSE_DOCS)
    def frame(
        file: str = Query(..., min_length=1),
        dataset: str = Query(..., min_length=1),
        index: int = Query(0, ge=0),
        threshold: int = Query(0, ge=0),
    ) -> Response:
        """Return one threshold-selected frame as raw little-endian bytes."""
        deps.ensure_hdf5_stack()
        h5py = deps.get_h5py()
        path = deps.resolve_file(file)
        with open_hdf5_for_read(h5py, path) as h5:
            try:
                view, extra_files = deps.resolve_dataset_view(h5, path, dataset)
            except KeyError as exc:
                raise HTTPException(status_code=404, detail="Dataset not found") from exc
            try:
                frame_data = deps.extract_frame(view, index=index, threshold=threshold)
            finally:
                for handle in extra_files:
                    handle.close()

            arr = _to_little_endian(np.asarray(frame_data))
            data = arr.tobytes(order="C")
            headers = build_binary_headers(dtype=arr.dtype.str, shape=arr.shape, frame=index)
            return Response(content=data, media_type="application/octet-stream", headers=headers)

    @app.get("/api/mask", responses=MASK_RESPONSE_DOCS)
    def mask(
        file: str = Query(..., min_length=1),
        threshold: int | None = Query(None, ge=0),
    ) -> Response:
        """Return the detector pixel mask as raw little-endian bytes."""
        deps.ensure_hdf5_stack()
        h5py = deps.get_h5py()
        path = deps.resolve_file(file)
        with open_hdf5_for_read(h5py, path) as h5:
            dset = deps.find_pixel_mask(h5, threshold=threshold)
            if not dset:
                raise HTTPException(status_code=404, detail="Pixel mask not found")
            if dset.ndim != 2:
                raise HTTPException(status_code=400, detail="Pixel mask has invalid shape")
            arr = _to_little_endian(np.asarray(dset))
            data = arr.tobytes(order="C")
            headers = build_binary_headers(dtype=arr.dtype.str, shape=arr.shape)
            add_optional_header(headers, "X-Mask-Path", dset.name)
            return Response(content=data, media_type="application/octet-stream", headers=headers)
