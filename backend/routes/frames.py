from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import numpy as np
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import Response


@dataclass(frozen=True)
class FrameRouteDeps:
    ensure_hdf5_stack: Callable[[], None]
    get_h5py: Callable[[], Any]
    resolve_file: Callable[[str], Path]
    resolve_dataset_view: Callable[[Any, Path, str], tuple[dict[str, Any], list[Any]]]
    extract_frame: Callable[[dict[str, Any], int, int], np.ndarray]
    find_pixel_mask: Callable[[Any, int | None], Any | None]
    read_threshold_energies: Callable[[Any, int], list[float | None]]


def _to_little_endian(arr: np.ndarray) -> np.ndarray:
    if arr.dtype.byteorder == ">" or (arr.dtype.byteorder == "=" and sys.byteorder == "big"):
        return arr.byteswap().newbyteorder("<")
    return arr


def register_frame_routes(app: FastAPI, deps: FrameRouteDeps) -> None:
    @app.get("/api/metadata")
    def metadata(
        file: str = Query(..., min_length=1), dataset: str = Query(..., min_length=1)
    ) -> dict[str, Any]:
        deps.ensure_hdf5_stack()
        h5py = deps.get_h5py()
        path = deps.resolve_file(file)
        with h5py.File(path, "r") as h5:
            try:
                view, extra_files = deps.resolve_dataset_view(h5, path, dataset)
                try:
                    shape = tuple(int(x) for x in view["shape"])
                    response = {
                        "path": dataset,
                        "shape": shape,
                        "dtype": str(view["dtype"]),
                        "ndim": int(view["ndim"]),
                        "chunks": view["dataset"].chunks if view["kind"] == "dataset" else None,
                        "maxshape": view["dataset"].maxshape if view["kind"] == "dataset" else None,
                        "linked_stack": view["kind"] == "linked_stack",
                    }
                    if int(view["ndim"]) == 4:
                        response["threshold_energies"] = deps.read_threshold_energies(h5, shape[1])
                    return response
                finally:
                    for handle in extra_files:
                        handle.close()
            except KeyError as exc:
                raise HTTPException(status_code=404, detail="Dataset not found") from exc

    @app.get("/api/frame")
    def frame(
        file: str = Query(..., min_length=1),
        dataset: str = Query(..., min_length=1),
        index: int = Query(0, ge=0),
        threshold: int = Query(0, ge=0),
    ) -> Response:
        deps.ensure_hdf5_stack()
        h5py = deps.get_h5py()
        path = deps.resolve_file(file)
        with h5py.File(path, "r") as h5:
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
            headers = {
                "X-Dtype": arr.dtype.str,
                "X-Shape": ",".join(str(x) for x in arr.shape),
                "X-Frame": str(index),
            }
            return Response(content=data, media_type="application/octet-stream", headers=headers)

    @app.get("/api/preview")
    def preview(
        file: str = Query(..., min_length=1),
        dataset: str = Query(..., min_length=1),
        index: int = Query(0, ge=0),
        max_size: int = Query(1024, ge=64, le=4096),
        threshold: int = Query(0, ge=0),
    ) -> Response:
        deps.ensure_hdf5_stack()
        h5py = deps.get_h5py()
        path = deps.resolve_file(file)
        with h5py.File(path, "r") as h5:
            try:
                view, extra_files = deps.resolve_dataset_view(h5, path, dataset)
            except KeyError as exc:
                raise HTTPException(status_code=404, detail="Dataset not found") from exc
            try:
                frame_data = deps.extract_frame(view, index=index, threshold=threshold)
            finally:
                for handle in extra_files:
                    handle.close()

            arr = np.asarray(frame_data)
            height, width = arr.shape
            scale = max(height / max_size, width / max_size, 1.0)
            if scale > 1:
                step = int(np.ceil(scale))
                arr = arr[::step, ::step]

            arr = _to_little_endian(arr)
            data = arr.tobytes(order="C")
            headers = {
                "X-Dtype": arr.dtype.str,
                "X-Shape": ",".join(str(x) for x in arr.shape),
                "X-Frame": str(index),
                "X-Preview": "1",
            }
            return Response(content=data, media_type="application/octet-stream", headers=headers)

    @app.get("/api/mask")
    def mask(
        file: str = Query(..., min_length=1),
        threshold: int | None = Query(None, ge=0),
    ) -> Response:
        deps.ensure_hdf5_stack()
        h5py = deps.get_h5py()
        path = deps.resolve_file(file)
        with h5py.File(path, "r") as h5:
            dset = deps.find_pixel_mask(h5, threshold=threshold)
            if not dset:
                raise HTTPException(status_code=404, detail="Pixel mask not found")
            if dset.ndim != 2:
                raise HTTPException(status_code=400, detail="Pixel mask has invalid shape")
            arr = _to_little_endian(np.asarray(dset))
            data = arr.tobytes(order="C")
            headers = {
                "X-Dtype": arr.dtype.str,
                "X-Shape": ",".join(str(x) for x in arr.shape),
                "X-Mask-Path": dset.name,
            }
            return Response(content=data, media_type="application/octet-stream", headers=headers)
