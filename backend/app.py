from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import hdf5plugin  # noqa: F401
import h5py
import numpy as np
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles
import shutil

DATA_DIR = Path(os.environ.get("VIEWER_DATA_DIR", "")).expanduser()
if not DATA_DIR:
    DATA_DIR = Path(__file__).resolve().parents[1]

app = FastAPI(title="ALBIS — ALBIS WEB VIEW")


def _safe_rel_path(name: str) -> Path:
    if not name:
        raise HTTPException(status_code=400, detail="Invalid file name")
    if name.startswith(("/", "\\")):
        raise HTTPException(status_code=400, detail="Invalid file name")
    raw = Path(name)
    if raw.is_absolute():
        raise HTTPException(status_code=400, detail="Invalid file name")
    if any(part == ".." or part.startswith(".") for part in raw.parts):
        raise HTTPException(status_code=400, detail="Invalid file name")
    return raw


def _resolve_file(name: str) -> Path:
    safe = _safe_rel_path(name)
    path = (DATA_DIR / safe).resolve()
    if not _is_within(path, DATA_DIR.resolve()):
        raise HTTPException(status_code=400, detail="Invalid file name")
    if not path.exists() or path.suffix.lower() not in {".h5", ".hdf5"}:
        raise HTTPException(status_code=404, detail="File not found")
    return path


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _resolve_external_path(base_file: Path, filename: str | None) -> Path | None:
    if not filename:
        return None
    target = Path(str(filename))
    if not target.is_absolute():
        target = base_file.parent / target
    target = target.expanduser().resolve()
    allowed_root = DATA_DIR.resolve() if DATA_DIR else base_file.parent.resolve()
    if allowed_root and not _is_within(target, allowed_root):
        return None
    if not target.exists() or target.suffix.lower() not in {".h5", ".hdf5"}:
        return None
    return target


def _dataset_info(name: str, obj: Any) -> dict[str, Any] | None:
    if not isinstance(obj, h5py.Dataset):
        return None
    shape = tuple(int(x) for x in obj.shape)
    dtype = str(obj.dtype)
    ndim = obj.ndim
    size = int(np.prod(shape)) if shape else 0
    return {
        "path": f"/{name}" if not name.startswith("/") else name,
        "shape": shape,
        "dtype": dtype,
        "ndim": ndim,
        "size": size,
        "chunks": obj.chunks,
        "maxshape": obj.maxshape,
    }


def _is_image_dataset(info: dict[str, Any]) -> bool:
    if info["ndim"] not in (2, 3):
        return False
    dtype = info["dtype"]
    return any(token in dtype for token in ("int", "uint", "float"))


MASK_PATHS = (
    "/entry/instrument/detector/detectorSpecific/pixel_mask",
    "/entry/instrument/detector/pixel_mask",
    "/entry/instrument/detector/detectorSpecific/bad_pixel_mask",
    "/entry/instrument/detector/bad_pixel_mask",
    "/entry/instrument/detector/pixel_mask_applied",
    "/entry/instrument/detector/detectorSpecific/pixel_mask_applied",
)


def _find_pixel_mask(h5: h5py.File) -> h5py.Dataset | None:
    for path in MASK_PATHS:
        if path in h5:
            obj = h5[path]
            if isinstance(obj, h5py.Dataset) and obj.ndim == 2:
                return obj
    return None


def _walk_datasets(
    obj: Any,
    base_path: str,
    file_path: Path,
    results: list[dict[str, Any]],
    ancestors: set[tuple[Path, bytes]],
    file_cache: dict[Path, h5py.File],
) -> None:
    if isinstance(obj, h5py.Dataset):
        info = _dataset_info(base_path, obj)
        if info:
            info["image"] = _is_image_dataset(info)
            results.append(info)
        return
    if not isinstance(obj, h5py.Group):
        return
    obj_ref = (file_path, obj.id)
    if obj_ref in ancestors:
        return
    next_ancestors = set(ancestors)
    next_ancestors.add(obj_ref)

    for name in obj.keys():
        try:
            link = obj.get(name, getlink=True)
        except Exception:
            continue
        child_path = f"{base_path}/{name}" if base_path != "/" else f"/{name}"
        if isinstance(link, h5py.ExternalLink):
            target_path = _resolve_external_path(file_path, link.filename)
            if not target_path:
                continue
            target_file = file_cache.get(target_path)
            if target_file is None:
                try:
                    target_file = h5py.File(target_path, "r")
                except OSError:
                    continue
                file_cache[target_path] = target_file
            try:
                target_obj = target_file[link.path]
            except Exception:
                continue
            _walk_datasets(target_obj, child_path, target_path, results, next_ancestors, file_cache)
            continue
        if isinstance(link, h5py.SoftLink):
            try:
                target_obj = obj[link.path]
            except Exception:
                continue
            _walk_datasets(target_obj, child_path, file_path, results, next_ancestors, file_cache)
            continue
        try:
            target_obj = obj[name]
        except Exception:
            continue
        _walk_datasets(target_obj, child_path, file_path, results, next_ancestors, file_cache)


def _resolve_dataset(
    h5: h5py.File, base_file: Path, dataset: str
) -> tuple[h5py.Dataset, list[h5py.File]]:
    parts = [p for p in dataset.strip("/").split("/") if p]
    if not parts:
        raise KeyError("Dataset not found")
    current: Any = h5["/"]
    current_file = base_file
    opened: list[h5py.File] = []

    for idx, part in enumerate(parts):
        if not isinstance(current, h5py.Group):
            raise KeyError("Dataset not found")
        link = current.get(part, getlink=True)
        if link is None:
            raise KeyError("Dataset not found")
        if isinstance(link, h5py.ExternalLink):
            target_path = _resolve_external_path(current_file, link.filename)
            if not target_path:
                raise KeyError("Dataset not found")
            try:
                target_file = h5py.File(target_path, "r")
            except OSError as exc:
                raise KeyError("Dataset not found") from exc
            opened.append(target_file)
            current_file = target_path
            try:
                current = target_file[link.path]
            except Exception as exc:
                raise KeyError("Dataset not found") from exc
        elif isinstance(link, h5py.SoftLink):
            try:
                current = current[link.path]
            except Exception as exc:
                raise KeyError("Dataset not found") from exc
        else:
            try:
                current = current[part]
            except Exception as exc:
                raise KeyError("Dataset not found") from exc

        if idx < len(parts) - 1 and isinstance(current, h5py.Dataset):
            raise KeyError("Dataset not found")

    if not isinstance(current, h5py.Dataset):
        raise KeyError("Dataset not found")
    return current, opened


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/files")
def files() -> dict[str, list[str]]:
    items: list[str] = []
    for pattern in ("*.h5", "*.hdf5"):
        for path in DATA_DIR.rglob(pattern):
            if not path.is_file():
                continue
            try:
                rel = path.relative_to(DATA_DIR).as_posix()
            except ValueError:
                continue
            if any(part.startswith(".") for part in Path(rel).parts):
                continue
            items.append(rel)
    return {"files": sorted(set(items))}


@app.post("/api/upload")
async def upload(file: UploadFile = File(...)) -> dict[str, str]:
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename")
    safe_path = _safe_rel_path(Path(file.filename).name)
    safe = safe_path.as_posix()
    if not safe.lower().endswith((".h5", ".hdf5")):
        raise HTTPException(status_code=400, detail="Only .h5/.hdf5 files are supported")
    dest = (DATA_DIR / safe).resolve()
    with dest.open("wb") as fh:
        shutil.copyfileobj(file.file, fh)
    return {"filename": safe}


@app.get("/api/datasets")
def datasets(file: str = Query(..., min_length=1)) -> dict[str, Any]:
    path = _resolve_file(file)
    results: list[dict[str, Any]] = []
    with h5py.File(path, "r") as h5:
        file_cache: dict[Path, h5py.File] = {path: h5}
        try:
            _walk_datasets(h5["/"], "/", path, results, set(), file_cache)
        finally:
            for cache_path, handle in file_cache.items():
                if cache_path == path:
                    continue
                try:
                    handle.close()
                except Exception:
                    pass
    return {"datasets": results}


@app.get("/api/metadata")
def metadata(file: str = Query(..., min_length=1), dataset: str = Query(..., min_length=1)) -> dict[str, Any]:
    path = _resolve_file(file)
    with h5py.File(path, "r") as h5:
        try:
            dset, extra_files = _resolve_dataset(h5, path, dataset)
            try:
                shape = tuple(int(x) for x in dset.shape)
                return {
                    "path": dataset,
                    "shape": shape,
                    "dtype": str(dset.dtype),
                    "ndim": dset.ndim,
                    "chunks": dset.chunks,
                    "maxshape": dset.maxshape,
                }
            finally:
                for handle in extra_files:
                    handle.close()
        except KeyError:
            raise HTTPException(status_code=404, detail="Dataset not found")


@app.get("/api/frame")
def frame(
    file: str = Query(..., min_length=1),
    dataset: str = Query(..., min_length=1),
    index: int = Query(0, ge=0),
) -> Response:
    path = _resolve_file(file)
    with h5py.File(path, "r") as h5:
        try:
            dset, extra_files = _resolve_dataset(h5, path, dataset)
        except KeyError:
            raise HTTPException(status_code=404, detail="Dataset not found")
        try:
            if dset.ndim == 3:
                if index >= dset.shape[0]:
                    raise HTTPException(status_code=416, detail="Frame index out of range")
                frame_data = dset[index, :, :]
            elif dset.ndim == 2:
                frame_data = dset[:, :]
            else:
                raise HTTPException(status_code=400, detail="Dataset is not 2D or 3D")
        finally:
            for handle in extra_files:
                handle.close()

        arr = np.asarray(frame_data)
        if arr.dtype.byteorder == ">" or (arr.dtype.byteorder == "=" and sys.byteorder == "big"):
            arr = arr.byteswap().newbyteorder("<")

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
) -> Response:
    path = _resolve_file(file)
    with h5py.File(path, "r") as h5:
        try:
            dset, extra_files = _resolve_dataset(h5, path, dataset)
        except KeyError:
            raise HTTPException(status_code=404, detail="Dataset not found")
        try:
            if dset.ndim == 3:
                if index >= dset.shape[0]:
                    raise HTTPException(status_code=416, detail="Frame index out of range")
                frame_data = dset[index, :, :]
            elif dset.ndim == 2:
                frame_data = dset[:, :]
            else:
                raise HTTPException(status_code=400, detail="Dataset is not 2D or 3D")
        finally:
            for handle in extra_files:
                handle.close()

        arr = np.asarray(frame_data)
        height, width = arr.shape
        scale = max(height / max_size, width / max_size, 1.0)
        if scale > 1:
            step = int(np.ceil(scale))
            arr = arr[::step, ::step]

        if arr.dtype.byteorder == ">" or (arr.dtype.byteorder == "=" and sys.byteorder == "big"):
            arr = arr.byteswap().newbyteorder("<")

        data = arr.tobytes(order="C")
        headers = {
            "X-Dtype": arr.dtype.str,
            "X-Shape": ",".join(str(x) for x in arr.shape),
            "X-Frame": str(index),
            "X-Preview": "1",
        }
        return Response(content=data, media_type="application/octet-stream", headers=headers)


@app.get("/api/mask")
def mask(file: str = Query(..., min_length=1)) -> Response:
    path = _resolve_file(file)
    with h5py.File(path, "r") as h5:
        dset = _find_pixel_mask(h5)
        if not dset:
            raise HTTPException(status_code=404, detail="Pixel mask not found")
        if dset.ndim != 2:
            raise HTTPException(status_code=400, detail="Pixel mask has invalid shape")
        arr = np.asarray(dset)
        if arr.dtype.byteorder == ">" or (arr.dtype.byteorder == "=" and sys.byteorder == "big"):
            arr = arr.byteswap().newbyteorder("<")
        data = arr.tobytes(order="C")
        headers = {
            "X-Dtype": arr.dtype.str,
            "X-Shape": ",".join(str(x) for x in arr.shape),
            "X-Mask-Path": dset.name,
        }
        return Response(content=data, media_type="application/octet-stream", headers=headers)


STATIC_DIR = Path(__file__).resolve().parents[1] / "frontend"
app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
