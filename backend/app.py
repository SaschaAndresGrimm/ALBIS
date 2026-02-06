from __future__ import annotations

import fnmatch
import io
import json
import os
import platform
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import hdf5plugin  # noqa: F401
import h5py
import numpy as np
import tifffile
import fabio
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
import shutil

DATA_DIR = Path(os.environ.get("VIEWER_DATA_DIR", "")).expanduser()
if not DATA_DIR:
    DATA_DIR = Path(__file__).resolve().parents[1]

app = FastAPI(title="ALBIS — ALBIS WEB VIEW")

AUTOLOAD_EXTS = {".h5", ".hdf5", ".tif", ".tiff", ".cbf"}
ALLOW_ABS_PATHS = os.environ.get("VIEWER_ALLOW_ABS", "1").lower() in {"1", "true", "yes", "on"}


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
    raw = Path(name)
    if raw.is_absolute():
        if not ALLOW_ABS_PATHS:
            raise HTTPException(status_code=400, detail="Absolute paths are disabled")
        path = raw.expanduser().resolve()
        if not path.exists() or path.suffix.lower() not in {".h5", ".hdf5"}:
            raise HTTPException(status_code=404, detail="File not found")
        return path
    safe = _safe_rel_path(name)
    path = (DATA_DIR / safe).resolve()
    if not _is_within(path, DATA_DIR.resolve()):
        raise HTTPException(status_code=400, detail="Invalid file name")
    if not path.exists() or path.suffix.lower() not in {".h5", ".hdf5"}:
        raise HTTPException(status_code=404, detail="File not found")
    return path


def _resolve_dir(name: str | None) -> Path:
    if name is None:
        return DATA_DIR.resolve()
    trimmed = name.strip()
    if trimmed in ("", ".", "./"):
        return DATA_DIR.resolve()
    raw = Path(trimmed)
    if raw.is_absolute():
        if not ALLOW_ABS_PATHS:
            raise HTTPException(status_code=400, detail="Absolute paths are disabled")
        path = raw.expanduser().resolve()
        if not path.exists() or not path.is_dir():
            raise HTTPException(status_code=404, detail="Directory not found")
        return path
    safe = _safe_rel_path(trimmed)
    path = (DATA_DIR / safe).resolve()
    if not _is_within(path, DATA_DIR.resolve()):
        raise HTTPException(status_code=400, detail="Invalid directory")
    if not path.exists() or not path.is_dir():
        raise HTTPException(status_code=404, detail="Directory not found")
    return path


def _resolve_image_file(name: str) -> Path:
    raw = Path(name)
    if raw.is_absolute():
        if not ALLOW_ABS_PATHS:
            raise HTTPException(status_code=400, detail="Absolute paths are disabled")
        path = raw.expanduser().resolve()
        if not path.exists() or path.suffix.lower() not in AUTOLOAD_EXTS:
            raise HTTPException(status_code=404, detail="File not found")
        return path
    safe = _safe_rel_path(name)
    path = (DATA_DIR / safe).resolve()
    if not _is_within(path, DATA_DIR.resolve()):
        raise HTTPException(status_code=400, detail="Invalid file name")
    if not path.exists() or path.suffix.lower() not in AUTOLOAD_EXTS:
        raise HTTPException(status_code=404, detail="File not found")
    return path


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _normalize_image_array(arr: np.ndarray, index: int = 0) -> np.ndarray:
    if arr.ndim == 2:
        frame = arr
    elif arr.ndim == 3:
        if arr.shape[-1] in (3, 4):
            frame = arr[..., 0]
        else:
            idx = max(0, min(index, arr.shape[0] - 1))
            frame = arr[idx]
    else:
        raise HTTPException(status_code=400, detail="Unsupported image shape")
    frame = np.ascontiguousarray(frame)
    if frame.dtype.byteorder == ">" or (frame.dtype.byteorder == "=" and sys.byteorder == "big"):
        frame = frame.byteswap().newbyteorder("<")
    return frame


def _read_tiff(path: Path, index: int = 0) -> np.ndarray:
    arr = tifffile.imread(path)
    return _normalize_image_array(np.asarray(arr), index=index)


def _read_cbf(path: Path) -> np.ndarray:
    image = fabio.open(str(path))
    arr = np.asarray(image.data)
    return _normalize_image_array(arr)


def _parse_ext_filter(exts: str | None) -> set[str]:
    if not exts:
        return set(AUTOLOAD_EXTS)
    cleaned: set[str] = set()
    for raw in exts.split(","):
        token = raw.strip().lower()
        if not token:
            continue
        if not token.startswith("."):
            token = f".{token}"
        cleaned.add(token)
    allowed = cleaned.intersection(AUTOLOAD_EXTS)
    return allowed or set(AUTOLOAD_EXTS)


def _matches_pattern(path: Path, pattern: str | None, root: Path) -> bool:
    if not pattern:
        return True
    norm = pattern.strip()
    if not norm:
        return True
    try:
        rel = path.relative_to(root).as_posix()
    except ValueError:
        rel = path.name
    target = rel if ("/" in norm or "\\" in norm) else path.name
    return fnmatch.fnmatch(target, norm)


def _latest_image_file(root: Path, allowed_exts: set[str], pattern: str | None) -> Path | None:
    latest_path: Path | None = None
    latest_mtime = -1.0
    root = root.resolve()
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix.lower() not in allowed_exts:
            continue
        if not _matches_pattern(path, pattern, root):
            continue
        try:
            rel = path.relative_to(root)
        except ValueError:
            continue
        if any(part.startswith(".") for part in rel.parts):
            continue
        try:
            mtime = path.stat().st_mtime
        except OSError:
            continue
        if mtime > latest_mtime:
            latest_mtime = mtime
            latest_path = path
    return latest_path


def _simplon_base(url: str, version: str) -> str:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise HTTPException(status_code=400, detail="Invalid SIMPLON base URL")
    base = url.rstrip("/")
    ver = (version or "1.8.0").strip().strip("/")
    if not ver:
        ver = "1.8.0"
    return f"{base}/monitor/api/{ver}"


def _simplon_set_mode(base: str, mode: str) -> None:
    payload = json.dumps({"value": mode}).encode("utf-8")
    req = urllib.request.Request(
        f"{base}/config/mode",
        data=payload,
        method="PUT",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            resp.read()
    except Exception as exc:
        raise HTTPException(status_code=502, detail="Failed to update SIMPLON monitor mode") from exc


def _simplon_fetch_monitor(base: str, timeout_ms: int) -> bytes | None:
    query = urllib.parse.urlencode({"timeout": max(0, int(timeout_ms))}) if timeout_ms else ""
    url = f"{base}/images/monitor"
    if query:
        url = f"{url}?{query}"
    try:
        with urllib.request.urlopen(url, timeout=max(timeout_ms / 1000 + 1, 2)) as resp:
            return resp.read()
    except urllib.error.HTTPError as exc:
        if exc.code in {204, 408}:
            return None
        raise HTTPException(status_code=502, detail=f"SIMPLON monitor error {exc.code}") from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail="Failed to fetch SIMPLON monitor image") from exc


def _resolve_external_path(base_file: Path, filename: str | None) -> Path | None:
    if not filename:
        return None
    target = Path(str(filename))
    if not target.is_absolute():
        target = base_file.parent / target
    target = target.expanduser().resolve()
    if not ALLOW_ABS_PATHS:
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


@app.get("/api/folders")
def folders() -> dict[str, list[str]]:
    dirs: set[str] = set()
    for path in DATA_DIR.rglob("*"):
        if not path.is_dir():
            continue
        try:
            rel = path.relative_to(DATA_DIR).as_posix()
        except ValueError:
            continue
        if not rel:
            continue
        if any(part.startswith(".") for part in Path(rel).parts):
            continue
        dirs.add(rel)
    return {"folders": sorted(dirs)}


@app.get("/api/choose-folder")
def choose_folder() -> Response:
    if not ALLOW_ABS_PATHS:
        raise HTTPException(status_code=403, detail="Absolute paths are disabled")
    if platform.system() != "Darwin":
        raise HTTPException(status_code=501, detail="Folder picker not supported on this OS")
    script = 'POSIX path of (choose folder with prompt "Select Auto Load folder")'
    try:
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").lower()
        if "user canceled" in stderr:
            return Response(status_code=204)
        raise HTTPException(status_code=500, detail="Folder picker failed") from exc
    path = result.stdout.strip()
    if not path:
        return Response(status_code=204)
    return JSONResponse({"path": path})


@app.get("/api/autoload/latest")
def autoload_latest(
    folder: str | None = Query(None),
    exts: str | None = Query(None),
    pattern: str | None = Query(None),
) -> Response:
    root = _resolve_dir(folder)
    allowed = _parse_ext_filter(exts)
    latest = _latest_image_file(root, allowed, pattern)
    if not latest:
        return Response(status_code=204)
    try:
        rel = latest.resolve().relative_to(DATA_DIR.resolve()).as_posix()
        absolute = False
        file_label = rel
    except ValueError:
        if not ALLOW_ABS_PATHS:
            raise HTTPException(status_code=400, detail="Invalid file location")
        absolute = True
        file_label = str(latest.resolve())
    return JSONResponse(
        {
            "file": file_label,
            "ext": latest.suffix.lower(),
            "mtime": latest.stat().st_mtime,
            "absolute": absolute,
        }
    )


@app.get("/api/image")
def image(
    file: str = Query(..., min_length=1),
    index: int = Query(0, ge=0),
) -> Response:
    path = _resolve_image_file(file)
    ext = path.suffix.lower()
    if ext in {".h5", ".hdf5"}:
        raise HTTPException(status_code=400, detail="Use /api/frame for HDF5 datasets")
    if ext in {".tif", ".tiff"}:
        arr = _read_tiff(path, index=index)
    elif ext == ".cbf":
        arr = _read_cbf(path)
    else:
        raise HTTPException(status_code=400, detail="Unsupported image format")

    data = arr.tobytes(order="C")
    headers = {
        "X-Dtype": arr.dtype.str,
        "X-Shape": ",".join(str(x) for x in arr.shape),
        "X-Frame": "0",
    }
    return Response(content=data, media_type="application/octet-stream", headers=headers)


@app.get("/api/simplon/monitor")
def simplon_monitor(
    url: str = Query(..., min_length=4),
    version: str = Query("1.8.0"),
    timeout: int = Query(500, ge=0),
    enable: bool = Query(True),
) -> Response:
    base = _simplon_base(url, version)
    if enable:
        _simplon_set_mode(base, "enabled")
    data = _simplon_fetch_monitor(base, timeout)
    if data is None:
        return Response(status_code=204)
    arr = _normalize_image_array(np.asarray(tifffile.imread(io.BytesIO(data))))
    data_bytes = arr.tobytes(order="C")
    headers = {
        "X-Dtype": arr.dtype.str,
        "X-Shape": ",".join(str(x) for x in arr.shape),
        "X-Frame": "0",
    }
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
    base = _simplon_base(url, version)
    _simplon_set_mode(base, mode_value)
    return {"status": "ok", "mode": mode_value}


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
