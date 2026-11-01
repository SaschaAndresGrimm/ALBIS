"""Image format helpers for ALBIS backend.

This module centralizes detector image file handling that is shared by
multiple endpoints (file load, metadata/header extraction, monitor parsing).
"""

from __future__ import annotations

import contextlib
import io
import logging
import re
import sys
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

import numpy as np
from fastapi import HTTPException

from .services.dectris_tiff_tag import (
    _DECTRIS_TIFF_TAG,
    _absolutize_dectris_offsets,
    _dectris_tiff_payload,
    _simplon_meta_from_tiff,
)
from .services.export_header import _mini_cbf_header_text, producer_string
from .services.pilatus_header import _parse_pilatus_header_text

# Lazy-loaded optional dependencies
_tifffile = None
_fabio_cbf_image_cls = None
_fabio_edf_image_cls = None
_fabio_tif_image_cls = None


def _ensure_tifffile() -> None:
    global _tifffile
    if _tifffile is None:
        import tifffile as tifffile_module  # type: ignore[import-not-found]

        _tifffile = tifffile_module


def _ensure_fabio_readers() -> None:
    global _fabio_cbf_image_cls
    global _fabio_edf_image_cls
    global _fabio_tif_image_cls

    if _fabio_cbf_image_cls is None:
        from fabio.cbfimage import CbfImage  # type: ignore[import-not-found]

        _fabio_cbf_image_cls = CbfImage
    if _fabio_edf_image_cls is None:
        from fabio.edfimage import EdfImage  # type: ignore[import-not-found]

        _fabio_edf_image_cls = EdfImage
    if _fabio_tif_image_cls is None:
        from fabio.tifimage import TifImage  # type: ignore[import-not-found]

        _fabio_tif_image_cls = TifImage


def _open_fabio_cbf_image(path: Path):
    _ensure_fabio_readers()
    try:
        return _fabio_cbf_image_cls().read(str(path))
    except Exception:
        if path.suffix.lower() != ".gz":
            raise
        import gzip
        import tempfile

        with gzip.open(path, "rb") as gz:
            data = gz.read()
        with tempfile.NamedTemporaryFile(suffix=".cbf", delete=False) as tmp:
            tmp.write(data)
            tmp_path = Path(tmp.name)
        try:
            return _fabio_cbf_image_cls().read(str(tmp_path))
        finally:
            with contextlib.suppress(Exception):
                tmp_path.unlink(missing_ok=True)


def _open_fabio_edf_image(path: Path):
    _ensure_fabio_readers()
    return _fabio_edf_image_cls().read(str(path))


def _open_fabio_tiff_image(path: Path):
    _ensure_fabio_readers()
    return _fabio_tif_image_cls().read(str(path))


def _open_fabio_image(path: Path):
    ext = _image_ext_name(path.name)
    try:
        if ext in {".cbf", ".cbf.gz"}:
            return _open_fabio_cbf_image(path)
        if ext == ".edf":
            return _open_fabio_edf_image(path)
        if ext in {".tif", ".tiff"}:
            return _open_fabio_tiff_image(path)
        return None
    except Exception:
        return None


def _fabio_header_text(image: Any) -> str:
    """The header of an open fabio image as text: a mini-CBF's header
    contents, or every header key and value."""
    if image is None:
        return ""
    header = getattr(image, "header", {}) or {}
    text = header.get("_array_data.header_contents") if isinstance(header, dict) else ""
    if isinstance(text, bytes):
        text = text.decode("utf-8", errors="ignore")
    if text:
        return str(text)
    try:
        return "\n".join(f"{k} {v}" for k, v in header.items())
    except Exception:
        return ""


def _pilatus_meta_from_fabio(path: Path) -> dict[str, Any]:
    image = _open_fabio_image(path)
    if image is None:
        return {}
    return _parse_pilatus_header_text(_fabio_header_text(image))


def _pilatus_meta_from_tiff(path: Path) -> dict[str, Any]:
    _ensure_tifffile()
    simplon_meta: dict[str, Any] = {}
    try:
        with _tifffile.TiffFile(path) as tiff:
            desc = ""
            try:
                desc = tiff.pages[0].description or ""
            except Exception:
                desc = ""
            simplon_meta = _simplon_meta_from_tiff(tiff)
    except Exception:
        desc = ""
    # Both, not one or the other. The private tag is typed and detector-written
    # so it wins any field they share, but it has no slot for the detector
    # model, pixel size, sensor thickness, tau, cutoff, gain or the angles --
    # and the text header has no slot for the series id, image number,
    # threshold ids or lost-pixel count. Returning the first non-empty one, as
    # this did, threw away whichever half it did not pick.
    desc_meta = _parse_pilatus_header_text(desc)
    merged = {**desc_meta, **simplon_meta}
    if merged:
        return merged
    try:
        return _pilatus_meta_from_fabio(path)
    except Exception:
        return {}


def _pilatus_meta_from_image(path: Path) -> dict[str, Any]:
    ext = _image_ext_name(path.name)
    if ext in {".tif", ".tiff"}:
        return _pilatus_meta_from_tiff(path)
    return _pilatus_meta_from_fabio(path)


def _pilatus_header_text(path: Path) -> str:
    ext = _image_ext_name(path.name)
    if ext in {".tif", ".tiff"}:
        _ensure_tifffile()
        try:
            with _tifffile.TiffFile(path) as tiff:
                desc = ""
                try:
                    desc = tiff.pages[0].description or ""
                except Exception:
                    desc = ""
            if desc:
                return str(desc)
        except Exception:
            pass
    return _fabio_header_text(_open_fabio_image(path))


def _read_image_with_header(path: Path) -> tuple[np.ndarray, dict[str, Any], str]:
    """A single-image file's pixels, its parsed header and the header text,
    from one read of the file.

    The same as _read_tiff / _read_cbf / _read_edf, _pilatus_meta_from_image
    and _pilatus_header_text together, which opened the file three to five
    times: for a long series on a network drive, the opening was most of the
    time. A TIFF without a header of its own still asks fabio, once.
    """
    ext = _image_ext_name(path.name)
    if ext in {".tif", ".tiff"}:
        _ensure_tifffile()
        try:
            with _tifffile.TiffFile(path) as tiff:
                try:
                    desc = str(tiff.pages[0].description or "")
                except Exception:
                    desc = ""
                simplon_meta: dict[str, Any] = {}
                with contextlib.suppress(Exception):
                    simplon_meta = _simplon_meta_from_tiff(tiff)
                arr = _normalize_image_array(np.asarray(tiff.asarray()))
        except HTTPException:
            raise
        except Exception as exc:
            raise _unreadable_image_error(path, exc) from exc
        meta = {**_parse_header_quietly(desc), **simplon_meta}
        if meta and desc:
            return arr, meta, desc
        fabio_text = _fabio_header_text(_open_fabio_image(path))
        return arr, meta or _parse_header_quietly(fabio_text), desc or fabio_text
    if ext in {".cbf", ".cbf.gz"}:
        opener = _open_fabio_cbf_image
    elif ext == ".edf":
        opener = _open_fabio_edf_image
    else:
        raise HTTPException(status_code=400, detail="Unsupported source format")
    opened: list[Any] = []

    def open_once(source: Path) -> Any:
        opened.append(opener(source))
        return opened[0]

    arr = _normalize_image_array(_decode_fabio_data(path, open_once))
    text = _fabio_header_text(opened[0] if opened else None)
    return arr, _parse_header_quietly(text), text


def _parse_header_quietly(text: str) -> dict[str, Any]:
    """The parsed header, or nothing: a header ALBIS cannot read costs the
    export its metadata, not the frame."""
    try:
        return _parse_pilatus_header_text(text)
    except Exception:
        return {}


def _image_ext_name(name: str) -> str:
    lower = name.lower()
    if lower.endswith(".cbf.gz"):
        return ".cbf.gz"
    return Path(lower).suffix


def _strip_image_ext(name: str, ext: str) -> str:
    if ext == ".cbf.gz" and name.lower().endswith(ext):
        return name[: -len(ext)]
    if name.lower().endswith(ext):
        return name[: -len(ext)]
    return Path(name).stem


def _split_series_name(name: str) -> tuple[str, str, str] | None:
    ext = _image_ext_name(name)
    stem = _strip_image_ext(name, ext)
    match = re.match(r"^(.*?)(\d+)([^\d]*)$", stem)
    if not match:
        return None
    prefix, digits, suffix = match.groups()
    if not digits:
        return None
    return prefix, digits, suffix


def _resolve_series_files(path: Path) -> tuple[list[Path], int]:
    """Resolve a sequence of numbered files into a sorted series."""
    ext = _image_ext_name(path.name)
    if ext not in {".tif", ".tiff", ".cbf", ".cbf.gz", ".edf"}:
        return [path], 0
    parts = _split_series_name(path.name)
    if not parts:
        return [path], 0
    prefix, digits, suffix = parts
    pattern = re.compile(
        rf"^{re.escape(prefix)}(\d+){re.escape(suffix)}{re.escape(ext)}$",
        re.IGNORECASE,
    )
    matches: list[tuple[int, Path]] = []
    try:
        for entry in path.parent.iterdir():
            if not entry.is_file():
                continue
            if _image_ext_name(entry.name) != ext:
                continue
            match = pattern.match(entry.name)
            if not match:
                continue
            idx = int(match.group(1))
            matches.append((idx, entry))
    except OSError:
        return [path], 0
    if not matches:
        return [path], 0
    matches.sort(key=lambda item: item[0])
    files = [item[1] for item in matches]
    index = 0
    for i, (_, entry) in enumerate(matches):
        if entry.name == path.name:
            index = i
            break
    return files, index


def _to_little_endian(arr: np.ndarray) -> np.ndarray:
    """Return `arr` with little-endian byte order, copying only when it differs.

    Frames leave the backend as raw bytes that the frontend decodes as
    little-endian, so anything big-endian has to be swapped first. NumPy 2.0
    removed `ndarray.newbyteorder`, so the relabel goes through a dtype view:
    `byteswap()` reorders the bytes but leaves the dtype still claiming
    big-endian, and the view fixes the label without touching the buffer again.

    Every caller must share this one implementation. Three copies of it existed
    previously and two were left on the removed API, which turned any
    big-endian source -- an HDF5 stack, a CBOR typed array from the
    JUNGFRAUJOCH bridge -- into a 500.
    """
    if arr.dtype.byteorder == ">" or (arr.dtype.byteorder == "=" and sys.byteorder == "big"):
        return arr.byteswap().view(arr.dtype.newbyteorder("<"))
    return arr


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
    frame = _to_little_endian(frame)
    if frame.dtype.kind in {"u", "i"} and frame.dtype.itemsize > 4:
        if frame.dtype.kind == "u":
            vmax = int(np.max(frame, initial=0))
            if vmax <= np.iinfo(np.uint32).max:
                frame = frame.astype(np.uint32, copy=False)
            else:
                frame = frame.astype(np.float64, copy=False)
        else:
            vmin = int(np.min(frame, initial=0))
            vmax = int(np.max(frame, initial=0))
            if vmin >= np.iinfo(np.int32).min and vmax <= np.iinfo(np.int32).max:
                frame = frame.astype(np.int32, copy=False)
            else:
                frame = frame.astype(np.float64, copy=False)
    return frame


def _unreadable_image_error(path: Path, exc: Exception) -> HTTPException:
    """Build the 422 raised when an image file exists but cannot be decoded.

    A viewer is pointed at whatever is on disk, and at a beamline that
    routinely includes a file the filewriter has not finished writing. Letting
    the decoder's own exception escape reports that as a 500 -- an ALBIS fault
    the user cannot act on -- when the honest answer is that the bytes are not
    readable yet. Mirrors how the MYTHEN readers below already report one.
    """
    # An empty message (fabio raises bare AssertionErrors) is useless to a
    # user, so fall back to the exception type; either way it is capped so a
    # verbose decoder never dominates the toast.
    reason = str(exc).strip().replace("\n", " ") or type(exc).__name__
    if len(reason) > 120:
        reason = reason[:117] + "..."
    return HTTPException(
        status_code=422,
        detail=f"Cannot read {path.name}: file may be incomplete or corrupt ({reason})",
    )


class _FabioErrorProbe(logging.Handler):
    """Capture fabio's own error records emitted on the calling thread.

    fabio does not always raise on a short file: a truncated EDF decodes into a
    correctly shaped array whose missing tail is silently zero-filled, and it
    reports that only by logging `Data stream is incomplete`. Left alone, ALBIS
    would render half a frame as genuine zero counts -- for a viewer used to
    judge data quality, quietly wrong is worse than an error.

    Records are matched by thread id because the handler is attached to a
    process-wide logger: export and series-sum jobs decode images on worker
    threads, and without the check one job's bad file could fail an unrelated
    request decoding a good one.
    """

    def __init__(self) -> None:
        super().__init__(level=logging.ERROR)
        self.thread_id = threading.get_ident()
        self.message = ""

    def emit(self, record: logging.LogRecord) -> None:
        if not self.message and record.thread == self.thread_id:
            self.message = record.getMessage()


def _decode_fabio_data(path: Path, opener: Callable[[Path], Any]) -> np.ndarray:
    """Decode a fabio-backed image, converting any decode failure into a 422."""
    probe = _FabioErrorProbe()
    fabio_logger = logging.getLogger("fabio")
    fabio_logger.addHandler(probe)
    try:
        image = opener(path)
        # `.data` is lazy: the read, and any truncation error, happens here.
        arr = np.asarray(image.data)
    except HTTPException:
        raise
    except Exception as exc:
        raise _unreadable_image_error(path, exc) from exc
    finally:
        fabio_logger.removeHandler(probe)
    if probe.message:
        raise _unreadable_image_error(path, RuntimeError(probe.message))
    return arr


def _read_tiff(path: Path, index: int = 0) -> np.ndarray:
    _ensure_tifffile()
    try:
        arr = np.asarray(_tifffile.imread(path))
    except Exception as exc:
        raise _unreadable_image_error(path, exc) from exc
    return _normalize_image_array(arr, index=index)


def _read_tiff_bytes(raw: bytes, index: int = 0) -> np.ndarray:
    _ensure_tifffile()
    arr = _tifffile.imread(io.BytesIO(raw))
    return _normalize_image_array(np.asarray(arr), index=index)


def _read_tiff_bytes_with_simplon_meta(raw: bytes) -> tuple[np.ndarray, dict[str, Any]]:
    _ensure_tifffile()
    meta: dict[str, Any] = {}
    try:
        with _tifffile.TiffFile(io.BytesIO(raw)) as tiff:
            meta = _simplon_meta_from_tiff(tiff, raw=raw)
            arr = _normalize_image_array(np.asarray(tiff.asarray()))
            return arr, meta
    except Exception:
        arr = _read_tiff_bytes(raw)
        return arr, meta


def _write_tiff(path: Path, arr: np.ndarray, metadata: dict[str, Any] | None = None) -> None:
    _ensure_tifffile()
    dectris_payload = _dectris_tiff_payload(metadata)
    extratags = None
    if dectris_payload:
        extratags = [(_DECTRIS_TIFF_TAG, 7, len(dectris_payload), dectris_payload, False)]
    # `Software` and `ImageDescription` are the standard TIFF places to say what
    # wrote a file and what it holds, and they cost nothing to a reader that
    # ignores them. Written unconditionally, so a summed or converted frame is
    # traceable even where no source metadata was parsed.
    #
    # ImageDescription carries the SAME text the CBF header does, not just the
    # provenance lines. The private DECTRIS tag has no slot for the detector
    # model, serial, location, pixel size, sensor thickness, tau, count cutoff,
    # gain or the rotation angles, and inventing tag ids for them would be
    # guessing inside someone else's numbering -- a collision with a real
    # assignment would have DECTRIS software misread the file. A standard text
    # tag cannot collide, and tifffile, PIL and ImageJ all surface it.
    _tifffile.imwrite(
        path,
        arr,
        photometric="minisblack",
        byteorder="<",
        software=producer_string(),
        description=_mini_cbf_header_text(metadata),
        extratags=extratags,
    )
    if dectris_payload:
        _absolutize_dectris_offsets(path, dectris_payload)


def _write_cbf(path: Path, arr: np.ndarray, metadata: dict[str, Any] | None = None) -> None:
    _ensure_fabio_readers()
    header: dict[str, Any] = {
        "_array_data.header_convention": "SLS_1.0",
        "_array_data.header_contents": _mini_cbf_header_text(metadata),
    }
    _fabio_cbf_image_cls(data=np.asarray(arr), header=header).write(str(path))


def _read_cbf(path: Path) -> np.ndarray:
    return _normalize_image_array(_decode_fabio_data(path, _open_fabio_cbf_image))


def _read_cbf_gz(path: Path) -> np.ndarray:
    return _normalize_image_array(_decode_fabio_data(path, _open_fabio_cbf_image))


def _read_edf(path: Path) -> np.ndarray:
    return _normalize_image_array(_decode_fabio_data(path, _open_fabio_edf_image))
