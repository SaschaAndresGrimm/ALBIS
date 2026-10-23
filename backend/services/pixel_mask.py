"""Pixel masks (beta): read one from a file or a detector, write one to either.

A mask is a 2-D uint32 array in DECTRIS's layout: each set bit is a reason the
pixel is masked (bit 0 a gap between modules, bits 1-4 defective pixels, and
so on). The bits are kept as they are everywhere here -- reading, saving and
sending -- so a mask that goes through ALBIS comes back with the same reasons.

The detector side speaks SIMPLON's array format, the base64 `__darray__` it
also answers a read with. Sending replaces the detector's mask, so the route
reads the one there first and keeps it as a backup.
"""

from __future__ import annotations

import base64
import io
import time
import urllib.error
from pathlib import Path
from typing import Any

import numpy as np
from fastapi import HTTPException

from .. import image_formats
from . import simplon_control
from .simplon import _raise_simplon_failure

MASK_DTYPE = np.dtype("<u4")
# Larger than any detector made; a guard against a malformed shape, not a limit.
MAX_PIXELS = 64 * 1024 * 1024
# Where a saved HDF5 mask keeps it: the NeXus path ALBIS reads masks from.
HDF5_MASK_PATH = "/entry/instrument/detector/pixel_mask"
# The keys a detector keeps its mask under, in the order they are tried: the
# whole detector, then the first threshold's (a detector with thresholds may
# answer only that).
DETECTOR_MASK_KEYS = ("pixel_mask", "threshold/1/pixel_mask")
# A 16M detector's mask is 18 M pixels, some 96 MB of base64 each way, which a
# DCU takes a while to encode or to check and apply.
_WRITE_TIMEOUT_S = 120.0
_READ_TIMEOUT_S = 60.0


def as_mask(arr: Any) -> np.ndarray:
    """A 2-D uint32 copy of a mask, or 400 when it cannot be one."""
    data = np.asarray(arr)
    if data.ndim == 3 and data.shape[0] == 1:
        data = data[0]
    if data.ndim != 2 or not data.size:
        raise HTTPException(status_code=400, detail="A pixel mask is one 2-D image.")
    if data.size > MAX_PIXELS:
        raise HTTPException(status_code=400, detail="This pixel mask is too large.")
    if data.dtype.kind == "b":
        data = data.astype(MASK_DTYPE)
    if data.dtype.kind not in {"u", "i"}:
        raise HTTPException(status_code=400, detail="A pixel mask holds whole numbers.")
    if data.dtype.kind == "i" and data.size and int(data.min()) < 0:
        raise HTTPException(status_code=400, detail="A pixel mask has no negative values.")
    if data.size and int(data.max()) > np.iinfo(np.uint32).max:
        raise HTTPException(status_code=400, detail="A pixel mask's values fit in 32 bits.")
    return np.ascontiguousarray(data, dtype=MASK_DTYPE)


def from_bytes(payload: bytes, shape: Any) -> np.ndarray:
    """The mask the browser sends: raw little-endian uint32 in C order."""
    try:
        height, width = (int(value) for value in shape)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="Invalid mask shape") from exc
    if height <= 0 or width <= 0 or height * width > MAX_PIXELS:
        raise HTTPException(status_code=400, detail="Invalid mask shape")
    if len(payload) != height * width * MASK_DTYPE.itemsize:
        raise HTTPException(status_code=400, detail="The mask does not match its shape.")
    return np.frombuffer(payload, dtype=MASK_DTYPE).reshape((height, width)).copy()


def count_masked(arr: np.ndarray) -> int:
    return int(np.count_nonzero(arr))


# ---- files ---------------------------------------------------------------


def read_tiff_mask(path: Path) -> np.ndarray:
    image_formats._ensure_tifffile()
    try:
        arr = image_formats._tifffile.imread(path)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="This TIFF cannot be read.") from exc
    return as_mask(arr)


def encode_tiff(arr: np.ndarray) -> bytes:
    """A mask as a uint32 TIFF, deflate-compressed: masks are mostly zeros."""
    image_formats._ensure_tifffile()
    buffer = io.BytesIO()
    image_formats._tifffile.imwrite(buffer, as_mask(arr), compression="zlib", metadata=None)
    return buffer.getvalue()


def encode_hdf5(arr: np.ndarray, h5py: Any) -> bytes:
    """A mask as a small NeXus file, at the path ALBIS and DECTRIS files use."""
    buffer = io.BytesIO()
    with h5py.File(buffer, "w") as h5:
        entry = h5.create_group("entry")
        entry.attrs["NX_class"] = "NXentry"
        instrument = entry.create_group("instrument")
        instrument.attrs["NX_class"] = "NXinstrument"
        detector = instrument.create_group("detector")
        detector.attrs["NX_class"] = "NXdetector"
        detector.create_dataset(
            "pixel_mask", data=as_mask(arr), compression="gzip", compression_opts=4, shuffle=True
        )
    return buffer.getvalue()


# ---- detector ------------------------------------------------------------


def decode_darray(value: Any) -> np.ndarray:
    """A SIMPLON `__darray__`: base64 data with its type and shape."""
    if not isinstance(value, dict) or "__darray__" not in value:
        raise ValueError("not a SIMPLON array")
    raw = base64.b64decode(value.get("data") or "")
    shape = value.get("shape") or []
    arr = np.frombuffer(raw, dtype=np.dtype(str(value.get("type") or ""))).reshape(
        (int(shape[0]), int(shape[1]))
    )
    return as_mask(arr)


def encode_darray(arr: np.ndarray) -> dict[str, Any]:
    """The same format back, as SIMPLON takes an array in a write.

    The shape is given in the order `decode_darray` reads it, rows first; the
    route reads the mask back after writing, so a detector that took it the
    other way round is caught there rather than left with a scrambled mask.
    """
    mask = as_mask(arr)
    return {
        "__darray__": [1, 0, 0],
        "type": MASK_DTYPE.str,
        "shape": [int(mask.shape[0]), int(mask.shape[1])],
        "filters": ["base64"],
        "data": base64.b64encode(mask.tobytes()).decode("ascii"),
    }


def read_detector_mask(url: str, version: str) -> tuple[np.ndarray, str]:
    """The detector's mask and the key it answered under."""
    base = simplon_control._base(url, version, "detector")
    for key in DETECTOR_MASK_KEYS:
        try:
            payload = simplon_control._get(f"{base}/config/{key}", _READ_TIMEOUT_S)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                continue
            _raise_simplon_failure(exc, base, "Failed to read the pixel mask")
        except Exception as exc:
            _raise_simplon_failure(exc, base, "Failed to read the pixel mask")
        value = payload.get("value") if isinstance(payload, dict) else None
        try:
            return decode_darray(value), key
        except (ValueError, TypeError, IndexError, HTTPException) as exc:
            raise HTTPException(
                status_code=502, detail="The detector's pixel mask could not be read."
            ) from exc
    raise HTTPException(status_code=404, detail="The detector has no pixel mask.")


def write_detector_mask(url: str, version: str, key: str, arr: np.ndarray) -> None:
    if key not in DETECTOR_MASK_KEYS:
        raise HTTPException(status_code=400, detail="Not a pixel mask key.")
    base = simplon_control._base(url, version, "detector")
    try:
        simplon_control._put(
            f"{base}/config/{key}", {"value": encode_darray(arr)}, _WRITE_TIMEOUT_S
        )
    except Exception as exc:
        _raise_simplon_failure(exc, base, "The detector did not accept the pixel mask")


def backup_name(serial: str = "") -> str:
    """`detector_<serial>_<time>.tiff`, the serial reduced to safe characters."""
    safe = "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in serial.strip())[:40]
    stamp = time.strftime("%Y%m%d_%H%M%S")
    return f"detector_{safe}_{stamp}.tiff" if safe else f"detector_{stamp}.tiff"
