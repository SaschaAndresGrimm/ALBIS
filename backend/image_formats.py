from __future__ import annotations

"""Image format helpers for ALBIS backend.

This module centralizes detector image file handling that is shared by
multiple endpoints (file load, metadata/header extraction, monitor parsing).
"""

import math
import re
import struct
import sys
import io
from pathlib import Path
from typing import Any

import numpy as np
from fastapi import HTTPException


# Lazy-loaded optional dependencies
_tifffile = None
_fabio = None


_DECTRIS_TIFF_TAG = 0xC7F8
_TIFF_TYPE_SIZES = {
    1: 1,  # BYTE
    2: 1,  # ASCII
    3: 2,  # SHORT
    4: 4,  # LONG
    5: 8,  # RATIONAL
    7: 1,  # UNDEFINED
    11: 4,  # FLOAT
    12: 8,  # DOUBLE
}

_PILATUS_NUM_RE = re.compile(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?")


def _ensure_tifffile() -> None:
    global _tifffile
    if _tifffile is None:
        import tifffile as tifffile_module  # type: ignore[import-not-found]

        _tifffile = tifffile_module


def _ensure_fabio() -> None:
    global _fabio
    if _fabio is None:
        import fabio as fabio_module  # type: ignore[import-not-found]

        _fabio = fabio_module


def _decode_tiff_values(type_code: int, count: int, data: bytes, byteorder: str) -> Any:
    if count <= 0:
        return None
    fmt_prefix = "<" if byteorder == "little" else ">"
    if type_code == 2:
        try:
            return data.split(b"\x00", 1)[0].decode("ascii", errors="replace")
        except Exception:
            return ""
    if type_code in {1, 7}:
        if count == 1:
            return int(data[0]) if data else None
        return [int(b) for b in data[:count]]
    if type_code == 3:
        size = count * 2
        raw = data[:size].ljust(size, b"\x00")
        values = struct.unpack(f"{fmt_prefix}{count}H", raw)
        return values[0] if count == 1 else list(values)
    if type_code == 4:
        size = count * 4
        raw = data[:size].ljust(size, b"\x00")
        values = struct.unpack(f"{fmt_prefix}{count}I", raw)
        return values[0] if count == 1 else list(values)
    if type_code == 11:
        size = count * 4
        raw = data[:size].ljust(size, b"\x00")
        values = struct.unpack(f"{fmt_prefix}{count}f", raw)
        return values[0] if count == 1 else list(values)
    if type_code == 12:
        size = count * 8
        raw = data[:size].ljust(size, b"\x00")
        values = struct.unpack(f"{fmt_prefix}{count}d", raw)
        return values[0] if count == 1 else list(values)
    return None


def _parse_dectris_ifd(
    raw: bytes, byteorder: str, offset: int = 0, absolute_offsets: bool = False
) -> dict[int, Any]:
    if not raw:
        return {}
    bo = "little" if byteorder == "<" else "big"
    if len(raw) < offset + 2:
        return {}
    count = int.from_bytes(raw[offset : offset + 2], bo)
    entry_offset = offset + 2
    base = 0 if absolute_offsets else offset
    entries: dict[int, Any] = {}
    for _ in range(count):
        if entry_offset + 12 > len(raw):
            break
        tag = int.from_bytes(raw[entry_offset : entry_offset + 2], bo)
        type_code = int.from_bytes(raw[entry_offset + 2 : entry_offset + 4], bo)
        value_count = int.from_bytes(raw[entry_offset + 4 : entry_offset + 8], bo)
        value_offset = raw[entry_offset + 8 : entry_offset + 12]
        entry_offset += 12
        size = _TIFF_TYPE_SIZES.get(type_code)
        if not size:
            continue
        total = size * value_count
        if total <= 4:
            value_bytes = value_offset[:total]
        else:
            value_ptr = int.from_bytes(value_offset, bo)
            if not absolute_offsets:
                value_ptr = base + value_ptr
            if value_ptr + total > len(raw):
                continue
            value_bytes = raw[value_ptr : value_ptr + total]
        entries[tag] = _decode_tiff_values(type_code, value_count, value_bytes, bo)
    return entries


def _parse_dectris_tag_value(value: Any, byteorder: str) -> dict[int, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return {int(k): v for k, v in value.items()}
    if isinstance(value, np.ndarray):
        value = value.tobytes()
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        return _parse_dectris_ifd(bytes(value), byteorder)
    if isinstance(value, (list, tuple)):
        if value and all(isinstance(item, tuple) and len(item) == 2 for item in value):
            try:
                return {int(k): v for k, v in value}
            except Exception:
                return {}
    return {}


def _first_number(value: Any) -> float | None:
    if isinstance(value, np.ndarray):
        value = value.tolist()
    if isinstance(value, (list, tuple)):
        if not value:
            return None
        value = value[0]
    if isinstance(value, np.generic):
        value = value.item()
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    number = _first_number(value)
    if number is None:
        return None
    try:
        return int(number)
    except (TypeError, ValueError):
        return None


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            return value.decode("ascii", errors="replace").strip("\x00")
        except Exception:
            return None
    if isinstance(value, (list, tuple)):
        if not value:
            return None
        return _as_str(value[0])
    return str(value)


def _as_pair(value: Any) -> tuple[float, float] | None:
    if isinstance(value, np.ndarray):
        value = value.tolist()
    if isinstance(value, (list, tuple)) and len(value) >= 2:
        first = _first_number(value[0])
        second = _first_number(value[1])
        if first is None or second is None:
            return None
        return float(first), float(second)
    return None


def _distance_to_mm(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    if value <= 10:
        return value * 1000.0
    return value


def _simplon_meta_from_tiff(tiff: Any, raw: bytes | None = None) -> dict[str, Any]:
    meta: dict[str, Any] = {}
    try:
        page = tiff.pages[0]
    except Exception:
        return meta
    tag = page.tags.get(_DECTRIS_TIFF_TAG)
    if tag is None:
        return meta
    entries: dict[int, Any] = {}
    if isinstance(tag.value, int):
        if raw is None:
            try:
                raw = tiff.filehandle.read()
            except Exception:
                raw = None
        if raw:
            entries = _parse_dectris_ifd(raw, tiff.byteorder, tag.value, absolute_offsets=True)
    if not entries:
        entries = _parse_dectris_tag_value(tag.value, tiff.byteorder)
    if not entries:
        return meta
    series_number = _as_int(entries.get(0x0002))
    image_number = _as_int(entries.get(0x0003))
    image_datetime = _as_str(entries.get(0x0004))
    threshold_energy = _first_number(entries.get(0x0006))
    incident_energy = _first_number(entries.get(0x0009))
    incident_wavelength = _first_number(entries.get(0x000A))
    beam_center = _as_pair(entries.get(0x0016))
    detector_distance = _first_number(entries.get(0x0017))
    energy_ev = None
    if incident_energy is not None and math.isfinite(incident_energy):
        energy_ev = float(incident_energy)
    elif incident_wavelength is not None and incident_wavelength > 0:
        energy_ev = 12398.4193 / float(incident_wavelength)
    meta.update(
        {
            "series_number": series_number,
            "image_number": image_number,
            "image_datetime": image_datetime,
            "threshold_energy_ev": threshold_energy,
            "energy_ev": energy_ev,
            "wavelength_a": incident_wavelength,
            "distance_mm": _distance_to_mm(detector_distance),
            "beam_center_px": beam_center,
        }
    )
    return meta


def _parse_unit_value(text: str) -> tuple[float | None, str]:
    if not text:
        return None, ""
    match = _PILATUS_NUM_RE.search(text)
    if not match:
        return None, ""
    value = float(match.group(0))
    unit_match = re.search(rf"{re.escape(match.group(0))}\s*([A-Za-zµÅ]+)", text)
    unit = unit_match.group(1) if unit_match else ""
    return value, unit


def _convert_length(value: float | None, unit: str, target: str) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    unit_l = unit.lower()
    if target == "um":
        if unit_l in {"m", "meter", "metre"}:
            return value * 1e6
        if unit_l == "mm":
            return value * 1e3
        if unit_l in {"um", "µm"}:
            return value
        if unit_l == "nm":
            return value * 1e-3
        return value
    if target == "mm":
        if unit_l in {"m", "meter", "metre"}:
            return value * 1e3
        if unit_l == "cm":
            return value * 10.0
        if unit_l == "mm":
            return value
        if unit_l in {"um", "µm"}:
            return value * 1e-3
        return value
    if target == "a":
        if unit_l in {"a", "å", "angstrom", "ang"}:
            return value
        if unit_l == "nm":
            return value * 10.0
        if unit_l in {"m", "meter", "metre"}:
            return value * 1e10
        if unit_l == "mm":
            return value * 1e7
        return value
    return value


def _parse_pilatus_header_text(text: str) -> dict[str, Any]:
    meta: dict[str, Any] = {}
    if not text:
        return meta
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            line = line[1:].strip()
        lower = line.lower()
        if "pixel_size" in lower and "pixel_size_um" not in meta:
            value, unit = _parse_unit_value(line)
            pixel_um = _convert_length(value, unit, "um")
            if pixel_um is not None:
                meta["pixel_size_um"] = float(pixel_um)
            continue
        if "beam_xy" in lower or "beam center" in lower or "beam_center" in lower:
            nums = _PILATUS_NUM_RE.findall(line)
            if len(nums) >= 2:
                meta["beam_center_px"] = (float(nums[0]), float(nums[1]))
            continue
        if "detector_distance" in lower or "detector distance" in lower:
            value, unit = _parse_unit_value(line)
            distance_mm = _convert_length(value, unit, "mm")
            if distance_mm is not None:
                meta["distance_mm"] = float(distance_mm)
            continue
        if "wavelength" in lower and "wavelength_a" not in meta:
            value, unit = _parse_unit_value(line)
            wavelength_a = _convert_length(value, unit, "a")
            if wavelength_a is not None:
                meta["wavelength_a"] = float(wavelength_a)
            continue
        if "energy" in lower and "threshold" not in lower and "energy_ev" not in meta:
            value, unit = _parse_unit_value(line)
            if value is None:
                continue
            unit_l = unit.lower()
            if unit_l in {"kev"}:
                meta["energy_ev"] = float(value * 1000.0)
            elif unit_l in {"ev"}:
                meta["energy_ev"] = float(value)
            continue
    if "energy_ev" not in meta:
        wavelength = meta.get("wavelength_a")
        if wavelength and wavelength > 0:
            meta["energy_ev"] = float(12398.4193 / wavelength)
    return meta


def _open_fabio_image(path: Path):
    _ensure_fabio()
    try:
        return _fabio.open(str(path))
    except Exception:
        if path.suffix.lower() != ".gz":
            return None
        try:
            import gzip
            import tempfile

            with gzip.open(path, "rb") as gz:
                data = gz.read()
            with tempfile.NamedTemporaryFile(suffix=".cbf", delete=False) as tmp:
                tmp.write(data)
                tmp_path = Path(tmp.name)
            try:
                return _fabio.open(str(tmp_path))
            finally:
                try:
                    tmp_path.unlink(missing_ok=True)
                except Exception:
                    pass
        except Exception:
            return None


def _pilatus_meta_from_fabio(path: Path) -> dict[str, Any]:
    image = _open_fabio_image(path)
    if image is None:
        return {}
    header = getattr(image, "header", {}) or {}
    text = header.get("_array_data.header_contents") if isinstance(header, dict) else ""
    if isinstance(text, bytes):
        text = text.decode("utf-8", errors="ignore")
    if not text:
        try:
            text = "\n".join(f"{k} {v}" for k, v in header.items())
        except Exception:
            text = ""
    meta = _parse_pilatus_header_text(text)
    return meta


def _pilatus_meta_from_tiff(path: Path) -> dict[str, Any]:
    _ensure_tifffile()
    try:
        with _tifffile.TiffFile(path) as tiff:
            desc = ""
            try:
                desc = tiff.pages[0].description or ""
            except Exception:
                desc = ""
    except Exception:
        desc = ""
    meta = _parse_pilatus_header_text(desc)
    if meta:
        return meta
    try:
        return _pilatus_meta_from_fabio(path)
    except Exception:
        return {}


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
    image = _open_fabio_image(path)
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


def _read_tiff(path: Path, index: int = 0) -> np.ndarray:
    _ensure_tifffile()
    arr = _tifffile.imread(path)
    return _normalize_image_array(np.asarray(arr), index=index)


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


def _write_tiff(path: Path, arr: np.ndarray) -> None:
    _ensure_tifffile()
    _tifffile.imwrite(path, arr, photometric="minisblack")


def _read_cbf(path: Path) -> np.ndarray:
    _ensure_fabio()
    image = _fabio.open(str(path))
    arr = np.asarray(image.data)
    return _normalize_image_array(arr)


def _read_cbf_gz(path: Path) -> np.ndarray:
    _ensure_fabio()
    try:
        image = _fabio.open(str(path))
        arr = np.asarray(image.data)
        return _normalize_image_array(arr)
    except Exception:
        import gzip
        import tempfile

        with gzip.open(path, "rb") as gz:
            data = gz.read()
        with tempfile.NamedTemporaryFile(suffix=".cbf", delete=False) as tmp:
            tmp.write(data)
            tmp_path = Path(tmp.name)
        try:
            image = _fabio.open(str(tmp_path))
            arr = np.asarray(image.data)
            return _normalize_image_array(arr)
        finally:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass


def _read_edf(path: Path) -> np.ndarray:
    _ensure_fabio()
    image = _fabio.open(str(path))
    arr = np.asarray(image.data)
    return _normalize_image_array(arr)
