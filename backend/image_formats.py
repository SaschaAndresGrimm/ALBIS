"""Image format helpers for ALBIS backend.

This module centralizes detector image file handling that is shared by
multiple endpoints (file load, metadata/header extraction, monitor parsing).
"""

from __future__ import annotations

import contextlib
import io
import json
import math
import re
import struct
import sys
from pathlib import Path
from typing import Any

import numpy as np
from fastapi import HTTPException

# Lazy-loaded optional dependencies
_tifffile = None
_fabio_cbf_image_cls = None
_fabio_edf_image_cls = None
_fabio_tif_image_cls = None


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
    if isinstance(value, bytes | bytearray):
        return _parse_dectris_ifd(bytes(value), byteorder)
    if (
        isinstance(value, list | tuple)
        and value
        and all(isinstance(item, tuple) and len(item) == 2 for item in value)
    ):
        try:
            return {int(k): v for k, v in value}
        except Exception:
            return {}
    return {}


def _first_number(value: Any) -> float | None:
    if isinstance(value, np.ndarray):
        value = value.tolist()
    if isinstance(value, list | tuple):
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
    if isinstance(value, list | tuple):
        if not value:
            return None
        return _as_str(value[0])
    return str(value)


def _as_pair(value: Any) -> tuple[float, float] | None:
    if isinstance(value, np.ndarray):
        value = value.tolist()
    if isinstance(value, list | tuple) and len(value) >= 2:
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


def _pilatus_is_12m_header_text(text: str) -> bool:
    upper = str(text or "").upper()
    return "PILATUS 12M" in upper and "S/N 120-0100" in upper


def _coerce_float_vector(value: Any, size: int) -> list[float] | None:
    if not isinstance(value, list | tuple) or len(value) != size:
        return None
    out: list[float] = []
    for item in value:
        number = _first_number(item)
        if number is None or not math.isfinite(number):
            return None
        out.append(float(number))
    return out


def _coerce_int_vector(value: Any, size: int) -> list[int] | None:
    if not isinstance(value, list | tuple) or len(value) != size:
        return None
    out: list[int] = []
    for item in value:
        number = _as_int(item)
        if number is None:
            return None
        out.append(int(number))
    return out


def _normalize_geometry_panel(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    name = str(payload.get("name") or "").strip()
    origin = _coerce_float_vector(payload.get("origin"), 3)
    fast_axis = _coerce_float_vector(payload.get("fast_axis"), 3)
    slow_axis = _coerce_float_vector(payload.get("slow_axis"), 3)
    pixel_size = _coerce_float_vector(payload.get("pixel_size"), 2)
    image_size = _coerce_int_vector(payload.get("image_size"), 2)
    raw_offset = _coerce_int_vector(payload.get("raw_image_offset"), 2)
    if not (
        name and origin and fast_axis and slow_axis and pixel_size and image_size and raw_offset
    ):
        return None
    if any(v <= 0 for v in pixel_size) or any(v <= 0 for v in image_size):
        return None
    return {
        "name": name,
        "origin_mm": origin,
        "fast_axis": fast_axis,
        "slow_axis": slow_axis,
        "pixel_size_mm": pixel_size,
        "image_size_px": image_size,
        "raw_offset_px": raw_offset,
    }


def _load_dials_expt_geometry(path: Path) -> list[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return []
    detectors = payload.get("detector")
    if not isinstance(detectors, list) or not detectors:
        return []
    detector = detectors[0]
    if not isinstance(detector, dict):
        return []
    panels = detector.get("panels")
    if not isinstance(panels, list):
        return []
    normalized = []
    for panel in panels:
        item = _normalize_geometry_panel(panel)
        if item is not None:
            normalized.append(item)
    return normalized


def _resolve_pilatus_12m_geometry_file(image_path: Path) -> Path | None:
    candidates = [
        image_path.parent / "imported.expt",
        image_path.parent / "P12M_geometry" / "imported.expt",
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


def _read_hdf5_embedded_geometry(path: Path) -> dict[str, Any] | None:
    try:
        import h5py  # type: ignore[import-not-found]
    except Exception:
        return None
    try:
        with h5py.File(path, "r") as h5:
            if "/entry/albis/geometry/json" not in h5:
                return None
            dataset = h5["/entry/albis/geometry/json"]
            try:
                raw = dataset[()]
            except Exception:
                return None
    except Exception:
        return None
    if isinstance(raw, np.ndarray):
        if raw.size != 1:
            return None
        raw = raw.reshape(-1)[0]
    if isinstance(raw, bytes):
        text = raw.decode("utf-8", errors="ignore")
    elif isinstance(raw, np.bytes_):
        text = raw.tobytes().decode("utf-8", errors="ignore")
    elif hasattr(raw, "item"):
        item = raw.item()
        if isinstance(item, bytes):
            text = item.decode("utf-8", errors="ignore")
        else:
            text = str(item)
    else:
        text = str(raw)
    try:
        payload = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    panels = []
    for panel in payload.get("panels", []):
        if not isinstance(panel, dict):
            return None
        item = {
            "name": str(panel.get("name") or "").strip(),
            "origin_mm": _coerce_float_vector(panel.get("origin_mm"), 3),
            "fast_axis": _coerce_float_vector(panel.get("fast_axis"), 3),
            "slow_axis": _coerce_float_vector(panel.get("slow_axis"), 3),
            "pixel_size_mm": _coerce_float_vector(panel.get("pixel_size_mm"), 2),
            "image_size_px": _coerce_int_vector(panel.get("image_size_px"), 2),
            "raw_offset_px": _coerce_float_vector(panel.get("raw_offset_px"), 2),
        }
        if not (
            item["name"]
            and item["origin_mm"]
            and item["fast_axis"]
            and item["slow_axis"]
            and item["pixel_size_mm"]
            and item["image_size_px"]
            and item["raw_offset_px"]
        ):
            return None
        panels.append(item)
    if not panels:
        return None
    source = str(payload.get("source") or "").strip()
    if source.startswith("embedded HDF5 geometry"):
        source_label = source
    elif source:
        source_label = f"embedded HDF5 geometry (from {Path(source).name})"
    else:
        source_label = "embedded HDF5 geometry"
    return {
        "mode": "geometry",
        "detector": str(payload.get("detector") or ""),
        "source": source_label,
        "panels": panels,
    }


def _resolve_hdf5_source_file(path: Path) -> Path | None:
    try:
        import h5py  # type: ignore[import-not-found]
    except Exception:
        return None
    try:
        with h5py.File(path, "r") as h5:
            raw = h5.attrs.get("source_file")
    except Exception:
        return None
    if isinstance(raw, bytes):
        text = raw.decode("utf-8", errors="ignore").strip()
    elif hasattr(raw, "item"):
        item = raw.item()
        if isinstance(item, bytes):
            text = item.decode("utf-8", errors="ignore").strip()
        else:
            text = str(item).strip()
    else:
        text = str(raw or "").strip()
    if not text:
        return None
    source_path = Path(text).expanduser()
    if not source_path.is_absolute():
        source_path = (path.parent / source_path).resolve()
    else:
        source_path = source_path.resolve()
    return source_path


def _pilatus_image_geometry_internal(
    path: Path,
    geometry_path: Path | None,
    visited: set[Path],
) -> dict[str, Any]:
    geometry = {"mode": "planar", "detector": "", "source": "", "panels": []}
    if geometry_path is not None:
        panels = _load_dials_expt_geometry(geometry_path)
        if not panels:
            return geometry
        return {
            "mode": "geometry",
            "detector": "pilatus-12m-dls-cshape",
            "source": str(geometry_path),
            "panels": panels,
        }
    try:
        resolved = path.resolve()
    except OSError:
        resolved = path
    if resolved in visited:
        return geometry
    visited.add(resolved)
    ext = _image_ext_name(path.name)
    if ext in {".h5", ".hdf5"}:
        embedded = _read_hdf5_embedded_geometry(path)
        if embedded:
            return embedded
        source_path = _resolve_hdf5_source_file(path)
        if source_path and source_path.exists():
            return _pilatus_image_geometry_internal(source_path, None, visited)
        return geometry
    if ext not in {".cbf", ".cbf.gz"}:
        return geometry
    header_text = _pilatus_header_text(path)
    if not _pilatus_is_12m_header_text(header_text):
        return geometry
    geometry_path = _resolve_pilatus_12m_geometry_file(path)
    if geometry_path is None:
        return geometry
    panels = _load_dials_expt_geometry(geometry_path)
    if not panels:
        return geometry
    return {
        "mode": "geometry",
        "detector": "pilatus-12m-dls-cshape",
        "source": str(geometry_path),
        "panels": panels,
    }


def _pilatus_image_geometry(path: Path, geometry_path: Path | None = None) -> dict[str, Any]:
    return _pilatus_image_geometry_internal(path, geometry_path, set())


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
    image = _open_fabio_cbf_image(path)
    arr = np.asarray(image.data)
    return _normalize_image_array(arr)


def _read_cbf_gz(path: Path) -> np.ndarray:
    image = _open_fabio_cbf_image(path)
    arr = np.asarray(image.data)
    return _normalize_image_array(arr)


def _read_edf(path: Path) -> np.ndarray:
    image = _open_fabio_edf_image(path)
    arr = np.asarray(image.data)
    return _normalize_image_array(arr)
