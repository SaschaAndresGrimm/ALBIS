"""Image format helpers for ALBIS backend.

This module centralizes detector image file handling that is shared by
multiple endpoints (file load, metadata/header extraction, monitor parsing).
"""

from __future__ import annotations

import contextlib
import io
import json
import logging
import math
import re
import struct
import sys
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any, BinaryIO

import numpy as np
from fastapi import HTTPException

try:
    from .build_info import ALBIS_COMMIT
    from .version import ALBIS_VERSION
except ImportError:  # pragma: no cover - supports `python backend/app.py`
    from build_info import ALBIS_COMMIT  # type: ignore[no-redef]
    from version import ALBIS_VERSION  # type: ignore[no-redef]

# Lazy-loaded optional dependencies
_tifffile = None
_fabio_cbf_image_cls = None
_fabio_edf_image_cls = None
_fabio_tif_image_cls = None


_DECTRIS_TIFF_TAG = 0xC7F8
# tifffile 2026.x decodes tag 51192 itself and hands back a dict keyed by these
# names rather than by the codes the rest of this module indexes on.
_DECTRIS_TAG_CODES = {
    "IfdVersion": 0x0000,
    "SeriesUniqueId": 0x0001,
    "SeriesNumber": 0x0002,
    "ImageNumber": 0x0003,
    "ImageDateTime": 0x0004,
    "ThresholdId": 0x0005,
    "ThresholdEnergy": 0x0006,
    "ExposureTime": 0x0007,
    "IncidentEnergy": 0x0009,
    "IncidentWavelength": 0x000A,
    "LostPixelCount": 0x0012,
    "BeamCenter": 0x0016,
    "DetectorDistance": 0x0017,
}
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


def _dectris_offsets_are_absolute(raw: bytes, base: int, length: int, bo: str) -> bool:
    """Say which base the value offsets in the IFD at `base` are measured from.

    An embedded IFD states file-absolute offsets, the same way EXIF does. ALBIS
    used to write them relative to the start of the tag payload instead, so
    files carrying the older layout are already out there and still have to
    read. For any real file the two readings put the pointers in ranges that do
    not overlap -- a relative pointer is smaller than the payload, an absolute
    one is past the header and the first IFD -- so scoring both and taking the
    one whose pointers land inside the payload tells them apart with no version
    stamp to go on.
    """
    if base + 2 > len(raw):
        return True
    end = base + length
    count = int.from_bytes(raw[base : base + 2], bo)
    data_start = base + 2 + count * 12 + 4
    absolute = relative = 0
    for index in range(count):
        entry = base + 2 + index * 12
        if entry + 12 > len(raw):
            break
        type_code = int.from_bytes(raw[entry + 2 : entry + 4], bo)
        value_count = int.from_bytes(raw[entry + 4 : entry + 8], bo)
        size = _TIFF_TYPE_SIZES.get(type_code)
        if not size:
            continue
        total = size * value_count
        if total <= 4:
            continue
        pointer = int.from_bytes(raw[entry + 8 : entry + 12], bo)
        if data_start <= pointer and pointer + total <= end:
            absolute += 1
        if data_start <= base + pointer and base + pointer + total <= end:
            relative += 1
    return absolute >= relative


def _find_tiff_tag_value_offset(handle: BinaryIO, tag_id: int) -> int:
    """Where the first IFD put the value of `tag_id`, read from an open file.

    Takes a handle rather than bytes, and reads only the header and the IFD
    table: the file this has to find a tag in is an exported frame, and most of
    it is pixel data there is no reason to pull into memory.

    Only classic little-endian TIFF, which is all `_write_tiff` produces.
    """
    handle.seek(0)
    header = handle.read(8)
    if len(header) < 8 or header[:4] != b"II\x2a\x00":
        return 0
    handle.seek(int.from_bytes(header[4:8], "little"))
    count = handle.read(2)
    if len(count) < 2:
        return 0
    entries = handle.read(int.from_bytes(count, "little") * 12)
    for entry in range(0, len(entries) - 11, 12):
        if int.from_bytes(entries[entry : entry + 2], "little") == tag_id:
            return int.from_bytes(entries[entry + 8 : entry + 12], "little")
    return 0


def _rebase_dectris_ifd(payload: bytes, base: int) -> bytes:
    """Point the value offsets in a packed DECTRIS IFD at where the payload landed.

    `_pack_dectris_ifd` cannot know its own file position -- tifffile picks that
    when it writes the tag -- so it packs offsets from the payload start and
    they are rebased here, once the file exists. Only the four-byte offset
    fields change, so the payload keeps its length and can be written back over
    the bytes already on disk. A negative base undoes a rebase, which is how the
    tests reproduce the layout earlier versions wrote.
    """
    if not payload or base == 0:
        return payload
    out = bytearray(payload)
    count = int.from_bytes(out[0:2], "little")
    for index in range(count):
        entry = 2 + index * 12
        if entry + 12 > len(out):
            break
        type_code = int.from_bytes(out[entry + 2 : entry + 4], "little")
        value_count = int.from_bytes(out[entry + 4 : entry + 8], "little")
        size = _TIFF_TYPE_SIZES.get(type_code)
        if not size or size * value_count <= 4:
            continue
        field = entry + 8
        pointer = int.from_bytes(out[field : field + 4], "little")
        out[field : field + 4] = struct.pack("<I", pointer + base)
    return bytes(out)


def _parse_dectris_tag_value(value: Any, byteorder: str) -> dict[int, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        entries: dict[int, Any] = {}
        for key, item in value.items():
            code = _DECTRIS_TAG_CODES.get(key) if isinstance(key, str) else None
            if code is None:
                try:
                    code = int(key)
                except (TypeError, ValueError):
                    continue
            entries[code] = item
        return entries
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


def _as_number_list(value: Any) -> list[float]:
    if value is None:
        return []
    if isinstance(value, np.ndarray):
        value = value.reshape(-1).tolist()
    elif not isinstance(value, list | tuple):
        value = [value]
    out: list[float] = []
    for item in value:
        number = _first_number(item)
        if number is not None and math.isfinite(number):
            out.append(float(number))
    return out


def _as_int_list(value: Any) -> list[int]:
    out: list[int] = []
    for number in _as_number_list(value):
        out.append(int(round(number)))
    return out


def _export_float(metadata: dict[str, Any] | None, key: str) -> float | None:
    if not metadata:
        return None
    number = _first_number(metadata.get(key))
    if number is None or not math.isfinite(number):
        return None
    return float(number)


def _export_int(metadata: dict[str, Any] | None, key: str) -> int | None:
    number = _export_float(metadata, key)
    if number is None:
        return None
    return int(round(number))


def _export_text(metadata: dict[str, Any] | None, key: str) -> str:
    if not metadata:
        return ""
    text = _as_str(metadata.get(key)) or ""
    return " ".join(text.replace("\r", " ").replace("\n", " ").split())


def _pack_dectris_ifd_value(type_code: int, value: Any) -> tuple[bytes, int] | None:
    if type_code == 2:
        text = str(value or "").encode("ascii", errors="replace")
        if not text.endswith(b"\x00"):
            text += b"\x00"
        return text, len(text)
    if type_code == 3:
        values = [v for v in _as_int_list(value) if 0 <= v <= 0xFFFF]
        if not values:
            return None
        return struct.pack(f"<{len(values)}H", *values), len(values)
    if type_code == 4:
        values = [v for v in _as_int_list(value) if 0 <= v <= 0xFFFFFFFF]
        if not values:
            return None
        return struct.pack(f"<{len(values)}I", *values), len(values)
    if type_code == 12:
        values = _as_number_list(value)
        if not values:
            return None
        return struct.pack(f"<{len(values)}d", *values), len(values)
    return None


def _pack_dectris_ifd(entries: list[tuple[int, int, Any]]) -> bytes:
    normalized: list[tuple[int, int, bytes, int]] = []
    for tag, type_code, value in sorted(entries, key=lambda item: item[0]):
        packed = _pack_dectris_ifd_value(type_code, value)
        if packed is None:
            continue
        value_bytes, count = packed
        normalized.append((tag, type_code, value_bytes, count))
    if not normalized:
        return b""

    data_start = 2 + len(normalized) * 12 + 4
    entry_bytes = bytearray()
    data_bytes = bytearray()
    for tag, type_code, value_bytes, count in normalized:
        if len(value_bytes) <= 4:
            value_field = value_bytes.ljust(4, b"\x00")
        else:
            if (data_start + len(data_bytes)) % 2:
                data_bytes.append(0)
            value_offset = data_start + len(data_bytes)
            value_field = struct.pack("<I", value_offset)
            data_bytes.extend(value_bytes)
        entry_bytes.extend(
            struct.pack("<HHI", int(tag), int(type_code), int(count)) + value_field
        )
    return struct.pack("<H", len(normalized)) + bytes(entry_bytes) + b"\x00\x00\x00\x00" + bytes(data_bytes)


def _dectris_tiff_payload(metadata: dict[str, Any] | None) -> bytes:
    if not metadata:
        return b""
    entries: list[tuple[int, int, Any]] = []
    series_unique_id = _export_text(metadata, "series_unique_id")
    if series_unique_id:
        entries.append((0x0001, 2, series_unique_id))
    series_number = _export_int(metadata, "series_number")
    if series_number is not None:
        entries.append((0x0002, 4, series_number))
    image_number = _export_int(metadata, "image_number")
    if image_number is not None:
        entries.append((0x0003, 4, image_number))
    image_datetime = _export_text(metadata, "image_datetime")
    if image_datetime:
        entries.append((0x0004, 2, image_datetime))
    threshold_ids = _as_int_list(metadata.get("threshold_ids"))
    if threshold_ids:
        entries.append((0x0005, 3, threshold_ids))
    threshold_energies = _as_number_list(metadata.get("threshold_energies_ev"))
    if threshold_energies:
        entries.append((0x0006, 12, threshold_energies))
    exposure_time = _export_float(metadata, "exposure_time_s")
    if exposure_time is not None:
        entries.append((0x0007, 12, exposure_time))
    incident_energy = _export_float(metadata, "incident_energy_ev")
    if incident_energy is not None:
        entries.append((0x0009, 12, incident_energy))
    wavelength = _export_float(metadata, "wavelength_a")
    if wavelength is not None:
        entries.append((0x000A, 12, wavelength))
    lost_pixels = _export_int(metadata, "lost_pixel_count")
    if lost_pixels is not None:
        entries.append((0x0012, 4, lost_pixels))
    beam_x = _export_float(metadata, "beam_center_x_px")
    beam_y = _export_float(metadata, "beam_center_y_px")
    if beam_x is not None and beam_y is not None:
        entries.append((0x0016, 12, [beam_x, beam_y]))
    detector_distance = _export_float(metadata, "detector_distance_m")
    if detector_distance is not None:
        entries.append((0x0017, 12, detector_distance))
    if not entries:
        return b""
    return _pack_dectris_ifd([(0x0000, 4, 0), *entries])


def _format_header_number(value: Any) -> str | None:
    number = _first_number(value)
    if number is None or not math.isfinite(number):
        return None
    return f"{float(number):.12g}"


def producer_string() -> str:
    """Name this build, the way a bug report or a citation needs it named."""
    return f"ALBIS {ALBIS_VERSION}" + (f" ({ALBIS_COMMIT})" if ALBIS_COMMIT else "")


def _provenance_lines(metadata: dict[str, Any] | None) -> list[str]:
    """Say who made this file, from what, and what was done to the pixels.

    An exported frame is not a copy of the detector's output. The dtype is
    widened, masked gaps become `-1` and bad or saturated pixels become `-2`.
    Those are the right conventions, but a mini-CBF header that declares
    `SLS_1.0` and lists detector, wavelength and distance -- and says nothing
    about ALBIS -- reads to XDS or DIALS as genuine detector output. For
    software that asks to be cited when it contributed to a result, derived data
    that cannot be traced back is the wrong default.
    """
    lines = [f"# Produced by {producer_string()} -- derived data, not raw detector output"]

    source = _export_text(metadata or {}, "source_name")
    if source:
        parts = [f"# Source: {source}"]
        dataset = _export_text(metadata or {}, "source_dataset")
        if dataset:
            parts.append(dataset)
        frame = _export_int(metadata or {}, "source_frame")
        if frame is not None:
            count = _export_int(metadata or {}, "source_frame_count")
            parts.append(f"frame {frame}" + (f"/{count}" if count else ""))
        threshold = _export_int(metadata or {}, "source_threshold")
        if threshold is not None:
            count = _export_int(metadata or {}, "source_threshold_count")
            parts.append(f"threshold {threshold}" + (f"/{count}" if count else ""))
        lines.append(" ".join(parts))

    lines.append("# Pixel substitutions: masked gaps = -1, bad or saturated = -2")
    return lines


def _mini_cbf_header_text(metadata: dict[str, Any] | None) -> str:
    lines: list[str] = []
    if not metadata:
        return "\n".join(_provenance_lines(metadata))
    detector = _export_text(metadata, "detector_description")
    serial = _export_text(metadata, "detector_serial_number")
    if detector or serial:
        line = f"# Detector: {detector or 'unknown'}"
        if serial:
            line += f", S/N {serial}"
        lines.append(line)

    pixel_x = _format_header_number(metadata.get("pixel_size_x_m"))
    pixel_y = _format_header_number(metadata.get("pixel_size_y_m"))
    if pixel_x and pixel_y:
        lines.append(f"# Pixel_size {pixel_x} m x {pixel_y} m")
    thickness = _format_header_number(metadata.get("sensor_thickness_m"))
    if thickness:
        lines.append(f"# Silicon sensor, thickness {thickness} m")
    exposure = _format_header_number(metadata.get("exposure_time_s"))
    if exposure:
        lines.append(f"# Exposure_time {exposure} s")
    period = _format_header_number(metadata.get("exposure_period_s"))
    if period:
        lines.append(f"# Exposure_period {period} s")
    cutoff = _export_int(metadata, "count_cutoff")
    if cutoff is not None:
        lines.append(f"# Count_cutoff {cutoff} counts")
    wavelength = _format_header_number(metadata.get("wavelength_a"))
    if wavelength:
        lines.append(f"# Wavelength {wavelength} A")
    energy = _format_header_number(metadata.get("incident_energy_ev"))
    if energy:
        lines.append(f"# Incident_energy {energy} eV")
    distance = _format_header_number(metadata.get("detector_distance_m"))
    if distance:
        lines.append(f"# Detector_distance {distance} m")
    beam_x = _format_header_number(metadata.get("beam_center_x_px"))
    beam_y = _format_header_number(metadata.get("beam_center_y_px"))
    if beam_x and beam_y:
        lines.append(f"# Beam_xy ({beam_x}, {beam_y}) pixels")
    start = _format_header_number(metadata.get("start_angle_deg"))
    if start:
        lines.append(f"# Start_angle {start} deg.")
    increment = _format_header_number(metadata.get("angle_increment_deg"))
    if increment:
        lines.append(f"# Angle_increment {increment} deg.")
    provenance = _provenance_lines(metadata)
    if not lines:
        # Nothing was parsed from the source, so keep the source's own header
        # rather than replacing it with silence. Provenance is appended to
        # whichever of the two bases we ended up with.
        #
        # Re-exporting a file ALBIS wrote carries that file's provenance here,
        # which is worth keeping -- it is the chain back to the original -- but
        # not worth repeating verbatim, so lines the new block already says are
        # dropped rather than stacked up on every round trip.
        source_header = str(metadata.get("source_header_text") or "").strip()
        carried = source_header.splitlines() if source_header else []
        lines = [line for line in carried if line.strip() not in {p.strip() for p in provenance}]
    return "\n".join(lines + provenance)


def _distance_to_mm(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    if value <= 10:
        return value * 1000.0
    return value


def _read_tiff_prefix(tiff: Any, size: int | None = None) -> bytes | None:
    """The first `size` bytes of the open file, leaving its position alone."""
    try:
        handle = tiff.filehandle
        position = handle.tell()
        handle.seek(0)
        data = handle.read() if size is None else handle.read(size)
        handle.seek(position)
        return data
    except Exception:
        return None


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
        # A detector-written file points at its IFD with a LONG and the offsets
        # inside are file-absolute, so they can reach anywhere and the whole
        # file has to be in hand.
        window = raw if raw is not None else _read_tiff_prefix(tiff)
        if window:
            entries = _parse_dectris_ifd(window, tiff.byteorder, tag.value, absolute_offsets=True)
    else:
        # The IFD is embedded in the tag. Read it back out of the file rather
        # than trusting tag.value: tifffile 2026.x decodes the tag itself and
        # assumes file-absolute offsets, which silently mangles every
        # out-of-line value in a file written with the older layout.
        base = int(getattr(tag, "valueoffset", 0) or 0)
        length = int(getattr(tag, "count", 0) or 0)
        if base > 0 and length > 0:
            # Every pointer in an embedded IFD lands inside the payload under
            # either layout, so stop short of the pixel data.
            window = raw if raw is not None else _read_tiff_prefix(tiff, base + length)
            if window:
                bo = "little" if tiff.byteorder == "<" else "big"
                entries = _parse_dectris_ifd(
                    window,
                    tiff.byteorder,
                    base,
                    absolute_offsets=_dectris_offsets_are_absolute(window, base, length, bo),
                )
    if not entries:
        entries = _parse_dectris_tag_value(tag.value, tiff.byteorder)
    if not entries:
        return meta
    series_unique_id = _as_str(entries.get(0x0001))
    series_number = _as_int(entries.get(0x0002))
    image_number = _as_int(entries.get(0x0003))
    image_datetime = _as_str(entries.get(0x0004))
    threshold_ids = _as_int_list(entries.get(0x0005))
    threshold_energy = _first_number(entries.get(0x0006))
    threshold_energies = _as_number_list(entries.get(0x0006))
    exposure_time = _first_number(entries.get(0x0007))
    incident_energy = _first_number(entries.get(0x0009))
    incident_wavelength = _first_number(entries.get(0x000A))
    lost_pixel_count = _as_int(entries.get(0x0012))
    beam_center = _as_pair(entries.get(0x0016))
    detector_distance = _first_number(entries.get(0x0017))
    energy_ev = None
    if incident_energy is not None and math.isfinite(incident_energy):
        energy_ev = float(incident_energy)
    elif incident_wavelength is not None and incident_wavelength > 0:
        energy_ev = 12398.4193 / float(incident_wavelength)
    meta.update(
        {
            "series_unique_id": series_unique_id,
            "series_number": series_number,
            "image_number": image_number,
            "image_datetime": image_datetime,
            "threshold_ids": threshold_ids,
            "threshold_energy_ev": threshold_energy,
            "threshold_energies_ev": threshold_energies,
            "exposure_time_s": exposure_time,
            "energy_ev": energy_ev,
            "wavelength_a": incident_wavelength,
            "lost_pixel_count": lost_pixel_count,
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
    meta = _parse_pilatus_header_text(desc)
    if meta:
        return meta
    if simplon_meta:
        return simplon_meta
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
        text = item.decode("utf-8", errors="ignore") if isinstance(item, bytes) else str(item)
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


def _absolutize_dectris_offsets(path: Path, payload: bytes) -> None:
    """Restate the DECTRIS tag's value offsets now that its file position is known.

    Rewrites the payload in place, over the bytes tifffile just wrote. Left
    undone, every value too large to sit inline in a tag entry decodes to
    whatever happens to be at that offset from the start of the file.
    """
    try:
        size = path.stat().st_size
        with path.open("r+b") as handle:
            base = _find_tiff_tag_value_offset(handle, _DECTRIS_TIFF_TAG)
            if base <= 0 or base + len(payload) > size:
                return
            rebased = _rebase_dectris_ifd(payload, base)
            if rebased == payload:
                return
            handle.seek(base)
            handle.write(rebased)
    except OSError:
        return


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
    _tifffile.imwrite(
        path,
        arr,
        photometric="minisblack",
        byteorder="<",
        software=producer_string(),
        description="\n".join(_provenance_lines(metadata)),
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


# ---------------------------------------------------------------------------
# MYTHEN(2) strip-detector acquisitions
#
# A MYTHEN acquisition is a folder holding one XML ``.cfg`` descriptor plus one
# ``FrameNNNN.dat`` file per exposure. Each ``.dat`` is ASCII "<channel> <count>"
# pairs describing a single 1D strip readout. We assemble the whole acquisition
# into a 2D array of shape (frames, channels) so it renders through the standard
# image pipeline as an intensity map (x = channel, y = frame, color = counts).
# ---------------------------------------------------------------------------

_MYTHEN_FRAME_RE = re.compile(r"(\d+)\.dat$", re.IGNORECASE)

# folder key -> (signature, array, metadata)
_mythen_cache: dict[str, tuple[tuple[int, float], np.ndarray, dict[str, Any]]] = {}


def _mythen_frame_files(folder: Path) -> list[Path]:
    """Return the acquisition's frame files sorted by their numeric index."""
    indexed: list[tuple[int, Path]] = []
    try:
        entries = list(folder.iterdir())
    except OSError:
        return []
    for entry in entries:
        if not entry.is_file():
            continue
        match = _MYTHEN_FRAME_RE.search(entry.name)
        if not match:
            continue
        indexed.append((int(match.group(1)), entry))
    indexed.sort(key=lambda item: item[0])
    return [path for _, path in indexed]


def _resolve_mythen_acquisition(path: Path) -> tuple[Path, Path | None]:
    """Resolve a ``.cfg`` or ``.dat`` path to (folder, cfg_path)."""
    ext = _image_ext_name(path.name)
    folder = path.parent
    if ext == ".cfg":
        return folder, path
    cfgs = sorted(folder.glob("*.cfg"))
    return folder, (cfgs[0] if cfgs else None)


def _parse_mythen_cfg(cfg_path: Path | None) -> dict[str, Any]:
    """Extract detector/acquisition metadata from a MYTHEN ``.cfg`` file."""
    meta: dict[str, Any] = {"detector": "MYTHEN"}
    if cfg_path is None or not cfg_path.is_file():
        return meta
    import xml.etree.ElementTree as ET

    try:
        root = ET.parse(cfg_path).getroot()
    except (ET.ParseError, OSError):
        return meta

    def _text(tag: str) -> str | None:
        el = root.find(tag)
        if el is None or el.text is None:
            return None
        text = el.text.strip()
        return text or None

    channels = _first_number(_text("channels"))
    if channels is not None:
        meta["channels"] = int(channels)
    frames = _first_number(_text("frames"))
    if frames is not None:
        meta["frames_declared"] = int(frames)
    energy_kev = _first_number(_text("energy"))
    if energy_kev is not None:
        meta["energy_ev"] = float(energy_kev) * 1000.0
    exposure_ms = _first_number(_text("exposureTime"))
    if exposure_ms is not None:
        meta["exposure_time_s"] = float(exposure_ms) / 1000.0
    period_ms = _first_number(_text("exposurePeriod"))
    if period_ms is not None:
        meta["exposure_period_s"] = float(period_ms) / 1000.0
    system_number = _text("systemNumber")
    if system_number:
        meta["system_number"] = system_number
    server_version = _text("serverVersion")
    if server_version:
        meta["server_version"] = server_version

    bad_channels: list[int] = []
    bad_root = root.find("badChannels")
    if bad_root is not None:
        for bad in bad_root.findall("bad"):
            value = _first_number(bad.text)
            if value is not None:
                bad_channels.append(int(value))
    if bad_channels:
        meta["bad_channels"] = sorted(set(bad_channels))

    module = root.find("modules/module")
    if module is not None:

        def _mod_number(tag: str) -> float | None:
            el = module.find(tag)
            if el is None:
                return None
            return _first_number(el.text)

        serial_el = module.find("serialNumber")
        if serial_el is not None and serial_el.text:
            meta["module_serial"] = serial_el.text.strip()
        material_el = module.find("material")
        if material_el is not None and material_el.text:
            meta["material"] = material_el.text.strip()
        threshold_kev = _mod_number("threshold")
        if threshold_kev is not None:
            meta["threshold_ev"] = float(threshold_kev) * 1000.0
        thickness_um = _mod_number("thickness")
        if thickness_um is not None:
            meta["sensor_thickness_um"] = float(thickness_um)
    return meta


def _read_mythen_dat(path: Path, n_channels: int | None) -> np.ndarray:
    """Parse one ``FrameNNNN.dat`` file into a 1D intensity vector (counts only)."""
    try:
        text = path.read_text()
    except OSError as exc:
        raise HTTPException(status_code=422, detail=f"Cannot read MYTHEN frame {path.name}") from exc
    tokens = text.split()
    if not tokens:
        return np.zeros(int(n_channels or 0), dtype=np.int64)
    try:
        values = np.array(tokens, dtype=np.int64)
    except ValueError as exc:
        raise HTTPException(
            status_code=422, detail=f"Malformed MYTHEN frame {path.name}"
        ) from exc
    # "<channel> <count>" pairs; fall back to a counts-only column layout.
    counts = values.reshape(-1, 2)[:, 1] if values.size % 2 == 0 else values
    if n_channels and counts.size != n_channels:
        fixed = np.zeros(int(n_channels), dtype=np.int64)
        limit = min(int(n_channels), counts.size)
        fixed[:limit] = counts[:limit]
        counts = fixed
    return counts


def _read_mythen_acquisition(path: Path) -> tuple[np.ndarray, dict[str, Any]]:
    """Assemble a MYTHEN acquisition into a (frames, channels) intensity map."""
    folder, cfg_path = _resolve_mythen_acquisition(path)
    frame_files = _mythen_frame_files(folder)
    if not frame_files:
        raise HTTPException(
            status_code=422,
            detail="No MYTHEN frame (.dat) files found next to this acquisition",
        )

    try:
        latest_mtime = max(fp.stat().st_mtime for fp in frame_files)
    except OSError:
        latest_mtime = 0.0
    signature = (len(frame_files), latest_mtime)
    cache_key = str(folder.resolve())
    cached = _mythen_cache.get(cache_key)
    if cached is not None and cached[0] == signature:
        return cached[1], cached[2]

    meta = _parse_mythen_cfg(cfg_path)
    n_channels = meta.get("channels")
    rows = [_read_mythen_dat(fp, n_channels) for fp in frame_files]
    width = n_channels or max((row.size for row in rows), default=0)
    matrix = np.zeros((len(rows), int(width)), dtype=np.int64)
    for i, row in enumerate(rows):
        limit = min(int(width), row.size)
        matrix[i, :limit] = row[:limit]
    arr = _normalize_image_array(matrix)

    meta["channels"] = int(width)
    meta["frames_read"] = len(rows)
    if cfg_path is not None:
        meta["config_file"] = cfg_path.name

    _mythen_cache[cache_key] = (signature, arr, meta)
    return arr, meta


def _mythen_header_text(path: Path) -> str:
    """Human-readable header summary for a MYTHEN acquisition."""
    _, cfg_path = _resolve_mythen_acquisition(path)
    meta = _parse_mythen_cfg(cfg_path)
    frame_files = _mythen_frame_files(path.parent)
    lines: list[str] = ["# MYTHEN acquisition"]
    if meta.get("module_serial"):
        lines.append(f"# Module S/N {meta['module_serial']}")
    if meta.get("system_number"):
        lines.append(f"# System {meta['system_number']}")
    if meta.get("channels"):
        lines.append(f"# Channels {int(meta['channels'])}")
    lines.append(f"# Frames {len(frame_files)}")
    if meta.get("exposure_time_s") is not None:
        lines.append(f"# Exposure_time {meta['exposure_time_s']:.6g} s")
    if meta.get("energy_ev") is not None:
        lines.append(f"# Incident_energy {meta['energy_ev']:.6g} eV")
    if meta.get("threshold_ev") is not None:
        lines.append(f"# Threshold_energy {meta['threshold_ev']:.6g} eV")
    if meta.get("material"):
        lines.append(f"# Sensor {meta['material']}")
    if meta.get("bad_channels"):
        bad = meta["bad_channels"]
        preview = ", ".join(str(c) for c in bad[:16]) + (" …" if len(bad) > 16 else "")
        lines.append(f"# Bad_channels ({len(bad)}): {preview}")
    if cfg_path is not None:
        try:
            lines.append("")
            lines.append(cfg_path.read_text().strip())
        except OSError:
            pass
    return "\n".join(lines)
