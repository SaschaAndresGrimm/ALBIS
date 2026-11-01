"""The private DECTRIS TIFF tag (51192): reading and writing its embedded IFD.

Decodes the tag from a detector-written or ALBIS-written TIFF into SIMPLON
metadata, packs the same metadata into a tag payload for an export, and rebases
the payload's value offsets once the file is on disk.
"""

from __future__ import annotations

import math
import struct
from pathlib import Path
from typing import Any, BinaryIO

import numpy as np

from .hdf5_units import wavelength_to_ev
from .image_values import (
    _as_int,
    _as_int_list,
    _as_number_list,
    _as_pair,
    _as_str,
    _distance_to_mm,
    _export_float,
    _export_int,
    _export_text,
    _first_number,
)

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
        entry_bytes.extend(struct.pack("<HHI", int(tag), int(type_code), int(count)) + value_field)
    return (
        struct.pack("<H", len(normalized))
        + bytes(entry_bytes)
        + b"\x00\x00\x00\x00"
        + bytes(data_bytes)
    )


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
        energy_ev = wavelength_to_ev(float(incident_wavelength), "a")
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
