"""Hold the DECTRIS TIFF tag to the layout every other reader assumes.

An IFD embedded in a tag states file-absolute value offsets, the same way EXIF
does. ALBIS wrote them relative to the start of the tag payload instead, and
read them back the same way, so nothing in the project noticed: only a reader
that had not made the same mistake would see it. tifffile 2026.x became such a
reader -- it decodes tag 51192 natively -- and every value too large to sit
inline in a tag entry came back as whatever bytes lived at that offset from the
start of the file.

So the reader here is deliberately written from the TIFF spec rather than
borrowed from `backend.image_formats`: a test that parses the file the same way
the writer packed it would have passed throughout.
"""

from __future__ import annotations

import struct
from pathlib import Path

import numpy as np
import pytest

tifffile = pytest.importorskip("tifffile")

from backend.image_formats import (  # noqa: E402
    _DECTRIS_TIFF_TAG,
    _rebase_dectris_ifd,
    _simplon_meta_from_tiff,
    _write_tiff,
)

METADATA = {
    "series_unique_id": "series-abc",
    "series_number": 42,
    "image_number": 101,
    "image_datetime": "2026-06-02T12:00:00Z",
    "threshold_ids": [1],
    "threshold_energies_ev": [6000.0],
    "exposure_time_s": 0.1,
    "incident_energy_ev": 12000.0,
    "wavelength_a": 1.0332,
    "beam_center_x_px": 123.4,
    "beam_center_y_px": 234.5,
    "detector_distance_m": 0.25,
}

_SIZES = {1: 1, 2: 1, 3: 2, 4: 4, 5: 8, 7: 1, 11: 4, 12: 8}


def _tag_entry(raw: bytes, tag_id: int) -> tuple[int, int]:
    """Find `tag_id` in IFD0 and return its (value offset, byte count)."""
    assert raw[:4] == b"II\x2a\x00", "expected classic little-endian TIFF"
    ifd = struct.unpack_from("<I", raw, 4)[0]
    for index in range(struct.unpack_from("<H", raw, ifd)[0]):
        entry = ifd + 2 + index * 12
        code, type_code, count = struct.unpack_from("<HHI", raw, entry)
        if code == tag_id:
            return struct.unpack_from("<I", raw, entry + 8)[0], count * _SIZES[type_code]
    raise AssertionError(f"tag {tag_id} is not in IFD0")


def _read_embedded_ifd(raw: bytes, offset: int) -> dict[int, object]:
    """Read an embedded IFD the way the TIFF spec says to: offsets are absolute."""
    entries: dict[int, object] = {}
    for index in range(struct.unpack_from("<H", raw, offset)[0]):
        entry = offset + 2 + index * 12
        code, type_code, count = struct.unpack_from("<HHI", raw, entry)
        total = _SIZES[type_code] * count
        if total <= 4:
            data = raw[entry + 8 : entry + 8 + total]
        else:
            pointer = struct.unpack_from("<I", raw, entry + 8)[0]
            data = raw[pointer : pointer + total]
        if type_code == 2:
            entries[code] = data.rstrip(b"\x00").decode("ascii")
        elif type_code == 12:
            values = list(struct.unpack(f"<{count}d", data))
            entries[code] = values[0] if count == 1 else values
        else:
            fmt = {1: "B", 3: "H", 4: "I"}[type_code]
            values = list(struct.unpack(f"<{count}{fmt}", data))
            entries[code] = values[0] if count == 1 else values
    return entries


def _write(path: Path) -> bytes:
    _write_tiff(path, np.arange(6, dtype=np.int32).reshape(2, 3), METADATA)
    return path.read_bytes()


def test_an_outside_reader_gets_the_values_albis_wrote(tmp_path: Path) -> None:
    """The whole bug: everything stored out of line used to decode to garbage."""
    raw = _write(tmp_path / "frame.tiff")
    offset, _ = _tag_entry(raw, _DECTRIS_TIFF_TAG)

    entries = _read_embedded_ifd(raw, offset)

    assert entries[0x0001] == "series-abc"
    assert entries[0x0002] == 42
    assert entries[0x0003] == 101
    assert entries[0x0004] == "2026-06-02T12:00:00Z"
    assert entries[0x0005] == 1
    assert entries[0x0006] == pytest.approx(6000.0)
    assert entries[0x0007] == pytest.approx(0.1)
    assert entries[0x0009] == pytest.approx(12000.0)
    assert entries[0x000A] == pytest.approx(1.0332)
    assert entries[0x0016] == pytest.approx([123.4, 234.5])
    assert entries[0x0017] == pytest.approx(0.25)


def test_every_offset_points_inside_the_tag_payload(tmp_path: Path) -> None:
    """A relative offset lands short of the payload; an absolute one lands in it."""
    raw = _write(tmp_path / "frame.tiff")
    offset, length = _tag_entry(raw, _DECTRIS_TIFF_TAG)

    pointers = []
    for index in range(struct.unpack_from("<H", raw, offset)[0]):
        entry = offset + 2 + index * 12
        _, type_code, count = struct.unpack_from("<HHI", raw, entry)
        if _SIZES[type_code] * count > 4:
            pointers.append(struct.unpack_from("<I", raw, entry + 8)[0])

    assert pointers, "nothing is stored out of line, so this proves nothing"
    assert all(offset < pointer < offset + length for pointer in pointers)


def test_albis_still_reads_its_own_output(tmp_path: Path) -> None:
    path = tmp_path / "frame.tiff"
    raw = _write(path)

    with tifffile.TiffFile(path) as tiff:
        meta = _simplon_meta_from_tiff(tiff, raw=raw)
        # `_pilatus_meta_from_tiff` has no file bytes to hand and reads the tag
        # off the open handle instead. Same answer, or the fix only half works.
        without_raw = _simplon_meta_from_tiff(tiff)

    assert meta["series_unique_id"] == "series-abc"
    assert meta["image_datetime"] == "2026-06-02T12:00:00Z"
    assert meta["exposure_time_s"] == pytest.approx(0.1)
    assert meta["beam_center_px"] == pytest.approx((123.4, 234.5))
    assert meta["distance_mm"] == pytest.approx(250.0)
    assert without_raw == meta


def test_a_file_from_an_older_albis_still_reads(tmp_path: Path) -> None:
    """Files already exported carry the relative layout and must keep working."""
    path = tmp_path / "legacy.tiff"
    raw = _write(path)
    offset, length = _tag_entry(raw, _DECTRIS_TIFF_TAG)
    payload = raw[offset : offset + length]
    # Undo the rebase, reproducing exactly what earlier versions wrote.
    legacy = _rebase_dectris_ifd(payload, -offset)
    assert legacy != payload
    path.write_bytes(raw[:offset] + legacy + raw[offset + length :])

    with tifffile.TiffFile(path) as tiff:
        meta = _simplon_meta_from_tiff(tiff, raw=path.read_bytes())

    assert meta["series_unique_id"] == "series-abc"
    assert meta["image_datetime"] == "2026-06-02T12:00:00Z"
    assert meta["exposure_time_s"] == pytest.approx(0.1)
    assert meta["beam_center_px"] == pytest.approx((123.4, 234.5))


def test_the_named_entries_tifffile_2026_returns_are_understood() -> None:
    """The dict fallback, for when the file bytes are not available to re-parse."""
    from backend.image_formats import _parse_dectris_tag_value

    entries = _parse_dectris_tag_value(
        {"SeriesUniqueId": "series-abc", "ImageNumber": 101, "BeamCenter": (123.4, 234.5)},
        "<",
    )

    assert entries[0x0001] == "series-abc"
    assert entries[0x0003] == 101
    assert entries[0x0016] == pytest.approx((123.4, 234.5))


def test_an_unknown_entry_name_is_dropped_rather_than_raising() -> None:
    """tifffile naming an entry ALBIS does not know must not fail the read."""
    from backend.image_formats import _parse_dectris_tag_value

    assert _parse_dectris_tag_value({"SomethingNew": 1, "ImageNumber": 7}, "<") == {0x0003: 7}
