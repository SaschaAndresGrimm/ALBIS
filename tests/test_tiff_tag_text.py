"""A TIFF's tags read by tifffile give the header text fabio read through Pillow."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import tifffile

from backend.services.tiff_tag_text import tiff_tag_header_text

ROOT = Path(__file__).resolve().parents[1]


def test_the_monitor_tiff_header_matches_what_fabio_read_through_pillow() -> None:
    # Recorded from fabio 2026.6.0 + Pillow before Pillow left the bundle.
    expected = (
        "numberOfColumns 2068\n"
        "numberOfRows 2162\n"
        "bitsPerSample 32\n"
        "compression 1\n"
        "photometricInterpretation 1\n"
        "model Dectris EIGER2 Si 4M\n"
        "stripOffsets 286\n"
        "samplesPerPixel 1\n"
        "rowsPerStrip 2162\n"
        "stripByteCounts 17884064\n"
        "software DECTRIS-DAQ release-2024.2\n"
        "sampleFormat 1"
    )
    with tifffile.TiffFile(ROOT / "testdata" / "monitor.tiff") as tiff:
        assert tiff_tag_header_text(tiff.pages[0]) == expected


def test_enums_become_numbers_and_one_element_values_are_unwrapped(tmp_path: Path) -> None:
    path = tmp_path / "x.tif"
    tifffile.imwrite(
        path,
        np.zeros((3, 4), dtype=np.uint16),
        software="ALBIS test",
        extratags=[(272, "s", 0, "Some Model", True)],
        rowsperstrip=1,
    )
    with tifffile.TiffFile(path) as tiff:
        text = tiff_tag_header_text(tiff.pages[0])
    lines = dict(line.split(" ", 1) for line in text.splitlines())
    assert lines["numberOfColumns"] == "4"
    assert lines["numberOfRows"] == "3"
    assert lines["compression"] == "1"  # not COMPRESSION.NONE
    assert lines["model"] == "Some Model"
    assert lines["software"] == "ALBIS test"
    # Three strips stay a tuple, as Pillow gave them.
    assert lines["stripByteCounts"] == "(8, 8, 8)"


def test_a_page_without_tags_gives_no_text() -> None:
    assert tiff_tag_header_text(object()) == ""
