"""Golden decode tests against the real detector files in ``testdata/``.

ALBIS's worst possible failure is displaying a wrong number, and the decode
path is where that would happen silently: a fabio or tifffile bump, a dtype
change, a transposed axis. The rest of the suite builds its fixtures with numpy
and h5py, so none of it touches a real PILATUS byte-offset stream or a real
DECTRIS private TIFF tag. These files were already in the repository; nothing
read them.

Three kinds of assertion, in order of how much they prove:

1. **Independently derived.** The CBF pixels are decoded a second time by the
   reference byte-offset decoder below, written from the CBF specification, and
   compared element by element with what the API returns. The CBF metadata is
   compared against the numbers in the file's own header text rather than
   against a value transcribed from a previous run.
2. **Self-guarding.** Each fixture's own integrity is checked first, so a
   corrupted or re-saved file fails as itself instead of quietly re-baselining
   everything below it.
3. **Golden.** Digests and the DECTRIS private-tag values, which cannot be
   re-derived from anything the file states in text. These only detect change;
   they were produced by the run that also passed (1) and (2).
"""

from __future__ import annotations

import base64
import hashlib
import re
from pathlib import Path

import numpy as np
import pytest
from fastapi.testclient import TestClient

from backend.app import app

TESTDATA = Path(__file__).resolve().parents[1] / "testdata"
CBF = TESTDATA / "in16c_010001.cbf"
CBF_SERIES = [TESTDATA / f"in16c_0100{n:02d}.cbf" for n in range(1, 11)]
TIFF = TESTDATA / "monitor.tiff"

client = TestClient(app)


def read_image(path: Path) -> tuple[np.ndarray, dict[str, str]]:
    response = client.get("/api/image", params={"file": str(path)})
    assert response.status_code == 200, response.text
    headers = {k.lower(): v for k, v in response.headers.items()}
    shape = tuple(int(part) for part in headers["x-shape"].split(","))
    array = np.frombuffer(response.content, dtype=headers["x-dtype"]).reshape(shape)
    return array, headers


def header_text(path: Path) -> str:
    response = client.get("/api/image/header", params={"file": str(path)})
    assert response.status_code == 200, response.text
    return response.json()["header"]


# --------------------------------------------------------------------------
# CBF: an independent decoder
# --------------------------------------------------------------------------

# The sentinel CBFlib writes between the MIME header and the compressed data.
CBF_BINARY_SENTINEL = b"\x0c\x1a\x04\xd5"


def split_cbf(raw: bytes) -> tuple[dict[str, str], bytes]:
    """Return the binary section's MIME fields and its compressed payload."""
    section = raw.index(b"--CIF-BINARY-FORMAT-SECTION--")
    sentinel = raw.index(CBF_BINARY_SENTINEL, section)
    mime = raw[section:sentinel].decode("ascii", "replace")
    fields = dict(re.findall(r"^([\w-]+):\s*(.+?)\s*$", mime, re.M))
    # conversions= sits on a continuation line of Content-Type, not on one of
    # its own, so it is not a field the loop above sees.
    conversions = re.search(r'conversions="([^"]+)"', mime)
    fields["conversions"] = conversions.group(1) if conversions else ""
    size = int(fields["X-Binary-Size"])
    return fields, raw[sentinel + len(CBF_BINARY_SENTINEL) :][:size]


def decode_byte_offset(blob: bytes, count: int) -> np.ndarray:
    """CBF_BYTE_OFFSET, written from the specification rather than reused.

    Each element is a delta from the previous one, in the shortest signed
    width that holds it; the most negative value of a width is the escape to
    the next width up.
    """
    out = np.empty(count, dtype=np.int64)
    view = memoryview(blob)
    value = 0
    pos = 0
    for index in range(count):
        delta = int.from_bytes(view[pos : pos + 1], "little", signed=True)
        pos += 1
        if delta == -(2**7):
            delta = int.from_bytes(view[pos : pos + 2], "little", signed=True)
            pos += 2
            if delta == -(2**15):
                delta = int.from_bytes(view[pos : pos + 4], "little", signed=True)
                pos += 4
                if delta == -(2**31):
                    delta = int.from_bytes(view[pos : pos + 8], "little", signed=True)
                    pos += 8
        value += delta
        out[index] = value
    assert pos == len(blob), f"{len(blob) - pos} trailing bytes the decoder did not consume"
    return out


def test_the_cbf_fixture_is_the_file_it_says_it_is() -> None:
    """Its own Content-MD5 covers the compressed stream; check it before use."""
    fields, blob = split_cbf(CBF.read_bytes())
    digest = base64.b64encode(hashlib.md5(blob).digest()).decode()
    assert digest == fields["Content-MD5"]
    assert fields["conversions"] == "x-CBF_BYTE_OFFSET"
    assert fields["X-Binary-Element-Type"].strip('"') == "signed 32-bit integer"


def test_cbf_pixels_match_an_independent_byte_offset_decoder() -> None:
    fields, blob = split_cbf(CBF.read_bytes())
    reference = decode_byte_offset(blob, int(fields["X-Binary-Number-of-Elements"])).reshape(
        int(fields["X-Binary-Size-Second-Dimension"]),
        int(fields["X-Binary-Size-Fastest-Dimension"]),
    )

    array, headers = read_image(CBF)

    # Row order and axis order too, not just the value set: reshaping the
    # reference the other way round would still have the same sum.
    assert array.shape == reference.shape
    assert np.array_equal(array, reference)
    assert headers["x-dtype"] == "<i4"


def test_cbf_metadata_matches_the_numbers_in_its_own_header() -> None:
    text = header_text(CBF)
    _, headers = read_image(CBF)

    def field(pattern: str) -> str:
        match = re.search(pattern, text)
        assert match, f"{pattern!r} missing from the header ALBIS returned"
        return match.group(1)

    assert float(headers["x-image-pixelsize-um"]) == pytest.approx(
        float(field(r"# Pixel_size\s+([\d.e-]+)\s*m")) * 1e6
    )
    assert float(headers["x-image-wavelength-a"]) == pytest.approx(
        float(field(r"# Wavelength\s+([\d.]+)\s*A"))
    )
    assert float(headers["x-image-detectordistance-mm"]) == pytest.approx(
        float(field(r"# Detector_distance\s+([\d.]+)\s*m")) * 1000
    )
    beam = re.search(r"# Beam_xy\s*\(\s*([\d.]+)\s*,\s*([\d.]+)\s*\)", text)
    assert beam
    assert float(headers["x-image-beamcenter-x"]) == pytest.approx(float(beam.group(1)))
    assert float(headers["x-image-beamcenter-y"]) == pytest.approx(float(beam.group(2)))

    # Energy is derived, not stated, so check the relation rather than a value.
    hc_ev_angstrom = 12398.4193
    assert float(headers["x-image-energy-ev"]) == pytest.approx(
        hc_ev_angstrom / float(headers["x-image-wavelength-a"]), rel=1e-6
    )


def test_cbf_decodes_to_the_same_pixels_it_always_has() -> None:
    """Golden. Verified by the two tests above at the time it was written."""
    array, _ = read_image(CBF)
    assert array.shape == (619, 487)
    assert (int(array.min()), int(array.max())) == (-2, 3363)
    assert int(array.sum()) == 1870204
    # -1 and -2 are the PILATUS conventions for a gap and a bad pixel; a
    # decoder that dropped the sign would lose both.
    assert int((array < 0).sum()) == 16577
    assert (
        hashlib.sha256(array.tobytes(order="C")).hexdigest()
        == "1b95829c57bcf52e8fbae967f1f6bdbfb69d549b7075a326dacc047f3148d9a3"
    )


def test_the_cbf_series_is_ten_different_frames() -> None:
    """A series test over ten copies of one frame would prove nothing."""
    sums = []
    for path in CBF_SERIES:
        array, headers = read_image(path)
        assert array.shape == (619, 487)
        assert headers["x-dtype"] == "<i4"
        sums.append(int(array.sum()))
    assert len(set(sums)) == len(sums)
    assert sums[0] == 1870204


# --------------------------------------------------------------------------
# TIFF: a DECTRIS EIGER2 monitor image
# --------------------------------------------------------------------------


def test_tiff_decodes_to_the_same_pixels_it_always_has() -> None:
    array, headers = read_image(TIFF)
    assert headers["x-dtype"] == "<u4"
    assert array.shape == (2162, 2068)
    # EIGER2 writes the unsigned maximum for a gap or a bad pixel, and the
    # frontend's saturation handling keys off exactly that value.
    assert int(array.max()) == 2**32 - 1
    assert int((array == 2**32 - 1).sum()) == 260568
    assert int(array.min()) == 0
    assert (
        hashlib.sha256(array.tobytes(order="C")).hexdigest()
        == "123c8de585ca3e35750cde77c0b2fde01bb19251e94e40521dbed83bb282d001"
    )


def test_tiff_header_reports_the_detector_that_wrote_it() -> None:
    text = header_text(TIFF)
    assert "Dectris EIGER2 Si 4M" in text
    assert "DECTRIS-DAQ" in text
    # The IFD's own dimensions, independent of how the pixels were reshaped.
    assert re.search(r"numberOfColumns\s+2068", text)
    assert re.search(r"numberOfRows\s+2162", text)


def test_tiff_private_dectris_tag_is_parsed() -> None:
    """Golden: these live in the private IFD and are stated nowhere in text.

    The offsets inside that IFD are the fiddly part -- some writers make them
    absolute and some relative -- so a regression here is silent and would
    reach the resolution rings as a wrong wavelength.
    """
    from backend.image_formats import _pilatus_meta_from_tiff

    meta = _pilatus_meta_from_tiff(TIFF)
    assert meta["series_unique_id"] == "01KGREHQGWZM858XZBSX6AV9N2"
    assert meta["series_number"] == 21
    assert meta["image_number"] == 0
    assert meta["threshold_energies_ev"] == [4000.0]
    assert meta["exposure_time_s"] == pytest.approx(3.0)
    assert meta["energy_ev"] == pytest.approx(8000.0)
    assert meta["wavelength_a"] == pytest.approx(1.5498024804150032)
    assert meta["lost_pixel_count"] == 0
    # This monitor image really was written with no geometry configured; the
    # fields above are what prove the tag parsed rather than defaulted.
    assert meta["distance_mm"] == 0.0
    assert tuple(meta["beam_center_px"]) == (0.0, 0.0)
