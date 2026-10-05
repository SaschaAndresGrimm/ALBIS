"""The DLS I23 PILATUS 12M's built-in geometry, and the distance its rings use.

`tests/fixtures/dls_i23_p12m_imported_panels.json` holds the 24 panel frames
of an `imported.expt` that `dials.import` wrote for real I23 data
(`DLS_I23_P12M_thau_00001.cbf.gz` from dials_data), supplied by the beamline,
as its `panels` list states them: relative to the detector's root frame, which
carries the frame's pose (`_load_dials_expt_geometry` composes the two). Those
local frames are the blueprint, so the built-in geometry has to reproduce them;
`frontend/tests/dls_i23_p12m_rings.test.js` then checks that the rings drawn
from them, posed by the header, land where dxtbx puts them.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import numpy as np
import pytest
import tifffile
from fabio.cbfimage import CbfImage
from fastapi.testclient import TestClient

from backend.app import app
from backend.detector_profiles import (
    dls_i23_pilatus_12m_beam_distance_mm,
    dls_i23_pilatus_12m_geometry,
    ring_meta,
)

ROOT = Path(__file__).resolve().parents[1]
DIALS_PANELS = json.loads(
    (ROOT / "tests" / "fixtures" / "dls_i23_p12m_imported_panels.json").read_text(encoding="utf-8")
)

# The real frame's header, less the lines nothing here reads.
I23_HEADER = """# Detector: PILATUS 12M, S/N 120-0100
# 2021-06-17T13:51:36.874
# Pixel_size 172e-6 m x 172e-6 m
# Silicon sensor, thickness 0.000320 m
# Wavelength 2.75520 A
# Detector_distance 0.01000 m
# Beam_xy (1080.00, 2595.00) pixels
"""


def _write_cbf(path: Path, header_text: str) -> None:
    image = CbfImage(data=np.arange(4, dtype=np.int32).reshape(2, 2))
    image.header = {"_array_data.header_contents": header_text}
    image.write(str(path))


def test_the_built_in_geometry_is_the_one_dials_imports() -> None:
    built = dls_i23_pilatus_12m_geometry()["panels"]

    assert len(built) == len(DIALS_PANELS) == 24
    for ours, theirs in zip(built, DIALS_PANELS, strict=True):
        assert ours["name"] == theirs["name"]
        assert ours["image_size_px"] == theirs["image_size_px"]
        assert ours["raw_offset_px"] == theirs["raw_offset_px"]
        for key in ("origin_mm", "fast_axis", "slow_axis", "pixel_size_mm"):
            assert ours[key] == pytest.approx(theirs[key], abs=1e-9), (ours["name"], key)


def test_the_payload_is_a_copy_callers_may_change() -> None:
    first = dls_i23_pilatus_12m_geometry()
    first["panels"][0]["origin_mm"][0] = 0.0

    assert dls_i23_pilatus_12m_geometry()["panels"][0]["origin_mm"][0] == pytest.approx(-184.9)


def test_the_beam_meets_the_blueprint_detector_at_its_radius() -> None:
    # Row 12 sits almost square to the beam, 250 mm from the sample.
    assert dls_i23_pilatus_12m_beam_distance_mm() == pytest.approx(250.1346, abs=1e-4)


def test_the_header_distance_is_read_as_an_offset_from_the_blueprint() -> None:
    meta = {
        "detector_description": "PILATUS 12M",
        "detector_serial_number": "120-0100",
        "distance_mm": 10.0,
    }

    assert ring_meta(meta)["distance_mm"] == pytest.approx(260.1346, abs=1e-4)
    assert meta["distance_mm"] == 10.0, "the parsed metadata itself must not change"


def test_a_missing_header_distance_means_the_blueprint_position() -> None:
    meta = {"detector_description": "PILATUS 12M", "detector_serial_number": "120-0100"}

    assert ring_meta(meta)["distance_mm"] == pytest.approx(250.1346, abs=1e-4)


@pytest.mark.parametrize(
    "meta",
    [
        {"detector_description": "PILATUS 6M", "detector_serial_number": "60-0001"},
        {"detector_description": "PILATUS 12M", "detector_serial_number": "120-0199"},
        {"detector_description": "EIGER2 XE 16M", "detector_serial_number": "120-0100"},
        {},
    ],
)
def test_every_other_detector_is_left_alone(meta: dict) -> None:
    meta = {**meta, "distance_mm": 200.0}

    assert ring_meta(meta) is meta


@pytest.mark.parametrize("suffix", [".cbf", ".tif"])
def test_the_image_route_reports_the_ring_distance(tmp_path: Path, suffix: str) -> None:
    image_path = tmp_path / f"thau_00001{suffix}"
    if suffix == ".cbf":
        _write_cbf(image_path, I23_HEADER)
    else:
        tifffile.imwrite(image_path, np.zeros((2, 2), dtype=np.int32), description=I23_HEADER)

    response = TestClient(app).get("/api/image", params={"file": str(image_path)})

    assert response.status_code == 200
    assert float(response.headers["x-image-detectordistance-mm"]) == pytest.approx(
        260.1346, abs=1e-4
    )
    assert float(response.headers["x-image-beamcenter-x"]) == pytest.approx(1080.0)
    assert float(response.headers["x-image-beamcenter-y"]) == pytest.approx(2595.0)


def test_the_raw_header_still_says_what_the_detector_wrote(tmp_path: Path) -> None:
    image_path = tmp_path / "thau_00001.cbf"
    _write_cbf(image_path, I23_HEADER)

    response = TestClient(app).get("/api/image/header", params={"file": str(image_path)})

    assert "Detector_distance 0.01000 m" in response.json()["header"]


def test_an_exported_cbf_keeps_the_header_distance_dials_expects(tmp_path: Path) -> None:
    """DIALS reads the exported header the way it reads the original: as an offset.

    Writing the ring distance back would move the detector 260 mm away in DIALS.
    """
    source = tmp_path / "thau_00001.cbf"
    out_dir = tmp_path / "export"
    _write_cbf(source, I23_HEADER)
    client = TestClient(app)

    start = client.post(
        "/api/export/data/start",
        json={
            "file": str(source),
            "format": "cbf",
            "output_dir": str(out_dir),
            "output_prefix": "thau",
            "frame_mode": "current",
        },
    )
    assert start.status_code == 200
    job_id = start.json()["job_id"]
    for _ in range(100):
        job = client.get("/api/export/data/status", params={"job_id": job_id}).json()
        if job["status"] not in {"queued", "running"}:
            break
        time.sleep(0.05)

    assert job["status"] == "done", job
    header = CbfImage().read(job["outputs"][0]).header["_array_data.header_contents"]
    assert "Detector_distance 0.01 m" in header
