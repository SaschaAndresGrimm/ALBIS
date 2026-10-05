"""DIALS geometry files are read in DIALS's own frame and turned to ALBIS's.

ALBIS's rings put the sample at the origin with the X-rays travelling along +z.
DIALS writes the beam direction pointing back to the source (X-rays along -z),
and each panel relative to its group in a hierarchy. Read as they stood, a
flat detector's panel sat behind the sample: `imported.expt` straight from
`dials.import` gave d 0.84 A where dxtbx gives 1.92 A, and no beam centre at
all. The I23 file only worked because its panels are local frames that happen
to point the other way.

`tests/fixtures/dials_pilatus_flat_imported.expt` is what dxtbx (conda-forge)
wrote for `testdata/in16c_010001.cbf`, reduced to its beam and detector.
`frontend/tests/dials_flat_rings.test.js` checks the rings drawn from the
loaded panels against dxtbx's own d-spacings.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from backend.image_formats import _load_dials_expt_geometry

FIXTURES = Path(__file__).resolve().parent / "fixtures"
FLAT_EXPT = FIXTURES / "dials_pilatus_flat_imported.expt"
FLAT_EXPECTED = json.loads((FIXTURES / "dials_pilatus_flat_albis.json").read_text(encoding="utf-8"))


def beam_hit(panel: dict) -> tuple[float, float, float]:
    """Where the +z ray from the sample meets the panel: (x px, y px, distance mm)."""
    origin = np.asarray(panel["origin_mm"])
    fast = np.asarray(panel["fast_axis"])
    slow = np.asarray(panel["slow_axis"])
    normal = np.cross(fast, slow)
    t = origin @ normal / normal[2]
    rel = np.array([0.0, 0.0, t]) - origin
    x = rel @ fast / panel["pixel_size_mm"][0] - 0.5 + panel["raw_offset_px"][0]
    y = rel @ slow / panel["pixel_size_mm"][1] - 0.5 + panel["raw_offset_px"][1]
    return float(x), float(y), float(t)


def test_a_flat_detector_faces_the_beam_where_its_header_says() -> None:
    (panel,) = _load_dials_expt_geometry(FLAT_EXPT)

    x, y, distance = beam_hit(panel)

    # The CBF header: Detector_distance 0.040 m, Beam_xy (244, 308) -- in
    # pixel-corner coordinates, so 243.5, 307.5 as pixel centres.
    assert distance == pytest.approx(40.0, abs=1e-9)
    assert x == pytest.approx(243.5, abs=1e-6)
    assert y == pytest.approx(307.5, abs=1e-6)


def test_the_loaded_panels_are_the_ones_the_ring_test_uses() -> None:
    assert _load_dials_expt_geometry(FLAT_EXPT) == FLAT_EXPECTED["albis_panels"]


def test_the_i23_hierarchy_is_composed_and_turned_to_the_beam(tmp_path: Path) -> None:
    # dials.show for that file prints row-00 at origin (185.76, -245.176,
    # 42.4929), fast (-1, 0, 0), slow (0, -0.143467, -0.989655) in the lab.
    # Turned half a turn about y, so the X-rays travel along +z, x and z
    # change sign: origin (-185.76, -245.176, -42.4929), fast (1, 0, 0),
    # slow (0, -0.143467, 0.989655).
    expt = {
        "beam": [{"direction": [0.0, 0.0, 1.0]}],
        "detector": [
            {
                "hierarchy": {
                    "fast_axis": [-1.0, 0.0, 0.0],
                    "slow_axis": [0.0, 1.0, 0.0],
                    "origin": [0.86, -0.172, -10.0],
                    "children": [{"panel": 0}],
                },
                "panels": [
                    {
                        "name": "row-00",
                        "fast_axis": [1.0, 0.0, 0.0],
                        "slow_axis": [0.0, -0.1434667129292877, 0.9896551431085807],
                        "origin": [-184.9, -245.00354499993313, -52.49288463654608],
                        "pixel_size": [0.172, 0.172],
                        "image_size": [2463, 195],
                        "raw_image_offset": [0, 0],
                    }
                ],
            }
        ],
    }
    path = tmp_path / "imported.expt"
    path.write_text(json.dumps(expt), encoding="utf-8")

    (panel,) = _load_dials_expt_geometry(path)

    assert panel["origin_mm"] == pytest.approx([-185.76, -245.17554, -42.49288], abs=1e-4)
    assert panel["fast_axis"] == pytest.approx([1.0, 0.0, 0.0])
    assert panel["slow_axis"] == pytest.approx([0.0, -0.1434667, 0.9896551], abs=1e-6)


def test_a_file_without_a_beam_is_taken_as_already_in_albis_frame(tmp_path: Path) -> None:
    panel = {
        "name": "row-00",
        "fast_axis": [1.0, 0.0, 0.0],
        "slow_axis": [0.0, 1.0, 0.0],
        "origin": [-10.0, -10.0, 100.0],
        "pixel_size": [1.0, 1.0],
        "image_size": [20, 20],
        "raw_image_offset": [0, 0],
    }
    path = tmp_path / "hand_written.expt"
    path.write_text(json.dumps({"detector": [{"panels": [panel]}]}), encoding="utf-8")

    (loaded,) = _load_dials_expt_geometry(path)

    assert loaded["origin_mm"] == [-10.0, -10.0, 100.0]


def test_a_tilted_beam_is_turned_onto_z(tmp_path: Path) -> None:
    # Same flat detector, with the whole lab rotated 30 degrees about y: the
    # beam hit, in pixels and millimetres, must not change.
    raw = json.loads(FLAT_EXPT.read_text(encoding="utf-8"))
    angle = np.radians(30)
    rotation = np.array(
        [[np.cos(angle), 0, np.sin(angle)], [0, 1, 0], [-np.sin(angle), 0, np.cos(angle)]]
    )
    raw["beam"][0]["direction"] = (rotation @ np.asarray(raw["beam"][0]["direction"])).tolist()
    root = raw["detector"][0]["hierarchy"]
    for key in ("fast_axis", "slow_axis", "origin"):
        root[key] = (rotation @ np.asarray(root[key])).tolist()
    path = tmp_path / "tilted.expt"
    path.write_text(json.dumps(raw), encoding="utf-8")

    (panel,) = _load_dials_expt_geometry(path)

    assert beam_hit(panel) == pytest.approx((243.5, 307.5, 40.0), abs=1e-6)


def test_the_example_in_the_power_user_guide_works(tmp_path: Path) -> None:
    guide = (Path(__file__).resolve().parents[1] / "docs" / "POWER_USER_GUIDE.md").read_text(
        encoding="utf-8"
    )
    section = guide[guide.index("## Geometry Files") :]
    example = section[
        section.index("```json")
        + len("```json") : section.index("```", section.index("```json") + 7)
    ]
    path = tmp_path / "example.expt"
    path.write_text(example, encoding="utf-8")

    (panel,) = _load_dials_expt_geometry(path)

    # "200 mm from the sample, with the beam at its centre."
    assert beam_hit(panel) == pytest.approx((499.5, 499.5, 200.0), abs=1e-6)
