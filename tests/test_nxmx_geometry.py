"""A detector on a 2θ arm is placed by its NXmx transformation chain.

Diamond I19-2 mounts its Eiger2 4M on a 2θ arm. The NeXus file says beam
centre (985.1, 1419.5) px and 85 mm, but those are the detector's 2θ = 0
description; with the arm at 20° the beam meets the detector at (572.6,
1419.5), and the stated centre is itself at 20° 2θ. ALBIS drew its rings around
the stated centre, so every ring and d-spacing was wrong for such data.

The chain below is the real file's (dials_data `four_circle_eiger`, collection
`02_CuHF2pyz2PF6b_2T`), value for value; `tests/fixtures/
dls_i19_2_two_theta_dxtbx.json` holds what dxtbx -- DIALS's geometry library --
makes of that file, without parallax correction since ALBIS applies none.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import h5py
import numpy as np
import pytest
from fastapi.testclient import TestClient

from backend.services.detector_geometry import _read_hdf5_embedded_geometry
from backend.services.nxmx_geometry import read_nxmx_geometry

FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "dls_i19_2_two_theta_dxtbx.json").read_text()
)


def _transformation(group, name, value, kind, vector, units, depends_on, offset=None):
    dset = group.create_dataset(name, data=np.asarray(value, dtype=float))
    dset.attrs["transformation_type"] = kind
    dset.attrs["vector"] = np.asarray(vector, dtype=float)
    dset.attrs["units"] = units
    dset.attrs["depends_on"] = depends_on
    if offset is not None:
        dset.attrs["offset"] = np.asarray(offset, dtype=float)
        dset.attrs["offset_units"] = "mm"
    return dset


def _i19_2_file(path: Path, two_theta_deg: float = 20.0, **overrides) -> Path:
    """The I19-2 detector chain as nexgen wrote it, value for value."""
    with h5py.File(path, "w") as h5:
        det = h5.create_group("entry/instrument/detector")
        det["description"] = b"Eiger 2X 4M"
        det["beam_center_x"] = 985.0666666666667
        det["beam_center_y"] = 1419.4666666666665
        det["detector_distance"] = np.array([0.085])
        det["detector_distance"].attrs["units"] = "m"
        module = det.create_group("module")
        module.attrs["NX_class"] = "NXdetector_module"
        module["data_origin"] = np.array([0, 0], dtype=np.int32)
        module["data_size"] = np.array([2162, 2068], dtype=np.int32)
        offset_parent = overrides.get("module_parent", "/entry/instrument/transformations/det_z")
        _transformation(
            module,
            "module_offset",
            0.12958389560435354,
            "translation",
            [0.82155271, -0.57013257, 0.0],
            overrides.get("module_units", "m"),
            offset_parent,
            offset=[0, 0, 0],
        )
        _transformation(
            module,
            "fast_pixel_direction",
            7.5e-05,
            "translation",
            [0.0, 1.0, 0.0],
            "m",
            "/entry/instrument/detector/module/module_offset",
            offset=overrides.get("pixel_offset", [0, 0, 0]),
        )
        _transformation(
            module,
            "slow_pixel_direction",
            7.5e-05,
            "translation",
            [-1.0, 0.0, 0.0],
            "m",
            "/entry/instrument/detector/module/module_offset",
            offset=[0, 0, 0],
        )
        tr = h5.create_group("entry/instrument/transformations")
        _transformation(
            tr,
            "det_z",
            [85.0],
            "translation",
            [0, 0, 1],
            "mm",
            "/entry/instrument/transformations/two_theta",
        )
        _transformation(
            tr,
            "two_theta",
            [two_theta_deg],
            "rotation",
            [-1, 0, 0],
            "deg",
            overrides.get("two_theta_parent", "."),
        )
    return path


def _d_at(panel: dict, x: int, y: int, wavelength: float) -> float:
    origin, fast, slow = map(
        np.asarray, (panel["origin_mm"], panel["fast_axis"], panel["slow_axis"])
    )
    point = (
        origin
        + (x + 0.5) * panel["pixel_size_mm"][0] * fast
        + (y + 0.5) * panel["pixel_size_mm"][1] * slow
    )
    two_theta = math.acos(point[2] / np.linalg.norm(point))
    return wavelength / (2 * math.sin(two_theta / 2))


def _read(path: Path) -> dict | None:
    with h5py.File(path, "r") as h5:
        return read_nxmx_geometry(h5)


# --------------------------------------------------------------------------
# The chain, against DIALS
# --------------------------------------------------------------------------


def test_the_panel_is_where_dxtbx_puts_it(tmp_path: Path) -> None:
    geometry = _read(_i19_2_file(tmp_path / "run.nxs"))
    assert geometry is not None and geometry["mode"] == "geometry"
    (panel,) = geometry["panels"]
    expected = FIXTURE["albis_geometry"]["panels"][0]
    for key in ("origin_mm", "fast_axis", "slow_axis", "pixel_size_mm", "raw_offset_px"):
        np.testing.assert_allclose(panel[key], expected[key], atol=1e-6)
    assert panel["image_size_px"] == [2068, 2162]


@pytest.mark.parametrize(("x", "y", "expected"), FIXTURE["dxtbx_d_spacings"])
def test_resolution_matches_dxtbx(tmp_path: Path, x: int, y: int, expected: float) -> None:
    (panel,) = _read(_i19_2_file(tmp_path / "run.nxs"))["panels"]
    d = _d_at(panel, x, y, FIXTURE["wavelength_a"])
    if expected > 100:
        # The beam itself (572.6, 1419.5): d runs off to thousands of Å, and its
        # digits follow rounding. What matters is that the beam is here.
        assert d > 1000
        return
    # 1e-7: the chain above spells module_offset's direction to 8 decimals; the
    # fixture comes from the file's full-precision one.
    assert d == pytest.approx(expected, rel=1e-7)


def test_the_stated_beam_centre_is_kept_as_the_reference(tmp_path: Path) -> None:
    """The 2θ = 0 values the Detector Geometry fields are measured against."""
    geometry = _read(_i19_2_file(tmp_path / "run.nxs"))
    assert geometry["reference_center_x_px"] == pytest.approx(985.0666666666667)
    assert geometry["reference_center_y_px"] == pytest.approx(1419.4666666666665)
    assert geometry["reference_distance_mm"] == pytest.approx(85.0)
    # Along the arm: the beam turned by the arm's 20°, away from the sample.
    np.testing.assert_allclose(
        geometry["distance_axis"], [0.0, math.sin(math.radians(20)), math.cos(math.radians(20))]
    )


# --------------------------------------------------------------------------
# When the flat model stays
# --------------------------------------------------------------------------


def test_a_detector_square_to_the_beam_keeps_the_flat_model(tmp_path: Path) -> None:
    assert _read(_i19_2_file(tmp_path / "run.nxs", two_theta_deg=0.0)) is None


def test_a_dectris_master_without_a_chain_keeps_the_flat_model(tmp_path: Path) -> None:
    with h5py.File(tmp_path / "x_master.h5", "w") as h5:
        det = h5.create_group("entry/instrument/detector")
        det["geometry/orientation/value"] = np.array([-1.0, 0, 0, 0, -1, 0])
        det["goniometer/two_theta"] = np.array([0.0])
    assert _read(tmp_path / "x_master.h5") is None


@pytest.mark.parametrize(
    "overrides",
    [
        {"two_theta_parent": "/entry/instrument/transformations/det_z"},  # a loop
        {"module_units": "furlong"},
        {"module_parent": "/entry/instrument/transformations/missing"},
        {"pixel_offset": [0.1, 0, 0]},  # writers disagree on what this means
    ],
)
def test_a_chain_it_cannot_follow_keeps_the_flat_model(tmp_path: Path, overrides) -> None:
    assert _read(_i19_2_file(tmp_path / "run.nxs", **overrides)) is None


# --------------------------------------------------------------------------
# The endpoint, the data files, and sums
# --------------------------------------------------------------------------


def _geometry(path: Path) -> dict:
    from backend.app import app

    response = TestClient(app, base_url="http://localhost").get(
        "/api/image/geometry", params={"file": str(path)}
    )
    assert response.status_code == 200, response.text
    return response.json()


def test_the_endpoint_serves_the_tilted_panel_with_its_reference(tmp_path: Path) -> None:
    payload = _geometry(_i19_2_file(tmp_path / "run.nxs"))
    assert payload["mode"] == "geometry"
    assert payload["source"] == "NXmx transformations"
    assert payload["reference_center_x_px"] == pytest.approx(985.0666666666667)
    assert len(payload["distance_axis"]) == 3


def test_a_data_file_on_its_own_takes_the_chain_from_its_master(tmp_path: Path) -> None:
    _i19_2_file(tmp_path / "run_w00001.nxs")
    with h5py.File(tmp_path / "run_000001.h5", "w") as h5:
        h5.create_dataset("data", data=np.zeros((1, 4, 4), dtype="u4"))

    assert _geometry(tmp_path / "run_000001.h5")["mode"] == "geometry"


def test_a_lone_data_file_stays_flat(tmp_path: Path) -> None:
    with h5py.File(tmp_path / "run_000001.h5", "w") as h5:
        h5.create_dataset("data", data=np.zeros((1, 4, 4), dtype="u4"))

    assert _geometry(tmp_path / "run_000001.h5")["mode"] == "planar"


def test_a_sum_keeps_the_reference_its_fields_are_measured_against(tmp_path: Path) -> None:
    """Without it a sum's own beam centre would shift its detector by 413 px."""
    payload = dict(FIXTURE["albis_geometry"])
    with h5py.File(tmp_path / "sum.h5", "w") as h5:
        h5["/entry/albis/geometry/json"] = json.dumps(payload)

    embedded = _read_hdf5_embedded_geometry(tmp_path / "sum.h5")

    assert embedded["reference_center_x_px"] == pytest.approx(payload["reference_center_x_px"])
    assert embedded["reference_distance_mm"] == pytest.approx(85.0)
    np.testing.assert_allclose(embedded["distance_axis"], payload["distance_axis"])
