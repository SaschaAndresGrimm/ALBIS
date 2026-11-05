"""The detector's 2θ, as a file states it, reaches the Detector Geometry fields.

An older DECTRIS master records `detector/goniometer/two_theta` -- the angle a
beamline sent through SIMPLON's `two_theta_start` -- but nothing about the axis
it turns about, which belongs to the beamline's arm. The analysis parameters
report the angle; the interface applies it once the user gives the direction.
"""

from __future__ import annotations

from pathlib import Path

import h5py
import numpy as np
import pytest
from fastapi.testclient import TestClient


def _params(path: Path) -> dict:
    from backend.app import app

    response = TestClient(app, base_url="http://localhost").get(
        "/api/analysis/params", params={"file": str(path), "dataset": "/entry/data/data"}
    )
    assert response.status_code == 200, response.text
    return response.json()


def _master(path: Path, two_theta=None, units: str = "degree", group: str = "goniometer") -> Path:
    with h5py.File(path, "w") as h5:
        h5.create_dataset("entry/data/data", data=np.zeros((1, 4, 4), dtype="u2"))
        det = h5.create_group("entry/instrument/detector")
        det["detector_distance"] = 0.134
        if two_theta is not None:
            name = "two_theta" if group == "goniometer" else "two_theta_start"
            dset = det.create_dataset(f"{group}/{name}", data=np.array([two_theta]))
            dset.attrs["units"] = units
    return path


def test_a_dectris_master_reports_its_two_theta(tmp_path: Path) -> None:
    assert _params(_master(tmp_path / "x_master.h5", 20.0))["two_theta_deg"] == pytest.approx(20.0)


def test_a_two_theta_in_radians_is_reported_in_degrees(tmp_path: Path) -> None:
    payload = _params(_master(tmp_path / "x_master.h5", np.radians(15.0), units="rad"))
    assert payload["two_theta_deg"] == pytest.approx(15.0)


def test_a_master_without_one_reports_none(tmp_path: Path) -> None:
    assert _params(_master(tmp_path / "x_master.h5"))["two_theta_deg"] is None


def test_odins_meta_file_reports_the_detectors_setting(tmp_path: Path) -> None:
    """A data file on its own takes it from its collection's meta file."""
    with h5py.File(tmp_path / "run_000001.h5", "w") as h5:
        h5.create_dataset("data", data=np.zeros((1, 4, 4), dtype="u4"))
    with h5py.File(tmp_path / "run_meta.h5", "w") as h5:
        h5["_dectris/two_theta_start"] = np.array([12.5])

    from backend.app import app

    payload = (
        TestClient(app, base_url="http://localhost")
        .get(
            "/api/analysis/params",
            params={"file": str(tmp_path / "run_000001.h5"), "dataset": "/data"},
        )
        .json()
    )
    assert payload["two_theta_deg"] == pytest.approx(12.5)
