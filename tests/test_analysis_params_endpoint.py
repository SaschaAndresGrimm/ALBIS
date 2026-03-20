from __future__ import annotations

from pathlib import Path

import h5py
import numpy as np
import pytest
from fabio.cbfimage import CbfImage
from fastapi.testclient import TestClient

from backend.app import app

PILATUS_HEADER = """# Detector: PILATUS 12M, S/N 120-0100
# Pixel_size 172e-6 m x 172e-6 m
# Photon_energy 7118 eV
# Detector_distance 0.25013 m
# Beam_xy (1083.9, 2593.48) pixels
"""


def _write_cbf(path: Path, header_text: str) -> None:
    image = CbfImage(data=np.arange(4, dtype=np.uint16).reshape(2, 2))
    image.header = {"_array_data.header_contents": header_text}
    image.write(str(path))


def test_analysis_params_inherits_missing_values_from_source_image(tmp_path: Path) -> None:
    client = TestClient(app)
    source_path = tmp_path / "7118_E1_1_00001.cbf"
    h5_path = tmp_path / "7118_E1_1_00001_series_sum.h5"
    _write_cbf(source_path, PILATUS_HEADER)

    with h5py.File(h5_path, "w") as h5:
        entry = h5.create_group("entry")
        data_group = entry.create_group("data")
        data_group.create_dataset("data", data=np.arange(12, dtype=np.uint16).reshape(3, 4))
        h5.attrs["source_file"] = str(source_path)

    response = client.get(
        "/api/analysis/params",
        params={"file": str(h5_path), "dataset": "/entry/data/data"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["shape"] == [3, 4]
    assert payload["distance_mm"] == pytest.approx(250.13)
    assert payload["pixel_size_um"] == pytest.approx(172.0)
    assert payload["energy_ev"] == pytest.approx(7118.0)
    assert payload["center_x_px"] == pytest.approx(1083.9)
    assert payload["center_y_px"] == pytest.approx(2593.48)
