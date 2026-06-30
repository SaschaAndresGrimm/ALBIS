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
    # Square source detector: per-axis sizes mirror the scalar.
    assert payload["pixel_size_x_um"] == pytest.approx(172.0)
    assert payload["pixel_size_y_um"] == pytest.approx(172.0)


def test_analysis_params_preserves_anisotropic_strixel_pixel_sizes(tmp_path: Path) -> None:
    """Non-square ("strixel") detectors must expose distinct x/y pixel sizes
    instead of averaging them, and convert beam centre per axis."""
    client = TestClient(app)
    h5_path = tmp_path / "pollux_strixel.h5"

    with h5py.File(h5_path, "w") as h5:
        entry = h5.create_group("entry")
        data_group = entry.create_group("data")
        data_group.create_dataset("data", data=np.arange(12, dtype=np.uint16).reshape(3, 4))
        detector = entry.create_group("instrument").create_group("detector")

        def _len(name: str, value: float) -> None:
            ds = detector.create_dataset(name, data=value)
            ds.attrs["units"] = "m"

        # 75 µm (fast/X) x 25 µm (slow/Y) elongated pixels.
        _len("x_pixel_size", 75e-6)
        _len("y_pixel_size", 25e-6)
        # Beam centre given in millimetres so the per-axis conversion is exercised.
        cx = detector.create_dataset("beam_center_x", data=75.0)  # mm
        cx.attrs["units"] = "mm"
        cy = detector.create_dataset("beam_center_y", data=12.5)  # mm
        cy.attrs["units"] = "mm"

    response = client.get(
        "/api/analysis/params",
        params={"file": str(h5_path), "dataset": "/entry/data/data"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["pixel_size_x_um"] == pytest.approx(75.0)
    assert payload["pixel_size_y_um"] == pytest.approx(25.0)
    # Reference (pixel_size_um) is the X size, NOT the (75+25)/2 = 50 average.
    assert payload["pixel_size_um"] == pytest.approx(75.0)
    # Beam centre converts with the matching per-axis pixel size:
    #   x: 75 mm / 0.075 mm = 1000 px ; y: 12.5 mm / 0.025 mm = 500 px
    assert payload["center_x_px"] == pytest.approx(1000.0)
    assert payload["center_y_px"] == pytest.approx(500.0)
