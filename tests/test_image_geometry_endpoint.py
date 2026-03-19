from __future__ import annotations

import json
from pathlib import Path

import numpy as np
from fabio.cbfimage import CbfImage
from fastapi.testclient import TestClient

from backend.app import app

P12M_HEADER = """# Detector: PILATUS 12M, S/N 120-0100
# Pixel_size 172e-6 m x 172e-6 m
# Wavelength 1.0 A
# Detector_distance 0.0 m
# Beam_xy (1079.5, 2603.0) pixels
"""

PLANAR_HEADER = """# Detector: PILATUS 6M, S/N 60-0001
# Pixel_size 172e-6 m x 172e-6 m
# Wavelength 1.0 A
# Detector_distance 0.2 m
# Beam_xy (123.4, 234.5) pixels
"""


def _write_cbf(path: Path, header_text: str) -> None:
    image = CbfImage(data=np.arange(4, dtype=np.uint16).reshape(2, 2))
    image.header = {"_array_data.header_contents": header_text}
    image.write(str(path))


def _write_geometry(path: Path) -> None:
    payload = {
        "__id__": "ExperimentList",
        "detector": [
            {
                "panels": [
                    {
                        "name": "row-00",
                        "origin": [-184.9, -245.0, -52.49],
                        "fast_axis": [1.0, 0.0, 0.0],
                        "slow_axis": [0.0, -0.1434, 0.9896],
                        "pixel_size": [0.172, 0.172],
                        "image_size": [2463, 195],
                        "raw_image_offset": [0, 0],
                    },
                    {
                        "name": "row-01",
                        "origin": [-184.9, -250.0, -16.38],
                        "fast_axis": [1.0, 0.0, 0.0],
                        "slow_axis": [0.0, 0.0016, 0.9999],
                        "pixel_size": [0.172, 0.172],
                        "image_size": [2463, 195],
                        "raw_image_offset": [0, 212],
                    },
                ]
            }
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_image_geometry_endpoint_returns_geometry_for_pilatus_12m_cbf(tmp_path: Path) -> None:
    client = TestClient(app)
    image_path = tmp_path / "scan_0001.cbf"
    geometry_dir = tmp_path / "P12M_geometry"
    geometry_dir.mkdir()
    _write_cbf(image_path, P12M_HEADER)
    _write_geometry(geometry_dir / "imported.expt")

    response = client.get("/api/image/geometry", params={"file": str(image_path)})

    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "geometry"
    assert payload["detector"] == "pilatus-12m-dls-cshape"
    assert payload["source"].endswith("P12M_geometry/imported.expt")
    assert len(payload["panels"]) == 2
    assert payload["panels"][0]["name"] == "row-00"
    assert payload["panels"][1]["raw_offset_px"] == [0, 212]


def test_image_geometry_endpoint_falls_back_to_planar_without_geometry_file(tmp_path: Path) -> None:
    client = TestClient(app)
    image_path = tmp_path / "scan_0001.cbf"
    _write_cbf(image_path, P12M_HEADER)

    response = client.get("/api/image/geometry", params={"file": str(image_path)})

    assert response.status_code == 200
    assert response.json() == {
        "mode": "planar",
        "detector": "",
        "source": "",
        "panels": [],
    }


def test_image_geometry_endpoint_ignores_non_12m_images_even_with_geometry_file(
    tmp_path: Path,
) -> None:
    client = TestClient(app)
    image_path = tmp_path / "scan_0001.cbf"
    _write_cbf(image_path, PLANAR_HEADER)
    _write_geometry(tmp_path / "imported.expt")

    response = client.get("/api/image/geometry", params={"file": str(image_path)})

    assert response.status_code == 200
    assert response.json()["mode"] == "planar"
    assert response.json()["panels"] == []
