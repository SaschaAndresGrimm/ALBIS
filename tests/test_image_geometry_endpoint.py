from __future__ import annotations

import json
from pathlib import Path

import h5py
import numpy as np
import pytest
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


def _touch(path: Path) -> None:
    path.write_bytes(b"test")


def _write_h5_embedded_geometry(
    path: Path,
    geometry_payload: dict[str, object] | str,
    *,
    source_file: Path | None = None,
) -> None:
    with h5py.File(path, "w") as h5:
        h5.create_group("entry")
        geometry_group = h5.require_group("/entry/albis/geometry")
        geometry_group.attrs["schema_version"] = 1
        geometry_group.create_dataset(
            "json",
            data=json.dumps(geometry_payload) if isinstance(geometry_payload, dict) else str(geometry_payload),
            dtype=h5py.string_dtype(encoding="utf-8"),
        )
        if source_file is not None:
            h5.attrs["source_file"] = str(source_file)


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
    assert payload["panels"][1]["raw_offset_px"] == pytest.approx([0, 212])


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


def test_image_geometry_endpoint_allows_manual_override_for_tiff(tmp_path: Path) -> None:
    client = TestClient(app)
    image_path = tmp_path / "sum_0001.tiff"
    geometry_path = tmp_path / "imported.expt"
    _touch(image_path)
    _write_geometry(geometry_path)

    response = client.get(
        "/api/image/geometry",
        params={"file": str(image_path), "geometry_file": str(geometry_path)},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "geometry"
    assert payload["source"].endswith("imported.expt")
    assert len(payload["panels"]) == 2


def test_image_geometry_endpoint_allows_manual_override_for_hdf5(tmp_path: Path) -> None:
    client = TestClient(app)
    image_path = tmp_path / "sum_0001.h5"
    geometry_path = tmp_path / "imported.expt"
    _touch(image_path)
    _write_geometry(geometry_path)

    response = client.get(
        "/api/image/geometry",
        params={"file": str(image_path), "geometry_file": str(geometry_path)},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "geometry"
    assert payload["source"].endswith("imported.expt")
    assert len(payload["panels"]) == 2


def test_image_geometry_endpoint_reads_embedded_geometry_from_hdf5(tmp_path: Path) -> None:
    client = TestClient(app)
    image_path = tmp_path / "sum_0001.h5"
    geometry_payload = {
        "mode": "geometry",
        "detector": "pilatus-12m-dls-cshape",
        "source": "P12M_geometry/imported.expt",
        "panels": [
            {
                "name": "row-00",
                "origin_mm": [-184.9, -245.0, 250.13],
                "fast_axis": [1.0, 0.0, 0.0],
                "slow_axis": [0.0, 0.0, 1.0],
                "pixel_size_mm": [0.172, 0.172],
                "image_size_px": [2463, 195],
                "raw_offset_px": [3.4, -5.2],
            }
        ],
    }
    _write_h5_embedded_geometry(image_path, geometry_payload)

    response = client.get("/api/image/geometry", params={"file": str(image_path)})

    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "geometry"
    assert payload["source"] == "embedded HDF5 geometry (from imported.expt)"
    assert payload["panels"][0]["raw_offset_px"] == [3.4, -5.2]


def test_image_geometry_endpoint_falls_back_from_invalid_embedded_hdf5_geometry_to_source(
    tmp_path: Path,
) -> None:
    client = TestClient(app)
    image_path = tmp_path / "sum_0001.h5"
    source_path = tmp_path / "scan_0001.cbf"
    geometry_dir = tmp_path / "P12M_geometry"
    geometry_dir.mkdir()
    _write_cbf(source_path, P12M_HEADER)
    _write_geometry(geometry_dir / "imported.expt")
    _write_h5_embedded_geometry(image_path, "not-json", source_file=source_path)

    response = client.get("/api/image/geometry", params={"file": str(image_path)})

    assert response.status_code == 200
    payload = response.json()
    assert payload["mode"] == "geometry"
    assert payload["source"].endswith("P12M_geometry/imported.expt")
    assert len(payload["panels"]) == 2


def test_image_geometry_endpoint_falls_back_to_planar_for_missing_manual_override(
    tmp_path: Path,
) -> None:
    client = TestClient(app)
    image_path = tmp_path / "sum_0001.h5"
    _touch(image_path)

    response = client.get(
        "/api/image/geometry",
        params={"file": str(image_path), "geometry_file": "missing/imported.expt"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "mode": "planar",
        "detector": "",
        "source": "",
        "panels": [],
    }


def test_image_geometry_endpoint_rejects_non_expt_override_files(tmp_path: Path) -> None:
    client = TestClient(app)
    image_path = tmp_path / "sum_0001.tiff"
    geometry_path = tmp_path / "geometry.txt"
    _touch(image_path)
    geometry_path.write_text("not-an-experiment", encoding="utf-8")

    response = client.get(
        "/api/image/geometry",
        params={"file": str(image_path), "geometry_file": str(geometry_path)},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "Geometry override must be a DIALS .expt file"
