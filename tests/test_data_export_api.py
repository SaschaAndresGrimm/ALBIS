from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import numpy as np
import pytest
from fastapi.testclient import TestClient

h5py = pytest.importorskip("h5py")
tifffile = pytest.importorskip("tifffile")
CbfImage = pytest.importorskip("fabio.cbfimage").CbfImage
pytest.importorskip("python_multipart")


def _client() -> TestClient:
    from backend.app import app

    return TestClient(app)


def _wait_for_job(client: TestClient, job_id: str) -> dict[str, Any]:
    for _ in range(100):
        response = client.get("/api/export/data/status", params={"job_id": job_id})
        assert response.status_code == 200
        payload = response.json()
        if payload["status"] not in {"queued", "running"}:
            return payload
        time.sleep(0.05)
    raise AssertionError("Data export job did not finish")


def test_data_export_hdf5_range_to_tiff(tmp_path: Path) -> None:
    source = tmp_path / "source.h5"
    out_dir = tmp_path / "converted"
    data = np.arange(3 * 2 * 4, dtype=np.int32).reshape(3, 2, 4)
    mask = np.zeros((2, 4), dtype=np.uint32)
    mask[0, 0] = 1
    mask[0, 1] = 0x10
    with h5py.File(source, "w") as h5:
        h5.create_dataset("/entry/data/data", data=data)
        h5.create_dataset("/entry/instrument/detector/detectorSpecific/pixel_mask", data=mask)

    client = _client()
    start = client.post(
        "/api/export/data/start",
        json={
            "file": str(source),
            "dataset": "/entry/data/data",
            "format": "tiff",
            "output_dir": str(out_dir),
            "frame_mode": "range",
            "frame_start": 2,
            "frame_end": 3,
            "threshold_mode": "current",
        },
    )

    assert start.status_code == 200
    job = _wait_for_job(client, start.json()["job_id"])
    assert job["status"] == "done"
    assert len(job["outputs"]) == 2
    first = tifffile.imread(job["outputs"][0])
    second = tifffile.imread(job["outputs"][1])
    assert np.issubdtype(first.dtype, np.signedinteger)
    expected_first = data[1].copy()
    expected_first[0, 0] = -1
    expected_first[0, 1] = -2
    expected_second = data[2].copy()
    expected_second[0, 0] = -1
    expected_second[0, 1] = -2
    assert np.array_equal(first, expected_first)
    assert np.array_equal(second, expected_second)


def test_data_export_hdf5_4d_all_thresholds_to_cbf(tmp_path: Path) -> None:
    source = tmp_path / "thresholds.h5"
    out_dir = tmp_path / "cbf"
    data = np.arange(2 * 2 * 3 * 4, dtype=np.int32).reshape(2, 2, 3, 4)
    with h5py.File(source, "w") as h5:
        h5.create_dataset("/entry/data/data", data=data)

    client = _client()
    start = client.post(
        "/api/export/data/start",
        json={
            "file": str(source),
            "dataset": "/entry/data/data",
            "format": "cbf",
            "output_dir": str(out_dir),
            "output_prefix": "threshold_export",
            "frame_mode": "current",
            "frame_start": 2,
            "threshold_mode": "all",
        },
    )

    assert start.status_code == 200
    job = _wait_for_job(client, start.json()["job_id"])
    assert job["status"] == "done"
    outputs = [Path(path) for path in job["outputs"]]
    assert len(outputs) == 2
    assert outputs[0].name == "threshold_export_f000002_thr01.cbf"
    assert outputs[1].name == "threshold_export_f000002_thr02.cbf"
    assert np.array_equal(CbfImage().read(str(outputs[0])).data, data[1, 0])
    assert np.array_equal(CbfImage().read(str(outputs[1])).data, data[1, 1])


def test_data_export_tiff_series_to_cbf(tmp_path: Path) -> None:
    frame_a = np.arange(12, dtype=np.uint16).reshape(3, 4)
    frame_b = frame_a + 100
    source_a = tmp_path / "scan_0001.tiff"
    source_b = tmp_path / "scan_0002.tiff"
    out_dir = tmp_path / "series_cbf"
    tifffile.imwrite(source_a, frame_a)
    tifffile.imwrite(source_b, frame_b)

    client = _client()
    start = client.post(
        "/api/export/data/start",
        json={
            "file": str(source_b),
            "format": "cbf",
            "output_dir": str(out_dir),
            "output_prefix": "scan",
            "frame_mode": "all",
        },
    )

    assert start.status_code == 200
    job = _wait_for_job(client, start.json()["job_id"])
    assert job["status"] == "done"
    outputs = [Path(path) for path in job["outputs"]]
    assert len(outputs) == 2
    assert outputs[0].name == "scan_f000001.cbf"
    assert outputs[1].name == "scan_f000002.cbf"
    assert np.array_equal(CbfImage().read(str(outputs[0])).data, frame_a)
    assert np.array_equal(CbfImage().read(str(outputs[1])).data, frame_b)
