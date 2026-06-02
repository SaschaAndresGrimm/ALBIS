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


def _write_h5_dataset(h5: Any, path: str, data: Any, units: str | None = None) -> None:
    group_path, name = path.rsplit("/", 1)
    group = h5.require_group(group_path)
    dset = group.create_dataset(name, data=data)
    if units:
        dset.attrs["units"] = units


def _write_h5_text(h5: Any, path: str, text: str) -> None:
    group_path, name = path.rsplit("/", 1)
    group = h5.require_group(group_path)
    group.create_dataset(name, data=text, dtype=h5py.string_dtype(encoding="utf-8"))


def _add_export_header_metadata(h5: Any) -> None:
    _write_h5_text(h5, "/entry/instrument/detector/description", "EIGER2 4M")
    _write_h5_text(h5, "/entry/instrument/detector/detector_number", "E-32")
    _write_h5_text(h5, "/entry/instrument/detector/detectorSpecific/series_id", "series-abc")
    _write_h5_text(h5, "/entry/start_time", "2026-06-02T12:00:00Z")
    _write_h5_dataset(
        h5, "/entry/instrument/detector/detectorSpecific/series_number", np.asarray(42)
    )
    _write_h5_dataset(
        h5, "/entry/instrument/detector/detectorSpecific/image_nr_start", np.asarray(100)
    )
    _write_h5_dataset(h5, "/entry/instrument/detector/x_pixel_size", 75e-6, "m")
    _write_h5_dataset(h5, "/entry/instrument/detector/y_pixel_size", 75e-6, "m")
    _write_h5_dataset(h5, "/entry/instrument/detector/sensor_thickness", 70.0, "m")
    _write_h5_dataset(h5, "/entry/instrument/detector/count_time", 0.1, "s")
    _write_h5_dataset(h5, "/entry/instrument/detector/frame_time", 0.2, "s")
    _write_h5_dataset(h5, "/entry/instrument/detector/detectorSpecific/saturation_value", 999_999)
    _write_h5_dataset(h5, "/entry/sample/beam/incident_wavelength", 1.0332, "angstrom")
    _write_h5_dataset(h5, "/entry/instrument/detector/detectorSpecific/photon_energy", 12_000, "eV")
    _write_h5_dataset(h5, "/entry/instrument/detector/detector_distance", 0.25, "m")
    _write_h5_dataset(h5, "/entry/instrument/detector/beam_center_x", 123.4)
    _write_h5_dataset(h5, "/entry/instrument/detector/beam_center_y", 234.5)
    _write_h5_dataset(h5, "/entry/instrument/detector/threshold_energy", 6_000, "eV")
    _write_h5_dataset(h5, "/entry/sample/goniometer/omega", np.asarray([0.0, 0.1]), "deg")
    _write_h5_dataset(h5, "/entry/sample/goniometer/omega_range_average", 0.1, "deg")


def test_data_export_hdf5_range_to_tiff(tmp_path: Path) -> None:
    source = tmp_path / "source.h5"
    out_dir = tmp_path / "converted"
    data = np.arange(3 * 2 * 4, dtype=np.uint32).reshape(3, 2, 4)
    data[1, 1, 2] = np.iinfo(np.uint32).max
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
    assert first.dtype == np.dtype(np.int32)
    expected_first = data[1].astype(np.int64)
    expected_first[0, 0] = -1
    expected_first[0, 1] = -2
    expected_first[1, 2] = -2
    expected_second = data[2].astype(np.int64)
    expected_second[0, 0] = -1
    expected_second[0, 1] = -2
    assert np.array_equal(first, expected_first)
    assert np.array_equal(second, expected_second)


def test_data_export_hdf5_to_tiff_writes_dectris_header(tmp_path: Path) -> None:
    from backend.image_formats import _simplon_meta_from_tiff

    source = tmp_path / "source_header.h5"
    out_dir = tmp_path / "tiff_header"
    data = np.arange(2 * 2 * 3, dtype=np.int32).reshape(2, 2, 3)
    with h5py.File(source, "w") as h5:
        h5.create_dataset("/entry/data/data", data=data)
        _add_export_header_metadata(h5)

    client = _client()
    start = client.post(
        "/api/export/data/start",
        json={
            "file": str(source),
            "dataset": "/entry/data/data",
            "format": "tiff",
            "output_dir": str(out_dir),
            "frame_mode": "current",
            "frame_start": 2,
            "threshold_mode": "current",
        },
    )

    assert start.status_code == 200
    job = _wait_for_job(client, start.json()["job_id"])
    assert job["status"] == "done"
    output = Path(job["outputs"][0])
    with tifffile.TiffFile(output) as tiff:
        assert tiff.pages[0].tags.get(0xC7F8) is not None
        meta = _simplon_meta_from_tiff(tiff, raw=output.read_bytes())
    assert meta["series_unique_id"] == "series-abc"
    assert meta["series_number"] == 42
    assert meta["image_number"] == 101
    assert meta["image_datetime"] == "2026-06-02T12:00:00Z"
    assert meta["threshold_ids"] == [1]
    assert meta["threshold_energy_ev"] == pytest.approx(6_000)
    assert meta["exposure_time_s"] == pytest.approx(0.1)
    assert meta["energy_ev"] == pytest.approx(12_000)
    assert meta["wavelength_a"] == pytest.approx(1.0332)
    assert meta["distance_mm"] == pytest.approx(250.0)
    assert meta["beam_center_px"] == pytest.approx((123.4, 234.5))


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


def test_data_export_hdf5_to_cbf_writes_minicbf_header(tmp_path: Path) -> None:
    source = tmp_path / "source_minicbf.h5"
    out_dir = tmp_path / "cbf_header"
    data = np.arange(2 * 2 * 3, dtype=np.int32).reshape(2, 2, 3)
    with h5py.File(source, "w") as h5:
        h5.create_dataset("/entry/data/data", data=data)
        _add_export_header_metadata(h5)

    client = _client()
    start = client.post(
        "/api/export/data/start",
        json={
            "file": str(source),
            "dataset": "/entry/data/data",
            "format": "cbf",
            "output_dir": str(out_dir),
            "frame_mode": "current",
            "frame_start": 2,
            "threshold_mode": "current",
        },
    )

    assert start.status_code == 200
    job = _wait_for_job(client, start.json()["job_id"])
    assert job["status"] == "done"
    image = CbfImage().read(job["outputs"][0])
    header = image.header
    header_text = str(header.get("_array_data.header_contents") or "")
    assert header.get("_array_data.header_convention") == "SLS_1.0"
    assert "Detector: EIGER2 4M, S/N E-32" in header_text
    assert "Pixel_size 7.5e-05 m x 7.5e-05 m" in header_text
    assert "Silicon sensor, thickness 7e-05 m" in header_text
    assert "Exposure_time 0.1 s" in header_text
    assert "Exposure_period 0.2 s" in header_text
    assert "Count_cutoff 999999 counts" in header_text
    assert "Wavelength 1.0332 A" in header_text
    assert "Incident_energy 12000 eV" in header_text
    assert "Detector_distance 0.25 m" in header_text
    assert "Beam_xy (123.4, 234.5) pixels" in header_text
    assert "Start_angle 0.1 deg." in header_text
    assert "Angle_increment 0.1 deg." in header_text


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
