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
    # The number last, the threshold before it: each threshold its own series.
    assert outputs[0].name == "threshold_export_thr1_00002.cbf"
    assert outputs[1].name == "threshold_export_thr2_00002.cbf"
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
    # Each frame keeps its source's own number.
    assert outputs[0].name == "scan_00001.cbf"
    assert outputs[1].name == "scan_00002.cbf"
    assert np.array_equal(CbfImage().read(str(outputs[0])).data, frame_a)
    assert np.array_equal(CbfImage().read(str(outputs[1])).data, frame_b)


# ---- Live frames: sent by the browser from its live history -----------------


def _post_live_frame(client: TestClient, data: np.ndarray, config: dict[str, Any]) -> Any:
    import json

    meta = {"dtype": data.dtype.str, "shape": list(data.shape), **config}
    return client.post(
        "/api/export/data/frame",
        data={"meta": json.dumps(meta)},
        files={"pixels": ("frame.bin", data.tobytes(), "application/octet-stream")},
    )


def test_live_frame_is_written_with_its_metadata_in_the_header(tmp_path: Path) -> None:
    client = _client()
    data = np.arange(8, dtype=np.uint32).reshape(2, 4)
    data[1, 3] = np.iinfo(np.uint32).max
    response = _post_live_frame(
        client,
        data,
        {
            "name": "monitor s189/i0009",
            "format": "cbf",
            "output_dir": str(tmp_path),
            "overwrite": False,
            "metadata": {
                "source_name": "SIMPLON monitor (192.168.1.10)",
                "incident_energy_ev": 8040,
                "wavelength_a": 1.5421,
                "detector_distance_m": 0.15,
                "beam_center_x_px": 512.5,
                "beam_center_y_px": "600",
                "series_number": 189,
                "image_number": 9,
                "image_datetime": "2026-10-12T10:41:14",
                "threshold_energy_ev": 4020,
                "not_a_header_field": "dropped",
            },
        },
    )
    assert response.status_code == 200, response.text
    path = Path(response.json()["path"])
    # The name is made safe for a file system.
    assert path.name == "monitor_s189_i0009.cbf"
    image = CbfImage()
    image.read(str(path))
    assert image.data.tolist() == [[0, 1, 2, 3], [4, 5, 6, -2]]
    header = image.header["_array_data.header_contents"]
    for line in (
        "# Incident_energy 8040 eV",
        "# Wavelength 1.5421 A",
        "# Detector_distance 0.15 m",
        "# Beam_xy (512.5, 600) pixels",
        "# Threshold_setting: 4020 eV",
        "# Source: SIMPLON monitor (192.168.1.10)",
    ):
        assert line in header
    assert "not_a_header_field" not in header and "dropped" not in header


def test_live_frame_keeps_an_existing_file_unless_overwrite(tmp_path: Path) -> None:
    client = _client()
    data = np.ones((3, 3), dtype="<f4")
    config = {"name": "live", "format": "tiff", "output_dir": str(tmp_path)}
    first = Path(_post_live_frame(client, data, {**config, "overwrite": False}).json()["path"])
    second = Path(_post_live_frame(client, data, {**config, "overwrite": False}).json()["path"])
    third = Path(_post_live_frame(client, data, {**config, "overwrite": True}).json()["path"])
    assert first.name == "live.tiff"
    assert second.name == "live_2.tiff"
    assert third == first
    # A numbered frame keeps its number last: the run goes before it.
    numbered = {**config, "name": "simplon_monitor_s189_00009"}
    a = Path(_post_live_frame(client, data, {**numbered, "overwrite": False}).json()["path"])
    b = Path(_post_live_frame(client, data, {**numbered, "overwrite": False}).json()["path"])
    assert (a.name, b.name) == (
        "simplon_monitor_s189_00009.tiff",
        "simplon_monitor_s189_2_00009.tiff",
    )
    assert tifffile.imread(str(first)).tolist() == [[1, 1, 1]] * 3


@pytest.mark.parametrize(
    ("dtype", "shape", "body_bytes", "detail"),
    [
        ("<u4", [2, 4], 31, "Pixel data does not match"),
        ("<c8", [2, 2], 32, "Unsupported pixel type"),
        ("nonsense", [2, 2], 16, "Unknown pixel type"),
        ("<u2", [2, 2, 2], 16, "A frame must be 2D"),
        ("<u2", "2x2", 8, "A frame must be 2D"),
    ],
)
def test_live_frame_refuses_what_does_not_add_up(
    tmp_path: Path, dtype: str, shape: Any, body_bytes: int, detail: str
) -> None:
    import json

    client = _client()
    meta = {
        "dtype": dtype,
        "shape": shape,
        "name": "x",
        "format": "tiff",
        "output_dir": str(tmp_path),
    }
    response = client.post(
        "/api/export/data/frame",
        data={"meta": json.dumps(meta)},
        files={"pixels": ("frame.bin", b"\x00" * body_bytes, "application/octet-stream")},
    )
    assert response.status_code == 400
    assert detail in response.json()["detail"]
    assert not any(tmp_path.iterdir())


def test_live_frame_refuses_invalid_meta(tmp_path: Path) -> None:
    client = _client()
    response = client.post(
        "/api/export/data/frame",
        data={"meta": "{not json"},
        files={"pixels": ("frame.bin", b"\x00" * 8, "application/octet-stream")},
    )
    assert response.status_code == 400


def test_live_frame_header_leaves_out_what_is_not_known(tmp_path: Path) -> None:
    """A live source reports unknowns as 0; the header states nothing for them."""
    client = _client()
    data = np.zeros((2, 2), dtype=np.uint16)
    response = _post_live_frame(
        client,
        data,
        {
            "name": "x",
            "format": "cbf",
            "output_dir": str(tmp_path),
            "metadata": {
                "incident_energy_ev": 0,
                "detector_distance_m": 0,
                "pixel_size_x_m": 0,
                "pixel_size_y_m": 0,
                "beam_center_x_px": 0,
                "beam_center_y_px": 0,
                "source_name": "SIMPLON monitor",
            },
        },
    )
    image = CbfImage()
    image.read(response.json()["path"])
    header = image.header["_array_data.header_contents"]
    for absent in ("Incident_energy", "Detector_distance", "Pixel_size", "Beam_xy"):
        assert absent not in header
    assert "# Source: SIMPLON monitor" in header


def test_export_names_read_back_as_a_series(tmp_path: Path) -> None:
    """The number last, as on a detector's images, so the export is one series."""
    from backend.image_formats import _resolve_series_files

    source = tmp_path / "series_190_master.h5"
    data = np.arange(3 * 2 * 2 * 2, dtype=np.uint32).reshape(3, 2, 2, 2)
    with h5py.File(source, "w") as h5:
        h5.create_dataset("/entry/data/data", data=data)
    client = _client()
    payload = {
        "file": str(source),
        "dataset": "/entry/data/data",
        "format": "cbf",
        "output_dir": str(tmp_path / "out"),
        "frame_mode": "all",
        "threshold_mode": "all",
        "overwrite": False,
    }
    first = _wait_for_job(
        client, client.post("/api/export/data/start", json=payload).json()["job_id"]
    )
    names = sorted(Path(p).name for p in first["outputs"])
    # Default name: the master file's without _master, the standard dataset left out.
    assert names == [f"series_190_thr{t}_{n:05d}.cbf" for t in (1, 2) for n in (1, 2, 3)]
    files, _ = _resolve_series_files(tmp_path / "out" / "series_190_thr2_00002.cbf")
    assert [f.name for f in files] == [
        "series_190_thr2_00001.cbf",
        "series_190_thr2_00002.cbf",
        "series_190_thr2_00003.cbf",
    ]
    # Again, overwrite off: a new name for the whole export, not a counter per file.
    second = _wait_for_job(
        client, client.post("/api/export/data/start", json=payload).json()["job_id"]
    )
    assert sorted(Path(p).name for p in second["outputs"]) == [
        f"series_190_2_thr{t}_{n:05d}.cbf" for t in (1, 2) for n in (1, 2, 3)
    ]


# ---- Inpainting: gaps and defective pixels filled instead of -1/-2 ----------


def _export(client: TestClient, payload: dict[str, Any]) -> list[Path]:
    response = client.post("/api/export/data/start", json=payload)
    assert response.status_code == 200, response.text
    job = _wait_for_job(client, response.json()["job_id"])
    assert job["status"] == "done", job
    return [Path(path) for path in job["outputs"]]


def test_inpainted_export_fills_gaps_and_defective_pixels_and_says_so(tmp_path: Path) -> None:
    source = tmp_path / "flat.h5"
    data = np.tile(np.arange(8, dtype=np.uint32) * 10, (1, 4, 1))
    data[0, 0, 7] = np.iinfo(np.uint32).max  # saturated: kept unless asked
    mask = np.zeros((4, 8), dtype=np.uint32)
    mask[:, 3:5] = 1  # a gap two pixels wide
    mask[2, 1] = 0b100  # a defective pixel
    with h5py.File(source, "w") as h5:
        h5.create_dataset("/entry/data/data", data=data)
        h5.create_dataset("/entry/instrument/detector/detectorSpecific/pixel_mask", data=mask)
    client = _client()
    base = {"file": str(source), "dataset": "/entry/data/data", "format": "cbf"}

    plain = _export(client, {**base, "output_dir": str(tmp_path / "plain")})
    assert CbfImage().read(str(plain[0])).data[1].tolist() == [0, 10, 20, -1, -1, 50, 60, 70]

    out = _export(client, {**base, "output_dir": str(tmp_path / "filled"), "inpaint": "defects"})
    image = CbfImage()
    image.read(str(out[0]))
    # The gradient carries straight through the gap; the dead pixel is its neighbours'.
    assert image.data[1].tolist() == [0, 10, 20, 30, 40, 50, 60, 70]
    assert image.data[2, 1] == 10
    assert image.data[0, 7] == -2
    header = image.header["_array_data.header_contents"]
    assert "gaps and defective pixels inpainted by ALBIS" in header

    every = _export(
        client,
        {**base, "output_dir": str(tmp_path / "all"), "inpaint": "saturated", "format": "tiff"},
    )
    filled = tifffile.imread(every[0])
    # A corner: between its row (60, 50) and its column (70 below).
    assert 55 <= filled[0, 7] <= 70
    assert "saturated pixels inpainted" in tifffile.TiffFile(every[0]).pages[0].description


def test_inpainted_export_reads_a_cbf_s_own_markers(tmp_path: Path) -> None:
    source = tmp_path / "scan_00001.cbf"
    frame = np.full((3, 5), 9, dtype=np.int32)
    frame[:, 2] = -1
    frame[0, 0] = -2
    image = CbfImage(data=frame)
    image.write(str(source))
    out = _export(
        _client(),
        {
            "file": str(source),
            "format": "cbf",
            "output_dir": str(tmp_path / "out"),
            "inpaint": "defects",
        },
    )
    assert np.all(CbfImage().read(str(out[0])).data == 9)


def test_inpaint_option_is_checked(tmp_path: Path) -> None:
    response = _client().post(
        "/api/export/data/start", json={"file": "x.cbf", "inpaint": "everything"}
    )
    assert response.status_code == 422


def test_live_frame_is_inpainted_with_the_mask_sent_along(tmp_path: Path) -> None:
    import json

    client = _client()
    data = np.tile(np.arange(6, dtype=np.uint32) * 2, (3, 1))
    data[:, 2] = np.iinfo(np.uint32).max
    mask = np.zeros(data.shape, dtype="<u4")
    mask[:, 2] = 1
    meta = {
        "dtype": data.dtype.str,
        "shape": list(data.shape),
        "name": "live",
        "format": "cbf",
        "output_dir": str(tmp_path),
        "inpaint": "defects",
    }
    response = client.post(
        "/api/export/data/frame",
        data={"meta": json.dumps(meta)},
        files={
            "pixels": ("frame.bin", data.tobytes(), "application/octet-stream"),
            "mask": ("mask.bin", mask.tobytes(), "application/octet-stream"),
        },
    )
    assert response.status_code == 200, response.text
    assert CbfImage().read(response.json()["path"]).data[0].tolist() == [0, 2, 4, 6, 8, 10]
    short = client.post(
        "/api/export/data/frame",
        data={"meta": json.dumps(meta)},
        files={
            "pixels": ("frame.bin", data.tobytes(), "application/octet-stream"),
            "mask": ("mask.bin", b"\0" * 8, "application/octet-stream"),
        },
    )
    assert short.status_code == 400


def test_inpainted_export_fills_a_margin_around_flagged_pixels(tmp_path: Path) -> None:
    source = tmp_path / "flat.h5"
    data = np.full((1, 7, 7), 10, dtype=np.uint32)
    data[0, 3, 2:5] = 500  # the neighbours of a dead pixel, gone wrong too
    mask = np.zeros((7, 7), dtype=np.uint32)
    mask[3, 3] = 0b10
    with h5py.File(source, "w") as h5:
        h5.create_dataset("/entry/data/data", data=data)
        h5.create_dataset("/entry/instrument/detector/detectorSpecific/pixel_mask", data=mask)
    client = _client()
    base = {
        "file": str(source),
        "dataset": "/entry/data/data",
        "format": "cbf",
        "inpaint": "defects",
    }
    plain = CbfImage().read(str(_export(client, {**base, "output_dir": str(tmp_path / "a")})[0]))
    assert plain.data[3, 2] == 500
    out = _export(client, {**base, "output_dir": str(tmp_path / "b"), "inpaint_margin": 1})
    image = CbfImage()
    image.read(str(out[0]))
    assert np.all(image.data == 10)
    assert "and 1 px around them inpainted" in image.header["_array_data.header_contents"]
    too_wide = client.post("/api/export/data/start", json={**base, "inpaint_margin": 6})
    assert too_wide.status_code == 422


def test_inpainted_export_can_set_one_value_clipped_to_the_type(tmp_path: Path) -> None:
    source = tmp_path / "flat.h5"
    data = np.full((1, 4, 4), 10, dtype=np.uint16)
    mask = np.zeros((4, 4), dtype=np.uint32)
    mask[:, 1] = 1
    with h5py.File(source, "w") as h5:
        h5.create_dataset("/entry/data/data", data=data)
        h5.create_dataset("/entry/instrument/detector/detectorSpecific/pixel_mask", data=mask)
    client = _client()
    base = {
        "file": str(source),
        "dataset": "/entry/data/data",
        "format": "cbf",
        "inpaint": "defects",
        "inpaint_method": "fixed",
    }
    out = _export(client, {**base, "output_dir": str(tmp_path / "a"), "inpaint_value": 3})
    image = CbfImage()
    image.read(str(out[0]))
    assert image.data[:, 1].tolist() == [3, 3, 3, 3] and image.data[0, 0] == 10
    assert (
        "gaps and defective pixels set to 3 by ALBIS" in image.header["_array_data.header_contents"]
    )
    huge = _export(client, {**base, "output_dir": str(tmp_path / "b"), "inpaint_value": 1e9})
    assert CbfImage().read(str(huge[0])).data[0, 1] == 65535
    bad = client.post("/api/export/data/start", json={**base, "inpaint_method": "blur"})
    assert bad.status_code == 422


# ---- Flatfield: every frame divided by a flat --------------------------------


def _flat_source(tmp_path: Path) -> tuple[Path, Path]:
    flat = np.full((4, 6), 1000, dtype=np.uint32)
    flat[:, :3] = 500  # half the beam is half as bright
    flat[0, 5] = 0  # nothing to divide by
    flat_path = tmp_path / "flat.tiff"
    tifffile.imwrite(flat_path, flat)
    source = tmp_path / "scan.h5"
    data = np.zeros((1, 4, 6), dtype=np.uint32)
    data[0, :, :3] = 250
    data[0, :, 3:] = 500
    with h5py.File(source, "w") as h5:
        h5.create_dataset("/entry/data/data", data=data)
    return source, flat_path


def test_flatfielded_export_divides_by_the_flat(tmp_path: Path) -> None:
    source, flat_path = _flat_source(tmp_path)
    client = _client()
    base = {"file": str(source), "dataset": "/entry/data/data", "flatfield": str(flat_path)}
    out = _export(client, {**base, "format": "tiff", "output_dir": str(tmp_path / "t")})
    image = tifffile.imread(out[0])
    assert image.dtype == np.float32
    mean = (500 * 12 + 1000 * 11) / 23
    # Corrected, both halves are alike: half the counts under half the beam.
    np.testing.assert_allclose(image[1], [0.5 * mean] * 6, rtol=1e-5)
    assert image[0, 5] == -2  # nothing to divide by, not inpainted
    description = tifffile.TiffFile(out[0]).pages[0].description
    assert "# Flatfield: divided by flat.tiff (counts, times its mean) by ALBIS" in description

    transmission = _export(
        client,
        {
            **base,
            "format": "tiff",
            "flatfield_scale": "transmission",
            "inpaint": "defects",
            "output_dir": str(tmp_path / "tr"),
        },
    )
    filled = tifffile.imread(transmission[0])
    np.testing.assert_allclose(filled, 0.5, rtol=1e-5)  # the hole inpainted too

    counts_cbf = _export(client, {**base, "format": "cbf", "output_dir": str(tmp_path / "c")})
    assert CbfImage().read(str(counts_cbf[0])).data[1, 0] == round(0.5 * mean)


def test_flatfield_refusals(tmp_path: Path) -> None:
    source, flat_path = _flat_source(tmp_path)
    client = _client()
    base = {"file": str(source), "dataset": "/entry/data/data", "output_dir": str(tmp_path / "o")}
    cbf = client.post(
        "/api/export/data/start",
        json={
            **base,
            "format": "cbf",
            "flatfield": str(flat_path),
            "flatfield_scale": "transmission",
        },
    )
    assert _wait_for_job(client, cbf.json()["job_id"])["status"] == "error"
    small = tmp_path / "small.tiff"
    tifffile.imwrite(small, np.ones((2, 2), dtype=np.uint16))
    wrong = client.post("/api/export/data/start", json={**base, "flatfield": str(small)})
    job = _wait_for_job(client, wrong.json()["job_id"])
    assert job["status"] == "error" and "size" in str(job["error"])


def test_the_current_image_is_kept_as_a_flatfield(tmp_path: Path, monkeypatch) -> None:
    import json
    from dataclasses import replace

    from backend.app import data_export

    # The service's own data folder, not the real one.
    monkeypatch.setattr(data_export, "_deps", replace(data_export._deps, data_dir=tmp_path))
    client = _client()
    data = np.arange(12, dtype=np.uint16).reshape(3, 4) + 100
    response = client.post(
        "/api/flatfield/save",
        data={"meta": json.dumps({"dtype": data.dtype.str, "shape": [3, 4]})},
        files={"pixels": ("flat.bin", data.tobytes(), "application/octet-stream")},
    )
    assert response.status_code == 200, response.text
    path = Path(response.json()["path"])
    assert path.parent == tmp_path / "flatfields" and path.name.startswith("flat_")
    assert np.array_equal(tifffile.imread(path), data)
    bad = client.post(
        "/api/flatfield/save",
        data={"meta": json.dumps({"dtype": "<u2", "shape": [3, 5]})},
        files={"pixels": ("flat.bin", data.tobytes(), "application/octet-stream")},
    )
    assert bad.status_code == 400


def _unsigned_source(tmp_path: Path) -> Path:
    # Counts beyond 16 bits, a gap, a defective and a saturated pixel.
    data = np.zeros((1, 2, 4), dtype=np.uint32)
    data[0, 0] = [5, 65_534, 70_000, 9]
    data[0, 1] = [1, 2, 3, np.iinfo(np.uint32).max]
    mask = np.zeros((2, 4), dtype=np.uint32)
    mask[1, 0] = 1  # gap
    mask[1, 1] = 0b100  # defective
    source = tmp_path / "wide.h5"
    with h5py.File(source, "w") as h5:
        h5.create_dataset("/entry/data/data", data=data)
        h5.create_dataset("/entry/instrument/detector/detectorSpecific/pixel_mask", data=mask)
        _write_h5_dataset(
            h5, "/entry/instrument/detector/detectorSpecific/saturation_value", 1_000_000
        )
    return source


def test_unsigned_16_bit_export_marks_and_clips_and_says_so(tmp_path: Path) -> None:
    client = _client()
    base = {"file": str(_unsigned_source(tmp_path)), "dataset": "/entry/data/data"}
    response = client.post(
        "/api/export/data/start",
        json={**base, "format": "tiff", "data_type": "uint16", "output_dir": str(tmp_path / "u16")},
    )
    job = _wait_for_job(client, response.json()["job_id"])
    assert job["status"] == "done", job
    assert (job["clipped_pixels"], job["clipped_files"]) == (1, 1)
    assert "1 pixel(s) clipped in 1 file(s)" in job["message"]
    image = tifffile.imread(job["outputs"][0])
    assert image.dtype == np.uint16
    # 70 000 clipped one below the marker, never wrapped to 4464; gap,
    # defective and saturated pixels all the marker.
    assert image.tolist() == [[5, 65_534, 65_534, 9], [65_535, 65_535, 3, 65_535]]
    description = tifffile.TiffFile(job["outputs"][0]).pages[0].description
    assert "# Data type: unsigned 16-bit" in description
    assert "clipped (1 pixels)" in description
    assert "# Pixel substitutions: masked gaps, bad or saturated = 65535" in description
    # A clipped count is an overload from the header's point of view.
    assert "# Count_cutoff 65534 counts" in description


def test_unsigned_32_bit_export_keeps_every_count(tmp_path: Path) -> None:
    out = _export(
        _client(),
        {
            "file": str(_unsigned_source(tmp_path)),
            "dataset": "/entry/data/data",
            "format": "tiff",
            "data_type": "uint32",
            "output_dir": str(tmp_path / "u32"),
        },
    )
    image = tifffile.imread(out[0])
    top = np.iinfo(np.uint32).max
    assert image.dtype == np.uint32
    assert image.tolist() == [[5, 65_534, 70_000, 9], [top, top, 3, top]]
    description = tifffile.TiffFile(out[0]).pages[0].description
    assert "# Count_cutoff 1000000 counts" in description


def test_unsigned_export_rounds_a_flatfield_and_refuses_what_it_cannot_hold(
    tmp_path: Path,
) -> None:
    source, flat_path = _flat_source(tmp_path)
    client = _client()
    base = {"file": str(source), "dataset": "/entry/data/data", "flatfield": str(flat_path)}
    out = _export(
        client,
        {**base, "format": "tiff", "data_type": "uint16", "output_dir": str(tmp_path / "f")},
    )
    image = tifffile.imread(out[0])
    mean = (500 * 12 + 1000 * 11) / 23
    assert image.dtype == np.uint16
    assert image[1, 0] == round(0.5 * mean)
    assert image[0, 5] == 65_535  # nothing to divide by
    # CBF is signed; transmission needs floating point.
    cbf = client.post(
        "/api/export/data/start", json={**base, "format": "cbf", "data_type": "uint16"}
    )
    assert cbf.status_code == 400
    transmission = client.post(
        "/api/export/data/start",
        json={
            **base,
            "format": "tiff",
            "data_type": "uint16",
            "flatfield_scale": "transmission",
            "output_dir": str(tmp_path / "t"),
        },
    )
    job = _wait_for_job(client, transmission.json()["job_id"])
    assert job["status"] == "error" and "floating point" in str(job["error"])
    wrong = client.post("/api/export/data/start", json={**base, "data_type": "int8"})
    assert wrong.status_code == 422


def test_live_frame_can_be_written_unsigned(tmp_path: Path) -> None:
    client = _client()
    data = np.array([[1, 80_000], [-1, 7]], dtype=np.int32)
    config = {"name": "live_u16", "format": "tiff", "output_dir": str(tmp_path)}
    response = _post_live_frame(client, data, {**config, "data_type": "uint16"})
    assert response.status_code == 200, response.text
    assert response.json()["clipped"] == 1
    image = tifffile.imread(response.json()["path"])
    assert image.dtype == np.uint16
    assert image.tolist() == [[1, 65_534], [65_535, 7]]
    cbf = _post_live_frame(client, data, {**config, "format": "cbf", "data_type": "uint16"})
    assert cbf.status_code == 400
    bad = _post_live_frame(client, data, {**config, "data_type": "float64"})
    assert bad.status_code == 400


def test_a_tiff_export_the_disk_cannot_hold_is_refused_before_it_starts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import shutil
    from collections import namedtuple

    usage = namedtuple("usage", "total used free")
    monkeypatch.setattr(shutil, "disk_usage", lambda _path: usage(10**9, 10**9, 1000))
    source = tmp_path / "scan.h5"
    with h5py.File(source, "w") as h5:
        h5.create_dataset("/entry/data/data", data=np.ones((3, 64, 64), dtype=np.uint32))
    client = _client()
    out_dir = tmp_path / "out"
    response = client.post(
        "/api/export/data/start",
        json={"file": str(source), "dataset": "/entry/data/data", "output_dir": str(out_dir)},
    )
    job = _wait_for_job(client, response.json()["job_id"])
    assert job["status"] == "error" and "Not enough disk space" in str(job["error"])
    assert not list(out_dir.glob("*.tiff"))


def _old_reads(path: Path) -> tuple[np.ndarray, dict[str, Any], str]:
    from backend import image_formats as fmt

    ext = fmt._image_ext_name(path.name)
    pixels = fmt._read_tiff(path) if ext in {".tif", ".tiff"} else fmt._read_cbf(path)
    return pixels, fmt._pilatus_meta_from_image(path), fmt._pilatus_header_text(path)


def test_one_read_gives_what_the_separate_reads_gave(tmp_path: Path) -> None:
    from backend import image_formats as fmt

    frame = (np.arange(48, dtype=np.int32) * 7).reshape(6, 8)
    metadata = {
        "detector_description": "PILATUS4 1M",
        "exposure_time_s": 0.5,
        "pixel_size_x_m": 75e-6,
        "wavelength_a": 1.0,
        "image_number": 7,
        "series_number": 3,
    }
    plain_tiff = tmp_path / "plain.tif"
    tifffile.imwrite(plain_tiff, frame.astype(np.uint32))
    written_tiff = tmp_path / "written.tiff"
    fmt._write_tiff(written_tiff, frame, metadata)
    written_cbf = tmp_path / "written.cbf"
    fmt._write_cbf(written_cbf, frame, metadata)
    plain_cbf = tmp_path / "plain.cbf"
    CbfImage(data=frame).write(str(plain_cbf))
    for path in (plain_tiff, written_tiff, written_cbf, plain_cbf):
        pixels, parsed, text = fmt._read_image_with_header(path)
        old_pixels, old_parsed, old_text = _old_reads(path)
        assert np.array_equal(pixels, old_pixels) and pixels.dtype == old_pixels.dtype, path
        assert parsed == old_parsed, path
        assert text == old_text, path
    assert fmt._read_image_with_header(written_tiff)[1]["exposure_time_s"] == 0.5


def test_frames_converted_side_by_side_land_in_order_each_in_its_own_file(
    tmp_path: Path,
) -> None:
    from backend.services import data_export as service

    assert service.EXPORT_WORKERS >= 1
    frames = 37  # more than the workers keep in flight
    data = np.stack([np.full((8, 9), index, dtype=np.uint32) for index in range(frames)])
    source = tmp_path / "many.h5"
    with h5py.File(source, "w") as h5:
        h5.create_dataset("/entry/data/data", data=data)
    out = _export(
        _client(),
        {
            "file": str(source),
            "dataset": "/entry/data/data",
            "output_dir": str(tmp_path / "h5out"),
            "inpaint": "defects",
        },
    )
    assert [path.name for path in out] == [f"many_{n:05d}.tiff" for n in range(1, frames + 1)]
    for index, path in enumerate(out):
        assert np.all(tifffile.imread(path) == index)
    # A series of single images: each read on a worker, same order.
    series = tmp_path / "series"
    series.mkdir()
    for index in range(frames):
        tifffile.imwrite(series / f"img_{index + 1:05d}.tif", data[index])
    out = _export(
        _client(),
        {"file": str(series / "img_00001.tif"), "output_dir": str(tmp_path / "tifout")},
    )
    assert [path.name for path in out] == [f"img_{n:05d}.tiff" for n in range(1, frames + 1)]
    for index, path in enumerate(out):
        assert np.all(tifffile.imread(path) == index)


def test_cancel_stops_a_parallel_export_early(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from backend.services.data_export import DataExportService

    slow_write = DataExportService._write_frame

    def write(self: DataExportService, **kwargs: Any) -> Any:
        time.sleep(0.02)
        return slow_write(self, **kwargs)

    monkeypatch.setattr(DataExportService, "_write_frame", write)
    source = tmp_path / "long.h5"
    with h5py.File(source, "w") as h5:
        h5.create_dataset("/entry/data/data", data=np.ones((400, 4, 4), dtype=np.uint32))
    client = _client()
    out_dir = tmp_path / "out"
    job_id = client.post(
        "/api/export/data/start",
        json={"file": str(source), "dataset": "/entry/data/data", "output_dir": str(out_dir)},
    ).json()["job_id"]
    for _ in range(200):
        if client.get("/api/export/data/status", params={"job_id": job_id}).json()["progress"]:
            break
        time.sleep(0.01)
    assert client.post("/api/export/data/cancel", json={"job_id": job_id}).json()["accepted"]
    job = _wait_for_job(client, job_id)
    assert job["status"] == "cancelled"
    assert 0 < len(list(out_dir.glob("*.tiff"))) < 400
