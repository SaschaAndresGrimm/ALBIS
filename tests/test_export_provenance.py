"""Cover what an exported file says about where it came from.

An export is not a copy of the detector's output: the dtype is widened, masked
gaps become -1 and bad or saturated pixels become -2. Those are the right
conventions, but a mini-CBF header that declares `SLS_1.0` and lists detector,
wavelength and distance, while saying nothing about ALBIS, reads to XDS or DIALS
as genuine detector output with values ALBIS chose. For software that asks to be
cited when it contributed to a result, that is the wrong default.
"""

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

from backend.image_formats import producer_string  # noqa: E402
from backend.version import ALBIS_VERSION  # noqa: E402


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


def _export(source: Path, out_dir: Path, output_format: str, **overrides: Any) -> list[str]:
    client = _client()
    request = {
        "file": str(source),
        "dataset": "/entry/data/data",
        "format": output_format,
        "output_dir": str(out_dir),
        "frame_mode": "all",
        "threshold_mode": "current",
    }
    request.update(overrides)
    start = client.post("/api/export/data/start", json=request)
    assert start.status_code == 200, start.text
    job = _wait_for_job(client, start.json()["job_id"])
    assert job["status"] == "done", job
    return list(job["outputs"])


@pytest.fixture
def source_stack(tmp_path: Path) -> Path:
    source = tmp_path / "series_master.h5"
    data = np.arange(3 * 2 * 4, dtype=np.uint32).reshape(3, 2, 4)
    with h5py.File(source, "w") as h5:
        h5.create_dataset("/entry/data/data", data=data)
    return source


def _cbf_header(path: str) -> str:
    image = CbfImage()
    image.read(path)
    return str(image.header.get("_array_data.header_contents", ""))


def test_producer_string_names_the_running_build() -> None:
    text = producer_string()

    assert text.startswith("ALBIS ")
    assert ALBIS_VERSION in text


def test_an_exported_cbf_says_albis_produced_it(source_stack: Path, tmp_path: Path) -> None:
    outputs = _export(source_stack, tmp_path / "cbf", "cbf")

    header = _cbf_header(outputs[0])

    assert producer_string() in header
    assert "derived data, not raw detector output" in header


def test_an_exported_cbf_names_the_frame_it_came_from(source_stack: Path, tmp_path: Path) -> None:
    outputs = _export(source_stack, tmp_path / "cbf", "cbf")

    assert len(outputs) == 3
    first = _cbf_header(outputs[0])
    last = _cbf_header(outputs[2])

    assert "series_master.h5" in first
    assert "/entry/data/data" in first
    assert "frame 1/3" in first
    assert "frame 3/3" in last


def test_an_exported_cbf_states_the_pixel_substitutions(source_stack: Path, tmp_path: Path) -> None:
    """The values are not the detector's, and the file has to say which ones."""
    outputs = _export(source_stack, tmp_path / "cbf", "cbf")

    header = _cbf_header(outputs[0])

    assert "masked gaps = -1" in header
    assert "bad or saturated = -2" in header


def test_provenance_does_not_replace_the_detector_metadata(tmp_path: Path) -> None:
    source = tmp_path / "detector.h5"
    with h5py.File(source, "w") as h5:
        h5.create_dataset("/entry/data/data", data=np.zeros((1, 2, 2), dtype=np.uint32))
        group = h5.require_group("/entry/instrument/detector")
        group.create_dataset(
            "description", data="EIGER2 CdTe 1M", dtype=h5py.string_dtype(encoding="utf-8")
        )
        distance = group.create_dataset("detector_distance", data=0.15)
        distance.attrs["units"] = "m"

    outputs = _export(source, tmp_path / "cbf", "cbf")
    header = _cbf_header(outputs[0])

    assert "EIGER2 CdTe 1M" in header
    assert "Detector_distance" in header
    assert producer_string() in header


def test_an_exported_tiff_carries_software_and_description(
    source_stack: Path, tmp_path: Path
) -> None:
    outputs = _export(source_stack, tmp_path / "tiff", "tiff")

    with tifffile.TiffFile(outputs[0]) as tiff:
        tags = tiff.pages[0].tags
        software = str(tags["Software"].value)
        description = str(tags["ImageDescription"].value)

    assert software == producer_string()
    assert "derived data, not raw detector output" in description
    assert "series_master.h5" in description
    assert "frame 1/3" in description


def test_a_multi_threshold_export_names_the_threshold(tmp_path: Path) -> None:
    source = tmp_path / "multi.h5"
    data = np.zeros((2, 2, 2, 2), dtype=np.uint32)
    with h5py.File(source, "w") as h5:
        h5.create_dataset("/entry/data/data", data=data)

    outputs = _export(source, tmp_path / "cbf", "cbf", frame_mode="current", threshold_mode="all")

    headers = [_cbf_header(path) for path in outputs]

    assert any("threshold 1/2" in header for header in headers)
    assert any("threshold 2/2" in header for header in headers)


def test_a_written_file_with_no_metadata_at_all_still_names_albis(tmp_path: Path) -> None:
    """Series summing writes TIFFs with no source metadata; those are derived too."""
    from backend.image_formats import _write_cbf, _write_tiff

    frame = np.zeros((4, 4), dtype=np.int32)
    cbf_path = tmp_path / "bare.cbf"
    tiff_path = tmp_path / "bare.tiff"

    _write_cbf(cbf_path, frame)
    _write_tiff(tiff_path, frame)

    assert producer_string() in _cbf_header(str(cbf_path))
    with tifffile.TiffFile(tiff_path) as tiff:
        assert str(tiff.pages[0].tags["Software"].value) == producer_string()


def test_exported_pixels_are_unchanged_by_the_header_work(
    source_stack: Path, tmp_path: Path
) -> None:
    """Provenance is a header change and must stay one."""
    outputs = _export(source_stack, tmp_path / "tiff", "tiff")
    expected = np.arange(3 * 2 * 4, dtype=np.uint32).reshape(3, 2, 4)

    for index, path in enumerate(outputs):
        assert np.array_equal(tifffile.imread(path), expected[index].astype(np.int32))
