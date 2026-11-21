"""Export Data writes what the viewer's Sum shows: windows of frames as one image.

With Sum 10 on, Current frame used to write the single frame while the screen
showed ten added up. What must hold: the counts are the window's sum, gaps and
defective pixels stay -1 and -2, the header says what the image is made of, a
range is one file per window with each frame in one file only, and a sum above
one frame's count cutoff is not marked saturated.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import numpy as np
import pytest
from fastapi.testclient import TestClient

from backend.services.frame_window import FrameCombine

h5py = pytest.importorskip("h5py")
tifffile = pytest.importorskip("tifffile")
fabio = pytest.importorskip("fabio")


def _client() -> TestClient:
    from backend.app import app

    return TestClient(app, base_url="http://localhost")


def _export(client: TestClient, payload: dict[str, Any]) -> dict[str, Any]:
    start = client.post("/api/export/data/start", json=payload)
    assert start.status_code == 200, start.text
    for _ in range(200):
        job = client.get(
            "/api/export/data/status", params={"job_id": start.json()["job_id"]}
        ).json()
        if job["status"] not in {"queued", "running"}:
            assert job["status"] == "done", job
            return job
        time.sleep(0.05)
    raise AssertionError("Data export job did not finish")


@pytest.fixture()
def stack(tmp_path: Path) -> tuple[Path, np.ndarray]:
    data = np.stack([np.full((2, 3), 10 * (i + 1), dtype=np.uint32) for i in range(5)])
    mask = np.zeros((2, 3), dtype=np.uint32)
    mask[0, 0] = 1  # gap
    mask[0, 1] = 0x10  # defective
    source = tmp_path / "stack.h5"
    with h5py.File(source, "w") as h5:
        h5.create_dataset("/entry/data/data", data=data)
        h5.create_dataset("/entry/instrument/detector/detectorSpecific/pixel_mask", data=mask)
        h5.create_dataset("/entry/instrument/detector/detectorSpecific/saturation_value", data=50)
    return source, data


def _cbf_header(path: str) -> str:
    return Path(path).read_bytes().split(b"_array_data.data")[0].decode("latin-1")


def test_the_current_frame_with_a_sum_is_the_window_added_up(stack, tmp_path: Path) -> None:
    source, data = stack
    job = _export(
        _client(),
        {
            "file": str(source),
            "dataset": "/entry/data/data",
            "format": "cbf",
            "output_dir": str(tmp_path / "out"),
            "frame_mode": "current",
            "frame_start": 2,
            "combine_count": 3,
        },
    )
    assert len(job["outputs"]) == 1
    assert "_sum3_" in Path(job["outputs"][0]).name
    arr = fabio.open(job["outputs"][0]).data
    # Frames 2-4: 20 + 30 + 40, over one frame's cutoff of 50 and still counts.
    assert arr[1, 2] == 90
    assert arr[0, 0] == -1
    assert arr[0, 1] == -2
    assert "sum of 3 frames 2-4" in _cbf_header(job["outputs"][0])


def test_a_range_with_a_sum_is_one_file_per_window(stack, tmp_path: Path) -> None:
    source, data = stack
    job = _export(
        _client(),
        {
            "file": str(source),
            "dataset": "/entry/data/data",
            "format": "tiff",
            "output_dir": str(tmp_path / "out"),
            "frame_mode": "all",
            "combine_count": 2,
            "combine_op": "mean",
        },
    )
    outputs = sorted(job["outputs"])
    assert len(outputs) == 3  # frames 1-2, 3-4, and 5 alone
    means = [float(tifffile.imread(path)[1, 2]) for path in outputs]
    assert means == pytest.approx([15.0, 35.0, 50.0])


def test_without_a_sum_the_export_writes_the_frames_themselves(stack, tmp_path: Path) -> None:
    source, _data = stack
    job = _export(
        _client(),
        {
            "file": str(source),
            "dataset": "/entry/data/data",
            "format": "cbf",
            "output_dir": str(tmp_path / "out"),
            "frame_mode": "range",
            "frame_start": 1,
            "frame_end": 2,
        },
    )
    assert len(job["outputs"]) == 2
    assert "Combined" not in _cbf_header(job["outputs"][0])


def test_a_cbf_series_sums_its_files_and_keeps_their_markers(tmp_path: Path) -> None:
    from backend.image_formats import _write_cbf

    paths = []
    for i in range(4):
        arr = np.full((2, 3), i + 1, dtype=np.int32)
        arr[0, 0] = -1
        if i == 1:
            arr[1, 1] = -2
        path = tmp_path / f"scan_{i + 1:05d}.cbf"
        _write_cbf(path, arr)
        paths.append(path)
    job = _export(
        _client(),
        {
            "file": str(paths[0]),
            "format": "cbf",
            "output_dir": str(tmp_path / "out"),
            "frame_mode": "current",
            "frame_start": 1,
            "combine_count": 3,
        },
    )
    arr = fabio.open(job["outputs"][0]).data
    assert arr[1, 2] == 1 + 2 + 3
    assert arr[0, 0] == -1
    assert arr[1, 1] == -2
    assert "sum of 3 frames 1-3" in _cbf_header(job["outputs"][0])


@pytest.mark.parametrize(
    ("frames", "total", "mode", "expected"),
    [
        ([4], 10, "current", [(4, 5)]),
        ([8], 10, "current", [(8, 2)]),
        (list(range(2, 9)), 10, "range", [(2, 5), (7, 2)]),
    ],
)
def test_the_windows_cover_each_frame_once(frames, total, mode, expected) -> None:
    assert FrameCombine(5).windows(frames, total, mode) == expected


@pytest.mark.parametrize("count", [0, 1001])
def test_a_window_outside_the_limits_is_refused(count) -> None:
    response = _client().post(
        "/api/export/data/start", json={"file": "x.h5", "combine_count": count}
    )
    assert response.status_code == 422
