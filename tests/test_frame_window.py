"""Several consecutive frames shown as one: `GET /api/frame?count=N`.

The viewer's Sum control adds frames together so a weak signal shows. What
must hold: the counts add up exactly, the sum does not overflow the source
type, gap and defective pixels stay recognisable as such instead of turning
into huge counts, and the last window of a series shrinks instead of failing.
"""

from __future__ import annotations

from pathlib import Path

import h5py
import numpy as np
import pytest
from fastapi.testclient import TestClient

from backend.services.frame_window import (
    MAX_FRAME_WINDOW,
    frame_window_count,
    reduce_frame_window,
    reduce_frames,
    series_window_files,
)

GAP32 = 0xFFFFFFFF


# --------------------------------------------------------------------------
# The window and the reduction
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("total", "start", "count", "expected"),
    [
        (100, 0, 10, 10),
        (100, 95, 10, 5),  # the last window shrinks
        (100, 99, 10, 1),
        (5, 0, 1, 1),
        (5, 0, 1000, 5),
    ],
)
def test_the_window_is_cut_at_the_end_of_the_series(total, start, count, expected) -> None:
    assert frame_window_count(total, start, count) == expected


def test_a_sum_of_uint32_frames_stays_uint32_and_exact() -> None:
    frames = [np.full((2, 3), 1000 + i, dtype=np.uint32) for i in range(10)]
    out = reduce_frames(frames, "sum")
    assert out.dtype == np.uint32
    assert np.all(out == sum(1000 + i for i in range(10)))


def test_a_sum_that_outgrows_the_source_type_is_widened() -> None:
    """Two uint16 frames of 60000 make 120000, which uint16 cannot hold."""
    frames = [np.full((2, 2), 60000, dtype=np.uint16)] * 2
    out = reduce_frames(frames, "sum")
    assert out.dtype.itemsize >= 4
    assert np.all(out == 120000)


def test_a_gap_pixel_in_any_frame_stays_a_gap_in_the_sum() -> None:
    """Added up, a 0xFFFFFFFF gap would read as a bright count of billions."""
    a = np.array([[1, GAP32], [3, 4]], dtype=np.uint32)
    b = np.array([[GAP32, 2], [3, 4]], dtype=np.uint32)
    out = reduce_frames([a, b], "sum")
    assert out.dtype == np.uint32
    assert out[0, 0] == GAP32
    assert out[0, 1] == GAP32
    assert out[1, 0] == 6
    assert out[1, 1] == 8


def test_a_gap_stays_the_marker_of_a_widened_type() -> None:
    a = np.array([[60000, 65535]], dtype=np.uint16)
    out = reduce_frames([a, a], "sum")
    assert out[0, 0] == 120000
    assert out[0, 1] == np.iinfo(out.dtype).max


def test_a_mean_is_float_with_gaps_as_nan() -> None:
    a = np.array([[1, GAP32]], dtype=np.uint32)
    b = np.array([[2, 5]], dtype=np.uint32)
    out = reduce_frames([a, b], "mean")
    assert out.dtype == np.float32
    assert out[0, 0] == pytest.approx(1.5)
    assert np.isnan(out[0, 1])


def test_float_frames_add_as_floats() -> None:
    frames = [np.array([[0.5, np.nan]], dtype=np.float32)] * 4
    out = reduce_frames(frames, "sum")
    assert out.dtype == np.float32
    assert out[0, 0] == pytest.approx(2.0)
    assert np.isnan(out[0, 1])


def test_a_pilatus_gap_stays_minus_one_and_is_not_added() -> None:
    """A CBF marks gaps -1: ten of them added made -10, which no reader knows."""
    frames = [np.array([[-1, 7]], dtype=np.int32)] * 10
    out = reduce_frames(frames, "sum")
    assert out.tolist() == [[-1, 70]]


def test_a_pixel_defective_in_one_frame_is_marked_not_mixed_into_counts() -> None:
    """Defective (-2) in one frame only: its counts in the others are not a sum."""
    a = np.array([[5, 5, -1]], dtype=np.int32)
    b = np.array([[5, -2, -2]], dtype=np.int32)
    out = reduce_frames([a, b], "sum")
    assert out.tolist() == [[10, -2, -2]]


def test_a_negative_marker_is_nan_in_a_mean() -> None:
    out = reduce_frames([np.array([[-1, 4]], dtype=np.int32)] * 2, "mean")
    assert np.isnan(out[0, 0])
    assert out[0, 1] == pytest.approx(4.0)


def test_the_series_window_is_cut_at_the_end_and_skips_links(tmp_path: Path) -> None:
    files = []
    for i in range(5):
        path = tmp_path / f"img_{i:05d}.cbf"
        path.write_bytes(b"")
        files.append(path)
    link = tmp_path / "img_00005.cbf"
    link.symlink_to(tmp_path.parent)
    files.append(link)
    assert series_window_files(files, 1, 3) == files[1:4]
    assert series_window_files(files, 3, 10) == files[3:5]


def test_the_window_reads_the_frames_it_covers_and_no_others() -> None:
    read: list[int] = []

    def load(i: int) -> np.ndarray:
        read.append(i)
        return np.full((1, 1), i, dtype=np.uint32)

    out = reduce_frame_window(load, start=4, count=3)
    assert read == [4, 5, 6]
    assert out[0, 0] == 15


def test_an_unknown_operation_is_refused() -> None:
    with pytest.raises(ValueError):
        reduce_frames([np.zeros((1, 1))], "median")


# --------------------------------------------------------------------------
# The endpoint
# --------------------------------------------------------------------------


@pytest.fixture()
def stack(tmp_path: Path) -> tuple[Path, np.ndarray]:
    frames = np.arange(12 * 2 * 3, dtype=np.uint32).reshape(12, 2, 3)
    frames[:, 0, 0] = GAP32
    path = tmp_path / "series.h5"
    with h5py.File(path, "w") as handle:
        handle.create_dataset("entry/data/data", data=frames, chunks=(1, 2, 3))
    return path, frames


def _client() -> TestClient:
    from backend.app import app

    return TestClient(app, base_url="http://localhost")


def _get(path: Path, **params) -> object:
    return _client().get(
        "/api/frame", params={"file": str(path), "dataset": "/entry/data/data", **params}
    )


def test_count_returns_the_sum_of_the_frames_from_index(stack) -> None:
    path, frames = stack
    response = _get(path, index=2, count=5)
    assert response.status_code == 200, response.text
    assert response.headers["x-frame"] == "2"
    assert response.headers["x-frame-count"] == "5"
    assert response.headers["x-frame-op"] == "sum"
    arr = np.frombuffer(response.content, dtype=response.headers["x-dtype"]).reshape(2, 3)
    expected = frames[2:7].astype(np.uint64).sum(axis=0)
    np.testing.assert_array_equal(arr.ravel()[1:], expected.ravel()[1:])
    assert arr[0, 0] == np.iinfo(arr.dtype).max


def test_the_last_window_holds_the_frames_left(stack) -> None:
    path, frames = stack
    response = _get(path, index=10, count=5)
    assert response.status_code == 200, response.text
    assert response.headers["x-frame-count"] == "2"
    arr = np.frombuffer(response.content, dtype=response.headers["x-dtype"]).reshape(2, 3)
    assert arr[1, 2] == frames[10, 1, 2] + frames[11, 1, 2]


def test_op_mean_returns_float32(stack) -> None:
    path, frames = stack
    response = _get(path, index=0, count=4, op="mean")
    assert response.status_code == 200, response.text
    assert response.headers["x-dtype"] == "<f4"
    assert response.headers["x-frame-op"] == "mean"
    arr = np.frombuffer(response.content, dtype="<f4").reshape(2, 3)
    assert arr[1, 1] == pytest.approx(frames[0:4, 1, 1].mean())


def test_count_one_is_the_plain_frame_without_window_headers(stack) -> None:
    path, frames = stack
    response = _get(path, index=3)
    assert response.status_code == 200, response.text
    assert "x-frame-count" not in response.headers
    arr = np.frombuffer(response.content, dtype="<u4").reshape(2, 3)
    np.testing.assert_array_equal(arr, frames[3])


def test_a_window_starting_past_the_end_is_out_of_range(stack) -> None:
    path, _frames = stack
    assert _get(path, index=12, count=3).status_code == 416


@pytest.mark.parametrize(
    "params", [{"count": 0}, {"count": MAX_FRAME_WINDOW + 1}, {"op": "median"}]
)
def test_a_window_outside_the_limits_is_refused(stack, params) -> None:
    path, _frames = stack
    assert _get(path, index=0, **params).status_code == 422


def test_a_4d_stack_sums_within_the_chosen_threshold(tmp_path: Path) -> None:
    data = np.zeros((4, 2, 1, 2), dtype=np.uint16)
    data[:, 0] = 1
    data[:, 1] = 10
    path = tmp_path / "thresholds.h5"
    with h5py.File(path, "w") as handle:
        handle.create_dataset("entry/data/data", data=data)
    response = _get(path, index=0, count=4, threshold=1)
    assert response.status_code == 200, response.text
    arr = np.frombuffer(response.content, dtype=response.headers["x-dtype"])
    assert arr.tolist() == [40, 40]


# --------------------------------------------------------------------------
# A series of one file per frame: `GET /api/image?count=N`
# --------------------------------------------------------------------------


@pytest.fixture()
def cbf_series(tmp_path: Path) -> tuple[list[Path], list[np.ndarray]]:
    from backend.image_formats import _write_cbf

    frames = []
    paths = []
    for i in range(6):
        arr = np.full((3, 4), i + 1, dtype=np.int32)
        arr[0, 0] = -1  # a gap in every frame
        if i == 2:
            arr[1, 1] = -2  # defective in one frame only
        path = tmp_path / f"scan_{i + 1:05d}.cbf"
        _write_cbf(path, arr)
        frames.append(arr)
        paths.append(path)
    return paths, frames


def _image(path: Path, **params) -> object:
    return _client().get("/api/image", params={"file": str(path), **params})


def test_count_sums_the_next_files_of_the_series(cbf_series) -> None:
    paths, _frames = cbf_series
    response = _image(paths[1], count=3)
    assert response.status_code == 200, response.text
    assert response.headers["x-frame-count"] == "3"
    arr = np.frombuffer(response.content, dtype=response.headers["x-dtype"]).reshape(3, 4)
    assert arr[2, 3] == 2 + 3 + 4
    assert arr[0, 0] == -1
    assert arr[1, 1] == -2


def test_the_last_window_of_a_file_series_holds_the_files_left(cbf_series) -> None:
    paths, _frames = cbf_series
    response = _image(paths[4], count=10, op="mean")
    assert response.status_code == 200, response.text
    assert response.headers["x-frame-count"] == "2"
    assert response.headers["x-dtype"] == "<f4"
    arr = np.frombuffer(response.content, dtype="<f4").reshape(3, 4)
    assert arr[2, 3] == pytest.approx(5.5)
    assert np.isnan(arr[0, 0])


def test_count_one_reads_the_file_alone(cbf_series) -> None:
    paths, frames = cbf_series
    response = _image(paths[2])
    assert response.status_code == 200, response.text
    assert "x-frame-count" not in response.headers
    arr = np.frombuffer(response.content, dtype=response.headers["x-dtype"]).reshape(3, 4)
    np.testing.assert_array_equal(arr, frames[2])
