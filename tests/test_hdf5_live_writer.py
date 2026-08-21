"""Cover reading an HDF5 file while a writer still holds it open.

This is the beamline case, not an edge case: a detector filewriter keeps its
output open in SWMR mode for the length of a series, and every other HDF5
fixture in this suite is a finished file that no writer is touching. A plain
read-only open of a live file is refused by HDF5 with the same bare `OSError`
it uses for a corrupt one, so before `open_hdf5_read_only` retried in SWMR mode
the whole live-acquisition path answered 422 "may be incomplete or corrupt" for
a file that was perfectly readable.

The writer has to be a separate process. HDF5 tracks the files *this* process
has open and lets it open one of them again, so an in-process writer proves
nothing -- the refusal only happens across processes, which is exactly where a
detector's filewriter lives.
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

import h5py
import numpy as np
import pytest
from fastapi.testclient import TestClient

from backend.app import app
from backend.services.hdf5_stack import open_hdf5_read_only

DATASET = "/entry/data/data"
HEIGHT, WIDTH = 8, 8

# Writes one frame, announces itself, then grows the series on request until it
# is told to stop. Frame N is filled with N, so a wrong frame or an empty decode
# is visible rather than just "200 with some bytes".
_WRITER = """
import sys, time
from pathlib import Path

import h5py
import numpy as np

path, ready, grow, stop, dataset = sys.argv[1:6]
height, width = 8, 8

handle = h5py.File(path, "w", libver="latest")
data = handle.create_dataset(
    dataset,
    shape=(1, height, width),
    maxshape=(None, height, width),
    chunks=(1, height, width),
    dtype="<u4",
)
handle.swmr_mode = True
data[0] = np.full((height, width), 0, dtype="<u4")
handle.flush()
Path(ready).write_text("1")

deadline = time.monotonic() + 120
while time.monotonic() < deadline and not Path(stop).exists():
    try:
        wanted = int(Path(grow).read_text())
    except (OSError, ValueError):
        wanted = data.shape[0]
    if wanted > data.shape[0]:
        first = data.shape[0]
        data.resize((wanted, height, width))
        for index in range(first, wanted):
            data[index] = np.full((height, width), index, dtype="<u4")
        handle.flush()
        Path(ready).write_text(str(wanted))
    time.sleep(0.02)

handle.close()
"""

client = TestClient(app)


def _frame(index: int) -> np.ndarray:
    return np.full((HEIGHT, WIDTH), index, dtype="<u4")


def _wait_for(path: Path, expected: str, what: str, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if path.read_text().strip() == expected:
                return
        except OSError:
            pass
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for {what}")


@pytest.fixture
def live_series(tmp_path: Path):
    """A live SWMR series plus a handle to grow it, as an external writer would."""
    path = tmp_path / "live_series.h5"
    ready = tmp_path / "ready"
    grow = tmp_path / "grow"
    stop = tmp_path / "stop"
    script = tmp_path / "writer.py"
    script.write_text(_WRITER, encoding="utf-8")

    writer = subprocess.Popen(
        [sys.executable, str(script), str(path), str(ready), str(grow), str(stop), DATASET]
    )

    def append_to(total: int) -> None:
        grow.write_text(str(total), encoding="utf-8")
        _wait_for(ready, str(total), f"the writer to reach {total} frames")

    try:
        _wait_for(ready, "1", "the writer to create the series")
        yield path, append_to
    finally:
        stop.write_text("1", encoding="utf-8")
        try:
            writer.wait(timeout=10)
        except subprocess.TimeoutExpired:
            writer.kill()
            writer.wait(timeout=10)


def test_plain_open_is_refused_so_the_retry_is_doing_the_work(live_series) -> None:
    """Guards the arrangement: if a plain open worked, the rest would prove nothing."""
    path, _ = live_series

    with pytest.raises(OSError) as caught:
        h5py.File(path, "r")

    assert "already open for write" in str(caught.value)


def test_open_hdf5_read_only_opens_a_file_a_writer_holds(live_series) -> None:
    path, _ = live_series

    with open_hdf5_read_only(h5py, path) as handle:
        assert handle[DATASET].shape == (1, HEIGHT, WIDTH)
        assert np.array_equal(handle[DATASET][0], _frame(0))


def test_reopening_sees_frames_written_since(live_series) -> None:
    path, append_to = live_series

    with open_hdf5_read_only(h5py, path) as handle:
        assert handle[DATASET].shape[0] == 1

    append_to(4)

    with open_hdf5_read_only(h5py, path) as handle:
        assert handle[DATASET].shape[0] == 4
        assert np.array_equal(handle[DATASET][3], _frame(3))


def test_live_series_is_navigable_through_the_api(live_series) -> None:
    path, append_to = live_series
    append_to(4)
    params = {"file": str(path), "dataset": DATASET}

    metadata = client.get("/api/metadata", params=params)
    assert metadata.status_code == 200
    assert metadata.json()["shape"] == [4, HEIGHT, WIDTH]

    frame = client.get("/api/frame", params={**params, "index": 3})
    assert frame.status_code == 200
    shape = [int(part) for part in frame.headers["X-Shape"].split(",")]
    pixels = np.frombuffer(frame.content, dtype=frame.headers["X-Dtype"]).reshape(shape)
    assert np.array_equal(pixels, _frame(3))


def test_frame_count_grows_with_the_acquisition(live_series) -> None:
    path, append_to = live_series
    params = {"file": str(path), "dataset": DATASET}

    assert client.get("/api/metadata", params=params).json()["shape"][0] == 1

    append_to(3)

    assert client.get("/api/metadata", params=params).json()["shape"][0] == 3


def test_datasets_are_discoverable_in_a_live_file(live_series) -> None:
    """Opening the file at all is the gate: dataset discovery shares the path."""
    path, _ = live_series

    response = client.get("/api/datasets", params={"file": str(path)})

    assert response.status_code == 200
    assert any(entry["path"] == DATASET for entry in response.json()["datasets"])


def test_a_corrupt_file_still_reports_its_real_problem(tmp_path: Path) -> None:
    """The SWMR retry must not turn a broken file into a confusing SWMR error."""
    path = tmp_path / "corrupt.h5"
    path.write_bytes(b"this is not an HDF5 file" * 8)

    with pytest.raises(OSError) as caught:
        open_hdf5_read_only(h5py, path)

    assert "signature" in str(caught.value).lower()

    response = client.get("/api/frame", params={"file": str(path), "dataset": DATASET})
    assert response.status_code == 422
    assert "incomplete or corrupt" in response.json()["detail"]


def test_a_missing_file_is_still_a_missing_file(tmp_path: Path) -> None:
    response = client.get(
        "/api/frame", params={"file": str(tmp_path / "absent.h5"), "dataset": DATASET}
    )

    assert response.status_code == 404


def test_metadata_says_a_writer_still_holds_the_file(live_series) -> None:
    """The signal a viewer needs to know whether asking again is worthwhile.

    Without it a client has to choose between polling every open file forever
    and never noticing a series grow.
    """
    path, _ = live_series

    payload = client.get("/api/metadata", params={"file": str(path), "dataset": DATASET}).json()

    assert payload["writer_present"] is True


def test_metadata_says_nothing_holds_a_finished_file(tmp_path: Path) -> None:
    path = tmp_path / "finished.h5"
    with h5py.File(path, "w") as handle:
        handle.create_dataset(DATASET, data=np.zeros((2, HEIGHT, WIDTH), dtype="<u4"))

    payload = client.get("/api/metadata", params={"file": str(path), "dataset": DATASET}).json()

    assert payload["writer_present"] is False
    assert payload["shape"] == [2, HEIGHT, WIDTH]


def test_writer_presence_drops_when_the_run_ends(live_series, tmp_path: Path) -> None:
    """A watch that never stops is a poll on every finished file forever."""
    path, append_to = live_series
    params = {"file": str(path), "dataset": DATASET}
    append_to(3)

    assert client.get("/api/metadata", params=params).json()["writer_present"] is True

    # End the run the way the fixture's teardown does, then read again.
    (tmp_path / "stop").write_text("1", encoding="utf-8")
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if client.get("/api/metadata", params=params).json()["writer_present"] is False:
            break
        time.sleep(0.1)

    payload = client.get("/api/metadata", params=params).json()
    assert payload["writer_present"] is False
    assert payload["shape"][0] == 3, "the frames written during the run are still there"
