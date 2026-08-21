"""Cover reading HDF5 written with a compression filter.

Every other HDF5 fixture in this suite is created by plain h5py with no filter,
so none of them touch hdf5plugin's native libraries -- which is what real
detector data needs, and the piece a packaged build can silently lose.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
from fastapi.testclient import TestClient

from backend.app import app

FIXTURE = Path(__file__).resolve().parents[1] / "testdata" / "compressed_stack.h5"
DATASET = "/entry/data/data"

client = TestClient(app)


def test_fixture_actually_uses_a_compression_filter() -> None:
    """Guards the fixture itself: uncompressed, it would prove nothing."""
    import h5py
    import hdf5plugin  # noqa: F401  (registers the filter plugins)

    with h5py.File(FIXTURE, "r") as handle:
        filters = dict(handle[DATASET]._filters)

    assert filters, (
        "testdata/compressed_stack.h5 has no compression filter; "
        "regenerate it with scripts/make_compressed_hdf5_fixture.py"
    )


def test_frames_decode_through_the_api() -> None:
    response = client.get(
        "/api/frame", params={"file": str(FIXTURE), "dataset": DATASET, "index": 2}
    )

    assert response.status_code == 200
    shape = [int(part) for part in response.headers["X-Shape"].split(",")]
    frame = np.frombuffer(response.content, dtype=response.headers["X-Dtype"]).reshape(shape)
    # The generator fills frame N with N, so a wrong frame or a silently empty
    # decode is visible rather than just "200 with some bytes".
    assert frame[0][0] == 2
    assert frame[2][0] == 1002


def test_datasets_endpoint_discovers_the_stack() -> None:
    response = client.get("/api/datasets", params={"file": str(FIXTURE)})

    assert response.status_code == 200
    paths = [entry["path"] for entry in response.json()["datasets"]]
    assert DATASET in paths
