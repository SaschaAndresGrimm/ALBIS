"""Statistics and the chart outline the inspector shows for numeric arrays.

The inspector showed a 40,000-value array as an index/value table of its
first rows, so a jump in a frame's start times or a reset counter could only
be found by scrolling. `GET /api/hdf5/value` now adds statistics over the
whole dataset and, for 1D arrays, a min/max outline to draw.
"""

from __future__ import annotations

from pathlib import Path
from unittest import mock

import numpy as np
import pytest
from fastapi.testclient import TestClient

h5py = pytest.importorskip("h5py")

from backend.app import app  # noqa: E402
from backend.services import hdf5_inspect  # noqa: E402
from backend.services.hdf5_inspect import OUTLINE_BUCKETS, array_summary  # noqa: E402


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _value(client: TestClient, path: Path, node: str) -> dict:
    response = client.get("/api/hdf5/value", params={"file": str(path), "path": node})
    assert response.status_code == 200
    return response.json()


def test_float_statistics_count_nan_instead_of_averaging_it(
    tmp_path: Path, client: TestClient
) -> None:
    master = tmp_path / "f.h5"
    with h5py.File(master, "w") as h5:
        h5["t"] = np.array([1.0, np.nan, 3.0, np.inf])

    stats = _value(client, master, "/t")["stats"]

    assert stats == {"count": 4, "non_finite": 2, "min": 1.0, "max": 3.0, "mean": 2.0}


def test_integer_extremes_stay_exact(tmp_path: Path, client: TestClient) -> None:
    """A uint64 counter past 2**53 would lose its last digits as a float."""
    big = 2**60 + 1
    master = tmp_path / "i.h5"
    with h5py.File(master, "w") as h5:
        h5["ids"] = np.array([3, big], dtype=np.uint64)

    stats = _value(client, master, "/ids")["stats"]

    assert stats["min"] == 3
    assert stats["max"] == big


def test_outline_keeps_a_single_spike(tmp_path: Path, client: TestClient) -> None:
    values = np.zeros(40_000)
    values[12_345] = 99.0
    master = tmp_path / "s.h5"
    with h5py.File(master, "w") as h5:
        h5["v"] = values

    outline = _value(client, master, "/v")["outline"]

    assert outline["step"] == 100
    assert len(outline["low"]) == OUTLINE_BUCKETS
    assert outline["high"][123] == 99.0
    assert max(v for v in outline["high"] if v is not None) == 99.0


def test_short_arrays_outline_every_value(tmp_path: Path, client: TestClient) -> None:
    master = tmp_path / "short.h5"
    with h5py.File(master, "w") as h5:
        h5["v"] = np.array([5, 7, 6], dtype=np.int16)

    outline = _value(client, master, "/v")["outline"]

    assert outline == {"step": 1, "low": [5, 7, 6], "high": [5, 7, 6]}


def test_two_dimensional_arrays_get_statistics_but_no_outline(
    tmp_path: Path, client: TestClient
) -> None:
    master = tmp_path / "2d.h5"
    with h5py.File(master, "w") as h5:
        h5["flat"] = np.ones((8, 8), dtype=np.float32)

    body = _value(client, master, "/flat")

    assert body["stats"]["mean"] == 1.0
    assert body["outline"] is None


def test_text_and_large_datasets_get_neither(tmp_path: Path, client: TestClient) -> None:
    master = tmp_path / "t.h5"
    with h5py.File(master, "w") as h5:
        h5["name"] = np.array([b"a", b"b"])
        h5["v"] = np.arange(10)

    assert _value(client, master, "/name")["stats"] is None
    with mock.patch.object(hdf5_inspect, "STATS_CELL_LIMIT", 5):
        body = _value(client, master, "/v")
    assert body["stats"] is None
    assert body["outline"] is None


def test_a_failed_read_leaves_the_summary_out_rather_than_failing() -> None:
    class Unreadable:
        ndim = 1
        size = 3
        dtype = np.dtype("float64")

        def __getitem__(self, _key):
            raise OSError("filter not available")

    assert array_summary(Unreadable()) == (None, None)
