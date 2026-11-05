"""A virtual dataset's frames are served in its sources' type, not its declared one.

Diamond's nexgen declares its NXmx master's `/entry/data/data` as int64 over
Odin's uint32 frames. Read as declared, each Eiger2 4M frame went out as 36 MB
instead of 18 MB, and as a type the interface cannot judge saturation in (it
gives up above 52 bits), so the 0xFFFFFFFF gap pixels showed as counts.
"""

from __future__ import annotations

from pathlib import Path

import h5py
import numpy as np
import pytest
from fastapi.testclient import TestClient

from backend.services.vds_dtype import VirtualDtypeCache, narrowed_dtype

GAP = np.uint32(0xFFFFFFFF)

# --------------------------------------------------------------------------
# The rule
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("declared", "sources", "fill", "expected"),
    [
        ("int64", ["uint32", "uint32"], 0, np.dtype("uint32")),  # nexgen over Odin
        ("int64", ["uint16"], 0, np.dtype("uint16")),
        ("uint32", [">u2"], 0, np.dtype("uint16")),  # byte order is not a reason to refuse
        ("int64", ["uint32", "uint16"], 0, None),  # sources disagree
        ("int64", ["float32"], 0, None),
        ("float64", ["uint32"], 0, None),  # a float VDS is left alone
        ("uint32", ["uint32"], 0, None),  # nothing to gain
        ("uint32", ["int16"], 0, None),  # uint32 cannot hold int16's negatives
        ("int64", ["uint32"], -1, None),  # unmapped regions read as -1
        ("int64", [], 0, None),
    ],
)
def test_the_narrower_type_is_used_only_when_nothing_can_change(
    declared: str, sources: list[str], fill: int, expected
) -> None:
    assert narrowed_dtype(declared, sources, fill) == expected


def test_the_answer_is_kept_per_file_version() -> None:
    """Every frame request resolves the dataset again; the sources are opened once."""
    calls: list[str] = []

    class _Source:
        file_name = "data_000001.h5"
        dset_name = "/data"

    class _VDS:
        is_virtual = True
        dtype = np.dtype("int64")
        fillvalue = 0

        def virtual_sources(self):
            return [_Source()]

    def _open(name: str, path: str):
        calls.append(name)
        return np.zeros(1, dtype="uint32"), []

    cache = VirtualDtypeCache()
    for _ in range(3):
        assert cache.read_dtype(_VDS(), ("f", 1, 1, "/d"), _open) == np.dtype("uint32")
    assert calls == ["data_000001.h5"]
    cache.read_dtype(_VDS(), ("f", 2, 2, "/d"), _open)  # the file was rewritten
    assert len(calls) == 2


def test_a_source_that_may_not_be_opened_leaves_the_declared_type() -> None:
    class _Source:
        file_name = "/etc/elsewhere.h5"
        dset_name = "/data"

    class _VDS:
        is_virtual = True
        dtype = np.dtype("int64")
        fillvalue = 0

        def virtual_sources(self):
            return [_Source()]

    def _refuse(name: str, path: str):
        raise KeyError("Virtual dataset source not permitted")

    assert VirtualDtypeCache().read_dtype(_VDS(), ("k",), _refuse) is None


# --------------------------------------------------------------------------
# End to end, in nexgen's layout
# --------------------------------------------------------------------------


def _odin_data(path: Path, frames: np.ndarray) -> None:
    with h5py.File(path, "w") as handle:
        handle.create_dataset("data", data=frames)


def _nexgen_master(path: Path, parts: list[tuple[str, int]], dtype: str, shape=(4, 5)) -> None:
    """nexgen: external links to each data file, and an int64 VDS over them via `.`."""
    total = sum(count for _, count in parts)
    layout = h5py.VirtualLayout(shape=(total, *shape), dtype=dtype)
    with h5py.File(path, "w") as handle:
        start = 0
        for index, (name, count) in enumerate(parts, start=1):
            link = f"/entry/data/data_{index:06d}"
            handle[link] = h5py.ExternalLink(name, "/data")
            layout[start : start + count] = h5py.VirtualSource(".", link, shape=(count, *shape))
            start += count
        handle["entry/data"].create_virtual_dataset("data", layout, fillvalue=0)


@pytest.fixture
def collection(tmp_path: Path) -> tuple[Path, np.ndarray]:
    rng = np.random.default_rng(7)
    frames = rng.integers(0, 50_000, size=(5, 4, 5), dtype=np.uint32)
    frames[:, 0, 0] = GAP  # a module gap, as the detector writes it
    _odin_data(tmp_path / "run_000001.h5", frames[:3])
    _odin_data(tmp_path / "run_000002.h5", frames[3:])
    _nexgen_master(tmp_path / "run.nxs", [("run_000001.h5", 3), ("run_000002.h5", 2)], "int64")
    return tmp_path / "run.nxs", frames


def _client() -> TestClient:
    from backend.app import app

    return TestClient(app, base_url="http://localhost")


def test_frames_of_an_int64_vds_over_uint32_go_out_as_uint32(collection) -> None:
    """The reported case: half the bytes, the same values, the gap value intact."""
    master, frames = collection
    client = _client()

    for index in (0, 2, 3, 4):  # both data files
        response = client.get(
            "/api/frame",
            params={"file": str(master), "dataset": "/entry/data/data", "index": index},
        )
        assert response.status_code == 200, response.text
        assert response.headers["x-dtype"] == "<u4"
        arr = np.frombuffer(response.content, dtype="<u4").reshape(4, 5)
        np.testing.assert_array_equal(arr, frames[index])
        assert arr[0, 0] == GAP


def test_the_metadata_reports_the_type_the_frames_arrive_in(collection) -> None:
    master, _frames = collection
    metadata = _client().get(
        "/api/metadata", params={"file": str(master), "dataset": "/entry/data/data"}
    )
    assert metadata.json()["dtype"] == "uint32"


def test_a_vds_over_mixed_types_keeps_its_declared_type(tmp_path: Path) -> None:
    _odin_data(tmp_path / "a_000001.h5", np.ones((2, 4, 5), dtype=np.uint32))
    _odin_data(tmp_path / "a_000002.h5", np.ones((2, 4, 5), dtype=np.uint16))
    _nexgen_master(tmp_path / "a.nxs", [("a_000001.h5", 2), ("a_000002.h5", 2)], "int64")

    response = _client().get(
        "/api/frame",
        params={"file": str(tmp_path / "a.nxs"), "dataset": "/entry/data/data", "index": 3},
    )

    assert response.headers["x-dtype"] == "<i8"


def test_an_ordinary_dataset_is_untouched(tmp_path: Path) -> None:
    path = tmp_path / "plain.h5"
    with h5py.File(path, "w") as handle:
        handle.create_dataset("data", data=np.arange(40, dtype=np.int64).reshape(2, 4, 5))

    response = _client().get(
        "/api/frame", params={"file": str(path), "dataset": "/data", "index": 1}
    )

    assert response.headers["x-dtype"] == "<i8"
