"""What the file inspector's listing says about each dataset.

A NeXus master keeps most of its metadata in scalar string datasets. h5py
reports their dtype as `object` and their shape as `()`, so the tree showed
"object" beside `definition`, `program_name` and `start_time`: no value, and a
type that described nothing. The listing now carries one-element values and
their units, and names string dtypes as `string`.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
from fastapi.testclient import TestClient

h5py = pytest.importorskip("h5py")

from backend.app import app  # noqa: E402


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _write(path: Path) -> None:
    with h5py.File(path, "w") as h5:
        entry = h5.create_group("entry")
        entry["definition"] = "NXmx"
        energy = entry.create_dataset("energy", data=np.array([12.4]))
        energy.attrs["units"] = "keV"
        entry["frames"] = np.int32(7)
        entry["image"] = np.zeros((4, 4), dtype=np.uint16)
        entry["notes"] = "x" * 500
        entry["flag"] = np.bool_(True)


def _children(client: TestClient, path: Path, group: str = "/entry") -> dict[str, dict]:
    response = client.get("/api/hdf5/tree", params={"file": str(path), "path": group})
    assert response.status_code == 200
    return {child["name"]: child for child in response.json()["children"]}


def test_scalar_string_reports_its_value_and_a_string_dtype(
    tmp_path: Path, client: TestClient
) -> None:
    master = tmp_path / "meta.h5"
    _write(master)

    row = _children(client, master)["definition"]

    assert row["value"] == "NXmx"
    assert row["dtype"] == "string"
    assert row["shape"] == []


def test_one_element_array_reports_value_and_units(tmp_path: Path, client: TestClient) -> None:
    master = tmp_path / "meta.h5"
    _write(master)

    children = _children(client, master)

    assert children["energy"]["value"] == pytest.approx(12.4)
    assert children["energy"]["units"] == "keV"
    assert children["frames"]["value"] == 7
    assert children["frames"].get("units") is None
    assert children["flag"]["value"] is True


def test_arrays_report_no_value_and_long_text_is_cut(tmp_path: Path, client: TestClient) -> None:
    master = tmp_path / "meta.h5"
    _write(master)

    children = _children(client, master)

    assert children["image"].get("value") is None
    assert children["image"]["dtype"] == "uint16"
    assert len(children["notes"]["value"]) == 201
    assert children["notes"]["value"].endswith("…")


def test_scalar_with_external_raw_storage_is_not_read(tmp_path: Path, client: TestClient) -> None:
    """The listing indexes children directly, past the storage confinement.

    So it must not read a dataset whose bytes live in another file: a crafted
    file could name any file the server can open.
    """
    raw = tmp_path / "raw.bin"
    raw.write_bytes(np.int32(42).tobytes())
    master = tmp_path / "ext.h5"
    with h5py.File(master, "w") as h5:
        h5.create_dataset("/entry/secret", shape=(1,), dtype="<i4", external=[(str(raw), 0, 4)])

    row = _children(client, master)["secret"]

    assert row.get("value") is None
    assert row["shape"] == [1]


def test_search_rows_carry_the_same_summary(tmp_path: Path, client: TestClient) -> None:
    master = tmp_path / "meta.h5"
    _write(master)

    response = client.get("/api/hdf5/search", params={"file": str(master), "query": "definition"})

    matches = response.json()["matches"]
    assert [m["path"] for m in matches] == ["/entry/definition"]
    assert matches[0]["value"] == "NXmx"


def test_node_and_value_name_string_dtypes(tmp_path: Path, client: TestClient) -> None:
    master = tmp_path / "meta.h5"
    _write(master)
    params = {"file": str(master), "path": "/entry/definition"}

    node = client.get("/api/hdf5/node", params=params).json()
    value = client.get("/api/hdf5/value", params=params).json()

    assert node["dtype"] == "string"
    assert value["dtype"] == "string"
