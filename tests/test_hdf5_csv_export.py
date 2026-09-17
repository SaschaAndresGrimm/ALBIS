"""The dataset CSV leads with its comments and says who produced it.

Part of moving every ALBIS CSV to one shape: a comment block, then one table.
This export was already a single table, so what changed is the prefix -- the
provenance that COMPATIBILITY.md promises of every written file, and the
`# truncated` marker, which used to be appended *after* the last row where a
reader that skips a comment prefix never sees it.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

import numpy as np
import pytest
from fastapi.testclient import TestClient

h5py = pytest.importorskip("h5py")

import backend.app as backend_app  # noqa: E402
from backend.version import ALBIS_VERSION  # noqa: E402


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """A client whose data root is `tmp_path`.

    Three places hold the root and each snapshots it at import, so all three
    move together -- the same dance `test_hdf5_storage_confinement` documents.
    """
    monkeypatch.setattr(backend_app.runtime_state, "data_dir", tmp_path)
    monkeypatch.setattr(
        backend_app,
        "path_policy",
        dataclasses.replace(backend_app.path_policy, data_dir=tmp_path),
    )
    monkeypatch.setattr(backend_app.hdf5_stack, "data_dir", tmp_path)
    return TestClient(backend_app.app)


def _write(path: Path, data: np.ndarray, dataset: str = "/entry/data/data") -> None:
    with h5py.File(path, "w") as h5:
        h5.create_dataset(dataset, data=data)


def _split(text: str) -> tuple[list[str], list[str]]:
    lines = text.splitlines()
    comments = [line for line in lines if line.startswith("#")]
    table = [line for line in lines if line and not line.startswith("#")]
    return comments, table


def test_csv_leads_with_provenance_then_one_table(client: TestClient, tmp_path: Path) -> None:
    _write(tmp_path / "scan.h5", np.arange(6, dtype=np.uint16).reshape(2, 3))

    response = client.get("/api/hdf5/csv", params={"file": "scan.h5", "path": "/entry/data/data"})

    assert response.status_code == 200
    lines = response.text.splitlines()
    comments, table = _split(response.text)
    # Every comment before the first row, so `comment="#"` leaves a clean table.
    assert lines[: len(comments)] == comments
    assert comments[0] == f"# Produced by ALBIS {ALBIS_VERSION}" or comments[0].startswith(
        f"# Produced by ALBIS {ALBIS_VERSION} ("
    )
    assert comments[1] == "# Source: scan.h5 /entry/data/data"
    assert table == ["0,1,2", "3,4,5"]


def test_the_truncated_marker_is_in_the_prefix_not_after_the_last_row(
    client: TestClient, tmp_path: Path
) -> None:
    """Where a reader that skips comments can still be told it has a preview."""
    _write(tmp_path / "big.h5", np.zeros((64, 64), dtype=np.uint16))

    response = client.get(
        "/api/hdf5/csv",
        params={"file": "big.h5", "path": "/entry/data/data", "max_cells": 64},
    )

    assert response.status_code == 200
    lines = response.text.splitlines()
    assert "# truncated" in lines
    comments, table = _split(response.text)
    assert lines[: len(comments)] == comments
    assert not lines[-1].startswith("#")
    # The rows that did come back are still a table, not a fragment.
    assert all(len(row.split(",")) == len(table[0].split(",")) for row in table)


def test_a_one_dimensional_dataset_keeps_its_index_and_value_header(
    client: TestClient, tmp_path: Path
) -> None:
    _write(tmp_path / "curve.h5", np.array([7, 8, 9], dtype=np.int32), dataset="/curve")

    response = client.get("/api/hdf5/csv", params={"file": "curve.h5", "path": "/curve"})

    _, table = _split(response.text)
    assert table == ["index,value", "0,7", "1,8", "2,9"]


def test_a_newline_in_a_group_name_cannot_forge_a_row(client: TestClient, tmp_path: Path) -> None:
    """The comment block carries a caller-influenced path, so it is kept to one line.

    HDF5 permits a newline in a group name. Written raw into `# Source:`, the
    tail of the name would land in the table as a data row.
    """
    _write(tmp_path / "odd.h5", np.array([1, 2], dtype=np.int32), dataset="/a\nb")

    response = client.get("/api/hdf5/csv", params={"file": "odd.h5", "path": "/a\nb"})

    assert response.status_code == 200
    comments, table = _split(response.text)
    assert len(comments) == 2
    assert comments[1] == "# Source: odd.h5 /a\\nb"
    assert table == ["index,value", "0,1", "1,2"]
