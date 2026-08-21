"""Cover what the scan budget looks like from the API.

The unit tests cover the walk and the cache. These cover the part a client sees:
that a listing which stopped early says so instead of passing itself off as
complete, and that the endpoint live autoload polls once a second does not walk
the filesystem again on every poll.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from backend.app import app, runtime_state, scan_cache

client = TestClient(app)


@pytest.fixture(autouse=True)
def clean_cache():
    scan_cache.clear()
    yield
    scan_cache.clear()


# Modification times are set explicitly rather than taken from the order the
# files were written. "Newest" has to be unambiguous for a test about the newest
# file, and on a filesystem with coarse timestamps a whole tree written in one
# tick shares an mtime -- at which point the winner is whichever the walk reached
# first, which is how this passed on APFS and failed on the CI runners.
_OLD_MTIME = 1_700_000_000
_NEW_MTIME = 1_700_009_999


def _age(path: Path, when: int) -> None:
    os.utime(path, (when, when))


@pytest.fixture
def tree(tmp_path: Path) -> Path:
    for run in range(4):
        folder = tmp_path / f"run_{run:02d}"
        folder.mkdir()
        for frame in range(5):
            frame_path = folder / f"frame_{frame:04d}.cbf"
            frame_path.write_bytes(b"")
            _age(frame_path, _OLD_MTIME)
    return tmp_path


def _write_newest(tree: Path, name: str = "zz_newest.cbf") -> Path:
    """Write a file that is unambiguously the newest in the tree."""
    path = tree / "run_00" / name
    path.write_bytes(b"")
    _age(path, _NEW_MTIME)
    return path


def test_a_complete_listing_is_not_marked_truncated(tree: Path) -> None:
    response = client.get("/api/files", params={"folder": str(tree)})

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["files"]) == 20
    assert payload["truncated"] is False


def test_a_listing_that_hit_the_budget_says_so(tree: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runtime_state, "max_scan_entries", 5)

    response = client.get("/api/files", params={"folder": str(tree)})

    assert response.status_code == 200
    payload = response.json()
    assert payload["truncated"] is True
    assert len(payload["files"]) < 20


def test_autoload_reports_a_truncated_search(tree: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The newest file may be the one the walk did not reach."""
    monkeypatch.setattr(runtime_state, "max_scan_entries", 9)

    response = client.get("/api/autoload/latest", params={"folder": str(tree)})

    assert response.status_code == 200
    assert response.json()["truncated"] is True
    assert response.headers["X-Scan-Truncated"] == "1"


def test_a_truncated_search_that_found_nothing_is_not_an_empty_folder(
    tree: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A 204 has no body, so the reason has to travel in a header.

    Otherwise "the budget ran out before any file was reached" is indistinguishable
    from "this folder is empty", and a watching client reports the wrong thing.
    """
    monkeypatch.setattr(runtime_state, "max_scan_entries", 2)

    response = client.get("/api/autoload/latest", params={"folder": str(tree)})

    assert response.status_code == 204
    assert response.headers["X-Scan-Truncated"] == "1"


def test_a_complete_search_sets_no_truncation_header(tree: Path) -> None:
    response = client.get("/api/autoload/latest", params={"folder": str(tree)})

    assert response.status_code == 200
    assert "X-Scan-Truncated" not in response.headers
    assert response.json()["truncated"] is False


def test_autoload_polls_do_not_rescan_within_the_ttl(
    tree: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """This is the fix for the poll that walked the whole tree every second."""
    monkeypatch.setattr(runtime_state, "scan_cache_sec", 60.0)
    params = {"folder": str(tree)}

    first = client.get("/api/autoload/latest", params=params)
    assert first.status_code == 200
    first_file = first.json()["file"]

    # A file that would win on mtime if the directory were walked again.
    _write_newest(tree)

    second = client.get("/api/autoload/latest", params=params)

    assert second.status_code == 200
    assert second.json()["file"] == first_file, "the poll rescanned inside the TTL"


def test_autoload_sees_a_new_file_once_the_ttl_expires(tree: Path, monkeypatch) -> None:
    monkeypatch.setattr(runtime_state, "scan_cache_sec", 0.0)
    params = {"folder": str(tree)}

    client.get("/api/autoload/latest", params=params)
    _write_newest(tree)

    response = client.get("/api/autoload/latest", params=params)

    assert response.status_code == 200
    assert response.json()["file"].endswith("zz_newest.cbf")


def test_autoload_caches_per_folder_pattern_and_extensions(tree: Path, monkeypatch) -> None:
    """Different watches must not answer each other's questions."""
    monkeypatch.setattr(runtime_state, "scan_cache_sec", 60.0)
    _write_newest(tree, "special_9999.cbf")

    everything = client.get("/api/autoload/latest", params={"folder": str(tree)})
    filtered = client.get(
        "/api/autoload/latest", params={"folder": str(tree), "pattern": "special_*.cbf"}
    )

    assert everything.status_code == 200
    assert filtered.status_code == 200
    assert filtered.json()["file"].endswith("special_9999.cbf")


def test_a_selected_subfolder_is_not_cached(tree: Path, monkeypatch) -> None:
    """The upload flow asks this endpoint to find the file it just wrote."""
    monkeypatch.setattr(runtime_state, "scan_cache_sec", 60.0)
    params = {"folder": str(tree)}

    before = client.get("/api/files", params=params).json()["files"]
    _write_newest(tree, "just_written.cbf")
    after = client.get("/api/files", params=params).json()["files"]

    assert len(after) == len(before) + 1
    assert any(name.endswith("just_written.cbf") for name in after)


def test_a_vanished_file_is_nothing_to_load_not_an_error(tree: Path, monkeypatch) -> None:
    """A cached path can be deleted between the scan and the next poll."""
    monkeypatch.setattr(runtime_state, "scan_cache_sec", 60.0)
    newest = _write_newest(tree)
    params = {"folder": str(tree)}

    assert (
        client.get("/api/autoload/latest", params=params).json()["file"].endswith("zz_newest.cbf")
    )
    newest.unlink()

    assert client.get("/api/autoload/latest", params=params).status_code == 204


def test_folders_listing_reports_truncation(tree: Path, monkeypatch) -> None:
    monkeypatch.setattr(runtime_state, "max_scan_entries", 2)

    response = client.get("/api/folders")

    assert response.status_code == 200
    assert "truncated" in response.json()


# --------------------------------------------------------------------------
# The interactive listing
# --------------------------------------------------------------------------


def test_browsing_a_normal_folder_is_not_marked_truncated(tree: Path) -> None:
    response = client.get("/api/browse", params={"path": str(tree)})

    assert response.status_code == 200
    payload = response.json()
    assert payload["truncated"] is False
    assert len(payload["folders"]) == 4


def test_browsing_a_huge_flat_folder_stops_and_says_so(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The shape a beamline folder actually has: one directory, many frames.

    The recursive scans were budgeted first because they looked like the risk.
    This is the endpoint the file browser calls, and unbounded it means a stat
    per entry, a dict per entry, a sort across all of them and the lot
    serialized -- for a listing nobody can read.
    """
    for index in range(40):
        (tmp_path / f"frame_{index:05d}.cbf").write_bytes(b"")
    monkeypatch.setattr(runtime_state, "max_scan_entries", 10)

    response = client.get("/api/browse", params={"path": str(tmp_path)})

    assert response.status_code == 200
    payload = response.json()
    assert payload["truncated"] is True
    assert len(payload["fileItems"]) < 40


def test_the_browse_budget_is_shared_by_folders_and_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One allowance per request, not one per pass."""
    for index in range(20):
        (tmp_path / f"run_{index:02d}").mkdir()
        (tmp_path / f"frame_{index:02d}.cbf").write_bytes(b"")
    monkeypatch.setattr(runtime_state, "max_scan_entries", 12)

    payload = client.get("/api/browse", params={"path": str(tmp_path)}).json()

    assert payload["truncated"] is True
    assert len(payload["folders"]) < 20


def test_an_unlimited_budget_still_lists_everything(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """0 means unlimited here, as it does for every other budget."""
    for index in range(30):
        (tmp_path / f"frame_{index:05d}.cbf").write_bytes(b"")
    monkeypatch.setattr(runtime_state, "max_scan_entries", 0)

    payload = client.get("/api/browse", params={"path": str(tmp_path)}).json()

    assert payload["truncated"] is False
    assert len(payload["fileItems"]) == 30
