"""File and dataset names that are not plain ASCII must not break a response.

Three ways an unusual name used to fail, found together after a report of
`a¦b.tiff` not opening:

- On Linux, a name stored in bytes that are not UTF-8 (a Latin-1 `a¦b.tiff`
  copied from Windows or an old archive) reaches Python with lone surrogates
  in it. Listed, it made the JSON response fail: the file browser showed an
  empty folder, whatever else was in it.
- A download named after an HDF5 dataset went into `Content-Disposition` as
  is, and a character outside Latin-1 made the export a 500.
- The log file was written in the locale's encoding, cp1252 on Windows, so
  the entry naming a Greek or Japanese file was lost.

macOS refuses to create a name that is not UTF-8, so the first case is
simulated by handing the code the name exactly as Linux would.
"""

from __future__ import annotations

import dataclasses
import logging
import types
from logging.handlers import RotatingFileHandler
from pathlib import Path
from urllib.parse import unquote

import numpy as np
import pytest
from fastapi.testclient import TestClient

h5py = pytest.importorskip("h5py")

import backend.app as backend_app  # noqa: E402
from backend.routes import files as files_routes  # noqa: E402
from backend.services import directory_scan  # noqa: E402
from backend.services.content_disposition import attachment_disposition  # noqa: E402
from backend.services.log_safety import sanitize_log_value  # noqa: E402

# `a¦b.tiff` with `¦` as the Latin-1 byte 0xA6, as `os.fsdecode` gives it on Linux.
UNDECODABLE = "a\udca6b.tiff"


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """A client whose data root is `tmp_path` (see test_hdf5_csv_export.py)."""
    monkeypatch.setattr(backend_app.runtime_state, "data_dir", tmp_path)
    monkeypatch.setattr(
        backend_app,
        "path_policy",
        dataclasses.replace(backend_app.path_policy, data_dir=tmp_path),
    )
    monkeypatch.setattr(backend_app.hdf5_stack, "data_dir", tmp_path)
    return TestClient(backend_app.app)


class _Entry:
    """A `DirEntry` stand-in for a regular file called `name`."""

    def __init__(self, folder: Path, name: str) -> None:
        self.name = name
        self.path = str(folder / name)

    def is_dir(self, follow_symlinks: bool = True) -> bool:
        return False

    def is_file(self, follow_symlinks: bool = True) -> bool:
        return True

    def stat(self, follow_symlinks: bool = True) -> types.SimpleNamespace:
        return types.SimpleNamespace(st_mtime=0.0, st_size=1)


class _Listing:
    def __init__(self, entries: list[_Entry]) -> None:
        self._entries = entries

    def __enter__(self):  # noqa: ANN204
        return iter(self._entries)

    def __exit__(self, *exc: object) -> None:
        return None


def _scandir_with(monkeypatch: pytest.MonkeyPatch, module: object, names: list[str]) -> None:
    monkeypatch.setattr(
        module.os,  # type: ignore[attr-defined]
        "scandir",
        lambda folder: _Listing([_Entry(Path(folder), name) for name in names]),
    )


def test_a_name_that_is_not_utf8_is_recognised() -> None:
    assert directory_scan.is_utf8_name("a¦b.tiff")
    assert directory_scan.is_utf8_name("温度_0001.cbf")
    assert not directory_scan.is_utf8_name(UNDECODABLE)


def test_the_file_browser_lists_the_rest_of_a_folder_with_such_a_name(
    client: TestClient, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog
) -> None:
    """One undecodable name used to make the listing a 500 -- an empty dialog."""
    _scandir_with(monkeypatch, files_routes, [UNDECODABLE, "ok.tiff"])

    with caplog.at_level(logging.WARNING, logger="albis"):
        response = client.get("/api/browse")

    assert response.status_code == 200
    assert response.json()["files"] == ["ok.tiff"]
    # Whoever wonders where the file went can find it, and the entry itself
    # survives being written: the surrogate is escaped, not passed through.
    warning = next(r.getMessage() for r in caplog.records if "not UTF-8" in r.getMessage())
    assert "a\\udca6b.tiff" in warning
    warning.encode("utf-8")


def test_a_folder_scan_skips_such_a_name(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    # The scan behind /api/files and autoload: its result is JSON too.
    _scandir_with(monkeypatch, directory_scan, [UNDECODABLE, "ok.tiff"])

    walk = directory_scan.BoundedWalk(directory_scan.ScanLimits())
    names = [name for _, name, _ in walk.entries(tmp_path)]

    assert names == ["ok.tiff"]


def test_the_log_sanitizer_escapes_a_lone_surrogate() -> None:
    assert sanitize_log_value(UNDECODABLE) == "a\\udca6b.tiff"
    # Real non-ASCII text is left readable.
    assert sanitize_log_value("温度 ¦ ü") == "温度 ¦ ü"


@pytest.mark.parametrize("dataset", ["/entry/Ω", "/entry/温度", '/entry/a"b'])
def test_a_dataset_with_any_name_exports_as_csv(
    client: TestClient, tmp_path: Path, dataset: str
) -> None:
    with h5py.File(tmp_path / "scan.h5", "w") as h5:
        h5.create_dataset(dataset, data=np.arange(3, dtype=np.uint16))

    response = client.get("/api/hdf5/csv", params={"file": "scan.h5", "path": dataset})

    assert response.status_code == 200
    disposition = response.headers["content-disposition"]
    expected = dataset.strip("/").replace("/", "_") + ".csv"
    assert unquote(disposition.split("filename*=UTF-8''", 1)[1]) == expected


def test_a_plain_name_keeps_the_simple_header() -> None:
    assert attachment_disposition("entry_data.csv") == 'attachment; filename="entry_data.csv"'


def test_the_fallback_name_is_quoted_safely() -> None:
    header = attachment_disposition('a"Ω\\b.csv')
    fallback = header.split('filename="', 1)[1].split('"', 1)[0]
    assert fallback == "a___b.csv"
    header.encode("latin-1")


def test_the_log_file_is_written_as_utf8() -> None:
    handlers = [
        h for h in logging.getLogger("albis").handlers if isinstance(h, RotatingFileHandler)
    ]
    assert handlers, "file logging is not set up"
    assert all(h.encoding == "utf-8" for h in handlers)
