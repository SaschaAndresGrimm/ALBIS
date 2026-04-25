from __future__ import annotations

import os
from pathlib import Path

from fastapi.testclient import TestClient

from backend.app import app


def test_browse_endpoint_supports_expt_extension_filter(tmp_path: Path) -> None:
    client = TestClient(app)
    (tmp_path / "series_0001.tiff").write_bytes(b"test")
    (tmp_path / "imported.expt").write_text("{}", encoding="utf-8")

    default_browse = client.get("/api/browse", params={"path": str(tmp_path)})
    expt_browse = client.get("/api/browse", params={"path": str(tmp_path), "exts": ".expt"})
    mixed_browse = client.get("/api/browse", params={"path": str(tmp_path), "exts": ".tiff,.expt"})

    assert default_browse.status_code == 200
    assert default_browse.json()["files"] == ["series_0001.tiff"]

    assert expt_browse.status_code == 200
    assert expt_browse.json()["files"] == ["imported.expt"]

    assert mixed_browse.status_code == 200
    assert mixed_browse.json()["files"] == ["imported.expt", "series_0001.tiff"]
    mixed_item = mixed_browse.json()["fileItems"][0]
    assert mixed_item["name"] == "imported.expt"
    assert mixed_item["ext"] == ".expt"
    assert mixed_item["path"] == str((tmp_path / "imported.expt").resolve())


def test_browse_endpoint_returns_parent_path_and_rich_items(tmp_path: Path) -> None:
    client = TestClient(app)
    nested = tmp_path / "raw"
    nested.mkdir()
    target = nested / "scan_0001.tiff"
    target.write_bytes(b"test")

    response = client.get("/api/browse", params={"path": str(nested)})

    assert response.status_code == 200
    payload = response.json()
    assert payload["currentPath"] == str(nested.resolve())
    assert payload["parentPath"] == str(tmp_path.resolve())
    assert payload["canGoUp"] is True
    assert payload["files"] == ["scan_0001.tiff"]
    assert payload["fileItems"] == [
        {
            "name": "scan_0001.tiff",
            "path": str(target.resolve()),
            "ext": ".tiff",
            "mtime": payload["fileItems"][0]["mtime"],
            "sizeBytes": 4,
            "isSeriesLead": False,
            "seriesCount": 1,
        }
    ]


def test_choose_file_endpoint_supports_expt_extension_filter(tmp_path: Path, monkeypatch) -> None:
    client = TestClient(app)
    geometry_path = (tmp_path / "imported.expt").resolve()
    geometry_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr("backend.routes.files._choose_file", lambda **_kwargs: str(geometry_path))

    response = client.get("/api/choose-file", params={"exts": ".expt"})

    assert response.status_code == 200
    assert response.json()["path"] == str(geometry_path)


def test_browse_endpoint_groups_series_and_sorts_naturally(tmp_path: Path) -> None:
    client = TestClient(app)
    series_1 = tmp_path / "series_0001.tiff"
    series_2 = tmp_path / "series_0002.tiff"
    image_2 = tmp_path / "image_2a.edf"
    image_10 = tmp_path / "image_10b.edf"
    h5_a = tmp_path / "scan_0001.h5"
    h5_b = tmp_path / "scan_0002.h5"

    for path in (series_1, series_2, image_2, image_10, h5_a, h5_b):
        path.write_bytes(b"test")

    os.utime(series_1, (100, 100))
    os.utime(series_2, (500, 500))
    os.utime(image_2, (200, 200))
    os.utime(image_10, (300, 300))
    os.utime(h5_a, (150, 150))
    os.utime(h5_b, (120, 120))

    natural = client.get("/api/browse", params={"path": str(tmp_path), "sort": "name_asc"})
    grouped = client.get(
        "/api/browse",
        params={"path": str(tmp_path), "series_mode": "first_only", "sort": "mtime_desc"},
    )

    assert natural.status_code == 200
    natural_files = natural.json()["files"]
    assert natural_files.index("image_2a.edf") < natural_files.index("image_10b.edf")

    assert grouped.status_code == 200
    grouped_payload = grouped.json()
    assert grouped_payload["files"] == [
        "series_0001.tiff",
        "image_10b.edf",
        "image_2a.edf",
        "scan_0001.h5",
        "scan_0002.h5",
    ]
    series_item = grouped_payload["fileItems"][0]
    assert series_item["name"] == "series_0001.tiff"
    assert series_item["isSeriesLead"] is True
    assert series_item["seriesCount"] == 2
    assert series_item["mtime"] == 500.0
    assert grouped_payload["files"].count("scan_0001.h5") == 1
    assert grouped_payload["files"].count("scan_0002.h5") == 1
