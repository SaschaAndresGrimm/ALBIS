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


def test_browse_endpoint_sorts_by_type_and_size(tmp_path: Path) -> None:
    client = TestClient(app)
    small = tmp_path / "b_small.tiff"
    large = tmp_path / "a_large.tiff"
    other = tmp_path / "c_other.edf"
    small.write_bytes(b"x")  # 1 byte
    large.write_bytes(b"x" * 100)  # 100 bytes
    other.write_bytes(b"xx")  # 2 bytes

    size_asc = client.get("/api/browse", params={"path": str(tmp_path), "sort": "size_asc"})
    size_desc = client.get("/api/browse", params={"path": str(tmp_path), "sort": "size_desc"})
    type_desc = client.get("/api/browse", params={"path": str(tmp_path), "sort": "type_desc"})

    assert size_asc.status_code == 200
    assert size_asc.json()["files"] == ["b_small.tiff", "c_other.edf", "a_large.tiff"]
    assert size_desc.json()["files"] == ["a_large.tiff", "c_other.edf", "b_small.tiff"]
    # type_desc: .tiff before .edf; names sorted within the reverse ordering.
    assert type_desc.json()["files"] == ["b_small.tiff", "a_large.tiff", "c_other.edf"]


def test_browse_endpoint_flags_missing_requested_path(tmp_path: Path) -> None:
    client = TestClient(app)
    (tmp_path / "real.tiff").write_bytes(b"x")

    missing = client.get("/api/browse", params={"path": str(tmp_path / "nope")})
    present = client.get("/api/browse", params={"path": str(tmp_path)})

    assert missing.status_code == 200
    # A non-existent path falls back to the data root and is flagged.
    assert missing.json()["requestedPathMissing"] is True
    assert present.status_code == 200
    assert present.json()["requestedPathMissing"] is False


def test_browse_endpoint_collapses_hdf5_master_data_series(tmp_path: Path) -> None:
    client = TestClient(app)
    master = tmp_path / "260616_CeO2_raw_master.h5"
    data_1 = tmp_path / "260616_CeO2_raw_data_000001.h5"
    data_2 = tmp_path / "260616_CeO2_raw_data_000002.h5"
    summed = tmp_path / "series_sum_dark_20260618_081052.h5"

    for path in (master, data_1, data_2, summed):
        path.write_bytes(b"test")
    os.utime(master, (100, 100))
    os.utime(data_1, (200, 200))
    os.utime(data_2, (900, 900))
    os.utime(summed, (300, 300))

    all_files = client.get("/api/browse", params={"path": str(tmp_path), "series_mode": "all"})
    grouped = client.get("/api/browse", params={"path": str(tmp_path), "series_mode": "first_only"})

    assert all_files.status_code == 200
    # Without grouping every master/data member is listed individually.
    assert all_files.json()["files"] == [
        "260616_CeO2_raw_data_000001.h5",
        "260616_CeO2_raw_data_000002.h5",
        "260616_CeO2_raw_master.h5",
        "series_sum_dark_20260618_081052.h5",
    ]

    assert grouped.status_code == 200
    grouped_payload = grouped.json()
    # The master represents the acquisition; data files are collapsed; the
    # standalone summed file is untouched.
    assert grouped_payload["files"] == [
        "260616_CeO2_raw_master.h5",
        "series_sum_dark_20260618_081052.h5",
    ]
    items = {item["name"]: item for item in grouped_payload["fileItems"]}
    lead = items["260616_CeO2_raw_master.h5"]
    assert lead["isSeriesLead"] is True
    assert lead["seriesCount"] == 2
    assert lead["mtime"] == 900.0
    assert items["series_sum_dark_20260618_081052.h5"]["isSeriesLead"] is False


def test_browse_endpoint_hdf5_data_only_series_uses_first_as_lead(tmp_path: Path) -> None:
    client = TestClient(app)
    data_1 = tmp_path / "scan_raw_data_000001.h5"
    data_2 = tmp_path / "scan_raw_data_000002.h5"
    for path in (data_1, data_2):
        path.write_bytes(b"test")

    grouped = client.get("/api/browse", params={"path": str(tmp_path), "series_mode": "first_only"})

    assert grouped.status_code == 200
    payload = grouped.json()
    # No master present: fall back to the first data file as the visible lead.
    assert payload["files"] == ["scan_raw_data_000001.h5"]
    lead = payload["fileItems"][0]
    assert lead["isSeriesLead"] is True
    assert lead["seriesCount"] == 2
