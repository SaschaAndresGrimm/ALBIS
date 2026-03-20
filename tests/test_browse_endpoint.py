from __future__ import annotations

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


def test_choose_file_endpoint_supports_expt_extension_filter(tmp_path: Path, monkeypatch) -> None:
    client = TestClient(app)
    geometry_path = (tmp_path / "imported.expt").resolve()
    geometry_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr("backend.routes.files._choose_file", lambda **_kwargs: str(geometry_path))

    response = client.get("/api/choose-file", params={"exts": ".expt"})

    assert response.status_code == 200
    assert response.json()["path"] == str(geometry_path)
