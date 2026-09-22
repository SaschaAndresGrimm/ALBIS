from __future__ import annotations

import threading
import urllib.request

from fastapi.testclient import TestClient

import backend.routes.system as system_routes
from backend.app import app, runtime_state, update_check_service, update_download_service
from backend.install_kind import INSTALL_KIND_MACOS_APP
from backend.services.update_check import ReleaseAsset, ReleaseMetadata

ASSET_NAME = "ALBIS-macos-arm64-v1.0.0-abc1234.dmg"
ASSET_URL = "https://github.com/SaschaAndresGrimm/ALBIS/releases/download/v1.0.0/" + ASSET_NAME


def _client() -> TestClient:
    return TestClient(app)


def _reset(monkeypatch, *, allow: bool = True) -> None:
    update_download_service.reset()
    update_check_service.clear_cache()
    monkeypatch.setattr(runtime_state, "allow_update_download", allow)


def test_download_status_starts_idle(monkeypatch) -> None:
    _reset(monkeypatch)
    payload = _client().get("/api/update-download/status").json()

    assert payload["status"] == "idle"
    assert payload["checksum"] == "pending"
    assert payload["path"] == ""


def test_update_check_reports_download_support_from_the_setting(monkeypatch) -> None:
    _reset(monkeypatch)
    monkeypatch.setattr(update_check_service, "current_version", "0.9.3")
    monkeypatch.setattr(update_check_service, "install_kind", INSTALL_KIND_MACOS_APP)
    monkeypatch.setattr(update_check_service, "target_arch", "arm64")
    monkeypatch.setattr(
        update_check_service,
        "_fetch_latest_release",
        lambda: ReleaseMetadata(
            "1.0.0",
            "https://example.invalid/releases/v1.0.0",
            (ReleaseAsset(name=ASSET_NAME, download_url=ASSET_URL),),
        ),
    )

    assert _client().get("/api/update-check").json()["download_supported"] is True

    # The release check caches for five minutes, so the setting has to be read
    # per request or switching it off would keep offering the download.
    monkeypatch.setattr(runtime_state, "allow_update_download", False)
    assert _client().get("/api/update-check").json()["download_supported"] is False


def test_update_check_reports_no_download_support_without_a_matching_asset(monkeypatch) -> None:
    _reset(monkeypatch)
    monkeypatch.setattr(update_check_service, "current_version", "0.9.3")
    monkeypatch.setattr(
        update_check_service,
        "_fetch_latest_release",
        lambda: ReleaseMetadata("1.0.0", "https://example.invalid/releases/v1.0.0"),
    )

    # A Docker or source install updates by command; the setting cannot conjure
    # an asset to fetch.
    assert _client().get("/api/update-check").json()["download_supported"] is False


def test_start_is_refused_when_the_setting_is_off(monkeypatch) -> None:
    _reset(monkeypatch, allow=False)
    response = _client().post(
        "/api/update-download/start", json={"url": ASSET_URL, "name": ASSET_NAME}
    )

    assert response.status_code == 403
    assert update_download_service.status()["status"] == "idle"


def test_start_rejects_a_url_or_name_the_backend_would_never_have_offered(monkeypatch) -> None:
    _reset(monkeypatch)
    client = _client()

    for body in (
        {"url": "https://evil.example/ALBIS.dmg", "name": ASSET_NAME},
        {"url": "http://github.com/a/b/releases/download/v1/x.dmg", "name": "x.dmg"},
        {"url": ASSET_URL, "name": "../../etc/passwd"},
        {"url": ASSET_URL, "name": "sub/dir.dmg"},
    ):
        assert client.post("/api/update-download/start", json=body).status_code == 400

    assert update_download_service.status()["status"] == "idle"


def test_start_while_a_download_runs_conflicts(monkeypatch) -> None:
    _reset(monkeypatch)
    release = threading.Event()

    class _BlockingOpener:
        def open(self, request, timeout=None):
            release.wait(5.0)
            raise OSError("cancelled by test")

    monkeypatch.setattr(urllib.request, "build_opener", lambda *_h: _BlockingOpener())
    client = _client()

    first = client.post("/api/update-download/start", json={"url": ASSET_URL, "name": ASSET_NAME})
    try:
        assert first.status_code == 200
        assert first.json()["status"] == "downloading"
        second = client.post(
            "/api/update-download/start", json={"url": ASSET_URL, "name": ASSET_NAME}
        )
        assert second.status_code == 409
    finally:
        release.set()


def test_reveal_is_a_404_until_a_verified_download_exists(monkeypatch) -> None:
    _reset(monkeypatch)
    assert _client().post("/api/update-download/reveal").status_code == 404


def test_reveal_opens_the_containing_folder_not_the_installer(monkeypatch, tmp_path) -> None:
    _reset(monkeypatch)
    downloaded = tmp_path / ASSET_NAME
    downloaded.write_bytes(b"installer")
    monkeypatch.setattr(update_download_service, "ready_path", lambda: downloaded)

    opened: list[str] = []
    monkeypatch.setattr(
        system_routes, "open_in_system", lambda path: opened.append(str(path)) or True
    )

    payload = _client().post("/api/update-download/reveal").json()

    # Opening a .exe is running it. ALBIS shows the folder and lets the user
    # apply the update.
    assert opened == [str(tmp_path)]
    assert payload["path"] == str(downloaded)
    assert payload["opened"] is True
