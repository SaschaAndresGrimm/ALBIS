"""Quit and Open Disk Image / Quit and Show in Explorer.

On macOS and with the Windows portable zip, the update cannot be installed
while ALBIS runs, so the last step shows the download and stops ALBIS. These
protect the order of that: nothing is shown, and nothing stops, unless the
request is allowed, a verified file exists and no work would be lost.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import backend.routes.system as system_routes
import backend.services.update_handoff as handoff
from backend import app as app_module
from backend.app import app, runtime_state, update_check_service, update_download_service
from backend.install_kind import (
    INSTALL_KIND_APPIMAGE,
    INSTALL_KIND_MACOS_APP,
    INSTALL_KIND_WINDOWS_INSTALLER,
    INSTALL_KIND_WINDOWS_PORTABLE,
)
from backend.lifecycle import shutdown_controller

ASSET_NAME = "ALBIS-macos-arm64-v1.0.0-abc1234.dmg"


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """A verified download, a launcher that can stop ALBIS, a macOS install."""
    update_download_service.reset()
    update_check_service.clear_cache()
    downloaded = tmp_path / ASSET_NAME
    downloaded.write_bytes(b"image")
    monkeypatch.setattr(update_download_service, "ready_path", lambda: downloaded)
    monkeypatch.setattr(runtime_state, "bind_host", "127.0.0.1")
    monkeypatch.setattr(shutdown_controller, "_hook", lambda: None)
    monkeypatch.setattr(app_module.series_summing, "has_running_job", lambda: False)
    monkeypatch.setattr(app_module.data_export, "has_running_job", lambda: False)
    monkeypatch.setattr(system_routes, "handoff_supported", lambda kind: True)

    log: list[str] = []

    def show(path: Path, kind: str) -> bool:
        log.append(f"show {path.name}")
        return True

    def request(logger, reason: str) -> bool:
        log.append("stop")
        return True

    monkeypatch.setattr(system_routes, "show_for_install", show)
    # Answered at once: the test does not wait for the deferred stop.
    monkeypatch.setattr(shutdown_controller, "request", request)
    yield TestClient(app, base_url="http://127.0.0.1"), log, downloaded


URL = "/api/update-download/open-and-quit"


def test_it_shows_the_download_and_then_stops(client) -> None:
    http, log, downloaded = client
    response = http.post(URL, json={})
    assert response.status_code == 200
    assert response.json() == {"status": "stopping", "path": str(downloaded)}
    # Shown first: once ALBIS has stopped, nothing could show it.
    assert log == [f"show {ASSET_NAME}", "stop"]


def test_it_is_refused_where_the_update_installs_over_a_running_albis(client, monkeypatch) -> None:
    http, log, _ = client
    monkeypatch.setattr(system_routes, "handoff_supported", lambda kind: False)
    assert http.post(URL, json={}).status_code == 403
    assert log == []


def test_a_shared_server_is_not_closed_for_an_update(client, monkeypatch) -> None:
    http, log, _ = client
    monkeypatch.setattr(runtime_state, "bind_host", "0.0.0.0")
    assert http.post(URL, json={}).status_code == 403
    assert log == []


def test_without_a_verified_download_nothing_happens(client, monkeypatch) -> None:
    http, log, _ = client
    monkeypatch.setattr(update_download_service, "ready_path", lambda: None)
    assert http.post(URL, json={}).status_code == 404
    assert log == []


def test_running_work_is_asked_about_before_anything_happens(client, monkeypatch) -> None:
    http, log, _ = client
    monkeypatch.setattr(app_module.series_summing, "has_running_job", lambda: True)
    response = http.post(URL, json={})
    assert response.status_code == 409
    assert response.json()["detail"] == {"code": "busy", "busy": ["series_sum"]}
    assert log == []
    # Confirmed by the user: shown and stopped.
    assert http.post(URL, json={"force": True}).status_code == 200
    assert log == [f"show {ASSET_NAME}", "stop"]


def test_albis_stays_open_when_the_download_cannot_be_shown(client, monkeypatch) -> None:
    """Stopping then would leave neither ALBIS nor the update on screen."""
    http, log, downloaded = client
    monkeypatch.setattr(system_routes, "show_for_install", lambda path, kind: False)
    response = http.post(URL, json={})
    assert response.status_code == 502
    assert str(downloaded) in response.json()["detail"]
    assert log == []


def test_the_request_rejects_unknown_fields(client) -> None:
    http, _, _ = client
    assert http.post(URL, json={"force": False, "path": "/etc"}).status_code == 422


def test_update_check_offers_the_handoff_only_where_albis_can_stop(client, monkeypatch) -> None:
    http, _, _ = client
    monkeypatch.setattr(update_check_service, "_fetch_latest_release", lambda: None)
    assert http.get("/api/update-check").json()["handoff_supported"] is True
    monkeypatch.setattr(shutdown_controller, "_hook", None)
    assert http.get("/api/update-check").json()["handoff_supported"] is False


def test_a_mac_install_opens_the_disk_image_then_stops(monkeypatch, tmp_path) -> None:
    opened: list[Path] = []
    revealed: list[Path] = []
    monkeypatch.setattr(handoff, "open_in_system", lambda p: opened.append(p) or True)
    monkeypatch.setattr(handoff, "reveal_in_file_manager", lambda p: revealed.append(p) or True)

    image = tmp_path / ASSET_NAME
    assert handoff.show_for_install(image, INSTALL_KIND_MACOS_APP) is True
    # Mounting shows the window with ALBIS and Applications; it runs nothing.
    assert opened == [image]
    assert revealed == []


def test_a_portable_zip_is_selected_never_opened(monkeypatch, tmp_path) -> None:
    opened: list[Path] = []
    revealed: list[Path] = []
    monkeypatch.setattr(handoff, "open_in_system", lambda p: opened.append(p) or True)
    monkeypatch.setattr(handoff, "reveal_in_file_manager", lambda p: revealed.append(p) or True)

    archive = tmp_path / "ALBIS-windows-x64-portable.zip"
    assert handoff.show_for_install(archive, INSTALL_KIND_WINDOWS_PORTABLE) is True
    # Opening a zip starts whatever program is registered for it.
    assert opened == []
    assert revealed == [archive]


def test_a_mac_install_never_opens_anything_but_a_disk_image(monkeypatch, tmp_path) -> None:
    opened: list[Path] = []
    monkeypatch.setattr(handoff, "open_in_system", lambda p: opened.append(p) or True)
    monkeypatch.setattr(handoff, "reveal_in_file_manager", lambda p: True)

    handoff.show_for_install(tmp_path / "ALBIS.zip", INSTALL_KIND_MACOS_APP)
    assert opened == []


def test_only_installs_that_hold_their_own_files_hand_off() -> None:
    assert handoff.handoff_supported(INSTALL_KIND_MACOS_APP)
    assert handoff.handoff_supported(INSTALL_KIND_WINDOWS_PORTABLE)
    # The installer closes ALBIS itself; an AppImage can be replaced running.
    assert not handoff.handoff_supported(INSTALL_KIND_WINDOWS_INSTALLER)
    assert not handoff.handoff_supported(INSTALL_KIND_APPIMAGE)
