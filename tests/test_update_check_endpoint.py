from __future__ import annotations

import ssl

from fastapi.testclient import TestClient

from backend.app import app, update_check_service
from backend.install_kind import (
    INSTALL_KIND_APPIMAGE,
    INSTALL_KIND_DOCKER,
    INSTALL_KIND_MACOS_APP,
    INSTALL_KIND_SOURCE,
    INSTALL_KIND_WINDOWS_INSTALLER,
    INSTALL_KIND_WINDOWS_PORTABLE,
)
from backend.services.update_check import (
    RELEASES_PAGE_URL,
    ReleaseAsset,
    ReleaseMetadata,
    _parse_assets,
    _ssl_context,
    select_download_asset,
    update_command_for,
)

# The asset names a real release carries, from the upload steps in
# `.github/workflows/release.yml`. Held here as a fixture so a rename in the
# workflow that would silently stop matching shows up as a failing test.
RELEASE_ASSET_NAMES = (
    "ALBIS-0.9.4-x86_64.AppImage",
    "ALBIS-0.9.4-x86_64.AppImage.sig",
    "ALBIS-0.9.4-x86_64-appimage-bundle.tar.gz",
    "ALBIS-linux-x64-v0.9.4-abc1234.tar.gz",
    "ALBIS-Setup-windows-x64-v0.9.4-abc1234.exe",
    "ALBIS-windows-x64-v0.9.4-abc1234.zip",
    "ALBIS-macos-arm64-v0.9.4-abc1234.dmg",
    "ALBIS-macos-x64-v0.9.4-abc1234.dmg",
    "SHA256SUMS.txt",
    "SHA256SUMS.txt.sig",
)

RELEASE_ASSETS = tuple(
    ReleaseAsset(
        name=name,
        download_url=f"https://github.com/SaschaAndresGrimm/ALBIS/releases/download/v0.9.4/{name}",
    )
    for name in RELEASE_ASSET_NAMES
)


def test_ssl_context_uses_verifying_certifi_bundle() -> None:
    # Packaged builds have no system trust store, so the GitHub update check must
    # verify against certifi's CA bundle rather than failing the TLS handshake.
    context = _ssl_context()
    assert isinstance(context, ssl.SSLContext)
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True


def _request_update_check() -> dict[str, str]:
    response = TestClient(app).get("/api/update-check")
    assert response.status_code == 200
    return response.json()


def test_update_check_endpoint_reports_newer_release(monkeypatch) -> None:
    update_check_service.clear_cache()
    monkeypatch.setattr(update_check_service, "current_version", "0.9.3")
    monkeypatch.setattr(
        update_check_service,
        "_fetch_latest_release",
        lambda: ReleaseMetadata("0.9.4", "https://example.invalid/releases/v0.9.4"),
    )

    payload = _request_update_check()

    assert payload == {
        "status": "update_available",
        "current_version": "0.9.3",
        "latest_version": "0.9.4",
        "release_url": "https://example.invalid/releases/v0.9.4",
        "message": "",
        # A checkout is what the test suite runs from, and it updates by
        # command rather than by downloading a release asset.
        "install_kind": INSTALL_KIND_SOURCE,
        "download_url": "",
        "download_name": "",
        "update_command": "git pull && pip install -r backend/requirements.txt",
        # No asset to fetch, so the in-app download is not offered whatever
        # `ui.allow_update_download` says, and a source checkout is never a
        # platform ALBIS applies an update on.
        "download_supported": False,
        "apply_supported": False,
    }


def test_update_check_endpoint_reports_up_to_date_for_equal_release(monkeypatch) -> None:
    update_check_service.clear_cache()
    monkeypatch.setattr(update_check_service, "current_version", "0.9.3")
    monkeypatch.setattr(
        update_check_service,
        "_fetch_latest_release",
        lambda: ReleaseMetadata("0.9.3", "https://example.invalid/releases/v0.9.3"),
    )

    payload = _request_update_check()

    assert payload["status"] == "up_to_date"
    assert payload["current_version"] == "0.9.3"
    assert payload["latest_version"] == "0.9.3"


def test_update_check_endpoint_reports_up_to_date_when_current_version_is_ahead(
    monkeypatch,
) -> None:
    update_check_service.clear_cache()
    monkeypatch.setattr(update_check_service, "current_version", "1.0.1")
    monkeypatch.setattr(
        update_check_service,
        "_fetch_latest_release",
        lambda: ReleaseMetadata("1.0.0", "https://example.invalid/releases/v1.0.0"),
    )

    payload = _request_update_check()

    assert payload["status"] == "up_to_date"
    assert payload["current_version"] == "1.0.1"
    assert payload["latest_version"] == "1.0.0"


def test_update_check_endpoint_treats_stable_release_as_newer_than_matching_prerelease(
    monkeypatch,
) -> None:
    update_check_service.clear_cache()
    monkeypatch.setattr(update_check_service, "current_version", "1.0.0-rc.1")
    monkeypatch.setattr(
        update_check_service,
        "_fetch_latest_release",
        lambda: ReleaseMetadata("1.0.0", "https://example.invalid/releases/v1.0.0"),
    )

    payload = _request_update_check()

    assert payload["status"] == "update_available"
    assert payload["current_version"] == "1.0.0-rc.1"
    assert payload["latest_version"] == "1.0.0"


def test_update_check_endpoint_returns_unavailable_for_timeout_or_malformed_response(
    monkeypatch,
) -> None:
    for failure in (TimeoutError("timed out"), ValueError("bad payload")):
        update_check_service.clear_cache()
        monkeypatch.setattr(update_check_service, "current_version", "0.9.3")
        monkeypatch.setattr(
            update_check_service,
            "_fetch_latest_release",
            lambda failure=failure: (_ for _ in ()).throw(failure),
        )

        payload = _request_update_check()

        assert payload["status"] == "unavailable"
        assert payload["current_version"] == "0.9.3"
        assert payload["latest_version"] == ""
        assert payload["release_url"] == RELEASES_PAGE_URL
        assert payload["message"]


def test_release_assets_are_parsed_and_non_github_urls_are_dropped() -> None:
    # The URL is opened in the user's browser, so a payload that names any host
    # but GitHub must lose its asset rather than get a click.
    parsed = _parse_assets(
        [
            {
                "name": "ALBIS-0.9.4-x86_64.AppImage",
                "browser_download_url": (
                    "https://github.com/SaschaAndresGrimm/ALBIS/releases/download/"
                    "v0.9.4/ALBIS-0.9.4-x86_64.AppImage"
                ),
            },
            {"name": "evil.AppImage", "browser_download_url": "https://example.invalid/evil"},
            {"name": "no-url.AppImage"},
            {"name": "", "browser_download_url": "https://github.com/x"},
            "not-a-dict",
        ]
    )

    assert [asset.name for asset in parsed] == ["ALBIS-0.9.4-x86_64.AppImage"]


def test_release_assets_tolerate_a_missing_or_malformed_array() -> None:
    assert _parse_assets(None) == ()
    assert _parse_assets({"name": "x"}) == ()


def test_each_install_kind_selects_the_asset_built_for_it() -> None:
    expected = {
        (INSTALL_KIND_APPIMAGE, "x64"): "ALBIS-0.9.4-x86_64.AppImage",
        (INSTALL_KIND_WINDOWS_INSTALLER, "x64"): "ALBIS-Setup-windows-x64-v0.9.4-abc1234.exe",
        (INSTALL_KIND_WINDOWS_PORTABLE, "x64"): "ALBIS-windows-x64-v0.9.4-abc1234.zip",
        (INSTALL_KIND_MACOS_APP, "arm64"): "ALBIS-macos-arm64-v0.9.4-abc1234.dmg",
        (INSTALL_KIND_MACOS_APP, "x64"): "ALBIS-macos-x64-v0.9.4-abc1234.dmg",
    }

    for (install_kind, arch), asset_name in expected.items():
        asset = select_download_asset(RELEASE_ASSETS, install_kind, arch)
        assert asset is not None, f"No asset selected for {install_kind}/{arch}"
        assert asset.name == asset_name


def test_asset_selection_never_offers_a_signature_or_checksum_file() -> None:
    for install_kind in (
        INSTALL_KIND_APPIMAGE,
        INSTALL_KIND_WINDOWS_INSTALLER,
        INSTALL_KIND_WINDOWS_PORTABLE,
        INSTALL_KIND_MACOS_APP,
    ):
        for arch in ("x64", "arm64"):
            asset = select_download_asset(RELEASE_ASSETS, install_kind, arch)
            if asset is None:
                continue
            assert not asset.name.endswith(".sig")
            assert "SHA256SUMS" not in asset.name


def test_asset_selection_offers_nothing_for_an_unknown_architecture() -> None:
    # A download for the wrong architecture is worse than no download, so an
    # architecture that does not appear in the release names must match nothing.
    for arch in ("", "ppc64le", "riscv64"):
        assert select_download_asset(RELEASE_ASSETS, INSTALL_KIND_MACOS_APP, arch) is None


def test_asset_selection_offers_nothing_for_command_updated_installs() -> None:
    for install_kind in (INSTALL_KIND_DOCKER, INSTALL_KIND_SOURCE, "something-new"):
        assert select_download_asset(RELEASE_ASSETS, install_kind, "x64") is None


def test_update_command_names_the_published_image_and_tag() -> None:
    assert update_command_for(INSTALL_KIND_DOCKER, "1.0.0") == (
        "docker pull ghcr.io/saschaandresgrimm/albis:v1.0.0"
    )
    # A release whose version could not be read still gets a usable command.
    assert update_command_for(INSTALL_KIND_DOCKER, "") == (
        "docker pull ghcr.io/saschaandresgrimm/albis:latest"
    )
    assert update_command_for(INSTALL_KIND_SOURCE, "1.0.0").startswith("git pull")
    assert update_command_for(INSTALL_KIND_MACOS_APP, "1.0.0") == ""


def test_update_check_endpoint_offers_the_matching_asset_for_a_packaged_install(
    monkeypatch,
) -> None:
    update_check_service.clear_cache()
    monkeypatch.setattr(update_check_service, "current_version", "0.9.3")
    monkeypatch.setattr(update_check_service, "install_kind", INSTALL_KIND_MACOS_APP)
    monkeypatch.setattr(update_check_service, "target_arch", "arm64")
    monkeypatch.setattr(
        update_check_service,
        "_fetch_latest_release",
        lambda: ReleaseMetadata("0.9.4", "https://example.invalid/releases/v0.9.4", RELEASE_ASSETS),
    )

    payload = _request_update_check()

    assert payload["status"] == "update_available"
    assert payload["install_kind"] == INSTALL_KIND_MACOS_APP
    assert payload["download_name"] == "ALBIS-macos-arm64-v0.9.4-abc1234.dmg"
    assert payload["download_url"].startswith("https://github.com/SaschaAndresGrimm/ALBIS/")
    assert payload["update_command"] == ""


def test_update_check_endpoint_offers_a_pull_command_inside_docker(monkeypatch) -> None:
    update_check_service.clear_cache()
    monkeypatch.setattr(update_check_service, "current_version", "0.9.3")
    monkeypatch.setattr(update_check_service, "install_kind", INSTALL_KIND_DOCKER)
    monkeypatch.setattr(update_check_service, "target_arch", "x64")
    monkeypatch.setattr(
        update_check_service,
        "_fetch_latest_release",
        lambda: ReleaseMetadata("0.9.4", "https://example.invalid/releases/v0.9.4", RELEASE_ASSETS),
    )

    payload = _request_update_check()

    # A container cannot replace its own image, so it must never be handed a
    # desktop installer to download.
    assert payload["download_url"] == ""
    assert payload["download_name"] == ""
    assert payload["update_command"] == "docker pull ghcr.io/saschaandresgrimm/albis:v0.9.4"


def test_update_check_endpoint_withholds_download_and_command_when_up_to_date(
    monkeypatch,
) -> None:
    update_check_service.clear_cache()
    monkeypatch.setattr(update_check_service, "current_version", "0.9.4")
    monkeypatch.setattr(update_check_service, "install_kind", INSTALL_KIND_MACOS_APP)
    monkeypatch.setattr(update_check_service, "target_arch", "arm64")
    monkeypatch.setattr(
        update_check_service,
        "_fetch_latest_release",
        lambda: ReleaseMetadata("0.9.4", "https://example.invalid/releases/v0.9.4", RELEASE_ASSETS),
    )

    payload = _request_update_check()

    assert payload["status"] == "up_to_date"
    assert payload["install_kind"] == INSTALL_KIND_MACOS_APP
    assert payload["download_url"] == ""
    assert payload["update_command"] == ""


def test_update_check_endpoint_still_reports_install_kind_when_github_is_unreachable(
    monkeypatch,
) -> None:
    update_check_service.clear_cache()
    monkeypatch.setattr(update_check_service, "install_kind", INSTALL_KIND_APPIMAGE)
    monkeypatch.setattr(
        update_check_service,
        "_fetch_latest_release",
        lambda: (_ for _ in ()).throw(TimeoutError("timed out")),
    )

    payload = _request_update_check()

    assert payload["status"] == "unavailable"
    assert payload["install_kind"] == INSTALL_KIND_APPIMAGE
    assert payload["release_url"] == RELEASES_PAGE_URL
