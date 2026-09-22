from __future__ import annotations

import sys
from pathlib import Path

from backend import install_kind as module
from backend.install_kind import (
    INSTALL_KIND_APPIMAGE,
    INSTALL_KIND_DOCKER,
    INSTALL_KIND_MACOS_APP,
    INSTALL_KIND_SOURCE,
    INSTALL_KIND_WINDOWS_INSTALLER,
    INSTALL_KIND_WINDOWS_PORTABLE,
    INSTALL_KINDS,
    read_install_kind,
    read_target_arch,
)


def _isolate(monkeypatch, *, frozen: bool, platform: str, docker: bool = False) -> None:
    """Pin every input `read_install_kind` reads, so a test states all of them."""
    monkeypatch.setattr(module, "_is_frozen", lambda: frozen)
    monkeypatch.setattr(sys, "platform", platform)
    monkeypatch.setattr(module, "_docker_from_env", lambda: docker)
    monkeypatch.setattr(module, "_docker_marker_present", lambda: False)


def test_docker_env_var_wins_over_a_frozen_desktop_build(monkeypatch) -> None:
    # A container cannot replace its own image whatever it was built from, so
    # the container signal has to outrank the platform.
    _isolate(monkeypatch, frozen=True, platform="linux", docker=True)
    assert read_install_kind() == INSTALL_KIND_DOCKER


def test_dockerenv_marker_is_honoured_for_images_not_built_here(monkeypatch) -> None:
    _isolate(monkeypatch, frozen=False, platform="linux")
    monkeypatch.setattr(module, "_docker_marker_present", lambda: True)
    assert read_install_kind() == INSTALL_KIND_DOCKER


def test_docker_env_var_is_read_as_a_flag_not_as_text(monkeypatch) -> None:
    for raw, expected in (
        ("1", True),
        ("true", True),
        ("yes", True),
        ("0", False),
        ("false", False),
        ("", False),
    ):
        monkeypatch.setenv("ALBIS_IN_DOCKER", raw)
        assert module._docker_from_env() is expected, raw
    monkeypatch.delenv("ALBIS_IN_DOCKER", raising=False)
    assert module._docker_from_env() is False


def test_an_unfrozen_run_is_a_checkout_on_every_platform(monkeypatch) -> None:
    for platform in ("linux", "darwin", "win32"):
        _isolate(monkeypatch, frozen=False, platform=platform)
        assert read_install_kind() == INSTALL_KIND_SOURCE


def test_frozen_linux_and_macos_builds_report_their_shipping_format(monkeypatch) -> None:
    _isolate(monkeypatch, frozen=True, platform="linux")
    assert read_install_kind() == INSTALL_KIND_APPIMAGE

    _isolate(monkeypatch, frozen=True, platform="darwin")
    assert read_install_kind() == INSTALL_KIND_MACOS_APP


def test_an_unknown_frozen_platform_degrades_to_the_generic_answer(monkeypatch) -> None:
    _isolate(monkeypatch, frozen=True, platform="freebsd14")
    assert read_install_kind() == INSTALL_KIND_SOURCE


def test_windows_install_registered_at_the_running_location_is_the_installer(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        module, "_windows_registered_install_location", lambda: r"C:\Users\x\Programs\ALBIS"
    )
    monkeypatch.setattr(
        module, "_executable_dir", lambda: Path(r"C:\Users\x\Programs\ALBIS").resolve()
    )
    assert module._windows_install_kind() == INSTALL_KIND_WINDOWS_INSTALLER


def test_windows_install_without_an_uninstall_key_is_the_portable_zip(monkeypatch) -> None:
    # Pointing a portable user at the installer would give them a second ALBIS
    # rather than a newer one, so "no key" must not read as "installed".
    monkeypatch.setattr(module, "_windows_registered_install_location", lambda: "")
    monkeypatch.setattr(module, "_executable_dir", lambda: Path(r"D:\tools\albis"))
    assert module._windows_install_kind() == INSTALL_KIND_WINDOWS_PORTABLE


def test_windows_install_running_from_outside_the_registered_location_is_portable(
    monkeypatch,
) -> None:
    # Both are installed and the portable copy is the one running.
    monkeypatch.setattr(
        module, "_windows_registered_install_location", lambda: r"C:\Users\x\Programs\ALBIS"
    )
    monkeypatch.setattr(module, "_executable_dir", lambda: Path(r"D:\tools\albis").resolve())
    assert module._windows_install_kind() == INSTALL_KIND_WINDOWS_PORTABLE


def test_windows_install_falls_back_to_the_installer_when_the_path_is_unusable(
    monkeypatch,
) -> None:
    # A key exists, so this is an installed copy; only the comparison failed.
    monkeypatch.setattr(
        module, "_windows_registered_install_location", lambda: r"C:\Users\x\Programs\ALBIS"
    )
    monkeypatch.setattr(module, "_executable_dir", lambda: None)
    assert module._windows_install_kind() == INSTALL_KIND_WINDOWS_INSTALLER


def test_registry_lookup_is_silent_when_winreg_is_absent() -> None:
    # Every non-Windows platform imports this module, so the lookup must return
    # empty rather than raise ImportError.
    if sys.platform != "win32":
        assert module._windows_registered_install_location() == ""


def test_install_kind_is_always_one_of_the_published_values(monkeypatch) -> None:
    for platform in ("linux", "darwin", "win32", "freebsd14"):
        for frozen in (True, False):
            _isolate(monkeypatch, frozen=frozen, platform=platform)
            assert read_install_kind() in INSTALL_KINDS


def test_a_failing_probe_never_propagates_out_of_the_detection(monkeypatch) -> None:
    def explode() -> bool:
        raise RuntimeError("registry on fire")

    monkeypatch.setattr(module, "_docker_from_env", explode)
    assert read_install_kind() == INSTALL_KIND_SOURCE


def test_architecture_is_normalized_onto_the_release_asset_vocabulary(monkeypatch) -> None:
    for raw, expected in (
        ("x86_64", "x64"),
        ("AMD64", "x64"),
        ("x64", "x64"),
        ("arm64", "arm64"),
        ("aarch64", "arm64"),
        # Unrecognised values pass through so they match no asset, rather than
        # being coerced into one that would be the wrong binary.
        ("riscv64", "riscv64"),
        ("", ""),
    ):
        monkeypatch.setattr(module.platform, "machine", lambda raw=raw: raw)
        assert read_target_arch() == expected
