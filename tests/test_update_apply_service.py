from __future__ import annotations

import logging
import stat
import threading
import time
from pathlib import Path

import pytest

from backend import lifecycle
from backend.install_kind import (
    INSTALL_KIND_APPIMAGE,
    INSTALL_KIND_DOCKER,
    INSTALL_KIND_MACOS_APP,
    INSTALL_KIND_SOURCE,
    INSTALL_KIND_WINDOWS_INSTALLER,
    INSTALL_KIND_WINDOWS_PORTABLE,
)
from backend.lifecycle import ShutdownController
from backend.services import update_apply as module
from backend.services.update_apply import (
    REASON_BUSY,
    REASON_DISABLED,
    REASON_NO_DOWNLOAD,
    REASON_NO_SHUTDOWN,
    REASON_NO_TARGET,
    REASON_UNSUPPORTED,
    REASON_UNVERIFIED,
    STATUS_APPLIED,
    STATUS_FAILED,
    WINDOWS_INSTALLER_ARGS,
    ApplyRefusedError,
    UpdateApplyService,
    appimage_target_path,
)

LOGGER = logging.getLogger("test")


def _verified_status(**overrides) -> dict[str, object]:
    status = {"status": "ready", "checksum": "verified", "signature": "unavailable"}
    status.update(overrides)
    return status


def _service(
    *,
    ready: Path | None,
    status: dict[str, object] | None = None,
    shutdown: ShutdownController | None = None,
) -> tuple[UpdateApplyService, list[str]]:
    fired: list[str] = []
    controller = shutdown if shutdown is not None else ShutdownController()
    if shutdown is None:
        controller.register(lambda: fired.append("shutdown"))
    return (
        UpdateApplyService(
            logger=LOGGER,
            shutdown=controller,
            ready_download=lambda: ready,
            download_status=lambda: status if status is not None else _verified_status(),
        ),
        fired,
    )


def _wait_for(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("Condition was not reached in time")


# -- eligibility ------------------------------------------------------------


def test_applying_is_off_unless_the_site_turned_it_on(tmp_path) -> None:
    # The default. A viewer that replaces itself is not something a beamline
    # workstation should do without being asked.
    service, _ = _service(ready=tmp_path / "x")
    assert (
        service.refusal_code(install_kind=INSTALL_KIND_APPIMAGE, allowed=False) == REASON_DISABLED
    )


def test_only_the_two_platforms_that_can_do_it_safely_are_offered(tmp_path, monkeypatch) -> None:
    installer = tmp_path / "ALBIS-Setup.exe"
    installer.write_bytes(b"installer")
    service, _ = _service(ready=installer)

    for install_kind in (
        INSTALL_KIND_MACOS_APP,
        INSTALL_KIND_DOCKER,
        INSTALL_KIND_SOURCE,
        INSTALL_KIND_WINDOWS_PORTABLE,
        "something-new",
    ):
        assert (
            service.refusal_code(install_kind=install_kind, allowed=True) == REASON_UNSUPPORTED
        ), install_kind

    # The Windows installer is one of the two that can.
    assert service.refusal_code(install_kind=INSTALL_KIND_WINDOWS_INSTALLER, allowed=True) is None


def test_a_source_run_cannot_close_itself_so_is_refused(tmp_path) -> None:
    # No launcher means nothing can stop the server, and an update that needs
    # the process gone would silently do nothing.
    bare = ShutdownController()
    service, _ = _service(ready=tmp_path / "x", shutdown=bare)
    assert (
        service.refusal_code(install_kind=INSTALL_KIND_WINDOWS_INSTALLER, allowed=True)
        == REASON_NO_SHUTDOWN
    )


def test_an_appimage_that_cannot_be_located_is_refused(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv(module.APPIMAGE_PATH_ENV, raising=False)
    service, _ = _service(ready=tmp_path / "x")
    assert (
        service.refusal_code(install_kind=INSTALL_KIND_APPIMAGE, allowed=True) == REASON_NO_TARGET
    )


def test_live_work_refuses_the_apply(tmp_path) -> None:
    installer = tmp_path / "ALBIS-Setup.exe"
    installer.write_bytes(b"installer")
    service, _ = _service(ready=installer)
    assert (
        service.refusal_code(
            install_kind=INSTALL_KIND_WINDOWS_INSTALLER, allowed=True, busy=["live_watch"]
        )
        == REASON_BUSY
    )


def test_without_a_download_there_is_nothing_to_apply(tmp_path) -> None:
    service, _ = _service(ready=None)
    assert (
        service.refusal_code(install_kind=INSTALL_KIND_WINDOWS_INSTALLER, allowed=True)
        == REASON_NO_DOWNLOAD
    )


def test_an_unverified_download_is_offered_as_a_file_but_never_installed(tmp_path) -> None:
    """The asymmetry the feature rests on.

    `update_download` hands over a file it could not verify, because that is no
    worse than a browser download. Running it for the user is a different act,
    so here the same file is refused.
    """
    installer = tmp_path / "ALBIS-Setup.exe"
    installer.write_bytes(b"installer")

    for status in (
        _verified_status(checksum="unavailable"),
        _verified_status(checksum="mismatch"),
        _verified_status(checksum="pending"),
        _verified_status(signature="invalid"),
    ):
        service, _ = _service(ready=installer, status=status)
        assert (
            service.refusal_code(install_kind=INSTALL_KIND_WINDOWS_INSTALLER, allowed=True)
            == REASON_UNVERIFIED
        ), status


def test_refusals_are_reported_in_the_order_that_matters_to_the_user(tmp_path) -> None:
    # A setting the user controls outranks a platform that will never support
    # it, which outranks the state of their session.
    service, _ = _service(ready=None)
    assert (
        service.refusal_code(install_kind=INSTALL_KIND_DOCKER, allowed=False, busy=["live_watch"])
        == REASON_DISABLED
    )
    assert (
        service.refusal_code(install_kind=INSTALL_KIND_DOCKER, allowed=True, busy=["live_watch"])
        == REASON_UNSUPPORTED
    )


def test_apply_raises_the_refusal_code_it_would_report(tmp_path) -> None:
    service, fired = _service(ready=None)
    with pytest.raises(ApplyRefusedError) as excinfo:
        service.apply(install_kind=INSTALL_KIND_APPIMAGE, allowed=False)
    assert excinfo.value.code == REASON_DISABLED
    assert fired == []


# -- the AppImage target ----------------------------------------------------


def test_the_appimage_target_comes_from_the_environment_but_is_checked(
    tmp_path, monkeypatch
) -> None:
    appimage = tmp_path / "ALBIS.AppImage"
    appimage.write_bytes(b"old")
    monkeypatch.setenv(module.APPIMAGE_PATH_ENV, str(appimage))
    assert appimage_target_path() == appimage.resolve()

    # A path that does not exist, names a directory, or is not set at all means
    # the target cannot be established -- refused rather than guessed.
    monkeypatch.setenv(module.APPIMAGE_PATH_ENV, str(tmp_path / "missing.AppImage"))
    assert appimage_target_path() is None
    monkeypatch.setenv(module.APPIMAGE_PATH_ENV, str(tmp_path))
    assert appimage_target_path() is None
    monkeypatch.setenv(module.APPIMAGE_PATH_ENV, "   ")
    assert appimage_target_path() is None
    monkeypatch.delenv(module.APPIMAGE_PATH_ENV)
    assert appimage_target_path() is None


# -- applying ---------------------------------------------------------------


def test_applying_an_appimage_swaps_the_file_and_keeps_its_mode(tmp_path, monkeypatch) -> None:
    appimage = tmp_path / "install" / "ALBIS.AppImage"
    appimage.parent.mkdir()
    appimage.write_bytes(b"old version")
    appimage.chmod(0o755)
    # Read back rather than assumed to be 0o755: Windows does not implement
    # Unix modes and reports 0o666 whatever chmod was given. The contract
    # under test is that the mode is *carried over* from the file being
    # replaced, which is the same promise on every platform.
    original_mode = stat.S_IMODE(appimage.stat().st_mode)
    monkeypatch.setenv(module.APPIMAGE_PATH_ENV, str(appimage))

    downloaded = tmp_path / "Downloads" / "ALBIS-1.0.0-x86_64.AppImage"
    downloaded.parent.mkdir()
    downloaded.write_bytes(b"new version")
    downloaded.chmod(0o644)

    monkeypatch.setattr(lifecycle, "SHUTDOWN_DELAY_SECONDS", 0.01)
    service, fired = _service(ready=downloaded)
    service.apply(install_kind=INSTALL_KIND_APPIMAGE, allowed=True)

    _wait_for(lambda: service.status()["status"] == STATUS_APPLIED)
    assert appimage.read_bytes() == b"new version"
    # An AppImage that is not executable is not an application; the mode is
    # carried over from the file being replaced rather than assumed. The
    # downloaded file's own mode (0o644 here) must not survive the swap.
    assert stat.S_IMODE(appimage.stat().st_mode) == original_mode
    # Staged beside the target, then renamed: nothing is left behind.
    assert sorted(p.name for p in appimage.parent.iterdir()) == ["ALBIS.AppImage"]
    # The downloaded file is copied, not consumed.
    assert downloaded.exists()
    _wait_for(lambda: fired == ["shutdown"])


def test_a_failed_appimage_swap_leaves_no_staged_file_and_does_not_close_albis(
    tmp_path, monkeypatch
) -> None:
    appimage = tmp_path / "install" / "ALBIS.AppImage"
    appimage.parent.mkdir()
    appimage.write_bytes(b"old version")
    monkeypatch.setenv(module.APPIMAGE_PATH_ENV, str(appimage))

    downloaded = tmp_path / "ALBIS-1.0.0-x86_64.AppImage"
    downloaded.write_bytes(b"new version")

    def _explode(*_args, **_kwargs):
        raise OSError("no space left on device")

    monkeypatch.setattr(module.shutil, "copyfile", _explode)
    service, fired = _service(ready=downloaded)
    service.apply(install_kind=INSTALL_KIND_APPIMAGE, allowed=True)

    _wait_for(lambda: service.status()["status"] == STATUS_FAILED)
    assert appimage.read_bytes() == b"old version"
    assert sorted(p.name for p in appimage.parent.iterdir()) == ["ALBIS.AppImage"]
    # Closing ALBIS for an update that did not happen would be the worst of
    # both outcomes.
    assert fired == []
    assert service.status()["message"]


def test_a_partial_copy_is_removed_rather_than_left_looking_like_a_spare(
    tmp_path, monkeypatch
) -> None:
    appimage = tmp_path / "ALBIS.AppImage"
    appimage.write_bytes(b"old version")
    monkeypatch.setenv(module.APPIMAGE_PATH_ENV, str(appimage))
    downloaded = tmp_path / "new.AppImage"
    downloaded.write_bytes(b"new version")

    real_copyfile = module.shutil.copyfile

    def _copy_then_fail(src, dst, **kwargs):
        real_copyfile(src, dst, **kwargs)
        raise OSError("interrupted after writing")

    monkeypatch.setattr(module.shutil, "copyfile", _copy_then_fail)
    service, _ = _service(ready=downloaded)
    service.apply(install_kind=INSTALL_KIND_APPIMAGE, allowed=True)

    _wait_for(lambda: service.status()["status"] == STATUS_FAILED)
    assert appimage.read_bytes() == b"old version"
    assert [p.name for p in tmp_path.iterdir() if p.name.startswith(".")] == []


def test_applying_on_windows_hands_the_job_to_the_installer(tmp_path, monkeypatch) -> None:
    """The installer already knows how to close ALBIS and replace its files.

    `scripts/installer_windows.iss` signals `ALBISShutdownEvent`, waits, then
    falls back to taskkill -- so applying is running it silently, not
    reimplementing any of that.
    """
    installer = tmp_path / "ALBIS-Setup-windows-x64-v1.0.0-abc1234.exe"
    installer.write_bytes(b"installer")

    spawned: list[list[str]] = []

    def _fake_popen(args, **kwargs):
        spawned.append(list(args))
        assert kwargs.get("close_fds") is True
        return object()

    monkeypatch.setattr(module.subprocess, "Popen", _fake_popen)
    monkeypatch.setattr(lifecycle, "SHUTDOWN_DELAY_SECONDS", 0.01)
    service, fired = _service(ready=installer)
    service.apply(install_kind=INSTALL_KIND_WINDOWS_INSTALLER, allowed=True)

    _wait_for(lambda: service.status()["status"] == STATUS_APPLIED)
    assert spawned == [[str(installer), *WINDOWS_INSTALLER_ARGS]]
    assert "/SILENT" in WINDOWS_INSTALLER_ARGS
    _wait_for(lambda: fired == ["shutdown"])


def test_a_second_apply_while_one_is_running_is_ignored(tmp_path, monkeypatch) -> None:
    installer = tmp_path / "ALBIS-Setup.exe"
    installer.write_bytes(b"installer")
    release = threading.Event()

    def _blocking_popen(args, **kwargs):
        release.wait(5.0)
        return object()

    monkeypatch.setattr(module.subprocess, "Popen", _blocking_popen)
    service, _ = _service(ready=installer)
    try:
        first = service.apply(install_kind=INSTALL_KIND_WINDOWS_INSTALLER, allowed=True)
        second = service.apply(install_kind=INSTALL_KIND_WINDOWS_INSTALLER, allowed=True)
        assert first["status"] == "applying"
        assert second["status"] == "applying"
    finally:
        release.set()


def test_an_applied_update_reports_what_to_do_when_albis_cannot_close_itself(
    tmp_path, monkeypatch
) -> None:
    # The hook is gone between the eligibility check and the swap. The update
    # is on disk, so this says what the user now has to do rather than calling
    # the whole thing a failure.
    appimage = tmp_path / "ALBIS.AppImage"
    appimage.write_bytes(b"old")
    monkeypatch.setenv(module.APPIMAGE_PATH_ENV, str(appimage))
    downloaded = tmp_path / "new.AppImage"
    downloaded.write_bytes(b"new")

    controller = ShutdownController()
    controller.register(lambda: None)
    service, _ = _service(ready=downloaded, shutdown=controller)
    monkeypatch.setattr(controller, "request", lambda *_args, **_kwargs: False)

    service.apply(install_kind=INSTALL_KIND_APPIMAGE, allowed=True)
    _wait_for(lambda: service.status()["status"] == STATUS_APPLIED)

    assert appimage.read_bytes() == b"new"
    assert service.status()["message"]


# -- the shutdown controller ------------------------------------------------


def test_shutdown_is_unavailable_until_a_launcher_registers_one() -> None:
    controller = ShutdownController()
    assert controller.is_available() is False
    assert controller.request(LOGGER, "test") is False

    controller.register(lambda: None)
    assert controller.is_available() is True


def test_shutdown_is_deferred_so_the_response_can_be_delivered(monkeypatch) -> None:
    monkeypatch.setattr(lifecycle, "SHUTDOWN_DELAY_SECONDS", 0.05)
    fired = threading.Event()
    controller = ShutdownController()
    controller.register(fired.set)

    assert controller.request(LOGGER, "test") is True
    # Not immediately: a response cannot be sent by a process that has exited.
    assert not fired.is_set()
    assert fired.wait(2.0)


def test_a_second_request_replaces_the_pending_one(monkeypatch) -> None:
    monkeypatch.setattr(lifecycle, "SHUTDOWN_DELAY_SECONDS", 0.05)
    calls: list[int] = []
    controller = ShutdownController()
    controller.register(lambda: calls.append(1))

    controller.request(LOGGER, "first")
    controller.request(LOGGER, "second")
    time.sleep(0.3)

    assert calls == [1]


def test_a_failing_hook_does_not_escape_the_timer_thread(monkeypatch) -> None:
    monkeypatch.setattr(lifecycle, "SHUTDOWN_DELAY_SECONDS", 0.01)
    controller = ShutdownController()

    def _explode() -> None:
        raise RuntimeError("server already gone")

    controller.register(_explode)
    assert controller.request(LOGGER, "test") is True
    time.sleep(0.2)
