"""Apply a downloaded update, on the two platforms where that is safe.

`update_download.py` leaves a verified installer on disk and shows the user the
folder. This does the last step for them -- but only where the platform makes
it a contained operation, and only when they have asked for it:

* **Linux AppImage.** The whole application is one file at a path the AppImage
  runtime hands us in `APPIMAGE`, under the user's own home. Replacing it is a
  staged copy and an `os.replace`, which is atomic: either the old file or the
  new one is there, never half of either.
* **Windows installer.** The installer already knows how to do this. It signals
  `ALBISShutdownEvent`, which `albis_launcher.py` listens for, waits for ALBIS
  to exit and falls back to `taskkill` -- see `scripts/installer_windows.iss`.
  So applying is running it silently and letting it drive.

Not macOS: replacing a running `.app` bundle needs a detached helper and risks
invalidating the notarization the user is relying on. Not Docker, which cannot
replace its own image. Not a source checkout, which is the user's working tree.
Those keep the download-and-show-the-folder behaviour.

Three rules this holds to, all of them because ALBIS is an image viewer on a
beamline workstation and not a consumer app:

1. **Off unless asked.** `ui.allow_update_apply` defaults to false.
2. **A verified file, or nothing.** An update whose checksum could not be
   verified is offered as a download but refused here. Handing someone a file
   to run is not the same as running it for them.
3. **Never over live work.** The caller states what is in progress and a
   non-empty answer refuses the request.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from logging import Logger
from pathlib import Path

from ..install_kind import INSTALL_KIND_APPIMAGE, INSTALL_KIND_WINDOWS_INSTALLER
from ..lifecycle import ShutdownController
from .update_download import CHECKSUM_VERIFIED, SIGNATURE_INVALID

# Kinds this module can act on. Everything else is a download, not an apply.
APPLICABLE_INSTALL_KINDS = (INSTALL_KIND_APPIMAGE, INSTALL_KIND_WINDOWS_INSTALLER)

# `/SILENT` shows a progress window but no wizard, which is what should happen
# after ALBIS disappears -- `/VERYSILENT` would leave the screen blank. The
# installer's own `[Run]` entry carries `skipifsilent`, so it does not relaunch
# ALBIS, and `/NORESTART` keeps it from proposing a reboot.
WINDOWS_INSTALLER_ARGS = ("/SILENT", "/NORESTART")

# The environment variable the AppImage runtime sets to the path of the
# AppImage file itself -- as opposed to the read-only mount it is running from.
APPIMAGE_PATH_ENV = "APPIMAGE"

STATUS_IDLE = "idle"
STATUS_APPLYING = "applying"
STATUS_APPLIED = "applied"
STATUS_FAILED = "failed"

# Why an apply is not on offer. Reported as a code so the interface can say it
# in the user's language rather than showing an English sentence from the API.
REASON_DISABLED = "disabled"
REASON_UNSUPPORTED = "unsupported_install"
REASON_NO_DOWNLOAD = "no_verified_download"
REASON_UNVERIFIED = "unverified_download"
REASON_BUSY = "busy"
REASON_NO_TARGET = "target_unknown"
REASON_NO_SHUTDOWN = "shutdown_unavailable"


class ApplyRefusedError(Exception):
    """The apply was not attempted, and `code` says which rule stopped it."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass
class ApplyState:
    status: str = STATUS_IDLE
    message: str = ""

    def as_payload(self) -> dict[str, object]:
        return {"status": self.status, "message": self.message}


def appimage_target_path() -> Path | None:
    """The AppImage file this process was started from, if it can be trusted.

    `APPIMAGE` comes from the environment, so it is checked rather than
    believed: it has to name an existing regular file whose directory ALBIS can
    write, because that is what `os.replace` needs. Anything else means the
    target cannot be established and the apply is refused instead of guessed.
    """
    raw = os.environ.get(APPIMAGE_PATH_ENV, "").strip()
    if not raw:
        return None
    try:
        candidate = Path(raw).resolve()
        if not candidate.is_file():
            return None
        if not os.access(candidate.parent, os.W_OK):
            return None
    except OSError:
        return None
    return candidate


class UpdateApplyService:
    def __init__(
        self,
        logger: Logger,
        shutdown: ShutdownController,
        ready_download: Callable[[], Path | None],
        download_status: Callable[[], dict[str, object]],
    ) -> None:
        self._logger = logger
        self._shutdown = shutdown
        self._ready_download = ready_download
        self._download_status = download_status
        self._lock = threading.Lock()
        self._state = ApplyState()

    # -- state ------------------------------------------------------------
    def status(self) -> dict[str, object]:
        with self._lock:
            return self._state.as_payload()

    def _set(self, status: str, message: str = "") -> None:
        with self._lock:
            self._state = ApplyState(status=status, message=message)

    # -- eligibility ------------------------------------------------------
    def refusal_code(
        self, *, install_kind: str, allowed: bool, busy: Sequence[str] = ()
    ) -> str | None:
        """Why an apply would be refused right now, or None if it would run.

        Evaluated in the order the reasons matter to the user: a setting they
        can change, then a platform that will never support it, then the state
        of their own session.
        """
        if not allowed:
            return REASON_DISABLED
        if install_kind not in APPLICABLE_INSTALL_KINDS:
            return REASON_UNSUPPORTED
        if not self._shutdown.is_available():
            # A source run has no launcher to stop the process.
            return REASON_NO_SHUTDOWN
        if install_kind == INSTALL_KIND_APPIMAGE and appimage_target_path() is None:
            return REASON_NO_TARGET
        if busy:
            return REASON_BUSY

        ready = self._ready_download()
        if ready is None:
            return REASON_NO_DOWNLOAD
        status = self._download_status()
        if status.get("checksum") != CHECKSUM_VERIFIED:
            # Offered as a download, refused as an apply: handing someone a
            # file to run is not the same as running it for them.
            return REASON_UNVERIFIED
        if status.get("signature") == SIGNATURE_INVALID:
            return REASON_UNVERIFIED
        return None

    # -- apply ------------------------------------------------------------
    def apply(
        self, *, install_kind: str, allowed: bool, busy: Sequence[str] = ()
    ) -> dict[str, object]:
        with self._lock:
            if self._state.status in (STATUS_APPLYING, STATUS_APPLIED):
                return self._state.as_payload()

        code = self.refusal_code(install_kind=install_kind, allowed=allowed, busy=busy)
        if code is not None:
            raise ApplyRefusedError(code)

        source = self._ready_download()
        if source is None:  # pragma: no cover - refusal_code already checked
            raise ApplyRefusedError(REASON_NO_DOWNLOAD)

        self._set(STATUS_APPLYING)
        thread = threading.Thread(
            target=self._run,
            args=(install_kind, source),
            name="albis-update-apply",
            daemon=True,
        )
        thread.start()
        return self.status()

    def _run(self, install_kind: str, source: Path) -> None:
        try:
            if install_kind == INSTALL_KIND_APPIMAGE:
                self._apply_appimage(source)
            else:
                self._launch_windows_installer(source)
        except Exception as exc:
            self._logger.warning("Update apply failed: %s", exc)
            self._set(
                STATUS_FAILED,
                "The update could not be applied. See the log for details.",
            )
            return

        self._set(STATUS_APPLIED)
        if not self._shutdown.request(self._logger, "update applied"):
            # Only reachable if the hook disappeared between the check and
            # here. The update is on disk either way, so this reports what the
            # user now has to do rather than calling it a failure.
            self._set(STATUS_APPLIED, "Close ALBIS to finish updating.")

    def _apply_appimage(self, source: Path) -> None:
        """Stage the new AppImage beside the old one, then swap it in.

        Staged in the target directory rather than replaced from the download
        folder, for two reasons: `os.replace` is only atomic within one
        filesystem, and `Downloads` is not guaranteed to be on the same one as
        `~/.local/share`. The staging file is removed on failure so a partial
        copy is never left looking like a spare AppImage.
        """
        target = appimage_target_path()
        if target is None:
            raise OSError("The running AppImage could not be located")

        staged = target.parent / f".{target.name}.albis-update"
        try:
            shutil.copyfile(source, staged)
            # An AppImage that is not executable is not an application. Copied
            # from the old file rather than assumed, so a site that tightened
            # the mode keeps its choice.
            os.chmod(staged, os.stat(target).st_mode & 0o7777)
            os.replace(staged, target)
        except Exception:
            try:
                staged.unlink(missing_ok=True)
            except OSError:
                self._logger.warning("Could not remove the staged AppImage")
            raise
        self._logger.info("Replaced the running AppImage at %s", target)

    def _launch_windows_installer(self, source: Path) -> None:
        """Start the installer and leave the rest to it.

        It signals `ALBISShutdownEvent`, waits for ALBIS to exit and falls back
        to `taskkill`, all of which already exists and is what an ordinary
        manual install does. Detached from this process, because this process
        is one of the things it is about to close.
        """
        creationflags = 0
        if sys.platform == "win32":  # pragma: no cover - Windows-only flags
            creationflags = getattr(subprocess, "DETACHED_PROCESS", 0) | getattr(
                subprocess, "CREATE_NEW_PROCESS_GROUP", 0
            )
        subprocess.Popen(  # noqa: S603 - a path this service downloaded and verified
            [str(source), *WINDOWS_INSTALLER_ARGS],
            close_fds=True,
            creationflags=creationflags,
        )
        self._logger.info("Started the ALBIS installer at %s", source)
