"""How this copy of ALBIS was installed, as opposed to which build it is.

`VERSION` answers "which release" and `build_info` answers "which
build". Neither answers the question the update notification actually needs:
*what should this user do about a newer release*. The answer is different for
every way ALBIS ships -- a Docker user pulls an image, an AppImage user
replaces one file, a Windows user runs an installer, someone on a checkout
runs `git pull` -- and until now the interface asked all of them to go read a
release page listing nine assets and work out which one was theirs.

Resolution order, first hit wins:

1. `ALBIS_IN_DOCKER` in the environment, set by the `Dockerfile`. A container
   cannot replace its own image, so this has to win over everything below it.
   Checked before `/.dockerenv` because that file does not exist under Podman
   and other runtimes, while the environment variable travels with the image.
2. `/.dockerenv`, for an image built from something other than our Dockerfile.
3. Not frozen -- no PyInstaller bundle -- means a checkout or a venv, and the
   working tree is the thing to update.
4. Frozen, by platform: an AppImage on Linux, a `.dmg`-installed app bundle on
   macOS, and on Windows either the installer or the portable zip, which are
   told apart by the uninstall key the installer writes.

Nothing here is allowed to be slow, raise, or touch the network: it runs on the
way to a single API response, and a wrong answer should degrade to the generic
"go to the releases page" instruction rather than fail the request.
"""

from __future__ import annotations

import os
import platform
import sys
from pathlib import Path

# Kinds are part of the `/api/update-check` contract, so the interface can key
# its instructions off them. Anything unrecognised must render as "open the
# releases page", which is what `source` already does.
INSTALL_KIND_DOCKER = "docker"
INSTALL_KIND_APPIMAGE = "appimage"
INSTALL_KIND_WINDOWS_INSTALLER = "windows_installer"
INSTALL_KIND_WINDOWS_PORTABLE = "windows_portable"
INSTALL_KIND_MACOS_APP = "macos_app"
INSTALL_KIND_SOURCE = "source"

INSTALL_KINDS: tuple[str, ...] = (
    INSTALL_KIND_DOCKER,
    INSTALL_KIND_APPIMAGE,
    INSTALL_KIND_WINDOWS_INSTALLER,
    INSTALL_KIND_WINDOWS_PORTABLE,
    INSTALL_KIND_MACOS_APP,
    INSTALL_KIND_SOURCE,
)

# Forces the detected install kind, for exercising the update flow from a
# checkout. Paired with `ALBIS_UPDATE_CHECK_VERSION`, it makes the asset
# matching, the download and the verification all run for real without first
# having to package a build.
#
# It changes only which instruction and which asset are offered. Every guard
# downstream still applies to the real environment: applying an update still
# needs a real `APPIMAGE` path and a launcher that can stop the process, so a
# `uvicorn` run cannot be talked into replacing anything.
INSTALL_KIND_ENV_VAR = "ALBIS_INSTALL_KIND"

_DOCKER_ENV_VAR = "ALBIS_IN_DOCKER"
_DOCKER_MARKER_FILE = "/.dockerenv"

# The InnoSetup `AppId` from `scripts/installer_windows.iss`. InnoSetup appends
# `_is1` to form the uninstall key, and the installer runs with
# `PrivilegesRequired=lowest`, so the key is per-user under HKCU.
_WINDOWS_UNINSTALL_SUBKEY = r"Software\Microsoft\Windows\CurrentVersion\Uninstall\ALBIS_is1"


def _is_frozen() -> bool:
    return bool(getattr(sys, "frozen", False))


def _docker_from_env() -> bool:
    raw = str(os.environ.get(_DOCKER_ENV_VAR, "")).strip().lower()
    return raw not in ("", "0", "false", "no")


def _docker_marker_present() -> bool:
    try:
        return Path(_DOCKER_MARKER_FILE).exists()
    except OSError:  # pragma: no cover - defensive; exists() rarely raises
        return False


def _executable_dir() -> Path | None:
    """Where the running binary lives, which is not where its bundle unpacks.

    `sys._MEIPASS` is a temporary extraction directory; `sys.executable` is the
    installed file the user double-clicked, and only that one can be compared
    against an install location.
    """
    try:
        return Path(sys.executable).resolve().parent
    except (OSError, ValueError):  # pragma: no cover - defensive
        return None


def _windows_registered_install_location() -> str:
    """The directory the ALBIS installer recorded, or `""` if it never ran.

    Returned as text rather than a `Path` so "no key" and "a key with an empty
    value" collapse into the same falsy answer the caller already handles.
    """
    try:
        # Imported here rather than at module scope: it does not exist off
        # Windows, and this module is imported on every platform.
        import winreg
    except ImportError:
        return ""
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, _WINDOWS_UNINSTALL_SUBKEY) as key:
            value, _ = winreg.QueryValueEx(key, "InstallLocation")
    except OSError:
        return ""
    return str(value or "").strip()


def _windows_install_kind() -> str:
    """Installer or portable zip.

    The portable zip is extracted anywhere the user likes and has no uninstall
    key, so the absence of one is the signal -- pointing a portable user at the
    installer would give them a *second* ALBIS rather than a newer one.

    When the key exists but names a different directory, the user has both and
    is currently running the portable copy. When the registry cannot be read at
    all -- no `winreg`, a locked hive -- the installer is the safer guess,
    because it is how nearly every Windows install of ALBIS arrives.
    """
    location = _windows_registered_install_location()
    if not location:
        return INSTALL_KIND_WINDOWS_PORTABLE

    executable_dir = _executable_dir()
    if executable_dir is None:
        return INSTALL_KIND_WINDOWS_INSTALLER
    try:
        registered = Path(location).resolve()
    except (OSError, ValueError):
        return INSTALL_KIND_WINDOWS_INSTALLER

    if executable_dir == registered or registered in executable_dir.parents:
        return INSTALL_KIND_WINDOWS_INSTALLER
    return INSTALL_KIND_WINDOWS_PORTABLE


def install_kind_override() -> str:
    """The forced install kind, or `""`. An unrecognised value is ignored.

    Ignored rather than obeyed, because a typo here would otherwise send the
    user an asset for a platform they are not on.
    """
    raw = str(os.environ.get(INSTALL_KIND_ENV_VAR, "")).strip().lower()
    return raw if raw in INSTALL_KINDS else ""


def read_install_kind() -> str:
    """Return how this copy of ALBIS was installed. Never raises."""
    override = install_kind_override()
    if override:
        return override
    try:
        if _docker_from_env() or _docker_marker_present():
            return INSTALL_KIND_DOCKER
        if not _is_frozen():
            return INSTALL_KIND_SOURCE
        if sys.platform.startswith("linux"):
            return INSTALL_KIND_APPIMAGE
        if sys.platform == "darwin":
            return INSTALL_KIND_MACOS_APP
        if sys.platform == "win32":
            return _windows_install_kind()
    except Exception:  # pragma: no cover - a wrong guess must not fail a request
        return INSTALL_KIND_SOURCE
    return INSTALL_KIND_SOURCE


def read_target_arch() -> str:
    """Normalise the running architecture onto the release-asset vocabulary.

    The release assets are named with `x64` and `arm64` (and `x86_64` for the
    AppImage), which is `scripts/version_info.py`'s vocabulary, not
    `platform.machine()`'s. Anything unrecognised is returned lowercased and
    simply will not match an asset, which is the correct outcome: no download
    button rather than a link to the wrong binary.
    """
    try:
        raw = str(platform.machine() or "").strip().lower()
    except Exception:  # pragma: no cover - defensive
        return ""
    if raw in ("x86_64", "amd64", "x64", "x86-64"):
        return "x64"
    if raw in ("arm64", "aarch64"):
        return "arm64"
    return raw


ALBIS_INSTALL_KIND = read_install_kind()
ALBIS_TARGET_ARCH = read_target_arch()
