"""Hand a verified download to the user and close ALBIS so it can be installed.

On macOS and with the Windows portable zip, installing an update means
replacing files the running ALBIS holds: Finder refuses to replace an app that
is open, and Windows will not overwrite files in use, so an extraction stops
halfway. Before this, the update dialog showed the folder and left the user to
find out that ALBIS had to be quit first, from the Dock or the File menu.

So these two installs get one step that does both: show the download, then
stop ALBIS the way File -> Quit ALBIS does. What "show" means differs:

* **macOS**: the disk image is opened, which mounts it and shows its window
  with ALBIS and the Applications folder side by side. Mounting runs nothing;
  the user still drags the app across.
* **Windows portable**: the zip is selected in Explorer, as Show in Folder
  does. Opening a zip would start whatever program is registered for it.

The Windows installer closes ALBIS by itself, and the AppImage can be replaced
while it runs, so neither needs this.
"""

from __future__ import annotations

from pathlib import Path

from ..install_kind import INSTALL_KIND_MACOS_APP, INSTALL_KIND_WINDOWS_PORTABLE
from .os_actions import open_in_system, reveal_in_file_manager

HANDOFF_INSTALL_KINDS = (INSTALL_KIND_MACOS_APP, INSTALL_KIND_WINDOWS_PORTABLE)


def handoff_supported(install_kind: str) -> bool:
    """Whether this install needs ALBIS closed before its update can go in."""
    return install_kind in HANDOFF_INSTALL_KINDS


def show_for_install(path: Path, install_kind: str) -> bool:
    """Open the disk image on macOS, otherwise select the file. True if shown.

    Only a `.dmg` is ever opened. The asset matching already guarantees that
    for a macOS install; checking it again here costs nothing and means a
    mismatch shows a file instead of opening one.
    """
    if install_kind == INSTALL_KIND_MACOS_APP and path.suffix.lower() == ".dmg":
        return open_in_system(path)
    return reveal_in_file_manager(path)
