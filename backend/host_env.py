"""Give the programs ALBIS starts the environment they would have had without it.

A packaged Linux ALBIS is started by PyInstaller's bootloader, which points
`LD_LIBRARY_PATH` at the bundle's own `_internal/` directory so the bundled
Python finds its libraries. Every program ALBIS starts inherits that: the
browser `xdg-open` launches, the file manager, the zenity and kdialog pickers.
They then load the bundle's copies of system libraries in place of the
system's own. The bundle is built on glibc 2.28 (AlmaLinux 8), so those copies
are old: on Rocky Linux 9, Firefox died with
``libstdc++.so.6: version `GLIBCXX_3.4.26' not found`` and ALBIS said
"opening browser" while nothing opened.

PyInstaller records the value it replaced in `LD_LIBRARY_PATH_ORIG`, or leaves
that unset when there was none, and documents restoring it before starting
system programs. Restoring it in ALBIS's own environment, once at startup,
does that for every child at once. It changes nothing for ALBIS itself: the
dynamic loader reads `LD_LIBRARY_PATH` when a process starts, and the running
process keeps the search path it started with.
"""

from __future__ import annotations

import os
import sys
from collections.abc import MutableMapping

_VAR = "LD_LIBRARY_PATH"
_ORIG = f"{_VAR}_ORIG"


def restore_host_library_path(
    environ: MutableMapping[str, str] | None = None,
    *,
    frozen: bool | None = None,
    platform: str | None = None,
) -> bool:
    """Undo the bootloader's `LD_LIBRARY_PATH` in `environ`; True if it changed.

    Only a frozen Linux build is touched. A source checkout's environment is
    the user's own, and on macOS and Windows the bootloader leaves it alone.
    """
    environ = os.environ if environ is None else environ
    frozen = bool(getattr(sys, "frozen", False)) if frozen is None else frozen
    platform = sys.platform if platform is None else platform
    if not frozen or not platform.startswith("linux"):
        return False
    before = environ.get(_VAR)
    original = environ.pop(_ORIG, None)
    if original:
        environ[_VAR] = original
    else:
        environ.pop(_VAR, None)
    return environ.get(_VAR) != before
