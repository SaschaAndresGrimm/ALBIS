"""Where the native file and folder choosers open.

They used to open wherever the toolkit liked -- the home folder, "Recent", the
folder ALBIS was started from -- so `data.root` meant nothing to them. A
beamline scientist with the root set to the experiment's folder landed in their
home folder on every File -> Open, and had to click their way back each time.

Now a chooser opens where the last one of its kind left off, and the first one
opens in the data root. Remembered for as long as this server runs, not
across restarts: after a restart the data root is the sensible place to start,
and a remembered folder may since have gone.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Literal

PickerKind = Literal["file", "folder"]


class PickerStartFolders:
    """The folder each kind of chooser opens in, kept per server process."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._last: dict[str, Path] = {}

    def start_for(self, kind: PickerKind, data_root: Path) -> Path | None:
        """The last folder of this kind if it still exists, else the data root.

        None when neither exists, and the toolkit decides: a chooser told to
        open in a missing folder opens somewhere arbitrary or refuses outright.
        """
        with self._lock:
            last = self._last.get(kind)
        for candidate in (last, data_root):
            if candidate is not None and _is_dir(candidate):
                return candidate
        return None

    def remember(self, kind: PickerKind, picked: Path) -> None:
        """Record a choice: a chosen folder itself, or the folder a file is in."""
        folder = picked if kind == "folder" else picked.parent
        with self._lock:
            self._last[kind] = folder


def _is_dir(path: Path) -> bool:
    try:
        return path.is_dir()
    except OSError:
        # An unreachable network mount says so by raising, not by being absent.
        return False
