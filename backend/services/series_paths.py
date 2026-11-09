"""The names /api/series gives a numbered series' files, relative to the data root.

The series lookup runs on every image a user opens and on every file auto-load
picks up, over a folder that on a beamline can hold tens of thousands of
frames. It used to resolve every file's real path, three times over: 0.75 s
for a 20,000-file folder on a local disk, and one network round trip per path
component per file on the storage beamlines keep their data on. The folder is
resolved once here instead. That gives the same names because the lookup lists
regular files only, never symlinks, so a file's real path is its folder's real
path plus its name.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path


def names_under_root(
    folder: Path, names: Sequence[str], current: str, data_dir: Path
) -> tuple[list[str], str | None]:
    """Name each file in `folder` relative to `data_dir`, in "/" form.

    Returns the names and the name of `current` among them, or None when
    `current` is not one of `names`. A folder outside the data root gives no
    names at all, as resolving each file did.
    """
    try:
        relative = folder.resolve().relative_to(data_dir.resolve())
    except ValueError:
        return [], None
    lead = "" if relative == Path(".") else f"{relative.as_posix()}/"
    return [f"{lead}{name}" for name in names], (f"{lead}{current}" if current in names else None)
