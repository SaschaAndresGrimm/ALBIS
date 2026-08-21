"""Directory walks that stop, and say that they stopped.

ALBIS discovers data by walking a directory: the file list, the folder list, and
the "newest matching file" that live autoload polls. On a workstation these are
free. On the filesystem a beamline actually stores data on they are not, and the
walks had no limit of any kind -- unlimited depth by default, no cap on entries
visited, no time budget. A `data.root` holding a few hundred thousand files
turned every one of them into seconds of work per call, on a threadpool worker,
returning a JSON document nobody could use. Autoload made that a poll: once a
second, for as long as the viewer was left open.

Two changes follow from that. A walk is bounded, by entries and by wall-clock
time, so no single request can take arbitrarily long. And a bounded walk reports
that it was cut short, because a truncated listing presented as a complete one
is worse than a slow one -- the file someone is looking for is missing and
nothing says why.

Depth is left unlimited by default deliberately. A depth limit hides files that
are *deeper*, silently and permanently; a budget stops at "enough for now" and
says so. The knob stays for anyone who wants it.
"""

from __future__ import annotations

import fnmatch
import os
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path

# How often the clock is consulted. A `time.monotonic()` per directory entry
# would be a measurable share of the walk's own cost on a fast local filesystem,
# and the budget does not need to be accurate to the microsecond.
CLOCK_INTERVAL_ENTRIES = 512


@dataclass(frozen=True)
class ScanLimits:
    """What a single walk is allowed to spend. Zero or `None` means unlimited."""

    max_depth: int | None = None
    max_entries: int = 0
    max_seconds: float = 0.0

    @staticmethod
    def from_config(
        *, max_depth: int = -1, max_entries: int = 0, max_seconds: float = 0.0
    ) -> ScanLimits:
        """Build limits from config values, where `-1` depth means unlimited."""
        return ScanLimits(
            max_depth=None if max_depth is None or max_depth < 0 else int(max_depth),
            max_entries=max(0, int(max_entries)),
            max_seconds=max(0.0, float(max_seconds)),
        )


@dataclass(frozen=True)
class ScanResult:
    """A finished listing. Immutable, so a cache can hand out the same object."""

    items: tuple[str, ...] = ()
    truncated: bool = False

    def as_list(self) -> list[str]:
        return list(self.items)


@dataclass(frozen=True)
class LatestFileResult:
    path: Path | None = None
    truncated: bool = False


@dataclass
class _Budget:
    limits: ScanLimits
    clock: Callable[[], float] = time.monotonic
    spent: int = 0
    exhausted: bool = False
    _started: float = field(default=0.0, init=False)

    def __post_init__(self) -> None:
        self._started = self.clock()

    def charge(self) -> bool:
        """Account for one directory entry. False once the walk must stop."""
        if self.exhausted:
            return False
        self.spent += 1
        if self.limits.max_entries and self.spent > self.limits.max_entries:
            self.exhausted = True
            return False
        if (
            self.limits.max_seconds
            and self.spent % CLOCK_INTERVAL_ENTRIES == 0
            and self.clock() - self._started >= self.limits.max_seconds
        ):
            self.exhausted = True
            return False
        return True


class BoundedWalk:
    """One bounded walk. Read `truncated` after the iterator is exhausted."""

    def __init__(self, limits: ScanLimits, clock: Callable[[], float] = time.monotonic) -> None:
        self._budget = _Budget(limits=limits, clock=clock)
        self._limits = limits

    @property
    def truncated(self) -> bool:
        return self._budget.exhausted

    def entries(self, root: Path) -> Iterator[tuple[str, str, bool]]:
        """Yield `(path, name, is_dir)` for everything under `root`.

        Hidden entries are skipped and symlinks are never followed, so a link
        loop cannot turn a bounded walk into an unbounded one. A directory that
        cannot be read is skipped rather than failing the whole scan: on a shared
        filesystem, one unreadable folder is normal.
        """
        max_depth = self._limits.max_depth
        stack: list[tuple[Path, int]] = [(root, 0)]
        while stack:
            base, depth = stack.pop()
            try:
                with os.scandir(base) as it:
                    for entry in it:
                        if not self._budget.charge():
                            return
                        name = entry.name
                        if name.startswith("."):
                            continue
                        try:
                            if entry.is_dir(follow_symlinks=False):
                                if max_depth is None or depth < max_depth:
                                    stack.append((Path(entry.path), depth + 1))
                                yield entry.path, name, True
                            elif entry.is_file(follow_symlinks=False):
                                yield entry.path, name, False
                        except OSError:
                            continue
            except OSError:
                continue


def _relative_posix(path_str: str, root: Path) -> str | None:
    try:
        rel = os.path.relpath(path_str, root)
    except ValueError:
        return None
    if rel.startswith(".."):
        return None
    return rel.replace(os.sep, "/")


def scan_image_files(
    root: Path,
    *,
    allowed_exts: set[str],
    ext_name: Callable[[str], str],
    limits: ScanLimits,
    clock: Callable[[], float] = time.monotonic,
) -> ScanResult:
    """Collect relative paths of image files under `root`, within the budget."""
    resolved = root.resolve()
    walk = BoundedWalk(limits, clock=clock)
    items: set[str] = set()
    for path_str, name, is_dir in walk.entries(resolved):
        if is_dir:
            continue
        if ext_name(name) not in allowed_exts:
            continue
        rel = _relative_posix(path_str, resolved)
        if rel is not None:
            items.add(rel)
    return ScanResult(items=tuple(sorted(items)), truncated=walk.truncated)


def scan_folders(
    root: Path,
    *,
    limits: ScanLimits,
    clock: Callable[[], float] = time.monotonic,
) -> ScanResult:
    """Collect relative subfolder paths under `root`, within the budget."""
    resolved = root.resolve()
    walk = BoundedWalk(limits, clock=clock)
    items: set[str] = set()
    for path_str, _name, is_dir in walk.entries(resolved):
        if not is_dir:
            continue
        rel = _relative_posix(path_str, resolved)
        if rel:
            items.add(rel)
    return ScanResult(items=tuple(sorted(items)), truncated=walk.truncated)


def latest_image_file(
    root: Path,
    *,
    allowed_exts: set[str],
    ext_name: Callable[[str], str],
    pattern: str | None = None,
    limits: ScanLimits,
    clock: Callable[[], float] = time.monotonic,
) -> LatestFileResult:
    """Find the most recently modified matching file under `root`.

    A pattern containing a separator is matched against the path relative to
    `root`, and a bare one against the file name, so `*.cbf` and
    `run_*/frame_*.cbf` both mean what they look like.

    A truncated walk means the answer is "the newest of what was looked at",
    which for autoload is a real difference: the file the detector just wrote
    may be the one that was not reached.
    """
    resolved = root.resolve()
    walk = BoundedWalk(limits, clock=clock)
    normalized = (pattern or "").strip()
    match_relative = "/" in normalized or "\\" in normalized
    latest_path: Path | None = None
    latest_mtime = -1.0
    for path_str, name, is_dir in walk.entries(resolved):
        if is_dir:
            continue
        if ext_name(name) not in allowed_exts:
            continue
        if normalized:
            if match_relative:
                target = _relative_posix(path_str, resolved)
                if target is None:
                    continue
            else:
                target = name
            if not fnmatch.fnmatch(target, normalized):
                continue
        try:
            mtime = os.stat(path_str).st_mtime
        except OSError:
            continue
        if mtime > latest_mtime:
            latest_mtime = mtime
            latest_path = Path(path_str)
    return LatestFileResult(path=latest_path, truncated=walk.truncated)
