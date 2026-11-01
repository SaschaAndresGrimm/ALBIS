"""What a dataset walk had to skip, counted and logged once per walk.

Split out of `hdf5_stack.py`, which fills a `WalkReport` while it walks a
file; `routes/hdf5.py` reads one to explain an orphaned master.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

_log = logging.getLogger("albis.hdf5_stack")

GROUP_CHILD_LIMIT = 10_000
"""How many children of any one group an enumeration will look at.

A filewriter told to emit one companion file per frame writes one external link
per frame into a single group, and a long burn-in run reaches millions: an ARINA
master measured here held 5,000,000 links in `/entry/data`. Nothing downstream
can present that many nodes -- the tree route alone needed 7.2 GiB to model them
-- so every enumeration stops at this cap and reports the group's true size
instead of trying to carry it.
"""

_MAX_SKIP_EXAMPLES = 5
"""Distinct example names carried in a walk's aggregated skip summary."""


def _remember(examples: list[str], filename: str | None) -> None:
    """Keep up to `_MAX_SKIP_EXAMPLES` distinct names for a summary line."""
    name = str(filename or "").strip()
    if name and len(examples) < _MAX_SKIP_EXAMPLES and name not in examples:
        examples.append(name)


@dataclass
class WalkReport:
    """What a dataset walk had to skip, aggregated rather than logged per node.

    A master whose companion files are missing has every link fail for the same
    reason. Logging each one wrote five million WARNING lines for the file that
    prompted this class, which is its own denial of service; the walk now counts
    reasons here and logs one line per walk.
    """

    skipped: dict[str, int] = field(default_factory=dict)
    missing_external: list[str] = field(default_factory=list)
    missing_external_count: int = 0
    unreadable_external: list[str] = field(default_factory=list)
    unreadable_external_count: int = 0
    unreadable_reasons: list[str] = field(default_factory=list)
    truncated_groups: list[tuple[str, int]] = field(default_factory=list)
    dead_link_groups: list[tuple[str, int]] = field(default_factory=list)
    """Groups where links were the only content and none of them resolved.

    This, not "the file held no images", is what an orphaned master looks like:
    a master carries its own calibration arrays, and `flatfield` and
    `pixel_mask` are both 2D, so a file with no frames at all still offers
    something displayable. Judging the group that should hold the frames keeps a
    partly-downloaded series openable -- if any link resolved, the group is not
    dead and whatever arrived is still served.
    """

    def note_skip(self, reason: str) -> None:
        self.skipped[reason] = self.skipped.get(reason, 0) + 1

    def note_dead_link_group(self, path: str, missing: int, total: int) -> None:
        """Record a dead group. `total` is its real size, `missing` what we saw."""
        if len(self.dead_link_groups) < _MAX_SKIP_EXAMPLES:
            self.dead_link_groups.append((path, max(missing, total)))

    def note_missing_external(self, filename: str | None) -> None:
        """Record a link whose target file is not there to open."""
        self.missing_external_count += 1
        _remember(self.missing_external, filename)

    def note_unreadable_external(self, filename: str | None, exc: BaseException) -> None:
        """Record a link whose target file is there but would not open.

        Kept apart from a missing file because by this point the path has been
        resolved, which means it existed and sat inside the data root -- so the
        file is on disk and something else stopped the read: no descriptors
        left, no permission, a mount that dropped out. Telling the user those
        files are "missing" would send them looking for data that is right
        where they put it.
        """
        self.unreadable_external_count += 1
        _remember(self.unreadable_external, filename)
        reason = str(exc).strip() or exc.__class__.__name__
        _remember(self.unreadable_reasons, reason)

    def note_truncated_group(self, path: str, total: int) -> None:
        if len(self.truncated_groups) < _MAX_SKIP_EXAMPLES:
            self.truncated_groups.append((path, total))

    def log_summary(self, file_path: Path) -> None:
        """Emit at most one line per category for the whole walk."""
        for path, total in self.truncated_groups:
            _log.warning(
                "Only the first %d of %d children of %s in %s were inspected",
                GROUP_CHILD_LIMIT,
                total,
                path,
                file_path,
            )
        if self.missing_external_count:
            _log.warning(
                "Skipped %d external link(s) in %s whose target file is missing (e.g. %s)",
                self.missing_external_count,
                file_path,
                ", ".join(self.missing_external) or "unknown",
            )
        if self.unreadable_external_count:
            _log.warning(
                "Skipped %d external link(s) in %s whose target file is present but "
                "would not open (e.g. %s): %s",
                self.unreadable_external_count,
                file_path,
                ", ".join(self.unreadable_external) or "unknown",
                "; ".join(self.unreadable_reasons) or "unknown",
            )
        for path, missing in self.dead_link_groups:
            _log.warning(
                "%s in %s holds only links and none of its %d link(s) resolved",
                path,
                file_path,
                missing,
            )
        for reason, count in sorted(self.skipped.items()):
            _log.warning("Skipped %d node(s) in %s: %s", count, file_path, reason)
