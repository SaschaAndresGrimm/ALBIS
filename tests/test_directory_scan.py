"""Cover the budget on directory walks, and what a walk says when it stops.

The walks had no limit at all: unlimited depth by default, no cap on entries, no
time budget. What made that a real failure rather than a slow path is autoload,
which polls one of them about once a second -- so on a beamline data directory
the server spent every second walking a filesystem it could not finish, and
returned a partial answer that looked complete.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from backend.services.directory_scan import (
    CLOCK_INTERVAL_ENTRIES,
    BoundedWalk,
    ScanLimits,
    latest_image_file,
    scan_folders,
    scan_image_files,
)

EXTS = {".h5", ".cbf", ".tif"}


def _ext_name(name: str) -> str:
    lower = name.lower()
    for ext in (".cbf.gz", ".h5", ".cbf", ".tif"):
        if lower.endswith(ext):
            return ext
    return Path(lower).suffix


def _tree(root: Path, runs: int = 3, frames: int = 4) -> None:
    for run in range(runs):
        folder = root / f"run_{run:02d}" / "images"
        folder.mkdir(parents=True)
        for frame in range(frames):
            (folder / f"frame_{frame:04d}.cbf").write_bytes(b"")


# --------------------------------------------------------------------------
# Limits
# --------------------------------------------------------------------------


def test_unlimited_by_default_still_finds_everything(tmp_path: Path) -> None:
    _tree(tmp_path)

    result = scan_image_files(tmp_path, allowed_exts=EXTS, ext_name=_ext_name, limits=ScanLimits())

    assert len(result.items) == 12
    assert not result.truncated


def test_entry_budget_stops_the_walk_and_says_so(tmp_path: Path) -> None:
    _tree(tmp_path, runs=4, frames=10)

    result = scan_image_files(
        tmp_path,
        allowed_exts=EXTS,
        ext_name=_ext_name,
        limits=ScanLimits(max_entries=6),
    )

    assert result.truncated
    assert len(result.items) < 40


def test_time_budget_stops_the_walk(tmp_path: Path) -> None:
    """A slow filesystem is the case this exists for, so the clock is faked."""
    _tree(tmp_path, runs=4, frames=CLOCK_INTERVAL_ENTRIES)
    ticks = iter(range(0, 10_000))

    result = scan_image_files(
        tmp_path,
        allowed_exts=EXTS,
        ext_name=_ext_name,
        limits=ScanLimits(max_seconds=2.0),
        clock=lambda: float(next(ticks)),
    )

    assert result.truncated


def test_a_budget_that_is_never_reached_reports_no_truncation(tmp_path: Path) -> None:
    _tree(tmp_path, runs=1, frames=2)

    result = scan_image_files(
        tmp_path,
        allowed_exts=EXTS,
        ext_name=_ext_name,
        limits=ScanLimits(max_entries=10_000, max_seconds=60.0),
    )

    assert result.items == ("run_00/images/frame_0000.cbf", "run_00/images/frame_0001.cbf")
    assert not result.truncated


def test_depth_limit_is_still_honoured(tmp_path: Path) -> None:
    (tmp_path / "top.cbf").write_bytes(b"")
    nested = tmp_path / "a" / "b"
    nested.mkdir(parents=True)
    (nested / "deep.cbf").write_bytes(b"")

    shallow = scan_image_files(
        tmp_path,
        allowed_exts=EXTS,
        ext_name=_ext_name,
        limits=ScanLimits.from_config(max_depth=1),
    )

    assert shallow.items == ("top.cbf",)
    assert not shallow.truncated


def test_from_config_maps_minus_one_to_unlimited() -> None:
    assert ScanLimits.from_config(max_depth=-1).max_depth is None
    assert ScanLimits.from_config(max_depth=-99).max_depth is None
    assert ScanLimits.from_config(max_depth=3).max_depth == 3
    assert ScanLimits.from_config(max_entries=-5).max_entries == 0
    assert ScanLimits.from_config(max_seconds=-1.0).max_seconds == 0.0


# --------------------------------------------------------------------------
# What the walk sees
# --------------------------------------------------------------------------


def test_hidden_entries_are_skipped(tmp_path: Path) -> None:
    (tmp_path / "visible.cbf").write_bytes(b"")
    (tmp_path / ".hidden.cbf").write_bytes(b"")
    hidden_dir = tmp_path / ".cache"
    hidden_dir.mkdir()
    (hidden_dir / "inside.cbf").write_bytes(b"")

    result = scan_image_files(tmp_path, allowed_exts=EXTS, ext_name=_ext_name, limits=ScanLimits())

    assert result.items == ("visible.cbf",)


@pytest.mark.skipif(os.name == "nt", reason="directory symlinks need privileges on Windows")
def test_symlinked_directories_are_not_followed(tmp_path: Path) -> None:
    """A link loop must not be able to turn a bounded walk into an endless one."""
    real = tmp_path / "real"
    real.mkdir()
    (real / "frame.cbf").write_bytes(b"")
    (tmp_path / "loop").symlink_to(tmp_path, target_is_directory=True)

    result = scan_image_files(
        tmp_path,
        allowed_exts=EXTS,
        ext_name=_ext_name,
        limits=ScanLimits(max_entries=1000),
    )

    assert result.items == ("real/frame.cbf",)
    assert not result.truncated


def test_unreadable_directories_do_not_fail_the_scan(tmp_path: Path) -> None:
    """One unreadable folder is normal on a shared filesystem."""
    (tmp_path / "readable.cbf").write_bytes(b"")
    blocked = tmp_path / "blocked"
    blocked.mkdir()
    (blocked / "hidden_by_permissions.cbf").write_bytes(b"")
    if os.name != "nt":
        blocked.chmod(0o000)

    try:
        result = scan_image_files(
            tmp_path, allowed_exts=EXTS, ext_name=_ext_name, limits=ScanLimits()
        )
    finally:
        if os.name != "nt":
            blocked.chmod(0o755)

    assert "readable.cbf" in result.items


def test_folders_are_listed_without_the_root_itself(tmp_path: Path) -> None:
    _tree(tmp_path, runs=2, frames=1)

    result = scan_folders(tmp_path, limits=ScanLimits())

    assert set(result.items) == {"run_00", "run_00/images", "run_01", "run_01/images"}
    assert not result.truncated


# --------------------------------------------------------------------------
# Newest matching file
# --------------------------------------------------------------------------


def test_latest_image_file_picks_the_newest(tmp_path: Path) -> None:
    older = tmp_path / "first.cbf"
    newer = tmp_path / "second.cbf"
    older.write_bytes(b"")
    newer.write_bytes(b"")
    os.utime(older, (1_000_000, 1_000_000))
    os.utime(newer, (2_000_000, 2_000_000))

    result = latest_image_file(tmp_path, allowed_exts=EXTS, ext_name=_ext_name, limits=ScanLimits())

    assert result.path == newer
    assert not result.truncated


def test_latest_image_file_matches_a_bare_pattern_against_the_name(tmp_path: Path) -> None:
    (tmp_path / "wanted_0001.cbf").write_bytes(b"")
    (tmp_path / "other_0001.cbf").write_bytes(b"")

    result = latest_image_file(
        tmp_path,
        allowed_exts=EXTS,
        ext_name=_ext_name,
        pattern="wanted_*.cbf",
        limits=ScanLimits(),
    )

    assert result.path is not None
    assert result.path.name == "wanted_0001.cbf"


def test_latest_image_file_matches_a_path_pattern_against_the_relative_path(
    tmp_path: Path,
) -> None:
    _tree(tmp_path, runs=2, frames=2)

    result = latest_image_file(
        tmp_path,
        allowed_exts=EXTS,
        ext_name=_ext_name,
        pattern="run_01/images/*.cbf",
        limits=ScanLimits(),
    )

    assert result.path is not None
    assert "run_01" in result.path.as_posix()


def test_latest_image_file_reports_a_truncated_search(tmp_path: Path) -> None:
    """The file the detector just wrote may be the one that was not reached."""
    _tree(tmp_path, runs=4, frames=10)

    result = latest_image_file(
        tmp_path,
        allowed_exts=EXTS,
        ext_name=_ext_name,
        limits=ScanLimits(max_entries=5),
    )

    assert result.truncated


def test_latest_image_file_on_an_empty_directory(tmp_path: Path) -> None:
    result = latest_image_file(tmp_path, allowed_exts=EXTS, ext_name=_ext_name, limits=ScanLimits())

    assert result.path is None
    assert not result.truncated


# --------------------------------------------------------------------------
# The walk itself
# --------------------------------------------------------------------------


def test_bounded_walk_reports_truncation_only_after_it_happens(tmp_path: Path) -> None:
    _tree(tmp_path, runs=2, frames=4)
    walk = BoundedWalk(ScanLimits(max_entries=3))

    seen = []
    for entry in walk.entries(tmp_path):
        seen.append(entry)

    assert walk.truncated
    assert len(seen) <= 3
