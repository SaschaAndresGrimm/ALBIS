"""How /api/series names a numbered series' files relative to the data root.

The lookup used to resolve every file's real path three times, which made
opening an image in a 20,000-file folder take 0.75 s on a local disk, and much
longer on network storage, on every open and every auto-loaded file.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from backend.services.series_paths import names_under_root


def _old_names(folder: Path, names: list[str], current: str, data_dir: Path):
    """What the route computed before, resolving every file."""
    root = data_dir.resolve()
    files, target = [], None
    for name in names:
        p = folder / name
        try:
            rel = p.resolve().relative_to(root)
        except ValueError:
            continue
        files.append(rel.as_posix())
        if p.resolve() == (folder / current).resolve():
            target = rel.as_posix()
    return files, target


NAMES = ["scan_0001.cbf", "scan_0002.cbf", "scan_0003.cbf"]


def _touch(folder: Path) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    for name in NAMES:
        (folder / name).write_bytes(b"")
    return folder


def test_files_at_the_root_are_named_bare(tmp_path: Path) -> None:
    files, current = names_under_root(_touch(tmp_path), NAMES, "scan_0002.cbf", tmp_path)

    assert files == NAMES
    assert current == "scan_0002.cbf"


def test_files_in_a_subfolder_carry_its_path(tmp_path: Path) -> None:
    folder = _touch(tmp_path / "visit" / "run 1")

    files, current = names_under_root(folder, NAMES, "scan_0003.cbf", tmp_path)

    assert files == [f"visit/run 1/{name}" for name in NAMES]
    assert current == "visit/run 1/scan_0003.cbf"


def test_a_data_root_reached_through_a_symlink_gives_the_same_names(tmp_path: Path) -> None:
    """The data root is often a link to the real storage."""
    real = tmp_path / "real"
    folder = _touch(real / "visit")
    link = tmp_path / "link"
    try:
        link.symlink_to(real, target_is_directory=True)
    except OSError:
        pytest.skip("this system does not allow creating symlinks")

    assert names_under_root(link / "visit", NAMES, "scan_0001.cbf", link) == _old_names(
        link / "visit", NAMES, "scan_0001.cbf", link
    )
    assert names_under_root(folder, NAMES, "scan_0001.cbf", link)[0] == [
        f"visit/{name}" for name in NAMES
    ]


def test_a_folder_outside_the_root_names_nothing(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    outside = _touch(tmp_path / "elsewhere")

    assert names_under_root(outside, NAMES, "scan_0001.cbf", root) == ([], None)


def test_a_current_file_not_in_the_list_is_not_named(tmp_path: Path) -> None:
    assert names_under_root(_touch(tmp_path), NAMES, "other.cbf", tmp_path) == (NAMES, None)


def test_the_folder_is_resolved_once_however_many_files_it_holds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Resolving per file is what made a 20,000-file folder take 0.75 s."""
    calls = []
    real_resolve = Path.resolve

    def counting_resolve(self, *args, **kwargs):
        calls.append(self)
        return real_resolve(self, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", counting_resolve)
    names = [f"scan_{i:05d}.cbf" for i in range(1, 2001)]

    files, _ = names_under_root(tmp_path, names, names[0], tmp_path)

    assert len(files) == 2000
    assert len(calls) == 2
