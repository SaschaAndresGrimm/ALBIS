"""scripts/bump_version.py against copies of the real files.

Copies, not fixtures: the point of the script is that it keeps working on the
files as they are, so a layout change in any of them should fail here first,
not in the middle of a release.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

from scripts.bump_version import FILES, BumpError, bump, is_newer

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "bump_version.py"
ENTRY = "### Added\n\n- **Something worth releasing.**\n\n"


@pytest.fixture()
def repo(tmp_path: Path) -> Path:
    for name in FILES:
        shutil.copy(ROOT / name, tmp_path / name)
    changelog = tmp_path / "CHANGELOG.md"
    text = changelog.read_text(encoding="utf-8")
    # Whatever is unreleased right now, give the copy something to release.
    text = text.replace("## [Unreleased]\n\n", "## [Unreleased]\n\n" + ENTRY, 1)
    changelog.write_text(text, encoding="utf-8")
    return tmp_path


def current(root: Path) -> str:
    return (root / "VERSION").read_text(encoding="utf-8").strip()


def next_major(root: Path) -> str:
    return f"{int(current(root).split('.')[0]) + 1}.0.0"


def test_every_file_states_the_new_version(repo: Path) -> None:
    version = next_major(repo)

    bump(repo, version, "2030-01-02")

    assert (repo / "VERSION").read_text(encoding="utf-8") == version + "\n"
    assert json.loads((repo / "package.json").read_text(encoding="utf-8"))["version"] == version
    lock = json.loads((repo / "package-lock.json").read_text(encoding="utf-8"))
    assert lock["version"] == version
    assert lock["packages"][""]["version"] == version
    assert (
        tomllib.loads((repo / "pyproject.toml").read_text(encoding="utf-8"))["project"]["version"]
        == version
    )
    citation = (repo / "CITATION.cff").read_text(encoding="utf-8")
    assert f"\nversion: {version}\n" in citation
    assert "\ndate-released: 2030-01-02\n" in citation


def test_the_changelog_section_is_dated_and_linked(repo: Path) -> None:
    previous = current(repo)
    version = next_major(repo)

    bump(repo, version, "2030-01-02")

    text = (repo / "CHANGELOG.md").read_text(encoding="utf-8")
    assert f"## [Unreleased]\n\n## [{version}] - 2030-01-02\n\n{ENTRY}" in text
    assert re.search(rf"^\[Unreleased\]: \S+/compare/v{re.escape(version)}\.\.\.HEAD$", text, re.M)
    assert re.search(
        rf"^\[{re.escape(version)}\]: \S+/compare/v{re.escape(previous)}\.\.\.v{re.escape(version)}$",
        text,
        re.M,
    )


def test_nothing_else_in_the_files_moves(repo: Path) -> None:
    before = {name: (repo / name).read_text(encoding="utf-8") for name in FILES}

    changes = bump(repo, next_major(repo), "2030-01-02")

    for name, (old, new) in changes.items():
        assert old == before[name]
        changed = [line for line in new.splitlines() if line not in set(old.splitlines())]
        # Version, date, section heading and link lines only.
        assert len(changed) <= 4, (name, changed)


def test_it_refuses_an_empty_unreleased_section(repo: Path) -> None:
    changelog = repo / "CHANGELOG.md"
    text = changelog.read_text(encoding="utf-8")
    # Empty the whole section, whatever the real changelog has in it today.
    body_start = text.index("## [Unreleased]\n") + len("## [Unreleased]\n")
    body_end = text.index("\n## [", body_start)
    changelog.write_text(text[:body_start] + text[body_end:], encoding="utf-8")

    with pytest.raises(BumpError, match="nothing under"):
        bump(repo, next_major(repo), "2030-01-02")


@pytest.mark.parametrize("version", ["0.0.1", "not-a-version", "1.2"])
def test_it_refuses_a_version_that_is_not_newer_or_not_a_version(repo: Path, version: str) -> None:
    with pytest.raises(BumpError):
        bump(repo, version, "2030-01-02")


def test_it_refuses_the_current_version(repo: Path) -> None:
    with pytest.raises(BumpError, match="not newer"):
        bump(repo, current(repo), "2030-01-02")


def test_it_writes_nothing_when_it_refuses(repo: Path) -> None:
    before = {name: (repo / name).read_text(encoding="utf-8") for name in FILES}
    (repo / "pyproject.toml").write_text(
        before["pyproject.toml"].replace("[project]", "[tool.renamed]"), encoding="utf-8"
    )

    with pytest.raises(BumpError, match="pyproject.toml"):
        bump(repo, next_major(repo), "2030-01-02")

    for name in FILES:
        if name != "pyproject.toml":
            assert (repo / name).read_text(encoding="utf-8") == before[name]


def test_a_dry_run_shows_the_diff_and_writes_nothing(repo: Path) -> None:
    before = {name: (repo / name).read_text(encoding="utf-8") for name in FILES}
    version = next_major(repo)

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            version,
            "--dry-run",
            "--date",
            "2030-01-02",
            "--root",
            str(repo),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert f"+{version}" in result.stdout
    assert "dry run: 6 files would change" in result.stdout
    assert {name: (repo / name).read_text(encoding="utf-8") for name in FILES} == before


@pytest.mark.parametrize(
    ("new", "old", "expected"),
    [
        ("0.22.1", "0.22.0", True),
        ("0.23.0", "0.22.9", True),
        ("0.22.0", "0.22.0", False),
        ("1.0.0", "1.0.0-rc2", True),
        ("1.0.0-rc10", "1.0.0-rc9", True),
        ("1.0.0-rc1", "1.0.0", False),
    ],
)
def test_newer_means_what_a_release_means(new: str, old: str, expected: bool) -> None:
    assert is_newer(new, old) is expected
