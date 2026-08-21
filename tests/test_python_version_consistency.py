"""Hold every statement of the supported Python version to the same number.

The version is named in six places: `.python-version`, `pyproject.toml`, the
tool targets for ruff and black, four workflows, and the Docker base image --
twice, once as the tag and once inside `HDF5_PLUGIN_PATH`. Nothing compared
them, and the failure modes are not symmetric:

A workflow left behind builds and tests on the old interpreter while the project
says otherwise. The `HDF5_PLUGIN_PATH` left behind is worse, because it does not
fail: the image builds, starts, and answers `/api/health`, and simply loses every
HDF5 compression filter -- so real detector data stops decoding while a smoke
test that only asks whether the server is up stays green.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
VERSION_FILE = ROOT / ".python-version"
SUPPORTED = VERSION_FILE.read_text(encoding="utf-8").strip()
WORKFLOWS = sorted((ROOT / ".github" / "workflows").glob("*.yml"))


def test_the_version_file_names_a_minor_version() -> None:
    assert re.fullmatch(r"3\.\d+", SUPPORTED), (
        f".python-version is {SUPPORTED!r}; the rest of this file compares against it, "
        "so it has to be a bare MAJOR.MINOR"
    )


def test_pyproject_requires_the_supported_version() -> None:
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    match = re.search(r'requires-python\s*=\s*"([^"]+)"', text)

    assert match, "pyproject.toml no longer declares requires-python"
    assert f">={SUPPORTED}" in match.group(
        1
    ), f"pyproject.toml requires {match.group(1)!r} but .python-version says {SUPPORTED}"


@pytest.mark.parametrize("tool", ["black", "ruff"])
def test_the_formatter_and_linter_target_the_supported_version(tool: str) -> None:
    """A stale target silently keeps rewriting code for an older syntax level."""
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    expected = "py" + SUPPORTED.replace(".", "")
    section = re.search(rf"\[tool\.{tool}[^\]]*\](.*?)(?=^\[|\Z)", text, re.M | re.S)

    assert section, f"pyproject.toml has no [tool.{tool}] section"
    targets = re.findall(r'target-version\s*=\s*(?:\[)?"([^"]+)"', section.group(1))
    assert targets, f"[tool.{tool}] declares no target-version"
    assert all(
        target == expected for target in targets
    ), f"[tool.{tool}] targets {targets} but .python-version says {SUPPORTED} ({expected})"


@pytest.mark.parametrize("workflow", WORKFLOWS, ids=lambda path: path.name)
def test_no_workflow_is_left_on_another_interpreter(workflow: Path) -> None:
    text = workflow.read_text(encoding="utf-8")
    pinned = set(re.findall(r'python-version:\s*\[?"([^"\]]+)"', text))

    unexpected = sorted(version for version in pinned if version != SUPPORTED)
    assert (
        not unexpected
    ), f"{workflow.name} sets up Python {unexpected} but .python-version says {SUPPORTED}"


def test_the_docker_image_and_its_plugin_path_agree_with_the_project() -> None:
    text = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    tags = set(re.findall(r"^FROM python:(\d+\.\d+)\.\d+-slim", text, re.M))
    assert tags, "no pinned python base image found in the Dockerfile"
    assert tags == {
        SUPPORTED
    }, f"the Dockerfile builds on Python {sorted(tags)} but .python-version says {SUPPORTED}"

    plugin_paths = set(re.findall(r"HDF5_PLUGIN_PATH=\S*?/python(\d+\.\d+)/", text))
    assert plugin_paths == {SUPPORTED}, (
        f"HDF5_PLUGIN_PATH points at python{sorted(plugin_paths)} but the image is "
        f"Python {SUPPORTED}. The image would still start and silently lose every "
        "HDF5 compression filter."
    )


def test_the_base_image_is_pinned_by_digest() -> None:
    """A tag can be re-pointed; a digest cannot. Bumping one means bumping both."""
    text = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    froms = re.findall(r"^FROM (python:\S+)", text, re.M)

    assert froms, "no FROM python: lines found"
    for image in froms:
        assert "@sha256:" in image, f"{image} is not pinned by digest"
    assert (
        len(set(froms)) == 1
    ), f"the build stages disagree on the base image: {sorted(set(froms))}"


def test_the_developer_guide_tells_contributors_the_right_version() -> None:
    guide = (ROOT / "docs" / "DEVELOPER_GUIDE.md").read_text(encoding="utf-8")

    assert f"Python {SUPPORTED}" in guide, (
        f"the Developer Guide does not name Python {SUPPORTED}; a contributor would set up "
        "the wrong interpreter"
    )
