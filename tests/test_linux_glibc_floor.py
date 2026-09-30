"""Hold the Linux glibc floor to one number across the build, its check and the docs.

The floor is set by where the bundle is built, not by anything ALBIS calls: a
binary needs a glibc at least as new as the one it was linked against. So the
build image, the `check_glibc_floor.sh` argument and the README's promise all
state the same fact. If they drift, the failure is silent in one direction: a
build image newer than the check's floor fails CI, but a floor raised to match a
newer image passes everything and quietly drops RHEL 8, which is what happened
when the build sat on Ubuntu 22.04 and the floor was 2.35.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
FLOOR = "2.28"
WORKFLOWS = [ROOT / ".github" / "workflows" / name for name in ("release.yml", "artifacts.yml")]
SUPPORTED_PYTHON = (ROOT / ".python-version").read_text(encoding="utf-8").strip()


def _jobs(workflow: Path) -> dict:
    return yaml.safe_load(workflow.read_text(encoding="utf-8"))["jobs"]


def _run_text(job: dict) -> str:
    return "\n".join(str(step.get("run", "")) for step in job["steps"])


@pytest.mark.parametrize("workflow", WORKFLOWS, ids=lambda path: path.name)
def test_linux_builds_in_a_pinned_image_matching_the_floor(workflow: Path) -> None:
    job = _jobs(workflow)["build_linux"]
    container = job.get("container")
    image = container["image"] if isinstance(container, dict) else container

    assert image, f"{workflow.name} builds Linux on the runner's own distribution"
    match = re.fullmatch(r"quay\.io/pypa/manylinux_(\d+)_(\d+)_x86_64@sha256:[0-9a-f]{64}", image)
    assert match, f"{workflow.name} builds Linux in {image!r}, not a digest-pinned manylinux image"
    assert f"{match.group(1)}.{match.group(2)}" == FLOOR, (
        f"{workflow.name} builds in manylinux_{match.group(1)}_{match.group(2)} "
        f"but the supported floor is glibc {FLOOR}"
    )


@pytest.mark.parametrize("workflow", WORKFLOWS, ids=lambda path: path.name)
def test_the_glibc_check_enforces_the_floor(workflow: Path) -> None:
    runs = _run_text(_jobs(workflow)["build_linux"])
    floors = re.findall(r"check_glibc_floor\.sh\s+(\S+)", runs)

    assert floors == [FLOOR], f"{workflow.name} checks glibc floors {floors}, expected [{FLOOR!r}]"


def test_both_workflows_build_in_the_same_image() -> None:
    images = {_jobs(workflow)["build_linux"]["container"]["image"] for workflow in WORKFLOWS}

    assert len(images) == 1, f"release and artifact builds use different images: {sorted(images)}"


@pytest.mark.parametrize("workflow", WORKFLOWS, ids=lambda path: path.name)
def test_the_tarball_is_started_on_other_distributions(workflow: Path) -> None:
    job = _jobs(workflow)["smoke_linux"]
    images = [entry["image"] for entry in job["strategy"]["matrix"]["include"]]

    assert job["needs"] == "build_linux"
    assert any(
        image.startswith("rockylinux/rockylinux:8@") for image in images
    ), f"{workflow.name} does not start the tarball on a glibc {FLOOR} distribution"
    for image in images:
        assert "@sha256:" in image, f"{workflow.name} smoke image {image} is not pinned by digest"
    assert "smoke_linux_distro.sh" in _run_text(job)


def test_a_release_waits_for_the_distribution_smoke_test() -> None:
    needs = _jobs(WORKFLOWS[0])["publish"]["needs"]

    assert "smoke_linux" in needs, "publish does not wait for the Linux distribution smoke test"


def test_the_build_interpreter_is_the_supported_python() -> None:
    text = (ROOT / "scripts" / "install_python_standalone.sh").read_text(encoding="utf-8")
    match = re.search(r'PBS_PYTHON_VERSION="\$\{PBS_PYTHON_VERSION:-(\d+\.\d+)\.\d+\}"', text)

    assert match, "scripts/install_python_standalone.sh no longer pins a Python version"
    assert match.group(1) == SUPPORTED_PYTHON, (
        f"the Linux release is built on Python {match.group(1)} "
        f"but .python-version says {SUPPORTED_PYTHON}"
    )


def test_the_readme_states_the_floor_the_build_enforces() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    stated = re.findall(r"require `glibc (\d+\.\d+)\+`", readme)

    assert stated == [FLOOR], f"README states glibc floors {stated}, the build enforces {FLOOR}"
