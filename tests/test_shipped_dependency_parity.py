"""Hold every artifact to the same pinned dependency set.

`backend/requirements.txt` is the pin, `THIRD_PARTY_LICENSES.md` is the claim
about what ships, and the Dockerfile builds one of the artifacts. All three had
drifted apart: the image installed `hdf5plugin==4.1.3` while the pin and the
licence table both said `7.0.0`, so the published container had different HDF5
filter support from the desktop builds and its licence notice described a version
it did not contain. Nothing failed, because nothing compared them.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
REQUIREMENTS = ROOT / "backend" / "requirements.txt"
DEV_REQUIREMENTS = ROOT / "requirements-dev.txt"
PRE_COMMIT = ROOT / ".pre-commit-config.yaml"
DOCKERFILE = ROOT / "Dockerfile"
LICENSES = ROOT / "THIRD_PARTY_LICENSES.md"

_PIN_RE = re.compile(r"^(?P<name>[A-Za-z0-9._-]+)(?P<extras>\[[^\]]*\])?==(?P<version>[^\s;]+)")


def _normalize(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).strip().lower()


def _pins() -> dict[str, str]:
    pins: dict[str, str] = {}
    for line in REQUIREMENTS.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = _PIN_RE.match(stripped)
        assert match, f"unparsed requirement line: {stripped!r}"
        pins[_normalize(match.group("name"))] = match.group("version")
    return pins


def test_requirements_are_all_pinned_to_an_exact_version() -> None:
    """The parity checks below only mean something if there is one version to compare."""
    assert _pins(), "no pinned requirements found; has the file format changed?"


@pytest.mark.parametrize("package,version", sorted(_pins().items()))
def test_the_docker_image_installs_the_pinned_version(package: str, version: str) -> None:
    """A version named in the Dockerfile must be the version that is pinned.

    The image is allowed to name a package -- to install it in a separate step
    for a documented reason -- but not to name a different version of it than
    every other artifact ships.
    """
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")

    for match in re.finditer(r'"([A-Za-z0-9._-]+)==([^"\s]+)"', dockerfile):
        name, pinned = _normalize(match.group(1)), match.group(2)
        if name != package:
            continue
        assert pinned == version, (
            f"Dockerfile installs {package}=={pinned} but backend/requirements.txt pins "
            f"{version}. The image would ship a different dependency set from every "
            "other artifact."
        )


def test_no_requirement_is_excluded_from_the_docker_install() -> None:
    """Carving a package out of the requirements install is how the drift happened."""
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")

    excluded = re.findall(r"grep -v '\^([A-Za-z0-9._-]+)==", dockerfile)

    assert not excluded, (
        f"the Dockerfile filters {excluded} out of backend/requirements.txt and installs it "
        "separately. If that is genuinely required, pin the same version and say why here."
    )


@pytest.mark.parametrize("package,version", sorted(_pins().items()))
def test_the_licence_table_states_the_version_that_ships(package: str, version: str) -> None:
    """The release checklist requires this column to match what is shipped."""
    rows = re.findall(
        r"^\|\s*([^|]+?)\s*\|\s*([0-9][^|]*?)\s*\|", LICENSES.read_text("utf-8"), re.M
    )
    listed = {_normalize(re.sub(r"\(.*?\)", "", name)): declared for name, declared in rows}

    assert package in listed, (
        f"{package} is shipped but has no row in THIRD_PARTY_LICENSES.md. Every "
        "redistributed dependency needs its licence recorded."
    )
    assert listed[package] == version, (
        f"THIRD_PARTY_LICENSES.md says {package} {listed[package]} but "
        f"backend/requirements.txt pins {version}. This column is derived: run "
        "`python scripts/sync_licence_table.py`. Whether the *licence* still holds after "
        "the bump is a separate question, asked by test_shipped_licence_metadata."
    )


# --------------------------------------------------------------------------
# Development tooling
# --------------------------------------------------------------------------


def _dev_pins() -> dict[str, str]:
    pins: dict[str, str] = {}
    for line in DEV_REQUIREMENTS.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith(("#", "-r ", "--")):
            continue
        match = _PIN_RE.match(stripped)
        assert match, (
            f"unpinned development requirement: {stripped!r}. A lint or format tool that "
            "resolves to whatever is newest turns an unrelated upstream release into a red "
            "build on a commit that changed nothing."
        )
        pins[_normalize(match.group("name"))] = match.group("version")
    return pins


def test_every_development_tool_is_pinned() -> None:
    """The assertion lives in the parser above; this proves it ran on something."""
    assert set(_dev_pins()) >= {"pytest", "ruff", "black"}


@pytest.mark.parametrize("tool", ["ruff", "black"])
def test_the_commit_hook_runs_the_same_tool_version_as_ci(tool: str) -> None:
    """A hook that passes while CI fails is worse than no hook at all.

    `.pre-commit-config.yaml` pinned ruff `v0.9.7` while CI installed whatever
    was newest -- seven minor versions apart, checking different rules.
    """
    pinned = _dev_pins()[tool]
    config = PRE_COMMIT.read_text(encoding="utf-8")

    revisions = re.findall(rf"{tool}[^\n]*\n\s*rev:\s*v?([0-9][^\s]*)", config)
    if not revisions:
        revisions = re.findall(rf"repo:\s*https://\S*{tool}\S*\n\s*rev:\s*v?([0-9][^\s]*)", config)
    assert revisions, f"no pre-commit revision found for {tool}"
    assert all(rev == pinned for rev in revisions), (
        f".pre-commit-config.yaml runs {tool} {revisions} but requirements-dev.txt pins "
        f"{pinned}, so the hook and CI check different things"
    )


def test_formatting_is_owned_by_one_tool() -> None:
    """Two formatters is a fight, not a policy.

    The hooks ran `ruff-format` while CI gates on `black`, and the two disagree:
    ruff-format rewrote files black considers correct, so a contributor with the
    hook installed had commits reformatted into a state CI then rejected.
    """
    config = PRE_COMMIT.read_text(encoding="utf-8")
    workflows = "\n".join(
        path.read_text(encoding="utf-8") for path in (ROOT / ".github" / "workflows").glob("*.yml")
    )

    # Matched as a hook id, not as a mention: the config explains in a comment
    # why ruff-format is absent, and a substring check would trip over that.
    enabled_hooks = re.findall(r"^\s*-\s*id:\s*(\S+)", config, re.M)
    assert "ruff-format" not in enabled_hooks, (
        "the commit hooks run ruff-format, but CI formats with black. Pick one; if it is to "
        "be ruff-format, change the workflows in the same commit."
    )
    assert "black --check" in workflows, "no workflow gates on black; has the formatter changed?"
