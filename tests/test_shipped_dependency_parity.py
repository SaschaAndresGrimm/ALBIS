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
        f"backend/requirements.txt pins {version}."
    )
