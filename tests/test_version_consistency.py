"""Keep every file that states the ALBIS version stating the same one.

The version lives in four places, and nothing used to compare them. The release
workflow checks the git tag against `VERSION` and the checklist asked a human to
eyeball the rest, which is how `package.json` and `pyproject.toml` can drift a
release apart without anything failing. `CITATION.cff` makes that worse rather
than better: a wrong version there ends up in someone's bibliography, where it
is discovered by a reader and not by us.

Parsing is deliberately done with regexes rather than a TOML or YAML library.
CI runs Python 3.10, where `tomllib` does not exist, and PyYAML is not a
declared dependency of this project -- adding either just to read one field
would cost more than the check is worth.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

SEMVER = re.compile(r"^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$")


def _version_file() -> str:
    return (ROOT / "VERSION").read_text(encoding="utf-8").strip()


def _package_json_version() -> str:
    return json.loads((ROOT / "package.json").read_text(encoding="utf-8"))["version"]


def _pyproject_version() -> str:
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    project = re.search(r"^\[project\]\s*$(.*?)(?=^\[|\Z)", text, re.M | re.S)
    assert project, "pyproject.toml has no [project] table"
    match = re.search(r'^version\s*=\s*"([^"]+)"', project.group(1), re.M)
    assert match, "pyproject.toml [project] has no version"
    return match.group(1)


def _citation_version() -> str:
    text = (ROOT / "CITATION.cff").read_text(encoding="utf-8")
    # Anchored at column zero so a commented-out example cannot satisfy it.
    match = re.search(r'^version:\s*"?([0-9][^"\s]*)"?\s*$', text, re.M)
    assert match, "CITATION.cff has no top-level version"
    return match.group(1)


SOURCES = {
    "VERSION": _version_file,
    "package.json": _package_json_version,
    "pyproject.toml": _pyproject_version,
    "CITATION.cff": _citation_version,
}


@pytest.mark.parametrize("name", sorted(SOURCES))
def test_version_matches_the_version_file(name: str) -> None:
    """`VERSION` is the source of truth; everything else restates it."""
    expected = _version_file()
    actual = SOURCES[name]()
    assert actual == expected, (
        f"{name} declares version {actual!r} but VERSION says {expected!r}. "
        "All four must match before a release; see docs/RELEASE_CHECKLIST.md."
    )


def test_version_file_is_a_release_version() -> None:
    """A tag is built from this string, so it has to be a version."""
    version = _version_file()
    assert SEMVER.match(version), f"VERSION contains {version!r}, which is not a semver string"


def test_citation_metadata_has_what_a_citation_needs() -> None:
    """GitHub renders the citation from these fields; missing ones degrade quietly."""
    text = (ROOT / "CITATION.cff").read_text(encoding="utf-8")

    for field in ("cff-version", "message", "title", "authors", "type", "license", "date-released"):
        assert re.search(rf"^{re.escape(field)}:", text, re.M), (
            f"CITATION.cff is missing the {field!r} field, which GitHub's citation "
            "rendering and CFF validators both expect."
        )

    released = re.search(r"^date-released:\s*(\d{4}-\d{2}-\d{2})\s*$", text, re.M)
    assert released, "date-released must be an unquoted ISO date (YYYY-MM-DD) for CFF 1.2.0"


def test_citation_file_is_valid_yaml_when_a_parser_is_available() -> None:
    """Structural check, skipped rather than made a dependency of the project."""
    yaml = pytest.importorskip("yaml", reason="PyYAML is not a declared dependency")

    data = yaml.safe_load((ROOT / "CITATION.cff").read_text(encoding="utf-8"))
    assert isinstance(data, dict), "CITATION.cff must parse to a mapping"
    assert data["cff-version"] == "1.2.0"

    authors = data.get("authors")
    assert isinstance(authors, list) and authors, "CITATION.cff must list at least one author"
    for author in authors:
        assert author.get("family-names"), "each author needs family-names"
        assert author.get("given-names"), "each author needs given-names"
