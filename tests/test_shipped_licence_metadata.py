"""Hold the licence claims in `THIRD_PARTY_LICENSES.md` to the installed packages.

`test_shipped_dependency_parity` compares the table's *version* column against
the pin. Nothing compared the *License* column against anything, so the column
that carries the legal claim was the one nothing checked: a dependency could be
relicensed between two versions and the notice we redistribute would keep saying
what it used to be.

The release checklist asks a human to "verify no new copyleft (GPL/AGPL)
dependency was introduced". That is a question the installed metadata can answer,
so it is asked here on every run instead of once per release from memory.

Only the identifier is checked. The rest of the column is curated prose that
metadata cannot express -- "MIT (+ bundled filter plugins, see below)", or
zstandard's note that libzstd is dual BSD-3-Clause/GPL-2.0 and taken under the
BSD terms -- and is deliberately left to a human.
"""

from __future__ import annotations

import re
from importlib.metadata import PackageNotFoundError, metadata
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
REQUIREMENTS = ROOT / "backend" / "requirements.txt"
LICENSES = ROOT / "THIRD_PARTY_LICENSES.md"

_PIN_RE = re.compile(r"^(?P<name>[A-Za-z0-9._-]+)(?P<extras>\[[^\]]*\])?==(?P<version>[^\s;]+)")
_ROW_RE = re.compile(r"^\|\s*([^|]+?)\s*\|\s*([0-9][^|]*?)\s*\|\s*([^|]+?)\s*\|", re.MULTILINE)
_SPDX_RE = re.compile(r"[A-Za-z0-9.+-]+")
_OPERATORS = {"AND", "OR", "WITH"}

# Strong copyleft, which ALBIS does not redistribute. LGPL and MPL are absent on
# purpose: certifi already ships under MPL-2.0, which is per-file copyleft and
# fine for an unmodified redistributed dependency, and the release checklist
# names GPL/AGPL specifically. Prefixes are matched against whole identifiers so
# that LGPL-2.1 is not read as a GPL.
_COPYLEFT_PREFIXES = ("GPL-", "AGPL-")

# Classifiers, for packages whose metadata offers no SPDX expression. Kept to
# the unambiguous ones: "BSD License" and "Apache Software License" do not name
# a version, so a package that only says that counts as unusable metadata below
# rather than being guessed at.
_CLASSIFIER_SPDX = {
    "MIT License": "MIT",
    "Mozilla Public License 2.0 (MPL 2.0)": "MPL-2.0",
}

# Packages whose installed metadata states no licence usably. Each needs a
# reason, and the test below fails when a package leaves this set, so the escape
# hatch cannot quietly widen.
_UNVERIFIABLE = {
    "hdf5plugin": (
        "the wheel carries no License-Expression, no License field and no licence "
        "classifier; the table's claim comes from the project's own LICENSE file, "
        "which also covers the bundled filter plugins listed further down"
    ),
}


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


def _normalize(name: str) -> str:
    return re.sub(r"[-_.]+", "-", re.sub(r"\(.*?\)", "", name)).strip().lower()


def _declared() -> dict[str, str]:
    """The identifier each table row claims, keyed by normalized package name."""
    declared: dict[str, str] = {}
    for name, _version, licence in _ROW_RE.findall(LICENSES.read_text(encoding="utf-8")):
        identifier = _SPDX_RE.match(licence.strip())
        if identifier:
            declared[_normalize(name)] = identifier.group(0)
    return declared


def _installed_identifiers(package: str) -> set[str] | None:
    """SPDX identifiers the installed distribution states, or None if it states none."""
    try:
        md = metadata(package)
    except PackageNotFoundError:
        return None

    expression = (md.get("License-Expression") or "").strip()
    if not expression:
        raw = (md.get("License") or "").strip()
        # A one-line value is an expression; FabIO's `License` is 6.5 KB of
        # Debian copyright file, which is text about a licence, not one.
        if raw and "\n" not in raw and len(raw) <= 60:
            expression = raw

    if expression:
        return {
            token
            for token in re.split(r"[()\s]+", expression)
            if token and token.upper() not in _OPERATORS
        }

    identifiers = {
        spdx
        for classifier in md.get_all("Classifier") or []
        if classifier.startswith("License ::")
        and (spdx := _CLASSIFIER_SPDX.get(classifier.split("::")[-1].strip()))
    }
    return identifiers or None


def _require_installed(package: str) -> set[str]:
    if package in _UNVERIFIABLE:
        pytest.skip(f"{package}: {_UNVERIFIABLE[package]}")
    identifiers = _installed_identifiers(package)
    if identifiers is None:
        try:
            metadata(package)
        except PackageNotFoundError:
            # pyobjc is pinned for macOS only, so it is legitimately absent on
            # the other two CI platforms.
            pytest.skip(f"{package} is not installed in this environment")
        pytest.fail(
            f"{package} states no usable licence in its installed metadata. Add it to "
            "_UNVERIFIABLE with the reason, and say in THIRD_PARTY_LICENSES.md where "
            "the claim comes from."
        )
    return identifiers


@pytest.mark.parametrize("package", sorted(_pins()))
def test_the_licence_table_states_a_licence_the_package_actually_declares(package: str) -> None:
    identifiers = _require_installed(package)
    declared = _declared()

    assert package in declared, f"{package} has no licence identifier in THIRD_PARTY_LICENSES.md"
    assert declared[package] in identifiers, (
        f"THIRD_PARTY_LICENSES.md says {package} is {declared[package]}, but the installed "
        f"{package} declares {sorted(identifiers)}. If upstream relicensed, the table and the "
        "licence text below it both need updating -- this is the notice we redistribute."
    )


def _copyleft(identifiers: set[str]) -> list[str]:
    return sorted(
        identifier
        for identifier in identifiers
        if identifier.upper().startswith(_COPYLEFT_PREFIXES)
    )


@pytest.mark.parametrize(
    "identifier,flagged",
    [
        ("GPL-2.0", True),
        ("GPL-3.0-or-later", True),
        ("AGPL-3.0", True),
        # Weak copyleft, and MPL-2.0 already ships in certifi: not this rule's business.
        ("LGPL-2.1", False),
        ("MPL-2.0", False),
        ("BSD-3-Clause", False),
        ("MIT", False),
    ],
)
def test_the_copyleft_rule_reads_identifiers_the_way_spdx_writes_them(
    identifier: str, flagged: bool
) -> None:
    """Otherwise the check below is one that can only ever pass in silence.

    The prefix match is the part worth pinning: `LGPL-2.1` contains `GPL`, and a
    substring test would refuse a licence this rule is not about.
    """
    assert bool(_copyleft({identifier})) is flagged


@pytest.mark.parametrize("package", sorted(_pins()))
def test_no_shipped_dependency_is_strong_copyleft(package: str) -> None:
    """The release checklist's "no new copyleft (GPL/AGPL)" step, asked every run."""
    copyleft = _copyleft(_require_installed(package))
    assert not copyleft, (
        f"{package} declares {copyleft}, which ALBIS does not redistribute. A dual-licensed "
        "dependency taken under its permissive terms belongs in the table's note column, the "
        "way zstandard's libzstd is."
    )


def test_the_unverifiable_set_only_names_packages_that_are_shipped() -> None:
    """An exemption for a package that is gone is an exemption nobody reviews."""
    stale = sorted(set(_UNVERIFIABLE) - set(_pins()))
    assert not stale, f"_UNVERIFIABLE names {stale}, which are no longer pinned"


def test_the_table_declares_an_identifier_for_every_shipped_package() -> None:
    """Includes the unverifiable ones: no metadata is not a reason to claim nothing."""
    missing = sorted(set(_pins()) - set(_declared()))
    assert not missing, f"no licence identifier in THIRD_PARTY_LICENSES.md for {missing}"
