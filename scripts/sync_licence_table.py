#!/usr/bin/env python3
"""Copy the pinned versions into the licence table's Version column.

`THIRD_PARTY_LICENSES.md` states the version of every redistributed dependency,
and `tests/test_shipped_dependency_parity.py` holds it to `backend/requirements.txt`.
That column is derived data with exactly one correct value, but it was maintained
by hand, so every dependency bump arrived with a red build and needed a manual
edit -- Dependabot only touches the requirements file and cannot satisfy a check
that compares the two.

Only the Version cell moves. The License and Copyright columns are curated: they
carry judgements the metadata cannot express ("MIT (+ bundled filter plugins)",
"dual BSD-3-Clause/GPL-2.0 -- used under BSD-3-Clause"), and generating them
would replace accurate prose with noise. Whether the *licence* still holds after
a bump is checked against the installed package metadata instead, by
`tests/test_shipped_licence_metadata.py`.

Rows for components that are not Python pins (the vendored frontend libraries)
are left alone.

    python scripts/sync_licence_table.py            # rewrite in place
    python scripts/sync_licence_table.py --check    # exit 1 if out of date
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REQUIREMENTS = ROOT / "backend" / "requirements.txt"
LICENSES = ROOT / "THIRD_PARTY_LICENSES.md"

_PIN_RE = re.compile(r"^(?P<name>[A-Za-z0-9._-]+)(?P<extras>\[[^\]]*\])?==(?P<version>[^\s;]+)")
# | Component | Version | License | Copyright |
_ROW_RE = re.compile(r"^(\|\s*)([^|]+?)(\s*\|\s*)([0-9][^|]*?)(\s*\|)", re.MULTILINE)


def normalize(name: str) -> str:
    """Fold a distribution name the way PEP 503 does, minus any parenthetical.

    The table labels rows for humans -- "NumPy", "pyobjc (macOS only)" -- so the
    display name has to be reduced before it can be matched against a pin.
    """
    return re.sub(r"[-_.]+", "-", re.sub(r"\(.*?\)", "", name)).strip().lower()


def read_pins(text: str) -> dict[str, str]:
    pins: dict[str, str] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = _PIN_RE.match(stripped)
        if match:
            pins[normalize(match.group("name"))] = match.group("version")
    return pins


def sync(table: str, pins: dict[str, str]) -> tuple[str, list[str]]:
    """Return the table with pinned versions applied, plus a note per change."""
    changes: list[str] = []

    def replace(match: re.Match[str]) -> str:
        lead, name, mid, version, tail = match.groups()
        pinned = pins.get(normalize(name))
        if pinned is None or pinned == version:
            return match.group(0)
        changes.append(f"{name.strip()}: {version} -> {pinned}")
        return f"{lead}{name}{mid}{pinned}{tail}"

    return _ROW_RE.sub(replace, table, count=0), changes


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="report what is out of date and exit non-zero, without writing",
    )
    args = parser.parse_args(argv)

    pins = read_pins(REQUIREMENTS.read_text(encoding="utf-8"))
    if not pins:
        print(f"no pins found in {REQUIREMENTS}; has the file format changed?", file=sys.stderr)
        return 2

    table = LICENSES.read_text(encoding="utf-8")
    updated, changes = sync(table, pins)

    if not changes:
        print(f"{LICENSES.name} already states the pinned versions")
        return 0

    for change in changes:
        print(change)

    if args.check:
        print(f"\n{LICENSES.name} is out of date; run: python {Path(__file__).name}")
        return 1

    LICENSES.write_text(updated, encoding="utf-8")
    print(f"\nwrote {len(changes)} version(s) to {LICENSES.name}")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
