#!/usr/bin/env python3
"""Set a new release version in every file that states it, and date the changelog.

A release states its version in six places -- `VERSION`, `package.json`,
`package-lock.json` (twice), `pyproject.toml` and `CITATION.cff` -- and dates
itself in two: `CITATION.cff` and the `CHANGELOG.md` section that moves out of
`[Unreleased]`, with a compare link at the bottom. `tests/test_version_consistency.py`
catches a version left behind, but nothing caught the rest: the lockfile went
two releases stale, and the date and links were easy to miss. This does all of
it the same way every time.

Every edit has to match exactly once. A file whose layout has drifted makes
this stop and name the file, rather than guess or skip it.

    python scripts/bump_version.py 0.23.0                    # rewrite in place
    python scripts/bump_version.py 0.23.0 --dry-run          # show the diff only
    python scripts/bump_version.py 0.23.0 --date 2026-10-10  # other than today
"""

from __future__ import annotations

import argparse
import datetime as dt
import difflib
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

_VERSION_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:-((?:rc|alpha|beta|pre)\d*))?$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class BumpError(ValueError):
    """The bump cannot be made as asked; the message says why and where."""


def parse_version(text: str) -> tuple[int, int, int, str]:
    match = _VERSION_RE.match(text.strip())
    if not match:
        raise BumpError(f"{text!r} is not a release version (MAJOR.MINOR.PATCH, optionally -rcN)")
    major, minor, patch, pre = match.groups()
    return int(major), int(minor), int(patch), pre or ""


def is_newer(new: str, current: str) -> bool:
    """Higher MAJOR.MINOR.PATCH, or the final release of a current pre-release."""
    new_core, new_pre = parse_version(new)[:3], parse_version(new)[3]
    cur_core, cur_pre = parse_version(current)[:3], parse_version(current)[3]
    if new_core != cur_core:
        return new_core > cur_core
    # Same core: 1.0.0-rc1 -> 1.0.0 or -> 1.0.0-rc2 moves forward; nothing else does.
    if not cur_pre:
        return False
    return not new_pre or _pre_key(new_pre) > _pre_key(cur_pre)


def _pre_key(pre: str) -> tuple[str, int]:
    """rc10 after rc9: the number compares as a number, not as text."""
    match = re.match(r"([a-z]+)(\d*)$", pre)
    return (match.group(1), int(match.group(2) or 0)) if match else (pre, 0)


def _replace_once(
    text: str, pattern: str, replacement: str, where: str, flags: int = re.MULTILINE
) -> str:
    result, count = re.subn(pattern, replacement, text, count=0, flags=flags)
    if count != 1:
        raise BumpError(f"{where}: expected exactly one match for {pattern!r}, found {count}")
    return result


def bump_changelog(text: str, version: str, date: str) -> str:
    head = "## [Unreleased]\n"
    start = text.find(head)
    if start < 0:
        raise BumpError("CHANGELOG.md: no '## [Unreleased]' section")
    if f"## [{version}]" in text:
        raise BumpError(f"CHANGELOG.md: already has a section for {version}")
    body_start = start + len(head)
    next_section = text.find("\n## [", body_start)
    body = text[body_start : next_section if next_section >= 0 else len(text)]
    if not re.search(r"^### .+\n+- ", body, re.MULTILINE):
        raise BumpError("CHANGELOG.md: nothing under [Unreleased] to release")
    text = text[:body_start] + f"\n## [{version}] - {date}\n" + text[body_start:]

    link = re.search(
        r"^\[Unreleased\]: (?P<base>\S+/compare/)v(?P<prev>[^.\s]+\.[^.\s]+\.[^.\s]+)\.\.\.HEAD$",
        text,
        re.MULTILINE,
    )
    if not link:
        raise BumpError("CHANGELOG.md: no '[Unreleased]: .../compare/vX.Y.Z...HEAD' link")
    base, prev = link.group("base"), link.group("prev")
    return text.replace(
        link.group(0),
        f"[Unreleased]: {base}v{version}...HEAD\n[{version}]: {base}v{prev}...v{version}",
        1,
    )


def bump_texts(texts: dict[str, str], version: str, date: str) -> dict[str, str]:
    """The six files' new contents, from their current ones. Raises BumpError."""
    parse_version(version)
    if not _DATE_RE.match(date):
        raise BumpError(f"{date!r} is not a date (YYYY-MM-DD)")
    current = texts["VERSION"].strip()
    if not is_newer(version, current):
        raise BumpError(f"{version} is not newer than the current {current}")

    out = dict(texts)
    out["VERSION"] = version + "\n"
    out["package.json"] = _replace_once(
        texts["package.json"],
        r'^(  "version": ")[^"]+(",?)$',
        rf"\g<1>{version}\g<2>",
        "package.json",
    )
    lock = _replace_once(
        texts["package-lock.json"],
        r'^(  "version": ")[^"]+(",?)$',
        rf"\g<1>{version}\g<2>",
        "package-lock.json",
    )
    out["package-lock.json"] = _replace_once(
        lock,
        r'(\n    "": \{\n      "name": "[^"]+",\n      "version": ")[^"]+(")',
        rf"\g<1>{version}\g<2>",
        "package-lock.json (root package)",
        flags=0,
    )
    out["pyproject.toml"] = _replace_once(
        texts["pyproject.toml"],
        r'(\[project\]\n(?:(?!\[)[^\n]*\n)*?version = ")[^"]+(")',
        rf"\g<1>{version}\g<2>",
        "pyproject.toml [project]",
        flags=0,
    )
    citation = _replace_once(
        texts["CITATION.cff"], r"^version: .*$", f"version: {version}", "CITATION.cff"
    )
    out["CITATION.cff"] = _replace_once(
        citation, r"^date-released: .*$", f"date-released: {date}", "CITATION.cff"
    )
    out["CHANGELOG.md"] = bump_changelog(texts["CHANGELOG.md"], version, date)
    return out


FILES = (
    "VERSION",
    "package.json",
    "package-lock.json",
    "pyproject.toml",
    "CITATION.cff",
    "CHANGELOG.md",
)


def bump(root: Path, version: str, date: str, *, write: bool = True) -> dict[str, tuple[str, str]]:
    """Bump the files under `root`. Returns {file: (old, new)} for those that change."""
    texts = {name: (root / name).read_text(encoding="utf-8") for name in FILES}
    new = bump_texts(texts, version, date)
    changes = {name: (texts[name], new[name]) for name in FILES if texts[name] != new[name]}
    if write:
        for name, (_, text) in changes.items():
            (root / name).write_text(text, encoding="utf-8")
    return changes


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("version", help="the new version, e.g. 0.23.0")
    parser.add_argument(
        "--date", default=dt.date.today().isoformat(), help="release date, YYYY-MM-DD"
    )
    parser.add_argument("--dry-run", action="store_true", help="print the diff, write nothing")
    parser.add_argument("--root", type=Path, default=ROOT, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    try:
        changes = bump(args.root, args.version, args.date, write=not args.dry_run)
    except BumpError as exc:
        print(f"bump_version: {exc}", file=sys.stderr)
        return 1
    for name, (old, new) in changes.items():
        if args.dry_run:
            sys.stdout.writelines(
                difflib.unified_diff(
                    old.splitlines(True), new.splitlines(True), f"a/{name}", f"b/{name}", n=1
                )
            )
        else:
            print(f"bumped {name}")
    if args.dry_run:
        print(f"(dry run: {len(changes)} files would change)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
