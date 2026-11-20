#!/usr/bin/env python3
"""Move the bundled interpreter to the newest python-build-standalone release.

The interpreter is copied into every download with its OpenSSL, SQLite and
expat, so a fix in any of them reaches users only when this pin moves.
Dependabot does not know python-build-standalone; this does its job. The
weekly workflow `.github/workflows/python-standalone-bump.yml` runs it and,
when there is a newer release, pushes a branch and opens an issue to make the
pull request from.

A release qualifies when it is at least MIN_AGE_DAYS old (time for a broken
build to be noticed and replaced upstream), carries the same Python minor
version as the current pin, and has an install_only build for every platform
ALBIS ships, each with a sha256 digest. The newest Python patch release in it
is taken.

    scripts/bump_python_standalone.py            # report only
    scripts/bump_python_standalone.py --write    # rewrite the pin
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INSTALL_SCRIPT = ROOT / "scripts" / "install_python_standalone.sh"
RELEASES_URL = "https://api.github.com/repos/astral-sh/python-build-standalone/releases?per_page=8"
MIN_AGE_DAYS = 14
PLATFORMS = (
    "x86_64-unknown-linux-gnu",
    "aarch64-unknown-linux-gnu",
    "x86_64-apple-darwin",
    "aarch64-apple-darwin",
    "x86_64-pc-windows-msvc",
)

_RELEASE_RE = re.compile(r'PBS_RELEASE="\$\{PBS_RELEASE:-(\d{8})\}"')
_VERSION_RE = re.compile(r'PBS_PYTHON_VERSION="\$\{PBS_PYTHON_VERSION:-(\d+\.\d+\.\d+)\}"')


class BumpError(RuntimeError):
    pass


def current_pin(script: str) -> tuple[str, str]:
    release = _RELEASE_RE.search(script)
    version = _VERSION_RE.search(script)
    if not release or not version:
        raise BumpError(
            "install_python_standalone.sh no longer pins PBS_RELEASE and PBS_PYTHON_VERSION"
        )
    return release.group(1), version.group(1)


def _asset_re(minor: str, tag: str) -> re.Pattern[str]:
    return re.compile(
        rf"^cpython-({re.escape(minor)}\.\d+)\+{tag}-(?P<platform>[a-z0-9_]+-[a-z0-9_-]+)-install_only\.tar\.gz$"
    )


def _patch(version: str) -> int:
    return int(version.rsplit(".", 1)[1])


def release_builds(release: dict, minor: str) -> tuple[str, dict[str, str]] | None:
    """The newest Python `minor`.x with a build for every platform, and their sha256."""
    tag = release["tag_name"]
    pattern = _asset_re(minor, tag)
    by_version: dict[str, dict[str, str]] = {}
    for asset in release.get("assets", []):
        match = pattern.match(asset.get("name", ""))
        digest = asset.get("digest") or ""
        if not match or not digest.startswith("sha256:"):
            continue
        by_version.setdefault(match.group(1), {})[match.group("platform")] = digest.removeprefix(
            "sha256:"
        )
    complete = [v for v, shas in by_version.items() if all(p in shas for p in PLATFORMS)]
    if not complete:
        return None
    version = max(complete, key=_patch)
    return version, {p: by_version[version][p] for p in PLATFORMS}


def choose_release(
    releases: list[dict], current: str, minor: str, now: datetime, min_age_days: int = MIN_AGE_DAYS
) -> tuple[str, str, dict[str, str]] | None:
    """The newest qualifying release after `current`, or None."""
    cutoff = now - timedelta(days=min_age_days)
    candidates = []
    for release in releases:
        tag = release.get("tag_name", "")
        if release.get("draft") or release.get("prerelease") or not re.fullmatch(r"\d{8}", tag):
            continue
        if tag <= current:
            continue
        published = datetime.fromisoformat(release["published_at"].replace("Z", "+00:00"))
        if published > cutoff:
            continue
        builds = release_builds(release, minor)
        if builds:
            candidates.append((tag, *builds))
    return max(candidates, default=None, key=lambda c: c[0])


def rewrite(script: str, release: str, version: str, shas: dict[str, str]) -> str:
    text = _RELEASE_RE.sub(f'PBS_RELEASE="${{PBS_RELEASE:-{release}}}"', script)
    text = _VERSION_RE.sub(f'PBS_PYTHON_VERSION="${{PBS_PYTHON_VERSION:-{version}}}"', text)
    for platform, sha in shas.items():
        line = re.compile(rf'(  {re.escape(platform)}\) default_sha=")[0-9a-f]{{64}}(" ;;)')
        text, count = line.subn(rf"\g<1>{sha}\g<2>", text)
        if count != 1:
            raise BumpError(f"no checksum line for {platform} in install_python_standalone.sh")
    return text


def fetch_releases() -> list[dict]:
    request = urllib.request.Request(
        RELEASES_URL, headers={"Accept": "application/vnd.github+json"}
    )
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--write", action="store_true", help="Rewrite the pin in place.")
    args = parser.parse_args(argv)

    script = INSTALL_SCRIPT.read_text(encoding="utf-8")
    current, version = current_pin(script)
    minor = version.rsplit(".", 1)[0]
    found = choose_release(fetch_releases(), current, minor, datetime.now(UTC))
    if found is None:
        print(f"python-build-standalone {current} (Python {version}) is current.")
        return 0
    release, new_version, shas = found
    print(
        f"python-build-standalone {release} (Python {new_version}) replaces {current} (Python {version})."
    )
    if args.write:
        INSTALL_SCRIPT.write_text(rewrite(script, release, new_version, shas), encoding="utf-8")
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as fh:
            fh.write(f"release={release}\nversion={new_version}\nprevious={current}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
