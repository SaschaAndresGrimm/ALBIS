"""The weekly interpreter bump picks the right release and rewrites only the pin."""

from __future__ import annotations

import importlib.util
from datetime import UTC, datetime
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "bump_python_standalone", ROOT / "scripts" / "bump_python_standalone.py"
)
bump = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(bump)

NOW = datetime(2026, 12, 15, tzinfo=UTC)
SCRIPT = bump.INSTALL_SCRIPT.read_text(encoding="utf-8")


def _release(tag: str, published: str, versions=("3.13.19",), platforms=bump.PLATFORMS, **extra):
    assets = [
        {
            "name": f"cpython-{v}+{tag}-{p}-install_only.tar.gz",
            "digest": "sha256:" + f"{tag}{v}{p}".encode().hex()[:64].ljust(64, "0"),
        }
        for v in versions
        for p in platforms
    ]
    # Noise every real release has: other flavours, other minors, checksums.
    assets += [
        {
            "name": f"cpython-3.14.2+{tag}-x86_64-unknown-linux-gnu-install_only.tar.gz",
            "digest": "sha256:" + "1" * 64,
        },
        {
            "name": f"cpython-3.13.19+{tag}-x86_64-unknown-linux-gnu-debug-full.tar.zst",
            "digest": "sha256:" + "2" * 64,
        },
    ]
    return {"tag_name": tag, "published_at": published, "assets": assets, **extra}


def test_the_current_pin_is_read_from_the_install_script() -> None:
    release, version = bump.current_pin(SCRIPT)
    assert len(release) == 8 and release.isdigit()
    assert version.startswith("3.13.")


def test_the_newest_release_old_enough_is_chosen() -> None:
    releases = [
        _release("20261210", "2026-12-10T00:00:00Z"),  # five days old: too fresh
        _release("20261125", "2026-11-25T00:00:00Z"),
        _release("20261118", "2026-11-18T00:00:00Z"),
    ]
    tag, version, shas = bump.choose_release(releases, "20261110", "3.13", NOW)
    assert (tag, version) == ("20261125", "3.13.19")
    assert set(shas) == set(bump.PLATFORMS)


def test_nothing_newer_than_the_pin_means_no_bump() -> None:
    releases = [
        _release("20261110", "2026-11-11T00:00:00Z"),
        _release("20261026", "2026-10-26T00:00:00Z"),
    ]
    assert bump.choose_release(releases, "20261110", "3.13", NOW) is None


def test_a_release_missing_a_platform_is_skipped() -> None:
    partial = _release("20261125", "2026-11-25T00:00:00Z", platforms=bump.PLATFORMS[:-1])
    assert bump.choose_release([partial], "20261110", "3.13", NOW) is None


def test_the_newest_python_patch_in_a_release_is_taken() -> None:
    release = _release("20261125", "2026-11-25T00:00:00Z", versions=("3.13.19", "3.13.20"))
    assert bump.choose_release([release], "20261110", "3.13", NOW)[1] == "3.13.20"


@pytest.mark.parametrize("flag", ["draft", "prerelease"])
def test_drafts_and_prereleases_are_ignored(flag: str) -> None:
    release = _release("20261125", "2026-11-25T00:00:00Z", **{flag: True})
    assert bump.choose_release([release], "20261110", "3.13", NOW) is None


def test_the_rewrite_changes_the_pin_and_every_checksum_and_nothing_else() -> None:
    shas = {p: f"{i:x}" * 64 for i, p in enumerate(bump.PLATFORMS, start=1)}
    text = bump.rewrite(SCRIPT, "20261125", "3.13.19", shas)
    assert bump.current_pin(text) == ("20261125", "3.13.19")
    for platform, sha in shas.items():
        assert f'{platform}) default_sha="{sha}" ;;' in text
    changed = [a for a, b in zip(SCRIPT.splitlines(), text.splitlines(), strict=True) if a != b]
    assert len(changed) == 2 + len(bump.PLATFORMS)
