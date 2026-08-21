"""Which build of ALBIS is running, as opposed to which version.

`VERSION` answers "which release is this" and is enough for a changelog. It is
not enough for a bug report: two builds of `0.11.0` can differ by a rebuild, a
hotfix branch, or a local checkout, and the release artifacts already
distinguish them -- they are named `v0.11.0-a1b2c3d`. That commit was computed
at packaging time by `scripts/version_info.py` for the filename and then thrown
away, so the running program could not say which build it was. Support had a
version number and no way to tell what was actually installed.

Resolution order, first hit wins:

1. `ALBIS_BUILD_COMMIT` in the environment. This is how the Docker image learns
   its commit, passed as a build argument.
2. A `BUILD_COMMIT` file beside `VERSION` -- written by the release workflow
   before PyInstaller runs, and bundled into the packaged app the same way
   `VERSION` is.
3. `git rev-parse`, for someone running from a checkout, where the working tree
   is the answer.
4. Nothing. An unstamped build reports no commit rather than inventing one, and
   the interface falls back to showing the version alone.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

_COMMIT_ENV_VAR = "ALBIS_BUILD_COMMIT"
_COMMIT_FILE = "BUILD_COMMIT"

# Hex only, and bounded. A commit reaches the interface and a bug report, so a
# stray newline or a whole `git describe` line should not travel with it.
_COMMIT_RE = re.compile(r"[0-9a-f]{7,40}")


# What the interface and a bug report both want to show. Sources disagree on
# length -- `git rev-parse --short` gives 7, a CI `github.sha` gives 40 -- so the
# length is decided here rather than at each call site.
_DISPLAY_LENGTH = 7


def _normalize(raw: str) -> str:
    token = str(raw or "").strip().lower()
    if not _COMMIT_RE.fullmatch(token):
        return ""
    return token[:_DISPLAY_LENGTH]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _commit_file_candidates() -> tuple[Path, ...]:
    """Mirror `version.py`: the bundle first, then the checkout."""
    candidates: list[Path] = []
    pyinstaller_root = getattr(sys, "_MEIPASS", "")
    if pyinstaller_root:
        candidates.append(Path(pyinstaller_root) / _COMMIT_FILE)
    candidates.append(_repo_root() / _COMMIT_FILE)
    return tuple(dict.fromkeys(candidates))


def _commit_from_env() -> str:
    return _normalize(os.environ.get(_COMMIT_ENV_VAR, ""))


def _commit_from_file() -> str:
    for path in _commit_file_candidates():
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError:
            continue
        commit = _normalize(raw)
        if commit:
            return commit
    return ""


def _commit_from_git() -> str:
    """Only meaningful in a checkout, and never allowed to be slow or loud.

    A packaged build has no `.git` and often no `git`, so this is skipped rather
    than attempted there -- the timeout is a backstop for a checkout on a
    filesystem where git hangs, not the normal path.
    """
    if getattr(sys, "_MEIPASS", ""):
        return ""
    if not (_repo_root() / ".git").exists():
        return ""
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short=7", "HEAD"],
            cwd=_repo_root(),
            capture_output=True,
            text=True,
            timeout=2.0,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    if out.returncode != 0:
        return ""
    return _normalize(out.stdout)


def read_commit() -> str:
    """Return the short commit this build came from, or `""` if unstamped."""
    for source in (_commit_from_env, _commit_from_file, _commit_from_git):
        commit = source()
        if commit:
            return commit
    return ""


ALBIS_COMMIT = read_commit()
