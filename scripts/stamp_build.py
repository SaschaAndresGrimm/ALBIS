#!/usr/bin/env python3
"""Write the commit a build came from, so the running program can name it.

`scripts/version_info.py` already resolves the commit to build artifact names
like `ALBIS-linux-x64-v0.11.0-a1b2c3d.tar.gz`. That answer was used for the
filename and discarded, leaving the program itself unable to say which build it
was -- a version number cannot separate two builds of the same release, which is
exactly what a bug report needs.

Run this before PyInstaller. It writes `BUILD_COMMIT` at the repository root,
which `ALBIS.spec` bundles beside `VERSION` and `backend/build_info.py` reads at
startup. The Docker image takes the same value as a build argument instead.

Writing the file is optional by design: an unstamped build reports no commit and
shows its version alone rather than failing.
"""

from __future__ import annotations

import argparse
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "BUILD_COMMIT"
COMMIT_RE = re.compile(r"[0-9a-f]{7,40}")


def resolve_commit(explicit: str = "") -> str:
    """Prefer an explicitly supplied commit, else ask git."""
    candidate = str(explicit or "").strip().lower()
    if not candidate:
        try:
            candidate = subprocess.check_output(
                ["git", "rev-parse", "--short=7", "HEAD"],
                cwd=ROOT,
                stderr=subprocess.DEVNULL,
                text=True,
            ).strip()
        except (OSError, subprocess.SubprocessError):
            candidate = ""
    candidate = candidate.lower()
    # A CI-supplied SHA is usually full length; the interface wants it short.
    if len(candidate) > 7 and COMMIT_RE.fullmatch(candidate):
        candidate = candidate[:7]
    return candidate if COMMIT_RE.fullmatch(candidate) else ""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--commit",
        default="",
        help="Commit to stamp. Defaults to git HEAD. CI passes the checked-out SHA.",
    )
    args = parser.parse_args(argv)

    commit = resolve_commit(args.commit)
    if not commit:
        # Deliberately stdout, not stderr. Going unstamped is a supported
        # outcome, and the Windows build script runs under
        # `$ErrorActionPreference = "Stop"`, where a native command writing to
        # stderr can terminate the whole build.
        print("stamp_build: no commit available; leaving the build unstamped")
        return 0

    OUTPUT.write_text(f"{commit}\n", encoding="utf-8")
    print(f"stamp_build: wrote {OUTPUT.name}={commit}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
