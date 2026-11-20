"""Which data files ALBIS.spec puts into the packaged program.

PyInstaller copied the whole `frontend/` folder, so every download carried the
Vitest suite, the agent notes and pictures only the README and the build
scripts use, and a build from a checkout with `npm ci` run would have carried
`node_modules` too. This module names what stays out, and why, in one place
the spec imports and `tests/test_bundle_contents.py` checks: whatever the page
asks for (`resources/...`, `assets/...`) must still be in the bundle.
"""

from __future__ import annotations

import os

# Folders under frontend/ the running program never serves.
FRONTEND_EXCLUDED_DIRS = frozenset(
    {
        "tests",  # the Vitest suite, 1.7 MB
        "node_modules",  # present after `npm ci` in a checkout
        "__pycache__",
    }
)

# Files under frontend/, as paths relative to it, the running program never
# serves.
FRONTEND_EXCLUDED_FILES = frozenset(
    {
        "AGENTS.md",
        "CLAUDE.md",
        ".DS_Store",
        # The README's screenshot (0.5 MB); the README links it on GitHub.
        "resources/albis.png",
        # Fallback icon for the build and install scripts (1 MB); they read it
        # from the checkout, not from the bundle.
        "resources/icon.png",
        "resources/albis_512x512.png",
    }
)

# Files from albis_assets/ served at /assets. The page uses the 32 px icon and
# the DECTRIS logo; the larger icons stay for whoever opens /assets directly,
# but the .icns, .ico and 1024 px icon (0.75 MB) are build inputs only (the
# app and installer icons are made from the checkout), and the two splash
# PNGs (1.3 MB) were not used by anything.
ALBIS_ASSETS = (
    "icon_16x16.png",
    "icon_16x16@2x.png",
    "icon_32x32.png",
    "icon_32x32@2x.png",
    "icon_64x64.png",
    "icon_128x128.png",
    "icon_256x256.png",
    "icon_512x512.png",
    # Shown in Help -> About; without it the installed app shows a broken image.
    "dectris_logo.svg",
)


def _excluded(rel_path: str) -> bool:
    parts = rel_path.split("/")
    if any(part in FRONTEND_EXCLUDED_DIRS for part in parts[:-1]):
        return True
    return rel_path in FRONTEND_EXCLUDED_FILES or parts[-1] in FRONTEND_EXCLUDED_FILES


def frontend_files(frontend_root: str) -> list[str]:
    """Paths relative to `frontend_root`, `/`-separated, that go into the bundle."""
    kept: list[str] = []
    for dirpath, dirnames, filenames in os.walk(frontend_root):
        dirnames[:] = sorted(d for d in dirnames if d not in FRONTEND_EXCLUDED_DIRS)
        for name in sorted(filenames):
            rel = os.path.relpath(os.path.join(dirpath, name), frontend_root)
            rel = rel.replace(os.sep, "/")
            if not _excluded(rel):
                kept.append(rel)
    return kept


def frontend_datas(frontend_root: str) -> list[tuple[str, str]]:
    """PyInstaller `datas` entries: (source file, destination folder)."""
    entries = []
    for rel in frontend_files(frontend_root):
        dest = os.path.dirname(os.path.join("frontend", *rel.split("/")))
        entries.append((os.path.join(frontend_root, *rel.split("/")), dest))
    return entries
