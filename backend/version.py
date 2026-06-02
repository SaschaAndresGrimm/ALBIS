"""Version helpers for ALBIS runtime and packaging scripts."""

from __future__ import annotations

import re
import sys
from pathlib import Path

_DEFAULT_VERSION = "0.0.0"
_VERSION_FILE = "VERSION"
_VERSION_RE = re.compile(r"[0-9A-Za-z][0-9A-Za-z.+-]*")


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _version_file_candidates() -> tuple[Path, ...]:
    """Return likely VERSION locations for source and PyInstaller runtimes."""
    candidates: list[Path] = []
    pyinstaller_root = getattr(sys, "_MEIPASS", "")
    if pyinstaller_root:
        candidates.append(Path(pyinstaller_root) / _VERSION_FILE)
    candidates.append(_repo_root() / _VERSION_FILE)
    return tuple(dict.fromkeys(candidates))


def read_version() -> str:
    """Read and validate the repository version token."""
    for version_path in _version_file_candidates():
        try:
            raw = version_path.read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if raw and _VERSION_RE.fullmatch(raw):
            return raw
    return _DEFAULT_VERSION


ALBIS_VERSION = read_version()
