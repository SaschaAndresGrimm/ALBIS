from __future__ import annotations

"""Version helpers for ALBIS runtime and packaging scripts."""

import re
from pathlib import Path

_DEFAULT_VERSION = "0.0.0"
_VERSION_FILE = "VERSION"
_VERSION_RE = re.compile(r"[0-9A-Za-z][0-9A-Za-z.+-]*")


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def read_version() -> str:
    """Read and validate the repository version token."""
    version_path = _repo_root() / _VERSION_FILE
    try:
        raw = version_path.read_text(encoding="utf-8").strip()
    except OSError:
        return _DEFAULT_VERSION
    if not raw:
        return _DEFAULT_VERSION
    if not _VERSION_RE.fullmatch(raw):
        return _DEFAULT_VERSION
    return raw


ALBIS_VERSION = read_version()
