from __future__ import annotations

import re
from pathlib import Path

from backend import version as version_module

ROOT = Path(__file__).resolve().parents[1]


def test_read_version_prefers_pyinstaller_runtime_version_file(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / "VERSION").write_text("2.3.4\n", encoding="utf-8")

    monkeypatch.setattr(version_module.sys, "_MEIPASS", str(tmp_path), raising=False)

    assert version_module.read_version() == "2.3.4"


def test_read_version_returns_default_when_no_valid_version_file(
    tmp_path: Path, monkeypatch
) -> None:
    missing = tmp_path / "missing"
    invalid = tmp_path / "invalid"
    invalid.write_text("not a version token\nwith newline\n", encoding="utf-8")

    monkeypatch.setattr(
        version_module,
        "_version_file_candidates",
        lambda: (missing / "VERSION", invalid),
    )

    assert version_module.read_version() == "0.0.0"


def test_pyinstaller_spec_bundles_runtime_version_file() -> None:
    spec_text = (ROOT / "ALBIS.spec").read_text(encoding="utf-8")

    assert re.search(r"\(\s*['\"]VERSION['\"]\s*,\s*['\"]\.['\"]\s*\)", spec_text)
