"""Cover the licence-table version sync, which edits a file nothing else regenerates.

The Version column is derived from `backend/requirements.txt` and was kept by
hand, so every dependency bump landed with `test_shipped_dependency_parity` red
and needed a manual edit. `scripts/sync_licence_table.py` does that edit; these
tests are what stop it from touching anything else.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from scripts.sync_licence_table import normalize, read_pins, sync

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "sync_licence_table.py"

TABLE = """## Summary

| Component | Version | License | Copyright |
|---|---|---|---|
| NumPy | 2.2.6 | BSD-3-Clause | © 2005-2023 NumPy Developers |
| tifffile | 2026.8.16 | BSD-3-Clause | © 2008-2025 Christoph Gohlke |
| hdf5plugin | 7.0.0 | MIT (+ bundled filter plugins, see below) | © ESRF |
| pyobjc (macOS only) | 12.2.2 | MIT | © 2002-2025 Ronald Oussoren et al. |
| html2canvas | 1.4.1 | MIT | © 2022 Niklas von Hertzen |

Prose below the table mentions 2026.8.16 and must not be rewritten.
"""

PINS = {
    "numpy": "2.5.2",
    "tifffile": "2026.8.23",
    "hdf5plugin": "7.0.0",
    "pyobjc": "12.2.2",
}


def test_it_moves_only_the_versions_that_differ() -> None:
    updated, changes = sync(TABLE, PINS)

    assert changes == ["NumPy: 2.2.6 -> 2.5.2", "tifffile: 2026.8.16 -> 2026.8.23"]
    assert "| NumPy | 2.5.2 |" in updated
    assert "| tifffile | 2026.8.23 |" in updated


def test_it_leaves_components_that_are_not_python_pins_alone() -> None:
    """html2canvas is vendored into the frontend; no requirements line describes it."""
    updated, _ = sync(TABLE, PINS)

    assert "| html2canvas | 1.4.1 |" in updated


def test_it_rewrites_no_prose() -> None:
    """A blunt search-and-replace of a version string would corrupt the notice."""
    updated, _ = sync(TABLE, PINS)

    assert "Prose below the table mentions 2026.8.16 and must not be rewritten." in updated


def test_it_preserves_the_curated_licence_and_copyright_columns() -> None:
    updated, _ = sync(TABLE, PINS)

    assert "| MIT (+ bundled filter plugins, see below) | © ESRF |" in updated
    assert "© 2005-2023 NumPy Developers" in updated


def test_it_matches_a_display_name_against_its_pin() -> None:
    """The table labels rows for people: "NumPy", "pyobjc (macOS only)"."""
    assert normalize("NumPy") == "numpy"
    assert normalize("pyobjc (macOS only)") == "pyobjc"
    assert normalize("python-multipart") == "python-multipart"
    assert normalize("dectris_compression") == "dectris-compression"


def test_it_is_idempotent() -> None:
    once, _ = sync(TABLE, PINS)
    twice, changes = sync(once, PINS)

    assert changes == []
    assert twice == once


def test_it_reads_extras_and_environment_markers_off_a_pin() -> None:
    pins = read_pins(
        "\n".join(
            [
                "# a comment",
                "uvicorn[standard]==0.52.4",
                'pyobjc==12.2.2; sys_platform == "darwin"',
                "",
            ]
        )
    )

    assert pins == {"uvicorn": "0.52.4", "pyobjc": "12.2.2"}


def test_the_checked_in_table_is_in_sync() -> None:
    """--check is what a human or a workflow runs; it must agree with the tests above."""
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--check"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr
