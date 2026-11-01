"""No Python module grows past what it is.

backend/AGENTS.md says not to take app.py's size as licence, and the files
past a thousand lines kept growing anyway: image_formats.py reached 2,071
lines and services/data_export.py 1,909 by 0.38.1. A module that size cannot
be read whole, by a person or by a coding agent with a context window, so a
change lands wherever the reader happened to look and the file grows again.

A new module stays under MAX_LINES; one already over it may not get longer.
When you shorten one, lower its baseline in the same change, so the gain
stays. A baseline may go up only when nothing smaller will do, with the reason
in the pull request. frontend/tests/code_size_guard.test.js does the same for
the frontend.
"""

from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
MAX_LINES = 1000

# Modules over MAX_LINES, at the length they may not exceed.
LINE_BASELINE = {
    "albis_launcher.py": 1097,
    "backend/app.py": 1107,
    "backend/services/hdf5_stack.py": 1230,
    "backend/services/series_summing.py": 1529,
}


def _modules() -> list[str]:
    paths = [
        ROOT / "albis_launcher.py",
        *(ROOT / "backend").glob("*.py"),
        *(ROOT / "backend" / "routes").glob("*.py"),
        *(ROOT / "backend" / "services").glob("*.py"),
    ]
    return sorted(path.relative_to(ROOT).as_posix() for path in paths)


def _line_count(path: str) -> int:
    return len((ROOT / path).read_text(encoding="utf-8").splitlines())


@pytest.mark.parametrize("path", _modules())
def test_module_stays_within_its_line_budget(path: str) -> None:
    lines = _line_count(path)
    budget = LINE_BASELINE.get(path, MAX_LINES)
    if path in LINE_BASELINE:
        hint = "Move the new code into a service of its own and call it from here."
    else:
        hint = "Split it by what each part owns (see backend/AGENTS.md)."
    assert lines <= budget, f"{path} has {lines} lines, budget {budget}. {hint}"


def test_baselines_are_current() -> None:
    """A file that shrank keeps its baseline otherwise, and can grow back."""
    stale = {
        path: _line_count(path)
        for path, budget in LINE_BASELINE.items()
        if _line_count(path) < budget
    }
    assert (
        not stale
    ), f"lower LINE_BASELINE to these (or drop the entry if under {MAX_LINES}): {stale}"
