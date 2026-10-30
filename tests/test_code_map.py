"""Every source module has a line in docs/CODE_MAP.md.

AGENTS.md sends contributors and coding agents to the code map to find which
file owns what. By 0.38.1 it named about 30 of the 124 frontend modules and
none of the update, logging or middleware modules in the backend -- `http.js`,
the module every backend call is meant to go through, was among the missing.
A reader who trusts the map concludes the module does not exist and writes a
second one. Nothing failed when a module was added without a line, so the map
fell behind one feature at a time.

The check is presence only: the description is for a person to keep honest.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
CODE_MAP = (ROOT / "docs" / "CODE_MAP.md").read_text(encoding="utf-8")
# Everything in backticks. Frontend entries may name the second and later
# modules of a group by file name alone; backend entries use the full path,
# because routes/ and services/ share file names (data_export.py).
MENTIONED = set(re.findall(r"`([^`\s]+)`", CODE_MAP))
MENTIONED_NAMES = {Path(token).name for token in MENTIONED}


def _frontend_modules() -> list[str]:
    return sorted(path.name for path in (ROOT / "frontend" / "modules").glob("*.js"))


def _backend_modules() -> list[str]:
    paths = [
        *(ROOT / "backend").glob("*.py"),
        *(ROOT / "backend" / "routes").glob("*.py"),
        *(ROOT / "backend" / "services").glob("*.py"),
    ]
    return sorted(path.relative_to(ROOT).as_posix() for path in paths if path.name != "__init__.py")


@pytest.mark.parametrize("name", _frontend_modules())
def test_every_frontend_module_is_in_the_code_map(name: str) -> None:
    assert name in MENTIONED_NAMES, (
        f"frontend/modules/{name} is not in docs/CODE_MAP.md. Add it to the "
        "group it belongs to under Frontend Module Map, with what it owns."
    )


@pytest.mark.parametrize("path", _backend_modules())
def test_every_backend_module_is_in_the_code_map(path: str) -> None:
    assert path in MENTIONED, (
        f"{path} is not in docs/CODE_MAP.md. Add it, by its full path, under "
        "the backend section it belongs to, with what it owns."
    )
