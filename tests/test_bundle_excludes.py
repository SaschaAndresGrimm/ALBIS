"""Packages ALBIS.spec keeps out of the bundle still leave every format readable."""

from __future__ import annotations

import importlib
import re
import sys
import xml.etree.ElementTree
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _excludes() -> list[str]:
    spec = (ROOT / "ALBIS.spec").read_text(encoding="utf-8")
    # Up to the list's own closing bracket: a comment inside may hold one.
    block = re.search(r"excludes=\[(.*?)\n\s*\],", spec, re.DOTALL).group(1)
    lines = (line.split("#", 1)[0] for line in block.splitlines())
    return re.findall(r'"([^"]+)"', "\n".join(lines))


def test_lxml_is_kept_out_of_the_bundle() -> None:
    assert "lxml" in _excludes()


def test_fabio_reads_xsd_without_lxml(monkeypatch) -> None:
    # What the bundle sees: lxml cannot be imported.
    monkeypatch.setitem(sys.modules, "lxml", None)
    monkeypatch.setitem(sys.modules, "lxml.etree", None)
    import fabio.xsdimage

    module = importlib.reload(fabio.xsdimage)
    try:
        assert module.etree is xml.etree.ElementTree
    finally:
        monkeypatch.undo()
        importlib.reload(fabio.xsdimage)


# Left out of the bundle to cut what ships (and its CVEs): see the comments in
# ALBIS.spec. Each one must stay something ALBIS can run without.
UNUSED = [
    "sqlite3",
    "_sqlite3",
    "watchfiles",
    "dotenv",
    "yaml",
    "setuptools",
    "pkg_resources",
    "packaging",
]

_RUN_WITHOUT = """
import sys
for name in {blocked!r}:
    sys.modules[name] = None  # import raises ImportError, as in the bundle

import backend.app
import fabio.openimage
import filelock
import uvicorn

assert filelock.FileLock is not None
config = uvicorn.Config(backend.app.app, log_config={{"version": 1}}, ws_ping_interval=None)
config.load()
print("ok")
"""


def test_unused_modules_are_kept_out_of_the_bundle() -> None:
    assert set(UNUSED) <= set(_excludes())


def test_albis_starts_its_server_without_the_excluded_modules() -> None:
    import subprocess

    result = subprocess.run(
        [sys.executable, "-c", _RUN_WITHOUT.format(blocked=UNUSED + ["lxml", "lxml.etree"])],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    assert result.stdout.strip().endswith("ok")
