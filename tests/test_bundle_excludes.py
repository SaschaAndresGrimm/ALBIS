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
    block = re.search(r"excludes=\[(.*?)\]", spec, re.DOTALL).group(1)
    return re.findall(r'"([^"]+)"', block)


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
