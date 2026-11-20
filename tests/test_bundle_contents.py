"""What ALBIS.spec bundles from frontend/ and albis_assets/.

The spec leaves out of the packaged program what it never serves (the test
suite, the README's screenshot, build-only icons). These tests fail if that
cut ever takes a file the page asks for: an image, font or script missing
from the bundle shows only in the installed app, as a broken picture or a
fallback font, never in a run from the checkout.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FRONTEND = ROOT / "frontend"

_spec = importlib.util.spec_from_file_location(
    "bundle_contents", ROOT / "scripts" / "bundle_contents.py"
)
bundle_contents = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bundle_contents)

# `resources/x`, `../resources/x`, `assets/x`, `vendor/x` in HTML, CSS and JS.
_REF = re.compile(r"""(?:\.\./)?\b(resources|assets|vendor)/([A-Za-z0-9_@.\-]+)""")


def _kept() -> set[str]:
    return set(bundle_contents.frontend_files(str(FRONTEND)))


def _page_sources(kept: set[str]) -> list[str]:
    return [rel for rel in kept if rel.endswith((".html", ".css", ".js"))]


def test_every_file_the_page_refers_to_is_bundled():
    kept = _kept()
    missing = []
    for rel in _page_sources(kept):
        text = (FRONTEND / rel).read_text(encoding="utf-8")
        for folder, name in _REF.findall(text):
            if folder == "assets":
                if name not in bundle_contents.ALBIS_ASSETS:
                    missing.append(f"{rel}: assets/{name}")
            elif f"{folder}/{name}" not in kept:
                missing.append(f"{rel}: {folder}/{name}")
    assert not missing, f"Referenced but not bundled: {missing}"


def test_bundled_assets_exist():
    absent = [
        name for name in bundle_contents.ALBIS_ASSETS if not (ROOT / "albis_assets" / name).exists()
    ]
    assert not absent


def test_the_page_itself_is_bundled():
    kept = _kept()
    for rel in ("index.html", "docs.html", "app.js", "style.css", "locales/en.json"):
        assert rel in kept
    assert any(rel.startswith("modules/") for rel in kept)


def test_tests_and_notes_stay_out():
    kept = _kept()
    assert not [rel for rel in kept if rel.startswith(("tests/", "node_modules/"))]
    assert "AGENTS.md" not in kept
    assert "resources/albis.png" not in kept


def test_datas_keep_the_folder_layout():
    datas = {
        Path(src).relative_to(FRONTEND).as_posix(): dest
        for src, dest in bundle_contents.frontend_datas(str(FRONTEND))
    }
    assert datas["index.html"] == "frontend"
    assert Path(datas["resources/splash.webp"]).as_posix() == "frontend/resources"
