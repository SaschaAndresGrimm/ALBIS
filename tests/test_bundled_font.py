"""The interface font is a shipped binary, so treat it like one.

`frontend/vendor/InterVariable.woff2` is the only binary asset the frontend
loads, it is referenced from exactly one `@font-face` in `style.css`, and it is
carried into every build by the blanket `("frontend", "frontend")` rule in the
PyInstaller spec. Each of those is a link that can break without anything else
failing: a moved file, a renamed rule, a truncated checkout. The interface
would still render -- in the fallback face -- and no other test would notice.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FONT = ROOT / "frontend" / "vendor" / "InterVariable.woff2"
OFL = ROOT / "frontend" / "vendor" / "InterVariable-OFL.txt"
STYLE = ROOT / "frontend" / "style.css"
LICENSES = ROOT / "THIRD_PARTY_LICENSES.md"

# web/InterVariable.woff2 from the upstream Inter-4.1.zip release, unmodified.
EXPECTED_SHA256 = "693b77d4f32ee9b8bfc995589b5fad5e99adf2832738661f5402f9978429a8e3"


def test_the_font_is_present_and_is_the_upstream_file() -> None:
    import hashlib

    assert FONT.is_file(), f"{FONT.relative_to(ROOT)} is missing"
    raw = FONT.read_bytes()
    # A woff2 begins with the signature 'wOF2'; a git-lfs pointer or an HTML
    # error page saved by mistake would not.
    assert raw[:4] == b"wOF2", "not a woff2 container"
    digest = hashlib.sha256(raw).hexdigest()
    assert digest == EXPECTED_SHA256, (
        "the bundled font is not the released Inter 4.1 web build. If this was "
        "an intentional upgrade, update EXPECTED_SHA256 and the version in "
        "THIRD_PARTY_LICENSES.md together."
    )


def test_the_stylesheet_points_at_the_file_that_exists() -> None:
    css = STYLE.read_text(encoding="utf-8")
    src = re.search(r'@font-face\s*\{[^}]*url\("([^"]+)"\)', css, re.S)
    assert src, "no @font-face with a url() in style.css"
    # style.css sits in frontend/, so the url is relative to that.
    referenced = (STYLE.parent / src.group(1)).resolve()
    assert referenced == FONT.resolve(), f"@font-face points at {src.group(1)}"

    # And the family it defines has to be the one --font-ui asks for first,
    # or every element quietly falls through to the next entry in the stack.
    face = re.search(r'@font-face\s*\{[^}]*font-family:\s*"([^"]+)"', css, re.S)
    assert face, "@font-face declares no font-family"
    ui = re.search(r"--font-ui:\s*([^;]+);", css)
    assert ui, "--font-ui is not defined"
    assert ui.group(1).strip().startswith(f'"{face.group(1)}"')


def test_the_licence_travels_with_the_font() -> None:
    assert OFL.is_file(), "the OFL text is not shipped beside the font"
    text = OFL.read_text(encoding="utf-8")
    assert "SIL OPEN FONT LICENSE VERSION 1.1" in text.upper()
    assert "The Inter Project Authors" in text

    table = LICENSES.read_text(encoding="utf-8")
    assert re.search(
        r"^\|\s*Inter\s*\|\s*4\.1\s*\|\s*OFL-1\.1\s*\|", table, re.M
    ), "THIRD_PARTY_LICENSES.md has no row for the bundled font"


def test_the_build_carries_the_frontend_directory() -> None:
    """The font ships only because the whole frontend tree is a data dir."""
    spec = next(ROOT.glob("*.spec"), None)
    assert spec is not None, "no PyInstaller spec found"
    assert '("frontend", "frontend")' in spec.read_text(
        encoding="utf-8"
    ), "the spec no longer bundles frontend/ wholesale; the font needs its own rule"
