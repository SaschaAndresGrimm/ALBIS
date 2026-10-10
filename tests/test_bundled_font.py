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


# -- The wordmark face -------------------------------------------------------
# Michroma sets the ALBIS wordmark (start screen, Help -> About, the help
# page) and nothing else. `ofl/michroma/Michroma-Regular.ttf` from the Google Fonts repository,
# version 1.100, unmodified.
WORDMARK = ROOT / "frontend" / "vendor" / "Michroma-Regular.ttf"
WORDMARK_OFL = ROOT / "frontend" / "vendor" / "Michroma-OFL.txt"
WORDMARK_SHA256 = "b62301163788bc5b7f8fcac0b74b184e34e1827e577b499ecb724da065098f87"


def test_the_wordmark_font_is_present_and_unmodified() -> None:
    import hashlib

    assert WORDMARK.is_file(), f"{WORDMARK.relative_to(ROOT)} is missing"
    raw = WORDMARK.read_bytes()
    assert raw[:4] == b"\x00\x01\x00\x00", "not a TrueType font"
    assert hashlib.sha256(raw).hexdigest() == WORDMARK_SHA256, (
        "the bundled Michroma is not the released 1.100 file. If this was an "
        "intentional upgrade, update WORDMARK_SHA256 and THIRD_PARTY_LICENSES.md together."
    )


def test_the_wordmark_face_is_used_for_the_wordmark_only() -> None:
    css = STYLE.read_text(encoding="utf-8")
    face = re.search(
        r'@font-face\s*\{[^}]*font-family:\s*"Michroma"[^}]*url\("([^"]+)"\)', css, re.S
    )
    assert face, "no @font-face for Michroma"
    assert (STYLE.parent / face.group(1)).resolve() == WORDMARK.resolve()
    # Through --font-wordmark, and that only on the two wordmarks.
    users = re.findall(r"([^{}]+)\{[^}]*var\(--font-wordmark\)", css)
    assert sorted(sel.strip() for sel in users) == [".about-wordmark", ".splash-title"]


def test_the_help_page_uses_the_shipped_faces_and_michroma_for_its_wordmarks_only() -> None:
    """docs.html has its own styles; it loads the same two files, nothing else."""
    docs = ROOT / "frontend" / "docs.html"
    html = docs.read_text(encoding="utf-8")
    faces = dict(
        re.findall(r'@font-face\s*\{[^}]*font-family:\s*"([^"]+)"[^}]*url\("([^"]+)"\)', html, re.S)
    )
    assert (docs.parent / faces["Inter"]).resolve() == FONT.resolve()
    assert (docs.parent / faces["Michroma"]).resolve() == WORDMARK.resolve()
    assert "fonts.googleapis" not in html and "@import" not in html
    users = re.findall(r"([^{}]+)\{[^}]*var\(--font-wordmark\)", html)
    assert sorted(sel.strip() for sel in users) == [".topbar-brand", ".wordmark"]


def test_the_wordmark_licence_travels_with_the_font() -> None:
    text = WORDMARK_OFL.read_text(encoding="utf-8")
    assert "SIL OPEN FONT LICENSE VERSION 1.1" in text.upper()
    assert "The Michroma Project Authors" in text
    table = LICENSES.read_text(encoding="utf-8")
    assert re.search(r"^\|\s*Michroma\s*\|\s*1\.100\s*\|\s*OFL-1\.1\s*\|", table, re.M)
