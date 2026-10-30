"""Keep the user guide's pictures and links in step with the guide.

The pictures in docs/images/user-guide/ are made by
scripts/guide_screenshots.mjs, so they can be made again when the interface
changes; a picture taken by hand cannot, and goes quietly out of date. A
picture the guide no longer shows is dead weight in every clone, and one the
guide shows but the folder lacks is a broken image on GitHub.

The in-app help (F1) links to sections of the guide by anchor. Renaming a
heading breaks those links without a sound, which is the other half of keeping
the guide's structure honest.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
USER_GUIDE = (DOCS / "USER_GUIDE.md").read_text(encoding="utf-8")
IMAGES = DOCS / "images" / "user-guide"
SCRIPT = (ROOT / "scripts" / "guide_screenshots.mjs").read_text(encoding="utf-8")
HELP = (ROOT / "frontend" / "docs.html").read_text(encoding="utf-8")

# Markdown images and the <img> tags used for pictures side by side.
_MD_IMAGE = re.compile(r"!\[([^\]]*)\]\((images/user-guide/[^)\s]+)\)")
_HTML_IMAGE = re.compile(r"<img\b[^>]*>")
_ATTR = re.compile(r'(\w+)="([^"]*)"')

# A full window is about 150 KB as WebP; more means a wrong format or size.
MAX_BYTES = 250_000


def _guide_images() -> list[tuple[str, str]]:
    """(path, alt text) for every picture the guide shows."""
    found = [(path, alt) for alt, path in _MD_IMAGE.findall(USER_GUIDE)]
    for tag in _HTML_IMAGE.findall(USER_GUIDE):
        attrs = dict(_ATTR.findall(tag))
        if attrs.get("src", "").startswith("images/user-guide/"):
            found.append((attrs["src"], attrs.get("alt", "")))
    return found


def _github_anchor(heading: str) -> str:
    """The anchor GitHub gives a Markdown heading."""
    text = re.sub(r"[^\w\- ]", "", heading.strip().lower())
    return text.replace(" ", "-")


def test_every_picture_the_guide_shows_exists_and_is_described() -> None:
    """A missing file is a broken image; a missing alt text leaves a reader
    with a screen reader, or a slow connection, with nothing."""
    images = _guide_images()
    assert images, "the guide shows no pictures; has the image syntax changed?"
    for path, alt in images:
        assert (DOCS / path).is_file(), f"{path} is shown in the guide but missing"
        assert alt.strip(), f"{path} has no alt text"


def test_every_picture_in_the_folder_is_shown_by_the_guide() -> None:
    shown = {Path(path).name for path, _ in _guide_images()}
    unused = sorted(p.name for p in IMAGES.iterdir() if p.name not in shown)
    assert not unused, f"pictures no section of the guide shows: {unused}"


def test_every_picture_can_be_made_again() -> None:
    """Each picture is a scene in scripts/guide_screenshots.mjs, so a change to
    the interface is one command away from an up-to-date guide."""
    made = set(re.findall(r'shot\("([a-z0-9-]+)"', SCRIPT))
    orphans = sorted(p.name for p in IMAGES.iterdir() if p.stem not in made)
    assert not orphans, f"pictures no scene in scripts/guide_screenshots.mjs makes: {orphans}"


def test_pictures_stay_small() -> None:
    large = sorted(
        f"{p.name} ({p.stat().st_size // 1000} KB)"
        for p in IMAGES.iterdir()
        if p.stat().st_size > MAX_BYTES
    )
    assert not large, f"pictures over {MAX_BYTES // 1000} KB: {large}"


def test_help_links_land_on_a_section_of_the_guide() -> None:
    """F1's "More in the User Guide" links name sections by anchor."""
    linked = set(re.findall(r"docs/USER_GUIDE\.md#([a-z0-9-]+)", HELP))
    assert linked, "no links from frontend/docs.html into the guide; has the markup changed?"
    headings = re.findall(r"^#{2,4} (.+)$", USER_GUIDE, flags=re.MULTILINE)
    anchors = {_github_anchor(h) for h in headings}
    missing = sorted(linked - anchors)
    assert not missing, f"frontend/docs.html links to sections the guide no longer has: {missing}"
