"""The About dialog's factual claims, checked against the code behind them.

About is where someone looks for what ALBIS reads, who wrote it, what licence
applies and how to cite it. Those are all stated as literal strings in
`frontend/index.html`, none of them near the code that makes them true, so they
go stale silently -- the formats list had already lost MYTHEN, which
`/api/image` has accepted the whole time.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INDEX = ROOT / "frontend" / "index.html"
STREAM_ROUTE = ROOT / "backend" / "routes" / "stream.py"
CITATION = ROOT / "CITATION.cff"
README = ROOT / "README.md"

# How each extension the image route accepts is named for a human. A new
# format arriving in the route without an entry here fails the test below,
# which is the point: it forces a decision about the About dialog.
EXT_DISPLAY = {
    ".h5": "HDF5",
    ".hdf5": "HDF5",
    ".cfg": "MYTHEN",
    ".dat": "MYTHEN",
    ".tif": "TIFF",
    ".tiff": "TIFF",
    ".cbf": "CBF",
    ".cbf.gz": "CBF",
    ".edf": "EDF",
}


def about_fact(key: str) -> str:
    """The <strong> value of the About card whose label uses this i18n key."""
    html = INDEX.read_text(encoding="utf-8")
    pattern = r'data-i18n="' + re.escape(key) + r'"[^>]*>.*?</span>\s*<strong[^>]*>(.*?)</strong>'
    match = re.search(pattern, html, re.S)
    assert match, f"no About card found for {key!r}"
    return re.sub(r"<[^>]+>", "", match.group(1)).strip()


def route_extensions() -> set[str]:
    """Extensions the /api/image handler branches on."""
    src = STREAM_ROUTE.read_text(encoding="utf-8")
    start = src.index('@app.get("/api/image"')
    end = src.index('@app.get("/api/image/header"', start)
    body = src[start:end]
    found = set(re.findall(r'"(\.[a-z0-9.]+)"', body))
    assert found, "no extensions parsed from the /api/image handler"
    return found


def test_every_readable_format_is_named_in_about() -> None:
    exts = route_extensions()
    unknown = sorted(e for e in exts if e not in EXT_DISPLAY)
    assert not unknown, (
        f"/api/image accepts {unknown} but EXT_DISPLAY does not name them. "
        "Add them here and decide whether About should list them."
    )

    listed = about_fact("about.fact.formats")
    expected = {EXT_DISPLAY[e] for e in exts}
    missing = sorted(name for name in expected if name not in listed)
    assert not missing, (
        f"About says {listed!r} but /api/image also reads {missing}. "
        "HDF5 is excluded from /api/image by design (it uses /api/frame) "
        "but is still a format ALBIS opens, so it belongs in the list."
    )


def test_about_does_not_advertise_a_format_that_cannot_be_opened() -> None:
    listed = about_fact("about.fact.formats")
    claimed = {part.strip() for part in listed.split(",") if part.strip()}
    openable = set(EXT_DISPLAY.values())
    assert (
        claimed <= openable
    ), f"About claims {sorted(claimed - openable)}, which ALBIS cannot read"


def test_the_author_affiliation_matches_the_citation_metadata() -> None:
    """A reader who cites ALBIS and a reader who opens About must agree."""
    html = INDEX.read_text(encoding="utf-8")
    match = re.search(r'class="about-fact-sub"[^>]*>(.*?)</em>', html, re.S)
    assert match, "About no longer states an author affiliation"
    shown = match.group(1).strip()

    cff = CITATION.read_text(encoding="utf-8")
    declared = re.search(r"^\s*affiliation:\s*(.+?)\s*$", cff, re.M)
    assert declared, "CITATION.cff declares no affiliation"
    assert shown == declared.group(1).strip(
        '"'
    ), f"About shows {shown!r}, CITATION.cff declares {declared.group(1)!r}"


def test_the_cited_doi_is_the_concept_doi_from_the_readme() -> None:
    """A version DOI here would freeze citations on one release."""
    shown = about_fact("about.fact.citation")
    assert re.fullmatch(r"10\.5281/zenodo\.\d+", shown), f"not a DOI: {shown!r}"
    assert shown in README.read_text(
        encoding="utf-8"
    ), f"About cites {shown}, which the README's DOI badge does not mention"
    assert shown in CITATION.read_text(
        encoding="utf-8"
    ), f"About cites {shown}, which CITATION.cff does not declare"
