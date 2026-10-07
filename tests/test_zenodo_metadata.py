"""Zenodo's metadata, held to CITATION.cff.

Each GitHub release is archived on Zenodo, which reads `.zenodo.json` when the
repository has one and then ignores CITATION.cff entirely. `.zenodo.json` is
here for what CITATION.cff cannot say -- links to the installers and the
documentation -- so everything the two share must stay identical, or the DOI
record and GitHub's "Cite this repository" box drift apart.

A value Zenodo does not know makes the next release fail to archive, so link
relations and resource types are limited to ones checked against Zenodo's
vocabulary (https://zenodo.org/api/vocabularies/relationtypes and
.../resourcetypes, all answering 200 when added).
"""

from __future__ import annotations

import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
ZENODO = json.loads((ROOT / ".zenodo.json").read_text(encoding="utf-8"))
CITATION = yaml.safe_load((ROOT / "CITATION.cff").read_text(encoding="utf-8"))

KNOWN_RELATIONS = {"isSourceOf", "isDocumentedBy", "isSupplementTo"}
KNOWN_RESOURCE_TYPES = {"software", "publication-softwaredocumentation"}
REPO_BLOB = "https://github.com/SaschaAndresGrimm/ALBIS/blob/main/"


def test_title_and_description_are_the_citation_s() -> None:
    assert ZENODO["title"] == CITATION["title"]
    assert ZENODO["description"] == " ".join(CITATION["abstract"].split())


def test_creators_are_the_citation_authors() -> None:
    expected = []
    for author in CITATION["authors"]:
        creator = {
            "name": f'{author["family-names"]}, {author["given-names"]}',
            "affiliation": author["affiliation"],
        }
        if author.get("orcid"):
            # CITATION.cff gives the full URL; Zenodo wants the bare iD.
            creator["orcid"] = str(author["orcid"]).rsplit("/", 1)[-1]
        expected.append(creator)
    assert ZENODO["creators"] == expected


def test_keywords_and_licence_match() -> None:
    assert ZENODO["keywords"] == CITATION["keywords"]
    assert ZENODO["license"] == CITATION["license"] == "MIT"
    assert ZENODO["upload_type"] == "software"


def test_no_version_is_pinned() -> None:
    """Zenodo takes the version from the release tag; a fixed one would go stale."""
    assert "version" not in ZENODO


def test_links_use_known_types_and_point_at_real_documents() -> None:
    links = ZENODO["related_identifiers"]
    assert links, "the record would lose its links to the installers and docs"
    for link in links:
        assert link["relation"] in KNOWN_RELATIONS, link
        assert link["resource_type"] in KNOWN_RESOURCE_TYPES, link
        identifier = link["identifier"]
        assert identifier.startswith("https://github.com/SaschaAndresGrimm/ALBIS/"), link
        if identifier.startswith(REPO_BLOB):
            assert (
                ROOT / identifier[len(REPO_BLOB) :]
            ).is_file(), f"{identifier} names no file in the repo"
