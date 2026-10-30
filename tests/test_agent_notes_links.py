"""The notes for coding agents must point at files that exist.

An agent follows a link in AGENTS.md as an instruction: "read this before
working here". A link left behind by a rename sends it nowhere, and it carries
on without the rules it was meant to read -- quietly, since nothing fails. The
recipes in `.claude/skills/` are reached only through these links, so they are
the first to go missing when a skill is renamed.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
AGENT_NOTES = sorted(
    path for path in [REPO_ROOT / "AGENTS.md", *REPO_ROOT.glob("*/AGENTS.md")] if path.is_file()
)
SKILLS = sorted((REPO_ROOT / ".claude" / "skills").glob("*/SKILL.md"))
# [text](target) with a relative target; external URLs and pure anchors are
# not files.
LINK = re.compile(r"\]\(([^)#\s]+)(?:#[^)]*)?\)")


def _relative_links(markdown: Path) -> list[str]:
    targets = LINK.findall(markdown.read_text(encoding="utf-8"))
    return [
        target for target in targets if "://" not in target and not target.startswith("mailto:")
    ]


@pytest.mark.parametrize("notes", AGENT_NOTES, ids=lambda path: str(path.relative_to(REPO_ROOT)))
def test_every_link_in_the_agent_notes_resolves(notes: Path) -> None:
    missing = [target for target in _relative_links(notes) if not (notes.parent / target).exists()]

    assert (
        not missing
    ), f"{notes.relative_to(REPO_ROOT)} links to files that do not exist: {missing}"


@pytest.mark.parametrize("skill", SKILLS, ids=lambda path: path.parent.name)
def test_every_shared_skill_is_listed_in_agents_md(skill: Path) -> None:
    """A skill only Claude Code would find is lost to every other agent."""
    agents_md = (REPO_ROOT / "AGENTS.md").read_text(encoding="utf-8")

    assert f".claude/skills/{skill.parent.name}/" in agents_md, (
        f"Skill {skill.parent.name!r} is not mentioned in AGENTS.md; "
        "add it under Recipes so agents other than Claude Code can find it."
    )


@pytest.mark.parametrize("skill", SKILLS, ids=lambda path: path.parent.name)
def test_every_shared_skill_names_itself_as_its_folder(skill: Path) -> None:
    """Claude Code invokes a skill by its `name`; the links use the folder."""
    front_matter = skill.read_text(encoding="utf-8").split("---")[1]
    name = re.search(r"^name:\s*(\S+)\s*$", front_matter, re.MULTILINE)

    assert name and name.group(1) == skill.parent.name
    assert re.search(r"^description:\s*\S", front_matter, re.MULTILINE)
