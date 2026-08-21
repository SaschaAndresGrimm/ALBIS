"""Hold the user-facing documentation to what the app actually does.

Documentation drifts silently: nothing fails when a format ships without a
mention, a shortcut is added without a line in the help, or a dialog is
regrouped and the guide keeps the old names. All three had happened here --
MYTHEN acquisitions were supported, tested and undiscoverable, and the settings
tabs were renamed while two documents still described the old sections.

These checks are deliberately narrow. They assert that things the app declares
about itself appear somewhere a user can read, not that the prose is any good.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
README = (ROOT / "README.md").read_text(encoding="utf-8")
HELP = (ROOT / "frontend" / "docs.html").read_text(encoding="utf-8")
GUIDE = (ROOT / "docs" / "POWER_USER_GUIDE.md").read_text(encoding="utf-8")
USER_GUIDE = (ROOT / "docs" / "USER_GUIDE.md").read_text(encoding="utf-8")
INDEX = (ROOT / "frontend" / "index.html").read_text(encoding="utf-8")
USER_FACING = f"{README}\n{HELP}\n{GUIDE}\n{USER_GUIDE}"


def _autoload_extensions() -> set[str]:
    """The file types the backend will actually open."""
    from backend.app import AUTOLOAD_EXTS

    return set(AUTOLOAD_EXTS)


@pytest.mark.parametrize("extension", sorted(_autoload_extensions()))
def test_every_openable_format_is_documented(extension: str) -> None:
    """A format users can open but never read about earns nothing.

    `.cfg` is the case that motivated this: MYTHEN acquisitions had a bespoke
    reader, six tests and no mention in any document a user sees.
    """
    assert extension in USER_FACING, (
        f"{extension} is accepted by the backend but appears in no user-facing "
        "document. Add it to the README format list and the in-app help."
    )


def test_menu_shortcuts_are_all_listed_in_the_in_app_help() -> None:
    """The app advertises these in its own menus, so F1 should agree."""
    advertised = set(re.findall(r'<span class="shortcut">([^<]+)</span>', INDEX))
    assert advertised, "no shortcuts found in index.html; has the markup changed?"

    missing = sorted(shortcut for shortcut in advertised if shortcut not in HELP)
    assert (
        not missing
    ), f"shortcuts shown in the menus but absent from frontend/docs.html: {missing}"


def test_settings_tab_names_match_what_the_documentation_calls_them() -> None:
    """Renaming a tab has twice left the docs describing groups that were gone."""
    slugs = set(re.findall(r'data-settings-tab="(\w+)"', INDEX))
    assert slugs, "no settings tabs found in index.html; has the markup changed?"

    english = json.loads((ROOT / "frontend" / "locales" / "en.json").read_text(encoding="utf-8"))
    for slug in sorted(slugs):
        name = english[f"settings.tab.{slug}"]
        assert name in USER_FACING, (
            f"settings tab {name!r} is not named in any user-facing document. "
            "The in-app help and the Power User Guide both describe the dialog."
        )


def test_documentation_does_not_reference_removed_controls() -> None:
    """Guards against the specific stale references already found once."""
    for stale in ("Save & Close", "Settings -> Application", "data access, and logging"):
        assert (
            stale not in USER_FACING
        ), f"{stale!r} no longer exists in the UI but is still documented."


def test_language_count_claimed_in_the_readme_is_accurate() -> None:
    """It is a selling point in Highlights, so it should not quietly go stale."""
    locales = len(list((ROOT / "frontend" / "locales").glob("*.json")))
    claimed = re.search(r"(\d+)\s+languages", README)

    assert claimed, "README no longer states a language count; update this test if intended"
    assert (
        int(claimed.group(1)) == locales
    ), f"README claims {claimed.group(1)} languages but {locales} locale files exist"


def test_user_guide_is_reachable_from_the_places_users_start() -> None:
    """A guide nobody links to is a guide nobody reads."""
    assert "docs/USER_GUIDE.md" in README, "the README does not link the User Guide"
    assert "USER_GUIDE.md" in HELP, "the in-app help does not link the User Guide"


@pytest.mark.parametrize(
    "anchor",
    sorted(set(re.findall(r"POWER_USER_GUIDE\.md#([\w-]+)", USER_GUIDE))),
)
def test_user_guide_cross_references_resolve(anchor: str) -> None:
    """Cross-document links rot silently when a heading is renamed."""
    headings = {
        re.sub(r"[^a-z0-9 -]", "", heading.lower()).strip().replace(" ", "-")
        for heading in re.findall(r"^#+\s+(.*)$", GUIDE, re.M)
    }
    assert (
        anchor in headings
    ), f"USER_GUIDE.md links to POWER_USER_GUIDE.md#{anchor}, which no heading produces"


def test_every_ui_config_key_is_documented_in_the_power_user_guide() -> None:
    """A configurable knob nobody can read about is not configurable.

    `auto_check_updates` and `language` were both settable, both in the schema,
    and both absent from the Settings Reference -- so the one outbound network
    request ALBIS makes had no documented off switch.
    """
    from backend.config import DEFAULT_CONFIG

    section = re.search(r"^#### `ui`$(.*?)(?=^#{1,4} |\Z)", GUIDE, re.M | re.S)
    assert section, "the Power User Guide no longer has a `ui` settings section"

    documented = set(re.findall(r"^- `(\w+)`", section.group(1), re.M))
    missing = sorted(set(DEFAULT_CONFIG["ui"]) - documented)
    assert (
        not missing
    ), f"ui config keys with no entry in the Power User Guide Settings Reference: {missing}"


def test_network_behaviour_is_disclosed_where_users_and_IT_will_look() -> None:
    """ALBIS contacts GitHub on startup; saying so is not optional.

    A viewer installed on a facility workstation gets approved by someone who
    needs to know what it sends. The disclosure is only useful where they look:
    the README, the in-app help, and the guide that documents the setting.
    """
    privacy = ROOT / "docs" / "NETWORK_AND_PRIVACY.md"
    assert privacy.is_file(), "docs/NETWORK_AND_PRIVACY.md is missing"

    text = privacy.read_text(encoding="utf-8")
    from backend.services.update_check import LATEST_RELEASE_API_URL

    assert LATEST_RELEASE_API_URL in text, (
        "the privacy document must name the exact URL ALBIS requests, so it stays "
        "honest if the endpoint changes"
    )
    assert (
        "auto_check_updates" in text
    ), "the privacy document must name the setting that disables it"

    assert "NETWORK_AND_PRIVACY.md" in README, "the README does not link the privacy document"
    assert "NETWORK_AND_PRIVACY.md" in HELP, "the in-app help does not link the privacy document"
    assert (
        "NETWORK_AND_PRIVACY.md" in GUIDE
    ), "the Power User Guide does not link the privacy document"


def test_albis_is_citable() -> None:
    """Scientific software that cannot be cited does not get credited."""
    assert (ROOT / "CITATION.cff").is_file(), "CITATION.cff is missing"
    assert "CITATION.cff" in README, "the README does not point users at how to cite ALBIS"
