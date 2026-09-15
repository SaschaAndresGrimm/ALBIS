"""The native chooser says what it is choosing, in the user's language.

Every folder chooser in ALBIS was titled "Select Auto Load folder" -- one
English literal in `os_actions`, shared by five call sites, so picking a log
directory, an export folder or the data root all announced themselves as
autoload.

Fixing it means the title has to come from somewhere the interface's
translations live, and be handed to PowerShell, AppleScript or zenity. The
tempting shape -- let the browser send the localized text -- would put
client-controlled strings into three interpreters. So the interface sends a
*purpose* and a *language*, both looked up in fixed sets, and the backend reads
ALBIS's own catalogue. These tests hold that boundary as much as the wording.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from backend.config import UI_LANGUAGES
from backend.services.os_actions import _applescript_double_quote, _powershell_single_quote
from backend.services.ui_prompts import (
    DEFAULT_FOLDER_PROMPT_KEY,
    PROMPT_KEYS,
    is_safe_prompt,
    resolve_prompt,
)

ROOT = Path(__file__).resolve().parents[1]
LOCALES = ROOT / "frontend" / "locales"

ALL_KEYS = sorted({*PROMPT_KEYS.values(), DEFAULT_FOLDER_PROMPT_KEY})


def _catalogue(language: str) -> dict:
    return json.loads((LOCALES / f"{language}.json").read_text(encoding="utf-8"))


# --------------------------------------------------------------------------
# The wording itself
# --------------------------------------------------------------------------


@pytest.mark.parametrize("language", sorted(UI_LANGUAGES))
def test_every_language_titles_every_chooser(language: str) -> None:
    catalogue = _catalogue(language)
    for key in ALL_KEYS:
        assert (
            isinstance(catalogue.get(key), str) and catalogue[key].strip()
        ), f"{language} has no title for {key}"


@pytest.mark.parametrize("language", sorted(UI_LANGUAGES))
def test_the_titles_within_a_language_are_all_different(language: str) -> None:
    """The bug was five call sites sharing one title.

    Distinct text per purpose is the whole point, so a copy-paste that left two
    purposes saying the same thing fails here.
    """
    catalogue = _catalogue(language)
    titles = [catalogue[key] for key in ALL_KEYS]
    assert len(set(titles)) == len(titles), f"{language} reuses a chooser title: {titles}"


@pytest.mark.parametrize("purpose", sorted(PROMPT_KEYS))
def test_each_purpose_resolves_in_each_language(purpose: str) -> None:
    for language in sorted(UI_LANGUAGES):
        resolved = resolve_prompt(LOCALES, purpose, language, "FALLBACK")
        assert resolved != "FALLBACK", f"{purpose} unresolved for {language}"


def test_the_languages_really_differ() -> None:
    """A translation that was never translated would pass everything above."""
    titles = {
        language: resolve_prompt(LOCALES, "log_dir", language, "FALLBACK")
        for language in ("en", "de", "ja", "zh-CN", "fr")
    }
    assert len(set(titles.values())) == len(titles), titles


# --------------------------------------------------------------------------
# The boundary: nothing a client sends reaches a shell
# --------------------------------------------------------------------------


@pytest.mark.parametrize("language", sorted(UI_LANGUAGES))
def test_no_title_needs_escaping_to_be_safe(language: str) -> None:
    """The invariant that keeps the escaping a second line of defence.

    A title ends up inside a PowerShell single-quoted string and an AppleScript
    double-quoted one. Both are escaped, but a translation containing a quote or
    a backslash would mean relying on that escaping alone -- so no translation
    contains one.
    """
    catalogue = _catalogue(language)
    for key in ALL_KEYS:
        assert is_safe_prompt(catalogue[key]), f"{language}/{key} holds a character it should not"


def test_an_apostrophe_is_allowed_because_it_is_escaped_properly() -> None:
    """Romance elisions are worth one correctly escaped character.

    Forbidding `'` would have cost French, Italian and Romansh their elisions
    -- "de l export" rather than "de l'export" -- to avoid doubling a quote
    that PowerShell doubles correctly.
    """
    assert is_safe_prompt("Sélectionner le dossier de sortie de l'export")
    assert _powershell_single_quote("de l'export") == "de l''export"
    assert "l'export" in resolve_prompt(LOCALES, "export_output", "fr", "FALLBACK")


@pytest.mark.parametrize(
    "text",
    [
        'say "hi"',
        "back\\slash",
        "dollar $sign",
        "grave `accent",
        "newline\nhere",
        "carriage\rreturn",
        "tab\there",
        "nul\x00byte",
        "",
    ],
)
def test_unsafe_titles_are_refused(text: str) -> None:
    assert not is_safe_prompt(text)


def test_an_unknown_purpose_falls_back_rather_than_being_trusted() -> None:
    # The purpose is a key into a fixed mapping, never a string that is used.
    for hostile in ("../../etc/passwd", "'; rm -rf /", "log_dir; whoami", "", None):
        resolved = resolve_prompt(LOCALES, hostile, "en", "FALLBACK")
        assert resolved == _catalogue("en")[DEFAULT_FOLDER_PROMPT_KEY]


def test_an_unknown_language_falls_back_to_english() -> None:
    for hostile in ("xx", "../../../etc/hosts", "en/../../secret", "", None):
        resolved = resolve_prompt(LOCALES, "log_dir", hostile, "FALLBACK")
        assert resolved == _catalogue("en")[PROMPT_KEYS["log_dir"]]


def test_a_catalogue_outside_the_locales_directory_is_not_read(tmp_path: Path) -> None:
    """The traversal guard, exercised on a file that really is reachable.

    `resolve_prompt` joins the language into a path. The route normalizes it
    against the supported set first, but a path built from a parameter is worth
    confining on its own.

    The relative path is computed rather than written by hand: an earlier
    version of this test used `../<name>/evil`, which climbed one level out of
    `frontend/locales` and pointed at nothing, so it passed with the guard
    deleted and proved only that a missing file is missing.
    """
    outside = tmp_path / "evil.json"
    outside.write_text(json.dumps({PROMPT_KEYS["log_dir"]: "pwned"}), encoding="utf-8")
    # Relative to the locales directory, so it genuinely escapes it.
    relative = os.path.relpath(tmp_path / "evil", LOCALES)
    assert relative.startswith(".."), relative
    # Confirm the target really is reachable that way, or the test is vacuous.
    assert (LOCALES / f"{relative}.json").resolve().is_file()

    resolved = resolve_prompt(LOCALES, "log_dir", relative, "FALLBACK")

    assert resolved != "pwned"
    assert resolved == _catalogue("en")[PROMPT_KEYS["log_dir"]]


# --------------------------------------------------------------------------
# Escaping, for the day a title does contain something awkward
# --------------------------------------------------------------------------


def test_applescript_escaping_closes_the_backslash_hole() -> None:
    """Escaping the quote alone was not enough.

    `a\\"` became `a\\\\"`, which AppleScript reads as an escaped backslash
    followed by the *closing* quote -- the string ends and the rest is script.
    The backslash has to be escaped first.
    """
    assert _applescript_double_quote('a\\"') == 'a\\\\\\"'
    assert _applescript_double_quote('say "hi"') == 'say \\"hi\\"'
    assert _applescript_double_quote("plain") == "plain"


def test_applescript_escaping_leaves_translations_alone() -> None:
    # Umlauts, macrons and CJK must survive untouched.
    for text in ("Ordner auswählen", "ログフォルダーを選択", "Tīpakohia te kōpaki"):
        assert _applescript_double_quote(text) == text


def test_powershell_escaping_doubles_the_quote() -> None:
    assert _powershell_single_quote("it's") == "it''s"
    assert _powershell_single_quote("'; whoami; '") == "''; whoami; ''"


# --------------------------------------------------------------------------
# The route, which is where the purpose and language actually arrive
# --------------------------------------------------------------------------


def test_the_route_titles_the_chooser_from_the_query(monkeypatch: pytest.MonkeyPatch) -> None:
    """Proves the wiring, not just the resolver.

    The chooser itself is never opened -- on this machine that would block on a
    real AppleScript dialog -- so the platform call is replaced and the title it
    would have been given is captured.
    """
    from fastapi.testclient import TestClient

    from backend.app import app
    from backend.routes import files as files_route

    seen: list[str] = []

    def fake_choose_folder(prompt: str = "") -> str:
        seen.append(prompt)
        return "/tmp/picked"

    monkeypatch.setattr(files_route, "_choose_folder", fake_choose_folder)
    client = TestClient(app)

    response = client.get("/api/choose-folder", params={"purpose": "log_dir", "lang": "de"})

    assert response.status_code == 200
    assert seen == [_catalogue("de")[PROMPT_KEYS["log_dir"]]]


@pytest.mark.parametrize(
    ("purpose", "language"),
    [("autoload", "en"), ("data_root", "ja"), ("export_output", "fr"), ("series_output", "sv")],
)
def test_the_route_honours_every_purpose(
    purpose: str, language: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from fastapi.testclient import TestClient

    from backend.app import app
    from backend.routes import files as files_route

    seen: list[str] = []
    monkeypatch.setattr(
        files_route, "_choose_folder", lambda prompt="": seen.append(prompt) or "/tmp/x"
    )

    TestClient(app).get("/api/choose-folder", params={"purpose": purpose, "lang": language})

    assert seen == [_catalogue(language)[PROMPT_KEYS[purpose]]]


def test_the_route_ignores_a_hostile_language(monkeypatch: pytest.MonkeyPatch) -> None:
    # Normalized against the supported set before it can become a path.
    from fastapi.testclient import TestClient

    from backend.app import app
    from backend.routes import files as files_route

    seen: list[str] = []
    monkeypatch.setattr(
        files_route, "_choose_folder", lambda prompt="": seen.append(prompt) or "/tmp/x"
    )

    TestClient(app).get(
        "/api/choose-folder", params={"purpose": "log_dir", "lang": "../../../etc/hosts"}
    )

    assert seen == [_catalogue("en")[PROMPT_KEYS["log_dir"]]]


def test_the_file_chooser_is_titled_too(monkeypatch: pytest.MonkeyPatch) -> None:
    from fastapi.testclient import TestClient

    from backend.app import app
    from backend.routes import files as files_route

    seen: list[str] = []
    monkeypatch.setattr(
        files_route,
        "_choose_file",
        lambda exts=None, prompt="": seen.append(prompt) or "/tmp/frame.h5",
    )

    TestClient(app).get("/api/choose-file", params={"lang": "de"})

    assert seen == [_catalogue("de")[PROMPT_KEYS["image"]]]
