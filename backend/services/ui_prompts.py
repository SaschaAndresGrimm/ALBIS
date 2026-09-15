"""Titles for the native file and folder choosers, in the user's language.

The chooser is drawn by the operating system from a string this process hands
to PowerShell, AppleScript or zenity, so the title could not simply be rendered
in the browser. Every one of them said "Select Auto Load folder" -- one English
literal, shared by five call sites, so picking a log directory announced itself
as an autoload folder.

The interface asks for a *purpose* and a *language*, both of which are checked
against fixed sets, and this reads the text out of the same locale catalogues
the interface itself uses. Nothing a client sends is ever passed to a shell:
the strings that reach it are ALBIS's own, and `PROMPT_KEYS` is the complete
list of what can be asked for. Sending the localized text directly would have
been less code and a command-injection hole in three interpreters.
"""

from __future__ import annotations

import json
from pathlib import Path

# Purpose -> the locale key naming it. A purpose outside this mapping falls
# back to the generic title rather than being trusted.
PROMPT_KEYS: dict[str, str] = {
    "autoload": "picker.folder.autoload",
    "series_output": "picker.folder.series_output",
    "export_output": "picker.folder.export_output",
    "data_root": "picker.folder.data_root",
    "log_dir": "picker.folder.log_dir",
    "image": "picker.file.image",
    "geometry": "picker.file.geometry",
}

DEFAULT_FOLDER_PROMPT_KEY = "picker.folder.default"

# Read straight from the file rather than through a cache: a chooser opens on a
# deliberate click, at most a few times a session, and a stale title after a
# language change would be its own small bug.
_CATALOGUE_SUFFIX = ".json"

# What a dialog title may not contain.
#
# The apostrophe is deliberately allowed: PowerShell single-quoted strings take
# it doubled, which `_powershell_single_quote` does correctly, and forbidding it
# would cost every Romance language its elisions -- "le dossier de sortie de l
# export" instead of "de l'export". Getting thirteen translations slightly wrong
# to avoid escaping one character correctly is the wrong trade.
#
# The double quote and the backslash are excluded even though
# `_applescript_double_quote` now handles both, and `$` and the backtick even
# though a PowerShell single-quoted string treats them literally. None of the
# translations need any of the four, so refusing them keeps the escaping in
# `os_actions` a second line of defence rather than the only one.
_FORBIDDEN_IN_PROMPT = frozenset('"`\\$\r\n\t\x00')


def is_safe_prompt(text: str) -> bool:
    """Whether a title is fit to hand to a dialog."""
    return bool(text) and not any(char in _FORBIDDEN_IN_PROMPT for char in text)


def _read_catalogue(locales_dir: Path, language: str) -> dict[str, object]:
    path = (locales_dir / f"{language}{_CATALOGUE_SUFFIX}").resolve()
    # The language has already been checked against a fixed set, so this cannot
    # be steered -- asserted anyway, because a path built from a parameter is
    # worth confining whatever the caller promised.
    if locales_dir.resolve() not in path.parents:
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def resolve_prompt(
    locales_dir: Path,
    purpose: str | None,
    language: str,
    fallback: str,
) -> str:
    """The dialog title for `purpose`, in `language`, or `fallback`.

    `fallback` is an English literal from the calling route, used when the
    purpose is unknown, the catalogue is missing, or the text in it turns out
    to contain something a dialog title should not.
    """
    key = PROMPT_KEYS.get(str(purpose or "").strip()) or DEFAULT_FOLDER_PROMPT_KEY
    for candidate in (language, "en"):
        if not candidate:
            continue
        text = _read_catalogue(locales_dir, candidate).get(key)
        if isinstance(text, str):
            stripped = text.strip()
            if is_safe_prompt(stripped):
                return stripped
    return fallback
