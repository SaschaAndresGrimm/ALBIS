"""`ui.hidden_tools`: which Analysis tools a user leaves out of the tab.

Stored as the tools hidden rather than shown, so a tool added in a later
release appears unless someone hid it on purpose. Only known names survive,
so a typo, or a key written by a newer ALBIS, hides nothing.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from backend.config import (
    CONFIG_PATH_ENV_VAR,
    DEFAULT_CONFIG,
    HIDEABLE_TOOLS,
    env_var_name,
    load_config,
    normalize_config,
)


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch: pytest.MonkeyPatch):
    for section, keys in DEFAULT_CONFIG.items():
        for key in keys:
            monkeypatch.delenv(env_var_name(section, key), raising=False)
    monkeypatch.delenv(CONFIG_PATH_ENV_VAR, raising=False)


def _hidden(value) -> list[str]:
    return normalize_config({"ui": {"hidden_tools": value}})["ui"]["hidden_tools"]


def test_nothing_is_hidden_by_default() -> None:
    assert normalize_config({})["ui"]["hidden_tools"] == []


def test_known_tools_are_kept_in_a_stable_order_once_each() -> None:
    assert _hidden(["combine", "Rings", "rings", " peaks "]) == ["rings", "peaks", "combine"]


def test_unknown_names_hide_nothing() -> None:
    assert _hidden(["roi", "rinsg", "fft"]) == []


def test_statistics_and_roi_cannot_be_hidden() -> None:
    assert "roi" not in HIDEABLE_TOOLS


def test_a_string_is_read_as_a_comma_separated_list_and_other_types_refused() -> None:
    assert _hidden("flatfield, inpainting") == ["flatfield", "inpainting"]
    with pytest.raises(ValueError, match="ui.hidden_tools"):
        _hidden(42)


def test_the_environment_takes_a_comma_separated_list(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ALBIS_UI_HIDDEN_TOOLS", "rings, peaks")
    config, _path = load_config()
    assert config["ui"]["hidden_tools"] == ["rings", "peaks"]
