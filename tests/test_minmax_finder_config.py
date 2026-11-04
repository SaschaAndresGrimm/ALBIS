"""`ui.minmax_finder`: the beta switch for Analysis -> Min/Max Finder.

The tool runs in the browser and needs no endpoint; the backend only keeps the
switch, off unless someone turns it on.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from backend.config import (
    CONFIG_PATH_ENV_VAR,
    DEFAULT_CONFIG,
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


def test_the_setting_is_off_unless_switched_on() -> None:
    assert DEFAULT_CONFIG["ui"]["minmax_finder"] is False
    assert normalize_config({})["ui"]["minmax_finder"] is False
    assert normalize_config({"ui": {"minmax_finder": "true"}})["ui"]["minmax_finder"] is True
    assert normalize_config({"ui": {"minmax_finder": 0}})["ui"]["minmax_finder"] is False


def test_the_environment_can_switch_it_on(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ALBIS_UI_MINMAX_FINDER", "1")
    config, _path = load_config()
    assert config["ui"]["minmax_finder"] is True
