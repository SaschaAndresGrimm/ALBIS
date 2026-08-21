"""Cover configuration coming from the environment and the command line.

Configuration used to come only from a JSON file in one of four locations. For
the published container images that is real friction: the conventional
`docker run -e ALBIS_DATA_ROOT=/data` did nothing, so every deployment had to
bake or mount a file to change one value.

The precedence is command line, then environment, then file, then defaults. The
command line does not have its own mechanism -- each flag sets the environment
variable for the same key -- so there is one order to explain and one to test.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.config import (
    CONFIG_PATH_ENV_VAR,
    DEFAULT_CONFIG,
    env_config_overrides,
    env_var_name,
    load_config,
)


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch: pytest.MonkeyPatch):
    """Start from an environment that says nothing about configuration."""
    for section, keys in DEFAULT_CONFIG.items():
        for key in keys:
            monkeypatch.delenv(env_var_name(section, key), raising=False)
    monkeypatch.delenv(CONFIG_PATH_ENV_VAR, raising=False)


def _write_config(directory: Path, payload: dict) -> Path:
    path = directory / "albis.config.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


# --------------------------------------------------------------------------
# Naming
# --------------------------------------------------------------------------


def test_variable_names_are_derived_from_the_key() -> None:
    """Derived, not listed, so the two cannot drift apart."""
    assert env_var_name("server", "host") == "ALBIS_SERVER_HOST"
    assert env_var_name("data", "allow_abs_paths") == "ALBIS_DATA_ALLOW_ABS_PATHS"
    assert env_var_name("ui", "language") == "ALBIS_UI_LANGUAGE"


@pytest.mark.parametrize(
    "section,key",
    [(section, key) for section, keys in DEFAULT_CONFIG.items() for key in keys],
)
def test_every_config_key_can_be_set_from_the_environment(
    section: str, key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(env_var_name(section, key), "1")

    overrides, applied = env_config_overrides()

    assert applied == [f"{section}.{key}"]
    assert overrides[section][key] in ("1", ["1"])


# --------------------------------------------------------------------------
# Precedence
# --------------------------------------------------------------------------


def test_the_environment_wins_over_the_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, {"server": {"host": "127.0.0.1", "port": 9000}})
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ALBIS_SERVER_HOST", "0.0.0.0")
    monkeypatch.setenv("ALBIS_SERVER_PORT", "8123")

    config, _path = load_config()

    assert config["server"]["host"] == "0.0.0.0"
    assert config["server"]["port"] == 8123


def test_the_file_still_decides_what_the_environment_does_not_mention(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, {"server": {"port": 9000}, "ui": {"language": "de"}})
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ALBIS_SERVER_PORT", "8123")

    config, _path = load_config()

    assert config["server"]["port"] == 8123
    assert config["ui"]["language"] == "de"


def test_an_empty_value_is_still_an_instruction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`ALBIS_DATA_ROOT=` means "use the default root", not "say nothing"."""
    _write_config(tmp_path, {"data": {"root": "/mnt/beamline"}})
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ALBIS_DATA_ROOT", "")

    config, _path = load_config()

    assert config["data"]["root"] == ""


def test_strings_are_coerced_to_the_type_the_key_needs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The environment only carries strings; the config is typed."""
    _write_config(tmp_path, {"data": {"allow_abs_paths": True}})
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ALBIS_DATA_ALLOW_ABS_PATHS", "false")
    monkeypatch.setenv("ALBIS_DATA_SCAN_CACHE_SEC", "4.5")
    monkeypatch.setenv("ALBIS_DATA_MAX_SCAN_ENTRIES", "1000")

    config, _path = load_config()

    assert config["data"]["allow_abs_paths"] is False
    assert config["data"]["scan_cache_sec"] == 4.5
    assert config["data"]["max_scan_entries"] == 1000


def test_a_list_key_accepts_a_comma_separated_value(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ALBIS_SERVER_ALLOWED_HOSTS", "albis.lab, albis , ")

    config, _path = load_config()

    assert config["server"]["allowed_hosts"] == ["albis.lab", "albis"]


def test_an_unusable_value_falls_back_instead_of_failing_to_start(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Starting on a sane default beats not starting at all."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ALBIS_SERVER_PORT", "not-a-port")

    config, _path = load_config()

    assert config["server"]["port"] == DEFAULT_CONFIG["server"]["port"]


def test_overridden_keys_are_reported_so_the_interface_can_say_so(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Saving the file cannot change these, so the dialog must not pretend it can."""
    from backend.config import env_override_keys

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("ALBIS_DATA_ROOT", "/mnt/beamline")
    monkeypatch.setenv("ALBIS_UI_LANGUAGE", "ja")

    load_config()

    assert set(env_override_keys()) == {"data.root", "ui.language"}


def test_nothing_is_reported_when_the_environment_is_silent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from backend.config import env_override_keys

    monkeypatch.chdir(tmp_path)

    load_config()

    assert env_override_keys() == []


# --------------------------------------------------------------------------
# ALBIS_CONFIG
# --------------------------------------------------------------------------


def test_the_config_file_can_be_named_outright(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    named = tmp_path / "somewhere" / "custom.json"
    named.parent.mkdir()
    named.write_text(json.dumps({"ui": {"language": "sv"}}), encoding="utf-8")
    _write_config(tmp_path, {"ui": {"language": "de"}})
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(CONFIG_PATH_ENV_VAR, str(named))

    config, path = load_config()

    assert path == named
    assert config["ui"]["language"] == "sv"


def test_a_named_config_that_does_not_exist_yet_is_created_there(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Writing the defaults elsewhere would leave the operator editing the wrong file."""
    named = tmp_path / "etc" / "albis.json"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(CONFIG_PATH_ENV_VAR, str(named))

    config, path = load_config()

    assert path == named
    assert named.is_file()
    assert json.loads(named.read_text(encoding="utf-8"))["server"]["host"] == "127.0.0.1"
    assert config["server"]["host"] == "127.0.0.1"


# --------------------------------------------------------------------------
# What the API reports
# --------------------------------------------------------------------------


def test_the_settings_endpoint_reports_environment_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fastapi.testclient import TestClient

    from backend.app import app

    monkeypatch.setattr("backend.app.env_override_keys", lambda: ["data.root"])
    payload = TestClient(app).get("/api/settings").json()

    assert payload["env_overrides"] == ["data.root"]
