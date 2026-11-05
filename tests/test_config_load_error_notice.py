"""A config file ALBIS could not read is reported to the interface, not only logged.

ALBIS starts on its defaults when the config will not parse -- a truncated
save, a trailing comma typed by hand, a key from a newer version -- and said so
only in the backend log. In the interface that looks exactly like settings it
forgot: a beamline scientist edited `data.root`, saw no effect, and had no way
to tell whether the file was read at all.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from backend import config


def _settings() -> dict:
    from backend.app import app

    response = TestClient(app, base_url="http://localhost").get("/api/settings")
    assert response.status_code == 200
    return response.json()


def test_a_config_that_loaded_reports_no_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "CONFIG_LOAD_ERROR", "")
    assert _settings()["load_error"] == ""


def test_an_unreadable_config_is_reported_with_its_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    reason = "/etc/albis/config.json: Expecting ',' delimiter: line 4 column 3 (char 61)"
    monkeypatch.setattr(config, "CONFIG_LOAD_ERROR", reason)
    assert _settings()["load_error"] == reason


def test_a_broken_file_sets_the_reason_and_names_the_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The way Mark's would have been written: a trailing comma before the brace."""
    broken = tmp_path / "config.json"
    broken.write_text('{"data": {"root": "/dls/i19-2/data", "allow_abs_paths": true,}}\n')
    monkeypatch.setenv("ALBIS_CONFIG", str(broken))
    monkeypatch.setattr(config, "CONFIG_LOAD_ERROR", "")

    loaded, path = config._load_config_from_disk()

    assert path == broken
    assert loaded["data"]["root"] == "", "defaults are what runs"
    assert config.config_load_error().startswith(str(broken))


def test_saving_over_the_file_clears_the_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Settings saves a file ALBIS wrote itself, so the warning would be stale."""
    monkeypatch.setattr(config, "CONFIG_LOAD_ERROR", f"{tmp_path}/config.json: broken")

    config.save_config(config.normalize_config(None), tmp_path / "config.json")

    assert config.config_load_error() == ""


def test_a_failed_save_keeps_the_reason(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "CONFIG_LOAD_ERROR", "config.json: broken")
    target = tmp_path / "config.json"
    target.mkdir()  # a directory where the file should be: os.replace fails

    with pytest.raises(OSError):
        config.save_config(config.normalize_config(None), target)

    assert config.config_load_error() == "config.json: broken"
