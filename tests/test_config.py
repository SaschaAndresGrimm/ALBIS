from __future__ import annotations

import sys
from pathlib import Path

import pytest

from backend.config import normalize_config, resolve_data_dir, resolve_log_dir


def test_normalize_config_accepts_partial_payload() -> None:
    config = normalize_config({"server": {"port": "8080"}, "logging": {"level": "debug"}})
    assert config["server"]["port"] == 8080
    assert config["logging"]["level"] == "DEBUG"


def test_normalize_config_defaults_to_random_port() -> None:
    config = normalize_config(None)
    assert config["server"]["port"] == 0


def test_normalize_config_defaults_to_english_ui_language() -> None:
    config = normalize_config(None)
    assert config["ui"]["language"] == "en"


def test_normalize_config_defaults_to_startup_update_checks_enabled() -> None:
    config = normalize_config(None)
    assert config["ui"]["auto_check_updates"] is True


def test_normalize_config_accepts_disabled_startup_update_checks() -> None:
    config = normalize_config({"ui": {"auto_check_updates": False}})
    assert config["ui"]["auto_check_updates"] is False


def test_normalize_config_normalizes_ui_language_aliases() -> None:
    config = normalize_config({"ui": {"language": "zh"}})
    assert config["ui"]["language"] == "zh-CN"
    config = normalize_config({"ui": {"language": "ja-JP"}})
    assert config["ui"]["language"] == "ja"
    config = normalize_config({"ui": {"language": "fr-FR"}})
    assert config["ui"]["language"] == "fr"
    config = normalize_config({"ui": {"language": "es-ES"}})
    assert config["ui"]["language"] == "es"
    config = normalize_config({"ui": {"language": "it-IT"}})
    assert config["ui"]["language"] == "it"
    config = normalize_config({"ui": {"language": "pt-BR"}})
    assert config["ui"]["language"] == "pt"
    config = normalize_config({"ui": {"language": "rm-CH"}})
    assert config["ui"]["language"] == "rm"
    config = normalize_config({"ui": {"language": "de-DE"}})
    assert config["ui"]["language"] == "de"
    config = normalize_config({"ui": {"language": "sv-SE"}})
    assert config["ui"]["language"] == "sv"
    config = normalize_config({"ui": {"language": "da-DK"}})
    assert config["ui"]["language"] == "da"
    config = normalize_config({"ui": {"language": "mi-NZ"}})
    assert config["ui"]["language"] == "mi"
    config = normalize_config({"ui": {"language": "gsw-CH"}})
    assert config["ui"]["language"] == "gsw"


def test_normalize_config_falls_back_to_english_for_invalid_language() -> None:
    config = normalize_config({"ui": {"language": "xx"}})
    assert config["ui"]["language"] == "en"


def test_normalize_config_rejects_unknown_section() -> None:
    with pytest.raises(ValueError, match="Unknown config section"):
        normalize_config({"network": {"port": 8080}})


def test_normalize_config_rejects_unknown_section_key() -> None:
    with pytest.raises(ValueError, match="Unknown key\\(s\\) in section 'server'"):
        normalize_config({"server": {"bind": "127.0.0.1"}})


def test_normalize_config_rejects_non_object_section() -> None:
    with pytest.raises(ValueError, match="must be an object"):
        normalize_config({"server": "127.0.0.1"})


def test_normalize_config_rejects_invalid_value_type() -> None:
    with pytest.raises(ValueError, match="Invalid type for 'server.port'"):
        normalize_config({"server": {"port": [8080]}})


def test_normalize_config_accepts_legacy_launcher_port() -> None:
    config = normalize_config({"launcher": {"port": "8081"}})
    assert config["server"]["port"] == 8081
    assert "port" not in config["launcher"]


def test_normalize_config_prefers_server_port_over_legacy_launcher_port() -> None:
    config = normalize_config({"server": {"port": 8080}, "launcher": {"port": 9090}})
    assert config["server"]["port"] == 8080


def test_resolve_log_dir_uses_relative_logging_dir(tmp_path: Path) -> None:
    config = normalize_config({"logging": {"dir": "./custom-logs"}})
    config_path = tmp_path / "albis.config.json"
    assert resolve_log_dir(config, config_path) == (tmp_path / "custom-logs").resolve()


def test_resolve_log_dir_uses_absolute_logging_dir(tmp_path: Path) -> None:
    absolute_log_dir = (tmp_path / "abs-logs").resolve()
    config = normalize_config({"logging": {"dir": str(absolute_log_dir)}})
    config_path = tmp_path / "albis.config.json"
    assert resolve_log_dir(config, config_path) == absolute_log_dir


def test_resolve_log_dir_defaults_to_user_config_logs_for_frozen(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    home.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    config = normalize_config({"logging": {"dir": ""}})
    config_path = tmp_path / "albis.config.json"
    assert resolve_log_dir(config, config_path) == (home / ".config" / "albis" / "logs").resolve()


def test_resolve_log_dir_defaults_to_data_root_for_source(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delattr(sys, "frozen", raising=False)
    config = normalize_config({"data": {"root": "./data-root"}, "logging": {"dir": ""}})
    config_path = tmp_path / "albis.config.json"
    assert resolve_data_dir(config, config_path) == (tmp_path / "data-root").resolve()
    assert resolve_log_dir(config, config_path) == (tmp_path / "data-root" / "logs").resolve()
