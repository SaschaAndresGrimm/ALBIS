from __future__ import annotations

import pytest

from backend.config import normalize_config


def test_normalize_config_accepts_partial_payload() -> None:
    config = normalize_config({"server": {"port": "8080"}, "logging": {"level": "debug"}})
    assert config["server"]["port"] == 8080
    assert config["logging"]["level"] == "DEBUG"


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
