"""Cover the launcher's command line.

The launcher parsed no arguments at all, so a lab deployment could not say
`albis --port 9000 --data-root /gpfs/beamline` and had to edit a JSON file
instead. Each flag now sets the environment variable for the same config key, so
the precedence order has one implementation rather than two.

The awkward part is not the flags -- it is that a desktop build is started by
the operating system, which passes arguments nobody typed.
"""

from __future__ import annotations

import pytest

from albis_launcher import _apply_cli_arguments, _build_arg_parser
from backend.config import CONFIG_PATH_ENV_VAR, DEFAULT_CONFIG, env_var_name


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch: pytest.MonkeyPatch):
    for section, keys in DEFAULT_CONFIG.items():
        for key in keys:
            monkeypatch.delenv(env_var_name(section, key), raising=False)
    monkeypatch.delenv(CONFIG_PATH_ENV_VAR, raising=False)


def test_flags_set_the_environment_for_the_same_config_key() -> None:
    applied, ignored = _apply_cli_arguments(
        ["--host", "0.0.0.0", "--port", "9000", "--data-root", "/gpfs/beamline"]
    )

    import os

    assert os.environ["ALBIS_SERVER_HOST"] == "0.0.0.0"
    assert os.environ["ALBIS_SERVER_PORT"] == "9000"
    assert os.environ["ALBIS_DATA_ROOT"] == "/gpfs/beamline"
    assert applied == ["--host", "--port", "--data-root"]
    assert ignored == []


def test_no_browser_is_the_flag_form_of_a_config_key() -> None:
    import os

    _apply_cli_arguments(["--no-browser"])

    assert os.environ["ALBIS_LAUNCHER_OPEN_BROWSER"] == "false"


def test_config_points_at_a_file_and_is_expanded() -> None:
    import os

    _apply_cli_arguments(["--config", "~/albis-lab.json"])

    assert os.environ[CONFIG_PATH_ENV_VAR].endswith("albis-lab.json")
    assert "~" not in os.environ[CONFIG_PATH_ENV_VAR]


def test_nothing_is_set_when_nothing_is_passed() -> None:
    import os

    applied, ignored = _apply_cli_arguments([])

    assert applied == []
    assert ignored == []
    assert "ALBIS_SERVER_HOST" not in os.environ


def test_arguments_the_operating_system_adds_are_ignored_not_fatal() -> None:
    """macOS passes `-psn_0_...`, and a document path when ALBIS opens a file.

    Refusing to start over an argument nobody typed would be a worse failure
    than ignoring it, so unknown arguments are reported and dropped.
    """
    applied, ignored = _apply_cli_arguments(
        ["-psn_0_774931", "/Users/someone/data/frame.cbf", "--host", "127.0.0.1"]
    )

    assert applied == ["--host"]
    assert ignored == ["-psn_0_774931", "/Users/someone/data/frame.cbf"]


def test_the_help_text_names_the_other_two_layers() -> None:
    """Someone reading --help should learn the precedence, not just the flags."""
    help_text = _build_arg_parser().format_help()

    assert "ALBIS_SERVER_HOST" in help_text
    assert "albis.config.json" in help_text
    assert "environment" in help_text


@pytest.mark.parametrize(
    "flag,expected",
    [
        ("--host", "ALBIS_SERVER_HOST"),
        ("--port", "ALBIS_SERVER_PORT"),
        ("--allowed-hosts", "ALBIS_SERVER_ALLOWED_HOSTS"),
        ("--data-root", "ALBIS_DATA_ROOT"),
        ("--log-level", "ALBIS_LOGGING_LEVEL"),
        ("--language", "ALBIS_UI_LANGUAGE"),
    ],
)
def test_every_flag_maps_to_a_real_config_key(flag: str, expected: str) -> None:
    """A flag pointing at a key that does not exist would silently do nothing."""
    import os

    _apply_cli_arguments([flag, "value"])

    assert os.environ[expected] == "value"
    section, key = expected[len("ALBIS_") :].lower().split("_", 1)
    assert key in DEFAULT_CONFIG[section]
