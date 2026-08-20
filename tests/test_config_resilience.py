"""Cover for the config paths that decide whether ALBIS starts at all.

`load_config()` runs before logging exists, so anything it raises does not
surface as an error -- it stops the app from starting, and from a double-clicked
desktop build that is indistinguishable from nothing happening.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from backend import config as config_module
from backend.config import (
    DEFAULT_CONFIG,
    config_load_error,
    load_config,
    normalize_config,
    save_config,
)

# Each of these is something a user can end up with without doing anything odd.
UNREADABLE_CONFIGS = {
    "truncated_by_a_crash_mid_save": '{ "ui": { "language": "de",',
    "empty_after_a_failed_write": "",
    "written_by_a_newer_albis": '{"ui": {"language": "de", "future_option": true}}',
    "section_of_the_wrong_type": '{"ui": "de"}',
    "root_is_not_an_object": "[1, 2, 3]",
    "not_json_at_all": "the quick brown fox",
}


@pytest.fixture
def config_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Make `tmp_path` the first candidate config location."""
    monkeypatch.chdir(tmp_path)
    return tmp_path


@pytest.mark.parametrize("label", sorted(UNREADABLE_CONFIGS))
def test_unreadable_config_falls_back_to_defaults_instead_of_raising(
    config_dir: Path, label: str
) -> None:
    (config_dir / "albis.config.json").write_text(UNREADABLE_CONFIGS[label], encoding="utf-8")

    config, path = load_config()

    assert config == normalize_config(None), "should be exactly the defaults"
    assert path == (config_dir / "albis.config.json").resolve()
    # The reason has to survive the call: nothing is logging yet, so this is the
    # only way the startup banner can report that settings were not applied.
    assert config_load_error()


def test_unreadable_config_is_left_on_disk_for_recovery(config_dir: Path) -> None:
    """It is the user's file and may be salvageable; overwriting destroys that."""
    original = '{ "ui": { "language": "de",'
    target = config_dir / "albis.config.json"
    target.write_text(original, encoding="utf-8")

    load_config()

    assert target.read_text(encoding="utf-8") == original


def test_readable_config_is_applied_and_reports_no_error(config_dir: Path) -> None:
    (config_dir / "albis.config.json").write_text(
        json.dumps({"ui": {"language": "de"}, "data": {"allow_abs_paths": False}}),
        encoding="utf-8",
    )

    config, _path = load_config()

    assert config["ui"]["language"] == "de"
    assert config["data"]["allow_abs_paths"] is False
    assert config_load_error() == ""


def test_load_config_clears_a_stale_error_on_a_later_success(config_dir: Path) -> None:
    target = config_dir / "albis.config.json"
    target.write_text("{ broken", encoding="utf-8")
    load_config()
    assert config_load_error()

    target.write_text(json.dumps({"ui": {"language": "fr"}}), encoding="utf-8")
    config, _path = load_config()

    assert config["ui"]["language"] == "fr"
    assert config_load_error() == ""


def test_saving_settings_recovers_from_an_unreadable_config(config_dir: Path) -> None:
    """The in-app recovery path: the save replaces the file that would not load."""
    target = config_dir / "albis.config.json"
    target.write_text("{ broken", encoding="utf-8")
    config, path = load_config()
    assert config_load_error()

    config["ui"]["language"] = "ja"
    save_config(config, path)

    reloaded, _path = load_config()
    assert reloaded["ui"]["language"] == "ja"
    assert config_load_error() == ""


def test_save_config_replaces_the_file_atomically(
    config_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A crash mid-save must leave the previous config, not a truncated one.

    Writing in place is what makes an unreadable config reachable in the first
    place, so this guards the fix rather than the symptom.
    """
    target = config_dir / "albis.config.json"
    save_config(normalize_config({"ui": {"language": "de"}}), target)
    intact = target.read_text(encoding="utf-8")

    real_replace = os.replace

    def die_before_replacing(src, dst, *args, **kwargs):
        raise OSError("simulated crash after writing, before renaming")

    monkeypatch.setattr(config_module.os, "replace", die_before_replacing)
    with pytest.raises(OSError):
        save_config(normalize_config({"ui": {"language": "ja"}}), target)
    monkeypatch.setattr(config_module.os, "replace", real_replace)

    assert target.read_text(encoding="utf-8") == intact, "old config must survive"
    config, _path = load_config()
    assert config["ui"]["language"] == "de"
    assert config_load_error() == ""
    # The temp file must not be left behind to accumulate.
    assert [p.name for p in config_dir.iterdir()] == ["albis.config.json"]


def test_docker_image_disables_absolute_paths() -> None:
    """The image listens on 0.0.0.0 with no auth, so the desktop default is wrong.

    Left at the default `true`, anything able to reach the port could browse and
    read the whole container filesystem rather than just the mounted data root.
    """
    dockerfile = Path(__file__).resolve().parents[1] / "Dockerfile"
    body = dockerfile.read_text(encoding="utf-8")

    assert '"allow_abs_paths": False' in body
    assert DEFAULT_CONFIG["data"]["allow_abs_paths"] is True, (
        "desktop default is intentionally the opposite; if this changes, "
        "revisit the reasoning in the Dockerfile comment"
    )
