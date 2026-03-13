from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSION_INFO_SCRIPT = ROOT / "scripts" / "version_info.py"


def _run_version_info(*args: str, env: dict[str, str] | None = None) -> str:
    output = subprocess.check_output(
        [sys.executable, str(VERSION_INFO_SCRIPT), *args],
        cwd=ROOT,
        env=env,
        text=True,
    )
    return output.strip()


def test_version_info_json_includes_target_fields() -> None:
    payload = json.loads(_run_version_info("--json"))
    assert payload["version"]
    assert payload["commit"]
    assert payload["tag"].startswith("v")
    assert payload["target_os"]
    assert payload["target_arch"]
    assert payload["target"] == f"{payload['target_os']}-{payload['target_arch']}"
    assert payload["appimage_arch"]


def test_version_info_target_can_be_overridden_via_env() -> None:
    env = os.environ.copy()
    env["ALBIS_TARGET_OS"] = "macos"
    env["ALBIS_TARGET_ARCH"] = "x86_64"
    payload = json.loads(_run_version_info("--json", env=env))
    assert payload["target_os"] == "macos"
    assert payload["target_arch"] == "x64"
    assert payload["target"] == "macos-x64"
    assert payload["appimage_arch"] == "x86_64"


def test_version_info_shell_output_contains_target_lines() -> None:
    lines = _run_version_info("--shell").splitlines()
    prefixes = {line.split("=", 1)[0] for line in lines if "=" in line}
    assert {
        "VERSION",
        "COMMIT",
        "TAG",
        "TARGET_OS",
        "TARGET_ARCH",
        "TARGET",
        "APPIMAGE_ARCH",
    }.issubset(prefixes)
