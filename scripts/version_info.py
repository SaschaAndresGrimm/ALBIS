#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import platform
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
VERSION_FILE = ROOT / "VERSION"


def read_version() -> str:
    try:
        raw = VERSION_FILE.read_text(encoding="utf-8").strip()
    except OSError:
        return "0.0.0"
    return raw or "0.0.0"


def read_commit() -> str:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=ROOT,
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        out = ""
    return out or "nogit"


def _normalize_target_os(raw: str) -> str:
    value = raw.strip().lower()
    if value.startswith("darwin") or value in {"mac", "macos", "osx"}:
        return "macos"
    if value.startswith("linux"):
        return "linux"
    if value.startswith("win") or value in {"cygwin", "msys"}:
        return "windows"
    cleaned = re.sub(r"[^a-z0-9]+", "", value)
    return cleaned or "unknown"


def _normalize_target_arch(raw: str) -> str:
    value = raw.strip().lower()
    if value in {"x86_64", "amd64", "x64", "x86-64"}:
        return "x64"
    if value in {"arm64", "aarch64"}:
        return "arm64"
    if value in {"x86", "i386", "i686"}:
        return "x86"
    if value.startswith("armv7"):
        return "armv7"
    if value.startswith("armv6"):
        return "armv6"
    cleaned = re.sub(r"[^a-z0-9]+", "", value)
    return cleaned or "unknown"


def _appimage_arch_for_target_arch(target_arch: str) -> str:
    if target_arch == "x64":
        return "x86_64"
    if target_arch == "arm64":
        return "aarch64"
    if target_arch == "x86":
        return "i686"
    if target_arch in {"armv6", "armv7"}:
        return "armhf"
    return target_arch


def detect_target() -> tuple[str, str, str]:
    raw_os = os.environ.get("ALBIS_TARGET_OS", "") or platform.system()
    raw_arch = os.environ.get("ALBIS_TARGET_ARCH", "") or platform.machine()
    target_os = _normalize_target_os(raw_os)
    target_arch = _normalize_target_arch(raw_arch)
    target = f"{target_os}-{target_arch}"
    return target_os, target_arch, target


def main() -> int:
    parser = argparse.ArgumentParser(description="Emit ALBIS version metadata for build scripts.")
    parser.add_argument("--json", action="store_true", help="Print JSON output")
    parser.add_argument(
        "--shell", action="store_true", help="Print shell-compatible KEY=VALUE lines"
    )
    args = parser.parse_args()

    version = read_version()
    commit = read_commit()
    tag = f"v{version}-{commit}"
    target_os, target_arch, target = detect_target()
    appimage_arch = _appimage_arch_for_target_arch(target_arch)
    payload = {
        "version": version,
        "commit": commit,
        "tag": tag,
        "target_os": target_os,
        "target_arch": target_arch,
        "target": target,
        "appimage_arch": appimage_arch,
    }

    if args.json:
        print(json.dumps(payload))
        return 0

    if args.shell:
        print(f"VERSION={version}")
        print(f"COMMIT={commit}")
        print(f"TAG={tag}")
        print(f"TARGET_OS={target_os}")
        print(f"TARGET_ARCH={target_arch}")
        print(f"TARGET={target}")
        print(f"APPIMAGE_ARCH={appimage_arch}")
        return 0

    print(tag)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
