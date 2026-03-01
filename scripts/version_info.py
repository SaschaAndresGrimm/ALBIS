#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
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
    payload = {"version": version, "commit": commit, "tag": tag}

    if args.json:
        print(json.dumps(payload))
        return 0

    if args.shell:
        print(f"VERSION={version}")
        print(f"COMMIT={commit}")
        print(f"TAG={tag}")
        return 0

    print(tag)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
