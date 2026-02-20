#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
APP_FILE = ROOT / "backend" / "app.py"


def read_version() -> str:
    text = APP_FILE.read_text(encoding="utf-8")
    match = re.search(r'ALBIS_VERSION\s*=\s*"([^"]+)"', text)
    if not match:
        return "0.0.0"
    return match.group(1).strip() or "0.0.0"


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
