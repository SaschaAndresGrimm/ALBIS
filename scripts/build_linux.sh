#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PYTHON_BIN="${PYTHON_BIN:-python3}"
VERSION_INFO="$("$PYTHON_BIN" scripts/version_info.py --shell)"
eval "$VERSION_INFO"

"$PYTHON_BIN" -m pip install --upgrade pyinstaller
"$PYTHON_BIN" -m PyInstaller ALBIS.spec

OUT="dist/ALBIS-linux-${TAG}.tar.gz"
rm -f "$OUT"
tar -czf "$OUT" -C dist ALBIS
echo "Output: dist/ALBIS and ${OUT}"
