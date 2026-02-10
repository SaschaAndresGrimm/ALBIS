#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PYTHON_BIN="${PYTHON_BIN:-python3}"
VERSION_INFO="$("$PYTHON_BIN" scripts/version_info.py --shell)"
eval "$VERSION_INFO"

SRC="dist/ALBIS"
if [ -d "dist/ALBIS.app" ]; then
  SRC="dist/ALBIS.app"
fi

if [ ! -d "$SRC" ]; then
  echo "Missing ${SRC}. Run ./scripts/build_mac.sh first."
  exit 1
fi

OUT="dist/ALBIS-mac-${TAG}.dmg"
rm -f "$OUT"

hdiutil create -volname "ALBIS ${VERSION}" -srcfolder "$SRC" -ov -format UDZO "$OUT"
echo "Output: $OUT"
