#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [ ! -d "dist/ALBIS" ]; then
  echo "Missing dist/ALBIS. Run ./scripts/build_mac.sh first."
  exit 1
fi

OUT="dist/ALBIS.dmg"
rm -f "$OUT"

hdiutil create -volname "ALBIS" -srcfolder "dist/ALBIS" -ov -format UDZO "$OUT"
echo "Output: $OUT"
