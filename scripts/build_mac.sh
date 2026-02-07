#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

python3 -m pip install --upgrade pyinstaller
python3 -m PyInstaller ALBIS.spec

if command -v ditto >/dev/null 2>&1; then
  rm -f dist/ALBIS-mac.zip
  ditto -c -k --sequesterRsrc --keepParent "dist/ALBIS" "dist/ALBIS-mac.zip"
else
  (cd dist && zip -r "ALBIS-mac.zip" "ALBIS")
fi

echo "Output: dist/ALBIS and dist/ALBIS-mac.zip"
