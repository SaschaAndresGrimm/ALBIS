#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

python3 -m pip install --upgrade pyinstaller
python3 -m PyInstaller ALBIS.spec

tar -czf dist/ALBIS-linux.tar.gz -C dist ALBIS
echo "Output: dist/ALBIS and dist/ALBIS-linux.tar.gz"
