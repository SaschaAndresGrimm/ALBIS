#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BOOTSTRAP_PYTHON="${PYTHON_BIN:-python3}"
ISOLATED_BUILD="${ALBIS_BUILD_ISOLATED:-1}"
BUILD_VENV="${ALBIS_BUILD_VENV:-$ROOT/.build-venv-linux}"

if [ "$ISOLATED_BUILD" = "1" ]; then
  if [ "${ALBIS_BUILD_CLEAN_VENV:-1}" = "1" ]; then
    rm -rf "$BUILD_VENV"
  fi
  if [ ! -x "$BUILD_VENV/bin/python" ]; then
    "$BOOTSTRAP_PYTHON" -m venv "$BUILD_VENV"
  fi
  PYTHON_BIN="$BUILD_VENV/bin/python"
else
  PYTHON_BIN="$BOOTSTRAP_PYTHON"
fi

VERSION_INFO="$("$PYTHON_BIN" scripts/version_info.py --shell)"
eval "$VERSION_INFO"

"$PYTHON_BIN" -m pip install --upgrade pip
if [ "$ISOLATED_BUILD" = "1" ]; then
  "$PYTHON_BIN" -m pip install -r backend/requirements.txt
fi
"$PYTHON_BIN" -m pip install --upgrade pyinstaller

# Prefer curated ALBIS icon assets when available.
if [ -f "albis_assets/albis_512x512.png" ]; then
  export ALBIS_ICON="$(pwd)/albis_assets/albis_512x512.png"
elif [ -f "frontend/ressources/icon.png" ]; then
  export ALBIS_ICON="$(pwd)/frontend/ressources/icon.png"
fi

# Non-interactive build: never prompt to remove existing output directories.
"$PYTHON_BIN" -m PyInstaller --noconfirm --clean ALBIS.spec

OUT="dist/ALBIS-${TARGET}-${TAG}.tar.gz"
rm -f "$OUT"
tar -czf "$OUT" -C dist ALBIS
echo "Output: dist/ALBIS and ${OUT}"
