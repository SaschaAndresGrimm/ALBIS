#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BOOTSTRAP_PYTHON="${PYTHON_BIN:-python3}"
ISOLATED_BUILD="${ALBIS_BUILD_ISOLATED:-1}"
BUILD_VENV="${ALBIS_BUILD_VENV:-$ROOT/.build-venv-linux}"
PYINSTALLER_VERSION="${ALBIS_PYINSTALLER_VERSION:-6.19.0}"

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

# Stamp the commit into the bundle so the running program can name its build.
# Optional by design: an unstamped build shows its version alone.
"$PYTHON_BIN" scripts/stamp_build.py --commit "$COMMIT"

"$PYTHON_BIN" -m pip install --upgrade pip
if [ "$ISOLATED_BUILD" = "1" ]; then
  "$PYTHON_BIN" -m pip install -r backend/requirements.txt
fi
"$PYTHON_BIN" -m pip install --upgrade "pyinstaller==${PYINSTALLER_VERSION}"

# Prefer curated ALBIS icon assets when available.
if [ -f "albis_assets/icon_512x512.png" ]; then
  export ALBIS_ICON="$(pwd)/albis_assets/icon_512x512.png"
elif [ -f "albis_assets/icon_256x256.png" ]; then
  export ALBIS_ICON="$(pwd)/albis_assets/icon_256x256.png"
elif [ -f "frontend/resources/icon.png" ]; then
  export ALBIS_ICON="$(pwd)/frontend/resources/icon.png"
fi

# Non-interactive build: never prompt to remove existing output directories.
"$PYTHON_BIN" -m PyInstaller --noconfirm --clean ALBIS.spec

# Drop the debug info the Linux wheels and the standalone Python carry; it was
# two thirds of the download. See the script for what it leaves alone and why.
./scripts/strip_linux_bundle.sh dist/ALBIS

# The bill of materials for this bundle, from the interpreter that built it:
# the runtime closure of the pins on this platform, not the repository.
"$PYTHON_BIN" scripts/generate_sbom.py --target "$TARGET" --output "dist/ALBIS-${TARGET}-${TAG}.cdx.json"

# xz rather than gzip: 34 MB instead of 50 MB for the arm64 bundle. Every
# distribution ALBIS supports has xz, and GNU tar unpacks it with -xf alone.
OUT="dist/ALBIS-${TARGET}-${TAG}.tar.xz"
rm -f "$OUT"
tar -cf - -C dist ALBIS | xz -9 -T0 >"$OUT"
echo "Output: dist/ALBIS and ${OUT}"
