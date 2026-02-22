#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PYTHON_BIN="${PYTHON_BIN:-python3}"
VERSION_INFO="$("$PYTHON_BIN" scripts/version_info.py --shell)"
eval "$VERSION_INFO"
export ALBIS_BUNDLE_VERSION="$VERSION"
export ALBIS_BUNDLE_BUILD="$VERSION"
OS_VERSION="$(sw_vers -productVersion 2>/dev/null || uname -r)"
OS_TAG="$(echo "${OS_VERSION}" | tr '.' '_' | tr -cd '[:alnum:]_')"
ZIP_OUT="dist/ALBIS-macos-${OS_TAG}-${TAG}.zip"
DMG_OUT="dist/ALBIS-macos-${OS_TAG}-${TAG}.dmg"

if ! "$PYTHON_BIN" -m PyInstaller --version >/dev/null 2>&1; then
  echo "PyInstaller not found for ${PYTHON_BIN}; installing to user site..."
  "$PYTHON_BIN" -m pip install --upgrade --user pyinstaller
fi

TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TEMP_DIR"' EXIT

# Prefer curated ALBIS icon assets when available.
ICON_ICNS_ASSET="albis_assets/albis_macos.icns"
ICON_PNG="albis_assets/albis_1024x1024.png"
if [ ! -f "$ICON_PNG" ]; then
  ICON_PNG="frontend/ressources/icon.png"
fi
ICON_ICNS="$TEMP_DIR/ALBIS.icns"
if [ -f "$ICON_ICNS_ASSET" ]; then
  export ALBIS_ICON="$ICON_ICNS_ASSET"
elif [ -f "$ICON_PNG" ] && command -v sips >/dev/null 2>&1 && command -v iconutil >/dev/null 2>&1; then
  ICONSET_DIR="$TEMP_DIR/ALBIS.iconset"
  mkdir -p "$ICONSET_DIR"
  for size in 16 32 128 256 512; do
    sips -z "$size" "$size" "$ICON_PNG" --out "$ICONSET_DIR/icon_${size}x${size}.png" >/dev/null
    double_size=$((size * 2))
    sips -z "$double_size" "$double_size" "$ICON_PNG" --out "$ICONSET_DIR/icon_${size}x${size}@2x.png" >/dev/null
  done
  iconutil -c icns "$ICONSET_DIR" -o "$ICON_ICNS"
  export ALBIS_ICON="$ICON_ICNS"
fi

# Non-interactive build: never prompt to remove existing output directories.
"$PYTHON_BIN" -m PyInstaller --noconfirm --clean ALBIS.spec

MAC_SRC="dist/ALBIS"
if [ -d "dist/ALBIS.app" ]; then
  MAC_SRC="dist/ALBIS.app"
fi

if command -v ditto >/dev/null 2>&1; then
  rm -f "$ZIP_OUT"
  ditto -c -k --sequesterRsrc --keepParent "$MAC_SRC" "$ZIP_OUT"
else
  rm -f "$ZIP_OUT"
  (cd dist && zip -r "$(basename "$ZIP_OUT")" "$(basename "$MAC_SRC")")
fi

if command -v hdiutil >/dev/null 2>&1; then
  rm -f "$DMG_OUT"
  DMG_SRC="$MAC_SRC"
  if [[ "$MAC_SRC" == *.app ]]; then
    DMG_STAGE="$TEMP_DIR/dmg-stage"
    mkdir -p "$DMG_STAGE"
    cp -R "$MAC_SRC" "$DMG_STAGE/$(basename "$MAC_SRC")"
    ln -s "/Applications" "$DMG_STAGE/Applications"
    DMG_SRC="$DMG_STAGE"
  fi
  hdiutil create -volname "ALBIS ${VERSION}" -srcfolder "$DMG_SRC" -ov -format UDZO "$DMG_OUT" >/dev/null
fi

echo "Output:"
echo "  dist/ALBIS"
[ -d "dist/ALBIS.app" ] && echo "  dist/ALBIS.app"
echo "  ${ZIP_OUT}"
[ -f "$DMG_OUT" ] && echo "  ${DMG_OUT}"
