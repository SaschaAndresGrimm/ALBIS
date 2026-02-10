#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PYTHON_BIN="${PYTHON_BIN:-python3}"
VERSION_INFO="$("$PYTHON_BIN" scripts/version_info.py --shell)"
eval "$VERSION_INFO"
ZIP_OUT="dist/ALBIS-mac-${TAG}.zip"
DMG_OUT="dist/ALBIS-mac-${TAG}.dmg"

if ! "$PYTHON_BIN" -m PyInstaller --version >/dev/null 2>&1; then
  echo "PyInstaller not found for ${PYTHON_BIN}; installing to user site..."
  "$PYTHON_BIN" -m pip install --upgrade --user pyinstaller
fi

TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TEMP_DIR"' EXIT

ICON_PNG="frontend/ressources/icon.png"
ICON_ICNS="$TEMP_DIR/ALBIS.icns"
if [ -f "$ICON_PNG" ] && command -v sips >/dev/null 2>&1 && command -v iconutil >/dev/null 2>&1; then
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

"$PYTHON_BIN" -m PyInstaller ALBIS.spec

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
  hdiutil create -volname "ALBIS ${VERSION}" -srcfolder "$MAC_SRC" -ov -format UDZO "$DMG_OUT" >/dev/null
fi

echo "Output:"
echo "  dist/ALBIS"
[ -d "dist/ALBIS.app" ] && echo "  dist/ALBIS.app"
echo "  ${ZIP_OUT}"
[ -f "$DMG_OUT" ] && echo "  ${DMG_OUT}"
