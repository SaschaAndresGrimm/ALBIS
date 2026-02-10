#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PYTHON_BIN="${PYTHON_BIN:-python3}"
VERSION_INFO="$("$PYTHON_BIN" scripts/version_info.py --shell)"
eval "$VERSION_INFO"
OS_VERSION="$(sw_vers -productVersion 2>/dev/null || uname -r)"
OS_TAG="$(echo "${OS_VERSION}" | tr '.' '_' | tr -cd '[:alnum:]_')"

SRC="dist/ALBIS"
if [ -d "dist/ALBIS.app" ]; then
  SRC="dist/ALBIS.app"
fi

if [ ! -d "$SRC" ]; then
  echo "Missing ${SRC}. Run ./scripts/build_mac.sh first."
  exit 1
fi

OUT="dist/ALBIS-macos-${OS_TAG}-${TAG}.dmg"
rm -f "$OUT"

TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TEMP_DIR"' EXIT

DMG_SRC="$SRC"
if [[ "$SRC" == *.app ]]; then
  DMG_STAGE="$TEMP_DIR/dmg-stage"
  mkdir -p "$DMG_STAGE"
  cp -R "$SRC" "$DMG_STAGE/$(basename "$SRC")"
  ln -s "/Applications" "$DMG_STAGE/Applications"
  DMG_SRC="$DMG_STAGE"
fi

hdiutil create -volname "ALBIS ${VERSION}" -srcfolder "$DMG_SRC" -ov -format UDZO "$OUT"
echo "Output: $OUT"
