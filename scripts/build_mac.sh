#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PYTHON_BIN="${PYTHON_BIN:-python3}"
PYINSTALLER_VERSION="${ALBIS_PYINSTALLER_VERSION:-6.19.0}"
VERSION_INFO="$("$PYTHON_BIN" scripts/version_info.py --shell)"
eval "$VERSION_INFO"
export ALBIS_BUNDLE_VERSION="$VERSION"
export ALBIS_BUNDLE_BUILD="$VERSION"

# Stamp the commit into the bundle so the running program can name its build.
# Optional by design: an unstamped build shows its version alone.
"$PYTHON_BIN" scripts/stamp_build.py --commit "$COMMIT"
ZIP_OUT="dist/ALBIS-${TARGET}-${TAG}.zip"
DMG_OUT="dist/ALBIS-${TARGET}-${TAG}.dmg"

echo "Installing pinned PyInstaller ${PYINSTALLER_VERSION} for ${PYTHON_BIN}..."
"$PYTHON_BIN" -m pip install --upgrade --user "pyinstaller==${PYINSTALLER_VERSION}"

TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TEMP_DIR"' EXIT

# Prefer curated ALBIS icon assets when available.
ICON_ICNS_ASSET="albis_assets/icon.icns"
ICON_PNG="albis_assets/icon_1024x1024.png"
if [ ! -f "$ICON_PNG" ]; then
  ICON_PNG="frontend/resources/icon.png"
fi
ICON_ICNS="$TEMP_DIR/ALBIS.icns"
if [ -f "$ICON_ICNS_ASSET" ]; then
  export ALBIS_ICON="$ICON_ICNS_ASSET"
elif [ -f "$ICON_PNG" ] && command -v sips >/dev/null 2>&1 && command -v iconutil >/dev/null 2>&1; then
  ICONSET_DIR="$TEMP_DIR/ALBIS.iconset"
  mkdir -p "$ICONSET_DIR"

  add_icon() {
    local target="$1"
    local size="$2"
    shift 2
    for candidate in "$@"; do
      if [ -f "$candidate" ]; then
        cp "$candidate" "$ICONSET_DIR/$target"
        return
      fi
    done
    sips -z "$size" "$size" "$ICON_PNG" --out "$ICONSET_DIR/$target" >/dev/null
  }

  add_icon "icon_16x16.png" 16 "albis_assets/icon_16x16.png"
  add_icon "icon_16x16@2x.png" 32 "albis_assets/icon_16x16@2x.png" "albis_assets/icon_32x32.png"
  add_icon "icon_32x32.png" 32 "albis_assets/icon_32x32.png" "albis_assets/icon_16x16@2x.png"
  add_icon "icon_32x32@2x.png" 64 "albis_assets/icon_32x32@2x.png" "albis_assets/icon_64x64.png"
  add_icon "icon_128x128.png" 128 "albis_assets/icon_128x128.png"
  add_icon "icon_128x128@2x.png" 256 "albis_assets/icon_256x256.png"
  add_icon "icon_256x256.png" 256 "albis_assets/icon_256x256.png"
  add_icon "icon_256x256@2x.png" 512 "albis_assets/icon_512x512.png"
  add_icon "icon_512x512.png" 512 "albis_assets/icon_512x512.png"
  add_icon "icon_512x512@2x.png" 1024 "albis_assets/icon_1024x1024.png"

  iconutil -c icns "$ICONSET_DIR" -o "$ICON_ICNS"
  export ALBIS_ICON="$ICON_ICNS"
fi

# Non-interactive build: never prompt to remove existing output directories.
"$PYTHON_BIN" -m PyInstaller --noconfirm --clean ALBIS.spec

MAC_SRC="dist/ALBIS"
if [ -d "dist/ALBIS.app" ]; then
  MAC_SRC="dist/ALBIS.app"
fi

if [[ "$MAC_SRC" == *.app ]] && { [ -n "${MACOS_SIGNING_IDENTITY:-}" ] || [ -n "${MACOS_SIGN_CERT_PATH:-}" ] || [ -n "${MACOS_SIGN_CERT_B64:-}" ]; }; then
  ./scripts/sign_macos.sh "$MAC_SRC"
else
  if command -v ditto >/dev/null 2>&1; then
    rm -f "$ZIP_OUT"
    ditto -c -k --sequesterRsrc --keepParent "$MAC_SRC" "$ZIP_OUT"
  else
    rm -f "$ZIP_OUT"
    (cd dist && zip -r "$(basename "$ZIP_OUT")" "$(basename "$MAC_SRC")")
  fi

  if command -v hdiutil >/dev/null 2>&1; then
    rm -f "$DMG_OUT"
    if [[ "$MAC_SRC" == *.app ]]; then
      # Same window/icon layout and background as the signed build -- see
      # scripts/build_styled_dmg.sh and scripts/dmg_layout.py -- so a local,
      # unsigned dev build looks the same as a release one.
      ./scripts/build_styled_dmg.sh "$MAC_SRC" "$DMG_OUT" "ALBIS ${VERSION}"
    else
      # PyInstaller's BUNDLE() step in ALBIS.spec runs unconditionally on
      # darwin, so dist/ALBIS.app always exists here in practice; this is a
      # defensive fallback for a bare onedir output, kept as it was.
      for attempt in 1 2 3; do
        hdi_log="$TEMP_DIR/hdiutil-create-${attempt}.log"
        if hdiutil create -volname "ALBIS ${VERSION}" -srcfolder "$MAC_SRC" -ov -format UDZO "$DMG_OUT" >"$hdi_log" 2>&1; then
          break
        fi
        if grep -q "Resource busy" "$hdi_log" && [ "$attempt" -lt 3 ]; then
          sleep $((attempt * 5))
          rm -f "$DMG_OUT"
          continue
        fi
        cat "$hdi_log"
        exit 1
      done
    fi
  fi
fi

echo "Output:"
echo "  dist/ALBIS"
[ -d "dist/ALBIS.app" ] && echo "  dist/ALBIS.app"
echo "  ${ZIP_OUT}"
[ -f "$DMG_OUT" ] && echo "  ${DMG_OUT}"
