#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

IDENTITY="${MACOS_SIGNING_IDENTITY:-}"
if [ -z "$IDENTITY" ]; then
  echo "[sign_macos] MACOS_SIGNING_IDENTITY not set; skipping signing/notarization."
  exit 0
fi

APP_PATH="${1:-dist/ALBIS.app}"
if [ ! -d "$APP_PATH" ]; then
  echo "[sign_macos] Missing app bundle: $APP_PATH"
  exit 1
fi

PYTHON_BIN="${PYTHON_BIN:-python3}"
VERSION_INFO="$($PYTHON_BIN scripts/version_info.py --shell)"
eval "$VERSION_INFO"
ZIP_OUT="dist/ALBIS-${TARGET}-${TAG}.zip"
DMG_OUT="dist/ALBIS-${TARGET}-${TAG}.dmg"

if ! command -v codesign >/dev/null 2>&1; then
  echo "[sign_macos] codesign not available."
  exit 1
fi

echo "[sign_macos] Signing app bundle: $APP_PATH"
codesign --force --deep --options runtime --timestamp --sign "$IDENTITY" "$APP_PATH"
codesign --verify --deep --strict --verbose=2 "$APP_PATH"

if command -v ditto >/dev/null 2>&1; then
  rm -f "$ZIP_OUT"
  ditto -c -k --sequesterRsrc --keepParent "$APP_PATH" "$ZIP_OUT"
else
  rm -f "$ZIP_OUT"
  (cd dist && zip -r "$(basename "$ZIP_OUT")" "$(basename "$APP_PATH")")
fi

if ! command -v hdiutil >/dev/null 2>&1; then
  echo "[sign_macos] hdiutil not available."
  exit 1
fi

TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TEMP_DIR"' EXIT
DMG_STAGE="$TEMP_DIR/dmg-stage"
mkdir -p "$DMG_STAGE"
cp -R "$APP_PATH" "$DMG_STAGE/$(basename "$APP_PATH")"
ln -s "/Applications" "$DMG_STAGE/Applications"
rm -f "$DMG_OUT"
hdiutil create -volname "ALBIS ${VERSION}" -srcfolder "$DMG_STAGE" -ov -format UDZO "$DMG_OUT" >/dev/null

APPLE_ID="${APPLE_ID:-}"
APPLE_TEAM_ID="${APPLE_TEAM_ID:-}"
APPLE_APP_SPECIFIC_PASSWORD="${APPLE_APP_SPECIFIC_PASSWORD:-}"

if [ -z "$APPLE_ID" ] && [ -z "$APPLE_TEAM_ID" ] && [ -z "$APPLE_APP_SPECIFIC_PASSWORD" ]; then
  echo "[sign_macos] Apple notarization credentials not set; skipping notarization."
  exit 0
fi

if [ -z "$APPLE_ID" ] || [ -z "$APPLE_TEAM_ID" ] || [ -z "$APPLE_APP_SPECIFIC_PASSWORD" ]; then
  echo "[sign_macos] Incomplete Apple notarization credentials; require APPLE_ID, APPLE_TEAM_ID, APPLE_APP_SPECIFIC_PASSWORD."
  exit 1
fi

if ! command -v xcrun >/dev/null 2>&1; then
  echo "[sign_macos] xcrun not available for notarization."
  exit 1
fi

echo "[sign_macos] Submitting DMG for notarization: $DMG_OUT"
xcrun notarytool submit "$DMG_OUT" \
  --apple-id "$APPLE_ID" \
  --team-id "$APPLE_TEAM_ID" \
  --password "$APPLE_APP_SPECIFIC_PASSWORD" \
  --wait

xcrun stapler staple "$APP_PATH"
xcrun stapler staple "$DMG_OUT"

echo "[sign_macos] Signing and notarization completed."
