#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

APP_PATH="${1:-dist/ALBIS.app}"
if [ ! -d "$APP_PATH" ]; then
  echo "[sign_macos] Missing app bundle: $APP_PATH"
  exit 1
fi

CERT_PATH="${MACOS_SIGN_CERT_PATH:-}"
CERT_B64="${MACOS_SIGN_CERT_B64:-}"
CERT_PASSWORD="${MACOS_SIGN_CERT_PASSWORD:-}"
IDENTITY="${MACOS_SIGNING_IDENTITY:-}"

if [ -z "$IDENTITY" ] && [ -z "$CERT_PATH" ] && [ -z "$CERT_B64" ]; then
  echo "[sign_macos] No macOS signing identity or certificate configured; skipping signing/notarization."
  exit 0
fi

PYTHON_BIN="${PYTHON_BIN:-python3}"
VERSION_INFO="$($PYTHON_BIN scripts/version_info.py --shell)"
eval "$VERSION_INFO"
ZIP_OUT="dist/ALBIS-${TARGET}-${TAG}.zip"
DMG_OUT="dist/ALBIS-${TARGET}-${TAG}.dmg"
TEMP_DIR="$(mktemp -d)"
KEYCHAIN_PATH=""
KEYCHAIN_PASSWORD=""

cleanup() {
  if [ -n "$KEYCHAIN_PATH" ] && command -v security >/dev/null 2>&1; then
    security delete-keychain "$KEYCHAIN_PATH" >/dev/null 2>&1 || true
  fi
  rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

if ! command -v codesign >/dev/null 2>&1; then
  echo "[sign_macos] codesign not available."
  exit 1
fi

decode_cert_payload() {
  local payload="$1"
  local out_file="$2"
  local compact
  compact="$(printf '%s' "$payload" | tr -d '[:space:]')"

  if printf '%s' "$compact" | base64 --decode >"$out_file" 2>/dev/null; then
    return 0
  fi
  if printf '%s' "$compact" | base64 -d >"$out_file" 2>/dev/null; then
    return 0
  fi
  if command -v python3 >/dev/null 2>&1; then
    python3 - "$compact" "$out_file" <<'PY'
import base64
import binascii
import sys

data = sys.argv[1].strip()
out_path = sys.argv[2]
candidates = [data, data.replace("-", "+").replace("_", "/")]

for candidate in candidates:
    if not candidate:
        continue
    padded = candidate + ("=" * ((4 - (len(candidate) % 4)) % 4))
    for payload in (candidate, padded):
        try:
            raw = base64.b64decode(payload, validate=False)
        except (ValueError, binascii.Error):
            continue
        if raw:
            with open(out_path, "wb") as handle:
                handle.write(raw)
            raise SystemExit(0)

raise SystemExit(1)
PY
    return $?
  fi
  return 1
}

if [ -n "$CERT_PATH" ] || [ -n "$CERT_B64" ]; then
  if [ -z "$CERT_PASSWORD" ]; then
    echo "[sign_macos] MACOS_SIGN_CERT_PASSWORD is required when MACOS_SIGN_CERT_PATH or MACOS_SIGN_CERT_B64 is set."
    exit 1
  fi
  if ! command -v security >/dev/null 2>&1; then
    echo "[sign_macos] security tool not available."
    exit 1
  fi

  CERT_FILE="$TEMP_DIR/signing-cert.p12"
  if [ -n "$CERT_PATH" ]; then
    if [ ! -f "$CERT_PATH" ]; then
      echo "[sign_macos] Certificate file not found: $CERT_PATH"
      exit 1
    fi
    cp "$CERT_PATH" "$CERT_FILE"
  else
    if ! decode_cert_payload "$CERT_B64" "$CERT_FILE"; then
      echo "[sign_macos] Failed to decode MACOS_SIGN_CERT_B64."
      exit 1
    fi
  fi

  if [ ! -s "$CERT_FILE" ]; then
    echo "[sign_macos] Imported certificate payload is empty."
    exit 1
  fi

  if command -v uuidgen >/dev/null 2>&1; then
    KEYCHAIN_PASSWORD="$(uuidgen)"
  else
    KEYCHAIN_PASSWORD="albis-signing-keychain"
  fi
  KEYCHAIN_PATH="$TEMP_DIR/albis-signing.keychain-db"

  security create-keychain -p "$KEYCHAIN_PASSWORD" "$KEYCHAIN_PATH" >/dev/null
  security set-keychain-settings -lut 21600 "$KEYCHAIN_PATH" >/dev/null
  security unlock-keychain -p "$KEYCHAIN_PASSWORD" "$KEYCHAIN_PATH" >/dev/null
  security import "$CERT_FILE" \
    -k "$KEYCHAIN_PATH" \
    -P "$CERT_PASSWORD" \
    -T /usr/bin/codesign \
    -T /usr/bin/security \
    -T /usr/bin/xcrun >/dev/null
  security set-key-partition-list -S apple-tool:,apple:,codesign: -s -k "$KEYCHAIN_PASSWORD" "$KEYCHAIN_PATH" >/dev/null

  if [ -z "$IDENTITY" ]; then
    IDENTITY="$(security find-identity -v -p codesigning "$KEYCHAIN_PATH" | awk -F'"' '/"/ { print $2; exit }')"
  fi
fi

if [ -z "$IDENTITY" ]; then
  echo "[sign_macos] Could not determine a macOS signing identity."
  exit 1
fi

KEYCHAIN_ARGS=()
if [ -n "$KEYCHAIN_PATH" ]; then
  KEYCHAIN_ARGS+=(--keychain "$KEYCHAIN_PATH")
fi

echo "[sign_macos] Signing app bundle: $APP_PATH"
codesign --force --deep --options runtime --timestamp "${KEYCHAIN_ARGS[@]}" --sign "$IDENTITY" "$APP_PATH"
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

DMG_STAGE="$TEMP_DIR/dmg-stage"
mkdir -p "$DMG_STAGE"
cp -R "$APP_PATH" "$DMG_STAGE/$(basename "$APP_PATH")"
ln -s "/Applications" "$DMG_STAGE/Applications"
rm -f "$DMG_OUT"
for attempt in 1 2 3; do
  hdi_log="$TEMP_DIR/hdiutil-create-${attempt}.log"
  if hdiutil create -volname "ALBIS ${VERSION}" -srcfolder "$DMG_STAGE" -ov -format UDZO "$DMG_OUT" >"$hdi_log" 2>&1; then
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

echo "[sign_macos] Signing DMG: $DMG_OUT"
codesign --force --timestamp "${KEYCHAIN_ARGS[@]}" --sign "$IDENTITY" "$DMG_OUT"
codesign --verify --verbose=2 "$DMG_OUT"

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
