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
KEYCHAIN_SEARCH_LIST_CHANGED=0
ORIGINAL_KEYCHAINS=()

cleanup() {
  if [ -n "$KEYCHAIN_PATH" ] && command -v security >/dev/null 2>&1; then
    if [ "$KEYCHAIN_SEARCH_LIST_CHANGED" = "1" ]; then
      security list-keychains -d user -s "${ORIGINAL_KEYCHAINS[@]}" >/dev/null 2>&1 || true
    fi
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
  while IFS= read -r keychain; do
    [ -n "$keychain" ] && ORIGINAL_KEYCHAINS+=("$keychain")
  done < <(security list-keychains -d user | sed -e 's/^[[:space:]]*"//' -e 's/"$//')
  security list-keychains -d user -s "$KEYCHAIN_PATH" "${ORIGINAL_KEYCHAINS[@]}" >/dev/null
  KEYCHAIN_SEARCH_LIST_CHANGED=1
  security import "$CERT_FILE" \
    -k "$KEYCHAIN_PATH" \
    -P "$CERT_PASSWORD" \
    -T /usr/bin/codesign \
    -T /usr/bin/security \
    -T /usr/bin/xcrun >/dev/null
  security set-key-partition-list -S apple-tool:,apple:,codesign: -s -k "$KEYCHAIN_PASSWORD" "$KEYCHAIN_PATH" >/dev/null

  if [ -z "$IDENTITY" ]; then
    IDENTITY="$(security find-identity -v -p codesigning "$KEYCHAIN_PATH" | awk -F'"' '/"Developer ID Application:/ { print $2; exit }')"
  fi
fi

if [ -z "$IDENTITY" ]; then
  echo "[sign_macos] Could not determine a Developer ID Application signing identity."
  exit 1
fi

resolve_identity_common_name() {
  local identity="$1"
  local find_args=(-v -p codesigning)
  if [ -n "$KEYCHAIN_PATH" ]; then
    find_args+=("$KEYCHAIN_PATH")
  fi

  security find-identity "${find_args[@]}" \
    | awk -F'"' -v identity="$identity" '
        index($0, identity) { print $2; found = 1; exit }
        $2 == identity { print $2; found = 1; exit }
        END { exit(found ? 0 : 1) }
      '
}

IDENTITY_COMMON_NAME="$IDENTITY"
if [[ "$IDENTITY_COMMON_NAME" != Developer\ ID\ Application:* ]]; then
  if ! IDENTITY_COMMON_NAME="$(resolve_identity_common_name "$IDENTITY")"; then
    echo "[sign_macos] Could not resolve signing identity: $IDENTITY"
    exit 1
  fi
fi

if [[ "$IDENTITY_COMMON_NAME" != Developer\ ID\ Application:* ]]; then
  echo "[sign_macos] macOS distribution requires a Developer ID Application identity, got: $IDENTITY_COMMON_NAME"
  exit 1
fi

SIGN_IDENTITY="$IDENTITY"
if [ -n "$KEYCHAIN_PATH" ]; then
  if SIGN_IDENTITY_HASH="$(security find-identity -v -p codesigning "$KEYCHAIN_PATH" | awk -F'"' -v identity="$IDENTITY_COMMON_NAME" '$2 == identity { print $1; exit }')" \
    && [ -n "$SIGN_IDENTITY_HASH" ]; then
    SIGN_IDENTITY="$(printf '%s' "$SIGN_IDENTITY_HASH" | awk '{ print $2 }')"
  fi
fi

echo "[sign_macos] Using signing identity: $IDENTITY_COMMON_NAME"

KEYCHAIN_ARGS=()
if [ -n "$KEYCHAIN_PATH" ]; then
  KEYCHAIN_ARGS+=(--keychain "$KEYCHAIN_PATH")
fi

make_app_zip() {
  # Absolute, because the `zip` fallback runs from inside the app's directory.
  local dest
  dest="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
  rm -f "$dest"
  if command -v ditto >/dev/null 2>&1; then
    ditto -c -k --sequesterRsrc --keepParent "$APP_PATH" "$dest"
  else
    (cd "$(dirname "$APP_PATH")" && zip -r -q "$dest" "$(basename "$APP_PATH")")
  fi
}

create_zip() {
  make_app_zip "$ZIP_OUT"
}

echo "[sign_macos] Signing app bundle: $APP_PATH"
codesign --force --deep --options runtime --timestamp "${KEYCHAIN_ARGS[@]}" --sign "$SIGN_IDENTITY" "$APP_PATH"
codesign --verify --deep --strict --verbose=2 "$APP_PATH"

if ! command -v hdiutil >/dev/null 2>&1; then
  echo "[sign_macos] hdiutil not available."
  exit 1
fi

# A DMG is a sealed copy of the app, so whatever the app is missing when this
# runs is missing for everyone who installs from it. That is why this is a
# function called late rather than a block run here: a notarization ticket
# stapled to $APP_PATH afterwards never reaches the copy inside the DMG, which
# is how v0.20.0 shipped a DMG whose app had no ticket and therefore asked
# Apple for one on every launch.
build_dmg() {
  local stage="$TEMP_DIR/dmg-stage"
  rm -rf "$stage"
  mkdir -p "$stage"
  cp -R "$APP_PATH" "$stage/$(basename "$APP_PATH")"
  ln -s "/Applications" "$stage/Applications"
  rm -f "$DMG_OUT"
  local attempt hdi_log
  for attempt in 1 2 3; do
    hdi_log="$TEMP_DIR/hdiutil-create-${attempt}.log"
    if hdiutil create -volname "ALBIS ${VERSION}" -srcfolder "$stage" -ov -format UDZO "$DMG_OUT" >"$hdi_log" 2>&1; then
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
  codesign --force --timestamp "${KEYCHAIN_ARGS[@]}" --sign "$SIGN_IDENTITY" "$DMG_OUT"
  codesign --verify --verbose=2 "$DMG_OUT"
}

notarize() {
  xcrun notarytool submit "$1" \
    --apple-id "$APPLE_ID" \
    --team-id "$APPLE_TEAM_ID" \
    --password "$APPLE_APP_SPECIFIC_PASSWORD" \
    --wait
}

APPLE_ID="${APPLE_ID:-}"
APPLE_TEAM_ID="${APPLE_TEAM_ID:-}"
APPLE_APP_SPECIFIC_PASSWORD="${APPLE_APP_SPECIFIC_PASSWORD:-}"

if [ -z "$APPLE_ID" ] && [ -z "$APPLE_TEAM_ID" ] && [ -z "$APPLE_APP_SPECIFIC_PASSWORD" ]; then
  echo "[sign_macos] Apple notarization credentials not set; skipping notarization."
  build_dmg
  create_zip
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

# The app is notarized on its own first. Notarizing only the DMG would still
# let `stapler staple` attach a ticket to $APP_PATH -- the ticket covers the
# nested code -- but by then the DMG has already sealed an unstapled copy.
echo "[sign_macos] Submitting app for notarization: $APP_PATH"
APP_NOTARIZE_ZIP="$TEMP_DIR/notarize-app.zip"
make_app_zip "$APP_NOTARIZE_ZIP"
notarize "$APP_NOTARIZE_ZIP"
xcrun stapler staple "$APP_PATH"
xcrun stapler validate "$APP_PATH"

# Now the copy sealed into the DMG carries its own ticket, so an app dragged
# out of it validates offline instead of asking Apple on every launch.
build_dmg

echo "[sign_macos] Submitting DMG for notarization: $DMG_OUT"
notarize "$DMG_OUT"
xcrun stapler staple "$DMG_OUT"
xcrun stapler validate "$DMG_OUT"

# Built last, from the stapled app, so the zip ships a ticket too.
create_zip

echo "[sign_macos] Signing and notarization completed."
