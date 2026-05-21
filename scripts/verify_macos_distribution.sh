#!/usr/bin/env bash
set -euo pipefail

APP_PATH="${1:-dist/ALBIS.app}"
DMG_PATH="${2:-}"
ZIP_PATH="${3:-}"
REQUIRE_NOTARIZATION="${REQUIRE_NOTARIZATION:-1}"

die() {
  echo "[verify_macos_distribution] Error: $*" >&2
  exit 1
}

log() {
  echo "[verify_macos_distribution] $*"
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

require_command codesign
require_command spctl

if [ "$REQUIRE_NOTARIZATION" = "1" ]; then
  require_command xcrun
fi

verify_developer_id_signature_info() {
  local label="$1"
  local signature_info="$2"

  if printf '%s\n' "$signature_info" | grep -q '^Signature=adhoc$'; then
    die "$label is signed ad-hoc, not with Developer ID"
  fi
  if ! printf '%s\n' "$signature_info" | grep -q '^Authority=Developer ID Application:'; then
    die "$label is not signed with a Developer ID Application certificate"
  fi
  if ! printf '%s\n' "$signature_info" | grep -Eq '^TeamIdentifier=[[:alnum:]]+'; then
    die "$label does not have a TeamIdentifier"
  fi
}

verify_app() {
  local app_path="$1"
  local signature_info

  [ -d "$app_path" ] || die "Missing app bundle: $app_path"

  log "Verifying app signature: $app_path"
  codesign --verify --deep --strict "$app_path"
  signature_info="$(codesign -dv --verbose=4 "$app_path" 2>&1)"
  verify_developer_id_signature_info "$app_path" "$signature_info"

  if ! printf '%s\n' "$signature_info" | grep -q 'flags=.*runtime'; then
    die "$app_path is missing the hardened runtime flag"
  fi

  spctl -a -vvv -t exec "$app_path"

  if [ "$REQUIRE_NOTARIZATION" = "1" ]; then
    xcrun stapler validate "$app_path"
  fi
}

verify_dmg() {
  local dmg_path="$1"
  local signature_info

  [ -f "$dmg_path" ] || die "Missing DMG: $dmg_path"

  log "Verifying DMG signature: $dmg_path"
  codesign --verify "$dmg_path"
  signature_info="$(codesign -dv --verbose=4 "$dmg_path" 2>&1)"
  verify_developer_id_signature_info "$dmg_path" "$signature_info"

  spctl -a -vvv -t open --context context:primary-signature "$dmg_path"

  if [ "$REQUIRE_NOTARIZATION" = "1" ]; then
    xcrun stapler validate "$dmg_path"
  fi
}

verify_zip() {
  local zip_path="$1"
  local temp_dir zip_app

  [ -f "$zip_path" ] || die "Missing ZIP: $zip_path"

  temp_dir="$(mktemp -d)"
  cleanup_zip() {
    rm -rf "$temp_dir"
  }
  trap cleanup_zip RETURN

  log "Extracting ZIP for signature verification: $zip_path"
  if command -v ditto >/dev/null 2>&1; then
    ditto -x -k "$zip_path" "$temp_dir"
  elif command -v unzip >/dev/null 2>&1; then
    unzip -q "$zip_path" -d "$temp_dir"
  else
    die "Missing required command: ditto or unzip"
  fi

  zip_app="$temp_dir/$(basename "$APP_PATH")"
  if [ ! -d "$zip_app" ]; then
    zip_app="$(find "$temp_dir" -maxdepth 2 -type d -name '*.app' | head -n1 || true)"
  fi
  [ -n "$zip_app" ] && [ -d "$zip_app" ] || die "No .app bundle found in ZIP: $zip_path"

  verify_app "$zip_app"
}

verify_app "$APP_PATH"

if [ -n "$DMG_PATH" ]; then
  verify_dmg "$DMG_PATH"
fi

if [ -n "$ZIP_PATH" ]; then
  verify_zip "$ZIP_PATH"
fi

log "macOS signing verification completed."
