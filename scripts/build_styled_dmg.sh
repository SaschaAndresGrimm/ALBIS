#!/usr/bin/env bash
set -euo pipefail

# Lay out a macOS release DMG the way scripts/sign_macos.sh and
# scripts/build_mac.sh both need it: background image, icon size, and the
# positions of the app and the Applications alias, all read from
# scripts/dmg_layout.py so the two can never drift apart -- see that file's
# module docstring for why that matters.
#
# Usage: build_styled_dmg.sh <app_path> <output_dmg_path> <volname> [background_dir]
#
# background_dir holds dmg_background.png and dmg_background@2x.png and
# defaults to albis_assets/, the committed pair. Passing another is for
# previewing art from scripts/generate_dmg_background.py --output-dir before
# committing it.
#
# What this does not do: sign, notarize or staple anything. It only builds
# the DMG's file layout; the caller handles the rest, exactly as it did
# before this existed (this replaces a `cp -R` + `hdiutil create` block that
# used to sit inline in each caller).

USAGE="Usage: build_styled_dmg.sh <app_path> <output_dmg_path> <volname> [background_dir]"
APP_PATH="${1:?$USAGE}"
OUTPUT_DMG="${2:?$USAGE}"
VOLNAME="${3:?$USAGE}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKGROUND_DIR="${4:-$ROOT/albis_assets}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
CREATE_DMG="$ROOT/scripts/vendor/create-dmg/create-dmg"
BACKGROUND_1X="$BACKGROUND_DIR/dmg_background.png"
BACKGROUND_2X="$BACKGROUND_DIR/dmg_background@2x.png"

[ -d "$APP_PATH" ] || { echo "[build_styled_dmg] Missing app bundle: $APP_PATH"; exit 1; }
[ -x "$CREATE_DMG" ] || { echo "[build_styled_dmg] Missing vendored create-dmg: $CREATE_DMG"; exit 1; }
for image in "$BACKGROUND_1X" "$BACKGROUND_2X"; do
  [ -f "$image" ] || { echo "[build_styled_dmg] Missing $image (run scripts/generate_dmg_background.py)"; exit 1; }
done
command -v tiffutil >/dev/null 2>&1 || { echo "[build_styled_dmg] tiffutil not available (macOS only)."; exit 1; }

# Single source of truth for window/icon geometry -- see scripts/dmg_layout.py.
eval "$("$PYTHON_BIN" "$ROOT/scripts/dmg_layout.py" --shell)"

TEMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TEMP_DIR"' EXIT

# Finder draws a background at one image pixel per window point and does not
# scale it, so a lone 2x image shows only its top-left quarter. A
# multi-resolution TIFF holding both lets Finder pick the one that matches
# the display: crisp on Retina, correctly sized everywhere.
BACKGROUND="$TEMP_DIR/dmg_background.tiff"
tiffutil -cathidpicheck "$BACKGROUND_1X" "$BACKGROUND_2X" -out "$BACKGROUND" >/dev/null

# create-dmg copies the whole contents of this folder into the volume and
# adds the Applications alias itself (--app-drop-link), so this holds only
# the app -- a symlink added here as well would collide with that.
SRC_FOLDER="$TEMP_DIR/dmg-src"
mkdir -p "$SRC_FOLDER"
cp -R "$APP_PATH" "$SRC_FOLDER/$(basename "$APP_PATH")"
APP_NAME="$(basename "$APP_PATH")"

rm -f "$OUTPUT_DMG"

# The Finder-scripting step occasionally times out cold ("AppleEvent timed
# out", -1712) on its very first invocation in a session and then succeeds
# cleanly on retry -- observed directly while building this, not a
# theoretical concern. create-dmg treats that as non-fatal on its own (it
# still finalizes a valid, just unstyled, DMG and exits 0), so the retry
# has to watch its output for the failure text rather than its exit code.
MAX_ATTEMPTS=3
attempt=1
styled=0
while [ "$attempt" -le "$MAX_ATTEMPTS" ]; do
  find "$TEMP_DIR" -maxdepth 1 -name "rw.*.dmg" -delete 2>/dev/null || true
  log="$TEMP_DIR/create-dmg-${attempt}.log"
  set +e
  "$CREATE_DMG" \
    --volname "$VOLNAME" \
    --window-pos "$DMG_WINDOW_POS_X" "$DMG_WINDOW_POS_Y" \
    --window-size "$DMG_WINDOW_W" "$DMG_WINDOW_H" \
    --icon-size "$DMG_ICON_SIZE" \
    --icon "$APP_NAME" "$DMG_APP_ICON_X" "$DMG_APP_ICON_Y" \
    --hide-extension "$APP_NAME" \
    --app-drop-link "$DMG_APPLICATIONS_ICON_X" "$DMG_APPLICATIONS_ICON_Y" \
    --background "$BACKGROUND" \
    --no-internet-enable \
    --overwrite \
    "$OUTPUT_DMG" \
    "$SRC_FOLDER" >"$log" 2>&1
  create_dmg_status=$?
  set -e
  cat "$log"

  if [ "$create_dmg_status" -ne 0 ]; then
    echo "[build_styled_dmg] create-dmg exited $create_dmg_status (attempt $attempt/$MAX_ATTEMPTS)"
  elif grep -q "Failed running AppleScript" "$log"; then
    echo "[build_styled_dmg] Finder styling failed (attempt $attempt/$MAX_ATTEMPTS); the DMG this produced is valid but plain."
  else
    styled=1
    break
  fi

  attempt=$((attempt + 1))
  [ "$attempt" -le "$MAX_ATTEMPTS" ] && sleep 5
done

[ -f "$OUTPUT_DMG" ] || { echo "[build_styled_dmg] No DMG was produced after $MAX_ATTEMPTS attempts."; exit 1; }

if [ "$styled" -ne 1 ]; then
  # Not fatal: every hard guarantee (signing, notarization, the app's own
  # contents) is untouched either way, and a DMG that opens Finder's default
  # icon layout is still a working installer. Loud rather than silent,
  # because a quietly-degraded release is exactly the failure mode
  # docs/RELEASE_CHECKLIST.md's post-release step now asks a human to catch.
  echo "::warning::macOS DMG styling did not apply after ${MAX_ATTEMPTS} attempts; ${OUTPUT_DMG} is a valid but plain DMG (default Finder layout, no background). See docs/RELEASE_CHECKLIST.md's post-release verification step."
fi

echo "[build_styled_dmg] Wrote $OUTPUT_DMG"
