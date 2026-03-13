#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIST_DIR="$ROOT/dist/ALBIS"

if [ ! -x "$DIST_DIR/ALBIS" ]; then
  echo "Missing dist/ALBIS/ALBIS. Run ./scripts/build_linux.sh first."
  exit 1
fi

PREFIX="${ALBIS_PREFIX:-$HOME/.local}"
if [ -z "$PREFIX" ] || [ "$PREFIX" = "/" ]; then
  echo "Refusing to install with ALBIS_PREFIX='$PREFIX'."
  exit 1
fi

BIN_DIR="$PREFIX/bin"
APP_DIR="$PREFIX/share/albis"
DESKTOP_DIR="$PREFIX/share/applications"
ICON_DIR="$PREFIX/share/icons/hicolor/512x512/apps"
LAUNCHER_PATH="$BIN_DIR/albis"
DESKTOP_FILE="$DESKTOP_DIR/albis.desktop"

mkdir -p "$BIN_DIR" "$DESKTOP_DIR" "$ICON_DIR"
rm -rf "$APP_DIR"
cp -a "$DIST_DIR" "$APP_DIR"

cat > "$LAUNCHER_PATH" <<EOF
#!/bin/sh
exec "$APP_DIR/ALBIS" "\$@"
EOF
chmod +x "$LAUNCHER_PATH"

ICON_SRC=""
if [ -f "$ROOT/albis_assets/icon_512x512.png" ]; then
  ICON_SRC="$ROOT/albis_assets/icon_512x512.png"
elif [ -f "$ROOT/albis_assets/icon_256x256.png" ]; then
  ICON_SRC="$ROOT/albis_assets/icon_256x256.png"
elif [ -f "$ROOT/frontend/ressources/icon.png" ]; then
  ICON_SRC="$ROOT/frontend/ressources/icon.png"
fi
if [ -n "$ICON_SRC" ]; then
  cp "$ICON_SRC" "$ICON_DIR/albis.png"
fi

cat > "$DESKTOP_FILE" <<EOF
[Desktop Entry]
Type=Application
Name=ALBIS
Comment=ALBIS detector image viewer
Exec=$LAUNCHER_PATH
Icon=albis
Categories=Science;
Terminal=false
StartupNotify=true
EOF
chmod 644 "$DESKTOP_FILE"

if command -v update-desktop-database >/dev/null 2>&1; then
  update-desktop-database "$DESKTOP_DIR" >/dev/null 2>&1 || true
fi
if command -v gtk-update-icon-cache >/dev/null 2>&1; then
  gtk-update-icon-cache -q "$PREFIX/share/icons/hicolor" >/dev/null 2>&1 || true
fi

echo "Installed ALBIS to:"
echo "  app:      $APP_DIR"
echo "  launcher: $LAUNCHER_PATH"
echo "  desktop:  $DESKTOP_FILE"
