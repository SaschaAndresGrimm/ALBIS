#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: ./scripts/install_linux_appimage.sh <path-to-AppImage>"
}

if [ "$#" -ne 1 ]; then
  usage
  exit 1
fi

APPIMAGE_SRC="$1"
if [ ! -f "$APPIMAGE_SRC" ]; then
  echo "AppImage not found: $APPIMAGE_SRC"
  exit 1
fi

PREFIX="${ALBIS_PREFIX:-$HOME/.local}"
if [ -z "$PREFIX" ] || [ "$PREFIX" = "/" ]; then
  echo "Refusing to install with ALBIS_PREFIX='$PREFIX'."
  exit 1
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="$PREFIX/bin"
APP_DIR="$PREFIX/share/albis"
DESKTOP_DIR="$PREFIX/share/applications"
ICON_DIR="$PREFIX/share/icons/hicolor/512x512/apps"
APPIMAGE_DEST="$APP_DIR/ALBIS.AppImage"
LAUNCHER_PATH="$BIN_DIR/albis"
DESKTOP_FILE="$DESKTOP_DIR/albis.desktop"

mkdir -p "$BIN_DIR" "$APP_DIR" "$DESKTOP_DIR" "$ICON_DIR"
cp "$APPIMAGE_SRC" "$APPIMAGE_DEST"
chmod +x "$APPIMAGE_DEST"

cat > "$LAUNCHER_PATH" <<EOF
#!/bin/sh
exec "$APPIMAGE_DEST" "\$@"
EOF
chmod +x "$LAUNCHER_PATH"

ICON_SRC=""
if [ -f "$ROOT/albis_assets/icon_512x512.png" ]; then
  ICON_SRC="$ROOT/albis_assets/icon_512x512.png"
elif [ -f "$ROOT/albis_assets/icon_256x256.png" ]; then
  ICON_SRC="$ROOT/albis_assets/icon_256x256.png"
elif [ -f "$ROOT/frontend/resources/icon.png" ]; then
  ICON_SRC="$ROOT/frontend/resources/icon.png"
fi
if [ -n "$ICON_SRC" ]; then
  cp "$ICON_SRC" "$ICON_DIR/albis.png"
fi

# MimeType and the %f in Exec are what make a file manager offer ALBIS for a
# detector image. They are repeated here rather than shared with
# packaging/linux/ALBIS.desktop because that one names the AppImage's internal
# binary and this one an absolute launcher path; tests/test_file_associations.py
# checks the two keep the same type list.
cat > "$DESKTOP_FILE" <<EOF
[Desktop Entry]
Type=Application
Name=ALBIS
Comment=ALBIS detector image viewer
Exec=$LAUNCHER_PATH %f
Icon=albis
Categories=Science;
MimeType=application/x-hdf5;image/x-cbf;image/x-edf;image/tiff;
Terminal=false
StartupNotify=true
EOF
chmod 644 "$DESKTOP_FILE"

# CBF and EDF have no registered media type, so without this a file manager has
# nothing to match them against and the association above never applies to them.
MIME_SRC="$ROOT/packaging/linux/ALBIS-mime.xml"
if [ ! -f "$MIME_SRC" ] && [ -f "$ROOT/usr/share/mime/packages/ALBIS.xml" ]; then
  MIME_SRC="$ROOT/usr/share/mime/packages/ALBIS.xml"
fi
if [ -f "$MIME_SRC" ]; then
  MIME_DIR="$PREFIX/share/mime/packages"
  mkdir -p "$MIME_DIR"
  cp "$MIME_SRC" "$MIME_DIR/ALBIS.xml"
  chmod 644 "$MIME_DIR/ALBIS.xml"
  if command -v update-mime-database >/dev/null 2>&1; then
    update-mime-database "$PREFIX/share/mime" >/dev/null 2>&1 || true
  fi
fi

if [ -d "$DESKTOP_DIR" ] && command -v update-desktop-database >/dev/null 2>&1; then
  update-desktop-database "$DESKTOP_DIR" >/dev/null 2>&1 || true
fi
if [ -d "$PREFIX/share/icons/hicolor" ] && command -v gtk-update-icon-cache >/dev/null 2>&1; then
  gtk-update-icon-cache -q "$PREFIX/share/icons/hicolor" >/dev/null 2>&1 || true
fi

echo "Installed ALBIS AppImage to:"
echo "  appimage: $APPIMAGE_DEST"
echo "  launcher: $LAUNCHER_PATH"
echo "  desktop:  $DESKTOP_FILE"
