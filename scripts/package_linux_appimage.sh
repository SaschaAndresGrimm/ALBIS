#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PYTHON_BIN="${PYTHON_BIN:-python3}"
VERSION_INFO="$("$PYTHON_BIN" scripts/version_info.py --shell)"
eval "$VERSION_INFO"

if [ ! -d "dist/ALBIS" ]; then
  echo "Missing dist/ALBIS. Run ./scripts/build_linux.sh first."
  exit 1
fi

if ! command -v appimagetool >/dev/null 2>&1; then
  echo "Missing appimagetool. Install it first, then rerun."
  exit 1
fi

APPDIR="dist/AppDir"
rm -rf "$APPDIR"
mkdir -p "$APPDIR/usr/bin"

cp "dist/ALBIS/ALBIS" "$APPDIR/usr/bin/ALBIS"

cat > "$APPDIR/ALBIS.desktop" <<'EOF'
[Desktop Entry]
Type=Application
Name=ALBIS
Exec=ALBIS
Icon=ALBIS
Categories=Science;
Terminal=false
EOF

cat > "$APPDIR/AppRun" <<'EOF'
#!/bin/sh
HERE="$(dirname "$(readlink -f "$0")")"
exec "$HERE/usr/bin/ALBIS" "$@"
EOF
chmod +x "$APPDIR/AppRun"

if [ -f "frontend/ressources/image.png" ]; then
  cp "frontend/ressources/image.png" "$APPDIR/ALBIS.png"
fi

OUT="dist/ALBIS-linux-${TAG}.AppImage"
rm -f "$OUT"
appimagetool "$APPDIR" "$OUT"
echo "Output: $OUT"
