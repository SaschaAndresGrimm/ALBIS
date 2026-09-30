#!/usr/bin/env bash
# Run the packaged-binary smoke test against the Linux tarball on another
# distribution, in a container of that distribution.
#
# The tarball is built on glibc 2.28 (manylinux_2_28) and is meant to run
# unchanged on every newer glibc. The build job can only prove it starts where
# it was built; this proves it starts on the oldest target and on current
# Ubuntu, each with nothing installed but the Python the smoke harness needs.
#
# It also starts ALBIS the way a user does, with a display and a stand-in
# `xdg-open`, and checks the browser would be started with the host's library
# path, not the bundle's. With the bundle's, Rocky 9's Firefox loaded the
# bundle's older libstdc++ and died while ALBIS said "opening browser".
#
# Usage: smoke_linux_distro.sh IMAGE TARBALL
#   IMAGE    container image to run in (a dnf- or apt-based distribution)
#   TARBALL  ALBIS-linux-x64-v<version>-<commit>.tar.gz
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 IMAGE TARBALL" >&2
  exit 2
fi
IMAGE="$1"
TARBALL="$(cd "$(dirname "$2")" && pwd)/$(basename "$2")"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
tar -xzf "$TARBALL" -C "$work"
if [ ! -x "$work/ALBIS/ALBIS" ]; then
  echo "Tarball has no ALBIS/ALBIS executable: $TARBALL" >&2
  exit 1
fi

cat >"$work/in_container.sh" <<'EOF'
set -eu
# The distribution's own Python runs the harness, never the bundle's, so the
# only thing tested against the distribution's libraries is the bundle itself.
# Python 3.12 on RHEL-family 8 comes from AppStream; the base 3.6 is too old
# for the harness.
if command -v dnf >/dev/null 2>&1; then
  dnf install -y -q python3.12 >/dev/null
  py=python3.12
else
  apt-get update -qq >/dev/null
  DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends python3 >/dev/null
  py=python3
fi
echo "$(. /etc/os-release && echo "$PRETTY_NAME"), $(ldd --version | head -n1)"
"$py" scripts/smoke_packaged_binary.py --binary /bundle/ALBIS/ALBIS --startup-timeout 120

for lib in libstdc++.so.6 libgcc_s.so.1; do
  if [ -e "/bundle/ALBIS/_internal/$lib" ]; then
    echo "FAIL: the bundle ships $lib; programs ALBIS starts can load it instead of the host's" >&2
    exit 1
  fi
done

# Launch as a user would: a display, and an xdg-open that records what the
# browser would inherit.
mkdir -p /tmp/fakebin /tmp/home
cat >/tmp/fakebin/xdg-open <<'X'
#!/bin/sh
printf '%s\n%s\n' "$1" "${LD_LIBRARY_PATH:-}" > /tmp/xdg-open.log
X
chmod +x /tmp/fakebin/xdg-open
PATH="/tmp/fakebin:$PATH" DISPLAY=:0 HOME=/tmp/home /bundle/ALBIS/ALBIS >/tmp/albis.log 2>&1 &
albis=$!
for _ in $(seq 1 240); do
  [ -s /tmp/xdg-open.log ] && break
  sleep 0.5
done
kill "$albis" 2>/dev/null || true
if [ ! -s /tmp/xdg-open.log ]; then
  echo "FAIL: ALBIS never started the browser" >&2
  cat /tmp/albis.log >&2
  exit 1
fi
url="$(sed -n 1p /tmp/xdg-open.log)"
library_path="$(sed -n 2p /tmp/xdg-open.log)"
case "$url" in
  http://*) ;;
  *) echo "FAIL: the browser was asked to open '$url'" >&2; exit 1 ;;
esac
case "$library_path" in
  */ALBIS/_internal*)
    echo "FAIL: the browser inherits the bundle's LD_LIBRARY_PATH ($library_path)" >&2
    exit 1
    ;;
esac
echo "Browser launch OK: $url with LD_LIBRARY_PATH='$library_path'"
EOF

docker run --rm --platform linux/amd64 \
  -v "$ROOT:/src:ro" \
  -v "$work:/bundle:ro" \
  -w /src \
  "$IMAGE" \
  sh /bundle/in_container.sh
