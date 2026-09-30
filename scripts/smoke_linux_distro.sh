#!/usr/bin/env bash
# Run the packaged-binary smoke test against the Linux tarball on another
# distribution, in a container of that distribution.
#
# The tarball is built on glibc 2.28 (manylinux_2_28) and is meant to run
# unchanged on every newer glibc. The build job can only prove it starts where
# it was built; this proves it starts on the oldest target and on current
# Ubuntu, each with nothing installed but the Python the smoke harness needs.
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

# The distribution's own Python runs the harness, never the bundle's, so the
# only thing tested against the distribution's libraries is the bundle itself.
# Python 3.12 on RHEL-family 8 comes from AppStream; the base 3.6 is too old
# for the harness.
docker run --rm --platform linux/amd64 \
  -v "$ROOT:/src:ro" \
  -v "$work:/bundle:ro" \
  -w /src \
  "$IMAGE" \
  sh -euc '
    if command -v dnf >/dev/null 2>&1; then
      dnf install -y -q python3.12 >/dev/null
      py=python3.12
    else
      apt-get update -qq >/dev/null
      DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends python3 >/dev/null
      py=python3
    fi
    echo "$(. /etc/os-release && echo "$PRETTY_NAME"), $(ldd --version | head -n1)"
    exec "$py" scripts/smoke_packaged_binary.py --binary /bundle/ALBIS/ALBIS --startup-timeout 120
  '
