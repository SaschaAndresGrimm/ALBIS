#!/usr/bin/env bash
# Install a pinned python-build-standalone CPython for the Linux release build.
#
# The Linux artifacts are built inside a manylinux_2_28 container so that they
# need nothing newer than glibc 2.28 (RHEL/Rocky/AlmaLinux 8). The interpreters
# that image ships under /opt/python are statically linked and carry no
# libpython3.x.so, which PyInstaller needs to bundle; actions/setup-python's
# builds target Ubuntu and do not run there at all. python-build-standalone
# ships a shared libpython and targets glibc 2.17, so it fits both needs.
#
# Usage: install_python_standalone.sh [DEST]
#   DEST  install prefix (default: /opt/albis-python); the interpreter lands at
#         DEST/bin/python3
set -euo pipefail

PBS_RELEASE="${PBS_RELEASE:-20260929}"
PBS_PYTHON_VERSION="${PBS_PYTHON_VERSION:-3.13.15}"
PBS_ASSET="${PBS_ASSET:-cpython-${PBS_PYTHON_VERSION}+${PBS_RELEASE}-x86_64-unknown-linux-gnu-install_only.tar.gz}"
PBS_SHA256="${PBS_SHA256:-d6b4e09474dfc219befabeae16264466f09615a991dcd080ae698833bdb3ed44}"
DEST="${1:-/opt/albis-python}"

tmp_file="$(mktemp)"
trap 'rm -f "$tmp_file"' EXIT

# `+` in the asset name must be percent-encoded in the download URL.
url="https://github.com/astral-sh/python-build-standalone/releases/download/${PBS_RELEASE}/${PBS_ASSET//+/%2B}"
curl -fsSL "$url" -o "$tmp_file"

actual_sha="$(sha256sum "$tmp_file" | awk '{print $1}')"
if [ "$actual_sha" != "$PBS_SHA256" ]; then
  echo "python-build-standalone checksum mismatch: expected $PBS_SHA256 got $actual_sha"
  exit 1
fi

rm -rf "$DEST"
mkdir -p "$DEST"
# The archive holds a single top-level `python/` directory.
tar -xzf "$tmp_file" -C "$DEST" --strip-components=1

if [ ! -f "$DEST/lib/libpython${PBS_PYTHON_VERSION%.*}.so.1.0" ]; then
  echo "Installed interpreter has no shared libpython; PyInstaller cannot bundle it."
  exit 1
fi

"$DEST/bin/python3" --version
echo "Installed python-build-standalone ${PBS_PYTHON_VERSION} (${PBS_RELEASE}) to ${DEST}"
