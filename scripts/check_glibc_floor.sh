#!/usr/bin/env bash
# Fail if any bundled ELF requires a glibc newer than the supported floor.
#
# AppImages / PyInstaller bundles are only forward-compatible: a binary linked
# against GLIBC_2.38 will not load on a host with an older glibc. Building on a
# newer runner than the oldest supported target therefore silently breaks the
# artifact there (see the ubuntu-22.04 pin). This guard makes that regression a
# hard build failure instead of a runtime crash on a user's machine.
#
# Usage: check_glibc_floor.sh [FLOOR] [ROOT]
#   FLOOR  max allowed glibc version (default: 2.35, i.e. Ubuntu 22.04)
#   ROOT   directory tree to scan (default: dist/ALBIS)
set -euo pipefail

FLOOR="${1:-2.35}"
ROOT="${2:-dist/ALBIS}"

if [ ! -d "$ROOT" ]; then
  echo "check_glibc_floor: scan root not found: $ROOT" >&2
  exit 2
fi
if ! command -v objdump >/dev/null 2>&1; then
  echo "check_glibc_floor: objdump not found; install binutils." >&2
  exit 2
fi

# Collect every GLIBC_x.y[.z] symbol version referenced by ELF files under ROOT.
versions=""
while IFS= read -r -d '' f; do
  case "$(file -b "$f" 2>/dev/null)" in
    ELF*) ;;
    *) continue ;;
  esac
  syms="$(objdump -T "$f" 2>/dev/null | grep -oE 'GLIBC_[0-9]+\.[0-9]+(\.[0-9]+)?' || true)"
  if [ -n "$syms" ]; then
    versions="${versions}${syms}"$'\n'
  fi
done < <(find "$ROOT" -type f -print0)

versions="$(printf '%s' "$versions" | sed '/^$/d; s/^GLIBC_//' | sort -uV)"
if [ -z "$versions" ]; then
  echo "check_glibc_floor: no GLIBC symbol versions found under $ROOT (unexpected)." >&2
  exit 2
fi

max="$(printf '%s\n' "$versions" | sort -V | tail -n1)"
highest="$(printf '%s\n%s\n' "$max" "$FLOOR" | sort -V | tail -n1)"

if [ "$max" != "$FLOOR" ] && [ "$highest" = "$max" ]; then
  echo "glibc floor VIOLATED: bundle requires GLIBC_${max} but floor is GLIBC_${FLOOR}." >&2
  echo "This artifact will not run on the oldest supported target." >&2
  echo "Highest glibc versions referenced:" >&2
  printf '  GLIBC_%s\n' $(printf '%s\n' "$versions" | sort -V | tail -n5) >&2
  exit 1
fi

echo "glibc floor OK: max required GLIBC_${max} <= floor GLIBC_${FLOOR}"
