#!/usr/bin/env bash
# Fail if any bundled ELF requires a glibc, or a C++ runtime, newer than the
# supported floor.
#
# AppImages / PyInstaller bundles are only forward-compatible: a binary linked
# against GLIBC_2.38 will not load on a host with an older glibc. Building on a
# newer runner than the oldest supported target therefore silently breaks the
# artifact there (see the manylinux_2_28 build image). This guard makes that
# regression a hard build failure instead of a runtime crash on a user's machine.
#
# The C++ runtime (libstdc++, libgcc_s) is not bundled but taken from the host
# (see ALBIS.spec), so the same holds for GLIBCXX_ symbol versions: nothing may
# ask for more than the oldest target's libstdc++ provides.
#
# Usage: check_glibc_floor.sh [FLOOR] [ROOT] [GLIBCXX_FLOOR]
#   FLOOR          max allowed glibc version (default: 2.28, i.e. RHEL 8)
#   ROOT           directory tree to scan (default: dist/ALBIS)
#   GLIBCXX_FLOOR  max allowed GLIBCXX version (default: 3.4.25, RHEL 8's GCC 8)
set -euo pipefail

FLOOR="${1:-2.28}"
ROOT="${2:-dist/ALBIS}"
GLIBCXX_FLOOR="${3:-3.4.25}"

if [ ! -d "$ROOT" ]; then
  echo "check_glibc_floor: scan root not found: $ROOT" >&2
  exit 2
fi
if ! command -v objdump >/dev/null 2>&1; then
  echo "check_glibc_floor: objdump not found; install binutils." >&2
  exit 2
fi

# Collect every GLIBC_x.y[.z] and GLIBCXX_x.y.z symbol version referenced by
# ELF files under ROOT.
symbols=""
while IFS= read -r -d '' f; do
  case "$(file -b "$f" 2>/dev/null)" in
    ELF*) ;;
    *) continue ;;
  esac
  syms="$(objdump -T "$f" 2>/dev/null | grep -oE 'GLIBC(XX)?_[0-9]+\.[0-9]+(\.[0-9]+)?' || true)"
  if [ -n "$syms" ]; then
    symbols="${symbols}${syms}"$'\n'
  fi
done < <(find "$ROOT" -type f -print0)

# check PREFIX FLOOR: fail if a PREFIX_x.y version above FLOOR is referenced.
check() {
  local prefix="$1" floor="$2" versions max highest
  versions="$(printf '%s' "$symbols" | grep -E "^${prefix}_" | sed "s/^${prefix}_//" | sort -uV || true)"
  if [ -z "$versions" ]; then
    if [ "$prefix" = GLIBC ]; then
      echo "check_glibc_floor: no GLIBC symbol versions found under $ROOT (unexpected)." >&2
      exit 2
    fi
    echo "${prefix} floor OK: no ${prefix} symbol versions referenced"
    return 0
  fi
  max="$(printf '%s\n' "$versions" | tail -n1)"
  highest="$(printf '%s\n%s\n' "$max" "$floor" | sort -V | tail -n1)"
  if [ "$max" != "$floor" ] && [ "$highest" = "$max" ]; then
    echo "${prefix} floor VIOLATED: bundle requires ${prefix}_${max} but floor is ${prefix}_${floor}." >&2
    echo "This artifact will not run on the oldest supported target." >&2
    echo "Highest ${prefix} versions referenced:" >&2
    printf '%s\n' "$versions" | tail -n5 | sed "s/^/  ${prefix}_/" >&2
    exit 1
  fi
  echo "${prefix} floor OK: max required ${prefix}_${max} <= floor ${prefix}_${floor}"
}

check GLIBC "$FLOOR"
check GLIBCXX "$GLIBCXX_FLOOR"
