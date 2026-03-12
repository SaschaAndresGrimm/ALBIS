#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

KEY_INPUT="${LINUX_GPG_PRIVATE_KEY_B64:-}"
if [ -z "$KEY_INPUT" ]; then
  echo "[sign_linux] LINUX_GPG_PRIVATE_KEY_B64 not set; skipping signing."
  exit 0
fi

if [ "$#" -lt 1 ]; then
  echo "[sign_linux] No files provided. Usage: ./scripts/sign_linux.sh <file> [<file> ...]"
  exit 1
fi

if ! command -v gpg >/dev/null 2>&1; then
  echo "[sign_linux] gpg not available."
  exit 1
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT
export GNUPGHOME="$TMP_DIR/gnupg"
mkdir -p "$GNUPGHOME"
chmod 700 "$GNUPGHOME"

KEY_FILE="$TMP_DIR/signing.key"

# Support either:
# 1) Base64-encoded private key payload (recommended for GitHub Secrets), or
# 2) Raw ASCII-armored private key block pasted directly into the secret.
#
# Also tolerate:
# - values wrapped in quotes
# - escaped newlines ("\n") in armored blocks
# - URL-safe base64 payloads (with missing padding)
KEY_NORMALIZED="$KEY_INPUT"
if [ "${#KEY_NORMALIZED}" -ge 2 ]; then
  if [ "${KEY_NORMALIZED:0:1}" = '"' ] && [ "${KEY_NORMALIZED: -1}" = '"' ]; then
    KEY_NORMALIZED="${KEY_NORMALIZED:1:${#KEY_NORMALIZED}-2}"
  elif [ "${KEY_NORMALIZED:0:1}" = "'" ] && [ "${KEY_NORMALIZED: -1}" = "'" ]; then
    KEY_NORMALIZED="${KEY_NORMALIZED:1:${#KEY_NORMALIZED}-2}"
  fi
fi

KEY_ARMORED="${KEY_NORMALIZED//\\n/$'\n'}"
KEY_ARMORED="${KEY_ARMORED//$'\r'/}"
if printf '%s' "$KEY_ARMORED" | grep -q "BEGIN PGP PRIVATE KEY BLOCK"; then
  printf '%s\n' "$KEY_ARMORED" >"$KEY_FILE"
else
  KEY_COMPACT="$(printf '%s' "$KEY_NORMALIZED" | tr -d '[:space:]')"
  decoded=0
  if printf '%s' "$KEY_COMPACT" | base64 --decode >"$KEY_FILE" 2>/dev/null; then
    decoded=1
  elif printf '%s' "$KEY_COMPACT" | base64 -d >"$KEY_FILE" 2>/dev/null; then
    decoded=1
  elif command -v python3 >/dev/null 2>&1; then
    if python3 - "$KEY_COMPACT" "$KEY_FILE" <<'PY'
import base64
import binascii
import sys

data = sys.argv[1].strip()
out_path = sys.argv[2]
candidates = [data, data.replace("-", "+").replace("_", "/")]

for candidate in candidates:
    if not candidate:
        continue
    padded = candidate + ("=" * ((4 - (len(candidate) % 4)) % 4))
    for payload in (candidate, padded):
        try:
            raw = base64.b64decode(payload, validate=False)
        except (ValueError, binascii.Error):
            continue
        if raw:
            with open(out_path, "wb") as handle:
                handle.write(raw)
            raise SystemExit(0)

raise SystemExit(1)
PY
    then
      decoded=1
    fi
  fi
  if [ "$decoded" -ne 1 ]; then
    echo "[sign_linux] Failed to decode LINUX_GPG_PRIVATE_KEY_B64."
    echo "[sign_linux] Provide base64 key data, an ASCII-armored private key block, or escaped armored block with \\n."
    exit 1
  fi
fi

if [ ! -s "$KEY_FILE" ]; then
  echo "[sign_linux] Decoded key payload is empty."
  exit 1
fi

if ! gpg --batch --import "$KEY_FILE" >/dev/null 2>&1; then
  echo "[sign_linux] Failed to import GPG private key."
  exit 1
fi

KEY_ID="${LINUX_GPG_KEY_ID:-}"
if [ -z "$KEY_ID" ]; then
  KEY_ID="$(gpg --batch --list-secret-keys --with-colons | awk -F: '$1=="sec" {print $5; exit}')"
fi
if [ -z "$KEY_ID" ]; then
  echo "[sign_linux] Could not determine signing key ID. Set LINUX_GPG_KEY_ID explicitly."
  exit 1
fi

PASSPHRASE="${LINUX_GPG_PASSPHRASE:-}"
if [ -z "$PASSPHRASE" ]; then
  echo "[sign_linux] LINUX_GPG_PASSPHRASE not set; attempting signing without passphrase."
fi

for file in "$@"; do
  if [ ! -f "$file" ]; then
    echo "[sign_linux] File not found: $file"
    exit 1
  fi
  sig_file="${file}.sig"
  rm -f "$sig_file"

  if [ -n "$PASSPHRASE" ]; then
    if ! printf '%s' "$PASSPHRASE" | gpg --batch --yes --pinentry-mode loopback --passphrase-fd 0 \
      --local-user "$KEY_ID" --output "$sig_file" --detach-sign "$file"; then
      echo "[sign_linux] Signing failed for $file"
      exit 1
    fi
  else
    if ! gpg --batch --yes --pinentry-mode loopback \
      --local-user "$KEY_ID" --output "$sig_file" --detach-sign "$file"; then
      echo "[sign_linux] Signing failed for $file"
      exit 1
    fi
  fi

  if ! gpg --batch --verify "$sig_file" "$file" >/dev/null 2>&1; then
    echo "[sign_linux] Signature verification failed for $file"
    exit 1
  fi
  echo "[sign_linux] Signed $file -> $sig_file"
done

echo "[sign_linux] Signing completed."
