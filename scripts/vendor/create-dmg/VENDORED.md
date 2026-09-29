# Vendored: create-dmg

Source: https://github.com/create-dmg/create-dmg
Pinned tag: `v1.3.0`
Pinned commit: `a2b71d0fda6d0df2a86dc7f67082d4d73e84c59f`
License: MIT (see `LICENSE` in this directory)

## Why vendored rather than installed

`scripts/sign_macos.sh` and `scripts/build_mac.sh` use this to lay out the
release DMG (background image, icon positions, window size) the way the
project's own build tools are pinned elsewhere — `ALBIS_PYINSTALLER_VERSION`,
the AppImage tool's version+checksum in `scripts/install_appimagetool.sh`, the
Docker base image digest. `brew install create-dmg` would work on a GitHub
Actions macOS runner, but it installs whatever Homebrew resolves *that day*,
which is exactly the kind of drift those other pins exist to avoid. Vendoring
the script's actual source (it's ~700 lines of bash plus one small AppleScript
template — no compiled binary, nothing to build) makes the pin reviewable in
the same diff as everything else, the same way `frontend/vendor/` already
vendors third-party frontend source rather than fetching it at build time.

This tool runs only on the packaging machine. It never ships inside
`ALBIS.app`, the DMG's contents, or any distributed artifact — the same
category as PyInstaller and the AppImage tool, neither of which appears in
`THIRD_PARTY_LICENSES.md` for that reason (see that file's own scope note).

## Files

- `create-dmg` — the tool itself, executable.
- `support/template.applescript` — the Finder-scripting template it reads
  relative to its own path (via `.this-is-the-create-dmg-repo`, below).
- `support/eula-resources-template.xml` — only used by the `--eula` flag,
  which this project does not pass; kept so this is a faithful, complete
  vendoring of the upstream `support/` directory rather than a partial one.
- `.this-is-the-create-dmg-repo` — a sentinel the script checks for, so it
  resolves `support/` next to itself instead of a Homebrew prefix path.
- `LICENSE` — upstream's MIT license, verbatim.

## Updating the pin

1. Pick a new upstream tag and note its commit (`git ls-remote
   --tags https://github.com/create-dmg/create-dmg`, or the GitHub API).
2. Replace `create-dmg`, `support/template.applescript` and
   `support/eula-resources-template.xml` with that tag's versions, and update
   `LICENSE` if it changed.
3. Update the tag/commit above.
4. Re-run the local prototype in `docs/RELEASE_CHECKLIST.md`'s DMG layout
   section before trusting it in CI — a new release has, in the past, changed
   flag names or Finder-scripting behavior.
