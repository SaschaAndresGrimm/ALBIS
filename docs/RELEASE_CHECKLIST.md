# ALBIS Release Checklist

This checklist is intended for production releases, including `v1.0.0`.

## 1. Prepare Release Content

- Ensure `main` is green and up to date.
- Confirm `VERSION` contains the target release version (for example `1.0.0`).
- Finalize `CHANGELOG.md`:
  - Move release items from `Unreleased` into a dated version section.
  - Update compare/release links at the bottom.
- Confirm README examples and artifact naming use `<version>` placeholders or current values.
- Confirm runtime/build input bumps are explicit and reviewed:
  - `backend/requirements.txt`
  - pinned Docker base image digest
  - pinned AppImage tool version/checksum

## 2. Run Local Quality Gates

```bash
ruff check backend tests scripts test_scripts
black --check tests scripts test_scripts
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest --cov=backend --cov-report=term-missing --cov-report=xml --cov-fail-under=50
npm run lint:js
npm run test:js
```

## 3. Perform Release Workflow Dry-Run

Use this to verify packaging and checks before tagging:

1. Open GitHub Actions and run the `Release` workflow manually (`workflow_dispatch`) on `main`.
2. Verify `verify`, `build_linux`, `build_windows`, and `build_macos` jobs pass.
3. Download workflow artifacts and inspect:
   - Linux x64 contains:
     - `ALBIS-linux-x64-v<version>-<commit>.tar.gz`
     - `ALBIS-<version>-x86_64.AppImage`
     - `ALBIS-<version>-x86_64-appimage-bundle.tar.gz`
     - when `LINUX_GPG_PRIVATE_KEY_B64` is configured: matching `.sig` files for each Linux artifact
   - Windows x64 contains:
     - `ALBIS-windows-x64-v<version>-<commit>.zip`
     - `ALBIS-Setup-windows-x64-v<version>-<commit>.exe`
   - macOS arm64 contains:
     - `ALBIS-macos-arm64-v<version>-<commit>.zip`
     - `ALBIS-macos-arm64-v<version>-<commit>.dmg`
   - macOS x64 contains:
     - `ALBIS-macos-x64-v<version>-<commit>.zip`
     - `ALBIS-macos-x64-v<version>-<commit>.dmg`
   - Linux tarballs include `v<version>-<commit>`; AppImage assets use the standard `ALBIS-<version>-x86_64*.AppImage` naming expected by AppImageHub.
4. Confirm install-path smoke checks passed:
   - Windows installer silent install -> launch -> uninstall
   - macOS mounted DMG app launch smoke
5. If macOS signing secrets were configured, confirm the workflow logs show `.app` signing, `.dmg` signing, and notarization/stapling.

Note: on manual dispatch from a branch, the `publish` job is intentionally skipped.

### Optional: Artifacts-Only Branch Build

Use the `Build Artifacts` workflow for branch testing without creating a tag/release.

1. Open GitHub Actions and run `Build Artifacts` on your target branch.
2. Optionally enable `run_verify` to execute quality gates before packaging.
3. Download artifacts (`artifacts-linux-x64-*`, `artifacts-windows-x64-*`, `artifacts-macos-arm64-*`, `artifacts-macos-x64-*`) and inspect:
   - Linux x64 includes `.tar.gz`, `.AppImage`, appimage bundle `.tar.gz`, `install_linux_appimage.sh`, `uninstall_linux.sh`, and `SHA256SUMS.txt`.
   - Windows x64 includes `.zip`, setup `.exe`, and `SHA256SUMS.txt`.
   - macOS arm64/x64 each include `.zip`, `.dmg`, and `SHA256SUMS.txt`.
   - Local install/run behavior is still correct.

## 4. Create and Publish Release Tag

```bash
git checkout main
git pull --ff-only
git tag -a v1.0.0 -m "ALBIS v1.0.0"
git push origin v1.0.0
```

Expected result:

- `Release` workflow runs from the tag.
- Tag/version check passes (`v1.0.0` equals `VERSION` content `1.0.0`).
- Signing/notarization secrets are present for Linux, Windows, and macOS before tagging.
  - macOS requires the base64-encoded `.p12` (`MACOS_SIGN_CERT_B64`) and its password (`MACOS_SIGN_CERT_PASSWORD`), plus notarization credentials.
- GitHub Release is published with:
  - Linux x64: `ALBIS-linux-x64-v<version>-<commit>.tar.gz` + `ALBIS-<version>-x86_64.AppImage` + `ALBIS-<version>-x86_64-appimage-bundle.tar.gz`
  - Linux x64 signatures (`.sig`) when Linux GPG signing secrets are configured
  - Windows x64: setup `.exe` + portable `.zip`
  - macOS arm64 and x64: `.dmg` + portable `.zip`
  - `SHA256SUMS.txt`

## 5. Post-Release Verification

- Confirm release page includes expected assets and `SHA256SUMS.txt`.
- Confirm each Linux artifact has a sibling `.sig` asset.
- Complete and attach the manual release sign-off matrix for each supported desktop platform:
  - download artifact
  - verify checksum/signature
  - install
  - first launch
  - open sample data
  - close/relaunch
  - uninstall
  - note any Gatekeeper/SmartScreen/signature prompts
- Move next development cycle notes into `Unreleased` in `CHANGELOG.md`.

## 6. Refresh AppImageHub Listing

- For the first AppImage release, or when AppStream metadata changes materially, open a PR against `AppImage/appimage.github.io`.
- Add or update a file under `data/` that contains only `https://github.com/SaschaAndresGrimm/ALBIS`.
- Wait for the AppImageHub catalog checks to pass before merging.
