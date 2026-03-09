# ALBIS Release Checklist

This checklist is intended for production releases, including `v1.0.0`.

## 1. Prepare Release Content

- Ensure `main` is green and up to date.
- Confirm `VERSION` contains the target release version (for example `1.0.0`).
- Finalize `CHANGELOG.md`:
  - Move release items from `Unreleased` into a dated version section.
  - Update compare/release links at the bottom.
- Confirm README examples and artifact naming use `<version>` placeholders or current values.

## 2. Run Local Quality Gates

```bash
ruff check backend tests scripts test_scripts
black --check tests scripts test_scripts
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest --cov=backend --cov-report=term-missing --cov-report=xml --cov-fail-under=20
npm run lint:js
```

## 3. Perform Release Workflow Dry-Run

Use this to verify packaging and checks before tagging:

1. Open GitHub Actions and run the `Release` workflow manually (`workflow_dispatch`) on `main`.
2. Verify `verify`, `build_linux`, `build_windows`, and `build_macos` jobs pass.
3. Download workflow artifacts and inspect:
   - Linux/Windows/macOS bundle naming includes `v<version>-<commit>`.
   - `SHA256SUMS.txt` exists and contains hashes for all uploaded artifacts.

Note: on manual dispatch from a branch, the `publish` job is intentionally skipped.

### Optional: Artifacts-Only Branch Build

Use the `Build Artifacts` workflow for branch testing without creating a tag/release.

1. Open GitHub Actions and run `Build Artifacts` on your target branch.
2. Optionally enable `run_verify` to execute quality gates before packaging.
3. Download artifacts (`artifacts-linux-*`, `artifacts-windows-*`, `artifacts-macos-*`) and inspect local install/run behavior.

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
- GitHub Release is published with Linux, Windows, macOS artifacts plus `SHA256SUMS.txt`.

## 5. Post-Release Verification

- Confirm release page includes expected assets and `SHA256SUMS.txt`.
- Validate download/install path on at least one machine per supported platform.
- Move next development cycle notes into `Unreleased` in `CHANGELOG.md`.
