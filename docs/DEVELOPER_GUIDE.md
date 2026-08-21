# Developer Guide

This guide is for developers who want to build, test, and contribute to ALBIS.
For general contribution guidelines, see [`CONTRIBUTING.md`](../CONTRIBUTING.md).

Companion references:

- [Architecture](ARCHITECTURE.md) — runtime components and data flows.
- [Code Map](CODE_MAP.md) — backend/frontend code navigation guide.
- [API Contracts](API_CONTRACTS.md) — stable HTTP contract reference.

## Prerequisites

- **Python 3.13** (see `.python-version`). This is the floor and the ceiling: 3.13 is what CI runs and what the release artifacts bundle. 3.14 is not supported yet because the pinned `numpy` has no 3.14 wheels; that upper bound comes off when numpy is bumped.
- **Node.js >= 22.22** (see `package.json` `engines`) for frontend linting, tests, and the i18n audit. CI runs Node 24.
- **git**, plus a C/build toolchain for any wheels that need compilation.

The frontend has **no build step**: it is plain ES modules served statically by the backend, so after editing `frontend/**` you just reload the browser.

## Run (backend + frontend)

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r backend/requirements.txt
python backend/app.py
```

Default config uses `server.port: 0` (random free port). Open the URL printed by Uvicorn at startup.

## Developer Quality Gates

Install dev tooling:

```bash
pip install -r requirements-dev.txt
npm ci
```

Run local checks:

```bash
ruff check backend albis_launcher.py tests scripts test_scripts
black --check tests scripts test_scripts
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest --cov=backend --cov-report=term-missing --cov-report=xml --cov-fail-under=77
npm run lint:js
npm run test:js
npm run review:i18n
```

`npm run test:js` executes Vitest in `jsdom`. The repository requires Node `>=22.22` (jsdom 30 depends on `undici` 8, which needs `webidl.util.markAsUncloneable` from Node 22); the script pins `node@24` so local runs match CI regardless of the Node on your PATH.

**`npm ci` itself must run on Node `>=22.22`** — no shim can cover it, because npm is the process doing the install. `.npmrc` sets `engine-strict=true` so an older Node fails immediately with `EBADENGINE` instead of producing a tree that installs cleanly and then dies at test time: an older npm silently skips optional platform packages such as `@rolldown/binding-<platform>` (pulled in by Vitest), and the run fails with `Cannot find module '@rolldown/binding-…'`. If you hit that error, reinstall with a current Node rather than debugging Vitest.

Translation review workflow:

- Guide: `docs/I18N_REVIEW_GUIDE.md`
- Shared glossary: `docs/I18N_GLOSSARY.csv`
- Audit command: `npm run review:i18n`

Optional pre-commit hooks:

```bash
pre-commit install
pre-commit run --all-files
```

The hooks run the same tool versions CI does — `requirements-dev.txt` pins them
and `.pre-commit-config.yaml` names the same numbers, with a test that fails when
the two drift apart. A hook that passes while CI fails is worse than no hook, so
bump both in one commit.

Formatting is black's, and only black's. `ruff` lints; `ruff format` is not part
of the setup, because it disagrees with black on this codebase and CI gates on
black.

CI runs on GitHub Actions.

## Development Tips

Faster inner loop while working:

- **Run a single backend test:** `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest tests/test_config.py -k normalize -q`.
- **Run a single frontend test file:** `npm run test:js -- frontend/tests/<file>.test.js`.
- **Auto-reload the backend on code changes:** set `"reload": true` under `server` in `albis.config.json`, then run `python backend/app.py`.
- **Verbose logs:** set `"level": "DEBUG"` under `logging` in `albis.config.json`. Backend logs go to `<resolved log dir>/albis.log`; frontend warnings/errors are forwarded to the backend via `/api/client-log`.
- **Pin the port** (handy when iterating against a fixed URL or an external producer): set `"port": <fixed>` under `server`.

Where to start reading the code:

1. `frontend/app.js` — the composition root; shows how everything is wired together.
2. [Code Map](CODE_MAP.md) — locate the controller/service/route owning a given behavior.
3. [Architecture](ARCHITECTURE.md) — follow a request end-to-end through the data flows.

## Architecture

ALBIS uses a server-client architecture:

- Backend server (FastAPI + Python):
  Loads detector/image data, handles monitor streams, computes metadata/analysis, and exposes REST endpoints.
- Frontend client (browser UI):
  Runs in the browser, renders images/overlays, and interacts with the backend over HTTP.
- Local deployment model:
  The backend typically runs on the same machine as the user, and the UI connects to `http://localhost:<port>`.

For runtime components, key state, and the detailed open/monitor/remote/handoff/series data flows, see [Architecture](ARCHITECTURE.md). For a file-level navigation map, see [Code Map](CODE_MAP.md). For the HTTP contract, see [API Contracts](API_CONTRACTS.md).

### Frontend Composition Root (Current)

Frontend code is now intentionally split by responsibility:

- `frontend/app.js`:
  - Composition root only.
  - Wires controllers/bindings together and keeps stable wrapper function names for initialization-order safety.
- `frontend/modules/*_controller.js`:
  - Domain/state logic (autoload, playback, metadata, ROI, overlays, rendering, export, layout/chrome).
- `frontend/modules/*_bindings*.js`:
  - DOM event wiring only.
  - Consume callback contracts from `app.js` wrappers and context factories.
- `frontend/modules/app_binding_contexts.js`:
  - Shapes large element/callback maps passed into binding bootstraps.
  - Used to keep `app.js` readable and prevent accidental callback/key drift.

### Frontend Mental Model (for new contributors)

The frontend has no framework. It is a single page wired together by hand, organized in layers. Read it top-to-bottom as: *state → pure helpers → controllers → bindings → composition root*, with a few cross-cutting singletons available everywhere.

```text
            ┌───────────────────────────────────────────────┐
 DOM events │  *_bindings.js   (attach listeners, no logic)  │
            └───────────────┬───────────────────────────────┘
                            │ call methods / app callbacks
            ┌───────────────▼───────────────────────────────┐
 domain     │  *_controller.js (own behavior; read/write     │
 logic      │                   state; touch the DOM)        │
            └───────────────┬───────────────────────────────┘
                            │ use
            ┌───────────────▼───────────────┐   ┌─────────────────────────┐
 pure       │  *_engine / *_utils (no DOM,   │   │ singletons (import directly│
 helpers    │   no state — easy to unit test)│   │  anywhere): i18n.t,        │
            └───────────────────────────────┘   │  toast.notify*, dialogs.*  │
                            ▲                     └─────────────────────────┘
            ┌───────────────┴───────────────────────────────┐
 wiring     │  app.js (composition root): owns `state`,      │
            │  grabs DOM refs, builds wrapper callbacks +     │
            │  context maps, instantiates everything in order │
            └─────────────────────────────────────────────────┘
```

- **`state.js`** defines the mutable state objects (`state`, `roiState`, `analysisState`) that `app.js` creates once and passes into controllers. Controllers read and mutate them; avoid hidden DOM-coupled state.
- **Pure helpers** (`roi_stats_engine.js`, `intensity_scale_utils.js`, `ring_geometry_utils.js`, `file_type_utils.js`, …) take inputs and return outputs with no DOM or `state` access. New math/transform logic belongs here so it can be unit-tested directly.
- **Controllers** own a domain (playback, ROI, rendering, …), hold references to their DOM elements and `state`, and expose methods.
- **Bindings** only attach DOM event listeners and forward to controller methods or `app.js` callbacks. If you find logic in a `*_bindings.js` file, it probably belongs in a controller.

### Frontend Wiring: two patterns

There are exactly two ways a module gets what it needs. Pick by whether the dependency is per-instance or app-wide.

1. **Callback injection (the default).** A controller/binding factory takes `{ apiBase, state, elements, callbacks }`. `app.js` constructs the `elements` and `callbacks` maps (often via `app_binding_contexts.js`) and passes them in. Use this for anything that touches `state`, specific DOM nodes, or other controllers.

   - **Why the wrapper functions in `app.js`?** Controllers are created in sequence, and some need to call into others that are created *later*. `app.js` exposes small **named function declarations** (e.g. `setStatus`, `exportFullImage`) that are hoisted and stable, then passes *those* as callbacks. This decouples wiring from construction order — a controller can call `setStatus` before the controller that ultimately backs it exists, without capturing an `undefined`. When adding a callback, add/keep a stable wrapper rather than passing a controller method directly.

2. **Directly-imported singletons.** A few cross-cutting concerns are stateless and needed from many places, so they are plain module singletons you `import` directly instead of threading through every callback bag:
   - `i18n.js` → `t(key, vars)` for translated strings.
   - `toast.js` → `notifyError/notifySuccess/notifyWarning/notifyInfo` for transient notifications.
   - `dialogs.js` → `showPromptDialog/showConfirmDialog` for promise-based modal prompts.

   Use a singleton only when the module is self-contained (no `state`, no per-instance config) and broadly used. If it needs app state or specific elements, use callback injection instead.

### Worked example: add a new toolbar action

To add, say, a "Reset view" button:

1. **DOM** — add the `<button id="reset-view">` in `frontend/index.html`, with `data-i18n` for its label and styles in `frontend/style.css`.
2. **Behavior** — implement it on the relevant controller (here `overview_viewport_controller.js`), exposing a `resetView()` method.
3. **Wiring** — in `app.js`, grab the element (`const resetViewBtn = document.getElementById("reset-view")`), and if other modules need to trigger it, add a stable wrapper (`function resetView() { overviewViewportController?.resetView(); }`).
4. **Event** — bind the click in the appropriate `*_bindings.js` (or its context map), calling the controller method / wrapper.
5. **Strings** — add the i18n key to **all** locales in `frontend/locales/` (keep key parity — see `frontend/tests/locale_integrity.test.js`) and surface user feedback with `setStatus(..., { tone })` and/or `notify*`.
6. **Tests** — add a `frontend/tests/*.test.js` spec (Vitest + jsdom); keep pure logic in a helper so it can be tested without the DOM.

### Refactor Boundaries (SOLID/DRY)

- Backend shared services:
  - `backend/services/path_policy.py`: shared path resolution and extension filtering policy.
  - `backend/services/os_actions.py`: shared open-path and native file/folder picker helpers.
  - `backend/routes/binary_response_utils.py`: reusable OpenAPI binary response docs + header builders.
- HDF5 service:
  - `backend/services/hdf5_stack.py` now uses a shared traversal resolver for dataset/node resolution.
  - Unit/scalar conversion logic is extracted into `backend/services/hdf5_units.py`.
- Series summing:
  - `backend/services/series_summing.py` keeps orchestration/state in service methods and uses shared internal reduction steps for HDF5 and image-series flows.
- Frontend ROI split:
  - `frontend/modules/roi_stats_engine.js`: pure ROI stats/histogram/mask logic.
  - `frontend/modules/roi_plot_renderer.js`: ROI plot rendering.
  - `frontend/modules/roi_csv_export.js`: ROI CSV payload construction.
  - `frontend/modules/roi_stats_controller.js` remains the wiring/orchestration layer.
- Frontend file-type DRY utilities:
  - `frontend/modules/file_type_utils.js` centralizes HDF5/header/series capability checks used from `frontend/app.js`.

## Packaging (PyInstaller)

ALBIS can be bundled into a **platform‑native app** (no Python required) using PyInstaller.

Build scripts for Linux/Windows now use an **isolated build venv by default** (`.build-venv-linux` / `.build-venv-windows`) to keep artifacts reproducible and avoid accidental dependency bloat from the host Python environment.

### Build (macOS)

```bash
./scripts/build_mac.sh
```

This produces versioned artifacts in `dist/`, e.g.:

- `ALBIS-macos-<arch>-v<version>-<commit>.zip`
- `ALBIS-macos-<arch>-v<version>-<commit>.dmg`

`build_mac.sh` also attempts to create a macOS `.app` bundle with icon support (from `frontend/ressources/icon.png`).
DMG images include an `Applications` shortcut for drag-and-drop installation.

To sign on build, set your Developer ID certificate and password before running the build:

```bash
export MACOS_SIGN_CERT_PATH="/path/to/your/albis-dev-id.p12"
export MACOS_SIGN_CERT_PASSWORD="<p12-password>"
export MACOS_SIGNING_IDENTITY="Developer ID Application: Your Name Or Company (TEAMID)"  # optional if the p12 contains a single signing identity
./scripts/build_mac.sh
```

Optional notarization env vars:

- `APPLE_ID`
- `APPLE_TEAM_ID`
- `APPLE_APP_SPECIFIC_PASSWORD`

If the signing env vars are set, `build_mac.sh` signs the `.app`, creates a signed `.dmg`, and notarizes/staples the macOS artifacts when Apple credentials are present. The release `.zip` is rebuilt after stapling so the zipped `.app` carries the notarization ticket.

Public tag releases require both signing and notarization secrets. CI rejects incomplete macOS signing configuration rather than publishing macOS artifacts that Gatekeeper would report as unidentified or potentially malicious.

### Build (Linux)

```bash
./scripts/build_linux.sh
./scripts/package_linux_appimage.sh
```

Example output:

- `ALBIS-linux-<arch>-v<version>-<commit>.tar.gz`
- `ALBIS-<version>-<appimage-arch>.AppImage`
- `install_linux_appimage.sh` and `uninstall_linux.sh` (kept in `scripts/`; release workflow bundles them with the AppImage)

AppImage assets use AppImage-standard architecture tags such as `x86_64`.

Optional build env controls:

- `ALBIS_BUILD_ISOLATED=0` uses your current Python environment instead of the build venv.
- `ALBIS_BUILD_CLEAN_VENV=0` reuses an existing build venv.
- `ALBIS_BUILD_VENV=/custom/path` overrides the build venv location.

`scripts/package_linux_appimage.sh` requires `appimagetool` on `PATH`.

Public GitHub Releases publish a Linux bundle:

- `ALBIS-<version>-<appimage-arch>-appimage-bundle.tar.gz`
  - includes `.AppImage`, `install_linux_appimage.sh`, and `uninstall_linux.sh`
  - release also includes standalone `ALBIS-<version>-<appimage-arch>.AppImage` and `ALBIS-linux-<arch>-v<version>-<commit>.tar.gz`

Recommended local desktop integration (user scope, AppImage):

```bash
./scripts/install_linux_appimage.sh dist/ALBIS-<version>-<appimage-arch>.AppImage
```

Manual tarball integration (user scope):

```bash
./scripts/install_linux.sh
```

Both installation scripts integrate ALBIS under `~/.local` (launcher + desktop entry + icon).

To remove it again (default keeps user data/config):

```bash
./scripts/uninstall_linux.sh
```

To also remove `~/ALBIS-data` and `~/.config/albis`:

```bash
./scripts/uninstall_linux.sh --purge-user-data
```

### Build (Windows)

```powershell
.\scripts\build_windows.ps1
.\scripts\package_windows_innosetup.ps1
```

Example output:

- `ALBIS-windows-<arch>-v<version>-<commit>.zip`
- `ALBIS-Setup-windows-<arch>-v<version>-<commit>.exe`

Windows signing setup and low-cost certificate options are covered in [`docs/WINDOWS_SIGNING.md`](WINDOWS_SIGNING.md).

Optional build env controls:

- `$env:ALBIS_BUILD_ISOLATED = "0"` uses your current Python environment.
- `$env:ALBIS_BUILD_CLEAN_VENV = "0"` reuses an existing build venv.
- `$env:ALBIS_BUILD_VENV = "C:\\path\\to\\venv"` overrides the build venv location.

Cross-target naming controls (all platforms):

- `ALBIS_TARGET_OS=<linux|windows|macos>` overrides normalized target OS tag.
- `ALBIS_TARGET_ARCH=<x64|arm64|...>` overrides normalized target architecture tag.

Public tag releases require the following signing/notarization environment variables in CI:

- macOS: `MACOS_SIGN_CERT_B64`, `MACOS_SIGN_CERT_PASSWORD`, `APPLE_ID`, `APPLE_TEAM_ID`, `APPLE_APP_SPECIFIC_PASSWORD`
  - optional: `MACOS_SIGNING_IDENTITY` to force a specific imported identity; otherwise the first Developer ID Application identity from the `.p12` is used
  - `MACOS_SIGN_CERT_B64` should contain the base64-encoded `.p12` payload
  - the `.p12` must contain a `Developer ID Application` certificate with its private key
- Windows: either Azure Artifact Signing variables/secrets or the legacy PFX variables
  - Azure variables: `AZURE_ARTIFACT_SIGNING_ENDPOINT`, `AZURE_ARTIFACT_SIGNING_ACCOUNT`, `AZURE_ARTIFACT_SIGNING_CERT_PROFILE`
  - Azure secrets: `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID`
  - legacy PFX secrets: `WINDOWS_SIGN_CERT_B64`, `WINDOWS_SIGN_CERT_PASSWORD`, `WINDOWS_SIGN_TIMESTAMP_URL`
  - when configured, the Windows pipeline signs `dist/ALBIS/ALBIS.exe`, the setup `.exe`, and the generated `unins*.exe`
- Linux (GPG detached signatures): `LINUX_GPG_PRIVATE_KEY_B64`, `LINUX_GPG_PASSPHRASE`, `LINUX_GPG_KEY_ID`
  - `LINUX_GPG_PRIVATE_KEY_B64` accepts base64-encoded private key data (recommended), a raw ASCII-armored private key block, or an escaped armored block using `\n`.

On a local Mac, you can bootstrap the macOS GitHub secrets in one step:

```bash
./scripts/bootstrap_macos_signing_ci.sh
```

The bootstrap script auto-detects `~/Documents/albis-dev-id.p12` when present, validates the certificate by importing it into a temporary keychain, infers `MACOS_SIGNING_IDENTITY`, and uploads the macOS signing secrets to the current GitHub repository via `gh`.

If `APPLE_ID`, `APPLE_TEAM_ID`, and `APPLE_APP_SPECIFIC_PASSWORD` are available from command-line flags, the current shell, or recent shell history, it uploads those too; otherwise GitHub CI still signs macOS artifacts and skips notarization.

The Inno installer creates Start Menu entries for:

- `ALBIS`
- `Open Logs`
- `Open Data Folder`
- `Edit Config`

Installer defaults:

- Per-user install scope under `%LOCALAPPDATA%\Programs\ALBIS`.
- Interactive installs show the standard Inno Setup destination page, so users can choose a custom install directory.
- Windows Add/Remove Programs uses the stable `AppId=ALBIS`, shows the ALBIS icon, and links support/updates to the GitHub project.
- Installer and uninstaller try to stop a running `ALBIS.exe` gracefully before falling back to forced termination, so upgrades and removals work better for the background-process model.
- No admin rights required (`PrivilegesRequired=lowest`).
- Portable `.zip` remains available from local builds and the `Build Artifacts` workflow.

Public GitHub Releases publish architecture-specific install + portable assets:

- macOS: `ALBIS-macos-arm64-*` and `ALBIS-macos-x64-*` (`.dmg` + `.zip`)
- Windows: `ALBIS-Setup-windows-x64-*` + `ALBIS-windows-x64-*.zip`
- Linux: `ALBIS-linux-x64-*.tar.gz`, `ALBIS-*-x86_64.AppImage`, and `ALBIS-*-x86_64-appimage-bundle.tar.gz`

Public release inputs are pinned:

- runtime dependencies in `backend/requirements.txt`
- PyInstaller via the build scripts
- Docker base image by digest
- AppImage tooling by explicit version + checksum in CI

### Output

The unpacked app payload is created under `dist/ALBIS/` (and on macOS additionally `dist/ALBIS.app`).
Use `albis.config.json` to change data path, host/port, logging, and launcher behavior.

## Versioning and Releases

- Repository release version source of truth: `VERSION`
- Build metadata helper: `scripts/version_info.py`
- Human-readable release history: `CHANGELOG.md`
- Tag format for releases: `v<version>` (for example `v1.0.0`)
- Release execution checklist (including workflow dry-run): `docs/RELEASE_CHECKLIST.md`
