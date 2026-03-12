# Developer Guide

This guide is for developers who want to build, test, and contribute to ALBIS.
For general contribution guidelines, see `CONTRIBUTING.md`.

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
ruff check backend tests scripts test_scripts
black --check tests scripts test_scripts
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest --cov=backend --cov-report=term-missing --cov-report=xml --cov-fail-under=20
npm run lint:js
npm run test:js
```

`npm run test:js` executes Vitest in `jsdom`. The repository requires Node `>=20`; the script currently enforces this via `node@20` to keep local runs reproducible.

Optional pre-commit hooks:

```bash
pre-commit install
pre-commit run --all-files
```

CI runs on GitHub Actions.

## Architecture

ALBIS uses a server-client architecture:

- Backend server (FastAPI + Python):
  Loads detector/image data, handles monitor streams, computes metadata/analysis, and exposes REST endpoints.
- Frontend client (browser UI):
  Runs in the browser, renders images/overlays, and interacts with the backend over HTTP.
- Local deployment model:
  The backend typically runs on the same machine as the user, and the UI connects to `http://localhost:<port>`.

Detailed implementation notes:

- System architecture and data flows: [ALBIS Architecture](#albis-architecture)
- Backend/frontend code navigation map: [ALBIS Code Map](#albis-code-map)
- API contract reference: [ALBIS API Contracts](#albis-api-contracts)

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

### Build (Linux)

```bash
./scripts/build_linux.sh
./scripts/package_linux_appimage.sh
```

Example output:
- `ALBIS-linux-<arch>-v<version>-<commit>.tar.gz`
- `ALBIS-linux-<arch>-v<version>-<commit>.AppImage`
- `install_linux_appimage.sh` and `uninstall_linux.sh` (kept in `scripts/`; release workflow bundles them with the AppImage)

Optional build env controls:
- `ALBIS_BUILD_ISOLATED=0` uses your current Python environment instead of the build venv.
- `ALBIS_BUILD_CLEAN_VENV=0` reuses an existing build venv.
- `ALBIS_BUILD_VENV=/custom/path` overrides the build venv location.

`scripts/package_linux_appimage.sh` requires `appimagetool` on `PATH`.

Public GitHub Releases publish a Linux bundle:
- `ALBIS-linux-<arch>-v<version>-<commit>-appimage-bundle.tar.gz`
  - includes `.AppImage`, `install_linux_appimage.sh`, and `uninstall_linux.sh`
  - release also includes standalone `ALBIS-linux-<arch>-v<version>-<commit>.AppImage` and `.tar.gz` assets

Recommended local desktop integration (user scope, AppImage):

```bash
./scripts/install_linux_appimage.sh dist/ALBIS-linux-<arch>-v<version>-<commit>.AppImage
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

Optional build env controls:
- `$env:ALBIS_BUILD_ISOLATED = "0"` uses your current Python environment.
- `$env:ALBIS_BUILD_CLEAN_VENV = "0"` reuses an existing build venv.
- `$env:ALBIS_BUILD_VENV = "C:\\path\\to\\venv"` overrides the build venv location.

Cross-target naming controls (all platforms):
- `ALBIS_TARGET_OS=<linux|windows|macos>` overrides normalized target OS tag.
- `ALBIS_TARGET_ARCH=<x64|arm64|...>` overrides normalized target architecture tag.

Optional signing/notarization environment variables used by CI:
- macOS: `MACOS_SIGNING_IDENTITY`, `APPLE_ID`, `APPLE_TEAM_ID`, `APPLE_APP_SPECIFIC_PASSWORD`
- Windows: `WINDOWS_SIGN_CERT_B64`, `WINDOWS_SIGN_CERT_PASSWORD`, `WINDOWS_SIGN_TIMESTAMP_URL`
- Linux (optional GPG detached signatures): `LINUX_GPG_PRIVATE_KEY_B64`, `LINUX_GPG_PASSPHRASE`, `LINUX_GPG_KEY_ID`

The Inno installer creates Start Menu entries for:
- `ALBIS`
- `Open Logs`
- `Open Data Folder`
- `Edit Config`

Installer defaults:
- Per-user install scope under `%LOCALAPPDATA%\Programs\ALBIS`.
- No admin rights required (`PrivilegesRequired=lowest`).
- Portable `.zip` remains available from local builds and the `Build Artifacts` workflow.

Public GitHub Releases publish architecture-specific install + portable assets:
- macOS: `ALBIS-macos-arm64-*` and `ALBIS-macos-x64-*` (`.dmg` + `.zip`)
- Windows: `ALBIS-Setup-windows-x64-*` + `ALBIS-windows-x64-*.zip`
- Linux: `ALBIS-linux-x64-*` (`.tar.gz` + `.AppImage` + appimage bundle tarball)

### Output

The unpacked app payload is created under `dist/ALBIS/` (and on macOS additionally `dist/ALBIS.app`).
Use `albis.config.json` to change data path, host/port, logging, and launcher behavior.

## Versioning and Releases

- Repository release version source of truth: `VERSION`
- Build metadata helper: `scripts/version_info.py`
- Human-readable release history: `CHANGELOG.md`
- Tag format for releases: `v<version>` (for example `v1.0.0`)
- Release execution checklist (including workflow dry-run): `docs/RELEASE_CHECKLIST.md`
# ALBIS Architecture

This document explains how ALBIS is structured internally and how data flows through the system.

## Overview

ALBIS is a local server-client application:

- Backend: FastAPI service in `backend/app.py`
- Frontend: Single-page browser UI in `frontend/index.html`, `frontend/app.js`, `frontend/style.css`
- Launcher: local process starter in `albis_launcher.py`

The browser UI calls backend REST endpoints to load frames, metadata, analysis outputs, and live monitor images.

## Runtime Components

### Backend (`backend/app.py`)

Responsibilities:

- Serve static frontend assets.
- Resolve and validate file/folder paths.
- Read HDF5/TIFF/CBF frame data.
- Ingest external frames through the Remote Stream API.
- Bridge JUNGFRAUJOCH preview ZeroMQ streams into the Remote Stream cache.
- Build dataset metadata (shape, dtype, thresholds, masks).
- Execute analysis endpoints (ROI, rings parameters, peak finding helpers, series summing).
- Handle monitor streaming and monitor mask fetching through SIMPLON.
- Maintain operational logging and user-facing health/log endpoints.

Key state:

- Configuration: loaded once from `backend/config.py`.
- Caches: file/folder scan caches and background series-summing job state.
- Logging: rotating logfile plus console output.

### Frontend (`frontend/app.js`)

Responsibilities:

- Act as the composition root for frontend modules.
- Keep wrapper callbacks stable for bindings/controllers to avoid initialization-order regressions.
- Instantiate and connect controller modules (autoload, file/data pipeline, playback, metadata, overlays, ROI, rendering, export, panel/chrome).
- Instantiate binding bootstrap modules and pass context maps.
- Coordinate startup/bootstrap sequence and top-level orchestration flow.

Rendering layers:

- Base image canvas.
- Overlay canvases: pixel labels, ROI, rings, peaks, cursor/hist tooltips.

### Config System (`backend/config.py`)

Load order:

1. Current working directory (`albis.config.json`)
2. Frozen app directory (if packaged)
3. Repository root config
4. User config path (`~/.config/albis/config.json`)

If no config exists:

- Source run: defaults are used.
- Packaged run: defaults are written to `~/.config/albis/config.json`.

### Packaging

Build scripts in `scripts/` generate platform-specific artifacts and include `version + short commit` in names.

- macOS: zip + DMG, app bundle support, icon conversion.
- Linux: tarball + AppImage.
- Windows: zip + Inno Setup installer.

## Main Data Flows

### Open dataset flow

1. Frontend selects file/dataset.
2. Frontend calls:
   - `/api/datasets` for dataset discovery
   - `/api/frame` for frame binary payload
   - `/api/mask` and `/api/analysis/params` for overlays and analysis defaults
3. Frontend decodes frame, updates renderer, histogram, overlays.

### Live monitor flow

1. Frontend switches mode to SIMPLON monitor.
2. Backend requests monitor TIFF payload from detector API.
3. Backend tries to fetch detector pixel mask and applies it consistently.
4. Frontend updates image and status badges (`WAIT/LIVE` and backend health).

### Remote stream flow

1. External producer pushes frame bytes + metadata to `POST /api/remote/v1/frame`.
2. Backend decodes payload (`raw`, TIFF, CBF/CBF.GZ, EDF) and stores latest frame per `source_id`.
3. Frontend in `Remote Stream` mode polls:
   - `GET /api/remote/v1/latest` for new frame bytes
   - `GET /api/remote/v1/meta` for enriched metadata (`peak_sets`, display fields)
4. Frontend updates frame, ring parameters, remote metadata panel, and peak overlays.

### JUNGFRAUJOCH preview flow

1. Frontend switches mode to `JUNGFRAUJOCH Preview`.
2. Backend starts a ZeroMQ SUB worker (`/api/jfjoch/preview/start`) and subscribes to preview PUB frames.
3. Worker decodes CBOR image messages, maps `spots` to `peak_sets`, and stores snapshots per `source_id`.
4. Frontend polls the existing Remote Stream endpoints (`/api/remote/v1/latest`, `/api/remote/v1/meta`) and renders frames/reflection overlays.

### Handoff flow

1. External producer posts manifest path to `POST /api/handoff/v1/jobs`.
2. Backend resolves manifest output target (`open_path`, optional `dataset`, optional `run_id`) and enqueues a handoff job.
3. Frontend polls `GET /api/handoff/v1/jobs/latest?after_id=...` and applies new jobs by opening the target path/dataset.

### Series summing flow

1. Frontend posts job config to `/api/analysis/series-sum/start`.
2. Backend starts background worker thread and updates in-memory job status.
3. Frontend polls `/api/analysis/series-sum/status`.
4. Backend writes HDF5/TIFF outputs and final status.

## Open-Source Maintainability Notes

- Keep backend endpoints thin and side-effect boundaries explicit.
- Keep frontend state transitions centralized (avoid hidden DOM-coupled state).
- Prefer pure helper functions for math/transforms and test them independently.
- Document every new analysis feature in both:
  - API contract (request/response behavior)
  - UI behavior (controls + defaults + performance impact)
# ALBIS Code Map

This document is a practical navigation guide for contributors.

## Core Entry Points

- `backend/app.py`:
  - FastAPI app bootstrap and middleware.
  - Runtime config/path wiring.
  - Dependency wiring into route modules and services.
- `backend/api_models.py`:
  - Pydantic request/response contracts for the HTTP API.
- `backend/config.py`:
  - Config discovery, parsing, normalization, and typed getters.
- `frontend/app.js`:
  - Frontend composition root and bootstrap wiring.
- `frontend/index.html`:
  - UI structure and mount points referenced by `app.js`.
- `frontend/style.css`:
  - Styling and responsive layout.

## Frontend Module Map

- `frontend/modules/app_binding_contexts.js`:
  - Factory helpers for element/callback context objects used by binding bootstraps and file-browser bootstrap.
- `frontend/modules/main_ui_bindings_bootstrap.js`:
  - Main menu/chrome/data-control binding orchestration.
- `frontend/modules/post_file_picker_bindings.js`:
  - Post-picker interaction binding orchestration (viewer, ROI, viewport, histogram drag, overview, window).
- `frontend/modules/file_data_pipeline_controller.js`:
  - File, dataset, and frame loading pipeline.
- `frontend/modules/file_session_controller.js`:
  - Session teardown and frame application flow.
- `frontend/modules/frame_playback_controller.js`:
  - Frame status, controls, request queueing, and playback timing.
- `frontend/modules/frame_metadata_controller.js`:
  - Autoload folders/files/metadata fetch orchestration.
- `frontend/modules/render_engine_controller.js` + `frontend/modules/intensity_scale_utils.js`:
  - WebGL/CPU renderer setup and intensity/statistics utility stack.
- `frontend/modules/overview_viewport_controller.js`:
  - Pan/zoom/touch/overview interaction model and viewport transforms.
- `frontend/modules/roi_stats_controller.js` + `frontend/modules/roi_interaction_controller.js`:
  - ROI wiring/orchestration and ROI interaction overlay editing.
- `frontend/modules/roi_stats_engine.js` + `frontend/modules/roi_plot_renderer.js` + `frontend/modules/roi_csv_export.js`:
  - Pure ROI compute helpers, ROI plot rendering, and CSV payload generation.
- `frontend/modules/file_type_utils.js`:
  - Shared HDF5/header/series capability checks used across controllers.
- `frontend/modules/overlay_render_controller.js` + `frontend/modules/histogram_render_controller.js`:
  - Overlay and histogram drawing/scheduling.
- `frontend/modules/autoload_*_controller.js`:
  - Autoload mode, settings persistence/UI sync, orchestration, and status/meta control.
- `frontend/modules/file_browser.js`:
  - File browser modal state and filesystem browsing interactions.

## Backend Route Modules

- `backend/routes/system.py`:
  - Health and settings endpoints (`/api/health`, `/api/settings`, logging helpers).
- `backend/routes/files.py`:
  - File/folder discovery, native pickers, browse, autoload latest, uploads.
- `backend/routes/frames.py`:
  - HDF5 frame binary endpoints (`/api/frame`, `/api/mask`) and metadata.
- `backend/routes/handoff.py`:
  - Handoff queue ingest and polling endpoints (`/api/handoff/v1/jobs*`).
- `backend/routes/hdf5.py`:
  - HDF5 inspector endpoints (`/api/datasets`, `/api/hdf5/tree|node|value|search|csv`).
- `backend/routes/analysis.py`:
  - Analysis and series-summing endpoints (`/api/analysis/*`).
- `backend/routes/stream.py`:
  - Single-image decoding, SIMPLON monitor/mask, remote stream ingest/latest/meta, JUNGFRAUJOCH preview controls.

## Backend Services

- `backend/services/hdf5_stack.py`:
  - HDF5 dataset discovery, metadata extraction, path resolution, frame extraction.
- `backend/services/hdf5_units.py`:
  - Pure scalar coercion/unit conversion helpers for detector metadata extraction.
- `backend/services/series_summing.py`:
  - Background job manager for series aggregation operations.
- `backend/services/series_ops.py`:
  - Series grouping/masking helpers used by summing workflows.
- `backend/services/remote_stream.py`:
  - Remote frame decode, metadata normalization, in-memory stream snapshot logic.
- `backend/services/jungfraujoch_preview.py`:
  - ZeroMQ preview subscriber, CBOR/Stream2 decode, and mapping to remote snapshot metadata.
- `backend/services/simplon.py`:
  - SIMPLON endpoint URL/mode helpers and monitor/mask fetch logic.
- `backend/services/path_policy.py`:
  - Shared path safety policy and extension filtering helpers.
- `backend/services/os_actions.py`:
  - Shared desktop open actions and native picker integrations.

## Format and Detector Parsers

- `backend/image_formats.py`:
  - TIFF/CBF/EDF readers, detector metadata parsing, Pilatus header extraction.

## Tests

- `tests/test_api_contracts.py`:
  - API schema/contract assertions (request strictness and OpenAPI response docs).
- `tests/test_api_smoke.py`:
  - End-to-end HTTP smoke tests (health + remote stream flow).
- `tests/test_handoff_api.py`:
  - Handoff route contract and queue-cap behavior checks.
- `tests/test_config.py`:
  - Config normalization and validation edge cases.
- `tests/test_series_helpers.py` and `tests/test_series_summing_service.py`:
  - Series detection and job-level behavior tests.
- `tests/test_remote_stream_helpers.py`:
  - Remote stream metadata and decode helper tests.
- `tests/test_path_policy.py`, `tests/test_binary_response_utils.py`, `tests/test_hdf5_units.py`, `tests/test_os_actions_helpers.py`:
  - Focused utility-level regression coverage for extracted shared modules.
- `tests/test_openapi_key_contract_baseline.py`:
  - Key OpenAPI contract snapshot parity check for compatibility-sensitive endpoints.

## Packaging and Release

- `VERSION`:
  - Single source of truth for release version.
- `scripts/version_info.py`:
  - Build metadata helper reading `VERSION`.
- `ALBIS.spec`:
  - PyInstaller bundle definition.
- `docs/RELEASE_CHECKLIST.md`:
  - Human release procedure and verification checklist.

## Where To Add New Features

- New HTTP endpoint:
  - Add request/response models in `backend/api_models.py`.
  - Implement endpoint in the relevant `backend/routes/*.py` module.
  - Wire dependencies from `backend/app.py` only if new runtime deps are needed.
  - Add/extend contract tests in `tests/test_api_contracts.py`.
- New backend computation/integration:
  - Put reusable logic in `backend/services/*` instead of route handlers.
- New frontend panel/workflow:
  - Add DOM structure in `frontend/index.html`.
  - Implement domain behavior in the relevant `frontend/modules/*` controller/binding module.
  - Keep `frontend/app.js` changes limited to composition wiring/wrapper callbacks/context assembly.
  - Add styles in `frontend/style.css`.
- User-visible behavior changes:
  - Update `README.md`, `CHANGELOG.md`, and relevant docs in `docs/`.
# ALBIS API Contracts

This document describes the stable HTTP contract for ALBIS clients and integrations.

OpenAPI source of truth:

- `GET /openapi.json`

Code source of truth:

- `backend/api_models.py`
- `backend/routes/*.py`

## Contract Principles

- Request bodies use strict Pydantic models (`extra = "forbid"`), so unknown JSON fields are rejected with `422`.
- JSON responses use explicit response models for typed schema generation.
- Binary endpoints explicitly document:
  - media type (`application/octet-stream`),
  - payload format (`string` with `binary` format in OpenAPI),
  - response headers required for decoding.
- `204 No Content` is used when "no current payload" is a valid non-error outcome.

## Binary Endpoints

### Frame and image payloads

- `GET /api/frame`
- `GET /api/mask`
- `GET /api/image`
- `GET /api/simplon/monitor`
- `GET /api/simplon/mask`
- `GET /api/remote/v1/latest`

Common headers:

- `X-Dtype`: NumPy dtype string used to decode raw bytes.
- `X-Shape`: comma-separated dimensions.
- `X-Frame`: frame index (where applicable).

Endpoint-specific headers are documented in OpenAPI for each route:

- `X-Mask-Path` for HDF5 pixel-mask source.
- `X-Simplon-*` for SIMPLON metadata.
- `X-Remote-*` for remote stream metadata and sequence control.

### CSV export

- `GET /api/hdf5/csv`

Response:

- `text/csv`
- `Content-Disposition` attachment header

## Remote Stream Semantics

- `POST /api/remote/v1/frame`: ingest a frame and metadata.
- `GET /api/remote/v1/latest`:
  - returns `200` with raw bytes when a frame is available,
  - returns `204` when no frame is available or no newer frame exists for `after_seq`.
- `GET /api/remote/v1/meta`:
  - returns `200` with `RemoteMetaResponse`,
  - returns `204` when no frame exists for source,
  - returns `409` with `RemoteMetaConflictResponse` when requested `seq` is no longer current.

## Handoff Semantics

- `POST /api/handoff/v1/jobs`: ingest one handoff manifest path and enqueue a typed job payload.
- `GET /api/handoff/v1/jobs/latest`:
  - returns `200` with `HandoffJobResponse` when a newer job than `after_id` exists,
  - returns `204` when no newer handoff job exists.
- Queue behavior:
  - bounded in-memory queue (max 1024 jobs),
  - oldest entries are evicted when full,
  - job IDs remain monotonic.

## JUNGFRAUJOCH Preview Bridge

- `POST /api/jfjoch/preview/start`: start or reconfigure backend ZeroMQ preview subscription.
- `POST /api/jfjoch/preview/stop`: stop active subscription worker.
- `GET /api/jfjoch/preview/status`: return current worker state and latest ingest counters.
- Preview frames are exposed through existing Remote Stream endpoints under configured `source_id`.

## Client Guidance

- Prefer schema-driven clients from `/openapi.json` for JSON endpoints.
- For binary endpoints, always decode payloads using `X-Dtype` + `X-Shape` rather than assumptions.
- Handle `204` as a normal polling outcome for stream-style endpoints.
- Treat `409` on `/api/remote/v1/meta` as a retry signal with current sequence from response body.
