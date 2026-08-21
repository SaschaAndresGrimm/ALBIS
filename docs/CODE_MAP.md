# ALBIS Code Map

This document is a practical navigation guide for contributors.

Related docs: [Developer Guide](DEVELOPER_GUIDE.md) · [Architecture](ARCHITECTURE.md) · [API Contracts](API_CONTRACTS.md)

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
  - Base-URL normalization (bare host/IP accepted), failure classification, and connection probe.
- `backend/services/path_policy.py`:
  - Shared path safety policy and extension filtering helpers.
- `backend/services/directory_scan.py`:
  - Depth-, entry- and time-bounded directory walks for file/folder discovery and the newest-matching-file search, reporting whether a scan was cut short.
- `backend/services/scan_cache.py`:
  - TTL cache for scan results with single-flight loading, so concurrent pollers share one walk.
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
