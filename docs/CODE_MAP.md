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
  - Main UI controller, rendering orchestration, interaction logic.
- `frontend/index.html`:
  - UI structure and mount points referenced by `app.js`.
- `frontend/style.css`:
  - Styling and responsive layout.

## Backend Route Modules

- `backend/routes/system.py`:
  - Health and settings endpoints (`/api/health`, `/api/settings`, logging helpers).
- `backend/routes/files.py`:
  - File/folder discovery, native pickers, browse, autoload latest, uploads.
- `backend/routes/frames.py`:
  - HDF5 frame binary endpoints (`/api/frame`, `/api/preview`, `/api/mask`) and metadata.
- `backend/routes/hdf5.py`:
  - HDF5 inspector endpoints (`/api/datasets`, `/api/hdf5/tree|node|value|search|csv`).
- `backend/routes/analysis.py`:
  - Analysis and series-summing endpoints (`/api/analysis/*`).
- `backend/routes/stream.py`:
  - Single-image decoding, SIMPLON monitor/mask, remote stream ingest/latest/meta.

## Backend Services

- `backend/services/hdf5_stack.py`:
  - HDF5 dataset discovery, metadata extraction, path resolution, frame extraction.
- `backend/services/series_summing.py`:
  - Background job manager for series aggregation operations.
- `backend/services/series_ops.py`:
  - Series grouping/masking helpers used by summing workflows.
- `backend/services/remote_stream.py`:
  - Remote frame decode, metadata normalization, in-memory stream snapshot logic.
- `backend/services/simplon.py`:
  - SIMPLON endpoint URL/mode helpers and monitor/mask fetch logic.

## Format and Detector Parsers

- `backend/image_formats.py`:
  - TIFF/CBF/EDF readers, detector metadata parsing, Pilatus header extraction.

## Tests

- `tests/test_api_contracts.py`:
  - API schema/contract assertions (request strictness and OpenAPI response docs).
- `tests/test_api_smoke.py`:
  - End-to-end HTTP smoke tests (health + remote stream flow).
- `tests/test_config.py`:
  - Config normalization and validation edge cases.
- `tests/test_series_helpers.py` and `tests/test_series_summing_service.py`:
  - Series detection and job-level behavior tests.
- `tests/test_remote_stream_helpers.py`:
  - Remote stream metadata and decode helper tests.

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
  - Add behavior in `frontend/app.js`.
  - Add styles in `frontend/style.css`.
- User-visible behavior changes:
  - Update `README.md`, `CHANGELOG.md`, and relevant docs in `docs/`.
