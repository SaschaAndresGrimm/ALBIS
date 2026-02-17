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

- Manage app UI state (dataset/frame/threshold/visualization controls).
- Render image via WebGL2 or CPU fallback.
- Render overlays (ROI, peak markers, resolution rings, histogram, cursor info).
- Manage menu interactions, keyboard shortcuts, panel behavior, and mobile gestures.
- Poll backend endpoints and orchestrate data source modes (file, watch folder, monitor).
- Poll remote frame sources and apply pushed metadata/overlays (resolution + peak sets).

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
- Linux: tarball + AppImage helper.
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
