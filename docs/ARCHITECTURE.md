# ALBIS Architecture

This document explains how ALBIS is structured internally and how data flows through the system.

Related docs: [Developer Guide](DEVELOPER_GUIDE.md) · [Code Map](CODE_MAP.md) · [API Contracts](API_CONTRACTS.md)

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

#### Middleware stack

Registered in `backend/app.py`, listed from innermost (closest to the router) out:

1. `RemoteGZipMiddleware` (`backend/response_compression.py`) — gzips responses for
   remote clients only. ALBIS is local-first, so under the default `auto` mode a
   loopback client is never compressed and pays nothing.
2. `log_requests` — request/response logging with severity by status and latency.
3. `_static_cache_policy` — `no-store` for entry documents, `no-cache` for module
   assets so `StaticFiles` can answer an unchanged file with a bodyless `304`.

**Registration order is load-bearing.** Starlette inserts each added middleware at
the head of the stack, so the *first* registered ends up innermost. Both items 2
and 3 are `BaseHTTPMiddleware`, which re-emits its response as a stream — and a
gzip layer placed outside one can never observe a body length, so it would
compress every small JSON response regardless of its `minimum_size` threshold.
Compression must therefore be registered before either of them.

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
