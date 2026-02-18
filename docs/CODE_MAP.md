# ALBIS Code Map

This file is a quick navigation guide for contributors.

## Core Files

- `backend/app.py`: API endpoints, file IO, metadata extraction, monitor integration, series-summing jobs.
- `backend/config.py`: config discovery, merge, and typed access helpers.
- `backend/image_formats.py`: detector image format readers (`tiff/cbf/cbf.gz/edf`) and metadata/header parsers.
- `frontend/app.js`: UI controller, renderer orchestration, overlays, interaction logic.
- `frontend/index.html`: UI structure and section anchors used by `app.js`.
- `frontend/style.css`: ALBULA-style layout, panel systems, responsive/touch behavior.
- `albis_launcher.py`: packaged app startup (uvicorn server + browser launch).

## Backend Regions (`backend/app.py`)

- Path and file safety:
  - `_safe_rel_path`, `_resolve_file`, `_resolve_dir`, `_resolve_image_file`
- File discovery and watch support:
  - `_iter_entries`, `_scan_files`, `_scan_folders`, `_latest_image_file`
- SIMPLON helpers:
  - `_simplon_base`, `_simplon_fetch_monitor`, `_simplon_fetch_pixel_mask`
- Image format and metadata helpers:
  - `backend/image_formats.py`: `_read_tiff`, `_read_cbf`, `_read_edf`, `_pilatus_header_text`, `_read_tiff_bytes_with_simplon_meta`
- Remote stream helpers:
  - `_remote_read_image_bytes`, `_remote_extract_metadata`, `_remote_store_frame`
- HDF5 inspection helpers:
  - `_dataset_info`, `_walk_datasets`, `_resolve_node`, `_resolve_dataset_view`
- Multi-file linked stack support:
  - `_aggregate_linked_stack_datasets`, `_resolve_group_linked_stack`
- Analysis and math utilities:
  - `_read_threshold_energies`, `_read_scalar`, unit conversion helpers
- Series summing:
  - `_run_series_summing_job`, `_iter_sum_groups`, `_mask_slices`

Endpoint clusters:

- Health/logging: `/api/health`, `/api/client-log`, `/api/open-log`
- File selection and loading: `/api/files`, `/api/folders`, `/api/frame`, `/api/image`
- HDF5 browser: `/api/hdf5/*`
- Analysis: `/api/analysis/*`
- SIMPLON monitor: `/api/simplon/*`
- Remote stream ingest: `/api/remote/v1/*`

## Frontend Regions (`frontend/app.js`)

- App state and DOM references:
  - Top of file: `const ...` DOM lookups + `state`, `analysisState`, `roiState`
- Renderer:
  - `createWebGLRenderer`, `createCpuRenderer`, `initRenderer`, `applyFrame`
- Histogram and contrast:
  - `computeHistogram`, `drawHistogram`, `drawColorbar`, `computeAutoLevels`
- ROI:
  - `updateRoiStats`, `drawRoiOverlay`, `drawRoiPlot`, ROI edit helpers
- Analysis overlays:
  - Resolution rings: `getResolutionAtPixel`, `drawResolutionOverlay`
  - Peak finder: `detectPeaks`, `runPeakFinder`, `drawPeakOverlay`
- Interaction:
  - Zoom/pan: `setZoom`, `zoomAt`, wheel/touch/pointer handlers
  - Menus/shortcuts: `openMenu`, `closeMenu`, `handleShortcut`
- Data source modes:
  - Autoload/watch folder/SIMPLON/Remote sections around `updateAutoloadUI` and polling helpers
  - Remote polling + metadata application: `autoloadRemoteTick`, `fetchRemoteMeta`, `applyRemoteMeta`

## Config and Packaging

- Config defaults and loading:
  - `backend/config.py`
- Build scripts:
  - `scripts/build_mac.sh`, `scripts/build_linux.sh`, `scripts/build_windows.ps1`
- Packaging helpers:
  - `scripts/package_mac_dmg.sh`, `scripts/package_linux_appimage.sh`, `scripts/package_windows_innosetup.ps1`
- Version metadata helper:
  - `scripts/version_info.py`
- PyInstaller definition:
  - `ALBIS.spec`

## Where To Add New Features

- New backend capability:
  - add endpoint and orchestration in `backend/app.py`
  - prefer parser/reader logic in `backend/image_formats.py` or dedicated helper modules
  - update `docs/ARCHITECTURE.md` and README if user-facing
- New frontend tool panel control:
  - add markup in `frontend/index.html`
  - add styles in `frontend/style.css`
  - wire logic in `frontend/app.js`
- New analysis overlay:
  - backend: provide parameters/data if needed
  - frontend: add scheduling + draw function + panel controls
