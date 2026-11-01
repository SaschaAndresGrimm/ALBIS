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
- `frontend/style.css` + `frontend/styles/NN-area.css`:
  - Styling and responsive layout. `style.css` holds the tokens in `:root`, the fonts and the base rules; `styles/` one file per area, linked from `index.html` in the order of their numbers, which is the cascade. See [UI Guide](UI_GUIDE.md#where-a-style-goes). `frontend/tests/style_guard.test.js` keeps every file on the tokens.

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
- `frontend/modules/autoload_mode_controller.js`, `autoload_orchestration_controller.js`, `autoload_settings_controller.js`, `autoload_status_controller.js`:
  - Autoload: the file-watch and SIMPLON runtime, the start/stop/tick state machine, settings persistence and UI sync, and the status/meta labels.
- `frontend/modules/file_browser.js`:
  - File browser modal state and filesystem browsing interactions.
- `frontend/modules/flatfield.js` + `frontend/modules/flatfield_controller.js` + `backend/services/flatfield.py`:
  - Flatfield correction: frames divided by a flat (counts: × the flat's mean; transmission: 0–1); unusable flat pixels (masked, ≤0, type max, <5 % of the mean) and the frame's flagged pixels become NaN, filled by inpainting when on. "Use current image" saves the frame via `/api/flatfield/save` to `<data>/flatfields/`; exports take `flatfield` (a path) and `flatfield_scale`, writing float32 TIFF. The viewer's frame pipeline (raw → flatfield → inpaint) is `createInpaintView` in inpainting.js; the histogram and auto contrast follow the corrected frame (`refreshDisplayContrast`).
- `frontend/modules/inpainting.js` + `frontend/modules/inpainting_controller.js` + `backend/services/inpainting.py`:
  - Inpainting: gaps and defective (optionally saturated) pixels filled by interpolating across each gap from the median of the five nearest pixels on either side, along rows and columns weighted 1/span. The JS copy fills the frame on screen (once per frame, for the renderer, ROI statistics and PNG/GIF exports); the numpy copy fills TIFF/CBF exports (`inpaint` on `/api/export/data/start` and `/frame`). Both are held to `tests/fixtures/inpainting_cases.json`.
- `frontend/modules/quit_controller.js` + `backend/lifecycle.py` (`IdleWatch`):
  - File → Quit ALBIS (`POST /api/quit`, offered when `/api/health` reports `can_quit`: a launcher-started server on a loopback address) and the idle stop (`launcher.idle_shutdown_min`: a tab counts as open while it holds the `/api/presence` WebSocket, `modules/presence.js`, and every `/api/` request also counts). On macOS the launcher quits the app once its server thread ends, so a stopped ALBIS does not linger in the Dock. Both end the process through `shutdown_controller`, which the launcher registers.
- `frontend/modules/known_detectors.js` + `frontend/modules/detector_picker.js`:
  - The known-detectors list shared by the SIMPLON address fields (Detector tab, SIMPLON monitor): a client of `/api/known-detectors` that shows changes at once and takes the server's answer, the same rules for that instant view; and the dropdown under each field.
- `frontend/modules/detector_control_controller.js`:
  - The beta Detector tab: connect, state and buttons (Acquire, Single image, Live), pre-flight check, progress and result, Acquisition and Data output built from the detector's own descriptors, Commands, Advanced, Activity; command feedback and the status poll.
- `frontend/modules/pixel_mask_edit.js` + `frontend/modules/pixel_mask_edit_controller.js`:
  - Beta Edit on image: which pixels a brush stroke, box, polygon or circle covers, the user mask bit, undo history; the toolbar over the image, its pointer and key handling (ahead of panning while open) and the overlay of pixels changed by hand.
- `frontend/modules/pixel_mask_controller.js`:
  - The beta Data → Pixel Mask section and its counterpart on the Detector tab: the data's own mask (or an edited copy of it, which `mask_cursor_controller.js` keeps in place across files and live frames until Discard edits), saving it, sending it to a detector after a confirmation that counts what changes, Restore of the backup, and opening a detector's mask as a file.
- `frontend/modules/pixel_mask_utils.js`:
  - The section's pure arithmetic and naming: counting a mask by reason (gap, defective, other), what replacing one mask with another changes, and the names masks are saved under.

### State, shared services and small helpers

- `frontend/modules/state.js`:
  - The state objects (`createAppState`, `createRoiState`, `createAnalysisState`); no logic.
- `frontend/modules/http.js`:
  - `fetchJSON` / `fetchJSONWithInit`, and `fetchResponse` for answers that are not one JSON document (bytes, headers, 204): every backend call should go through these, which turn a failure into a translated message.
- `frontend/modules/native_picker.js`:
  - `chooseNativePath`: the system's own file and folder dialogs via `/api/choose-file|folder` (204 a cancel, 409 no native dialog).
- `frontend/modules/app_elements.js`:
  - The elements `index.html` declares by id that `app.js` uses, looked up in one place (`queryAppElements`); see [decision 0005](decisions/0005-app-elements-table-and-stable-wrappers.md).
- `frontend/modules/format_utils.js`, `frontend/modules/typed_array_utils.js`:
  - Pure helpers: how values read in the interface (statistics, plot ticks, energies, inspector values, shortened paths), and frames as the backend sends them (dtype to typed array, shape header, float32 copy, median).
- `frontend/modules/i18n.js`:
  - `t()`, the language preference, and applying `data-i18n*` attributes.
- `frontend/modules/toast.js`, `frontend/modules/dialogs.js`, `frontend/modules/modal_manager.js`:
  - Toasts (`notify*`), promise-based prompt and confirm dialogs in place of `window.prompt`/`confirm`, and the modal stack with its focus handling.
- `frontend/modules/frame_cache.js`, `frontend/modules/buffer_hash.js`, `frontend/modules/transient_frame_load_state.js`:
  - The bounded LRU cache of decoded frames, a hash for telling live frames apart, and the shared in-flight frame-load state.
- `frontend/modules/canvas_fonts.js`, `frontend/modules/range_fill.js`, `frontend/modules/number_stepper.js`, `frontend/modules/zoom_slider.js`, `frontend/modules/control_availability.js`:
  - Drawn-control helpers: the canvas font stack, the filled part of a slider track, the number field's up/down buttons, the logarithmic zoom slider scale, and marking a control unavailable while keeping its reason reachable.
- `frontend/modules/client_log.js`:
  - The page's half of the log: entries queued and posted to `/api/client-log` one at a time; `client_logging_bindings.js` routes `console.warn`/`error` and uncaught errors to it.
- `frontend/modules/handoff_controller.js`:
  - Polls the handoff queue (`/api/handoff/v1/jobs/latest`) and opens the newest job's file; the last job handled is kept in localStorage.
- `frontend/modules/window_clone_controller.js`:
  - File → Duplicate Window, and what a new window does at boot: restore a duplicated window, or open the file ALBIS was launched with. What travels is `window_clone_state.js`.
- `frontend/modules/launch_target.js`, `frontend/modules/pointer_focus_release.js`:
  - The file ALBIS was launched to open (from the URL fragment), and returning focus after a pointer click so keyboard shortcuts keep working.

### Bootstrap, chrome and menus

- `frontend/modules/runtime_bootstrap.js`:
  - Initial UI defaults and the startup sequence.
- `frontend/modules/chrome_toolbar_controller.js`, `frontend/modules/chrome_bindings.js`, `frontend/modules/window_bindings.js`, `frontend/modules/client_logging_bindings.js`:
  - Toolbar, footer and idle-chrome state; their listeners; window-level listeners; global client-side error logging.
- `frontend/modules/menu_actions.js`, `frontend/modules/menu_bindings.js`, `frontend/modules/shortcut_handlers.js`:
  - The menu action dispatcher, menu and global listeners, and the keyboard shortcuts.
- `frontend/modules/command_palette.js`, `frontend/modules/command_palette_commands.js`, `frontend/modules/command_availability.js`:
  - The command palette, its command list, and the one place that says whether a file command can run now (shared by the File menu, the palette and the shortcuts).
- `frontend/modules/panel_layout_controller.js`, `frontend/modules/panel_tab_bindings.js`, `frontend/modules/ui_panels.js`, `frontend/modules/analysis_tools.js`:
  - Side-panel layout, sections and mobile drag; tab and section listeners; the remembered tab; which optional Analysis tools a user hides.
- `frontend/modules/help_tooltips.js`, `frontend/modules/info_tips.js`:
  - Hover hints (`ui.tool_hints`) and the "?" buttons that explain an option in place.
- `frontend/modules/settings_controller.js`, `frontend/modules/backend_status_controller.js`, `frontend/modules/backend_log_viewer_controller.js`, `frontend/modules/update_check_controller.js`:
  - The Settings dialog, the backend health and live badge, View Log, and the release check with what to do for each kind of install.

### Opening files and live sources

- `frontend/modules/file_open_flow.js`, `frontend/modules/upload_flow.js`, `frontend/modules/file_ingress_bindings.js`:
  - Opening a picked file, uploading one, and the file picker and drag-and-drop listeners.
- `frontend/modules/recent_files.js`, `frontend/modules/recent_files_controller.js`:
  - The store of recently opened files and the File → Open Recent submenu.
- `frontend/modules/data_controls_bindings.js`, `frontend/modules/threshold_playback_controller.js`, `frontend/modules/source_metadata_controller.js`:
  - Data-source and frame listeners, the threshold selector and playback controls, and parsing the source's metadata into the interface.
- `frontend/modules/autoload_controls.js`, `frontend/modules/live_series_watch.js`, `frontend/modules/live_history_controller.js`:
  - Autoload and live-source listeners, following a series that is still being written, and the ring buffer of recent live frames.
- `frontend/modules/remote_stream_controller.js`, `frontend/modules/jfjoch_bridge_controller.js`, `frontend/modules/jfjoch_probe_controller.js`, `frontend/modules/jfjoch_endpoint_utils.js`:
  - The Remote Stream and JUNGFRAUJOCH runtime, the JUNGFRAUJOCH bridge lifecycle, its Test button, and normalising the endpoint field.
- `frontend/modules/simplon_probe_controller.js`, `frontend/modules/simplon_url_utils.js`, `frontend/modules/simplon_diagnostics.js`:
  - The SIMPLON Test connection button, normalising the base URL, and wording for the backend's failure codes.
- `frontend/modules/inspector_panel_controller.js`, `frontend/modules/inspector_bindings.js`:
  - The HDF5 inspector tree, search and image header panel, and their listeners.

### Viewing, overlays and analysis

- `frontend/modules/viewport_bindings.js`, `frontend/modules/overview_bindings.js`, `frontend/modules/viewer_controls_bindings.js`, `frontend/modules/histogram_drag_bindings.js`, `frontend/modules/colorbar_window_bindings.js`:
  - Canvas gestures, the overview minimap, the viewer controls, dragging the histogram's limits, and the colour bar as a window/level control.
- `frontend/modules/magnifier_controller.js`:
  - The magnifier lens, drawn from the data rather than the screen.
- `frontend/modules/mask_colors.js`, `frontend/modules/mask_legend.js`, `frontend/modules/pixel_mask_find.js`:
  - Mask colours chosen to stay distinct from the colour map, the chips under the colour bar that show and change them, and finding hot, dead and cold pixels in one image (beta).
- `frontend/modules/overlay_painters.js`:
  - Overlay drawing shared by the on-screen canvases and the GIF export.
- `frontend/modules/analysis_overlay_controller.js`, `frontend/modules/analysis_controls_bindings.js`:
  - Rings and peak analysis, and the Analysis tab's listeners.
- `frontend/modules/ring_geometry_utils.js`, `frontend/modules/ring_list_controller.js`, `frontend/modules/resolution_ring_interaction_controller.js`:
  - Resolution ring geometry (including tilted and multi-panel detectors), the list of d-spacings with presets, and dragging the beam centre and rings on the canvas.
- `frontend/modules/geometry_params.js`, `frontend/modules/geometry_params_controller.js`, `frontend/modules/geometry_override_utils.js`:
  - The detector geometry ALBIS calculates with (the data's values and the user's override), Data → Detector Geometry, and geometry-file scoping per file or series.
- `frontend/modules/roi_geometry_utils.js`, `frontend/modules/roi_controls_bindings.js`, `frontend/modules/roi_plot_bindings.js`:
  - ROI geometry in physical units (non-square pixels), the ROI controls' listeners, and the ROI plot's pan, zoom and resize.
- `frontend/modules/series_sum_controller.js`:
  - Analysis → Combine Frames, run as a background job.
- `frontend/modules/viewer_sync_controller.js`, `frontend/modules/window_clone_state.js`:
  - Linking viewport, contrast and ROI between windows, and what a duplicated window takes with it.

### Exports

- `frontend/modules/image_export.js`, `frontend/modules/image_export_controller.js`:
  - File → Export Image…: output sizes, the resolution stamp, and the dialog.
- `frontend/modules/export_splash_controller.js`:
  - The image as the viewer shows it, composed onto one canvas for the quick exports and the splash.
- `frontend/modules/animation_export_controller.js`, `frontend/modules/gif_encoder.js`:
  - The animated GIF export and a dependency-free GIF encoder.
- `frontend/modules/data_export_controller.js`, `frontend/modules/csv_export_utils.js`:
  - The data-format (TIFF/CBF/HDF5) export workflow, and the one CSV shape every CSV export uses.

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
- `backend/routes/known_detectors.py` + `backend/services/known_detectors.py`:
  - The shared known-detectors list (`/api/known-detectors`): one JSON file per server (`ui.known_detectors_file`, default `~/.config/albis/known_detectors.json`), changed one request at a time under a lock with atomic writes, merging the lists earlier versions kept in the browser.
- `backend/routes/detector_control.py`:
  - Beta Detector tab endpoints (`/api/detector/*`): describe, status, config writes, commands, files and series fetch; 404 unless `ui.detector_control` is on.
- `backend/routes/pixel_mask.py` + `backend/services/pixel_mask.py`:
  - Beta pixel masks (`/api/pixel-mask/*`): read a mask from an HDF5 file or TIFF, encode one as HDF5 or TIFF to download, read a detector's mask over SIMPLON, copy it to an HDF5 file in `masks/`, and replace it with a backup in `masks/backups/` and a read-back check; 404 unless `ui.pixel_mask` is on (and `ui.detector_control` for the detector).

- `backend/routes/data_export.py` + `backend/services/data_export.py`:
  - Data-format exports as background jobs (`/api/export/data/start|frame|status|cancel`) and saving a flatfield (`/api/flatfield/save`).
- `backend/routes/binary_response_utils.py`:
  - Shared OpenAPI docs and headers for the endpoints that answer with binary frames.

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
- `backend/services/simplon_control.py`:
  - SIMPLON reads and writes for the Detector tab: API version detection, descriptors and status (including SIMPLON 1.6 keys), the allowed commands and the background command runner, the detector's file list and series download.
- `backend/services/path_policy.py`:
  - Shared path safety policy and extension filtering helpers.
- `backend/services/directory_scan.py`:
  - Depth-, entry- and time-bounded directory walks for file/folder discovery and the newest-matching-file search, reporting whether a scan was cut short.
- `backend/services/scan_cache.py`:
  - TTL cache for scan results with single-flight loading, so concurrent pollers share one walk.
- `backend/services/os_actions.py`:
  - Shared desktop open actions and native picker integrations.

- `backend/services/handoff_queue.py`:
  - The thread-safe in-memory queue behind the handoff endpoints.
- `backend/services/content_disposition.py`:
  - A `Content-Disposition` header for a download, whatever characters its file name holds.
- `backend/services/log_safety.py`, `backend/services/log_tail.py`:
  - Making a caller-influenced string safe as one log line, and reading the end of the log for View Log.
- `backend/services/ui_prompts.py`:
  - Titles for the native file and folder choosers, in the user's language.
- `backend/services/update_check.py`, `backend/services/update_download.py`, `backend/services/update_apply.py`:
  - The GitHub release check, downloading and verifying a release asset, and applying it where that is safe.

## Backend Support Modules

- `backend/request_guard.py`, `backend/response_compression.py`:
  - Middleware: rejecting cross-site requests made on the user's behalf, and client-aware response compression.
- `backend/log_format.py`:
  - The one layout of log lines for the backend, uvicorn and the launcher.
- `backend/version.py`, `backend/build_info.py`, `backend/install_kind.py`:
  - Which version, which build, and how this copy was installed; the update check decides what to offer from them.
- `backend/detector_profiles.py`:
  - Detectors recognised by serial number, with their geometry.
- `backend/file_associations.py`, `backend/host_env.py`:
  - The file types ALBIS registers with the desktop, and a clean environment for programs ALBIS starts.

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
- `tests/test_detector_control.py`, `tests/fake_simplon.py`:
  - Detector tab endpoints against a simulated SIMPLON detector control unit; `test_scripts/fake_simplon_dcu.py` runs the same simulation by hand (`--thresholds 1-4`, `--api-version 1.6.0`).
- `tests/test_openapi_key_contract_baseline.py`:
  - Key OpenAPI contract snapshot parity check for compatibility-sensitive endpoints.

## Packaging and Release

- `VERSION`:
  - Single source of truth for release version.
- `scripts/version_info.py`:
  - Build metadata helper reading `VERSION`.
- `ALBIS.spec`:
  - PyInstaller bundle definition.
- `scripts/live_check.mjs` (`npm run ui:live`), `test_scripts/fake_simplon_dcu.py`, `test_scripts/fake_jfjoch_publisher.py`:
  - The live-source check in Chrome against a simulated detector and JUNGFRAUJOCH stream; see [Live Test Plan](LIVE_TEST_PLAN.md).
- `docs/PACKAGING.md`:
  - How the PyInstaller bundle and the per-platform installers are built.
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
  - Add styles to the area's file in `frontend/styles/` (see the UI Guide).
- User-visible behavior changes:
  - Update `README.md`, `CHANGELOG.md`, and relevant docs in `docs/`.
