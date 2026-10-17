# Graph Report - ALBIS  (2026-10-17)

## Corpus Check
- Large corpus: 567 files · ~777,405 words. Semantic extraction will be expensive (many Claude tokens). Consider running on a subfolder.

## Summary
- 6589 nodes · 15709 edges · 415 communities (233 shown, 182 thin omitted)
- Extraction: 93% EXTRACTED · 7% INFERRED · 0% AMBIGUOUS · INFERRED: 1099 edges (avg confidence: 0.86)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- Frontend App Core
- Data Export Service
- Detector Control UI State
- Series Summing Service
- Config Normalization
- API Models & Stream Routes
- Update Check & Log Viewer
- Backend App Imports
- Update Apply Service
- Series Sum Request Validation
- Data Export API Tests
- Detector Param Editing
- Viewer Sync Controller
- i18n Review Tooling
- HDF5 Units
- HDF5 Storage Confinement
- ROI Stats Controller
- File Associations
- i18n & Settings
- Overview Viewport Zoom
- System Routes & Compression
- File Browse Routes
- Simplon Probe Tests
- Detector Control Tests
- Overlay Painters
- Window Handoff & Commands
- Image Export
- OS File Actions
- Frontend Test Fixtures
- Simplon URL Helpers
- Intensity Scale Utils
- Simplon Control Client
- Frontend Controller Wiring
- Export Splash Controller
- HDF5 Stack Service
- Picker Prompts
- Image Formats (Pilatus/TIFF)
- Version Bump Script
- Frame Routes & Binary Headers
- Build Info
- Documentation Consistency Tests
- ROI & Peaks UI Sync
- HDF5 Orphaned Master
- Fake Simplon Server
- test_scan_budget_api.py
- createGeometryParamsController
- load_config
- test_hdf5_file_locking.py
- subprocess
- test_launcher_macos_open_files.py
- albis_launcher.py
- test_mythen_acquisition.py
- test_known_detectors.py
- createAnimationExportController
- main_ui_bindings_bootstrap.js
- createChromeToolbarController
- file_browser.js
- loadAndRenderBrowser
- roi_stats_engine.js
- register_hdf5_routes
- ScanCacheService
- prepareRingGeometry
- normalizeSimplonUrlInput
- vitest
- test_directory_scan.py
- numpy
- jungfraujoch_preview.py
- createFileDataPipelineController
- setPanelTab
- series_summing.py
- test_update_check_endpoint.py
- createDataExportController
- createFileBrowserController
- json
- Path
- update_check.py
- os
- createHelpTooltipController
- test_hdf5_live_writer.py
- test_installer_images.py
- _apply_cli_arguments
- test_launcher_open_target.py
- test_windows_dialog_foreground.py
- geometry_params_controller.js
- createLiveHistoryController
- re
- test_config_resilience.py
- Any
- test_export_provenance.py
- install_kind.py
- test_update_notification_plumbing.py
- createModalManager
- createMaskCursorController
- createFramePlaybackController
- bindRoiPlotInteractions
- update_check_controller.js
- test_cbf_header_round_trip.py
- UpdateDownloadService
- is_host_allowed
- jfjoch_tag_hook
- inpainting.js
- createInspectorPanelController
- createSettingsController
- test_response_compression.py
- test_golden_decode.py
- test_dectris_tiff_tag_layout.py
- services/known_detectors.py
- getCircularRoiOuterRadius
- createFlatfieldController
- lzwEncode
- known_detectors.js
- test_update_download_service.py
- Path
- remote_stream.py
- ndarray
- test_shipped_licence_metadata.py
- window_clone_state.js
- createSourceMetadataController
- test_image_geometry_endpoint.py
- socket
- _DockMenuHandler
- test_jfjoch_message_handling.py
- createAnalysisOverlayController
- createPanelLayoutController
- argparse
- test_update_apply_endpoint.py
- WalkReport
- test_release_readiness_scenarios.py
- createResolutionRingInteractionControlle
- ring_geometry_utils.js
- test_shipped_dependency_parity.py
- analysis_overlay_controller.js
- createSeriesSumController
- createRoiInteractionController
- test_macos_distribution_scripts.py
- test_detector_profiles.py
- command_availability.js
- autoload_mode_controller.js
- toast.js
- wA
- test_about_dialog_claims.py
- test_documented_check_commands.py
- test_unreadable_and_endian_sources.py
- file_type_utils.js
- post_file_picker_bindings.js
- test_hdf5_csv_export.py
- test_request_guard.py
- test_host_env.py
- register_analysis_routes
- JungfraujochPreviewBridge
- attachDetectorPicker
- recent_files_controller.test.js
- h5py
- generate_installer_images.py
- test_linux_glibc_floor.py
- drive
- test_api_smoke.py
- _load_dials_expt_geometry
- register_detector_control_routes
- DownloadRefusedError
- version.py
- fastapi_testclient
- createOverlayRenderController
- createRenderEngineController
- generate_dmg_background.py
- test_handoff_api.py
- test_path_policy.py
- test_version_consistency.py
- ResponseCompressionMiddleware
- ZstdResponder
- test_update_download_endpoint.py
- data_export_controller.js
- bootstrap_macos_signing_ci.sh
- HandoffQueueService
- checksums_url_for
- autoload_controls.js
- update_check_controller.test.js
- state.js
- bindViewportInteractions
- verify_macos_distribution.sh
- is_cross_site_request
- resetLiveHistory
- createCommandPaletteController
- createFrameCache
- roi_plot_renderer.js
- createThresholdPlaybackController
- _as_float
- app_binding_contexts.js
- canvasFont
- uploadAndOpenSelectedFiles
- detector_control_controller.test.js
- _frame
- register_data_export_routes
- request_guard.py
- local_host_names
- register_known_detector_routes
- openMenu
- normalizeSimplonBaseUrl
- dialogs.js
- shortcut_handlers.test.js
- sign_macos.sh
- test_browse_endpoint.py
- _windows_install_kind
- test_a_detached_signature_verifies_only_
- createLiveSeriesWatch
- test_documented_entry_points.py
- test_jfjoch_preview.py
- test_static_cache_headers.py
- chrome_toolbar_controller.js
- applyBrowsePaneWidth
- createDetailsHeader
- createInfoTips
- data_export_controller.test.js
- roi_plot_bindings.test.js
- version_info.py
- test_log_tail_api.py
- MonkeyPatch
- test_zenodo_metadata.py
- _compose_expt_frame
- RequestGuardMiddleware
- .dangling_link_target
- getLanguage
- createPointerFocusRelease
- file_browser.test.js
- smoke_windows_installer.ps1
- test_powershell_json_encoding.py
- _static_cache_policy
- test_raising_attaches_to_the_foreground_
- CommandRunner
- settings_controller.test.js
- _FakeResponse
- parse_accept_encoding
- help_tooltips_i18n.test.js
- inspector_tree_truncation.test.js
- jfjoch_probe_controller.js
- launch_target.test.js
- image_export_controller.test.js
- build_mac.sh
- ._resolution_from_start
- updateInspectorHeaderVisibility
- effectiveExtsParam
- te
- gs
- parse_checksums
- fetchJfjochPreviewStatus
- handleLocalFileSelection
- syncOverlayAnchors
- runtime_bootstrap.js
- UA
- xB
- check_glibc_floor.sh
- install_linux_appimage.sh
- sign_linux.sh
- smoke_macos_dmg.sh
- uninstall_linux.sh
- startAutoload
- flushClientLogs
- client_logging_bindings.js
- an
- Be
- dA
- Kr
- Lr
- build_linux.sh
- build_styled_dmg.sh
- install_appimagetool.sh
- install_linux.sh
- install_python_standalone.sh
- package_linux_appimage.sh
- package_mac_dmg.sh
- setup_branch_protection.sh
- smoke_linux_distro.sh
- test_zstd_is_available_in_this_build
- alignMaskToFrame
- applyCanvasTransform
- applyExternalFrame
- applyFrame
- applyImageMeta
- applyLiveSourceSnapshot
- applyRingEdit
- applyRoiCenterFromInputs
- applyRoiEdit
- buildColumnMask
- buildNegativeMask
- clearHistogram
- clearImageGeometry
- clearImageHeader
- clearInspectorSearch
- clearMaskState
- clearRingHover
- clearRoi
- clearRoiPlotLimitsForKey
- closeFooterVersionPopover
- consumePendingFrameRequest
- createLiveSourceSnapshot
- deferPixelOverlayRedraw
- deferViewportInteraction
- exportPeakCsv
- exportRoiCsv
- fetchInspectorTree
- fetchSimplonMask
- formatTimeStamp
- getAnchorForHandle
- getDefaultThresholdIndex
- getEffectiveScrollLeft
- getEffectiveScrollTop
- getImagePointFromEvent
- getMaxPanelWidth
- getMinZoom
- getOverviewHandleAt
- getResolutionAtPixel
- getRingHandleAt
- getRingInteractionState
- getRoiHandleAt
- getRoiPlotKey
- getRoiPlotLimits
- getRoiPlotLog
- getViewRect
- handleContrastChanged
- hasAnyManualRoiPlotLimits
- hasManualRoiPlotLimits
- hasPendingFrameRequest
- hideCursorOverlay
- hideRoiTooltip
- hideSplash
- hideUploadProgress
- histogramValueToX
- histogramXToValue
- initializePanelTabA11y
- initializeSectionContentWrappers
- initRenderer
- isFrameLoading
- isPhonePanelLayout
- isPointInRoi
- isRingEditing
- isRoiEditing
- isTouchGestureActive
- isViewportInteractionActive
- loadAutoloadSettings
- loadFrame
- loadInspectorRoot
- nearestMobilePanelSnap
- normalizeWheelDelta
- overviewEventToImage
- overviewEventToOverview
- panToImageCenter
- parseRemoteMeta
- parseSimplonMeta
- processPendingFrameRequest
- queueWheelZoom
- redrawRoiPlots
- refreshPeakResolutions
- registerChromeActivity
- renderInspectorLink
- renderInspectorTree
- renderPeakList
- resizeViewFromHandle
- runInspectorSearch
- saveSettingsFromModal
- selectInspectorRow
- setAutoloadLatest
- setEffectiveScroll
- setFps
- setFrameStep
- setMobilePanelSnap
- setRoiPlotAxisLimits
- setRoiPlotLog
- setSeriesSumProgress
- setSimplonMode
- setZoom
- showHistTooltip
- showInspectorNode
- snapHistogramValue
- startBackendHeartbeat
- startMobilePanelDrag
- startRingEdit
- startRoiEdit
- startTouchGesture
- stopAutoload
- stopRingEdit
- stopRoiEdit
- stopTouchGesture
- syncMaskAvailability
- syncRoiPlotLimitControls
- toggleSection
- toggleToolbarMorePopover
- toggleToolbarPlaybackPopover
- updateAutoloadUI
- updateCursorOverlay
- updateFpsLabel
- updateGlobalStats
- updateJfjochMetaUI
- updateLiveHistoryEntry
- updateMobilePanelDrag
- updatePanCapability
- updateRemoteMetaUI
- updateRingHover
- updateRoiStats
- updateRoiTooltip
- updateSimplonMetaUI
- updateSplashCallToAction
- updateThresholdOptions
- updateUiIdleAndAnchors
- zoomAt
- dectris-albis

## God Nodes (most connected - your core abstractions)
1. `t()` - 371 edges
2. `vitest` - 104 edges
3. `createDetectorControlController()` - 101 edges
4. `createFileBrowserController()` - 78 edges
5. `createRoiStatsController()` - 67 edges
6. `_StrictModel` - 59 edges
7. `DataExportService` - 55 edges
8. `SeriesSummingService` - 49 edges
9. `createOverviewViewportController()` - 48 edges
10. `createAnimationExportController()` - 47 edges

## Surprising Connections (you probably didn't know these)
- `read_tiff()` --indirect_call--> `frame()`  [INFERRED]
  tests/test_series_summing_service.py → backend/routes/frames.py
- `_policy()` --indirect_call--> `_image_ext_name()`  [INFERRED]
  tests/test_path_policy.py → backend/image_formats.py
- `client()` --uses--> `AnalysisRouteDeps`  [INFERRED]
  tests/test_series_sum_request_validation.py → backend/routes/analysis.py
- `test_drops_what_is_not_a_detector_entry()` --calls--> `sanitize()`  [EXTRACTED]
  tests/test_known_detectors.py → backend/services/known_detectors.py
- `_make_deps()` --indirect_call--> `mask_flag_value()`  [INFERRED]
  tests/test_series_summing_service.py → backend/services/series_ops.py

## Import Cycles
- None detected.

## Communities (415 total, 182 thin omitted)

### Community 0 - "Frontend App Core"
Cohesion: 0.00
Nodes (545): aboutClose, aboutModal, aboutVersion, analysisOverlayController, analysisState, animationExportCancel, animationExportClose, animationExportFps (+537 more)

### Community 1 - "Data Export Service"
Cohesion: 0.06
Nodes (8): _DataExportCancelledError, DataExportService, apply_flat(), Flat, prepare_flat(), test_a_flat_with_nothing_usable_is_none(), test_counts_keep_the_scale_and_transmission_runs_to_one(), test_divides_like_the_viewer()

### Community 2 - "Detector Control UI State"
Cohesion: 0.09
Nodes (78): createDetectorControlController(), acquire(), applyDescription(), ask(), cancelChange(), clearMonitor(), commandButton(), connect() (+70 more)

### Community 3 - "Series Summing Service"
Cohesion: 0.06
Nodes (31): SeriesSummingService, series_files(), _make_deps(), test_a_failed_hdf5_job_leaves_no_partial_output(), test_series_summing_service_cancel_job(), read_tiff(), resolve_series_files(), test_series_summing_service_embeds_effective_geometry_in_hdf5_output() (+23 more)

### Community 4 - "Config Normalization"
Cohesion: 0.07
Nodes (48): _start_macos_menus(), _apply_runtime_config(), _init_logging(), RuntimeState, _settings_payload(), _apply_legacy_config_compat(), _candidate_paths(), _deep_merge() (+40 more)

### Community 5 - "API Models & Stream Routes"
Cohesion: 0.06
Nodes (46): DataExportCancelRequest, DataExportStartRequest, DetectorCommandRequest, DetectorConfigRequest, DetectorSeriesFetchRequest, HandoffJobCreateRequest, HDF5AttrItem, HDF5DatasetEntry (+38 more)

### Community 6 - "Update Check & Log Viewer"
Cohesion: 0.08
Nodes (66): createBackendLogViewerController(), clearPoll(), close(), isOpen(), loadTail(), open(), openOnHost(), refreshUi() (+58 more)

### Community 7 - "Backend App Imports"
Cohesion: 0.07
Nodes (15): HandoffJobResponse, _ensure_hdf5_stack(), _get_h5py(), _parse_ext_filter(), DataExportRouteDeps, DetectorControlRouteDeps, _extract_open_target(), HandoffRouteDeps (+7 more)

### Community 8 - "Update Apply Service"
Cohesion: 0.07
Nodes (28): ShutdownController, appimage_target_path(), ApplyState, UpdateApplyService, _service(), test_a_failed_appimage_swap_leaves_no_staged_file_and_does_not_close_albis(), _explode(), test_a_failing_hook_does_not_escape_the_timer_thread() (+20 more)

### Community 9 - "Series Sum Request Validation"
Cohesion: 0.09
Nodes (40): client(), _FakeJobs, jobs(), _request(), _start(), test_a_declined_cancel_is_reported_as_declined(), test_a_field_the_client_invented_is_refused(), test_a_missing_file_is_rejected() (+32 more)

### Community 10 - "Data Export API Tests"
Cohesion: 0.08
Nodes (41): main(), normalize(), read_pins(), sync(), replace(), _add_export_header_metadata(), _client(), _export() (+33 more)

### Community 11 - "Detector Param Editing"
Cohesion: 0.06
Nodes (57): ADVANCED_HELP, BUSY_STATES, COMPRESSION_FACTOR, CORE_GROUPS, buildParamRow(), imageSwitch(), labelFor(), labelQuickButtons() (+49 more)

### Community 12 - "Viewer Sync Controller"
Cohesion: 0.10
Nodes (51): createSourceId(), createViewerSyncController(), applyRemoteContrast(), applyRemoteRoi(), applyRemoteViewport(), applyViewportCenter(), cancelAllQueuedPublishes(), cancelQueuedPublish() (+43 more)

### Community 13 - "i18n Review Tooling"
Cohesion: 0.06
Nodes (50): author, description, devDependencies, eslint, jsdom, vitest, engines, node (+42 more)

### Community 14 - "HDF5 Units"
Cohesion: 0.08
Nodes (36): _distance_to_mm(), coerce_scalar(), get_units(), norm_unit(), to_ev(), to_mm(), to_um(), wavelength_to_ev() (+28 more)

### Community 16 - "HDF5 Storage Confinement"
Cohesion: 0.07
Nodes (23): _is_within(), PathPolicy, confined_client(), _external_storage_file(), _plain_stack(), _policy(), roots(), _service() (+15 more)

### Community 17 - "ROI Stats Controller"
Cohesion: 0.09
Nodes (49): computeMedian(), formatRoiTick(), isSaturatedValue(), quickSelect(), createRoiStatsController(), accumulateRoiPixelCounters(), applyMaskToValue(), buildRoiHistogram() (+41 more)

### Community 18 - "File Associations"
Cohesion: 0.06
Nodes (27): FileAssociation, is_associated_path(), test_appimage_desktop_template_has_expected_fields(), test_appimage_metainfo_template_is_well_formed(), _classes_key(), _desktop_entry(), _iss_text(), _spec_info_plist() (+19 more)

### Community 19 - "i18n & Settings"
Cohesion: 0.06
Nodes (32): applyLanguagePreference(), applyElementI18n(), applyI18nToDom(), applyPluralTokens(), dictionaries, getStoredLanguagePreference(), getValue(), hasStoredLanguagePreference() (+24 more)

### Community 20 - "Overview Viewport Zoom"
Cohesion: 0.10
Nodes (40): createOverviewViewportController(), applyCanvasTransform(), applyEffectiveScroll(), clampEffectiveScroll(), clampPanOffsetsToBounds(), clampToRange(), consumePendingFrameRequest(), deferViewportInteraction() (+32 more)

### Community 21 - "System Routes & Compression"
Cohesion: 0.08
Nodes (35): ClientLogRequest, HealthResponse, LogTailResponse, PathStatusResponse, SettingsPayloadResponse, SettingsSaveRequest, UpdateApplyStartRequest, UpdateApplyStatusResponse (+27 more)

### Community 22 - "File Browse Routes"
Cohesion: 0.08
Nodes (34): AutoloadLatestResponse, BrowseFileItem, BrowseResponse, FilesListResponse, FoldersListResponse, PathSelectionResponse, SeriesInfoResponse, UploadResponse (+26 more)

### Community 23 - "Simplon Probe Tests"
Cohesion: 0.06
Nodes (25): simplon_probe(), _closed_port(), fake_simplon(), fake_simplon_16(), fake_simplon_exotic(), _FakeSimplonHandler, _serve(), _Simplon16Handler (+17 more)

### Community 24 - "Detector Control Tests"
Cohesion: 0.16
Nodes (30): main(), FakeDCUServer, client(), _command(), _command_v(), dcu(), _initialize(), _set() (+22 more)

### Community 25 - "Overlay Painters"
Cohesion: 0.10
Nodes (42): createOverlayCompositor(), paint(), paintLabels(), GIF_SCALES, GIF_TYPES, LABEL_BLACK_INDEX, LABEL_WHITE_INDEX, MASK_BLACK_RGB (+34 more)

### Community 26 - "Window Handoff & Commands"
Cohesion: 0.05
Nodes (44): applyHandoffJob(), autoloadTick(), cancelSeriesSumming(), ensureFileMode(), exportFullImage(), exportViewerWindow(), exportVisibleArea(), fileLabel() (+36 more)

### Community 27 - "Image Export"
Cohesion: 0.12
Nodes (39): canSaveImage(), renderVisibleAsShown(), chunkType(), createImageExportController(), openDialog(), overlaySnapshot(), pixelAspect(), pixelValuesState() (+31 more)

### Community 28 - "OS File Actions"
Cohesion: 0.10
Nodes (30): choose_file(), choose_folder(), _darwin_picker_types(), _display_available(), is_applescript_cancel(), _linux_choose_file(), _linux_choose_folder(), _normalize_picker_exts() (+22 more)

### Community 29 - "Frontend Test Fixtures"
Cohesion: 0.06
Nodes (21): checkbox(), createController(), EN, makeState(), runExport(), seriesState(), stubOverlayCanvas(), HTML_FILES (+13 more)

### Community 30 - "Simplon URL Helpers"
Cohesion: 0.09
Nodes (25): classify_simplon_failure(), _detector_message(), _is_api_version(), normalize_simplon_base_url(), _raise_simplon_failure(), _simplon_api_base(), simplon_base(), _simplon_config_value() (+17 more)

### Community 31 - "Intensity Scale Utils"
Cohesion: 0.08
Nodes (38): buildPalette(), chooseHistogramBins(), computeAutoLevels(), computeHistogram(), computeStats(), formatPixelLabelValue(), getDtypeInfo(), getPaletteColorCount() (+30 more)

### Community 32 - "Simplon Control Client"
Cohesion: 0.11
Nodes (24): api_version(), _base(), _coerce(), run(), data_url(), describe(), _descriptor(), detector_state() (+16 more)

### Community 33 - "Frontend Controller Wiring"
Cohesion: 0.09
Nodes (33): bindAnalysisControlInteractions(), validatePeaksCountInput(), validatePeaksSnrInput(), clampFrameIndex(), createBackendStatusController(), buildStamp(), checkBackendHealth(), sleep() (+25 more)

### Community 34 - "Export Splash Controller"
Cohesion: 0.10
Nodes (36): showPromptDialog(), composeViewerImage(), createExportSplashController(), canvasToBlob(), defaultExportName(), downloadBlob(), downloadCanvasImage(), drawGlowDot() (+28 more)

### Community 36 - "Picker Prompts"
Cohesion: 0.10
Nodes (21): _applescript_double_quote(), is_safe_prompt(), _read_catalogue(), resolve_prompt(), _catalogue(), test_a_catalogue_outside_the_locales_directory_is_not_read(), test_an_apostrophe_is_allowed_because_it_is_escaped_properly(), test_an_unknown_language_falls_back_to_english() (+13 more)

### Community 37 - "Image Formats (Pilatus/TIFF)"
Cohesion: 0.13
Nodes (29): _absolutize_dectris_offsets(), _coerce_float_vector(), _coerce_int_vector(), _convert_length(), _ensure_fabio_readers(), _find_tiff_tag_value_offset(), _image_ext_name(), _normalize_geometry_panel() (+21 more)

### Community 38 - "Version Bump Script"
Cohesion: 0.12
Nodes (21): bump(), bump_changelog(), bump_texts(), BumpError, is_newer(), main(), parse_version(), _pre_key() (+13 more)

### Community 39 - "Frame Routes & Binary Headers"
Cohesion: 0.11
Nodes (22): FrameMetadataResponse, _to_little_endian(), add_optional_header(), build_binary_headers(), octet_stream_responses(), sanitize_header_value(), FrameRouteDeps, register_frame_routes() (+14 more)

### Community 40 - "Build Info"
Cohesion: 0.08
Nodes (16): _commit_file_candidates(), _commit_from_env(), _commit_from_file(), _commit_from_git(), _normalize(), read_commit(), _repo_root(), _isolate_from_the_real_repo() (+8 more)

### Community 41 - "Documentation Consistency Tests"
Cohesion: 0.06
Nodes (18): _autoload_extensions(), _flat(), test_albis_is_citable(), test_documentation_does_not_reference_removed_controls(), test_documented_binary_headers_are_the_ones_the_code_sends(), test_every_config_key_is_documented_in_the_power_user_guide(), test_every_introduction_uses_the_lead(), test_every_openable_format_is_documented() (+10 more)

### Community 42 - "ROI & Peaks UI Sync"
Cohesion: 0.06
Nodes (34): applyPanelState(), applyPlatformShortcutLabels(), applySyncedRoi(), buildSkeletonList(), cloneSyncedRoiPoint(), currentFrameStatusText(), drawRoiOverlay(), getRoiModeLabel() (+26 more)

### Community 43 - "HDF5 Orphaned Master"
Cohesion: 0.14
Nodes (17): client(), _service(), test_a_dangling_link_read_back_as_none_is_not_taken_for_a_hard_link(), test_a_master_with_all_its_data_is_untouched(), test_a_mix_of_absent_and_unreadable_files_reports_both(), test_a_partly_downloaded_master_still_opens(), test_dangling_link_target_names_the_file_without_following_it(), test_data_files_that_are_present_but_unreadable_are_not_called_missing() (+9 more)

### Community 44 - "Fake Simplon Server"
Cohesion: 0.11
Nodes (13): _add_thresholds(), _default_params(), FakeDCU, _handler(), _data(), do_GET(), do_HEAD(), do_PUT() (+5 more)

### Community 45 - "test_scan_budget_api.py"
Cohesion: 0.11
Nodes (19): _age(), clean_cache(), test_a_complete_listing_is_not_marked_truncated(), test_a_complete_search_sets_no_truncation_header(), test_a_listing_that_hit_the_budget_says_so(), test_a_selected_subfolder_is_not_cached(), test_a_truncated_search_that_found_nothing_is_not_an_empty_folder(), test_a_vanished_file_is_nothing_to_load_not_an_error() (+11 more)

### Community 46 - "createGeometryParamsController"
Cohesion: 0.17
Nodes (33): createGeometryParamsController(), applyPickedGeometryFile(), clearGeometryFile(), clearSource(), getActiveGeometryFile(), hasSource(), inlineText(), missingRequired() (+25 more)

### Community 47 - "load_config"
Cohesion: 0.15
Nodes (17): env_config_overrides(), env_override_keys(), load_config(), clean_environment(), test_a_list_key_accepts_a_comma_separated_value(), test_a_named_config_that_does_not_exist_yet_is_created_there(), test_an_empty_value_is_still_an_instruction(), test_an_unusable_value_falls_back_instead_of_failing_to_start() (+9 more)

### Community 48 - "test_hdf5_file_locking.py"
Cohesion: 0.10
Nodes (13): is_hdf5_lock_failure(), open_hdf5_for_read_reporting_writer(), open_hdf5_read_only_reporting_writer(), h5_file(), Locked, test_a_genuinely_corrupt_file_is_still_reported_as_corrupt(), test_a_locked_file_still_opens_and_reads_correctly(), test_a_missing_file_is_still_a_404() (+5 more)

### Community 49 - "subprocess"
Cohesion: 0.09
Nodes (12): _load_dmg_layout(), _png_dimensions(), test_both_packaging_paths_call_the_shared_dmg_builder(), test_hidpi_background_is_exactly_twice_the_standard_one(), test_icon_size_fits_within_creat_dmgs_own_ceiling(), test_scripts_the_macos_build_executes_are_committed_executable(), test_shell_mode_needs_nothing_beyond_the_standard_library(), test_shell_output_matches_the_layout_object() (+4 more)

### Community 50 - "test_launcher_macos_open_files.py"
Cohesion: 0.11
Nodes (14): delegate(), h5(), _settle(), _StubApp, test_a_document_arriving_after_the_activation_opens_one_window(), test_a_document_arriving_before_the_activation_opens_one_window(), test_a_document_is_never_dropped_by_the_throttle(), test_a_selection_with_nothing_openable_leaves_the_plain_window() (+6 more)

### Community 51 - "albis_launcher.py"
Cohesion: 0.11
Nodes (17): _configure_launcher_logger(), _default_launcher_log_path(), _ensure_stdio_streams(), _install_windows_shutdown_listener(), _wait_for_shutdown_event(), _launcher_log(), _load_last_server(), main() (+9 more)

### Community 52 - "test_mythen_acquisition.py"
Cohesion: 0.10
Nodes (19): _is_channel_index_column(), _mythen_counts_column(), _mythen_frame_files(), _mythen_header_text(), _read_mythen_acquisition(), _read_mythen_dat(), _pair_layout(), test_a_counts_only_frame_keeps_every_channel() (+11 more)

### Community 53 - "test_known_detectors.py"
Cohesion: 0.13
Nodes (15): default_path(), KnownDetectorStore, resolve_path(), client(), test_a_damaged_file_reads_as_empty(), test_a_list_that_cannot_be_saved_is_an_error_naming_the_file(), test_changes_from_another_site_are_refused(), test_drops_what_is_not_a_detector_entry() (+7 more)

### Community 54 - "createAnimationExportController"
Cohesion: 0.16
Nodes (30): createAnimationExportController(), buildGifPalette(), cancelExport(), defaultGifScale(), fetchFrame(), fileStem(), formatBytes(), frameToIndices() (+22 more)

### Community 55 - "main_ui_bindings_bootstrap.js"
Cohesion: 0.11
Nodes (17): bindChromeUiInteractions(), bindDataControlInteractions(), bindFileIngress(), bindInspectorInteractions(), initializeMainUiBindings(), bindMenuAndGlobalInteractions(), focusItemAt(), getActivePanel() (+9 more)

### Community 56 - "createChromeToolbarController"
Cohesion: 0.13
Nodes (30): createChromeToolbarController(), buildInfoText(), buildViewerSourceText(), clearChromeIdleTimer(), closeFooterVersionPopover(), closeToolbarMorePopover(), closeToolbarPlaybackPopover(), copyBuildInfo() (+22 more)

### Community 57 - "file_browser.js"
Cohesion: 0.08
Nodes (30): aggregateSeriesLocally(), BROWSE_SERIES_CAPABLE_EXTS, BROWSE_SERIES_CAPABLE_SET, COLUMN_SORTS, compareNatural(), filterBrowseItems(), submitTypedPath(), DEFAULT_BROWSE_EXTS (+22 more)

### Community 58 - "loadAndRenderBrowser"
Cohesion: 0.14
Nodes (31): buildBreadcrumbSegments(), applyBrowseModeLayout(), clearBrowseSelection(), clearSearchQuery(), closeFileBrowser(), getAvailableFormatGroups(), handleBrowseEnter(), loadAndRenderBrowser() (+23 more)

### Community 59 - "roi_stats_engine.js"
Cohesion: 0.16
Nodes (25): updateRoiHistogramBinSettings(), accumulateRoiPixelCounters(), applyMaskToValue(), buildFixedRoiHistogram(), buildRoiHistogram(), computeGlobalStats(), countingMedian(), createRoiAccumulator() (+17 more)

### Community 60 - "register_hdf5_routes"
Cohesion: 0.11
Nodes (20): HDF5DatasetsResponse, HDF5NodeResponse, HDF5SearchResponse, HDF5TreeChild, HDF5TreeResponse, HDF5ValueResponse, register_hdf5_routes(), hdf5_csv() (+12 more)

### Community 61 - "ScanCacheService"
Cohesion: 0.12
Nodes (14): _Entry, ScanCacheService, test_a_zero_ttl_always_reloads(), test_clear_drops_everything(), test_concurrent_callers_run_the_scan_once(), slow_load(), worker(), test_entries_are_pruned_so_a_walked_filesystem_cannot_grow_it() (+6 more)

### Community 63 - "prepareRingGeometry"
Cohesion: 0.11
Nodes (20): getSeriesSumGeometryContext(), applyGeometryOverrides(), getGeometryReferencePose(), HC_EV_ANGSTROM, prepareRingGeometry(), serializeGeometryPayload(), applyImageGeometry(), fixture (+12 more)

### Community 64 - "normalizeSimplonUrlInput"
Cohesion: 0.12
Nodes (21): createAutoloadOrchestrationController(), autoloadTick(), ensureFileMode(), startAutoload(), stopAutoload(), createAutoloadStatusController(), formatTimeStamp(), setAutoloadLatest() (+13 more)

### Community 65 - "vitest"
Cohesion: 0.09
Nodes (11): createFrameMetadataController(), frameCountFromShape(), loadAnalysisParams(), loadAutoloadFolders(), loadFiles(), loadMetadata(), refreshFrameCount(), build() (+3 more)

### Community 66 - "test_directory_scan.py"
Cohesion: 0.21
Nodes (19): scan_image_files(), ScanLimits, _ext_name(), test_a_budget_that_is_never_reached_reports_no_truncation(), test_depth_limit_is_still_honoured(), test_entry_budget_stops_the_walk_and_says_so(), test_folders_are_listed_without_the_root_itself(), test_from_config_maps_minus_one_to_unlimited() (+11 more)

### Community 67 - "numpy"
Cohesion: 0.14
Nodes (13): _direction(), grow_holes(), inpaint(), test_fixture_actually_uses_a_compression_filter(), test_frames_decode_through_the_api(), test_a_dead_pixel_takes_its_neighbours_and_the_rest_stays(), test_a_gradient_carries_straight_through_a_gap(), test_a_margin_is_a_square_and_capped() (+5 more)

### Community 68 - "jungfraujoch_preview.py"
Cohesion: 0.11
Nodes (15): jfjoch_probe_endpoint(), normalize_jfjoch_endpoint(), validate_jfjoch_endpoint(), _closed_port(), listening_port(), test_normalize_jfjoch_endpoint(), test_normalize_jfjoch_endpoint_is_idempotent(), test_preview_start_rejects_an_endpoint_without_a_port() (+7 more)

### Community 69 - "createFileDataPipelineController"
Cohesion: 0.15
Nodes (23): applyImageGeometry(), loadImageGeometry(), reloadGeometryForCurrentFile(), createFileDataPipelineController(), applyFramePayload(), datasetScanFailureText(), finishFrameLoad(), frameCacheKey() (+15 more)

### Community 70 - "setPanelTab"
Cohesion: 0.07
Nodes (28): applySyncedContrast(), applyUiSettings(), bootstrapApp(), drawHistogram(), drawSplash(), fetchSettingsConfig(), formatValue(), handleRoiChanged() (+20 more)

### Community 71 - "series_summing.py"
Cohesion: 0.11
Nodes (13): iter_sum_groups(), mask_flag_value(), mask_slices(), _SeriesSummingCancelledError, SeriesSummingDeps, test_a_writer_without_the_metadata_dep_still_produces_a_header(), test_iter_sum_groups_chunks(), test_iter_sum_groups_invalid_range() (+5 more)

### Community 72 - "test_update_check_endpoint.py"
Cohesion: 0.13
Nodes (23): _asset_match_specs(), _parse_assets(), ReleaseMetadata, select_download_asset(), update_command_for(), _request_update_check(), test_asset_selection_never_offers_a_signature_or_checksum_file(), test_asset_selection_offers_nothing_for_an_unknown_architecture() (+15 more)

### Community 73 - "createDataExportController"
Cohesion: 0.23
Nodes (28): liveExportEntries(), createDataExportController(), cancelExport(), clearFinishedResult(), closeDialog(), currentLiveEntry(), flatfieldFields(), flatfieldWritable() (+20 more)

### Community 74 - "createFileBrowserController"
Cohesion: 0.19
Nodes (28): createFileBrowserController(), applyDirectorySelectionDefaults(), canConfirmBrowseSelection(), confirmBrowseSelection(), createDetailsRowContent(), createListRowContent(), createSeriesBadge(), focusBrowseItem() (+20 more)

### Community 75 - "json"
Cohesion: 0.13
Nodes (17): _assert_decodes_compressed_hdf5(), _assert_decodes_image(), _assert_health_version(), _assert_http_asset(), _assert_zstd_bundled(), _find_free_port(), _get(), main() (+9 more)

### Community 76 - "Path"
Cohesion: 0.12
Nodes (3): TestHdf5DetectorCarryover, TestSeriesOperationsTiffHeader, TestTiffDescription

### Community 77 - "update_check.py"
Cohesion: 0.13
Nodes (9): UpdateCheckResponse, _compare_prerelease_identifiers(), compare_versions(), _normalize_version_token(), _parse_version(), ParsedVersion, ReleaseCheckService, _resolve_current_version() (+1 more)

### Community 78 - "os"
Cohesion: 0.13
Nodes (9): _scan_folders(), BoundedWalk, _Budget, latest_image_file(), LatestFileResult, _relative_posix(), scan_folders(), ScanResult (+1 more)

### Community 79 - "createHelpTooltipController"
Cohesion: 0.14
Nodes (19): createHelpTooltipController(), applyHelpMap(), describeTarget(), findHelpTarget(), hideHelp(), initHelpTooltips(), positionHelpTooltip(), refreshHelpTooltips() (+11 more)

### Community 80 - "test_hdf5_live_writer.py"
Cohesion: 0.11
Nodes (15): _frame(), live_series(), append_to(), LiveSeries, test_a_corrupt_file_still_reports_its_real_problem(), test_a_missing_file_is_still_a_missing_file(), test_datasets_are_discoverable_in_a_live_file(), test_frame_count_grows_with_the_acquisition() (+7 more)

### Community 81 - "test_installer_images.py"
Cohesion: 0.14
Nodes (16): _as_tuple(), _iss_glob(), _iss_value(), _png_alpha(), _png_header(), _script_version(), test_background_is_opaque(), test_background_matches_the_documented_area_at_each_dpi() (+8 more)

### Community 82 - "_apply_cli_arguments"
Cohesion: 0.11
Nodes (13): _apply_cli_arguments(), _build_arg_parser(), env_var_name(), test_variable_names_are_derived_from_the_key(), clean_environment(), test_arguments_the_operating_system_adds_are_ignored_not_fatal(), test_config_points_at_a_file_and_is_expanded(), test_every_flag_maps_to_a_real_config_key() (+5 more)

### Community 83 - "test_launcher_open_target.py"
Cohesion: 0.15
Nodes (17): _open_target_url(), _resolve_open_target(), h5(), test_a_bare_path_is_taken_as_the_file_to_open(), test_a_path_that_is_not_a_file_is_declined(), test_an_associated_file_is_accepted_and_made_absolute(), test_an_unopenable_path_is_ignored_rather_than_fatal(), test_an_unregistered_extension_is_declined() (+9 more)

### Community 84 - "test_windows_dialog_foreground.py"
Cohesion: 0.09
Nodes (7): file_script(), folder_script(), script(), test_no_literal_format_placeholder_survived(), test_the_drawing_assembly_is_loaded_before_it_is_used(), test_the_owner_is_shown_so_it_has_a_window_handle(), test_the_selection_is_still_written_to_stdout()

### Community 85 - "geometry_params_controller.js"
Cohesion: 0.14
Nodes (18): DIGITS, formatGeometrySource(), VALIDATION_KEYS, createGeometryOverride(), emptyGeometryValues(), GEOMETRY_KEYS, GEOMETRY_OVERRIDE_STORAGE_KEY, loadGeometryOverride() (+10 more)

### Community 86 - "createLiveHistoryController"
Cohesion: 0.19
Nodes (21): cloneAnalysis(), cloneObject(), clonePeakSets(), cloneSnapshot(), createLiveHistoryController(), appendLiveFrame(), getEntries(), goLive() (+13 more)

### Community 87 - "re"
Cohesion: 0.11
Nodes (12): test_the_build_carries_the_frontend_directory(), test_the_help_page_uses_the_shipped_faces_and_michroma_for_its_wordmarks_only(), test_the_licence_travels_with_the_font(), test_the_stylesheet_points_at_the_file_that_exists(), test_the_wordmark_face_is_used_for_the_wordmark_only(), test_the_wordmark_licence_travels_with_the_font(), test_no_workflow_is_left_on_another_interpreter(), test_pyproject_requires_the_supported_version() (+4 more)

### Community 88 - "test_config_resilience.py"
Cohesion: 0.12
Nodes (10): _lifespan(), config_load_error(), config_dir(), test_docker_image_disables_absolute_paths(), test_load_config_clears_a_stale_error_on_a_later_success(), test_readable_config_is_applied_and_reports_no_error(), test_save_config_replaces_the_file_atomically(), test_saving_settings_recovers_from_an_unreadable_config() (+2 more)

### Community 89 - "Any"
Cohesion: 0.18
Nodes (20): _active_threshold_ev(), _as_int_list(), _as_number_list(), _as_pair(), _as_str(), _decode_tiff_values(), _dectris_offsets_are_absolute(), _dectris_tiff_payload() (+12 more)

### Community 90 - "test_export_provenance.py"
Cohesion: 0.18
Nodes (15): producer_string(), _cbf_header(), _client(), _export(), source_stack(), test_a_multi_threshold_export_names_the_threshold(), test_a_written_file_with_no_metadata_at_all_still_names_albis(), test_an_exported_cbf_names_the_frame_it_came_from() (+7 more)

### Community 91 - "install_kind.py"
Cohesion: 0.15
Nodes (17): _docker_from_env(), _docker_marker_present(), _is_frozen(), read_install_kind(), read_target_arch(), _windows_registered_install_location(), _isolate(), test_a_failing_probe_never_propagates_out_of_the_detection() (+9 more)

### Community 92 - "test_update_notification_plumbing.py"
Cohesion: 0.09
Nodes (12): install_kind_override(), bundled_signing_key(), _publication_stage_pattern(), test_an_unparseable_version_override_is_ignored_not_obeyed(), test_appimage_architecture_token_matches_the_name_the_build_produces(), test_dockerfile_marks_the_image_as_a_container_install(), test_packaging_bundles_the_signing_key_when_one_is_present(), test_release_workflow_publishes_candidates_as_prereleases() (+4 more)

### Community 93 - "createModalManager"
Cohesion: 0.15
Nodes (24): animationExportController, closeAboutModal(), closeBackendLogViewer(), closeCommandPalette(), closeSettingsModal(), closeTopModal(), closeUpdateCheckModal(), dataExportController (+16 more)

### Community 94 - "createMaskCursorController"
Cohesion: 0.14
Nodes (18): parseBadChannels(), createMaskCursorController(), alignMaskToFrame(), buildColumnMask(), clearMaskState(), fetchSimplonMask(), formatCursorValue(), hideCursorOverlay() (+10 more)

### Community 95 - "createFramePlaybackController"
Cohesion: 0.13
Nodes (15): createFramePlaybackController(), consumePendingFrameRequest(), currentFrameStatusText(), isFrameLoading(), processPendingFrameRequest(), queuePendingFrameRequest(), requestFrame(), scheduleNextPlaybackTick() (+7 more)

### Community 96 - "bindRoiPlotInteractions"
Cohesion: 0.18
Nodes (24): bindRoiPlotInteractions(), applyRoiAxisLimitInput(), beginRoiPlotPan(), clearRoiAxisLimitsForEntry(), closeRoiAxisLimitPopovers(), endRoiPlotPan(), formatAxisLimitInputValue(), getAxisLimitKey() (+16 more)

### Community 97 - "update_check_controller.js"
Cohesion: 0.11
Nodes (12): formatBytes(), INSTRUCTION_KEYS, RUNNING_DOWNLOAD_STATES, applyStatus(), DICTIONARY, initialize(), renderShell(), buildFetchMock() (+4 more)

### Community 98 - "test_cbf_header_round_trip.py"
Cohesion: 0.11
Nodes (12): _exported_header(), parsed(), source_header(), test_a_zero_start_angle_is_written_rather_than_dropped(), test_energy_is_derived_when_only_a_wavelength_is_stated(), test_every_instrument_field_is_read(), test_pixel_array_and_path_fields_are_deliberately_not_read(), test_the_exported_header_states_what_the_original_stated() (+4 more)

### Community 99 - "UpdateDownloadService"
Cohesion: 0.17
Nodes (3): default_download_dir(), _Job, UpdateDownloadService

### Community 100 - "is_host_allowed"
Cohesion: 0.10
Nodes (13): is_host_allowed(), is_ip_literal(), test_a_concrete_bind_address_allows_itself(), test_a_cross_site_page_cannot_make_albis_call_a_detector(), test_configured_hosts_allow_a_reverse_proxy_in_front_of_a_loopback_bind(), test_cross_site_cannot_open_a_native_picker(), test_loopback_bind_accepts_this_machine(), test_loopback_bind_rejects_any_other_name() (+5 more)

### Community 101 - "jfjoch_tag_hook"
Cohesion: 0.13
Nodes (15): _decode_dectris_compression(), _decode_multi_dim_array(), _decode_typed_array(), jfjoch_tag_hook(), _select_channel_image(), test_a_column_major_multidim_array_is_read_column_first(), test_a_row_major_multidim_array_keeps_its_shape(), test_a_typed_array_accepts_a_memoryview_payload() (+7 more)

### Community 102 - "inpainting.js"
Cohesion: 0.20
Nodes (21): addDirection(), createInpaintingController(), apply(), updateState(), integralData(), levelValue(), createInpaintCache(), fillHoles() (+13 more)

### Community 103 - "createInspectorPanelController"
Cohesion: 0.19
Nodes (23): createInspectorPanelController(), buildInspectorRow(), buildInspectorTable1D(), buildInspectorTable2D(), clearImageHeader(), clearInspectorSearch(), fetchInspectorTree(), formatInspectorCell() (+15 more)

### Community 104 - "createSettingsController"
Cohesion: 0.20
Nodes (24): createSettingsController(), applyEnvOverrides(), applyUiSettings(), captureRestartBaseline(), closeSettingsModal(), collectSettingsForm(), envOverridableFields(), fieldLabel() (+16 more)

### Community 105 - "test_response_compression.py"
Cohesion: 0.13
Nodes (13): compressed(), test_a_client_accepting_nothing_we_produce_gets_raw_bytes(), test_an_already_encoded_response_is_left_alone(), test_event_streams_are_not_compressed(), test_gzip_is_used_when_the_client_cannot_take_zstd(), test_health_reports_which_encodings_this_build_can_produce(), test_small_bodies_are_not_compressed(), test_small_responses_are_left_alone() (+5 more)

### Community 106 - "test_golden_decode.py"
Cohesion: 0.14
Nodes (12): decode_byte_offset(), header_text(), read_image(), split_cbf(), test_cbf_decodes_to_the_same_pixels_it_always_has(), test_cbf_metadata_matches_the_numbers_in_its_own_header(), test_cbf_pixels_match_an_independent_byte_offset_decoder(), test_the_cbf_fixture_is_the_file_it_says_it_is() (+4 more)

### Community 107 - "test_dectris_tiff_tag_layout.py"
Cohesion: 0.16
Nodes (11): _parse_dectris_tag_value(), _rebase_dectris_ifd(), _read_embedded_ifd(), _tag_entry(), test_a_file_from_an_older_albis_still_reads(), test_albis_still_reads_its_own_output(), test_an_outside_reader_gets_the_values_albis_wrote(), test_an_unknown_entry_name_is_dropped_rather_than_raising() (+3 more)

### Community 108 - "services/known_detectors.py"
Cohesion: 0.19
Nodes (15): forget(), merge(), normalize_address(), record(), sanitize(), _text(), _trim(), update() (+7 more)

### Community 109 - "getCircularRoiOuterRadius"
Cohesion: 0.25
Nodes (16): overlayRenderController, bindRoiControlInteractions(), applyCircularRoiGeometry(), clampCircularRoiCenterDelta(), clampCircularRoiInnerRadius(), DEFAULT_DIRECTION, getCircularRoiDirection(), getCircularRoiOuterRadius() (+8 more)

### Community 110 - "createFlatfieldController"
Cohesion: 0.19
Nodes (19): applyFlat(), createFlatfieldController(), chooseFile(), fits(), flatCopy(), refresh(), updateState(), useCurrentImage() (+11 more)

### Community 111 - "lzwEncode"
Cohesion: 0.17
Nodes (11): ByteBuffer, GifWriter, LZW_MASKS, lzwEncode(), charOut(), clBlock(), clHash(), flushChar() (+3 more)

### Community 112 - "known_detectors.js"
Cohesion: 0.20
Nodes (17): createKnownDetectors(), announce(), call(), change(), forgetDetector(), MAX_KEPT_DETECTORS, MAX_RECENT_DETECTORS, recordDetector() (+9 more)

### Community 113 - "test_update_download_service.py"
Cohesion: 0.26
Nodes (18): _install_fake_network(), _run_to_completion(), _service(), test_a_checksum_list_whose_signature_fails_is_refused_outright(), test_a_checksum_mismatch_deletes_the_file_and_fails(), test_a_file_absent_from_the_checksum_list_is_reported_as_unverified(), test_a_missing_content_length_leaves_progress_unscaled(), test_a_response_beyond_the_size_cap_is_cut_off() (+10 more)

### Community 114 - "Path"
Cohesion: 0.10
Nodes (10): _get_log_path(), _latest_image_file(), _resolve_dir(), _resolve_file(), _resolve_image_file(), _resolve_optional_path(), _resource_root(), _safe_rel_path() (+2 more)

### Community 115 - "remote_stream.py"
Cohesion: 0.19
Nodes (16): _as_int(), _first_number(), _parse_mythen_cfg(), _mod_number(), remote_extract_metadata(), remote_parse_meta(), _remote_parse_peak_sets(), remote_parse_shape() (+8 more)

### Community 116 - "ndarray"
Cohesion: 0.16
Nodes (12): _decode_fabio_data(), _ensure_tifffile(), _FabioErrorProbe, _normalize_image_array(), _read_cbf(), _read_cbf_gz(), _read_edf(), _read_tiff() (+4 more)

### Community 117 - "test_shipped_licence_metadata.py"
Cohesion: 0.15
Nodes (12): metadata(), _copyleft(), _declared(), _installed_identifiers(), _normalize(), _pins(), _require_installed(), test_no_shipped_dependency_is_strong_copyleft() (+4 more)

### Community 118 - "window_clone_state.js"
Cohesion: 0.19
Nodes (19): duplicateWindow(), restoreCloneFromHash(), createRoiState(), boolOf(), captureWindowState(), claimClonePayload(), CLONE_HASH_PREFIX, CLONE_PAYLOAD_VERSION (+11 more)

### Community 119 - "createSourceMetadataController"
Cohesion: 0.23
Nodes (20): readHeaderText(), createSourceMetadataController(), applyAnalysisMeta(), applyImageMeta(), applyLiveSourceSnapshot(), applyRemoteMeta(), applySimplonMeta(), cloneMetaObject() (+12 more)

### Community 120 - "test_image_geometry_endpoint.py"
Cohesion: 0.25
Nodes (17): test_a_manual_override_still_wins_over_the_built_in_geometry(), test_an_unreadable_geometry_file_next_to_the_data_falls_back_to_the_built_in_one(), test_another_pilatus_12m_is_not_taken_for_the_i23_one(), test_image_geometry_endpoint_allows_manual_override_for_hdf5(), test_image_geometry_endpoint_allows_manual_override_for_tiff(), test_image_geometry_endpoint_falls_back_from_invalid_embedded_hdf5_geometry_to_source(), test_image_geometry_endpoint_falls_back_to_planar_for_missing_manual_override(), test_image_geometry_endpoint_ignores_non_12m_images_even_with_geometry_file() (+9 more)

### Community 121 - "socket"
Cohesion: 0.15
Nodes (12): _bind_auto_port(), _bind_last_port(), _bind_socket(), _create_bound_socket(), _port_answers(), _free_port(), test_a_free_last_port_is_bound_again(), test_a_port_another_program_listens_on_is_left_alone() (+4 more)

### Community 122 - "_DockMenuHandler"
Cohesion: 0.23
Nodes (3): _DockMenuHandler, _log_macos_event(), _normalize_host()

### Community 123 - "test_jfjoch_message_handling.py"
Cohesion: 0.15
Nodes (15): jfjoch_peak_sets_from_spots(), _image_message(), test_a_frame_that_cannot_be_stored_is_reported_not_raised(), test_a_message_with_no_image_stores_nothing(), test_a_spot_list_that_is_not_a_list_is_no_spots(), test_a_successful_frame_clears_an_earlier_error(), test_an_absent_channel_falls_back_to_the_first_one(), test_an_enormous_spot_list_is_capped() (+7 more)

### Community 124 - "createAnalysisOverlayController"
Cohesion: 0.20
Nodes (20): createAnalysisOverlayController(), buildPeakIntegralImages(), detectPeaks(), localBackground(), refineCentroid(), scorePeak(), getDefaultCenter(), getResolutionAtPixel() (+12 more)

### Community 125 - "createPanelLayoutController"
Cohesion: 0.20
Nodes (19): createPanelLayoutController(), applyPanelState(), clampMobilePanelSnap(), getMaxPanelWidth(), initializePanelTabA11y(), initializeSectionContentWrappers(), isPhonePanelLayout(), nearestMobilePanelSnap() (+11 more)

### Community 126 - "argparse"
Cohesion: 0.15
Nodes (9): DmgLayout, main(), build_ico(), collect_inputs(), main(), parse_args(), read_png(), main() (+1 more)

### Community 127 - "test_update_apply_endpoint.py"
Cohesion: 0.20
Nodes (14): _get_backend_busy(), _client(), _reset(), test_apply_is_disabled_by_default(), test_apply_status_reports_the_service_state_and_a_current_refusal(), test_apply_support_is_reported_on_the_update_check(), test_apply_support_is_true_when_nothing_structural_refuses_it(), test_reported_busy_entries_are_filtered_to_real_values() (+6 more)

### Community 128 - "WalkReport"
Cohesion: 0.14
Nodes (4): _dead_master_detail(), datasets(), _remember(), WalkReport

### Community 129 - "test_release_readiness_scenarios.py"
Cohesion: 0.23
Nodes (13): _decode_frame(), _load_smoke_module(), test_hdf5_routes_cover_linked_stack_traversal_and_frames(), test_image_route_roundtrip_and_metadata_across_supported_formats(), test_mask_route_falls_back_to_single_threshold_filewriter2_mask(), test_native_pickers_return_conflict_when_unavailable(), test_packaged_binary_smoke_harness_with_dummy_server(), test_remote_stream_roundtrip_for_encoded_payloads() (+5 more)

### Community 130 - "createResolutionRingInteractionControlle"
Cohesion: 0.17
Nodes (13): createResolutionRingInteractionController(), applyRingEdit(), commitInput(), getPointerCanvasPos(), getRingHandleAt(), getScreenRings(), handleKey(), imageToScreen() (+5 more)

### Community 131 - "ring_geometry_utils.js"
Cohesion: 0.19
Nodes (19): buildGeometryRingSegments(), cross(), distanceToPanelBoundsPx(), dot(), findPanelRayHit(), flushSegment(), INCIDENT_BEAM, intersectRayWithPanel() (+11 more)

### Community 132 - "test_shipped_dependency_parity.py"
Cohesion: 0.15
Nodes (10): _dev_pins(), _normalize(), _pins(), test_every_development_tool_is_pinned(), test_formatting_is_owned_by_one_tool(), test_no_requirement_is_excluded_from_the_docker_install(), test_requirements_are_all_pinned_to_an_exact_version(), test_the_commit_hook_runs_the_same_tool_version_as_ci() (+2 more)

### Community 133 - "analysis_overlay_controller.js"
Cohesion: 0.23
Nodes (13): exportPeakCsv(), buildCsvTable(), csvField(), csvProvenanceLines(), formatCsvNumber(), plotSeriesColumns(), buildRoiCsvExportPayload(), roiCsvExportUnavailableReason() (+5 more)

### Community 134 - "createSeriesSumController"
Cohesion: 0.24
Nodes (19): showConfirmDialog(), createSeriesSumController(), buildMedianEstimate(), cancelSeriesSumming(), defaultSeriesSumOutputPath(), formatGiB(), isTiffPath(), maxFramesPerMedianGroup() (+11 more)

### Community 135 - "createRoiInteractionController"
Cohesion: 0.20
Nodes (17): createRoiInteractionController(), drawRoiHandles(), drawRoiOverlay(), getBoxScreenBounds(), getCircularHandleScreenPoints(), getLineScreenEndpoints(), getPointerCanvasPos(), getRoiHandleAt() (+9 more)

### Community 136 - "test_macos_distribution_scripts.py"
Cohesion: 0.17
Nodes (11): _notarized_path(), _release_jobs(), _step_line(), test_each_release_artifact_is_uploaded_by_one_job_only(), test_stapling_is_retried_before_the_build_fails(), test_the_app_is_notarized_in_its_own_right_not_only_via_the_dmg(), test_the_app_is_stapled_before_the_dmg_seals_a_copy_of_it(), test_the_distributable_zip_is_built_after_the_app_is_stapled() (+3 more)

### Community 137 - "test_detector_profiles.py"
Cohesion: 0.17
Nodes (11): dls_i23_pilatus_12m_geometry(), test_a_missing_header_distance_means_the_blueprint_position(), test_an_exported_cbf_keeps_the_header_distance_dials_expects(), test_every_other_detector_is_left_alone(), test_the_beam_meets_the_blueprint_detector_at_its_radius(), test_the_built_in_geometry_is_the_one_dials_imports(), test_the_header_distance_is_read_as_an_offset_from_the_blueprint(), test_the_image_route_reports_the_ring_distance() (+3 more)

### Community 138 - "command_availability.js"
Cohesion: 0.25
Nodes (9): fileCommandUnavailableReason(), canConvertFile(), canDuplicateWindow(), canExportAnimation(), canExportData(), canStartSeriesOperation(), isLiveExportSource(), LIVE_EXPORT_MODES (+1 more)

### Community 139 - "autoload_mode_controller.js"
Cohesion: 0.22
Nodes (11): createAutoloadModeController(), autoloadSimplonTick(), _autoloadSimplonTickImpl(), autoloadWatchTick(), _autoloadWatchTickImpl(), simplonFrameIdentity(), hashBuffer(), describeSimplonFailure() (+3 more)

### Community 140 - "toast.js"
Cohesion: 0.20
Nodes (16): ASSERTIVE_TYPES, clearTimer(), DEFAULT_DURATIONS, dismissAllToasts(), getRegion(), liveToasts, normalizeType(), notifyError() (+8 more)

### Community 141 - "wA"
Cohesion: 0.20
Nodes (17): A(), B(), n(), r(), B(), cn(), E(), H() (+9 more)

### Community 142 - "test_about_dialog_claims.py"
Cohesion: 0.14
Nodes (9): about_fact(), _license_holders(), route_extensions(), test_about_does_not_advertise_a_format_that_cannot_be_opened(), test_every_copyright_statement_names_the_licence_holders(), test_every_readable_format_is_named_in_about(), test_the_author_affiliation_matches_the_citation_metadata(), test_the_cited_doi_is_the_concept_doi_from_the_readme() (+1 more)

### Community 143 - "test_documented_check_commands.py"
Cohesion: 0.14
Nodes (8): _ci_run_lines(), ci_steps(), _code_block_lines(), test_a_documented_check_block_matches_the_ci_commands(), test_every_documented_coverage_gate_agrees_with_ci(), test_the_ci_pytest_step_still_enables_the_coverage_plugin(), test_the_documented_pytest_command_is_runnable_as_written(), test_the_pull_request_template_checklist_matches_ci()

### Community 144 - "test_unreadable_and_endian_sources.py"
Cohesion: 0.22
Nodes (10): test_frame_endpoint_serves_big_endian_datasets_as_little_endian(), test_intact_images_still_decode(), test_little_endian_sources_are_passed_through_untouched(), test_mask_endpoint_serves_big_endian_pixel_mask(), test_remote_ingest_accepts_big_endian_raw_frames(), test_truncated_fabio_image_reports_422(), test_truncated_hdf5_reports_422(), test_truncated_tiff_reports_422() (+2 more)

### Community 145 - "file_type_utils.js"
Cohesion: 0.18
Nodes (15): isHdf5File(), isHeaderCapableFile(), isSeriesCapable(), CBF_EXTS, EDF_EXTS, HDF_EXTS, HEADER_CAPABLE_EXTS, isHdf5File() (+7 more)

### Community 146 - "post_file_picker_bindings.js"
Cohesion: 0.21
Nodes (10): bindHistogramDragInteractions(), applyHistogramDragValue(), flushHistogramDragFrame(), scheduleHistogramDragFrame(), stopHistDrag(), bindOverviewInteractions(), bindPanelAndSectionInteractions(), initializePostFilePickerBindings() (+2 more)

### Community 147 - "test_hdf5_csv_export.py"
Cohesion: 0.24
Nodes (7): client(), _split(), test_a_newline_in_a_group_name_cannot_forge_a_row(), test_a_one_dimensional_dataset_keeps_its_index_and_value_header(), test_csv_leads_with_provenance_then_one_table(), test_the_truncated_marker_is_in_the_prefix_not_after_the_last_row(), _write()

### Community 148 - "test_request_guard.py"
Cohesion: 0.12
Nodes (7): test_a_wildcard_bind_rejects_a_rebound_name(), test_a_wildcard_bind_serves_lan_clients_by_address(), test_cross_site_reads_are_still_served(), test_cross_site_upload_cannot_write_a_file(), test_remote_stream_ingest_still_works_for_a_script(), test_the_frontend_own_requests_are_allowed(), test_the_guarded_stream_routes_are_the_ones_that_reach_a_third_host()

### Community 149 - "test_host_env.py"
Cohesion: 0.20
Nodes (8): _open_browser(), restore_host_library_path(), test_a_second_call_changes_nothing(), test_an_opened_browser_needs_no_message(), test_no_library_path_before_means_none_after(), test_only_a_packaged_linux_build_is_touched(), test_the_users_own_library_path_comes_back(), test_when_no_browser_opens_the_launcher_says_where_to_point_one()

### Community 150 - "register_analysis_routes"
Cohesion: 0.23
Nodes (13): AnalysisParamsResponse, SeriesSumCancelRequest, SeriesSumCancelResponse, SeriesSumStartRequest, SeriesSumStartResponse, AnalysisRouteDeps, register_analysis_routes(), analysis_params() (+5 more)

### Community 151 - "JungfraujochPreviewBridge"
Cohesion: 0.23
Nodes (5): _as_int(), _BridgeConfig, JungfraujochPreviewBridge, bridge(), test_as_int_truncates_and_rejects_the_unusable()

### Community 152 - "attachDetectorPicker"
Cohesion: 0.34
Nodes (15): attachDetectorPicker(), close(), iconButton(), labelToggle(), nameEditor(), options(), pick(), render() (+7 more)

### Community 153 - "recent_files_controller.test.js"
Cohesion: 0.22
Nodes (12): createRecentFiles(), clear(), list(), persist(), record(), remove(), parseEntries(), readStored() (+4 more)

### Community 154 - "h5py"
Cohesion: 0.21
Nodes (6): main(), test_an_i23_source_image_reports_the_distance_the_rings_need(), test_analysis_params_inherits_missing_values_from_source_image(), test_analysis_params_preserves_anisotropic_strixel_pixel_sizes(), _write_cbf(), test_plain_open_is_refused_so_the_retry_is_doing_the_work()

### Community 155 - "generate_installer_images.py"
Cohesion: 0.24
Nodes (8): back_image(), banner(), gradient(), icon(), main(), ridge(), small_image(), with_shadow()

### Community 156 - "test_linux_glibc_floor.py"
Cohesion: 0.26
Nodes (10): _jobs(), _run_text(), test_a_release_waits_for_the_distribution_smoke_test(), test_both_workflows_build_in_the_same_image(), test_linux_builds_in_a_pinned_image_matching_the_floor(), test_the_build_interpreter_is_the_supported_python(), test_the_glibc_check_enforces_the_floor(), test_the_readme_states_the_floor_the_build_enforces() (+2 more)

### Community 157 - "drive"
Cohesion: 0.16
Nodes (8): chunks(), drive(), send(), make_app(), sample_app(), test_loopback_client_is_not_compressed_end_to_end(), test_non_http_scopes_pass_straight_through(), test_off_mode_sends_raw_bytes_to_a_remote_client()

### Community 158 - "test_api_smoke.py"
Cohesion: 0.14
Nodes (3): _register_static_mime_types(), test_frontend_module_entrypoint_is_served_as_javascript(), test_upload_returns_503_when_target_is_read_only()

### Community 159 - "_load_dials_expt_geometry"
Cohesion: 0.25
Nodes (9): _load_dials_expt_geometry(), _rotation_to_albis(), beam_hit(), test_a_file_without_a_beam_is_taken_as_already_in_albis_frame(), test_a_flat_detector_faces_the_beam_where_its_header_says(), test_a_tilted_beam_is_turned_onto_z(), test_the_example_in_the_power_user_guide_works(), test_the_i23_hierarchy_is_composed_and_turned_to_the_beam() (+1 more)

### Community 160 - "register_detector_control_routes"
Cohesion: 0.21
Nodes (11): register_detector_control_routes(), detector_command(), detector_config(), detector_describe(), detector_file_download(), detector_files(), detector_series_fetch(), detector_status() (+3 more)

### Community 161 - "DownloadRefusedError"
Cohesion: 0.19
Nodes (6): DownloadRefusedError, DownloadState, validate_asset_name(), validate_asset_url(), test_asset_name_must_be_a_plain_filename(), test_asset_url_must_be_a_github_release_download()

### Community 162 - "version.py"
Cohesion: 0.19
Nodes (6): read_version(), _repo_root(), _version_file_candidates(), test_read_version_prefers_pyinstaller_runtime_version_file(), test_read_version_returns_default_when_no_valid_version_file(), test_the_version_override_only_moves_the_comparison()

### Community 164 - "createOverlayRenderController"
Cohesion: 0.26
Nodes (14): screenView(), createOverlayRenderController(), clearPixelOverlay(), currentView(), deferPixelOverlayRedraw(), drawPeakOverlay(), drawPixelOverlay(), drawResolutionOverlay() (+6 more)

### Community 165 - "createRenderEngineController"
Cohesion: 0.24
Nodes (11): createRenderEngineController(), createCpuRenderer(), createProgram(), createShader(), createWebGLRenderer(), initRenderer(), getWebglRenderCompatibility(), SATURATED_PIXEL_RGBA (+3 more)

### Community 166 - "generate_dmg_background.py"
Cohesion: 0.28
Nodes (8): build(), drop_shadow(), faceted_arrow(), faceted_ridge(), main(), radial_glow(), Theme, vertical_gradient()

### Community 167 - "test_handoff_api.py"
Cohesion: 0.23
Nodes (8): _isolate_handoff_state(), test_handoff_create_and_latest_roundtrip(), test_handoff_create_honours_the_absolute_path_gate(), test_handoff_create_rejects_a_manifest_that_is_not_json(), test_handoff_create_reports_a_missing_manifest_as_not_found(), test_handoff_create_still_accepts_absolute_manifests_by_default(), test_handoff_queue_cap_evicts_oldest_jobs(), _write_manifest()

### Community 168 - "test_path_policy.py"
Cohesion: 0.32
Nodes (11): _policy(), test_parse_ext_filter_normalizes_and_intersects(), test_resolve_dir_resolves_default_and_relative(), test_resolve_hdf5_file_allows_relative_and_validates_suffix(), test_resolve_hdf5_file_blocks_absolute_when_disabled(), test_resolve_image_file_supports_compound_extensions(), test_resolve_manifest_file_accepts_absolute_json(), test_resolve_manifest_file_confines_relative_names_to_the_data_dir() (+3 more)

### Community 169 - "test_version_consistency.py"
Cohesion: 0.15
Nodes (8): _citation_version(), _package_json_version(), _pyproject_version(), test_citation_file_is_valid_yaml(), test_citation_metadata_has_what_a_citation_needs(), test_version_file_is_a_release_version(), test_version_matches_the_version_file(), _version_file()

### Community 170 - "ResponseCompressionMiddleware"
Cohesion: 0.19
Nodes (8): is_loopback_client(), ResponseCompressionMiddleware, test_a_client_that_is_absent_from_the_scope_is_treated_as_remote(), test_loopback_detection(), test_mode_decides_when_compression_engages(), test_negotiation_picks_the_best_mutually_supported_encoding(), test_on_mode_covers_the_reverse_proxy_case(), test_unrecognized_mode_falls_back_to_auto()

### Community 172 - "test_update_download_endpoint.py"
Cohesion: 0.33
Nodes (11): ReleaseAsset, _client(), _reset(), test_download_status_starts_idle(), test_reveal_is_a_404_until_a_verified_download_exists(), test_reveal_shows_the_installer_without_opening_it(), test_start_is_refused_when_the_setting_is_off(), test_start_rejects_a_url_or_name_the_backend_would_never_have_offered() (+3 more)

### Community 173 - "data_export_controller.js"
Cohesion: 0.24
Nodes (13): defaultOutputDir(), defaultOutputPrefix(), dirname(), DTYPE_OF_ARRAY, fileStem(), finiteNumber(), LIVE_PREFIX, liveFrameDtype() (+5 more)

### Community 174 - "bootstrap_macos_signing_ci.sh"
Cohesion: 0.31
Nodes (13): base64_file(), detect_signing_identity(), die(), history_value(), log(), require_command(), resolve_cert_path(), resolve_repo() (+5 more)

### Community 175 - "HandoffQueueService"
Cohesion: 0.29
Nodes (4): HandoffQueueService, test_handoff_queue_assigns_ids_and_returns_latest_match(), test_handoff_queue_evicts_oldest_jobs_at_capacity(), test_handoff_queue_snapshot_and_latest_return_copies()

### Community 176 - "checksums_url_for"
Cohesion: 0.15
Nodes (6): _build_opener(), checksums_url_for(), DownloadFailedError, _HttpsOnlyRedirectHandler, test_an_https_downgrade_on_redirect_is_refused(), test_checksum_list_is_resolved_inside_the_same_release()

### Community 177 - "autoload_controls.js"
Cohesion: 0.31
Nodes (10): bindAutoloadControls(), describeJfjochEndpointProblem(), MALFORMED_SCHEME_PATTERN, normalizeJfjochEndpointInput(), ZMQ_SCHEMES, createJfjochProbeController(), clearJfjochProbeMessage(), describeFailure() (+2 more)

### Community 178 - "update_check_controller.test.js"
Cohesion: 0.23
Nodes (7): createMenuActionHandler(), buildFetchMock(), initializeModules(), renderModalShell(), buildFetchMock(), initializeModules(), renderModalShell()

### Community 179 - "state.js"
Cohesion: 0.29
Nodes (12): createAnimationExportState(), createAppState(), createAutoloadState(), createDataExportState(), createFrameDataState(), createMaskState(), createSeriesSumState(), createSourceState() (+4 more)

### Community 180 - "bindViewportInteractions"
Cohesion: 0.18
Nodes (4): bindViewportInteractions(), onMove(), onUp(), bind()

### Community 181 - "verify_macos_distribution.sh"
Cohesion: 0.47
Nodes (11): detach_mounted_dmg(), die(), log(), require_command(), verify_macos_distribution.sh script, validate_ticket(), verify_app(), verify_developer_id_signature_info() (+3 more)

### Community 182 - "is_cross_site_request"
Cohesion: 0.23
Nodes (7): is_cross_site_request(), _headers(), test_a_client_that_is_not_a_browser_is_left_alone(), test_browser_reporting_another_site_is_cross_site(), test_frontend_and_user_navigation_are_not_cross_site(), test_origin_is_the_fallback_when_sec_fetch_site_is_absent(), test_sec_fetch_site_is_trusted_over_a_spoofable_origin()

### Community 183 - "resetLiveHistory"
Cohesion: 0.17
Nodes (12): autoloadJfjochTick(), autoloadModeController, autoloadOrchestrationController, autoloadRemoteTick(), autoloadSimplonTick(), autoloadWatchTick(), closeCurrentFile(), loadAutoloadFile() (+4 more)

### Community 184 - "createCommandPaletteController"
Cohesion: 0.33
Nodes (9): createCommandPaletteController(), close(), execute(), filterCommands(), handleKeydown(), isOpen(), open(), render() (+1 more)

### Community 185 - "createFrameCache"
Cohesion: 0.32
Nodes (9): createFrameCache(), budget(), clear(), evictDownTo(), get(), set(), stats(), cacheWith() (+1 more)

### Community 186 - "roi_plot_renderer.js"
Cohesion: 0.24
Nodes (10): D_AXIS_TICK_FONT, D_AXIS_TITLE_FONT, formatResolutionLabel(), PEAK_LABEL_FONT, PLOT_LABEL_FONT, renderRoiPlot(), resolutionDisplayValue(), createMockContext() (+2 more)

### Community 187 - "createThresholdPlaybackController"
Cohesion: 0.26
Nodes (6): createThresholdPlaybackController(), getDefaultThresholdIndex(), getThresholdDisplayOrder(), getThresholdIndexAtOffset(), updatePlayButtons(), updateThresholdOptions()

### Community 188 - "_as_float"
Cohesion: 0.25
Nodes (6): _as_float(), _Store, test_a_malformed_multidim_array_is_refused(), test_a_message_with_no_usable_image_yields_nothing(), test_as_float_accepts_what_a_cbor_message_can_carry(), test_as_float_rejects_what_cannot_be_a_measurement()

### Community 189 - "app_binding_contexts.js"
Cohesion: 0.20
Nodes (9): initializeMainUiBindings(), createFileBrowserControllerContext(), createMainUiBindingsCallbacks(), createMainUiBindingsContext(), createMainUiBindingsElements(), createPostFilePickerBindingsCallbacks(), createPostFilePickerBindingsContext(), createPostFilePickerBindingsElements() (+1 more)

### Community 190 - "canvasFont"
Cohesion: 0.29
Nodes (8): CANVAS_FONT_STACK, canvasFont(), whenCanvasFontReady(), createHistogramRenderController(), clearHistogram(), drawColorbar(), drawHistogram(), histogramValueToX()

### Community 191 - "uploadAndOpenSelectedFiles"
Cohesion: 0.33
Nodes (10): createUploadFlowController(), findExistingFile(), hideUploadProgress(), showUploadProgress(), updateUploadProgress(), uploadAndOpenSelectedFiles(), isSupportedUploadFile(), sortFilesForSeriesUpload() (+2 more)

### Community 192 - "detector_control_controller.test.js"
Cohesion: 0.31
Nodes (8): armed(), buildElements(), DESCRIPTORS, EN, loadModule(), panel(), setup(), thresholds()

### Community 193 - "_frame"
Cohesion: 0.27
Nodes (7): _frame(), frame_file(), test_a_zstd_frame_from_the_real_app_decodes_to_the_raw_frame(), test_client_without_compression_support_still_gets_a_readable_frame(), test_decode_headers_survive_compression(), test_loopback_client_is_not_compressed(), test_remote_client_receives_a_compressed_frame()

### Community 194 - "register_data_export_routes"
Cohesion: 0.29
Nodes (8): DataExportCancelResponse, DataExportFrameResponse, DataExportStartResponse, register_data_export_routes(), data_export_cancel(), data_export_frame(), data_export_start(), flatfield_save()

### Community 195 - "request_guard.py"
Cohesion: 0.22
Nodes (4): _log_network_exposure(), is_loopback_host(), strip_port(), test_strip_port_handles_ipv6_and_ports()

### Community 196 - "local_host_names"
Cohesion: 0.22
Nodes (5): local_host_names(), reset_local_host_names_cache(), test_a_wildcard_bind_serves_this_machine_by_name(), test_local_host_names_is_cached_then_refreshed(), test_local_host_names_reports_this_machine()

### Community 197 - "register_known_detector_routes"
Cohesion: 0.40
Nodes (9): register_known_detector_routes(), address_of(), answer(), change(), known_detectors_forget(), known_detectors_import(), known_detectors_list(), known_detectors_seen() (+1 more)

### Community 198 - "openMenu"
Cohesion: 0.20
Nodes (10): closeMenu(), closeSubmenus(), closeToolbarMorePopover(), closeToolbarPlaybackPopover(), openAboutModal(), openMenu(), scheduleClose(), setActiveMenu() (+2 more)

### Community 199 - "normalizeSimplonBaseUrl"
Cohesion: 0.36
Nodes (6): createAutoloadSettingsController(), loadAutoloadSettings(), placeAutoloadStatusBlock(), updateAutoloadUI(), normalizeJfjochEndpoint(), normalizeSimplonBaseUrl()

### Community 200 - "dialogs.js"
Cohesion: 0.31
Nodes (8): getFocusable(), openDialog(), accept(), cancel(), close(), getOverlay(), loadDialogs(), pressKey()

### Community 201 - "shortcut_handlers.test.js"
Cohesion: 0.33
Nodes (7): createShortcutHandlers(), handleNavShortcut(), handleShortcut(), FOCUSABLE_SELECTOR, isFocusableChrome(), isFormElement(), makeHandlers()

### Community 202 - "sign_macos.sh"
Cohesion: 0.38
Nodes (8): build_dmg(), create_zip(), decode_cert_payload(), make_app_zip(), notarize(), resolve_identity_common_name(), sign_macos.sh script, staple()

### Community 203 - "test_browse_endpoint.py"
Cohesion: 0.36
Nodes (8): test_browse_endpoint_collapses_hdf5_master_data_series(), test_browse_endpoint_flags_missing_requested_path(), test_browse_endpoint_groups_series_and_sorts_naturally(), test_browse_endpoint_hdf5_data_only_series_uses_first_as_lead(), test_browse_endpoint_returns_parent_path_and_rich_items(), test_browse_endpoint_sorts_by_type_and_size(), test_browse_endpoint_supports_expt_extension_filter(), test_choose_file_endpoint_supports_expt_extension_filter()

### Community 204 - "_windows_install_kind"
Cohesion: 0.22
Nodes (6): _executable_dir(), _windows_install_kind(), test_windows_install_falls_back_to_the_installer_when_the_path_is_unusable(), test_windows_install_registered_at_the_running_location_is_the_installer(), test_windows_install_running_from_outside_the_registered_location_is_portable(), test_windows_install_without_an_uninstall_key_is_the_portable_zip()

### Community 205 - "test_a_detached_signature_verifies_only_"
Cohesion: 0.25
Nodes (5): verify_detached_signature(), _short_temp_root(), test_a_detached_signature_verifies_only_under_the_bundled_key(), make_key(), run()

### Community 206 - "createLiveSeriesWatch"
Cohesion: 0.47
Nodes (7): createLiveSeriesWatch(), isRunning(), setWriterPresent(), start(), stop(), tick(), build()

### Community 208 - "test_documented_entry_points.py"
Cohesion: 0.31
Nodes (4): _free_port(), _health(), _serving(), test_a_documented_command_serves_the_health_endpoint()

### Community 209 - "test_jfjoch_preview.py"
Cohesion: 0.31
Nodes (4): _decode_error_type(), test_jfjoch_multi_dim_array_rejects_invalid_dimensions(), test_jfjoch_preview_control_endpoints_lifecycle(), test_jfjoch_typed_array_rejects_non_bytes_payload()

### Community 210 - "test_static_cache_headers.py"
Cohesion: 0.33
Nodes (5): _client(), test_api_responses_keep_their_own_cache_semantics(), test_entry_documents_are_never_stored(), test_module_assets_are_revalidated_rather_than_uncacheable(), test_unchanged_asset_revalidates_to_an_empty_304()

### Community 212 - "applyBrowsePaneWidth"
Cohesion: 0.29
Nodes (8): applyBrowsePaneWidth(), clampBrowsePaneWidth(), isStackedBrowseLayout(), onMove(), onUp(), persistBrowsePaneWidth(), persistFilesystemMode(), writeStoredValue()

### Community 213 - "createDetailsHeader"
Cohesion: 0.25
Nodes (8): applySort(), canSortByMtime(), canSortBySize(), createDetailsHeader(), isSortModeAvailable(), persistBrowseControlPreferences(), sortForColumnClick(), syncBrowseSortState()

### Community 214 - "createInfoTips"
Cohesion: 0.57
Nodes (8): createInfoTips(), bind(), ensureBubble(), hide(), position(), refresh(), show(), text()

### Community 215 - "data_export_controller.test.js"
Cohesion: 0.39
Nodes (7): build(), buildLive(), dialogElements(), EN, isHdfFile(), liveState(), makeState()

### Community 216 - "roi_plot_bindings.test.js"
Cohesion: 0.36
Nodes (5): clearAxisLimits(), createBindings(), createCanvas(), createPlotContainer(), setAxisLimits()

### Community 217 - "version_info.py"
Cohesion: 0.46
Nodes (7): _appimage_arch_for_target_arch(), detect_target(), main(), _normalize_target_arch(), _normalize_target_os(), read_commit(), read_version()

### Community 218 - "test_log_tail_api.py"
Cohesion: 0.54
Nodes (5): _configure_log_path(), test_log_tail_clamps_requested_line_count(), test_log_tail_does_not_use_full_file_convenience_reads(), test_log_tail_empty_file_returns_empty_payload(), test_log_tail_returns_recent_lines_and_metadata()

### Community 219 - "MonkeyPatch"
Cohesion: 0.29
Nodes (4): loopback_bind(), test_a_deployment_reached_by_its_own_name_says_so(), test_configured_allowed_host_is_served(), wildcard_bind()

### Community 221 - "_compose_expt_frame"
Cohesion: 0.29
Nodes (4): _compose_expt_frame(), _expt_frame(), _expt_lab_frames(), walk()

### Community 224 - "getLanguage"
Cohesion: 0.43
Nodes (7): browseOutputDir(), createFileOpenController(), openFileModal(), openPathInViewer(), runOpenFileModal(), getLanguage(), browseIntoPathField()

### Community 225 - "createPointerFocusRelease"
Cohesion: 0.43
Nodes (4): createPointerFocusRelease(), attach(), detach(), handleClick()

### Community 226 - "file_browser.test.js"
Cohesion: 0.48
Nodes (4): buildBrowseDom(), buildFetchMock(), createController(), jsonResponse()

### Community 228 - "smoke_windows_installer.ps1"
Cohesion: 0.38
Nodes (3): Get-FreeTcpPort(), Start-AlbisBackgroundProcess(), Wait-ForHealth()

### Community 233 - "settings_controller.test.js"
Cohesion: 0.60
Nodes (5): buildFetchMock(), initializeController(), readLocale(), renderSettingsShell(), { showConfirmDialogMock }

### Community 234 - "_FakeResponse"
Cohesion: 0.33
Nodes (3): _FakeResponse, open(), open()

### Community 235 - "parse_accept_encoding"
Cohesion: 0.40
Nodes (3): parse_accept_encoding(), test_accept_encoding_parsing_handles_whitespace_and_case(), test_malformed_quality_is_treated_as_unacceptable()

### Community 237 - "inspector_tree_truncation.test.js"
Cohesion: 0.50
Nodes (3): BIG, controllerWith(), EN

### Community 240 - "image_export_controller.test.js"
Cohesion: 0.50
Nodes (3): EN, pngBlob(), setup()

### Community 241 - "build_mac.sh"
Cohesion: 0.50
Nodes (4): add_icon(), ALBIS_BUNDLE_BUILD, ALBIS_BUNDLE_VERSION, build_mac.sh script

### Community 243 - "updateInspectorHeaderVisibility"
Cohesion: 0.50
Nodes (4): setDataControlsForHdf5(), setDataControlsForImage(), setDataControlsForSeries(), updateInspectorHeaderVisibility()

### Community 244 - "effectiveExtsParam"
Cohesion: 0.50
Nodes (4): effectiveExtsParam(), getActiveBrowseExts(), loadBrowseDirectory(), serializeExts()

### Community 245 - "te"
Cohesion: 0.50
Nodes (4): ee(), He(), te(), ye()

### Community 246 - "gs"
Cohesion: 0.50
Nodes (4): gs(), ns(), rs(), ts()

### Community 248 - "fetchJfjochPreviewStatus"
Cohesion: 0.67
Nodes (3): fetchJfjochPreviewStatus(), remoteStreamController, startJfjochPreviewBridge()

### Community 249 - "handleLocalFileSelection"
Cohesion: 0.67
Nodes (3): handleLocalFileSelection(), persistAutoloadSettings(), setAutoloadStatus()

### Community 250 - "syncOverlayAnchors"
Cohesion: 0.67
Nodes (3): syncOverlayAnchors(), syncToolbarMoreControls(), updateFullscreenUi()

### Community 252 - "UA"
Cohesion: 0.67
Nodes (3): FA(), lA(), UA()

### Community 253 - "xB"
Cohesion: 0.67
Nodes (3): fe(), oe(), xB()

## Knowledge Gaps
- **5 isolated node(s):** `eslint`, `dectris-albis`, `ALBIS_BUNDLE_VERSION`, `ALBIS_BUNDLE_BUILD`, `GNUPGHOME`
  These have ≤1 connection - possible missing edges. (Counts symbols only; 2019 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **182 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `t()` connect `Frontend Controller Wiring` to `Frontend App Core`, `Detector Control UI State`, `analysis_overlay_controller.js`, `Update Check & Log Viewer`, `createSeriesSumController`, `command_availability.js`, `autoload_mode_controller.js`, `Detector Param Editing`, `toast.js`, `Viewer Sync Controller`, `ROI Stats Controller`, `post_file_picker_bindings.js`, `i18n & Settings`, `attachDetectorPicker`, `Overlay Painters`, `Window Handoff & Commands`, `Image Export`, `Export Splash Controller`, `createRenderEngineController`, `ROI & Peaks UI Sync`, `data_export_controller.js`, `createGeometryParamsController`, `autoload_controls.js`, `createAnimationExportController`, `main_ui_bindings_bootstrap.js`, `createChromeToolbarController`, `createCommandPaletteController`, `file_browser.js`, `loadAndRenderBrowser`, `roi_stats_engine.js`, `createThresholdPlaybackController`, `prepareRingGeometry`, `normalizeSimplonUrlInput`, `vitest`, `uploadAndOpenSelectedFiles`, `createFileDataPipelineController`, `setPanelTab`, `normalizeSimplonBaseUrl`, `dialogs.js`, `createDataExportController`, `createFileBrowserController`, `createHelpTooltipController`, `chrome_toolbar_controller.js`, `createDetailsHeader`, `geometry_params_controller.js`, `createInfoTips`, `createLiveHistoryController`, `createMaskCursorController`, `createFramePlaybackController`, `getLanguage`, `bindRoiPlotInteractions`, `update_check_controller.js`, `inpainting.js`, `createInspectorPanelController`, `createSettingsController`, `inspector_tree_truncation.test.js`, `createFlatfieldController`, `jfjoch_probe_controller.js`, `getCircularRoiOuterRadius`, `window_clone_state.js`, `createSourceMetadataController`, `handleLocalFileSelection`, `syncOverlayAnchors`, `runtime_bootstrap.js`, `createAnalysisOverlayController`, `createPanelLayoutController`?**
  _High betweenness centrality (0.046) - this node is a cross-community bridge._
- **What connects `eslint`, `dectris-albis`, `ALBIS_BUNDLE_VERSION` to the rest of the system?**
  _5 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Frontend App Core` be split into smaller, more focused modules?**
  _Cohesion score 0.0034423407917383822 - nodes in this community are weakly interconnected._
- **Why does `HDF5StackService` connect `HDF5 Stack Service` to `WalkReport`, `Backend App Imports`, `HDF5 Orphaned Master`, `HDF5 Storage Confinement`, `._resolution_from_start`, `.dangling_link_target`?**
  _High betweenness centrality (0.017) - this node is a cross-community bridge._
- **Should `Data Export Service` be split into smaller, more focused modules?**
  _Cohesion score 0.060784313725490195 - nodes in this community are weakly interconnected._
- **Why does `DataExportService` connect `Data Export Service` to `Backend App Imports`?**
  _High betweenness centrality (0.015) - this node is a cross-community bridge._
- **Should `Detector Control UI State` be split into smaller, more focused modules?**
  _Cohesion score 0.09087958455047063 - nodes in this community are weakly interconnected._