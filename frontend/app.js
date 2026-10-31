/*
 * ALBIS frontend controller.
 *
 * This file drives:
 * - UI state and interactions (menus, tabs, shortcuts, gestures)
 * - Data-source orchestration (file/monitor/remote/JUNGFRAUJOCH + optional watch)
 * - Rendering (WebGL2 primary + CPU fallback)
 * - Overlay layers (ROI, rings, peaks, pixel labels, histogram)
 *
 * Use `docs/CODE_MAP.md` for a quick file-level navigation guide.
 */

import { API, fetchJSON, fetchJSONWithInit } from "./modules/http.js";
import { PIXEL_LABEL_MIN_CELL_PX, pixelLabelResolver, pixelLabelsAreFloat } from "./modules/overlay_painters.js";
import { createAnalysisState, createAppState, createRoiState } from "./modules/state.js";
import { queryAppElements } from "./modules/app_elements.js";
import { computeMedian, parseShape, toFloat32, typedArrayFrom } from "./modules/typed_array_utils.js";
import { dirnameFromPath, formatEnergy, formatInspectorValue, formatRoiTick, formatStat, formatTimeStamp, middleTruncate } from "./modules/format_utils.js";
import {
  applyI18nToDom,
  getLanguage,
  initializeI18n,
  onLanguageChange,
  setLanguage,
  t,
} from "./modules/i18n.js";
import { notifyError, notifySuccess, notifyWarning } from "./modules/toast.js";
import { applyPanelTab, loadStoredPanelTab } from "./modules/ui_panels.js";
import { createFileBrowserController } from "./modules/file_browser.js";
import { bindAnalysisControlInteractions } from "./modules/analysis_controls_bindings.js";
import { createHelpTooltipController } from "./modules/help_tooltips.js";
import { createInfoTips } from "./modules/info_tips.js";
import { finalizeRuntimeBootstrap, initializeUiDefaults } from "./modules/runtime_bootstrap.js";
import { bindClientLogging } from "./modules/client_logging_bindings.js";
import { createCommandPaletteController } from "./modules/command_palette.js";
import { createSettingsController } from "./modules/settings_controller.js";
import { createModalManager } from "./modules/modal_manager.js";
import { createUpdateCheckController } from "./modules/update_check_controller.js";
import { createBackendLogViewerController } from "./modules/backend_log_viewer_controller.js";
import { buildCommandPaletteCommands } from "./modules/command_palette_commands.js";
import { createUploadFlowController } from "./modules/upload_flow.js";
import { createMenuActionHandler } from "./modules/menu_actions.js";
import { createShortcutHandlers } from "./modules/shortcut_handlers.js";
import { createPointerFocusRelease } from "./modules/pointer_focus_release.js";
import { attachAllNumberSteppers } from "./modules/number_stepper.js";
import { attachRangeFill } from "./modules/range_fill.js";
import { createFileOpenController } from "./modules/file_open_flow.js";
import { createLiveSeriesWatch } from "./modules/live_series_watch.js";
import { createRecentFiles } from "./modules/recent_files.js";
import { createWindowCloneController } from "./modules/window_clone_controller.js";
import { createHandoffController } from "./modules/handoff_controller.js";
import { createClientLog, formatClientArg } from "./modules/client_log.js";
import { createRecentFilesController } from "./modules/recent_files_controller.js";
import { createSeriesSumController } from "./modules/series_sum_controller.js";
import { createDataExportController } from "./modules/data_export_controller.js";
import { createAnimationExportController } from "./modules/animation_export_controller.js";
import {
  canExportAnimation,
  canDuplicateWindow,
  canExportData,
  isLiveExportSource,
  canSaveImage,
} from "./modules/command_availability.js";
import { createBackendStatusController } from "./modules/backend_status_controller.js";
import { createJfjochBridgeController } from "./modules/jfjoch_bridge_controller.js";
import { createRemoteStreamController } from "./modules/remote_stream_controller.js";
import { createAutoloadModeController } from "./modules/autoload_mode_controller.js";
import { createAutoloadOrchestrationController } from "./modules/autoload_orchestration_controller.js";
import { createAutoloadSettingsController } from "./modules/autoload_settings_controller.js";
import { createAutoloadStatusController } from "./modules/autoload_status_controller.js";
import { createFileDataPipelineController } from "./modules/file_data_pipeline_controller.js";
import { createRoiStatsController } from "./modules/roi_stats_controller.js";
import { roiCsvExportUnavailableReason } from "./modules/roi_csv_export.js";
import { setControlAvailability } from "./modules/control_availability.js";
import { createOverlayRenderController } from "./modules/overlay_render_controller.js";
import { createHistogramRenderController } from "./modules/histogram_render_controller.js";
import { createRenderEngineController } from "./modules/render_engine_controller.js";
import { createOverviewViewportController } from "./modules/overview_viewport_controller.js";
import { getCircularRoiOuterRadius, getRoiImageBounds } from "./modules/roi_geometry_utils.js";
import { createImageExportController } from "./modules/image_export_controller.js";
import { createDetectorControlController } from "./modules/detector_control_controller.js";
import { createPixelMaskController, hostOf } from "./modules/pixel_mask_controller.js";
import { createPixelMaskEditController } from "./modules/pixel_mask_edit_controller.js";
import { knownDetectors } from "./modules/known_detectors.js";
import { createInpaintView } from "./modules/inpainting.js";
import { createInpaintingController } from "./modules/inpainting_controller.js";
import { startPresence } from "./modules/presence.js";
import { bindColorbarWindowInteractions } from "./modules/colorbar_window_bindings.js";
import { createMaskLegend } from "./modules/mask_legend.js";
import { currentMaskColors } from "./modules/mask_colors.js";
import { createRingListController } from "./modules/ring_list_controller.js";
import { createAnalysisToolsController } from "./modules/analysis_tools.js";
import { createFlatfieldController } from "./modules/flatfield_controller.js";
import { createMagnifierController } from "./modules/magnifier_controller.js";
import { createQuitController } from "./modules/quit_controller.js";
import { whenCanvasFontReady } from "./modules/canvas_fonts.js";
import { createViewerSyncController } from "./modules/viewer_sync_controller.js";
import { createFramePlaybackController } from "./modules/frame_playback_controller.js";
import { createFrameMetadataController } from "./modules/frame_metadata_controller.js";
import { createInspectorPanelController } from "./modules/inspector_panel_controller.js";
import { createAnalysisOverlayController } from "./modules/analysis_overlay_controller.js";
import { createMaskCursorController } from "./modules/mask_cursor_controller.js";
import { createRoiInteractionController } from "./modules/roi_interaction_controller.js";
import { createResolutionRingInteractionController } from "./modules/resolution_ring_interaction_controller.js";
import { createSourceMetadataController } from "./modules/source_metadata_controller.js";
import { createExportSplashController } from "./modules/export_splash_controller.js";
import { createChromeToolbarController } from "./modules/chrome_toolbar_controller.js";
import { createPanelLayoutController } from "./modules/panel_layout_controller.js";
import { createThresholdPlaybackController } from "./modules/threshold_playback_controller.js";
import { createFileSessionController } from "./modules/file_session_controller.js";
import { createLiveHistoryController } from "./modules/live_history_controller.js";
import {
  buildGeometryRequestKey,
  getGeometryScopeKey,
} from "./modules/geometry_override_utils.js";
import { applyGeometryOverrides, serializeGeometryPayload } from "./modules/ring_geometry_utils.js";
import { createGeometryParamsController } from "./modules/geometry_params_controller.js";
import { initializeMainUiBindings as initializeMainUiBindingsBootstrap } from "./modules/main_ui_bindings_bootstrap.js";
import { initializePostFilePickerBindings } from "./modules/post_file_picker_bindings.js";
import {
  createMainUiBindingsElements,
  createMainUiBindingsCallbacks,
  createMainUiBindingsContext,
  createPostFilePickerBindingsElements,
  createPostFilePickerBindingsCallbacks,
  createPostFilePickerBindingsContext,
  createFileBrowserControllerContext,
  createRuntimeBootstrapContext,
} from "./modules/app_binding_contexts.js";
import {
  getWebglUnsignedDtypeKey,
  isWebglUnsignedRawCandidate,
  getWebglUnsignedUploadInfo,
  getDtypeInfo,
  formatPixelLabelValue as formatPixelLabelValueUtil,
  computeAutoLevels as computeAutoLevelsUtil,
  computeStats as computeStatsUtil,
  getPaletteColorCount,
  mapValueToNorm as mapValueToNormUtil,
  buildPalette,
} from "./modules/intensity_scale_utils.js";
import {
  isHdf5File,
  isHdfFile,
  isHeaderCapableFile,
  isSeriesCapableFile as isSeriesCapableFileUtil,
} from "./modules/file_type_utils.js";

await initializeI18n();

const platformHint = String(
  navigator.userAgentData?.platform || navigator.platform || navigator.userAgent || "",
).toLowerCase();
const isMacPlatform = platformHint.includes("mac");
const isWindowsPlatform = platformHint.includes("windows") || platformHint.includes("win32") || platformHint.includes("win64");
const isLinuxPlatform = platformHint.includes("linux");
if (isWindowsPlatform) {
  document.body?.classList.add("platform-windows");
} else if (isLinuxPlatform) {
  document.body?.classList.add("platform-linux");
}

// The elements index.html declares by id (modules/app_elements.js).
const appElements = queryAppElements();
// The elements app.js works with itself; controllers get them all as
// `...appElements` and take what they need.
const {
  datasetSelect, thresholdSelect, toolbarThresholdSelect, toolbarPlaybackToggle, toolbarPlaybackPopover,
  toolbarMoreStep, toolbarMoreFps, viewerSyncWrap, viewerSyncToggle, viewerSyncOptionsToggle,
  viewerSyncPopover, viewerSyncPositionToggle, viewerSyncContrastToggle, viewerSyncRoiToggle, frameRange,
  frameIndex, frameStep, fpsSelect, autoScaleToggle, minInput, maxInput, statusEl, loadingEl, metaRenderer,
  toolbarPath, fullscreenToggle, invertToggle, colormapSelect, toolsPanel, canvasWrap, canvas, pixelOverlay,
  roiOverlay, histCanvas, histColorbar, overviewCanvas, pixelLabelToggle, splashCanvas, resolutionOverlay,
  peakOverlay, autoloadMode, autoloadDir, dataSourceStateEl, dataSourceSkeletonEl, inspectorPath,
  inspectorType, inspectorShape, inspectorDtype, inspectorAttrs, inspectorPreview, simplonUrl, simplonVersion,
  roiHelp, roiModeSelect, roiHistogramToggle, roiPlotControls, roiParams, roiRadiusField, roiRadiusInput,
  roiCenterFields, roiCenterXInput, roiCenterYInput, roiCenterSnapBtn, roiRingFields, roiInnerInput,
  roiOuterInput, roiExportCsvBtn, roiClearBtn, roiStartEl, roiEndEl, roiSizeLabel, roiSizeEl, roiAreaEl,
  roiTotalEl, roiGapEl, roiDefectiveEl, roiSaturatedEl, roiMinEl, roiMaxEl, roiSumEl, roiMedianEl, roiMeanEl,
  roiStdEl, roiLinePlot, roiBoxPlotX, roiBoxPlotY, roiHistPlot, roiHistBinsAuto, roiHistBinCount,
  roiHistBinChip, roiHistBinManualRow, roiLineCanvas, roiXCanvas, roiYCanvas, roiHistCanvas, roiEnableToggle,
  roiSectionStateEl, roiSummaryEl, geometrySection, geometryStateEl, geometrySummaryEl,
  geometryOverrideToggle, geometryOverrideHint, geometryResetButton, geometryFileInput, geometryFileHint,
  geometryFileBrowse, geometryFileClear, geometryFileStatusEl, seriesSumResult, seriesSumStep,
  seriesSumStepHint, dropdown, recentFilesSubmenu, recentFilesParent, saveAsParent, aboutModal, aboutClose,
  updateCheckModal, logViewerModal, imageExportModal, imageExportClose, imageExportSource, imageExportRegion,
  imageExportScale, imageExportDpi, imageExportOverlays, imageExportOverlaysField, imageExportPixelValues,
  imageExportPixelValuesField, imageExportOverlaysHint, imageExportPixelValuesHint, imageExportSummary,
  imageExportStart, animationExportModal, animationExportClose, animationExportSource,
  animationExportFrameMode, animationExportFrameStep, animationExportRangeStartField,
  animationExportRangeEndField, animationExportRangeStart, animationExportRangeEnd, animationExportRegion,
  animationExportScale, animationExportFps, animationExportLoop, animationExportOverlays,
  animationExportOverlaysField, animationExportPixelValues, animationExportPixelValuesField,
  animationExportOverlaysHint, animationExportPixelValuesHint, animationExportSummary,
  animationExportProgress, animationExportProgressFill, animationExportProgressText, animationExportStart,
  animationExportCancel, toolbarPlaybackExportGif, settingsModal, settingsLanguage, fileInput, uploadBar,
  uploadBarFill, uploadBarText, commandModal, browseModal, maskEditBar,
} = appElements;
const splashHint = document.querySelector(".splash-hint");
const panelBody = toolsPanel?.querySelector(".panel-body");
const panelTabs = document.querySelectorAll(".panel-tab");
const panelTabContents = document.querySelectorAll(".panel-tab-content");
const appLayout = document.querySelector(".app");
const pixelCtx = pixelOverlay?.getContext("2d");
const roiCtx = roiOverlay?.getContext("2d");
const histCtx = histCanvas.getContext("2d");
const histColorCtx = histColorbar?.getContext("2d");
const overviewCtx = overviewCanvas?.getContext("2d");
const canvasShell = document.querySelector(".canvas-shell");
const sectionToggles = document.querySelectorAll("[data-section-toggle]");
const sectionSwitches = document.querySelectorAll(".section-switch");
const splashCtx = splashCanvas?.getContext("2d");
const splashActions = document.querySelector(".splash-actions");
const viewerFooterEl = document.querySelector(".viewer-footer");
const resolutionCtx = resolutionOverlay?.getContext("2d");
const peakCtx = peakOverlay?.getContext("2d");
const inspectorSection = document.querySelector(".panel-section.inspector");
const roiHistBinPresetBtns = document.querySelectorAll(".roi-histogram-bin-preset");
const roiLineCtx = roiLineCanvas?.getContext("2d");
const roiXCtx = roiXCanvas?.getContext("2d");
const roiYCtx = roiYCanvas?.getContext("2d");
const roiHistCtx = roiHistCanvas?.getContext("2d");
const geometryInputs = {
  distanceMm: document.getElementById("geometry-distance"),
  pixelSizeXUm: document.getElementById("geometry-pixel-x"),
  pixelSizeYUm: document.getElementById("geometry-pixel-y"),
  energyEv: document.getElementById("geometry-energy"),
  centerX: document.getElementById("geometry-center-x"),
  centerY: document.getElementById("geometry-center-y"),
};
const geometryHints = {
  distanceMm: document.getElementById("geometry-distance-hint"),
  pixelSize: document.getElementById("geometry-pixel-hint"),
  energyEv: document.getElementById("geometry-energy-hint"),
  center: document.getElementById("geometry-center-hint"),
};
const menuButtons = document.querySelectorAll(".menu-item[data-menu]");
const dropdownPanels = document.querySelectorAll(".dropdown-panel");
const submenuParents = document.querySelectorAll(".dropdown-submenu-parent");
const recentFiles = createRecentFiles();
const menuActions = document.querySelectorAll(".dropdown-item[data-action]");
const menuItemsByAction = new Map(
  Array.from(menuActions, (item) => [item.dataset.action, item])
);
const settingsTabs = document.querySelector(".settings-tabs");

let renderer = null;
let overviewViewportController = null;
let viewerSyncController = null;
let framePlaybackController = null;
let frameMetadataController = null;
let exportSplashController = null;
let inpaintingController = null;
let analysisToolsController = null;
let maskLegend = null;
let flatfieldController = null;
let magnifierController = null;
let quitController = null;
let sourceMetadataController = null;
let geometryParamsController = null;
let chromeToolbarController = null;
let panelLayoutController = null;
let thresholdPlaybackController = null;
let autoloadStatusController = null;
let fileSessionController = null;
let fileDataPipelineController = null;
let liveHistoryController = null;
let updateCheckController = null;
let backendLogViewerController = null;
let activeMenu = "file";
let closeTimer = null;
let histogramScheduled = false;
const overviewInteractionState = {
  dragging: false,
  dragOffset: { x: 0, y: 0 },
  dragMode: null,
  handle: null,
  anchor: null,
  resizeCenter: false,
};
let sectionStateStore = {};
let roiDragging = false;
let panelTabState = "view";
let detectorControlController = null;
let pixelMaskController = null;
let pixelMaskEditController = null;
const coarsePointerQuery = window.matchMedia("(hover: none), (pointer: coarse)");
const overlayCanvasMetrics = new WeakMap();
const modalFocusRestore = new WeakMap();
const modalStack = [];
const MODAL_FOCUSABLE_SELECTOR = [
  "button:not([disabled])",
  "[href]",
  "input:not([disabled]):not([type=\"hidden\"])",
  "select:not([disabled])",
  "textarea:not([disabled])",
  "[tabindex]:not([tabindex=\"-1\"])",
].join(", ");

const PLATFORM_SHORTCUTS = {
  "new-window": { mac: "⌘N", other: "Ctrl+N" },
  "duplicate-window": { mac: "⇧⌘N", other: "Ctrl+Shift+N" },
  open: { mac: "⌘O", other: "Ctrl+O" },
  "close-file": { mac: "⌘W", other: "Ctrl+W" },
  "save-full": { mac: "⌘S", other: "Ctrl+S" },
  "save-visible": { mac: "⇧⌘S", other: "Shift+Ctrl+S" },
  "save-window": { mac: "⌥⌘S", other: "Alt+Ctrl+S" },
  "export-image": { mac: "⇧⌘E", other: "Shift+Ctrl+E" },
  "export-animation": { mac: "⌘G", other: "Ctrl+G" },
  "export-data": { mac: "⇧⌘X", other: "Shift+Ctrl+X" },
  "settings-open": { mac: "⌘,", other: "Ctrl+," },
  "command-palette": { mac: "⌘K", other: "Ctrl+K" },
};

const roiState = createRoiState();
const analysisState = createAnalysisState();
const state = createAppState();
state.language = getLanguage();

const PLOT_THEME = {
  bg: "rgba(12, 18, 27, 0.96)",
  frame: "rgba(78, 100, 137, 0.45)",
  axis: "rgba(132, 156, 196, 0.82)",
  grid: "rgba(88, 112, 152, 0.24)",
  text: "#dce8ff",
  bar: "rgba(236, 243, 255, 0.88)",
  line: "rgba(236, 243, 255, 0.95)",
  lineGlow: "rgba(102, 178, 255, 0.2)",
  markerOutline: "rgba(8, 14, 22, 0.85)",
  peak: "rgba(255, 196, 110, 0.95)",
};

const SECTION_STATE_VARIANTS = ["is-empty", "is-loading", "is-active", "is-warning"];
const CHROME_IDLE_DELAY_MS = 2200;

const { logClient } = createClientLog({ apiBase: API });

bindClientLogging({
  formatClientArg,
  logClient,
});
// The controllers, under short names, for those that call each other
// (decision 0006). Filled as each is created, so a call reaches it as soon as
// it exists, as the forwarding functions it replaces did; a controller that
// needs one created after it calls through here at the time of the call.
const core = {};

const MIN_ZOOM = 0.02;
const MAX_ZOOM = 50;
const MOBILE_PANEL_SNAP_POINTS = [0.6, 1];
const FRAME_STEP_OPTIONS = [1, 10, 100, 1000];
const PIXEL_LABEL_DEFAULT_MIN_CELL_PX = PIXEL_LABEL_MIN_CELL_PX;
const PIXEL_LABEL_DEFAULT_MAX_LABELS = 4000;
const PIXEL_LABEL_DENSE_ZOOM_PX = 24;
const PIXEL_LABEL_INTERACTION_IDLE_MS = 140;
const PIXEL_LABEL_HALO_MAX_LABELS = 3200;
const VIEWPORT_INTERACTION_IDLE_MS = 180;
const PEAK_BAD_MASK_BITS = 0x1f;
const TRANSIENT_LOADING_STATUS_KEYS = [
  "status.data.loading_image",
  "status.data.loading_frame",
  "status.frame.loading_metadata",
  "status.data.loading_dataset_metadata",
  "status.files.loading",
];

function isTransientLoadingStatus(text) {
  const normalized = String(text || "").trim();
  if (!normalized) return false;
  return TRANSIENT_LOADING_STATUS_KEYS.some((key) => normalized === t(key));
}

function getMinZoom() {
  return overviewViewportController ? overviewViewportController.getMinZoom() : MIN_ZOOM;
}

function getEffectiveScrollLeft() {
  return overviewViewportController ? overviewViewportController.getEffectiveScrollLeft() : 0;
}

function getEffectiveScrollTop() {
  return overviewViewportController ? overviewViewportController.getEffectiveScrollTop() : 0;
}

function isViewportInteractionActive() {
  return overviewViewportController ? overviewViewportController.isViewportInteractionActive() : false;
}

function queuePendingFrameRequest(index) {
  if (fileDataPipelineController) {
    fileDataPipelineController.queuePendingFrame(index);
    return;
  }
  state.pendingFrame = index;
}

function hasPendingFrameRequest() {
  return fileDataPipelineController ? fileDataPipelineController.hasPendingFrameRequest() : state.pendingFrame !== null;
}

function consumePendingFrameRequest() {
  if (fileDataPipelineController) {
    return fileDataPipelineController.consumePendingFrameRequest();
  }
  if (state.pendingFrame === null) return null;
  const next = state.pendingFrame;
  state.pendingFrame = null;
  return next;
}

function isFrameLoading() {
  return fileDataPipelineController ? fileDataPipelineController.isFrameLoading() : Boolean(state.isLoading);
}

function setEffectiveScroll(targetX, targetY, schedule = true) {
  overviewViewportController?.setEffectiveScroll(targetX, targetY, schedule);
}

function applyLanguagePreference(language, options = {}) {
  const { source = "user" } = options;
  const next = setLanguage(language, { persist: source !== "runtime", applyDom: true });
  state.language = next;
  if (settingsLanguage) {
    settingsLanguage.value = next;
  }
  return next;
}

const STATUS_TONE_NOTIFIERS = {
  error: notifyError,
  warning: notifyWarning,
  success: notifySuccess,
};

function setStatus(text, options = {}) {
  const { frameStatus = false, tone = null } = options;
  const normalized = String(text || "").trim();
  // A toned status is also raised as a toast so the user notices it even after
  // the footer pill moves on to the next ambient message.
  const notifier = tone ? STATUS_TONE_NOTIFIERS[tone] : null;
  if (notifier && normalized) {
    notifier(normalized);
  }
  if (!statusEl) return;
  if (frameStatus) {
    statusEl.textContent = t("common.ready");
  } else {
    statusEl.textContent = normalized || t("common.idle");
  }
  updateViewerFooter();
}

function currentFrameStatusText() {
  if (!framePlaybackController) {
    const total = Math.max(1, Number(state.frameCount) || 1);
    const index = Math.max(0, Math.min(total - 1, Number(state.frameIndex) || 0));
    return t("status.frame.position", { current: index + 1, total });
  }
  return framePlaybackController.currentFrameStatusText();
}

function setSummaryChip(element, text, tone = "default") {
  if (!element) return;
  element.textContent = text || "";
  element.classList.toggle("is-active", tone === "active");
  element.classList.toggle("is-warning", tone === "warning");
}

function setFieldHint(inputEl, hintEl, message = "") {
  const hasMessage = Boolean(message);
  if (inputEl) {
    inputEl.classList.toggle("is-invalid", hasMessage);
  }
  if (!hintEl) return;
  hintEl.textContent = hasMessage ? message : "";
  hintEl.classList.toggle("is-hidden", !hasMessage);
}

function buildSkeletonList(lineCount = 5) {
  const wrapper = document.createElement("div");
  wrapper.className = "skeleton-list";
  for (let i = 0; i < Math.max(1, lineCount); i += 1) {
    const line = document.createElement("div");
    line.className = "skeleton-line";
    if (i % 3 === 1) {
      line.classList.add("is-mid");
    } else if (i % 3 === 2) {
      line.classList.add("is-short");
    }
    wrapper.appendChild(line);
  }
  return wrapper;
}

function renderSkeletonBlock(container, lineCount = 5) {
  if (!container) return;
  container.innerHTML = "";
  const skeleton = buildSkeletonList(lineCount);
  if (container.tagName?.toLowerCase() === "ul") {
    const li = document.createElement("li");
    li.className = "inspector-empty";
    li.appendChild(skeleton);
    container.appendChild(li);
    return;
  }
  container.appendChild(skeleton);
}

function estimateToolbarChars() {
  if (!toolbarPath) return 80;
  const width = Math.max(160, toolbarPath.clientWidth || 0);
  return Math.max(24, Math.floor(width / 7.4));
}

function formatValue(value) {
  if (!Number.isFinite(value)) return "";
  const dtype = state.dtype || "";
  if (dtype.includes("f")) {
    return value.toFixed(3);
  }
  return Math.round(value).toString();
}

function formatPixelLabelValue(value, cellPx, mode = "auto") {
  return formatPixelLabelValueUtil(value, cellPx, mode, state.dtype);
}

const {
  setToolHintsEnabled,
  initHelpTooltips,
  refreshHelpTooltips,
} = createHelpTooltipController({
  state,
  platformShortcutLabel,
  roiCanvases: [roiLineCanvas, roiXCanvas, roiYCanvas],
});

window.addEventListener("DOMContentLoaded", initHelpTooltips, { once: true });
const infoTips = createInfoTips();

function resetInspectorDetails() {
  if (inspectorPath) inspectorPath.textContent = "-";
  if (inspectorType) inspectorType.textContent = "-";
  if (inspectorShape) inspectorShape.textContent = "-";
  if (inspectorDtype) inspectorDtype.textContent = "-";
  if (inspectorAttrs) inspectorAttrs.innerHTML = "";
  if (inspectorPreview) inspectorPreview.innerHTML = "";
}

function setSectionBadgeState(element, tone, message) {
  if (!element) return;
  const nextTone = tone === "loading" || tone === "active" || tone === "warning" ? tone : "empty";
  SECTION_STATE_VARIANTS.forEach((className) => element.classList.remove(className));
  element.classList.add(`is-${nextTone}`);
  element.textContent = message || "";
}

function setDataSourceSectionState(tone, message, showSkeleton = false) {
  setSectionBadgeState(dataSourceStateEl, tone, message);
  if (!dataSourceSkeletonEl) return;
  if (showSkeleton) {
    renderSkeletonBlock(dataSourceSkeletonEl, 4);
    dataSourceSkeletonEl.classList.remove("is-hidden");
    dataSourceSkeletonEl.setAttribute("aria-hidden", "false");
    return;
  }
  dataSourceSkeletonEl.classList.add("is-hidden");
  dataSourceSkeletonEl.setAttribute("aria-hidden", "true");
  dataSourceSkeletonEl.innerHTML = "";
}

const inspectorPanelController = createInspectorPanelController({
  apiBase: API,
  state,
  elements: {
    ...appElements,
    inspectorSection,
  },
  callbacks: {
    fetchJSON,
    isHdf5File,
    isHeaderCapableFile,
    setSectionBadgeState,
    renderSkeletonBlock,
    formatInspectorValue,
    resetInspectorDetails,
  },
});
const {
  clearImageHeader,
  clearInspectorSearch,
  renderInspectorLink,
  renderInspectorTree,
  selectInspectorRow,
  updateInspectorHeaderVisibility,
} = inspectorPanelController;
core.inspectorPanel = inspectorPanelController;

function syncOverlayCanvas(overlay, ctx) {
  if (!overlay || !ctx || !canvasWrap) return null;
  const width = canvasWrap.clientWidth || 1;
  const height = canvasWrap.clientHeight || 1;
  const left = canvasWrap.offsetLeft || 0;
  const top = canvasWrap.offsetTop || 0;
  const dpr = window.devicePixelRatio || 1;
  const pixelWidth = Math.max(1, Math.floor(width * dpr));
  const pixelHeight = Math.max(1, Math.floor(height * dpr));
  const prev = overlayCanvasMetrics.get(overlay);
  if (!prev || prev.left !== left) {
    overlay.style.left = `${left}px`;
  }
  if (!prev || prev.top !== top) {
    overlay.style.top = `${top}px`;
  }
  if (!prev || prev.width !== width) {
    overlay.style.width = `${width}px`;
  }
  if (!prev || prev.height !== height) {
    overlay.style.height = `${height}px`;
  }
  if (!prev || prev.pixelWidth !== pixelWidth || prev.pixelHeight !== pixelHeight) {
    overlay.width = pixelWidth;
    overlay.height = pixelHeight;
  }
  overlayCanvasMetrics.set(overlay, { left, top, width, height, pixelWidth, pixelHeight, dpr });
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  return { width, height, dpr };
}

const maskCursorController = createMaskCursorController({
  core,
  apiBase: API,
  state,
  elements: {
    ...appElements,
    canvasShell,
  },
  callbacks: {
    isHdfFile,
    parseDtype,
    parseShape,
    typedArrayFrom,
    getActiveSaturationMax,
    redraw,
    getDtypeInfo,
    formatValue,
    isSaturatedValue,
    getInpaintedFrame,
    getCursorClearance: () => magnifierController?.clearance() || 0,
    getResolutionAtPixel,
    getEffectiveScrollLeft,
    getEffectiveScrollTop,
    // Late-bound: the Pixel Mask section is created after this controller.
    getPinnedMask: () => pixelMaskController?.getPinnedMask() || null,
    onMaskChanged: () => pixelMaskController?.render(),
    getCursorNote: (index) => pixelMaskController?.cursorNote(index) || "",
  },
});
const {
  alignMaskToFrame,
  buildNegativeMask,
  clearMaskState,
  getImagePointFromEvent,
  hideCursorOverlay,
  hideHistTooltip,
  showHistTooltip,
  snapHistogramValue,
  updateCursorOverlay,
  updateMaskUI,
} = maskCursorController;
core.maskCursor = maskCursorController;

const roiInteractionController = createRoiInteractionController({
  core,
  state,
  roiState,
  elements: {
    ...appElements,
    roiCtx,
  },
  callbacks: {
    getEffectiveScrollLeft,
    getEffectiveScrollTop,
    syncOverlayCanvas,
    handleRoiChanged,
  },
});
const {
  applyRoiEdit,
  drawRoiOverlay,
  getRoiHandleAt,
  isPointInRoi,
  isRoiEditing,
  startRoiEdit,
  stopRoiEdit,
} = roiInteractionController;
core.roiInteraction = roiInteractionController;

function scheduleRoiOverlay() {
  roiInteractionController.scheduleRoiOverlay();
  // Redrawn with the view like the ROI: the pixel mask editor's overlay.
  pixelMaskEditController?.scheduleDraw();
}

const analysisOverlayController = createAnalysisOverlayController({
  core,
  state,
  analysisState,
  elements: {
    ...appElements,

  },
  constants: {
    peakBadMaskBits: PEAK_BAD_MASK_BITS,
  },
  callbacks: {
    setSectionBadgeState,
    setSummaryChip,
    buildSkeletonList,
    formatStat,
    setFieldHint,
    getActiveSaturationMax,
    isSaturatedValue,
    setStatus,
  },
});
const {
  exportPeakCsv,
  getRingParams,
  refreshPeakResolutions,
  renderPeakList,
  schedulePeakFinder,
  updateRingsSectionState,
} = analysisOverlayController;

// The ring d-spacings: as many as wanted, the one list everything reads.
const ringListController = createRingListController({
  analysisState,
  elements: {
    list: document.getElementById("ring-list"),
    addBtn: document.getElementById("ring-add"),
    iceBtn: document.getElementById("ring-preset-ice"),
    clearBtn: document.getElementById("ring-clear"),
  },
  callbacks: {
    onChange: () => {
      updateRingsSectionState();
      core.overlayRender.scheduleResolutionOverlay();
      analysisOverlayController.refreshPeakResolutions?.();
    },
  },
});

function getResolutionAtPixel(ix, iy, params = getRingParams()) {
  return analysisOverlayController.getResolutionAtPixel(ix, iy, params);
}

const resolutionRingInteractionController = createResolutionRingInteractionController({
  core,
  state,
  analysisState,
  elements: {
    canvasWrap,
  },
  callbacks: {
    setBeamCenter: (x, y) => geometryParamsController?.setBeamCenter(x, y),
    getEffectiveScrollLeft,
    getEffectiveScrollTop,
    getRingParams,
    getResolutionAtPixel,
    setRingValue: (index, d) => ringListController.setRingValue(index, d),
    commitRings: () => ringListController.commit(),
  },
});
const {
  applyRingEdit,
  clearRingHover,
  getRingHandleAt,
  getRingInteractionState,
  isRingEditing,
  startRingEdit,
  stopRingEdit,
  updateRingHover,
} = resolutionRingInteractionController;

function getRoiModeLabel(mode) {
  if (mode === "line") return t("roi.mode.line");
  if (mode === "box") return t("roi.mode.box");
  if (mode === "circle") return t("roi.mode.circle");
  if (mode === "annulus") return t("roi.mode.annulus");
  return t("roi.mode.default");
}

// Export CSV is the one ROI control that acts on the statistics rather than
// describing them, so it greys out when there is nothing to write, with the
// reason on hover. Same shape as the peak list's button.
function syncRoiCsvExportAvailability() {
  setControlAvailability(roiExportCsvBtn, roiCsvExportUnavailableReason(state, roiState));
}

function updateRoiSectionState() {
  syncRoiCsvExportAvailability();
  if (!roiSectionStateEl) return;
  // The status line carries actionable guidance in the empty/disabled/ready
  // states; keep it visible for those.
  roiSectionStateEl.classList.remove("is-hidden");
  if (!state.hasFrame) {
    setSectionBadgeState(roiSectionStateEl, "empty", t("roi.section.load_frame"));
    setSummaryChip(roiSummaryEl, t("roi.summary.no_frame"));
    return;
  }
  if (!roiState.enabled) {
    setSectionBadgeState(roiSectionStateEl, "empty", t("roi.section.disabled"));
    setSummaryChip(roiSummaryEl, t("summary.off"));
    return;
  }
  const modeLabel = getRoiModeLabel(roiState.mode);
  if (!roiState.start || !roiState.end) {
    setSectionBadgeState(
      roiSectionStateEl,
      "empty",
      t("roi.section.mode_ready", { mode: modeLabel }),
    );
    setSummaryChip(roiSummaryEl, t("roi.summary.mode_ready", { mode: modeLabel }));
    return;
  }
  // Active state: the summary badge and the populated stats grid already convey
  // this, so hide the redundant status line to declutter the panel.
  setSectionBadgeState(roiSectionStateEl, "active", t("roi.section.mode_active", { mode: modeLabel }));
  roiSectionStateEl.classList.add("is-hidden");
  setSummaryChip(roiSummaryEl, t("roi.summary.mode_active", { mode: modeLabel }), "active");
}

function updatePeaksSectionState() {
  analysisOverlayController.updatePeaksSectionState();
  // Inpainting's section follows the frame the same way (and empties with it).
  inpaintingController?.updateState();
}

function getSeriesSumGeometryContext() {
  if (analysisState.ringMode !== "geometry" || !analysisState.ringGeometry) {
    return null;
  }
  const effectiveGeometry = applyGeometryOverrides(analysisState.ringGeometry, {
    centerX: analysisState.centerX,
    centerY: analysisState.centerY,
    distanceMm: analysisState.distanceMm,
  });
  const geometry = serializeGeometryPayload(effectiveGeometry);
  if (!geometry) {
    return null;
  }
  return {
    geometry,
    distanceMm: Number.isFinite(analysisState.distanceMm) ? analysisState.distanceMm : null,
    pixelSizeUm: Number.isFinite(analysisState.pixelSizeUm) ? analysisState.pixelSizeUm : null,
    energyEv: Number.isFinite(analysisState.energyEv) ? analysisState.energyEv : null,
    centerX: Number.isFinite(analysisState.centerX) ? analysisState.centerX : null,
    centerY: Number.isFinite(analysisState.centerY) ? analysisState.centerY : null,
  };
}

const seriesSumController = createSeriesSumController({
  core,
  apiBase: API,
  state,
  elements: {
    ...appElements,

  },
  callbacks: {
    isHdfFile,
    validateSeriesStepInput,
    useAsFlat: (path) => flatfieldController?.useFile(path),
    setStatus,
    loadAutoloadFile,
    fetchJSON,
    fetchJSONWithInit,
    getSeriesSumGeometryContext,
  },
});
const { setSeriesSumProgress, updateSeriesSumUi } = seriesSumController;
core.seriesSum = seriesSumController;

const dataExportController = createDataExportController({
  core,
  apiBase: API,
  state,
  elements: {
    ...appElements,

  },
  callbacks: {
    isHdfFile,
    openModal: (...args) => openModal(...args),
    closeModal: (...args) => closeModal(...args),
    setStatus,
    fetchJSON,
    fetchJSONWithInit,
    loadAutoloadFile,
  },
});
const {
  openDialog: openDataExportDialog,
  setProgress: setDataExportProgress,
  updateUi: updateDataExportUi,
} = dataExportController;

const animationExportController = createAnimationExportController({
  apiBase: API,
  state,
  elements: {
    modal: animationExportModal,
    closeBtn: animationExportClose,
    source: animationExportSource,
    frameMode: animationExportFrameMode,
    rangeStartField: animationExportRangeStartField,
    rangeEndField: animationExportRangeEndField,
    rangeStart: animationExportRangeStart,
    rangeEnd: animationExportRangeEnd,
    frameStep: animationExportFrameStep,
    regionSelect: animationExportRegion,
    fpsSelect: animationExportFps,
    loopCheckbox: animationExportLoop,
    overlaysCheckbox: animationExportOverlays,
    overlaysField: animationExportOverlaysField,
    pixelValuesCheckbox: animationExportPixelValues,
    pixelValuesField: animationExportPixelValuesField,
    overlaysHint: animationExportOverlaysHint,
    pixelValuesHint: animationExportPixelValuesHint,
    scaleSelect: animationExportScale,
    summary: animationExportSummary,
    progress: animationExportProgress,
    progressFill: animationExportProgressFill,
    progressText: animationExportProgressText,
    startBtn: animationExportStart,
    cancelBtn: animationExportCancel,
  },
  callbacks: {
    buildPalette,
    getPaletteColorCount,
    mapValueToNorm,
    getActiveSaturationMax,
    isSaturatedValue,
    parseDtype,
    parseShape,
    typedArrayFrom,
    getVisibleRegion: () => exportSplashController?.getVisibleRegion(),
    getOverlayState: () => ({
      ringsEnabled: analysisState.ringsEnabled,
      ringParams: analysisState.ringsEnabled ? analysisOverlayController.getRingParams() : null,
      peaksEnabled: analysisState.peaksEnabled,
      pixelAspect: state.pixelAspect || 1,
    }),
    // Re-runs the spot finder on an exported frame with the settings the panel
    // is showing, so the markers in the GIF belong to the frame they sit on.
    getPixelLabelSettings,
    pixelLabelsForFrame,
    detectPeaksInFrame: (frame) =>
      analysisOverlayController.detectPeaks(
        Math.max(1, Math.min(1000, Math.round(Number(analysisState.peakCount) || 25))),
        Math.max(0, Math.min(50, Number(analysisState.peakMinSnr) || 0)),
        frame
      ),
    openModal: (...args) => openModal(...args),
    closeModal: (...args) => closeModal(...args),
    setStatus,
    // Each frame filled like the one on screen, while inpainting is on.
    inpaintFrame: (data) => inpaintView.frameFor(data, getActiveSaturationMax()),
    inpaintFrameFull: (data) => inpaintView.frameForFull(data, getActiveSaturationMax()),
  },
});
const { openDialog: openAnimationExportDialog } = animationExportController;

const imageExportController = createImageExportController({
  state,
  elements: {
    modal: imageExportModal,
    closeBtn: imageExportClose,
    source: imageExportSource,
    regionSelect: imageExportRegion,
    scaleSelect: imageExportScale,
    dpiSelect: imageExportDpi,
    overlaysCheckbox: imageExportOverlays,
    overlaysField: imageExportOverlaysField,
    pixelValuesCheckbox: imageExportPixelValues,
    pixelValuesField: imageExportPixelValuesField,
    overlaysHint: imageExportOverlaysHint,
    pixelValuesHint: imageExportPixelValuesHint,
    summary: imageExportSummary,
    startBtn: imageExportStart,
  },
  callbacks: {
    getVisibleRegion: () => exportSplashController?.getVisibleRegion(),
    renderRegionToCanvas: (region) => exportSplashController?.renderRegionToCanvas(region),
    canvasToBlob: (canvas) => exportSplashController?.canvasToBlob(canvas),
    saveBlobAs: (name, produce) => exportSplashController?.saveBlobAs(name, produce),
    defaultExportName: (kind) => exportSplashController?.defaultExportName(kind),
    // What the viewer draws over the image right now: the rings with their
    // parameters, the peaks found in this frame, and an ROI that is in use.
    getOverlaySnapshot: () => {
      const roiInUse = roiState.enabled && roiState.active && roiState.start && roiState.end;
      return {
        ringParams: analysisState.ringsEnabled ? analysisOverlayController.getRingParams() : null,
        peaks: analysisState.peaksEnabled && Array.isArray(analysisState.peaks) ? analysisState.peaks : [],
        roi: roiInUse ? { ...roiState } : null,
        outerRadius: roiInUse ? getCircularRoiOuterRadius(roiState, state.pixelAspect || 1) : 0,
      };
    },
    getPixelLabelSettings,
    pixelLabelsForFrame,
    getInpaintedFrame,
    // Wrapped: both are defined further down, and a plain reference here
    // would be read before they exist.
    openModal: (...args) => openModal(...args),
    closeModal: (...args) => closeModal(...args),
    setStatus,
  },
});
const { openDialog: openImageExportDialog } = imageExportController;

// Why a File menu command cannot run right now, or "" when it can. The rules
// themselves live in command_availability.js, shared with the command palette
// (which leaves these commands out instead of greying them) and with each
// command's own handler, which keeps the last guard for the keyboard shortcuts:
// they reach the handler directly, past both the menu and the palette.
function fileCommandUnavailableReason(action) {
  switch (action) {
    case "close-file":
      return state.file ? "" : t("status.file.no_file_loaded");
    case "save-full":
    case "save-visible":
    case "save-window":
    case "export-image":
      return canSaveImage(state) ? "" : t("status.export.no_image");
    case "export-animation":
      if (canExportAnimation(state)) return "";
      return t(
        state.file ? "status.animation_export.not_series" : "status.animation_export.no_file"
      );
    case "export-data":
      if (canExportData(state, isHdfFile)) return "";
      if (isLiveExportSource(state)) return t("status.data_export.no_live_image");
      return t(state.file ? "status.data_export.no_dataset" : "status.data_export.no_file");
    case "duplicate-window":
      if (canDuplicateWindow(state, isHdfFile)) return "";
      if (state.autoload?.running && state.autoload.mode !== "file") {
        return t("status.duplicate_window.live_source");
      }
      return t(state.file ? "status.data_export.no_dataset" : "status.file.no_file_loaded");
    default:
      return "";
  }
}

const GATED_FILE_COMMANDS = [
  "close-file",
  "duplicate-window",
  "save-full",
  "save-visible",
  "save-window",
  "export-image",
  "export-animation",
  "export-data",
];

function syncFileCommandAvailability() {
  GATED_FILE_COMMANDS.forEach((action) => {
    setControlAvailability(menuItemsByAction.get(action), fileCommandUnavailableReason(action));
  });
  // Save As has no action of its own and is dead exactly when its three
  // children are, so it greys out with them rather than opening a submenu of
  // three disabled entries.
  setControlAvailability(saveAsParent, fileCommandUnavailableReason("save-full"));
  // The playback popover's GIF button is the one entry point outside the menu
  // that can be greyed out the same way.
  setControlAvailability(toolbarPlaybackExportGif, fileCommandUnavailableReason("export-animation"));
}

toolbarPlaybackExportGif?.addEventListener("click", () => {
  // Collapse the playback popover before raising the modal on top of it.
  toolbarPlaybackPopover?.setAttribute("aria-hidden", "true");
  toolbarPlaybackToggle?.setAttribute("aria-expanded", "false");
  openAnimationExportDialog();
});

function setLoading(show) {
  if (!loadingEl) return;
  loadingEl.style.display = show ? "block" : "none";
  updateRoiSectionState();
  updateRingsSectionState();
  updatePeaksSectionState();
  if (!show && statusEl && state.hasFrame) {
    const status = (statusEl.textContent || "").trim();
    if (isTransientLoadingStatus(status)) {
      setStatus(currentFrameStatusText(), { frameStatus: true });
    }
  }
}

function localizePlaybackOptionLabels() {
  const localizeStepSelect = (selectEl) => {
    if (!selectEl) return;
    Array.from(selectEl.options).forEach((option) => {
      const value = String(option.value || "").trim();
      if (!value) return;
      option.textContent = t("toolbar.step.option", { value });
    });
  };

  const localizeFpsSelect = (selectEl) => {
    if (!selectEl) return;
    Array.from(selectEl.options).forEach((option) => {
      const value = String(option.value || "").trim();
      if (!value) return;
      option.textContent = t("toolbar.playback.fps_option", { value });
    });
  };

  localizeStepSelect(frameStep);
  localizeStepSelect(toolbarMoreStep);
  localizeFpsSelect(fpsSelect);
  localizeFpsSelect(toolbarMoreFps);
}

// Under the Open file button, only what the button does not already say: the
// keyboard shortcut, or -- with ALBIS on another machine -- that files can be
// dropped here to upload them.
function syncSplashHintMode() {
  if (!splashHint) return;
  if (backendIsLocal) {
    splashHint.dataset.i18n = "splash.hint.shortcut";
    splashHint.dataset.i18nVars = JSON.stringify({ key: platformShortcutLabel("open", "⌘O") });
  } else {
    splashHint.dataset.i18n = "splash.hint.drop";
    delete splashHint.dataset.i18nVars;
  }
}

function refreshLocalizedUi() {
  syncSplashHintMode();
  applyI18nToDom(document);
  // Built from t() with counts and key names, not from data-i18n alone.
  pixelMaskController?.render();
  pixelMaskEditController?.localize();
  localizePlaybackOptionLabels();
  refreshHelpTooltips();
  updateRoiModeUI();
  core.roiInteraction.scheduleRoiUpdate();
  applyPanelState();
  core.chromeToolbar?.updateToolbar();
  updateViewerFooter();
  updatePlayButtons();
  core.backendStatus.updateLiveBadge();
  updateAboutVersion();
  updateDataSourceSummary();
  updateRingsSectionState();
  updatePeaksSectionState();
  updateSeriesSumUi();
  syncFileCommandAvailability();
  updateCheckController?.refreshUi();
  backendLogViewerController?.refreshUi();
  viewerSyncController?.refreshUi();
  validateSeriesStepInput(false);
  if (commandPaletteController?.isOpen()) {
    commandPaletteController.render();
  }
}

function scheduleHistogram() {
  if (histogramScheduled) return;
  histogramScheduled = true;
  window.requestAnimationFrame(() => {
    histogramScheduled = false;
    if (state.histogram) {
      drawHistogram(state.histogram);
    }
  });
}

function getViewRect() {
  return overviewViewportController ? overviewViewportController.getViewRect() : null;
}

function overviewEventToImage(event) {
  return overviewViewportController ? overviewViewportController.overviewEventToImage(event) : null;
}

function overviewEventToOverview(event) {
  return overviewViewportController ? overviewViewportController.overviewEventToOverview(event) : null;
}

function getOverviewHandleAt(point) {
  return overviewViewportController ? overviewViewportController.getOverviewHandleAt(point) : null;
}

function getAnchorForHandle(view, handle, keepCenter) {
  return overviewViewportController
    ? overviewViewportController.getAnchorForHandle(view, handle, keepCenter)
    : null;
}

function setZoom(value) {
  overviewViewportController?.setZoom(value);
}

function normalizeWheelDelta(event) {
  return overviewViewportController ? overviewViewportController.normalizeWheelDelta(event) : 0;
}

function isTouchGestureActive() {
  return overviewViewportController ? overviewViewportController.isTouchGestureActive() : false;
}

function getDefaultThresholdIndex() {
  return thresholdPlaybackController
    ? thresholdPlaybackController.getDefaultThresholdIndex()
    : 0;
}

function getThresholdIndexAtOffset(offset) {
  return thresholdPlaybackController
    ? thresholdPlaybackController.getThresholdIndexAtOffset(offset)
    : 0;
}

function getMaxPanelWidth() {
  return panelLayoutController ? panelLayoutController.getMaxPanelWidth() : Math.max(220, Math.min(900, window.innerWidth - 24));
}

// The same test panel_layout_controller.js makes; asked of the browser, so the
// toolbar can use it before the panel layout exists.
function isPhonePanelLayout() {
  return Boolean(coarsePointerQuery?.matches) && window.innerWidth < 768;
}

function nearestMobilePanelSnap(value) {
  return panelLayoutController ? panelLayoutController.nearestMobilePanelSnap(value) : MOBILE_PANEL_SNAP_POINTS[0];
}

function updateFrameControls() {
  framePlaybackController?.updateFrameControls();
  syncFileCommandAvailability();
}

function startPlayback() {
  if (liveHistoryController?.isLiveHistoryActive()) {
    if (state.autoload.livePaused) {
      liveHistoryController.goLive();
      void core.autoloadOrchestration.autoloadTick();
    } else {
      liveHistoryController.pauseLive();
    }
    return;
  }
  framePlaybackController?.startPlayback();
}

function requestFrame(index) {
  if (liveHistoryController?.isLiveHistoryActive()) {
    liveHistoryController.showLiveHistoryFrame(index);
    return;
  }
  framePlaybackController?.requestFrame(index);
}

function updateFullscreenUi() {
  const active = Boolean(document.fullscreenElement);
  document.body.classList.toggle("is-fullscreen", active);
  if (fullscreenToggle) {
    fullscreenToggle.classList.toggle("is-active", active);
    fullscreenToggle.textContent = active ? "🗗" : "⛶";
    const ariaLabel = active ? t("toolbar.fullscreen.exit") : t("toolbar.fullscreen.enter");
    fullscreenToggle.setAttribute("aria-label", ariaLabel);
    fullscreenToggle.title = `${ariaLabel} (F)`;
  }
  core.chromeToolbar?.syncToolbarMoreControls();
  syncOverlayAnchors();
}

async function toggleFullscreen() {
  try {
    if (!document.fullscreenElement) {
      await document.documentElement.requestFullscreen();
    } else {
      await document.exitFullscreen();
    }
  } catch (err) {
    console.error(err);
    setStatus(t("status.fullscreen_unavailable"), { tone: "warning" });
  }
}

function setActiveMenu(menu, anchor) {
  activeMenu = menu;
  menuButtons.forEach((btn) => {
    const isActive = btn.dataset.menu === menu;
    btn.classList.toggle("is-active", isActive);
    btn.setAttribute("aria-expanded", String(isActive));
  });
  dropdownPanels.forEach((panel) => {
    panel.classList.toggle("is-active", panel.dataset.menu === menu);
  });
  if (anchor && dropdown) {
    const chrome = document.querySelector(".chrome");
    const anchorRect = anchor.getBoundingClientRect();
    const chromeRect = chrome.getBoundingClientRect();
    dropdown.style.left = `${anchorRect.left - chromeRect.left}px`;
  }
}

function isCoarsePointerDevice() {
  return Boolean(coarsePointerQuery?.matches);
}

function platformShortcutLabel(action, fallback = "") {
  const entry = PLATFORM_SHORTCUTS[action];
  if (!entry) return fallback;
  return isMacPlatform ? entry.mac : entry.other;
}

function applyPlatformShortcutLabels() {
  menuActions.forEach((item) => {
    const action = item.dataset.action || "";
    const shortcutEl = item.querySelector(".shortcut");
    if (!shortcutEl) return;
    const label = platformShortcutLabel(action);
    if (!label) return;
    shortcutEl.textContent = label;
  });
}

function closeSubmenus() {
  submenuParents.forEach((parent) => {
    parent.classList.remove("is-open");
    parent.setAttribute("aria-expanded", "false");
  });
}

function openMenu(menu, anchor) {
  if (!dropdown) return;
  closeToolbarPlaybackPopover();
  closeToolbarMorePopover();
  closeSubmenus();
  if (menu === "file") {
    // Rebuilt on open rather than kept in sync: the list is short, and one
    // source of truth for what is on screen beats two.
    recentFilesController?.render();
    syncFileCommandAvailability();
  }
  dropdown.classList.add("is-open");
  dropdown.setAttribute("aria-hidden", "false");
  setActiveMenu(menu, anchor);
}

function closeMenu() {
  if (!dropdown) return;
  closeSubmenus();
  dropdown.classList.remove("is-open");
  dropdown.setAttribute("aria-hidden", "true");
  menuButtons.forEach((btn) => btn.setAttribute("aria-expanded", "false"));
}

function scheduleClose() {
  window.clearTimeout(closeTimer);
  closeTimer = window.setTimeout(() => {
    closeMenu();
  }, 250);
}

function cancelClose() {
  window.clearTimeout(closeTimer);
}

const fileOpenController = createFileOpenController({
  core,
  apiBase: API,
  state,
  getBackendIsLocal: () => backendIsLocal,
  elements: {
    ...appElements,

  },
  callbacks: {
    dirnameFromPath,
    option,
    fileLabel: (...args) => fileLabel(...args),
    isHdfFile,
    loadDatasets,
    loadImageSeries,
    closeMenu,
    setStatus,
    openFileDialog: (...args) => openFileDialog(...args),
    recordRecentFile: (path) => recordRecentFile(path),
  },
});
core.fileOpen = fileOpenController;

async function openPathInViewer(path, options = {}) {
  await fileOpenController.openPathInViewer(path, options);
}

const recentFilesController = createRecentFilesController({
  recentFiles,
  elements: {
    submenu: recentFilesSubmenu,
    parent: recentFilesParent,
  },
  callbacks: {
    openPath: (path) => openPathInViewer(path),
    closeMenu: () => closeMenu(),
    setStatus: (...args) => setStatus(...args),
  },
});

function recordRecentFile(path) {
  recentFiles.record(path);
  recentFilesController.render();
}

const uploadFlowController = createUploadFlowController({
  core,
  apiBase: API,
  state,
  elements: {
    ...appElements,

  },
  callbacks: {
    setLoading,
    setStatus,
    openPathInViewer: (...args) => openPathInViewer(...args),
    fileLabel: (...args) => fileLabel(...args),
    fetchJSON,
  },
});
const { hideUploadProgress } = uploadFlowController;
core.uploadFlow = uploadFlowController;

function showDocumentDropDisabledStatus() {
  setStatus(t("status.upload.local_drop_disabled"));
}

function showProcessingProgress(label = "Processing…") {
  if (!uploadBar) return;
  uploadBar.classList.add("is-active");
  uploadBar.classList.add("is-processing");
  uploadBar.setAttribute("aria-hidden", "false");
  if (uploadBarFill) uploadBarFill.style.width = "40%";
  if (uploadBarText) uploadBarText.textContent = label;
}

function hideProcessingProgress() {
  if (!uploadBar) return;
  uploadBar.classList.remove("is-processing");
  if (!uploadBar.classList.contains("is-active")) return;
  uploadBar.classList.remove("is-active");
  uploadBar.setAttribute("aria-hidden", "true");
}

function setPanelTab(tabId, persist = true) {
  panelTabState = tabId;
  applyPanelTab({
    tabId,
    panelTabs,
    panelTabContents,
    persist,
    persistKey: "albis.panelTab",
    onAfterChange: (activeTabId) => {
      core.overviewViewport?.scheduleOverview();
      scheduleHistogram();
      core.overlayRender.schedulePixelOverlay();
      core.overlayRender.scheduleResolutionOverlay();
      if (activeTabId === "analysis") {
        // Defer ROI refresh so projection canvases are visible and sized after tab switch.
        window.requestAnimationFrame(() => {
          scheduleRoiOverlay();
          core.roiInteraction.scheduleRoiUpdate();
        });
      }
    },
  });
}

function setDataControlsForHdf5() {
  if (datasetSelect) datasetSelect.disabled = false;
  if (thresholdSelect) thresholdSelect.disabled = false;
  if (toolbarThresholdSelect) toolbarThresholdSelect.disabled = false;
  if (frameRange) frameRange.disabled = false;
  if (frameIndex) frameIndex.disabled = false;
  if (frameStep) frameStep.disabled = false;
  if (fpsSelect) fpsSelect.disabled = false;
  updateInspectorHeaderVisibility(state.file);
}

function setDataControlsForImage() {
  if (datasetSelect) datasetSelect.disabled = true;
  if (thresholdSelect) thresholdSelect.disabled = true;
  if (toolbarThresholdSelect) toolbarThresholdSelect.disabled = true;
  if (frameRange) frameRange.disabled = true;
  if (frameIndex) frameIndex.disabled = true;
  if (frameStep) frameStep.disabled = true;
  if (fpsSelect) fpsSelect.disabled = true;
  updateInspectorHeaderVisibility(state.file);
}

function setDataControlsForSeries() {
  if (datasetSelect) datasetSelect.disabled = true;
  if (thresholdSelect) thresholdSelect.disabled = true;
  if (toolbarThresholdSelect) toolbarThresholdSelect.disabled = true;
  if (frameRange) frameRange.disabled = false;
  if (frameIndex) frameIndex.disabled = false;
  if (frameStep) frameStep.disabled = false;
  if (fpsSelect) fpsSelect.disabled = false;
  updateInspectorHeaderVisibility(state.file);
}

autoloadStatusController = createAutoloadStatusController({
  core,
  apiBase: API,
  state,
  elements: {
    ...appElements,

  },
  callbacks: {
    setStatus,
  },
});
const { setAutoloadLatest, updateAutoloadMeta } = autoloadStatusController;
core.autoloadStatus = autoloadStatusController;

// The geometry ALBIS calculates with, and the user's override of it. Created
// before anything that reports metadata, since all of that goes through it.
geometryParamsController = createGeometryParamsController({
  apiBase: API,
  state,
  analysisState,
  isBackendLocal: () => backendIsLocal,
  elements: {
    sectionStateEl: geometryStateEl,
    summaryChipEl: geometrySummaryEl,
    overrideToggle: geometryOverrideToggle,
    overrideHint: geometryOverrideHint,
    inputs: geometryInputs,
    hints: geometryHints,
    resetButton: geometryResetButton,
    geometryFile: geometryFileInput,
    geometryFileHint,
    geometryBrowse: geometryFileBrowse,
    geometryClear: geometryFileClear,
    geometryStatusEl: geometryFileStatusEl,
    inlineSummaries: Array.from(document.querySelectorAll("[data-geometry-inline]")),
  },
  callbacks: {
    onParamsChanged: () => {
      updateRingsSectionState();
      core.overlayRender.scheduleResolutionOverlay();
      refreshPeakResolutions();
    },
    // Same zoom, new aspect: recomputes the centring, the canvas transform,
    // the pan limits and the overlays drawn over the image.
    onPixelAspectChanged: () => {
      setZoom(state.zoom);
      core.overviewViewport?.scheduleOverview();
    },
    reloadGeometry: () => reloadGeometryForCurrentFile(),
    revealSection: () => revealGeometrySection(),
    setSectionBadgeState,
    setSummaryChip,
    setStatus,
    openFileDialog: (...args) => openFileDialog(...args),
  },
});

sourceMetadataController = createSourceMetadataController({
  core,
  state,
  analysisState,
  elements: {
    ...appElements,

  },
  callbacks: {
    geometryParams: geometryParamsController,
  },
});
const {
  applyImageMeta,
  applyLiveSourceSnapshot,
  updateRemoteMetaUI,
  updateSimplonMetaUI,
} = sourceMetadataController;
core.sourceMetadata = sourceMetadataController;
sourceMetadataController.updateGeometryUi();

panelLayoutController = createPanelLayoutController({
  core,
  state,
  constants: {
    mobilePanelSnapPoints: MOBILE_PANEL_SNAP_POINTS,
    initialMobilePanelSnap: MOBILE_PANEL_SNAP_POINTS[0],
  },
  elements: {
    ...appElements,
    coarsePointerQuery,
    appLayout,
    panelBody,
    panelTabs,
    panelTabContents,
  },
  callbacks: {
    scheduleHistogram,
    recenterCanvas: () => {
      if (state.hasFrame) setZoom(state.zoom);
    },
    getSectionStateStore: () => sectionStateStore,
    setSectionStateStore: (next) => {
      sectionStateStore = next;
    },
  },
});
const {
  applyPanelState,
  initializePanelTabA11y,
  initializeSectionContentWrappers,
  setPanelWidth,
  startMobilePanelDrag,
  togglePanel,
  toggleSection,
  updateMobilePanelDrag,
} = panelLayoutController;
core.panelLayout = panelLayoutController;

chromeToolbarController = createChromeToolbarController({
  state,
  constants: {
    frameStepOptions: FRAME_STEP_OPTIONS,
    chromeIdleDelayMs: CHROME_IDLE_DELAY_MS,
  },
  elements: {
    ...appElements,

  },
  callbacks: {
    middleTruncate,
    fileLabel,
    formatTimeStamp,
    setSummaryChip,
    estimateToolbarChars,
    updateSeriesSumUi,
    isPhonePanelLayout,
    isMenuOpen: () => Boolean(activeMenu),
    onOpenUpdateCheck: () => {
      updateCheckController?.openAndCheck();
    },
    onCopyBuildInfoUnavailable: (text) => {
      // Both clipboard routes refused. Put the text somewhere it can still be
      // read and copied by hand rather than reporting a silent success.
      notifyWarning(text);
    },
  },
});
const {
  closeFooterVersionPopover,
  closeToolbarMorePopover,
  closeToolbarPlaybackPopover,
  registerChromeActivity,
  syncOverlayAnchors,
  toggleFooterVersionPopover,
  toggleToolbarMorePopover,
  toggleToolbarPlaybackPopover,
  updateDataSourceSummary,
  updateFooterVersions,
  updateFpsLabel,
  updateViewerFooter,
} = chromeToolbarController;
core.chromeToolbar = chromeToolbarController;

thresholdPlaybackController = createThresholdPlaybackController({
  core,
  state,
  constants: {
    frameStepOptions: FRAME_STEP_OPTIONS,
  },
  elements: {
    ...appElements,

  },
  callbacks: {
    formatEnergy,
    option,
    updateViewerFooter,
    updateFpsLabel,
    startPlayback,
    requestFrame,
  },
});
const { setFps, setFrameStep, updatePlayButtons, updateThresholdOptions } = thresholdPlaybackController;
core.thresholdPlayback = thresholdPlaybackController;

exportSplashController = createExportSplashController({
  state,
  elements: {
    ...appElements,
    splashCtx,
    splashActions,
  },
  callbacks: {
    buildPalette,
    getPaletteColorCount,
    mapValueToNorm,
    getActiveSaturationMax,
    getEffectiveScrollLeft,
    getEffectiveScrollTop,
    isSaturatedValue,
    setStatus,
    getInpaintedFrame,
  },
});
const {
  drawSplash,
  exportFullImage,
  exportVisibleArea,
  hideSplash,
  setSplashStatus,
  showSplash,
  updateSplashCallToAction,
} = exportSplashController;
core.exportSplash = exportSplashController;

fileSessionController = createFileSessionController({
  core,
  state,
  analysisState,
  elements: {
    ...appElements,

  },
  callbacks: {
    clearGeometrySource: () => geometryParamsController?.clearSource(),
    clearMaskState,
    clearImageHeader,
    setDataSourceSectionState,
    setStatus,
    setLoading,
    hideUploadProgress,
    hideProcessingProgress,
    showSplash,
    setSplashStatus,
    updateInspectorHeaderVisibility,
    updateFrameControls,
    updateThresholdOptions,
    renderPeakList,
    updateRingsSectionState,
    updatePeaksSectionState,
    updatePlayButtons,
    option,
    setDataControlsForImage,
    setDataControlsForSeries,
    buildNegativeMask,
    updateMaskUI,
    getRenderer: () => renderer,
    isWebglUnsignedRawCandidate,
    toFloat32,
    computeStats,
    computeAutoLevels,
    getContrastData,
    formatValue,
    alignMaskToFrame,
    redraw,
    hideSplash,
    schedulePeakFinder,
    scheduleHistogram,
  },
});
core.fileSession = fileSessionController;
const { applyFrame } = fileSessionController;

liveHistoryController = createLiveHistoryController({
  core,
  state,
  callbacks: {
    applyLiveSourceSnapshot,
    updateFrameControls,
    updatePlayButtons,
    updateAutoloadUI,
    setStatus,
  },
});

const backendStatusController = createBackendStatusController({
  apiBase: API,
  state,
  elements: {
    ...appElements,

  },
  callbacks: {
    updateFooterVersions,
    updateSplashCallToAction,
    setSplashStatus,
    onQuitAvailability: () => quitController?.sync(),
  },
});
const { startBackendHeartbeat, updateAboutVersion } = backendStatusController;
core.backendStatus = backendStatusController;

async function fetchSettingsConfig() {
  try {
    const payload = await fetchJSON(`${API}/settings`);
    const config = payload?.config || {};
    core.settings.applyUiSettings(config?.ui, { source: "config" });
    core.overlayRender.schedulePixelOverlay();
  } catch (err) {
    console.warn("Settings fetch failed", err);
  }
}

function maybeCheckForStartupUpdates() {
  if (!state.autoCheckUpdates) {
    state.updateStatus = "disabled";
    updateFooterVersions();
    return;
  }
  void updateCheckController
    ?.checkOnStartup({ enabled: state.autoCheckUpdates })
    .then((payload) => {
      // A null payload means the request failed or was superseded; either way
      // the honest report is that the check did not produce an answer.
      state.updateStatus = payload?.status || "unavailable";
      state.updateLatestVersion = payload?.latest_version || "";
      updateFooterVersions();
    })
    .catch(() => {
      state.updateStatus = "unavailable";
      updateFooterVersions();
    });
}

async function bootstrapApp() {
  showSplash();
  drawSplash();
  setSplashStatus("splash.status.starting_backend");
  await core.backendStatus.waitForBackendReady();
  setSplashStatus("splash.status.loading_settings");
  await fetchSettingsConfig();
  setSplashStatus("splash.status.loading_file_list");
  await core.frameMetadata?.loadAutoloadFolders();
  await core.frameMetadata?.loadFiles();
  setSplashStatus("splash.status.ready_open_file");
  maybeCheckForStartupUpdates();
}

const autoloadSettingsController = createAutoloadSettingsController({
  core,
  state,
  elements: {
    ...appElements,

  },
  callbacks: {
    closeToolbarPlaybackPopover,
    updateSimplonMetaUI,
    updateRemoteMetaUI,
    updateAutoloadMeta,
    updateThresholdOptions,
    updateDataSourceSummary,
    setDataSourceSectionState,
    setAutoloadLatest,
    updatePlayButtons,
    isBackendLocal: () => backendIsLocal,
  },
});
const { loadAutoloadSettings, persistAutoloadSettings } = autoloadSettingsController;

function updateAutoloadUI() {
  autoloadSettingsController.updateAutoloadUI();
  geometryParamsController?.render();
}

const autoloadOrchestrationController = createAutoloadOrchestrationController({
  core,
  state,
  analysisState,
  elements: {
    ...appElements,

  },
  callbacks: {
    updateAutoloadUI,
    updateAutoloadMeta,
    setStatus,
    persistAutoloadSettings,
    resetLiveHistory: () => resetLiveHistory(),
    updateRemoteMetaUI,
  },
});
core.autoloadOrchestration = autoloadOrchestrationController;

const autoloadModeController = createAutoloadModeController({
  core,
  apiBase: API,
  state,
  callbacks: {
    setAutoloadLatest,
    updateAutoloadMeta,
    loadAutoloadFile: (...args) => loadAutoloadFile(...args),
    parseDtype,
    parseShape,
    typedArrayFrom,
    parseSimplonMeta,
    createLiveSourceSnapshot,
    appendLiveFrame,
    logClient,
    formatSimplonTimestamp,
  },
});
core.autoloadMode = autoloadModeController;

const jfjochBridgeController = createJfjochBridgeController({
  core,
  apiBase: API,
  state,
  callbacks: {
  },
});
core.jfjochBridge = jfjochBridgeController;

const remoteStreamController = createRemoteStreamController({
  core,
  apiBase: API,
  state,
  callbacks: {
    updateAutoloadMeta,
    parseDtype,
    parseShape,
    typedArrayFrom,
    parseRemoteMeta,
    createLiveSourceSnapshot,
    applyLiveSourceSnapshot,
    appendLiveFrame,
    updateLiveHistoryEntry,
    resetLiveHistory,
  },
});
core.remoteStream = remoteStreamController;

framePlaybackController = createFramePlaybackController({
  core,
  state,
  elements: {
    frameRange,
    frameIndex,
  },
  callbacks: {
    updatePlayButtons,
    setLoading,
    isViewportInteractionActive,
    queuePendingFrameRequest,
    consumePendingFrameRequest,
    isFrameLoading,
  },
});
core.framePlayback = framePlaybackController;
const { processPendingFrameRequest } = framePlaybackController;

frameMetadataController = createFrameMetadataController({
  core,
  apiBase: API,
  state,
  elements: {
    ...appElements,

  },
  callbacks: {
    fetchJSON,
    option,
    fileLabel,
    setDataControlsForHdf5,
    setDataSourceSectionState,
    setStatus,
    onWriterPresenceChange: (present) => liveSeriesWatch?.setWriterPresent(present),
    showSplash,
    setSplashStatus,
    setLoading,
    showProcessingProgress,
    hideProcessingProgress,
    getDefaultThresholdIndex,
    updateFrameControls,
    updateThresholdOptions,
    isHdf5File,
    geometryParams: geometryParamsController,
    loadImageGeometry,
  },
});
core.frameMetadata = frameMetadataController;

const liveSeriesWatch = createLiveSeriesWatch({
  refreshFrameCount: () => frameMetadataController.refreshFrameCount(),
  callbacks: {
    onGrew: (frameCount) => {
      // Only when the count actually changes, so a run in progress does not
      // rewrite the status line every second.
      setStatus(t("status.frame.series_growing", { count: frameCount }));
      core.overviewViewport?.scheduleOverview();
    },
  },
});

fileDataPipelineController = createFileDataPipelineController({
  core,
  apiBase: API,
  state,
  elements: {
    ...appElements,

  },
  callbacks: {
    fetchJSON,
    option,
    fileLabel,
    isSeriesCapable,
    isHdfFile,
    setDataControlsForHdf5,
    setDataControlsForSeries,
    loadImageGeometry,
    updateFrameControls,
    updatePlayButtons,
    requestFrame,
    parseDtype,
    parseShape,
    typedArrayFrom,
    applyImageMeta,
    applyFrame,
    processPendingFrameRequest,
    currentFrameStatusText,
    setLoading,
    setStatus,
    showSplash,
    setSplashStatus,
    setDataSourceSectionState,
    showProcessingProgress,
    hideProcessingProgress,
  },
});
core.fileDataPipeline = fileDataPipelineController;

async function loadAutoloadFile(file) {
  resetLiveHistory();
  return fileDataPipelineController.loadAutoloadFile(file);
}

async function loadImageSeries(file) {
  resetLiveHistory();
  return fileDataPipelineController.loadImageSeries(file);
}

const settingsController = createSettingsController({
  core,
  apiBase: API,
  state,
  constants: {
    pixelLabelDefaultMinCellPx: PIXEL_LABEL_DEFAULT_MIN_CELL_PX,
    pixelLabelDefaultMaxLabels: PIXEL_LABEL_DEFAULT_MAX_LABELS,
  },
  elements: {
    ...appElements,
    settingsTabs,
  },
  callbacks: {
    setToolHintsEnabled,
    openModal: (...args) => openModal(...args),
    closeModal: (...args) => closeModal(...args),
    closeMenu,
    setStatus,
    applyLanguagePreference,
    // Late-bound: settings can be applied before the controller below exists.
    setDetectorControlEnabled: (enabled) => detectorControlController?.setEnabled(enabled),
    setPixelMaskEnabled: (enabled) => pixelMaskController?.setEnabled(enabled),
    // Kept in state too: settings can arrive before the Analysis tools exist,
    // and are applied when they do.
    setHiddenTools: (hiddenTools) => {
      state.hiddenTools = hiddenTools;
      analysisToolsController?.apply(hiddenTools);
    },
  },
});
core.settings = settingsController;

// Beta: the Detector tab. Hidden until ui.detector_control is on; the
// backend refuses its endpoints until then too.
const $detector = (id) => document.getElementById(id);
detectorControlController = createDetectorControlController({
  apiBase: API,
  elements: {
    tab: $detector("panel-tab-detector"),
    content: $detector("detector-content"),
    urlInput: $detector("detector-url"),
    connectBtn: $detector("detector-connect"),
    addressHost: $detector("detector-address"),
    message: $detector("detector-message"),
    summary: $detector("detector-summary"),
    live: $detector("detector-live"),
    model: $detector("detector-model"),
    serial: $detector("detector-serial"),
    statePill: $detector("detector-state"),
    stateText: $detector("detector-state-text"),
    progress: $detector("detector-progress"),
    progressBar: $detector("detector-bar"),
    progressLeft: $detector("detector-progress-left"),
    progressRight: $detector("detector-progress-right"),
    confirm: $detector("detector-confirm"),
    confirmText: $detector("detector-confirm-text"),
    confirmYes: $detector("detector-confirm-yes"),
    confirmNo: $detector("detector-confirm-no"),
    primaryBtn: $detector("detector-primary"),
    stopBtn: $detector("detector-stop"),
    followToggle: $detector("detector-follow"),
    sensors: $detector("detector-sensors"),
    notice: $detector("detector-notice"),
    noticeText: $detector("detector-notice-text"),
    noticeDismiss: $detector("detector-notice-dismiss"),
    sections: document.querySelectorAll("#detector-content .detector-extra"),
    paramsHost: $detector("detector-params"),
    lockNote: $detector("detector-lock"),
    seriesSummary: $detector("detector-series-summary"),
    outputSummary: $detector("detector-output-summary"),
    outputsHost: $detector("detector-outputs"),
    filesHost: $detector("detector-files"),
    logHost: $detector("detector-log"),
    advancedHost: $detector("detector-advanced"),
    commandsHost: $detector("detector-commands"),
  },
  callbacks: {
    getPanelTab: () => panelTabState,
    setPanelTab: (tabId) => setPanelTab(tabId),
    // The "?" of each setting is created with the rows, so it is bound then.
    refreshInfoTips: () => infoTips.refresh(),
    // A series copied from the detector opens like any file: the data source
    // becomes File, as with File -> Open, instead of staying on the live view.
    openPath: async (path) => {
      await core.autoloadOrchestration.ensureFileMode({ disableMonitor: false });
      await openPathInViewer(path);
    },
    // The monitor's images are what ALBIS's SIMPLON data source shows: point it
    // at the same detector and switch to it.
    // Already following this detector: leave it be, so each new series does
    // not restart the live view. Selected but stopped, or another detector:
    // (re)start it.
    watchLive: (detectorUrl, version) => {
      const before = state.autoload.simplonUrl;
      if (simplonUrl) {
        simplonUrl.value = detectorUrl;
        simplonUrl.dispatchEvent(new window.Event("change"));
      }
      if (simplonVersion && version && simplonVersion.value !== version) {
        simplonVersion.value = version;
        simplonVersion.dispatchEvent(new window.Event("change"));
      }
      if (autoloadMode && autoloadMode.value !== "simplon") {
        autoloadMode.value = "simplon";
        autoloadMode.dispatchEvent(new window.Event("change"));
      } else if (!state.autoload.running || before !== state.autoload.simplonUrl) {
        void core.autoloadOrchestration.startAutoload();
      }
    },
    onChange: () => pixelMaskController?.render(),
  },
});
detectorControlController.setEnabled(state.detectorControl);

// Beta: Data -> Pixel Mask, and its section on the Detector tab. Hidden until
// ui.pixel_mask is on; the backend refuses its endpoints until then too.
pixelMaskController = createPixelMaskController({
  apiBase: API,
  state,
  elements: {
    section: document.getElementById("pixel-mask-section"),
    summaryChip: document.getElementById("summary-pixel-mask"),
    stateEl: document.getElementById("pixel-mask-state"),
    sourceEl: document.getElementById("pixel-mask-source"),
    saveBtn: document.getElementById("pixel-mask-save"),
    saveMenu: document.getElementById("pixel-mask-save-menu"),
    saveH5Btn: document.getElementById("pixel-mask-save-h5"),
    saveTiffBtn: document.getElementById("pixel-mask-save-tiff"),
    detSection: document.getElementById("detector-pixel-mask"),
    detSummary: document.getElementById("detector-pixel-mask-summary"),
    detInfo: document.getElementById("detector-pixel-mask-info"),
    detOpenBtn: document.getElementById("detector-pixel-mask-open"),
    detBackup: document.getElementById("detector-pixel-mask-backup"),
    detBackupText: document.getElementById("detector-pixel-mask-backup-text"),
    detRestoreBtn: document.getElementById("detector-pixel-mask-restore"),
    detClearBtn: document.getElementById("detector-pixel-mask-clear"),
    editBtn: document.getElementById("pixel-mask-edit"),
    discardBtn: document.getElementById("pixel-mask-discard"),
    editsEl: document.getElementById("pixel-mask-edits"),
    sendBtn: document.getElementById("pixel-mask-send"),
    findBtn: document.getElementById("pixel-mask-find"),
    findPanel: document.getElementById("pixel-mask-find-panel"),
    findHot: document.getElementById("pixel-mask-find-hot"),
    findHotSigma: document.getElementById("pixel-mask-find-hot-sigma"),
    findCold: document.getElementById("pixel-mask-find-cold"),
    findColdPercent: document.getElementById("pixel-mask-find-cold-percent"),
    findResultEl: document.getElementById("pixel-mask-find-result"),
    findAddBtn: document.getElementById("pixel-mask-find-add"),
    findCloseBtn: document.getElementById("pixel-mask-find-close"),
  },
  callbacks: {
    refreshMask: () => maskCursorController.refreshPinnedMask(),
    getDetector: () => detectorControlController?.getConnection() || null,
    onDetectorMaskWritten: (url) => {
      const autoload = state.autoload || {};
      if (autoload.running && autoload.mode === "simplon" && hostOf(autoload.simplonUrl) === hostOf(url)) {
        state.autoload.lastMaskAttempt = Date.now();
        void maskCursorController.fetchSimplonMask();
      }
    },
    // Watching a detector live with detector control on: send to that one.
    getMonitorDetector: () => {
      const autoload = state.autoload || {};
      if (!state.detectorControl || !autoload.running || autoload.mode !== "simplon" || !autoload.simplonUrl) return null;
      return { url: autoload.simplonUrl, version: autoload.simplonVersion || "1.8.0", serial: "", model: "", locked: false };
    },
    logDetector: (text, kind) => detectorControlController?.log(text, kind),
    setStatus,
    setSummaryChip,
    setSectionBadgeState,
    // The detector's mask, saved as a file, opens like a series copied from it.
    openPath: async (path) => {
      await core.autoloadOrchestration.ensureFileMode({ disableMonitor: false });
      await openPathInViewer(path);
    },
    openEditor: () => pixelMaskEditController?.open() || false,
    closeEditor: (options) => pixelMaskEditController?.close(options),
    resetEditor: () => pixelMaskEditController?.reset(),
    isEditing: () => Boolean(pixelMaskEditController?.active),
    setMarkers: (markers) => pixelMaskEditController?.setMarkers(markers),
  },
});

pixelMaskEditController = createPixelMaskEditController({
  state,
  isMac: isMacPlatform,
  elements: {
    canvasWrap,
    overlay: document.getElementById("mask-edit-overlay"),
    bar: maskEditBar,
    toolButtons: Array.from(maskEditBar?.querySelectorAll("[data-mask-tool]") || []),
    modeButtons: Array.from(maskEditBar?.querySelectorAll("[data-mask-mode]") || []),
    brushSize: document.getElementById("mask-edit-brush-size"),
    undoBtn: document.getElementById("mask-edit-undo"),
    redoBtn: document.getElementById("mask-edit-redo"),
    doneBtn: document.getElementById("mask-edit-done"),
    countEl: document.getElementById("mask-edit-count"),
    hintEl: document.getElementById("mask-edit-hint"),
  },
  callbacks: {
    getTarget: () => pixelMaskController.getEditTarget(),
    onCommit: () => pixelMaskController.onEditCommit(),
    onClose: (options) => pixelMaskController.onEditClose(options),
    getImagePoint: (event, options) => getImagePointFromEvent(event, options),
    // The image->screen map the other overlays use.
    getView: () => {
      const zoom = state.zoom || 1;
      return {
        scaleX: zoom,
        scaleY: zoom * (state.pixelAspect || 1),
        offsetX: (state.renderOffsetX || 0) - getEffectiveScrollLeft(),
        offsetY: (state.renderOffsetY || 0) - getEffectiveScrollTop(),
      };
    },
    syncOverlayCanvas,
    describeEdits: () => pixelMaskController.describeEdits(),
    getMaskColors: () => currentMaskColors(state),
  },
});
pixelMaskController.setEnabled(state.pixelMask);
// The detectors this server has talked to, under the SIMPLON address fields.
void knownDetectors.start();

const {
  getTopOpenModal,
  focusModal,
  openModal,
  closeModal,
  trapModalFocus,
} = createModalManager({
  modalFocusRestore,
  modalStack,
  focusableSelector: MODAL_FOCUSABLE_SELECTOR,
});

/**
 * Work in progress that applying an update would abandon.
 *
 * Applying closes ALBIS, so anything mid-flight is a reason not to: a live
 * watch is following a running acquisition, and the three exports are writing
 * files. An open file on its own is not listed -- it is reopened in a second,
 * and the dialog already says ALBIS will close.
 *
 * Returned as codes rather than sentences because the backend is sent this
 * list and takes the same view of it.
 */
function collectUpdateApplyBlockers() {
  const blockers = [];
  if (state.autoload?.watchEnabled) blockers.push("live_watch");
  if (state.seriesSum?.running) blockers.push("series_sum");
  if (state.dataExport?.running) blockers.push("data_export");
  if (state.animationExport?.running) blockers.push("animation_export");
  return blockers;
}

updateCheckController = createUpdateCheckController({
  apiBase: API,
  state,
  elements: {
    ...appElements,

  },
  callbacks: {
    openModal: (...args) => openModal(...args),
    closeModal: (...args) => closeModal(...args),
    getApplyBlockers: () => collectUpdateApplyBlockers(),
  },
});
core.updateCheck = updateCheckController;

function closeTopModal(options = {}) {
  const modalEl = getTopOpenModal();
  if (!modalEl) return false;
  if (modalEl === commandModal) {
    core.commandPalette.close(options);
    return true;
  }
  if (modalEl === browseModal) {
    closeFileBrowser(options);
    return true;
  }
  if (modalEl === settingsModal) {
    closeSettingsModal(options);
    return true;
  }
  if (modalEl === updateCheckModal) {
    closeUpdateCheckModal(options);
    return true;
  }
  if (modalEl === logViewerModal) {
    closeBackendLogViewer(options);
    return true;
  }
  if (modalEl === aboutModal) {
    closeAboutModal(options);
    return true;
  }
  return closeModal(modalEl, options);
}

function openAboutModal() {
  closeMenu();
  openModal(aboutModal, { focusTarget: aboutClose });
}

function closeAboutModal({ restoreFocus = true } = {}) {
  closeModal(aboutModal, { restoreFocus });
}

function closeUpdateCheckModal({ restoreFocus = true } = {}) {
  updateCheckController?.close({ restoreFocus });
}

function closeBackendLogViewer({ restoreFocus = true } = {}) {
  backendLogViewerController?.close({ restoreFocus });
}

function closeSettingsModal({ restoreFocus = true } = {}) {
  settingsController.closeSettingsModal({ restoreFocus });
}

function isCommandPaletteOpen() {
  return Boolean(commandModal?.classList.contains("is-open"));
}

function getCommandPaletteCommands() {
  return buildCommandPaletteCommands({
    core,
    state,
    panelTabState,
    backendIsLocal,
    platformShortcutLabel,
    isHdfFile,
    getThresholdIndexAtOffset,
    actions: {
      closeCurrentFile,
      startPlayback,
      requestFrame,
      exportFullImage,
      exportVisibleArea,
      openDataExportDialog,
      openAnimationExportDialog,
      openImageExportDialog,
      toggleFullscreen,
      toggleMagnifier: () => magnifierController?.toggleLocked(),
      togglePixelValues: () => pixelLabelToggle?.click(),
      quitAlbis: () => quitController?.quit(),
      togglePanel,
      setPanelTab,
      handleMenuAction,
      openDebugFilePicker: () => openFileDialog(),
      openDebugFolderPicker: () => openFileBrowser("debug-folder", null),
      openDebugGeometryPicker: () => openFileDialog({ exts: ".expt" }),
    },
  });
}

const commandPaletteController = createCommandPaletteController({
  elements: {
    ...appElements,

  },
  callbacks: {
    getCommands: getCommandPaletteCommands,
    closeMenu,
    closeToolbarPlaybackPopover,
    closeToolbarMorePopover,
    focusModal,
    openModal,
    closeModal,
  },
});
const { handleKeydown: handleCommandPaletteKeydown, render: renderCommandPalette } = commandPaletteController;
core.commandPalette = commandPaletteController;

const menuActionHandler = createMenuActionHandler({
  core,
  state,
  callbacks: {
    toggleFullscreen,
    openAboutModal,
    closeCurrentFile,
    openDataExportDialog,
    openAnimationExportDialog,
    openImageExportDialog,
    exportFullImage,
    exportVisibleArea,
    quitAlbis: () => quitController?.quit(),
  },
});

async function handleMenuAction(action) {
  await menuActionHandler(action);
}

// Keeps Tab working as play/pause after the toolbar's own buttons are
// clicked; see the module for why only pointer clicks release focus.
createPointerFocusRelease().attach();
attachRangeFill();
attachAllNumberSteppers();

const { handleShortcut, handleNavShortcut } = createShortcutHandlers({
  core,
  state,
  elements: {
    ...appElements,

  },
  callbacks: {
    handleMenuAction,
    isCommandPaletteOpen,
    closeCurrentFile,
    startPlayback,
    getThresholdIndexAtOffset,
    toggleFullscreen,
    togglePanel,
    requestFrame,
  },
});

function option(label, value) {
  const opt = document.createElement("option");
  opt.value = value;
  opt.textContent = label;
  return opt;
}

function isSeriesCapable(path) {
  return isSeriesCapableFileUtil(path);
}

function fileLabel(path) {
  if (!path) return "";
  const parts = path.split(/[/\\\\]/);
  return parts[parts.length - 1] || path;
}

let renderEngineController = null;

async function loadDatasets() {
  resetLiveHistory();
  return fileDataPipelineController.loadDatasets();
}

function formatSimplonTimestamp(raw) {
  return sourceMetadataController
    ? sourceMetadataController.formatSimplonTimestamp(raw)
    : (raw ? String(raw) : "");
}

async function loadImageGeometry(file, scopeKey = getGeometryScopeKey(state, file)) {
  const key = String(scopeKey || getGeometryScopeKey(state, file) || "");
  const overridePath = geometryParamsController?.getActiveGeometryFile() || "";
  const requestKey = buildGeometryRequestKey(key || file || "", overridePath);
  if (!file) {
    core.sourceMetadata?.applyImageGeometry({ mode: "planar", panels: [] }, requestKey);
    return;
  }
  if (
    analysisState.ringGeometryKey &&
    analysisState.ringGeometryKey === requestKey &&
    (!overridePath || analysisState.ringMode === "geometry")
  ) {
    return;
  }
  analysisState.ringGeometryKey = requestKey;
  analysisState.geometryOverrideActive = false;
  const params = new URLSearchParams({ file: String(file) });
  if (overridePath) {
    params.set("geometry_file", overridePath);
  }
  try {
    const payload = await fetchJSON(`${API}/image/geometry?${params.toString()}`);
    if (analysisState.ringGeometryKey !== requestKey) return;
    core.sourceMetadata?.applyImageGeometry(payload, requestKey, { overrideActive: Boolean(overridePath) });
  } catch (err) {
    console.error(err);
    if (analysisState.ringGeometryKey !== requestKey) return;
    core.sourceMetadata?.applyImageGeometry({ mode: "planar", panels: [] }, requestKey);
  }
}

/** Fetch the open file's geometry again, after the geometry override changed. */
async function reloadGeometryForCurrentFile() {
  if (!state.file) return;
  if (isHdf5File(state.file)) {
    await frameMetadataController?.loadAnalysisParams();
    return;
  }
  await loadImageGeometry(state.file, getGeometryScopeKey(state, state.file));
}

/** The Rings and Peak Finder summaries' "Edit": open Data → Detector Geometry. */
function revealGeometrySection() {
  setPanelTab("data");
  if (geometrySection) {
    core.panelLayout?.setSectionState(geometrySection, false);
    geometrySection.scrollIntoView({ block: "nearest", behavior: "smooth" });
    const firstField = geometryOverrideToggle?.checked ? geometryInputs.distanceMm : geometryOverrideToggle;
    firstField?.focus({ preventScroll: true });
  }
}

function parseSimplonMeta(headers) {
  return sourceMetadataController ? sourceMetadataController.parseSimplonMeta(headers) : { analysis: {}, meta: {} };
}

function parseRemoteMeta(headers) {
  return sourceMetadataController ? sourceMetadataController.parseRemoteMeta(headers) : { analysis: {}, meta: {} };
}

function createLiveSourceSnapshot(payload) {
  return sourceMetadataController ? sourceMetadataController.createLiveSourceSnapshot(payload) : payload;
}

function appendLiveFrame(entry) {
  return liveHistoryController
    ? liveHistoryController.appendLiveFrame(entry)
    : { appended: false, rendered: false };
}

function updateLiveHistoryEntry(dedupeKey, patch) {
  return liveHistoryController ? liveHistoryController.updateLiveHistoryEntry(dedupeKey, patch) : false;
}

function resetLiveHistory() {
  liveSeriesWatch?.stop();
  liveHistoryController?.resetLiveHistory();
}

function parseDtype(header) {
  return header || state.dtype;
}

// Pixel values for an export, as the viewer shows them: whether they are on,
// the smallest cell they are drawn into, and each pixel's label in a frame.
function getPixelLabelSettings() {
  return {
    enabled: Boolean(state.pixelLabels),
    minCellPx: Math.max(8, Number(state.pixelLabelMinCellPx) || PIXEL_LABEL_DEFAULT_MIN_CELL_PX),
  };
}

function pixelLabelsForFrame({ data, values = null, filled = null, width, height, dtype = state.dtype }, cellPx) {
  const format = String(state.pixelLabelFormat || "auto").toLowerCase();
  const maskReady =
    state.maskEnabled &&
    state.maskAvailable &&
    state.maskRaw &&
    state.maskShape &&
    state.maskShape[0] === height &&
    state.maskShape[1] === width;
  const satMax = getActiveSaturationMax();
  return {
    float: pixelLabelsAreFloat(values ? "<f4" : dtype, format),
    labelAt: pixelLabelResolver({
      data,
      values,
      filled,
      mask: maskReady ? state.maskRaw : null,
      cellPx,
      format,
      dtype,
      isSaturated: state.maskSaturatedEnabled ? (value) => isSaturatedValue(value, satMax) : null,
    }),
  };
}

function getActiveSaturationMax() {
  return Number.isFinite(state.stats?.satMax) ? state.stats.satMax : null;
}

function isSaturatedValue(value, satMax) {
  if (!Number.isFinite(value) || !Number.isFinite(satMax)) return false;
  const tolerance = Math.max(1e-9, Math.abs(satMax) * 1e-6);
  return Math.abs(value - satMax) <= tolerance;
}

function computeAutoLevels(data, satMaxInput) {
  // The type of the values given: a flatfield-corrected frame is float even
  // when the raw one is integer, and integer data keeps a span of at least one
  // count, which would stretch transmission values (0 to 1) to twice their range.
  // (A raw 64-bit integer frame also arrives as a Float64Array: only a frame
  // other than the raw one is taken as float.)
  const dtype = data !== state.dataRaw && data instanceof Float32Array ? "<f4" : state.dtype;
  return computeAutoLevelsUtil(data, satMaxInput, state.stats, dtype);
}

function computeStats(data) {
  return computeStatsUtil(data, state.dtype, state.histLogX);
}

function mapValueToNorm(value) {
  return mapValueToNormUtil(value, {
    min: state.min,
    max: state.max,
    colormap: state.colormap,
    invert: state.invert,
  });
}

function getHistTooltipPosition(canvasRect, x) {
  const container = histCanvas.parentElement;
  if (!container) return { left: x, top: 0 };
  const containerRect = container.getBoundingClientRect();
  const left = x + canvasRect.left - containerRect.left + 8;
  const top = canvasRect.top - containerRect.top + 6;
  return { left, top };
}

overviewViewportController = createOverviewViewportController({
  core,
  state,
  overviewState: overviewInteractionState,
  elements: {
    ...appElements,
    overviewCtx,
    viewerFooterEl,
  },
  constants: {
    MIN_ZOOM,
    MAX_ZOOM,
    VIEWPORT_INTERACTION_IDLE_MS,
  },
  theme: {
    PLOT_THEME,
  },
  callbacks: {
    deferPixelOverlayRedraw,
    scheduleRoiOverlay,
    requestFrame,
    hasPendingFrameRequest,
    consumePendingFrameRequest,
    isFrameLoading,
    updateViewerFooter,
    onViewportChanged: ({ reason }) => {
      viewerSyncController?.handleViewportChanged(reason);
    },
  },
});
const {
  panToImageCenter,
  queueWheelZoom,
  resizeViewFromHandle,
  startTouchGesture,
  stopTouchGesture,
  updateTouchGesture,
  zoomAt,
} = overviewViewportController;
core.overviewViewport = overviewViewportController;

viewerSyncController = createViewerSyncController({
  state,
  roiState,
  elements: {
    syncWrap: viewerSyncWrap,
    syncToggle: viewerSyncToggle,
    syncOptionsToggle: viewerSyncOptionsToggle,
    syncPopover: viewerSyncPopover,
    syncViewportToggle: viewerSyncPositionToggle,
    syncContrastToggle: viewerSyncContrastToggle,
    syncRoiToggle: viewerSyncRoiToggle,
    canvasWrap,
  },
  callbacks: {
    getViewRect,
    getEffectiveScrollLeft,
    getEffectiveScrollTop,
    setZoom,
    setEffectiveScroll,
    applySyncedContrast,
    applySyncedRoi,
  },
});
core.viewerSync = viewerSyncController;

// The frame with its gaps and defective pixels filled, while inpainting is on:
// worked out once per frame, for the renderer, the statistics and the exports.
const inpaintView = createInpaintView({
  state,
  getActiveSaturationMax,
  isSaturatedValue,
  getFlat: () => flatfieldController?.getFlat() || null,
});
function getInpaintedFrame() {
  return inpaintView.frame();
}

// The frame the histogram and auto contrast come from: the flatfield-corrected
// one while flatfield correction applies, else null (the raw frame).
function getContrastData() {
  const view = inpaintView.frame();
  return view?.flatfielded ? view.data : null;
}

renderEngineController = createRenderEngineController({
  core,
  state,
  elements: {
    canvas,
    metaRenderer,
  },
  callbacks: {
    setStatus,
    toFloat32,
    isSaturatedValue,
    getWebglUnsignedDtypeKey,
    isWebglUnsignedRawCandidate,
    getWebglUnsignedUploadInfo,
    getPaletteColorCount,
    mapValueToNorm,
    buildPalette,
    getActiveSaturationMax,
    getInpaintedFrame,
    getUserAgent: () => navigator.userAgent,
    getRenderer: () => renderer,
    setRenderer: (nextRenderer) => {
      renderer = nextRenderer;
    },
  },
});
const { initRenderer } = renderEngineController;

const overlayRenderController = createOverlayRenderController({
  state,
  analysisState,
  elements: {
    ...appElements,
    pixelCtx,
    peakCtx,
    resolutionCtx,
  },
  constants: {
    pixelLabelDefaultMinCellPx: PIXEL_LABEL_DEFAULT_MIN_CELL_PX,
    pixelLabelDefaultMaxLabels: PIXEL_LABEL_DEFAULT_MAX_LABELS,
    pixelLabelDenseZoomPx: PIXEL_LABEL_DENSE_ZOOM_PX,
    pixelLabelInteractionIdleMs: PIXEL_LABEL_INTERACTION_IDLE_MS,
    pixelLabelHaloMaxLabels: PIXEL_LABEL_HALO_MAX_LABELS,
  },
  callbacks: {
    syncOverlayCanvas,
    getActiveSaturationMax,
    getEffectiveScrollLeft,
    getEffectiveScrollTop,
    formatPixelLabelValue,
    isSaturatedValue,
    getInpaintedFrame,
    getRingParams,
    updateRingsSectionState,
    getRingInteractionState,
    getRoiImageBounds: () => getRoiImageBounds(roiState, state.pixelAspect || 1),
  },
});
core.overlayRender = overlayRenderController;

function deferPixelOverlayRedraw(delayMs = PIXEL_LABEL_INTERACTION_IDLE_MS) {
  overlayRenderController.deferPixelOverlayRedraw(delayMs);
}

const histogramRenderController = createHistogramRenderController({
  state,
  elements: {
    histCanvas,
    histCtx,
    histColorbar,
    histColorCtx,
  },
  callbacks: {
    formatValue,
    buildPalette,
    getPaletteColorCount,
    mapValueToNorm,
  },
  constants: {
    PLOT_THEME,
  },
});
const { drawHistogram, histogramValueToX, histogramXToValue } = histogramRenderController;
core.histogramRender = histogramRenderController;

// Mask colours chosen for the colour map (modules/mask_colors.js), with a key
// under the colour bar, and the colour bar as a window/level control.
maskLegend = createMaskLegend({
  state,
  element: document.getElementById("hist-mask-legend"),
  getInpaintedFrame: () => getInpaintedFrame(),
  getActiveSaturationMax: () => getActiveSaturationMax(),
  isSaturatedValue: (value, satMax) => isSaturatedValue(value, satMax),
  onChange: () => {
    redraw();
    scheduleHistogram();
  },
});
bindColorbarWindowInteractions({
  core,
  state,
  elements: { colorbar: histColorbar, minInput, maxInput, autoScaleToggle },
  callbacks: {
    histogramValueToX: (value, width) => histogramValueToX(value, width),
    histogramXToValue: (x, width) => histogramXToValue(x, width),
    snapHistogramValue: (value) => snapHistogramValue(value),
    formatValue: (value) => formatValue(value),
    redraw: () => redraw(),
    scheduleHistogram: () => scheduleHistogram(),
    handleContrastChanged: (reason) => core.viewerSync?.handleContrastChanged(reason),
  },
});

const SYNC_ROI_MODES = new Set(["line", "box", "circle", "annulus"]);

function hasSelectOption(select, value) {
  if (!select || !value) return false;
  return Array.from(select.options || []).some((option) => option.value === value);
}

function applySyncedContrast(payload) {
  if (!state.hasFrame || !payload || typeof payload !== "object") return false;
  const min = Number(payload.min);
  const max = Number(payload.max);
  if (!Number.isFinite(min) || !Number.isFinite(max) || max < min) return false;

  state.autoScale = Boolean(payload.autoScale);
  state.min = min;
  state.max = max;
  state.invert = Boolean(payload.invert);
  const colormap = typeof payload.colormap === "string" ? payload.colormap : "";
  if (colormap && hasSelectOption(colormapSelect, colormap)) {
    state.colormap = colormap;
    colormapSelect.value = colormap;
  }
  if (autoScaleToggle) autoScaleToggle.checked = state.autoScale;
  if (minInput) minInput.value = formatValue(state.min);
  if (maxInput) maxInput.value = formatValue(state.max);
  if (invertToggle) invertToggle.checked = state.invert;
  redraw();
  scheduleHistogram();
  return true;
}

function cloneSyncedRoiPoint(point) {
  const x = Number(point?.x);
  const y = Number(point?.y);
  if (!Number.isFinite(x) || !Number.isFinite(y)) return null;
  return { x, y };
}

function applySyncedRoi(payload) {
  if (!payload || typeof payload !== "object") return false;
  const mode = SYNC_ROI_MODES.has(payload.mode) ? payload.mode : "line";
  const active = Boolean(payload.active);
  const start = active ? cloneSyncedRoiPoint(payload.start) : null;
  const end = active ? cloneSyncedRoiPoint(payload.end) : null;
  if (active && (!start || !end)) return false;

  const outerRadius = Math.max(0, Math.round(Number(payload.outerRadius) || 0));
  const innerRadius = Math.min(
    outerRadius,
    Math.max(0, Math.round(Number(payload.innerRadius) || 0)),
  );
  roiState.enabled = payload.enabled !== false;
  roiState.mode = mode;
  roiState.active = active;
  roiState.start = start;
  roiState.end = end;
  roiState.outerRadius = outerRadius;
  roiState.innerRadius = mode === "circle" ? 0 : innerRadius;
  roiState.stats = null;
  roiState.lineProfile = null;
  roiState.xProjection = null;
  roiState.yProjection = null;
  roiState.histogramDistribution = null;

  if (roiEnableToggle) roiEnableToggle.checked = roiState.enabled;
  if (roiModeSelect) roiModeSelect.value = roiState.mode;
  updateRoiModeUI();
  if (roiState.mode === "circle") {
    if (roiRadiusInput) roiRadiusInput.value = active ? String(outerRadius) : "";
  } else if (roiState.mode === "annulus") {
    if (roiInnerInput) roiInnerInput.value = active ? String(roiState.innerRadius) : "";
    if (roiOuterInput) roiOuterInput.value = active ? String(outerRadius) : "";
  } else {
    if (roiRadiusInput) roiRadiusInput.value = "";
    if (roiInnerInput) roiInnerInput.value = "";
    if (roiOuterInput) roiOuterInput.value = "";
  }
  core.roiStats.updateRoiCenterInputs();
  scheduleRoiOverlay();
  core.roiInteraction.scheduleRoiUpdate();
  return true;
}

function handleRoiChanged(reason = "change") {
  viewerSyncController?.handleRoiChanged(reason);
  // The resolution-ring labels place themselves clear of the ROI, so they are
  // stale the moment it moves. Every ROI mutation funnels through here. Via
  // the hoisted wrapper, not overlayRenderController directly: that is a const
  // declared further down the file, and this function can run before it.
  core.overlayRender.scheduleResolutionOverlay();
}

const roiStatsController = createRoiStatsController({
  state,
  roiState,
  roiCenterXInput,
  roiCenterYInput,
  roiCenterSnapBtn,
  roiParams,
  roiLinePlot,
  roiBoxPlotX,
  roiBoxPlotY,
  roiHistogramPlot: roiHistPlot,
  roiPlotControls,
  roiRadiusField,
  roiCenterFields,
  roiRingFields,
  roiSizeLabel,
  roiHelp,
  roiModeSelect,
  roiClearBtn,
  roiRadiusInput,
  roiInnerInput,
  roiOuterInput,
  roiStartEl,
  roiEndEl,
  roiSizeEl,
  roiAreaEl,
  roiTotalEl,
  roiGapEl,
  roiDefectiveEl,
  roiSaturatedEl,
  roiMinEl,
  roiMaxEl,
  roiSumEl,
  roiMedianEl,
  roiMeanEl,
  roiStdEl,
  roiLineCanvas,
  roiLineCtx,
  roiXCanvas,
  roiXCtx,
  roiYCanvas,
  roiYCtx,
  roiHistCanvas,
  roiHistCtx,
  roiHistogramToggle,
  roiHistBinsAuto,
  roiHistBinCount,
  roiHistBinChip,
  roiHistBinManualRow,
  roiHistBinPresetBtns,
  updateRoiSectionState,
  drawRoiOverlay: (...args) => drawRoiOverlay(...args),
  getActiveSaturationMax,
  getInpaintedFrame,
  isSaturatedValue,
  computeMedian,
  formatStat,
  formatRoiTick,
  PLOT_THEME,
  setStatus,
  getRingParams,
  getResolutionAtPixel,
});
const {
  applyRoiCenterFromInputs,
  clearRoiPlotLimitsForKey,
  exportRoiCsv,
  getRoiPlotKey,
  getRoiPlotLimits,
  getRoiPlotLog,
  hasAnyManualRoiPlotLimits,
  hasManualRoiPlotLimits,
  hideRoiTooltip,
  redrawRoiPlots,
  setRoiPlotAxisLimits,
  setRoiPlotLog,
  syncRoiPlotLimitControls,
  updateRoiModeUI,
  updateRoiTooltip,
} = roiStatsController;
core.roiStats = roiStatsController;

function closeCurrentFile() {
  liveSeriesWatch.stop();
  resetLiveHistory();
  fileSessionController.closeCurrentFile();
}

function redraw() {
  renderEngineController.redraw();
  // Which mask colours are on screen, and their chips under the colour bar.
  maskLegend?.update();
  // What inpainting filled in this frame, and the footer's "Inpainted";
  // whether the flat fits it, and "Flatfielded".
  inpaintingController?.updateState();
  flatfieldController?.updateState();
  // The lens follows the frame and the contrast.
  magnifierController?.refresh();
}

const mainUiBindingsElements = createMainUiBindingsElements({
  ...appElements,
  menuButtons,
  submenuParents,
  menuActions,
  canvasShell,
});

const mainUiBindingsCallbacks = createMainUiBindingsCallbacks({
  applyPlatformShortcutLabels,
  cancelClose,
  scheduleClose,
  isCoarsePointerDevice,
  openMenu,
  closeMenu,
  closeSubmenus,
  closeToolbarPlaybackPopover,
  closeToolbarMorePopover,
  closeFooterVersionPopover,
  registerChromeActivity,
  trapModalFocus,
  isCommandPaletteOpen,
  handleCommandPaletteKeydown,
  closeTopModal,
  getTopOpenModal,
  handleNavShortcut,
  handleMenuAction,
  handleShortcut,
  selectInspectorRow,
  renderInspectorLink,
  setSectionBadgeState,
  renderSkeletonBlock,
  renderInspectorTree,
  clearInspectorSearch,
  isDocumentDropEnabled: () => !backendIsLocal,
  showDocumentDropDisabledStatus,
  closeAboutModal,
  closeSettingsModal,
  renderCommandPalette,
  isHdfFile,
  loadDatasets,
  recordRecentFile,
  loadImageSeries,
  requestFrame,
  setFrameStep,
  setFps,
});

function initializeMainUiBindings() {
  initializeMainUiBindingsBootstrap(
    createMainUiBindingsContext({
      core,
      state,
      elements: mainUiBindingsElements,
      callbacks: mainUiBindingsCallbacks,
      activeMenuRef: () => activeMenu,
      commandPaletteController,
    }),
  );
}

const fileBrowserControllerContext = createFileBrowserControllerContext({
  core,
  apiBase: API,
  elements: {
    ...appElements,

  },
  callbacks: {
    openModal,
    closeModal,
    setStatus,
    getAutoloadState: () => state.autoload,
    persistAutoloadSettings,
  },
});

const {
  isBackendLocal: backendIsLocal,
  openFileBrowser,
  openFileDialog,
  closeFileBrowser,
  restoreFilesystemMode,
} = createFileBrowserController(fileBrowserControllerContext);

syncSplashHintMode();
applyI18nToDom(splashHint);

backendLogViewerController = createBackendLogViewerController({
  apiBase: API,
  backendIsLocal,
  elements: {
    ...appElements,

  },
  callbacks: {
    openModal: (...args) => openModal(...args),
    closeModal: (...args) => closeModal(...args),
    setStatus,
  },
});
core.backendLogViewer = backendLogViewerController;

initializeMainUiBindings();

// Canvas text is rasterised the moment it is drawn and does not re-flow when a
// `font-display: swap` face arrives later, the way DOM text does. The bundled
// woff2 comes off the same local origin as the page, so it is normally there
// well before a frame is open -- but on a cold cache the histogram and the
// splash can be painted first, and they would then stay in the fallback face
// beside DOM labels already using Inter. One redraw once the face is usable.
whenCanvasFontReady().then(() => {
  drawSplash();
  if (state.histogram) drawHistogram(state.histogram);
  core.overviewViewport?.scheduleOverview();
  scheduleHistogram();
  core.overlayRender.schedulePixelOverlay();
  scheduleRoiOverlay();
  core.overlayRender.scheduleResolutionOverlay();
  core.overlayRender.schedulePeakOverlay();
});

async function handleLocalFileSelection(mode) {
  fileInput.accept = ".h5,.hdf5,.tif,.tiff,.cbf,.cbf.gz,.edf,.cfg";
  fileInput.onchange = (event) => {
    const file = event.target.files?.[0];
    if (!file) return;

    if (mode === "autoload") {
      // For local filesystem, we can only use the HTML file input
      // Store the selected filename (this is a client-side only selection)
      autoloadDir.value = file.name;
      state.autoload.dir = file.name;
      persistAutoloadSettings();
      // Note: actual file loading from user's machine would require additional upload endpoint
      core.autoloadStatus?.setAutoloadStatus(t("autoload.status.local_file_selected_upload_needed"));
    }
  };
  fileInput.click();
}

const postFilePickerBindingsElements = createPostFilePickerBindingsElements({
  ...appElements,
  roiHistBinPresetBtns,
  panelTabs,
  sectionToggles,
  sectionSwitches,
  appLayout,
  roiHistogramPlot: roiHistPlot,
});

const postFilePickerBindingsCallbacks = createPostFilePickerBindingsCallbacks({
  getRingParams,
  updateAutoloadUI,
  persistAutoloadSettings,
  updateRemoteMetaUI,
  openFileBrowser,
  handleLocalFileSelection,
  logClient,
  redraw,
  scheduleHistogram,
  computeAutoLevels,
  getContrastData,
  formatValue,
  handleRoiChanged,
  redrawRoiPlots,
  schedulePeakFinder,
  snapHistogramValue,
  setZoom,
  zoomAt,
  requestFrame,
  startPlayback,
  toggleToolbarPlaybackPopover,
  toggleToolbarMorePopover,
  setFrameStep,
  setFps,
  togglePanel,
  closeToolbarMorePopover,
  toggleFullscreen,
  toggleFooterVersionPopover,
  registerChromeActivity,
  updateFullscreenUi,
  startMobilePanelDrag,
  updateMobilePanelDrag,
  stopRoiEdit,
  updateRoiModeUI,
  scheduleRoiOverlay,
  setStatus,
  exportRoiCsv,
  applyRoiCenterFromInputs,
  initializeSectionContentWrappers,
  initializePanelTabA11y,
  setPanelTab,
  toggleSection,
  loadStoredPanelTab,
  applyPanelState,
  setPanelWidth,
  normalizeWheelDelta,
  queueWheelZoom,
  startTouchGesture,
  updateTouchGesture,
  stopTouchGesture,
  isTouchGestureActive,
  getEffectiveScrollLeft,
  getEffectiveScrollTop,
  setEffectiveScroll,
  getImagePointFromEvent,
  getRoiHandleAt,
  isPointInRoi,
  startRoiEdit,
  updateCursorOverlay,
  isRoiEditing,
  applyRoiEdit,
  getRingHandleAt,
  startRingEdit,
  applyRingEdit,
  stopRingEdit,
  isRingEditing,
  updateRingHover,
  clearRingHover,
  hideCursorOverlay,
  getMinZoom,
  updateRoiTooltip,
  hideRoiTooltip,
  getRoiPlotKey,
  getRoiPlotLimits,
  getRoiPlotLog,
  setRoiPlotLog,
  setRoiPlotAxisLimits,
  syncRoiPlotLimitControls,
  clearRoiPlotLimitsForKey,
  hasManualRoiPlotLimits,
  hasAnyManualRoiPlotLimits,
  overviewEventToImage,
  overviewEventToOverview,
  getViewRect,
  getOverviewHandleAt,
  getAnchorForHandle,
  resizeViewFromHandle,
  panToImageCenter,
  histogramValueToX,
  histogramXToValue,
  getHistTooltipPosition,
  showHistTooltip,
  hideHistTooltip,
  drawHistogram,
  drawSplash,
  isHdfFile,
  loadDatasets,
  loadImageSeries,
  setRoiDragging: (next) => {
    roiDragging = next;
  },
  getRoiDragging: () => roiDragging,
  getSectionStateStore: () => sectionStateStore,
  setSectionStateStore: (next) => {
    sectionStateStore = next;
  },
});

initializePostFilePickerBindings(createPostFilePickerBindingsContext({
  core,
  apiBase: API,
  state,
  analysisState,
  roiState,
  backendIsLocal,
  overviewState: overviewInteractionState,
  constants: {
    MAX_ZOOM,
  },
  elements: postFilePickerBindingsElements,
  callbacks: postFilePickerBindingsCallbacks,
}));

initRenderer();

initializeUiDefaults({
  state,
  roiState,
  backendIsLocal,
  elements: {
    ...appElements,

  },
  callbacks: {
    restoreFilesystemMode,
    showSplash,
    drawSplash,
    setFps,
    setFrameStep,
    updateRoiModeUI,
  },
});

function validateSeriesStepInput(commit = false) {
  if (!seriesSumStep) return 1;
  if ((seriesSumResult?.value || "one") !== "groups") {
    setFieldHint(seriesSumStep, seriesSumStepHint, "");
    return 1;
  }
  const raw = String(seriesSumStep.value || "").trim();
  if (!raw) {
    setFieldHint(seriesSumStep, seriesSumStepHint, t("series.step_hint.integer_required"));
    return null;
  }
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) {
    setFieldHint(seriesSumStep, seriesSumStepHint, t("series.step_hint.integer_required"));
    return null;
  }
  const normalized = Math.max(1, Math.round(parsed));
  if (commit) {
    seriesSumStep.value = String(normalized);
  }
  if (normalized !== parsed && !commit) {
    setFieldHint(seriesSumStep, seriesSumStepHint, t("series.step_hint.using_nearest"));
  } else {
    setFieldHint(seriesSumStep, seriesSumStepHint, "");
  }
  return normalized;
}
quitController = createQuitController({
  apiBase: API,
  state,
  elements: { menuItem: document.getElementById("menu-quit") },
  callbacks: {
    collectBusyWork: () => collectUpdateApplyBlockers(),
    setStatus,
  },
});

magnifierController = createMagnifierController({
  state,
  elements: { canvasWrap, canvasShell, toggleBtn: document.getElementById("magnifier-toggle") },
  callbacks: {
    buildPalette,
    getPaletteColorCount,
    mapValueToNorm,
    getActiveSaturationMax,
    isSaturatedValue,
    getInpaintedFrame,
    getEffectiveScrollLeft,
    getEffectiveScrollTop,
    getPixelLabelSettings,
    pixelLabelsForFrame,
    onClearanceChange: () => maskCursorController.repositionCursorOverlay(),
  },
});

flatfieldController = createFlatfieldController({
  core,
  apiBase: API,
  state,
  elements: {
    toggle: document.getElementById("flat-toggle"),
    useCurrentBtn: document.getElementById("flat-use-current"),
    chooseBtn: document.getElementById("flat-choose"),
    scaleSelect: document.getElementById("flat-scale"),
    stateEl: document.getElementById("flat-state"),
    summaryEl: document.getElementById("summary-flat"),
    footerChip: document.getElementById("footer-flatfielded"),
  },
  callbacks: {
    redraw: () => {
      redraw();
      inpaintingController?.updateState();
    },
    refreshDisplayContrast: (updateLevels) => fileSessionController?.refreshDisplayContrast(updateLevels),
    scheduleHistogram,
    setSectionBadgeState,
    setSummaryChip,
    setStatus,
    getActiveSaturationMax,
    isSaturatedValue,
    parseDtype,
    parseShape,
    typedArrayFrom,
  },
});

inpaintingController = createInpaintingController({
  core,
  state,
  elements: {
    toggle: document.getElementById("inpaint-toggle"),
    saturatedToggle: document.getElementById("inpaint-saturated"),
    noiseToggle: document.getElementById("inpaint-noise"),
    marginInput: document.getElementById("inpaint-margin"),
    methodSelect: document.getElementById("inpaint-method"),
    valueRow: document.getElementById("inpaint-value-row"),
    valueInput: document.getElementById("inpaint-value"),
    backgroundBtn: document.getElementById("inpaint-value-background"),
    foregroundBtn: document.getElementById("inpaint-value-foreground"),
    stateEl: document.getElementById("inpaint-state"),
    summaryEl: document.getElementById("summary-inpaint"),
    footerChip: document.getElementById("footer-inpainted"),
  },
  callbacks: {
    getInpaintedFrame,
    redraw,
    setSectionBadgeState,
    setSummaryChip,
  },
});

// After every tool's controller: hiding a tool switches it off through its own
// switch, which needs that switch's handlers bound.
analysisToolsController = createAnalysisToolsController({
  state,
  elements: {
    summary: document.getElementById("analysis-hidden-tools"),
    summaryText: document.getElementById("analysis-hidden-tools-text"),
    customiseBtn: document.getElementById("analysis-hidden-tools-customise"),
  },
  callbacks: {
    openToolSettings: () => core.settings.openSettingsModal("tools"),
  },
});
analysisToolsController.apply(state.hiddenTools || []);

bindAnalysisControlInteractions({
  core,
  apiBase: API,
  state,
  analysisState,
  backendIsLocal,
  elements: {
    ...appElements,

  },
  callbacks: {
    setFieldHint,
    updateRingsSectionState,
    schedulePeakFinder,
    refreshPeakResolutions,
    exportPeakCsv,
    updateSeriesSumUi,
    validateSeriesStepInput,
    handleLocalFileSelection,
    openFileBrowser,
    useSeriesOutputAsFlat: () => seriesSumController.useOutputAsFlat(),
  },
});

renderPeakList();
setSeriesSumProgress(0, t("series.progress.idle"));
updateSeriesSumUi();
setDataExportProgress(0, t("data_export.progress.idle"));
updateDataExportUi();
onLanguageChange(() => {
  analysisToolsController?.refresh();
  maskLegend?.refresh();
  ringListController.refresh();
  refreshLocalizedUi();
  infoTips.refresh();
  updateDataExportUi();
  if (isHdf5File(state.file)) {
    void core.inspectorPanel.loadInspectorRoot();
  }
});
refreshLocalizedUi();
finalizeRuntimeBootstrap(createRuntimeBootstrapContext({
  state,
  callbacks: {
    getMaxPanelWidth,
    nearestMobilePanelSnap,
    applyPanelState,
    loadAutoloadSettings,
    updatePlayButtons,
    updateViewerFooter,
    setDataSourceSectionState,
    updateFullscreenUi,
    updateAboutVersion,
    initHelpTooltips,
    startBackendHeartbeat,
    bootstrapApp,
    setSplashStatus,
    setStatus,
    showSplash,
    setLoading,
  },
  core,
}));

/** Switch to `dataset` if the open HDF5 file lists it; false when it does not. */
async function selectDatasetIfPresent(dataset) {
  if (!datasetSelect) return false;
  const exists = Array.from(datasetSelect.options).some((opt) => opt.value === dataset);
  if (!exists) return false;
  state.dataset = dataset;
  datasetSelect.value = dataset;
  await core.frameMetadata?.loadMetadata();
  return true;
}

const windowCloneController = createWindowCloneController({
  core,
  state,
  analysisState,
  roiState,
  callbacks: {
    fileCommandUnavailableReason,
    setStatus,
    getViewport: () => viewerSyncController?.readViewport() || null,
    applyViewportCenter: (center) => viewerSyncController?.applyViewportCenter(center),
    openPathInViewer,
    isHdfFile,
    selectDatasetIfPresent,
    requestFrame,
    updateMaskUI,
    applySyncedContrast,
    applySyncedRoi,
    setRings: (rings) => ringListController.setRings(rings),
    updateRingsSectionState,
    updatePeaksSectionState: () => analysisOverlayController.updatePeaksSectionState(),
    schedulePeakFinder: () => analysisOverlayController.schedulePeakFinder(),
  },
});
core.windowClone = windowCloneController;

// Stable wrappers: the menu and the boot sequence are handed these before the
// controller above exists.
createHandoffController({
  apiBase: API,
  callbacks: { openPathInViewer, isHdfFile, selectDatasetIfPresent, setStatus },
}).start();
// Held for as long as this tab is open: see modules/presence.js.
startPresence({ apiBase: API });
