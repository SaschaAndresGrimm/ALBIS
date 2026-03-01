/*
 * ALBIS frontend controller.
 *
 * This file drives:
 * - UI state and interactions (menus, tabs, shortcuts, gestures)
 * - Data-source orchestration (file/monitor/remote/JUNGFRAUJOCH + optional watch)
 * - Rendering (WebGL2 primary + CPU fallback)
 * - Overlay layers (ROI, rings, peaks, pixel labels, histogram)
 *
 * Use `docs/DEVELOPER_GUIDE.md` for a quick function-level navigation guide.
 */

import { API, fetchJSON, fetchJSONWithInit } from "./modules/http.js";
import { createAnalysisState, createAppState, createRoiState } from "./modules/state.js";
import { applyPanelTab, loadStoredPanelTab } from "./modules/ui_panels.js";
import { createFileBrowserController } from "./modules/file_browser.js";
import { bindInspectorInteractions } from "./modules/inspector_bindings.js";
import { bindFileIngress } from "./modules/file_ingress_bindings.js";
import { bindMenuAndGlobalInteractions } from "./modules/menu_bindings.js";
import { bindDataControlInteractions } from "./modules/data_controls_bindings.js";
import { bindAnalysisControlInteractions } from "./modules/analysis_controls_bindings.js";
import { createHelpTooltipController } from "./modules/help_tooltips.js";
import { bindChromeUiInteractions } from "./modules/chrome_bindings.js";
import { finalizeRuntimeBootstrap, initializeUiDefaults } from "./modules/runtime_bootstrap.js";
import { bindClientLogging } from "./modules/client_logging_bindings.js";
import { createCommandPaletteController } from "./modules/command_palette.js";
import { createSettingsController } from "./modules/settings_controller.js";
import { createModalManager } from "./modules/modal_manager.js";
import { buildCommandPaletteCommands } from "./modules/command_palette_commands.js";
import { createUploadFlowController } from "./modules/upload_flow.js";
import { createMenuActionHandler } from "./modules/menu_actions.js";
import { createShortcutHandlers } from "./modules/shortcut_handlers.js";
import { createFileOpenController } from "./modules/file_open_flow.js";
import { createSeriesSumController } from "./modules/series_sum_controller.js";
import { createBackendStatusController } from "./modules/backend_status_controller.js";
import { createJfjochBridgeController } from "./modules/jfjoch_bridge_controller.js";
import { createRemoteStreamController } from "./modules/remote_stream_controller.js";
import { createAutoloadModeController } from "./modules/autoload_mode_controller.js";
import { createAutoloadOrchestrationController } from "./modules/autoload_orchestration_controller.js";
import { createAutoloadSettingsController } from "./modules/autoload_settings_controller.js";
import { createFileDataPipelineController } from "./modules/file_data_pipeline_controller.js";
import { createRoiStatsController } from "./modules/roi_stats_controller.js";
import { createOverlayRenderController } from "./modules/overlay_render_controller.js";
import { createHistogramRenderController } from "./modules/histogram_render_controller.js";
import { createRenderEngineController } from "./modules/render_engine_controller.js";
import { createOverviewViewportController } from "./modules/overview_viewport_controller.js";
import { initializePostFilePickerBindings } from "./modules/post_file_picker_bindings.js";
import {
  getWebglUnsignedDtypeKey as getWebglUnsignedDtypeKeyUtil,
  isWebglUnsignedRawCandidate as isWebglUnsignedRawCandidateUtil,
  getWebglUnsignedUploadInfo as getWebglUnsignedUploadInfoUtil,
  getDtypeInfo as getDtypeInfoUtil,
  chooseHistogramBins as chooseHistogramBinsUtil,
  computeHistogram as computeHistogramUtil,
  computeAutoLevels as computeAutoLevelsUtil,
  computeStats as computeStatsUtil,
  getPaletteColorCount as getPaletteColorCountUtil,
  mapValueToNorm as mapValueToNormUtil,
  buildPalette as buildPaletteUtil,
} from "./modules/intensity_scale_utils.js";

const platformHint = String(
  navigator.userAgentData?.platform || navigator.platform || navigator.userAgent || "",
).toLowerCase();
const isMacPlatform = platformHint.includes("mac");
const isWindowsPlatform = platformHint.includes("windows") || platformHint.includes("win32") || platformHint.includes("win64");
if (isWindowsPlatform) {
  document.body?.classList.add("platform-windows");
}

const fileSelect = document.getElementById("file-select");
const datasetSelect = document.getElementById("dataset-select");
const fileField = document.getElementById("file-field");
const datasetField = document.getElementById("dataset-field");
const thresholdField = document.getElementById("threshold-field");
const thresholdSelect = document.getElementById("threshold-select");
const toolbarThresholdWrap = document.getElementById("toolbar-threshold-wrap");
const toolbarThresholdSelect = document.getElementById("toolbar-threshold");
const toolbarFrameWrap = document.getElementById("toolbar-frame-wrap");
const toolbarFrameIndexWrap = document.getElementById("toolbar-frame-index-wrap");
const toolbarStepWrap = document.getElementById("toolbar-step-wrap");
const toolbarFpsWrap = document.getElementById("toolbar-fps-wrap");
const toolbarPlaybackWrap = document.getElementById("toolbar-playback-wrap");
const toolbarPlaybackToggle = document.getElementById("toolbar-playback-toggle");
const toolbarPlaybackPopover = document.getElementById("toolbar-playback-popover");
const toolbarMoreWrap = document.getElementById("toolbar-more-wrap");
const toolbarMoreToggle = document.getElementById("toolbar-more-toggle");
const toolbarMorePopover = document.getElementById("toolbar-more-popover");
const toolbarMoreStep = document.getElementById("toolbar-more-step");
const toolbarMoreFps = document.getElementById("toolbar-more-fps");
const toolbarMoreThresholdField = document.getElementById("toolbar-more-threshold-field");
const toolbarMoreThreshold = document.getElementById("toolbar-more-threshold");
const toolbarMorePanelToggle = document.getElementById("toolbar-more-panel-toggle");
const toolbarMoreFullscreen = document.getElementById("toolbar-more-fullscreen");
const frameRange = document.getElementById("frame-range");
const frameIndex = document.getElementById("frame-index");
const frameStep = document.getElementById("frame-step");
const fpsSelect = document.getElementById("fps-select");
const autoScaleToggle = document.getElementById("auto-scale");
const minInput = document.getElementById("min-input");
const maxInput = document.getElementById("max-input");
const maskToggle = document.getElementById("mask-toggle");
const maskSaturatedToggle = document.getElementById("mask-saturated-toggle");
const zoomRange = document.getElementById("zoom-range");
const zoomValue = document.getElementById("zoom-value");
const resetView = document.getElementById("reset-view");
const exportBtn = document.getElementById("export-btn");
const statusEl = document.getElementById("status");
const loadingEl = document.getElementById("loading");
const metaShape = document.getElementById("meta-shape");
const metaDtype = document.getElementById("meta-dtype");
const metaRange = document.getElementById("meta-range");
const metaRenderer = document.getElementById("meta-renderer");
const toolbarPath = document.getElementById("toolbar-path");
const backendBadge = document.getElementById("backend-badge");
const fullscreenToggle = document.getElementById("fullscreen-toggle");
const aboutVersion = document.getElementById("about-version");
const autoContrastBtn = document.getElementById("auto-contrast");
const invertToggle = document.getElementById("invert-color");
const colormapSelect = document.getElementById("colormap-select");
const prevBtn = document.getElementById("btn-prev");
const nextBtn = document.getElementById("btn-next");
const playBtn = document.getElementById("btn-play");
const toolsPanel = document.getElementById("side-panel");
const panelSheetHandle = document.getElementById("panel-sheet-handle");
const panelResizer = document.getElementById("panel-resizer");
const panelFab = document.getElementById("panel-fab");
const panelCollapseBtn = document.getElementById("panel-collapse-btn");
const panelBody = toolsPanel?.querySelector(".panel-body");
const panelTabs = document.querySelectorAll(".panel-tab");
const panelTabContents = document.querySelectorAll(".panel-tab-content");
const appLayout = document.querySelector(".app");
const canvasWrap = document.getElementById("canvas-wrap");
const canvas = document.getElementById("image-canvas");
const pixelOverlay = document.getElementById("pixel-overlay");
const pixelCtx = pixelOverlay?.getContext("2d");
const roiOverlay = document.getElementById("roi-overlay");
const roiCtx = roiOverlay?.getContext("2d");
const histCanvas = document.getElementById("hist-canvas");
const histCtx = histCanvas.getContext("2d");
const histColorbar = document.getElementById("hist-colorbar");
const histColorCtx = histColorbar?.getContext("2d");
const histTooltip = document.getElementById("hist-tooltip");
const histLogX = document.getElementById("hist-log-x");
const histLogY = document.getElementById("hist-log-y");
const overviewCanvas = document.getElementById("overview-canvas");
const overviewCtx = overviewCanvas?.getContext("2d");
const cursorOverlay = document.getElementById("cursor-overlay");
const canvasShell = document.querySelector(".canvas-shell");
const pixelLabelToggle = document.getElementById("pixel-label-toggle");
const sectionToggles = document.querySelectorAll("[data-section-toggle]");
const sectionSwitches = document.querySelectorAll(".section-switch");
const splash = document.getElementById("splash");
const splashCanvas = document.getElementById("splash-canvas");
const splashCtx = splashCanvas?.getContext("2d");
const splashStatus = document.getElementById("splash-status");
const splashActions = document.querySelector(".splash-actions");
const splashOpenFileBtn = document.getElementById("splash-open-file");
const viewerFooterEl = document.querySelector(".viewer-footer");
const footerFileEl = document.getElementById("footer-file");
const footerZoomEl = document.getElementById("footer-zoom");
const footerVersionToggleEl = document.getElementById("footer-version-toggle");
const footerVersionPopoverEl = document.getElementById("footer-version-popover");
const footerFrontendVersionEl = document.getElementById("footer-version-frontend");
const footerBackendVersionEl = document.getElementById("footer-version-backend");
const resolutionOverlay = document.getElementById("resolution-overlay");
const resolutionCtx = resolutionOverlay?.getContext("2d");
const peakOverlay = document.getElementById("peak-overlay");
const peakCtx = peakOverlay?.getContext("2d");
const autoloadMode = document.getElementById("autoload-mode");
const filesystemMode = document.getElementById("filesystem-mode");
const autoloadDir = document.getElementById("autoload-dir");
const autoloadInterval = document.getElementById("autoload-interval");
const autoloadStatus = document.getElementById("autoload-status");
const autoloadLatest = document.getElementById("autoload-latest");
const autoloadFolder = document.getElementById("autoload-folder");
const autoloadWatch = document.getElementById("autoload-watch");
const autoloadWatchEnabled = document.getElementById("autoload-watch-enabled");
const autoloadWatchOptions = document.getElementById("autoload-watch-options");
const autoloadTypesRow = document.getElementById("autoload-types");
const autoloadSimplon = document.getElementById("autoload-simplon");
const autoloadRemote = document.getElementById("autoload-remote");
const autoloadJfjoch = document.getElementById("autoload-jfjoch");
const simplonMetaPanel = document.getElementById("simplon-meta");
const simplonSeriesEl = document.getElementById("simplon-series");
const simplonImageEl = document.getElementById("simplon-image");
const simplonTimeEl = document.getElementById("simplon-time");
const simplonEnergyEl = document.getElementById("simplon-energy");
const simplonThresholdEl = document.getElementById("simplon-threshold");
const simplonWavelengthEl = document.getElementById("simplon-wavelength");
const simplonDistanceEl = document.getElementById("simplon-distance");
const simplonCenterEl = document.getElementById("simplon-center");
const remoteSourceInput = document.getElementById("remote-source-id");
const remoteIntervalInput = document.getElementById("remote-interval");
const remoteMetaPanel = document.getElementById("remote-meta");
const remoteSourceEl = document.getElementById("remote-source");
const remoteSeqEl = document.getElementById("remote-seq");
const remoteSeriesEl = document.getElementById("remote-series");
const remoteImageEl = document.getElementById("remote-image");
const remoteTimeEl = document.getElementById("remote-time");
const remoteEnergyEl = document.getElementById("remote-energy");
const remoteWavelengthEl = document.getElementById("remote-wavelength");
const remoteDistanceEl = document.getElementById("remote-distance");
const remoteCenterEl = document.getElementById("remote-center");
const remotePeakSetsEl = document.getElementById("remote-peak-sets");
const jfjochMetaPanel = document.getElementById("jfjoch-meta");
const jfjochSourceEl = document.getElementById("jfjoch-source");
const jfjochSeqEl = document.getElementById("jfjoch-seq");
const jfjochSeriesEl = document.getElementById("jfjoch-series");
const jfjochImageEl = document.getElementById("jfjoch-image");
const jfjochTimeEl = document.getElementById("jfjoch-time");
const jfjochReflectionsEl = document.getElementById("jfjoch-reflections");
const jfjochChannelMetaEl = document.getElementById("jfjoch-channel-meta");
const jfjochBridgeStatusEl = document.getElementById("jfjoch-bridge-status");
const dataSourceSummaryEl = document.getElementById("summary-data-source");
const dataSourceStateEl = document.getElementById("data-source-state");
const dataSourceSkeletonEl = document.getElementById("data-source-skeleton");
const inspectorSection = document.querySelector(".panel-section.inspector");
const imageHeaderSection = document.getElementById("image-header-section");
const imageHeaderStateEl = document.getElementById("image-header-state");
const imageHeaderText = document.getElementById("image-header-text");
const imageHeaderEmpty = document.getElementById("image-header-empty");
const imageHeaderLoading = document.getElementById("image-header-loading");
const inspectorTree = document.getElementById("inspector-tree");
const inspectorDetails = document.getElementById("inspector-details");
const inspectorPath = document.getElementById("inspector-path");
const inspectorType = document.getElementById("inspector-type");
const inspectorShape = document.getElementById("inspector-shape");
const inspectorDtype = document.getElementById("inspector-dtype");
const inspectorAttrs = document.getElementById("inspector-attrs");
const inspectorPreview = document.getElementById("inspector-preview");
const inspectorSearchInput = document.getElementById("inspector-search-input");
const inspectorSearchClear = document.getElementById("inspector-search-clear");
const inspectorResults = document.getElementById("inspector-results");
const inspectorStateEl = document.getElementById("inspector-state");
const autoloadBrowse = document.getElementById("autoload-browse");
const autoloadSelectFile = document.getElementById("autoload-select-file");
const autoloadDirList = document.getElementById("autoload-dir-list");
const autoloadPattern = document.getElementById("autoload-pattern");
const autoloadTypeHdf5 = document.getElementById("autoload-type-hdf5");
const autoloadTypeTiff = document.getElementById("autoload-type-tiff");
const autoloadTypeCbf = document.getElementById("autoload-type-cbf");
const simplonUrl = document.getElementById("simplon-url");
const simplonVersion = document.getElementById("simplon-version");
const simplonTimeout = document.getElementById("simplon-timeout");
const simplonEnable = document.getElementById("simplon-enable");
const jfjochEndpointInput = document.getElementById("jfjoch-preview-endpoint");
const jfjochSourceInput = document.getElementById("jfjoch-source-id");
const jfjochTopicInput = document.getElementById("jfjoch-topic");
const jfjochChannelInput = document.getElementById("jfjoch-channel");
const jfjochIntervalInput = document.getElementById("jfjoch-interval");
const liveBadge = document.getElementById("live-badge");
const roiHelp = document.getElementById("roi-help");
const roiModeSelect = document.getElementById("roi-mode");
const roiLogToggle = document.getElementById("roi-log");
const roiPlotControls = document.getElementById("roi-plot-controls");
const roiParams = document.getElementById("roi-params");
const roiRadiusField = document.getElementById("roi-radius-field");
const roiRadiusInput = document.getElementById("roi-radius");
const roiCenterFields = document.getElementById("roi-center-fields");
const roiCenterXInput = document.getElementById("roi-center-x");
const roiCenterYInput = document.getElementById("roi-center-y");
const roiRingFields = document.getElementById("roi-ring-fields");
const roiInnerInput = document.getElementById("roi-inner-radius");
const roiOuterInput = document.getElementById("roi-outer-radius");
const roiLimitsEnable = document.getElementById("roi-limits-enable");
const roiExportCsvBtn = document.getElementById("roi-export-csv");
const roiClearBtn = document.getElementById("roi-clear-btn");
const roiStartEl = document.getElementById("roi-start");
const roiEndEl = document.getElementById("roi-end");
const roiSizeLabel = document.getElementById("roi-size-label");
const roiSizeEl = document.getElementById("roi-size");
const roiTotalEl = document.getElementById("roi-total");
const roiGapEl = document.getElementById("roi-gap");
const roiDefectiveEl = document.getElementById("roi-defective");
const roiSaturatedEl = document.getElementById("roi-saturated");
const roiMinEl = document.getElementById("roi-min");
const roiMaxEl = document.getElementById("roi-max");
const roiSumEl = document.getElementById("roi-sum");
const roiMedianEl = document.getElementById("roi-median");
const roiMeanEl = document.getElementById("roi-mean");
const roiStdEl = document.getElementById("roi-std");
const roiLinePlot = document.getElementById("roi-line-plot");
const roiBoxPlotX = document.getElementById("roi-box-plot-x");
const roiBoxPlotY = document.getElementById("roi-box-plot-y");
const roiLineCanvas = document.getElementById("roi-line-canvas");
const roiLineCtx = roiLineCanvas?.getContext("2d");
const roiXCanvas = document.getElementById("roi-x-canvas");
const roiXCtx = roiXCanvas?.getContext("2d");
const roiYCanvas = document.getElementById("roi-y-canvas");
const roiYCtx = roiYCanvas?.getContext("2d");
const roiEnableToggle = document.getElementById("roi-enable");
const roiSectionStateEl = document.getElementById("roi-state");
const roiSummaryEl = document.getElementById("summary-roi");
const ringsToggle = document.getElementById("rings-toggle");
const ringsDistance = document.getElementById("rings-distance");
const ringsDistanceHint = document.getElementById("rings-distance-hint");
const ringsPixel = document.getElementById("rings-pixel");
const ringsPixelHint = document.getElementById("rings-pixel-hint");
const ringsEnergy = document.getElementById("rings-energy");
const ringsEnergyHint = document.getElementById("rings-energy-hint");
const ringsCenterX = document.getElementById("rings-center-x");
const ringsCenterY = document.getElementById("rings-center-y");
const ringsSectionStateEl = document.getElementById("rings-state");
const ringsSummaryEl = document.getElementById("summary-rings");
const ringInputs = [
  document.getElementById("ring-r1"),
  document.getElementById("ring-r2"),
  document.getElementById("ring-r3"),
].filter(Boolean);
const peaksEnableToggle = document.getElementById("peaks-enable");
const peaksCountInput = document.getElementById("peaks-count");
const peaksCountHint = document.getElementById("peaks-count-hint");
const peaksExportBtn = document.getElementById("peaks-export");
const peaksBody = document.getElementById("peaks-body");
const peaksSectionStateEl = document.getElementById("peaks-state");
const peaksSummaryEl = document.getElementById("summary-peaks");
const seriesSumMode = document.getElementById("series-sum-mode");
const seriesSumOperation = document.getElementById("series-sum-operation");
const seriesSumStepField = document.getElementById("series-sum-step-field");
const seriesSumStepLabel = document.getElementById("series-sum-step-label");
const seriesSumStep = document.getElementById("series-sum-step");
const seriesSumStepHint = document.getElementById("series-sum-step-hint");
const seriesSumRangeStartField = document.getElementById("series-sum-range-start-field");
const seriesSumRangeEndField = document.getElementById("series-sum-range-end-field");
const seriesSumRangeStart = document.getElementById("series-sum-range-start");
const seriesSumRangeEnd = document.getElementById("series-sum-range-end");
const seriesSumNormalizeEnable = document.getElementById("series-sum-normalize-enable");
const seriesSumNormalizeFrameField = document.getElementById("series-sum-normalize-frame-field");
const seriesSumNormalizeFrame = document.getElementById("series-sum-normalize-frame");
const seriesSumOutput = document.getElementById("series-sum-output");
const seriesSumBrowse = document.getElementById("series-sum-browse");
const seriesSumFormat = document.getElementById("series-sum-format");
const seriesSumMask = document.getElementById("series-sum-mask");
const seriesSumStart = document.getElementById("series-sum-start");
const seriesSumCancel = document.getElementById("series-sum-cancel");
const seriesSumProgress = document.getElementById("series-sum-progress");
const seriesSumProgressFill = document.getElementById("series-sum-progress-fill");
const seriesSumProgressText = document.getElementById("series-sum-progress-text");
const menuButtons = document.querySelectorAll(".menu-item[data-menu]");
const dropdown = document.getElementById("menu-dropdown");
const dropdownPanels = document.querySelectorAll(".dropdown-panel");
const submenuParents = document.querySelectorAll(".dropdown-submenu-parent");
const menuActions = document.querySelectorAll(".dropdown-item[data-action]");
const aboutModal = document.getElementById("about-modal");
const aboutClose = document.getElementById("about-close");
const settingsModal = document.getElementById("settings-modal");
const settingsClose = document.getElementById("settings-close");
const settingsCancel = document.getElementById("settings-cancel");
const settingsSave = document.getElementById("settings-save");
const settingsSaveClose = document.getElementById("settings-save-close");
const settingsConfigPath = document.getElementById("settings-config-path");
const settingsMessage = document.getElementById("settings-message");
const settingsServerExternal = document.getElementById("settings-server-external");
const settingsServerPort = document.getElementById("settings-server-port");
const settingsServerReload = document.getElementById("settings-server-reload");
const settingsStartupTimeout = document.getElementById("settings-startup-timeout");
const settingsOpenBrowser = document.getElementById("settings-open-browser");
const settingsToolHints = document.getElementById("settings-tool-hints");
const settingsPixelLabelMin = document.getElementById("settings-pixel-label-min");
const settingsPixelLabelMax = document.getElementById("settings-pixel-label-max");
const settingsPixelLabelFormat = document.getElementById("settings-pixel-label-format");
const settingsPixelLabelDrag = document.getElementById("settings-pixel-label-drag");
const settingsDataRoot = document.getElementById("settings-data-root");
const settingsAllowAbs = document.getElementById("settings-allow-abs");
const settingsScanCache = document.getElementById("settings-scan-cache");
const settingsMaxScanDepth = document.getElementById("settings-max-scan-depth");
const settingsMaxUpload = document.getElementById("settings-max-upload");
const settingsLogLevel = document.getElementById("settings-log-level");
const settingsLogDir = document.getElementById("settings-log-dir");
const fileInput = document.getElementById("file-input");
const uploadBar = document.getElementById("upload-bar");
const uploadBarFill = document.getElementById("upload-bar-fill");
const uploadBarText = document.getElementById("upload-bar-text");
const commandModal = document.getElementById("command-modal");
const commandInput = document.getElementById("command-input");
const commandList = document.getElementById("command-list");
const browseModal = document.getElementById("browse-modal");
const browseBreadcrumb = document.getElementById("browse-breadcrumb");
const browseFoldersList = document.getElementById("browse-folders-list");
const browseFilesList = document.getElementById("browse-files-list");
const browsePathInput = document.getElementById("browse-path-input");
const browseStatus = document.getElementById("browse-status");
const browseSelectBtn = document.getElementById("browse-select");
const browseCancelBtn = document.getElementById("browse-cancel");
const browseCloseBtn = document.getElementById("browse-close");

let renderer = null;
let overviewViewportController = null;
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
let roiOverlayScheduled = false;
let roiUpdateScheduled = false;
let roiDragging = false;
let roiEditing = false;
let roiEditHandle = null;
let roiEditStart = null;
let roiEditSnapshot = null;
let peakFinderScheduled = false;
let activeFrameLoadController = null;
let panelTabState = "view";
let inspectorSelectedRow = null;
let inspectorSearchRequestId = 0;
let sectionA11yCounter = 0;
const coarsePointerQuery = window.matchMedia("(hover: none), (pointer: coarse)");
let mobilePanelSnap = 0.6;
let mobilePanelDragActive = false;
let mobilePanelDragPointer = null;
let mobilePanelDragStartY = 0;
let mobilePanelDragStartSnap = mobilePanelSnap;
let html2canvasLoadPromise = null;
let footerVersionPopoverOpen = false;
let chromeIdleTimer = null;
let chromeIdleActive = false;
let chromeActivityTs = 0;
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
  open: { mac: "⌘O", other: "Ctrl+O" },
  "close-file": { mac: "⌘W", other: "Ctrl+W" },
  "save-full": { mac: "⌘S", other: "Ctrl+S" },
  "save-visible": { mac: "⇧⌘S", other: "Shift+Ctrl+S" },
  "save-window": { mac: "⌥⌘S", other: "Alt+Ctrl+S" },
  "export-full": { mac: "⌘E", other: "Ctrl+E" },
  "export-visible": { mac: "⇧⌘E", other: "Shift+Ctrl+E" },
  "export-window": { mac: "⌥⌘E", other: "Alt+Ctrl+E" },
  "settings-open": { mac: "⌘,", other: "Ctrl+," },
  "command-palette": { mac: "⌘K", other: "Ctrl+K" },
};

const roiState = createRoiState();
const analysisState = createAnalysisState();
const state = createAppState();

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
};

const clientLogBuffer = [];
let clientLogTimer = null;
let clientLogSending = false;
const SECTION_STATE_VARIANTS = ["is-empty", "is-loading", "is-active", "is-warning"];
const CHROME_IDLE_DELAY_MS = 2200;

function formatClientArg(arg) {
  if (arg instanceof Error) {
    return `${arg.message}${arg.stack ? `\n${arg.stack}` : ""}`;
  }
  if (typeof arg === "object") {
    try {
      return JSON.stringify(arg);
    } catch {
      return String(arg);
    }
  }
  return String(arg);
}

function logClient(level, message, context, extra) {
  if (!message) return;
  clientLogBuffer.push({
    level,
    message: String(message).slice(0, 2000),
    context,
    extra,
    url: window.location.href,
    userAgent: navigator.userAgent,
  });
  if (!clientLogTimer) {
    clientLogTimer = window.setTimeout(flushClientLogs, 250);
  }
}

async function flushClientLogs() {
  clientLogTimer = null;
  if (clientLogSending || clientLogBuffer.length === 0) return;
  clientLogSending = true;
  const entry = clientLogBuffer.shift();
  try {
    await fetch(`${API}/client-log`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(entry),
    });
  } catch {
    // drop on network errors
  } finally {
    clientLogSending = false;
    if (clientLogBuffer.length) {
      clientLogTimer = window.setTimeout(flushClientLogs, 250);
    }
  }
}

bindClientLogging({
  formatClientArg,
  logClient,
});
const MIN_ZOOM = 0.02;
const MAX_ZOOM = 50;
const APP_FRONTEND_BUILD = "local";
const DEFAULT_RING_COUNT = 3;
const MOBILE_PANEL_SNAP_POINTS = [0.6, 1];
const FRAME_STEP_OPTIONS = [1, 10, 100, 1000];
const PIXEL_LABEL_DEFAULT_MIN_CELL_PX = 18;
const PIXEL_LABEL_DEFAULT_MAX_LABELS = 4000;
const PIXEL_LABEL_DENSE_ZOOM_PX = 24;
const PIXEL_LABEL_INTERACTION_IDLE_MS = 140;
const PIXEL_LABEL_HALO_MAX_LABELS = 3200;
const VIEWPORT_INTERACTION_IDLE_MS = 180;
const PEAK_BAD_MASK_BITS = 0x1f;

function getMinZoom() {
  return overviewViewportController ? overviewViewportController.getMinZoom() : MIN_ZOOM;
}

function getEffectiveScrollLeft() {
  return overviewViewportController ? overviewViewportController.getEffectiveScrollLeft() : 0;
}

function getEffectiveScrollTop() {
  return overviewViewportController ? overviewViewportController.getEffectiveScrollTop() : 0;
}

function applyCanvasTransform() {
  overviewViewportController?.applyCanvasTransform();
}

function isViewportInteractionActive() {
  return overviewViewportController ? overviewViewportController.isViewportInteractionActive() : false;
}

function cancelActiveFrameLoad() {
  if (!activeFrameLoadController) return;
  try {
    activeFrameLoadController.abort();
  } catch {
    // ignore abort errors
  }
}

function deferViewportInteraction(delayMs = VIEWPORT_INTERACTION_IDLE_MS) {
  overviewViewportController?.deferViewportInteraction(delayMs);
}

function setEffectiveScroll(targetX, targetY, schedule = true) {
  overviewViewportController?.setEffectiveScroll(targetX, targetY, schedule);
}

function updatePanCapability() {
  overviewViewportController?.updatePanCapability();
}

function setStatus(text) {
  if (!statusEl) return;
  const normalized = String(text || "").trim();
  statusEl.textContent = /^Frame\s+\d+\s*\/\s*\d+$/i.test(normalized) ? "Ready" : normalized || "Idle";
  updateViewerFooter();
}

function currentFrameStatusText() {
  const total = Math.max(1, Number(state.frameCount) || 1);
  const index = Math.max(0, Math.min(total - 1, Number(state.frameIndex) || 0));
  return `Frame ${index + 1} / ${total}`;
}

function middleTruncate(text, maxChars) {
  const value = String(text || "");
  if (!value) return "";
  const limit = Math.max(6, Math.floor(Number(maxChars) || 0));
  if (value.length <= limit) return value;
  const side = Math.max(2, Math.floor((limit - 1) / 2));
  return `${value.slice(0, side)}…${value.slice(-side)}`;
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

function compactCount(value) {
  if (!Number.isFinite(value)) return "";
  const abs = Math.abs(value);
  const sign = value < 0 ? "-" : "";
  if (abs >= 1e9) return `${sign}${(abs / 1e9).toFixed(abs >= 1e10 ? 0 : 1).replace(/\.0$/, "")}G`;
  if (abs >= 1e6) return `${sign}${(abs / 1e6).toFixed(abs >= 1e7 ? 0 : 1).replace(/\.0$/, "")}M`;
  if (abs >= 1e3) return `${sign}${(abs / 1e3).toFixed(abs >= 1e4 ? 0 : 1).replace(/\.0$/, "")}k`;
  return `${Math.round(value)}`;
}

function formatPixelLabelValue(value, cellPx, mode = "auto") {
  if (!Number.isFinite(value)) return "";
  const pixelWidth = Math.max(8, Number(cellPx) || 0);
  const maxChars = Math.max(1, Math.floor((pixelWidth - 2) / 5.6));
  const rounded = Math.round(value);
  const scientific = Number(value).toExponential(1).replace("+", "");
  const compact = compactCount(rounded);
  const integer = String(rounded);

  if (mode === "integer") {
    return integer.length <= maxChars ? integer : compact.length <= maxChars ? compact : "";
  }
  if (mode === "scientific") {
    return scientific.length <= maxChars ? scientific : "";
  }

  // Auto: prioritize readability and fit.
  if (integer.length <= maxChars) return integer;
  if (compact.length <= maxChars) return compact;
  if (scientific.length <= maxChars) return scientific;
  return "";
}

function formatStat(value) {
  if (!Number.isFinite(value)) return "-";
  const abs = Math.abs(value);
  if (abs >= 1e6 || (abs > 0 && abs < 1e-3)) {
    return value.toExponential(3);
  }
  if (Number.isInteger(value)) {
    return value.toString();
  }
  return value.toFixed(3);
}

function formatRoiTick(value) {
  if (!Number.isFinite(value)) return "-";
  const abs = Math.abs(value);
  if (abs >= 1e5 || (abs > 0 && abs < 1e-3)) {
    return value.toExponential(2);
  }
  if (abs >= 1000) return value.toFixed(0);
  if (abs >= 100) return value.toFixed(1);
  if (abs >= 10) return value.toFixed(2);
  return value.toFixed(3).replace(/0+$/, "").replace(/\.$/, "");
}



const {
  setToolHintsEnabled,
  initHelpTooltips,
} = createHelpTooltipController({
  state,
  platformShortcutLabel,
  roiCanvases: [roiLineCanvas, roiXCanvas, roiYCanvas],
});

window.addEventListener("DOMContentLoaded", initHelpTooltips, { once: true });

function quickSelect(values, k) {
  let left = 0;
  let right = values.length - 1;
  while (left < right) {
    const pivot = values[(left + right) >> 1];
    let i = left;
    let j = right;
    while (i <= j) {
      while (values[i] < pivot) i += 1;
      while (values[j] > pivot) j -= 1;
      if (i <= j) {
        [values[i], values[j]] = [values[j], values[i]];
        i += 1;
        j -= 1;
      }
    }
    if (k <= j) {
      right = j;
    } else if (k >= i) {
      left = i;
    } else {
      break;
    }
  }
  return values[k];
}

function computeMedian(values) {
  if (!values || values.length === 0) return Number.NaN;
  const n = values.length;
  const work = values.slice();
  const mid = Math.floor(n / 2);
  const high = quickSelect(work, mid);
  if (n % 2 === 1) {
    return high;
  }
  const low = quickSelect(work, mid - 1);
  return (low + high) * 0.5;
}

function formatEnergy(value) {
  if (!Number.isFinite(value)) return "";
  const rounded = Math.round(value);
  if (Math.abs(value - rounded) < 0.05) {
    return String(rounded);
  }
  return value.toFixed(1);
}

function isHdf5File(path) {
  return typeof path === "string" && (path.toLowerCase().endsWith(".h5") || path.toLowerCase().endsWith(".hdf5"));
}

function isHeaderCapableFile(path) {
  if (typeof path !== "string") return false;
  const lower = path.toLowerCase();
  return (
    lower.endsWith(".cbf") ||
    lower.endsWith(".cbf.gz") ||
    lower.endsWith(".edf") ||
    lower.endsWith(".tif") ||
    lower.endsWith(".tiff")
  );
}

function formatInspectorValue(value) {
  if (value === null || value === undefined) return "-";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

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

function setImageHeaderSectionState(tone, message) {
  setSectionBadgeState(imageHeaderStateEl, tone, message);
}

function setImageHeaderLoading(loading) {
  if (!imageHeaderLoading) return;
  if (loading) {
    renderSkeletonBlock(imageHeaderLoading, 7);
    imageHeaderLoading.classList.remove("is-hidden");
    imageHeaderLoading.setAttribute("aria-hidden", "false");
    return;
  }
  imageHeaderLoading.classList.add("is-hidden");
  imageHeaderLoading.setAttribute("aria-hidden", "true");
  imageHeaderLoading.innerHTML = "";
}

function setInspectorMessage(message) {
  if (!inspectorTree) return;
  inspectorSelectedRow = null;
  inspectorTree.innerHTML = `<div class="inspector-empty">${message}</div>`;
  resetInspectorDetails();
  setSectionBadgeState(inspectorStateEl, /fail|error/i.test(message) ? "warning" : "empty", message);
  if (inspectorResults) {
    inspectorResults.innerHTML = "";
    inspectorResults.classList.add("is-hidden");
  }
}

function setImageHeader(text) {
  if (!imageHeaderText || !imageHeaderEmpty) return;
  const headerText = typeof text === "string" ? text.trim() : "";
  const hasText = headerText.length > 0;
  setImageHeaderLoading(false);
  imageHeaderText.textContent = hasText ? text : "";
  imageHeaderText.classList.toggle("is-hidden", !hasText);
  imageHeaderEmpty.classList.toggle("is-hidden", hasText);
  if (hasText) {
    setImageHeaderSectionState("active", "Header loaded.");
  } else {
    setImageHeaderSectionState("empty", "No header available.");
  }
}

function clearImageHeader() {
  state.imageHeaderFile = "";
  state.imageHeaderText = "";
  setImageHeaderLoading(false);
  setImageHeader("");
}

async function loadImageHeader(file) {
  if (!file || !isHeaderCapableFile(file)) {
    clearImageHeader();
    return;
  }
  if (state.imageHeaderFile === file && state.imageHeaderText) {
    setImageHeader(state.imageHeaderText);
    return;
  }
  setImageHeaderSectionState("loading", "Loading image header…");
  setImageHeaderLoading(true);
  if (imageHeaderText) {
    imageHeaderText.textContent = "";
    imageHeaderText.classList.add("is-hidden");
  }
  if (imageHeaderEmpty) {
    imageHeaderEmpty.classList.add("is-hidden");
  }
  try {
    const data = await fetchJSON(`${API}/image/header?file=${encodeURIComponent(file)}`);
    const text = typeof data.header === "string" ? data.header : "";
    state.imageHeaderFile = file;
    state.imageHeaderText = text;
    setImageHeader(text);
  } catch (err) {
    console.warn(err);
    state.imageHeaderFile = file;
    state.imageHeaderText = "";
    setImageHeaderLoading(false);
    if (imageHeaderText) {
      imageHeaderText.textContent = "";
      imageHeaderText.classList.add("is-hidden");
    }
    if (imageHeaderEmpty) {
      imageHeaderEmpty.classList.remove("is-hidden");
      imageHeaderEmpty.textContent = "No header available.";
    }
    setImageHeaderSectionState("warning", "Header unavailable for this image.");
  }
}

function updateInspectorHeaderVisibility(file) {
  const target = file || "";
  const showInspector = Boolean(target && isHdf5File(target));
  const showHeader = Boolean(target && isHeaderCapableFile(target));
  if (inspectorSection) inspectorSection.classList.toggle("is-hidden", !showInspector);
  if (imageHeaderSection) imageHeaderSection.classList.toggle("is-hidden", !showHeader);
  if (showInspector) {
    clearImageHeader();
    if (!inspectorTree || !inspectorTree.children.length) {
      setInspectorMessage("Select an HDF5 file to browse metadata.");
    }
  } else {
    if (inspectorSection) setInspectorMessage("File inspector is available for HDF5 files only.");
    if (showHeader) {
      loadImageHeader(target);
    } else {
      clearImageHeader();
    }
  }
}

function clearInspectorSearch() {
  if (inspectorSearchInput) inspectorSearchInput.value = "";
  if (inspectorResults) {
    inspectorResults.innerHTML = "";
    inspectorResults.classList.add("is-hidden");
  }
}

function renderInspectorResults(results, query) {
  if (!inspectorResults) return;
  if (!query) {
    inspectorResults.innerHTML = "";
    inspectorResults.classList.add("is-hidden");
    setSectionBadgeState(inspectorStateEl, "active", "Metadata browser ready.");
    return;
  }
  inspectorResults.classList.remove("is-hidden");
  if (!Array.isArray(results) || results.length === 0) {
    inspectorResults.innerHTML = `<div class="inspector-empty">No matches.</div>`;
    setSectionBadgeState(inspectorStateEl, "empty", `No matches for "${query}".`);
    return;
  }
  setSectionBadgeState(
    inspectorStateEl,
    "active",
    `Found ${results.length} match${results.length === 1 ? "" : "es"} for "${query}".`
  );
  inspectorResults.innerHTML = "";
  results.forEach((item) => {
    const row = document.createElement("div");
    row.className = "inspector-result";
    row.dataset.path = item.path || "";
    row.dataset.type = item.type || "";
    row.tabIndex = 0;
    row.setAttribute("role", "button");
    if (item.type === "link" && item.target) {
      row.dataset.target = item.target;
    }
    const name = document.createElement("span");
    name.className = "inspector-result-name";
    name.textContent = item.path || item.name || "";
    const meta = document.createElement("span");
    meta.className = "inspector-result-meta";
    let metaText = "";
    if (item.type === "dataset" && item.shape && item.dtype) {
      metaText = `${item.shape.join("×")} ${item.dtype}`;
    } else if (item.type === "link" && item.target) {
      metaText = item.target;
    } else {
      metaText = item.type || "";
    }
    meta.textContent = metaText;
    const resultName = String(name.textContent || "").trim();
    row.setAttribute("aria-label", metaText ? `${resultName}, ${metaText}` : resultName);
    row.appendChild(name);
    row.appendChild(meta);
    inspectorResults.appendChild(row);
  });
}

async function runInspectorSearch(query) {
  const requestId = ++inspectorSearchRequestId;
  if (!isHdf5File(state.file)) {
    setSectionBadgeState(inspectorStateEl, "empty", "File inspector is available for HDF5 files only.");
    if (inspectorResults) {
      inspectorResults.innerHTML = "";
      inspectorResults.classList.add("is-hidden");
    }
    return;
  }
  if (!query) {
    renderInspectorResults([], "");
    return;
  }
  setSectionBadgeState(inspectorStateEl, "loading", `Searching metadata for "${query}"…`);
  try {
    const data = await fetchJSON(
      `${API}/hdf5/search?file=${encodeURIComponent(state.file)}&query=${encodeURIComponent(query)}`
    );
    if (requestId !== inspectorSearchRequestId) return;
    renderInspectorResults(data.matches || [], query);
  } catch (err) {
    if (requestId !== inspectorSearchRequestId) return;
    console.error(err);
    setSectionBadgeState(inspectorStateEl, "warning", "Search failed. Please try again.");
    if (inspectorResults) {
      inspectorResults.classList.remove("is-hidden");
      inspectorResults.innerHTML = `<div class="inspector-empty">Search failed.</div>`;
    }
  }
}

function renderInspectorLink(path, target) {
  if (inspectorPath) inspectorPath.textContent = path || "-";
  if (inspectorType) inspectorType.textContent = "link";
  if (inspectorShape) inspectorShape.textContent = "-";
  if (inspectorDtype) inspectorDtype.textContent = "-";
  if (inspectorAttrs) {
    inspectorAttrs.innerHTML = "";
    const attrRow = document.createElement("div");
    attrRow.className = "inspector-attr-row";
    const name = document.createElement("span");
    name.textContent = "Target";
    const value = document.createElement("span");
    value.textContent = target || "-";
    attrRow.appendChild(name);
    attrRow.appendChild(value);
    inspectorAttrs.appendChild(attrRow);
  }
  if (inspectorPreview) inspectorPreview.innerHTML = "";
  setSectionBadgeState(inspectorStateEl, "active", "Link target loaded.");
}

function formatInspectorCell(value) {
  if (value === null || value === undefined) return "-";
  if (typeof value === "number") {
    return Number.isFinite(value) ? String(value) : "-";
  }
  return String(value);
}

function buildInspectorTable1D(values) {
  const table = document.createElement("table");
  table.className = "inspector-table";
  const head = document.createElement("thead");
  const headRow = document.createElement("tr");
  ["Index", "Value"].forEach((label) => {
    const th = document.createElement("th");
    th.textContent = label;
    headRow.appendChild(th);
  });
  head.appendChild(headRow);
  table.appendChild(head);
  const body = document.createElement("tbody");
  values.forEach((value, idx) => {
    const row = document.createElement("tr");
    const indexCell = document.createElement("td");
    indexCell.textContent = String(idx);
    const valueCell = document.createElement("td");
    valueCell.textContent = formatInspectorCell(value);
    row.appendChild(indexCell);
    row.appendChild(valueCell);
    body.appendChild(row);
  });
  table.appendChild(body);
  return table;
}

function buildInspectorTable2D(values) {
  const table = document.createElement("table");
  table.className = "inspector-table";
  const head = document.createElement("thead");
  const headRow = document.createElement("tr");
  const corner = document.createElement("th");
  corner.textContent = "";
  headRow.appendChild(corner);
  const cols = values.length ? values[0].length : 0;
  for (let c = 0; c < cols; c += 1) {
    const th = document.createElement("th");
    th.textContent = String(c);
    headRow.appendChild(th);
  }
  head.appendChild(headRow);
  table.appendChild(head);
  const body = document.createElement("tbody");
  values.forEach((rowValues, r) => {
    const row = document.createElement("tr");
    const indexCell = document.createElement("td");
    indexCell.textContent = String(r);
    row.appendChild(indexCell);
    rowValues.forEach((value) => {
      const cell = document.createElement("td");
      cell.textContent = formatInspectorCell(value);
      row.appendChild(cell);
    });
    body.appendChild(row);
  });
  table.appendChild(body);
  return table;
}

function renderInspectorPreview(data) {
  if (!inspectorPreview) return;
  inspectorPreview.innerHTML = "";
  if (!data || data.preview === null || data.preview === undefined) {
    return;
  }
  const actions = document.createElement("div");
  actions.className = "inspector-preview-actions";
  const link = document.createElement("a");
  link.href = `${API}/hdf5/value?file=${encodeURIComponent(state.file)}&path=${encodeURIComponent(
    data.path || ""
  )}&max_cells=65536`;
  link.target = "_blank";
  link.rel = "noopener";
  link.textContent = "Open in new tab";
  actions.appendChild(link);
  if (Array.isArray(data.shape) && data.shape.length > 0) {
    const csvLink = document.createElement("a");
    csvLink.href = `${API}/hdf5/csv?file=${encodeURIComponent(state.file)}&path=${encodeURIComponent(
      data.path || ""
    )}&max_cells=65536`;
    csvLink.textContent = "Download CSV";
    actions.appendChild(csvLink);
  }
  inspectorPreview.appendChild(actions);

  const preview = data.preview;
  if (Array.isArray(preview)) {
    const table = Array.isArray(preview[0]) ? buildInspectorTable2D(preview) : buildInspectorTable1D(preview);
    inspectorPreview.appendChild(table);
  } else {
    const text = document.createElement("div");
    text.textContent = formatInspectorValue(preview);
    inspectorPreview.appendChild(text);
  }

  if (data.preview_shape) {
    const note = document.createElement("div");
    note.className = "inspector-preview-note";
    const shapeText = data.preview_shape.join("×");
    note.textContent = `Preview ${shapeText}${data.truncated ? " (truncated)" : ""}`;
    if (data.slice && Array.isArray(data.slice.lead) && data.slice.lead.length) {
      note.textContent += ` • Slice [${data.slice.lead.join(", ")}]`;
    }
    inspectorPreview.appendChild(note);
  }
}

function buildInspectorRow(node) {
  const li = document.createElement("li");
  li.className = "inspector-node";
  li.dataset.path = node.path || "";
  li.dataset.type = node.type || "";
  if (node.type === "link" && node.target) {
    li.dataset.target = node.target;
  }
  const row = document.createElement("div");
  row.className = "inspector-row";

  const toggle = document.createElement("button");
  toggle.className = "inspector-toggle";
  if (node.type === "group" && node.hasChildren) {
    toggle.textContent = "▸";
  } else {
    toggle.textContent = "";
    toggle.classList.add("is-hidden");
  }

  const name = document.createElement("span");
  name.textContent = node.name || node.path || "/";

  const meta = document.createElement("span");
  meta.className = "inspector-meta";
  if (node.type === "dataset" && node.shape && node.dtype) {
    meta.textContent = `${node.shape.join("×")} ${node.dtype}`;
  } else if (node.type === "link" && node.target) {
    meta.textContent = node.target;
  } else if (node.type === "group") {
    meta.textContent = "Group";
  } else {
    meta.textContent = node.type || "";
  }

  row.appendChild(toggle);
  row.appendChild(name);
  row.appendChild(meta);
  li.appendChild(row);

  if (node.type === "group") {
    const children = document.createElement("ul");
    children.className = "inspector-children";
    li.appendChild(children);
  }

  return li;
}

function renderInspectorTree(nodes, container) {
  if (!container) return;
  container.innerHTML = "";
  const target =
    container.classList.contains("inspector-children") ? container : document.createElement("ul");
  if (!container.classList.contains("inspector-children")) {
    target.className = "inspector-node";
  }
  nodes.forEach((node) => {
    target.appendChild(buildInspectorRow(node));
  });
  if (target !== container) {
    container.appendChild(target);
  }
}

async function fetchInspectorTree(path = "/") {
  const res = await fetchJSON(
    `${API}/hdf5/tree?file=${encodeURIComponent(state.file)}&path=${encodeURIComponent(path)}`
  );
  return res.children || [];
}

async function loadInspectorRoot() {
  if (!inspectorTree) return;
  clearInspectorSearch();
  if (!isHdf5File(state.file)) {
    setInspectorMessage("File inspector is available for HDF5 files only.");
    return;
  }
  setSectionBadgeState(inspectorStateEl, "loading", "Loading metadata tree…");
  renderSkeletonBlock(inspectorTree, 7);
  resetInspectorDetails();
  try {
    const children = await fetchInspectorTree("/");
    renderInspectorTree(children, inspectorTree);
    inspectorSelectedRow = null;
    resetInspectorDetails();
    if (children.length) {
      setSectionBadgeState(inspectorStateEl, "active", "Metadata tree loaded.");
    } else {
      setSectionBadgeState(inspectorStateEl, "empty", "No metadata nodes found.");
    }
  } catch (err) {
    console.error(err);
    setInspectorMessage("Failed to load HDF5 tree.");
  }
}

function selectInspectorRow(row) {
  if (inspectorSelectedRow) {
    inspectorSelectedRow.classList.remove("is-selected");
  }
  inspectorSelectedRow = row;
  if (inspectorSelectedRow) {
    inspectorSelectedRow.classList.add("is-selected");
  }
}

async function showInspectorNode(path) {
  if (!inspectorDetails) return;
  setSectionBadgeState(inspectorStateEl, "loading", "Loading node details…");
  try {
    const data = await fetchJSON(
      `${API}/hdf5/node?file=${encodeURIComponent(state.file)}&path=${encodeURIComponent(path)}`
    );
    if (inspectorPath) inspectorPath.textContent = data.path || path;
    if (inspectorType) inspectorType.textContent = data.type || "-";
    if (inspectorShape) inspectorShape.textContent = data.shape ? data.shape.join("×") : "-";
    if (inspectorDtype) inspectorDtype.textContent = data.dtype || "-";
    if (inspectorAttrs) {
      inspectorAttrs.innerHTML = "";
      if (Array.isArray(data.attrs) && data.attrs.length) {
        data.attrs.forEach((attr) => {
          const row = document.createElement("div");
          row.className = "inspector-attr-row";
          const name = document.createElement("span");
          name.textContent = attr.name;
          const val = document.createElement("span");
          val.textContent = formatInspectorValue(attr.value);
          row.appendChild(name);
          row.appendChild(val);
          inspectorAttrs.appendChild(row);
        });
      }
    }
    if (data.type === "dataset") {
      try {
        const valueData = await fetchJSON(
          `${API}/hdf5/value?file=${encodeURIComponent(state.file)}&path=${encodeURIComponent(path)}`
        );
        renderInspectorPreview(valueData);
      } catch (err) {
        console.error(err);
        if (inspectorPreview) inspectorPreview.innerHTML = "";
      }
    } else if (inspectorPreview) {
      inspectorPreview.innerHTML = "";
    }
    setSectionBadgeState(inspectorStateEl, "active", `Loaded ${data.type || "node"} details.`);
  } catch (err) {
    console.error(err);
    setInspectorMessage("Failed to load node details.");
  }
}

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

function getImagePointFromEvent(event) {
  if (!state.hasFrame || !canvasWrap) return null;
  const rect = canvasWrap.getBoundingClientRect();
  const x = event.clientX - rect.left;
  const y = event.clientY - rect.top;
  if (x < 0 || y < 0 || x > rect.width || y > rect.height) return null;
  const zoom = state.zoom || 1;
  const offsetX = state.renderOffsetX || 0;
  const offsetY = state.renderOffsetY || 0;
  const imgX = (getEffectiveScrollLeft() + x - offsetX) / zoom;
  const imgY = (getEffectiveScrollTop() + y - offsetY) / zoom;
  const ix = Math.max(0, Math.min(state.width - 1, Math.round(imgX)));
  const iy = Math.max(0, Math.min(state.height - 1, Math.round(imgY)));
  return { x: ix, y: iy };
}

function normalizeMaskData(data) {
  if (!data) return null;
  if (data instanceof Uint32Array) return data;
  const out = new Uint32Array(data.length);
  for (let i = 0; i < data.length; i += 1) {
    out[i] = data[i];
  }
  return out;
}

function buildNegativeMask(data) {
  if (!data || !data.length) return null;
  let hasMask = false;
  const mask = new Uint32Array(data.length);
  for (let i = 0; i < data.length; i += 1) {
    const value = data[i];
    if (!Number.isFinite(value)) continue;
    if (value < 0) {
      hasMask = true;
      mask[i] = value === -1 ? 1 : 0x1e;
    }
  }
  return hasMask ? mask : null;
}

function alignMaskToFrame() {
  if (
    !state.maskRaw ||
    !Array.isArray(state.maskShape) ||
    state.maskShape.length !== 2 ||
    !state.width ||
    !state.height
  ) {
    return;
  }
  const [maskH, maskW] = state.maskShape;
  if (maskH === state.height && maskW === state.width) {
    return;
  }
  if (maskH === state.width && maskW === state.height) {
    const transposed = new Uint32Array(state.width * state.height);
    for (let y = 0; y < state.height; y += 1) {
      for (let x = 0; x < state.width; x += 1) {
        transposed[y * state.width + x] = state.maskRaw[x * state.height + y];
      }
    }
    state.maskRaw = transposed;
    state.maskShape = [state.height, state.width];
  }
}

function updateMaskUI() {
  if (maskToggle) {
    maskToggle.disabled = !state.maskAvailable;
    maskToggle.checked = Boolean(state.maskEnabled && state.maskAvailable);
  }
  if (maskSaturatedToggle) {
    const hasSatMax = Number.isFinite(getActiveSaturationMax());
    maskSaturatedToggle.disabled = !hasSatMax;
    maskSaturatedToggle.checked = Boolean(state.maskSaturatedEnabled && hasSatMax);
  }
}

function syncMaskAvailability(forceEnable = false) {
  const matches =
    state.maskRaw &&
    Array.isArray(state.maskShape) &&
    state.maskShape.length === 2 &&
    state.width &&
    state.height &&
    state.maskShape[0] === state.height &&
    state.maskShape[1] === state.width;
  state.maskAvailable = Boolean(matches);
  if (!state.maskAvailable) {
    state.maskEnabled = false;
  } else if (forceEnable || state.maskAuto) {
    state.maskEnabled = true;
  }
  updateMaskUI();
}

function clearMaskState() {
  state.maskRaw = null;
  state.maskShape = null;
  state.maskAvailable = false;
  state.maskEnabled = false;
  state.maskAuto = true;
  state.maskFile = "";
  state.maskPath = "";
  updateMaskUI();
}

async function loadMask(forceEnable = false) {
  if (!state.file || !isHdfFile(state.file)) {
    clearMaskState();
    return;
  }
  const maskKey =
    state.thresholdCount > 1 ? `${state.file}#${state.thresholdIndex}` : state.file;
  if (state.maskFile === maskKey && state.maskRaw) {
    syncMaskAvailability(forceEnable);
    return;
  }
  state.maskFile = maskKey;
  state.maskRaw = null;
  state.maskShape = null;
  state.maskAvailable = false;
  if (forceEnable) {
    state.maskEnabled = true;
  }
  updateMaskUI();
  try {
    const thresholdParam =
      state.thresholdCount > 1 ? `&threshold=${state.thresholdIndex}` : "";
    const res = await fetch(`${API}/mask?file=${encodeURIComponent(state.file)}${thresholdParam}`);
    if (!res.ok) {
      state.maskEnabled = false;
      updateMaskUI();
      return;
    }
    const buffer = await res.arrayBuffer();
    const dtype = parseDtype(res.headers.get("X-Dtype"));
    const shape = parseShape(res.headers.get("X-Shape"));
    const data = typedArrayFrom(buffer, dtype);
    state.maskRaw = normalizeMaskData(data);
    state.maskShape = shape;
    state.maskPath = res.headers.get("X-Mask-Path") || "";
    alignMaskToFrame();
    syncMaskAvailability(forceEnable);
    if (state.hasFrame) {
      updateGlobalStats();
      redraw();
      scheduleRoiUpdate();
    }
  } catch (err) {
    console.error(err);
    state.maskEnabled = false;
    state.maskAvailable = false;
    updateMaskUI();
  }
}

function snapHistogramValue(value) {
  if (!Number.isFinite(value)) return value;
  const info = getDtypeInfo(state.dtype);
  if (info && (info.kind === "u" || info.kind === "i")) {
    return Math.round(value);
  }
  return value;
}

function showHistTooltip(text, x, y) {
  if (!histTooltip) return;
  histTooltip.textContent = text;
  histTooltip.style.left = `${Math.round(x)}px`;
  histTooltip.style.top = `${Math.round(y)}px`;
  histTooltip.classList.add("is-visible");
  histTooltip.setAttribute("aria-hidden", "false");
}

function hideHistTooltip() {
  if (!histTooltip) return;
  histTooltip.classList.remove("is-visible");
  histTooltip.setAttribute("aria-hidden", "true");
}

function showCursorOverlay(text, clientX, clientY) {
  if (!cursorOverlay || !canvasShell) return;
  cursorOverlay.textContent = text;
  cursorOverlay.classList.add("is-visible");
  cursorOverlay.setAttribute("aria-hidden", "false");
  const shellRect = canvasShell.getBoundingClientRect();
  let left = clientX - shellRect.left + 12;
  let top = clientY - shellRect.top + 12;
  const maxLeft = shellRect.width - cursorOverlay.offsetWidth - 6;
  const maxTop = shellRect.height - cursorOverlay.offsetHeight - 6;
  left = Math.min(maxLeft, Math.max(6, left));
  top = Math.min(maxTop, Math.max(6, top));
  cursorOverlay.style.left = `${left}px`;
  cursorOverlay.style.top = `${top}px`;
}

function hideCursorOverlay() {
  if (!cursorOverlay) return;
  cursorOverlay.classList.remove("is-visible");
  cursorOverlay.setAttribute("aria-hidden", "true");
}

function updateCursorOverlay(event) {
  if (!state.hasFrame || !state.dataRaw || !state.width || !state.height) {
    hideCursorOverlay();
    return;
  }
  if (!canvasWrap || !canvasShell) {
    hideCursorOverlay();
    return;
  }
  const rect = canvasWrap.getBoundingClientRect();
  const x = event.clientX - rect.left;
  const y = event.clientY - rect.top;
  if (x < 0 || y < 0 || x > rect.width || y > rect.height) {
    hideCursorOverlay();
    return;
  }
  const zoom = state.zoom || 1;
  const offsetX = state.renderOffsetX || 0;
  const offsetY = state.renderOffsetY || 0;
  const imgX = (getEffectiveScrollLeft() + x - offsetX) / zoom;
  const imgY = (getEffectiveScrollTop() + y - offsetY) / zoom;
  const ix = Math.floor(imgX);
  const iy = Math.floor(imgY);
  if (ix < 0 || iy < 0 || ix >= state.width || iy >= state.height) {
    hideCursorOverlay();
    return;
  }
  const idx = iy * state.width + ix;
  let labelValue = formatValue(state.dataRaw[idx]);
  const satMax = getActiveSaturationMax();
  if (
    state.maskEnabled &&
    state.maskAvailable &&
    state.maskRaw &&
    state.maskShape &&
    state.maskShape[0] === state.height &&
    state.maskShape[1] === state.width
  ) {
    const maskValue = state.maskRaw[idx];
    if (maskValue & 1) {
      labelValue = "G";
    } else if (maskValue & 0x1e) {
      labelValue = "D";
    }
  }
  if (state.maskSaturatedEnabled && labelValue !== "G" && labelValue !== "D" && isSaturatedValue(state.dataRaw[idx], satMax)) {
    labelValue = "S";
  }
  const resolutionValue = getResolutionAtPixel(ix, iy);
  const resolutionText = Number.isFinite(resolutionValue) ? `  d ${resolutionValue.toFixed(1)} Å` : "";
  const label = `X ${ix}  Y ${iy}  Value ${labelValue}${resolutionText}`;
  showCursorOverlay(label, event.clientX, event.clientY);
}

function scheduleRoiOverlay() {
  if (roiOverlayScheduled) return;
  roiOverlayScheduled = true;
  window.requestAnimationFrame(() => {
    roiOverlayScheduled = false;
    drawRoiOverlay();
  });
}

function drawRoiOverlay() {
  if (!roiOverlay || !roiCtx || !canvasWrap) return;
  const metrics = syncOverlayCanvas(roiOverlay, roiCtx);
  if (!metrics) return;
  const { width, height } = metrics;
  roiCtx.clearRect(0, 0, width, height);
  if (!roiState.enabled || !roiState.active || !roiState.start || !roiState.end) return;
  const zoom = state.zoom || 1;
  const offsetX = state.renderOffsetX || 0;
  const offsetY = state.renderOffsetY || 0;
  const viewX = getEffectiveScrollLeft() / zoom;
  const viewY = getEffectiveScrollTop() / zoom;
  const x0 = (roiState.start.x - viewX) * zoom + offsetX;
  const y0 = (roiState.start.y - viewY) * zoom + offsetY;
  const x1 = (roiState.end.x - viewX) * zoom + offsetX;
  const y1 = (roiState.end.y - viewY) * zoom + offsetY;

  roiCtx.save();
  roiCtx.setLineDash([6, 4]);
  roiCtx.lineJoin = "round";
  roiCtx.lineCap = "round";
  const strokeWithHalo = () => {
    roiCtx.lineWidth = 4;
    roiCtx.strokeStyle = "rgba(0, 0, 0, 0.7)";
    roiCtx.stroke();
    roiCtx.lineWidth = 2;
    roiCtx.strokeStyle = "rgba(255, 255, 255, 0.95)";
    roiCtx.stroke();
  };
  if (roiState.mode === "line") {
    roiCtx.beginPath();
    roiCtx.moveTo(x0, y0);
    roiCtx.lineTo(x1, y1);
    strokeWithHalo();
  } else if (roiState.mode === "box") {
    const left = Math.min(x0, x1);
    const top = Math.min(y0, y1);
    const w = Math.abs(x1 - x0);
    const h = Math.abs(y1 - y0);
    if (w > 0 && h > 0) {
      roiCtx.save();
      roiCtx.setLineDash([]);
      roiCtx.fillStyle = "rgba(160, 160, 160, 0.08)";
      roiCtx.fillRect(left, top, w, h);
      roiCtx.restore();
    }
    roiCtx.beginPath();
    roiCtx.rect(left, top, w, h);
    strokeWithHalo();
  } else if (roiState.mode === "circle" || roiState.mode === "annulus") {
    const radius = Math.hypot(x1 - x0, y1 - y0);
    if (radius > 0) {
      roiCtx.save();
      roiCtx.setLineDash([]);
      roiCtx.fillStyle = "rgba(160, 160, 160, 0.08)";
      roiCtx.beginPath();
      roiCtx.arc(x0, y0, radius, 0, Math.PI * 2);
      if (roiState.mode === "annulus" && roiState.innerRadius > 0) {
        const inner = roiState.innerRadius * zoom;
        roiCtx.moveTo(x0 + inner, y0);
        roiCtx.arc(x0, y0, inner, 0, Math.PI * 2);
        try {
          roiCtx.fill("evenodd");
        } catch {
          roiCtx.fill();
        }
      } else {
        roiCtx.fill();
      }
      roiCtx.restore();
    }
    roiCtx.beginPath();
    roiCtx.arc(x0, y0, radius, 0, Math.PI * 2);
    strokeWithHalo();
    if (roiState.mode === "annulus" && roiState.innerRadius > 0) {
      roiCtx.beginPath();
      roiCtx.arc(x0, y0, roiState.innerRadius * zoom, 0, Math.PI * 2);
      strokeWithHalo();
    }
  }
  roiCtx.restore();

  drawRoiHandles(roiCtx, x0, y0, x1, y1, zoom);
}

function drawRoiHandles(ctx, x0, y0, x1, y1, zoom) {
  const handleSize = 8;
  const half = handleSize / 2;
  ctx.save();
  ctx.lineWidth = 1;
  ctx.setLineDash([]);
  ctx.fillStyle = "rgba(255, 255, 255, 0.95)";
  ctx.strokeStyle = "rgba(0, 0, 0, 0.8)";

  const drawHandle = (x, y) => {
    ctx.fillRect(x - half, y - half, handleSize, handleSize);
    ctx.strokeRect(x - half, y - half, handleSize, handleSize);
  };

  const drawCross = (x, y) => {
    ctx.strokeStyle = "rgba(255, 255, 255, 0.95)";
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    ctx.moveTo(x - 6, y);
    ctx.lineTo(x + 6, y);
    ctx.moveTo(x, y - 6);
    ctx.lineTo(x, y + 6);
    ctx.stroke();
    ctx.strokeStyle = "rgba(0, 0, 0, 0.8)";
  };

  if (roiState.mode === "line") {
    drawHandle(x0, y0);
    drawHandle(x1, y1);
  } else if (roiState.mode === "box") {
    drawHandle(x0, y0);
    drawHandle(x1, y0);
    drawHandle(x1, y1);
    drawHandle(x0, y1);
  } else if (roiState.mode === "circle" || roiState.mode === "annulus") {
    drawCross(x0, y0);
    const dx = x1 - x0;
    const dy = y1 - y0;
    const outer = Math.hypot(dx, dy);
    const ux = outer > 0 ? dx / outer : 1;
    const uy = outer > 0 ? dy / outer : 0;
    drawHandle(x1, y1);
    if (roiState.mode === "annulus" && roiState.innerRadius > 0) {
      drawHandle(x0 + roiState.innerRadius * zoom * ux, y0 + roiState.innerRadius * zoom * uy);
    }
  }
  ctx.restore();
}

function getDefaultCenter() {
  if (Array.isArray(state.shape) && state.shape.length >= 2) {
    const width = state.shape[state.shape.length - 1];
    const height = state.shape[state.shape.length - 2];
    if (Number.isFinite(width) && Number.isFinite(height)) {
      return { x: width / 2, y: height / 2 };
    }
  }
  if (state.width && state.height) {
    return { x: state.width / 2, y: state.height / 2 };
  }
  return { x: 0, y: 0 };
}

function getRingParams() {
  const distanceMm = Number(ringsDistance?.value || analysisState.distanceMm);
  const pixelSizeUm = Number(ringsPixel?.value || analysisState.pixelSizeUm);
  const energyEv = Number(ringsEnergy?.value || analysisState.energyEv);
  const centerX = Number(ringsCenterX?.value);
  const centerY = Number(ringsCenterY?.value);
  const centerKnown = Number.isFinite(centerX) || Number.isFinite(centerY) || Number.isFinite(analysisState.centerX) || Number.isFinite(analysisState.centerY);
  const center = {
    x: Number.isFinite(centerX) ? centerX : analysisState.centerX,
    y: Number.isFinite(centerY) ? centerY : analysisState.centerY,
  };
  const fallback = getDefaultCenter();
  if (!Number.isFinite(center.x)) center.x = fallback.x;
  if (!Number.isFinite(center.y)) center.y = fallback.y;
  const maxCount = Math.max(1, ringInputs.length);
  const ringLimit = Math.max(1, Math.min(maxCount, DEFAULT_RING_COUNT));
  const rings = ringInputs
    .map((input, index) => {
      const value = Number(input?.value || analysisState.rings[index]);
      return Number.isFinite(value) && value > 0 ? value : null;
    })
    .filter((value) => value !== null)
    .slice(0, ringLimit);
  return {
    distanceMm: Number.isFinite(distanceMm) ? distanceMm : null,
    pixelSizeUm: Number.isFinite(pixelSizeUm) ? pixelSizeUm : null,
    energyEv: Number.isFinite(energyEv) ? energyEv : null,
    centerX: center.x,
    centerY: center.y,
    centerKnown,
    rings,
  };
}

function getResolutionAtPixel(ix, iy, params = getRingParams()) {
  if (!Number.isFinite(ix) || !Number.isFinite(iy)) return null;
  if (!params || !params.distanceMm || !params.pixelSizeUm || !params.energyEv) return null;
  const lambda = 12398.4193 / params.energyEv;
  if (!Number.isFinite(lambda) || lambda <= 0) return null;
  const pixelSizeMm = params.pixelSizeUm / 1000;
  if (!Number.isFinite(pixelSizeMm) || pixelSizeMm <= 0) return null;

  const dxPx = ix - params.centerX;
  const dyPx = iy - params.centerY;
  const radiusPx = Math.hypot(dxPx, dyPx);
  const radiusMm = radiusPx * pixelSizeMm;
  const twoTheta = Math.atan2(radiusMm, params.distanceMm);
  const sinArg = Math.sin(twoTheta / 2);
  if (!Number.isFinite(sinArg) || sinArg <= 0) return null;
  const d = lambda / (2 * sinArg);
  return Number.isFinite(d) && d > 0 ? d : null;
}

function getRoiModeLabel(mode) {
  if (mode === "line") return "Line ROI";
  if (mode === "box") return "Box ROI";
  if (mode === "circle") return "Circle ROI";
  if (mode === "annulus") return "Annulus ROI";
  return "ROI";
}

function updateRoiSectionState() {
  if (!roiSectionStateEl) return;
  if (!state.hasFrame) {
    setSectionBadgeState(roiSectionStateEl, "empty", "Load a frame to use ROI tools.");
    setSummaryChip(roiSummaryEl, "No frame");
    return;
  }
  if (!roiState.enabled) {
    setSectionBadgeState(roiSectionStateEl, "empty", "ROI overlay disabled. Enable it to define and edit regions.");
    setSummaryChip(roiSummaryEl, "Off");
    return;
  }
  const modeLabel = getRoiModeLabel(roiState.mode);
  if (!roiState.start || !roiState.end) {
    setSectionBadgeState(
      roiSectionStateEl,
      "empty",
      `${modeLabel} ready. Right-drag on the image to define the region.`
    );
    setSummaryChip(roiSummaryEl, `${modeLabel} ready`);
    return;
  }
  setSectionBadgeState(roiSectionStateEl, "active", `${modeLabel} active.`);
  setSummaryChip(roiSummaryEl, `${modeLabel} active`, "active");
}

function updateRingsSectionState() {
  if (!ringsSectionStateEl) return;
  if (!analysisState.ringsEnabled) {
    setSectionBadgeState(ringsSectionStateEl, "empty", "Enable rings to show resolution guides.");
    setSummaryChip(ringsSummaryEl, "Off");
    return;
  }
  if (!state.hasFrame) {
    setSectionBadgeState(ringsSectionStateEl, "loading", "Load a frame to render rings.");
    setSummaryChip(ringsSummaryEl, "Waiting frame");
    return;
  }
  const params = getRingParams();
  if (!params.distanceMm || !params.pixelSizeUm || !params.energyEv) {
    setSectionBadgeState(
      ringsSectionStateEl,
      "empty",
      "Provide detector distance, pixel size, and photon energy to compute rings."
    );
    setSummaryChip(ringsSummaryEl, "Missing geometry", "warning");
    return;
  }
  if (!params.rings.length) {
    setSectionBadgeState(ringsSectionStateEl, "empty", "Add at least one ring value.");
    setSummaryChip(ringsSummaryEl, "No rings", "warning");
    return;
  }
  setSectionBadgeState(
    ringsSectionStateEl,
    "active",
    `Showing ${params.rings.length} ring${params.rings.length === 1 ? "" : "s"}.`
  );
  setSummaryChip(ringsSummaryEl, `${params.rings.length} ring${params.rings.length === 1 ? "" : "s"}`, "active");
}

function updatePeaksSectionState() {
  if (!peaksSectionStateEl) return;
  if (!analysisState.peaksEnabled) {
    setSectionBadgeState(peaksSectionStateEl, "empty", "Enable Peak Finder to detect peaks.");
    setSummaryChip(peaksSummaryEl, "Off");
    return;
  }
  if (!state.hasFrame) {
    setSectionBadgeState(peaksSectionStateEl, "loading", "Load a frame to detect peaks.");
    setSummaryChip(peaksSummaryEl, "Waiting frame");
    return;
  }
  if (state.playing || state.isLoading || peakFinderScheduled) {
    setSectionBadgeState(peaksSectionStateEl, "active", "Peak Finder active. Updating in the background.");
    setSummaryChip(peaksSummaryEl, "Active", "active");
    return;
  }
  if (!analysisState.peaks.length) {
    setSectionBadgeState(peaksSectionStateEl, "empty", "No peaks detected on this frame.");
    setSummaryChip(peaksSummaryEl, "No peaks");
    return;
  }
  setSectionBadgeState(
    peaksSectionStateEl,
    "active",
    `Detected ${analysisState.peaks.length} peak${analysisState.peaks.length === 1 ? "" : "s"}.`
  );
  setSummaryChip(
    peaksSummaryEl,
    `${analysisState.peaks.length} peak${analysisState.peaks.length === 1 ? "" : "s"}`,
    "active"
  );
}

function renderPeakList() {
  if (!peaksBody) return;
  const isBusy = analysisState.peaksEnabled && state.hasFrame && (state.isLoading || peakFinderScheduled);
  if (isBusy && peaksBody.childElementCount > 0) {
    updatePeaksSectionState();
    return;
  }
  peaksBody.innerHTML = "";
  if (isBusy) {
    peaksBody.appendChild(buildSkeletonList(6));
    updatePeaksSectionState();
    return;
  }
  if (!analysisState.peaksEnabled) {
    const empty = document.createElement("div");
    empty.className = "peaks-empty";
    empty.textContent = "Enable \"Find peaks\" to detect diffraction peaks.";
    peaksBody.appendChild(empty);
    updatePeaksSectionState();
    return;
  }
  if (!analysisState.peaks.length) {
    const empty = document.createElement("div");
    empty.className = "peaks-empty";
    empty.textContent = state.hasFrame ? "No peaks detected." : "Load a frame to detect peaks.";
    peaksBody.appendChild(empty);
    updatePeaksSectionState();
    return;
  }
  analysisState.peaks.forEach((peak, idx) => {
    const row = document.createElement("button");
    row.type = "button";
    row.className = "peaks-row";
    const isSelected = analysisState.selectedPeaks.includes(idx);
    if (isSelected) {
      row.classList.add("is-selected");
    }
    row.innerHTML = `<span>${idx + 1}</span><span>${peak.x}</span><span>${peak.y}</span><span>${formatStat(peak.intensity)}</span>`;
    row.addEventListener("click", (event) => {
      const anchor = analysisState.peakSelectionAnchor;
      if (event.shiftKey && Number.isInteger(anchor) && anchor >= 0 && anchor < analysisState.peaks.length) {
        const start = Math.min(anchor, idx);
        const end = Math.max(anchor, idx);
        const range = [];
        for (let i = start; i <= end; i += 1) {
          range.push(i);
        }
        analysisState.selectedPeaks = range;
      } else if (event.metaKey || event.ctrlKey) {
        if (analysisState.selectedPeaks.includes(idx)) {
          analysisState.selectedPeaks = analysisState.selectedPeaks.filter((v) => v !== idx);
        } else {
          analysisState.selectedPeaks = [...analysisState.selectedPeaks, idx].sort((a, b) => a - b);
        }
        analysisState.peakSelectionAnchor = idx;
      } else if (isSelected && analysisState.selectedPeaks.length === 1) {
        analysisState.selectedPeaks = [];
        analysisState.peakSelectionAnchor = null;
      } else {
        analysisState.selectedPeaks = [idx];
        analysisState.peakSelectionAnchor = idx;
      }
      if (!Number.isInteger(analysisState.peakSelectionAnchor) && analysisState.selectedPeaks.length) {
        analysisState.peakSelectionAnchor = idx;
      }
      renderPeakList();
      schedulePeakOverlay();
    });
    peaksBody.appendChild(row);
  });
  updatePeaksSectionState();
}

function detectPeaks(maxPeaks) {
  if (!state.hasFrame || !state.dataRaw || !state.width || !state.height) return [];
  const width = state.width;
  const height = state.height;
  if (width < 3 || height < 3 || maxPeaks < 1) return [];

  const data = state.dataRaw;
  const maskReady =
    state.maskEnabled &&
    state.maskAvailable &&
    state.maskRaw &&
    state.maskShape &&
    state.maskShape[0] === height &&
    state.maskShape[1] === width;
  const mask = maskReady ? state.maskRaw : null;
  const satMaskEnabled = Boolean(state.maskSaturatedEnabled);
  const satMax = getActiveSaturationMax();

  const shouldIgnorePixel = (index) => {
    if (mask && (mask[index] & PEAK_BAD_MASK_BITS)) return true;
    if (satMaskEnabled && isSaturatedValue(data[index], satMax)) return true;
    return false;
  };

  const candidates = [];
  const candidateLimit = Math.min(4096, Math.max(128, maxPeaks * 24));
  let minCandidateValue = Number.POSITIVE_INFINITY;
  let minCandidateIndex = -1;

  function pushCandidate(x, y, value) {
    if (candidates.length < candidateLimit) {
      candidates.push({ x, y, intensity: value });
      if (value < minCandidateValue) {
        minCandidateValue = value;
        minCandidateIndex = candidates.length - 1;
      }
      return;
    }
    if (value <= minCandidateValue || minCandidateIndex < 0) return;
    candidates[minCandidateIndex] = { x, y, intensity: value };
    minCandidateValue = Number.POSITIVE_INFINITY;
    minCandidateIndex = -1;
    for (let i = 0; i < candidates.length; i += 1) {
      if (candidates[i].intensity < minCandidateValue) {
        minCandidateValue = candidates[i].intensity;
        minCandidateIndex = i;
      }
    }
  }

  for (let y = 1; y < height - 1; y += 1) {
    const row = y * width;
    for (let x = 1; x < width - 1; x += 1) {
      const idx = row + x;
      const v = data[idx];
      if (!Number.isFinite(v) || v <= 0) continue;
      if (shouldIgnorePixel(idx)) continue;

      let left = data[idx - 1];
      let right = data[idx + 1];
      let up = data[idx - width];
      let down = data[idx + width];
      if (shouldIgnorePixel(idx - 1)) left = Number.NEGATIVE_INFINITY;
      if (shouldIgnorePixel(idx + 1)) right = Number.NEGATIVE_INFINITY;
      if (shouldIgnorePixel(idx - width)) up = Number.NEGATIVE_INFINITY;
      if (shouldIgnorePixel(idx + width)) down = Number.NEGATIVE_INFINITY;
      if (!Number.isFinite(left)) left = Number.NEGATIVE_INFINITY;
      if (!Number.isFinite(right)) right = Number.NEGATIVE_INFINITY;
      if (!Number.isFinite(up)) up = Number.NEGATIVE_INFINITY;
      if (!Number.isFinite(down)) down = Number.NEGATIVE_INFINITY;

      if (!(v > left && v >= right && v > up && v >= down)) continue;
      pushCandidate(x, y, v);
    }
  }

  candidates.sort((a, b) => b.intensity - a.intensity);
  const selected = [];
  const minSeparation = Math.max(4, Math.round(Math.min(width, height) * 0.004));
  const minSeparationSq = minSeparation * minSeparation;
  for (let i = 0; i < candidates.length; i += 1) {
    const candidate = candidates[i];
    let tooClose = false;
    for (let j = 0; j < selected.length; j += 1) {
      const dx = candidate.x - selected[j].x;
      const dy = candidate.y - selected[j].y;
      if (dx * dx + dy * dy < minSeparationSq) {
        tooClose = true;
        break;
      }
    }
    if (!tooClose) {
      selected.push(candidate);
      if (selected.length >= maxPeaks) break;
    }
  }
  return selected;
}

function runPeakFinder() {
  peakFinderScheduled = false;
  if (!analysisState.peaksEnabled) {
    analysisState.peaks = [];
    analysisState.selectedPeaks = [];
    analysisState.peakSelectionAnchor = null;
    renderPeakList();
    schedulePeakOverlay();
    return;
  }
  const requested = Math.max(1, Math.min(1000, Math.round(Number(peaksCountInput?.value || analysisState.peakCount || 25))));
  analysisState.peakCount = requested;
  if (peaksCountInput) {
    peaksCountInput.value = String(requested);
    setFieldHint(peaksCountInput, peaksCountHint, "");
  }
  analysisState.peaks = detectPeaks(requested);
  analysisState.selectedPeaks = analysisState.selectedPeaks.filter(
    (idx) => Number.isInteger(idx) && idx >= 0 && idx < analysisState.peaks.length
  );
  if (
    !Number.isInteger(analysisState.peakSelectionAnchor) ||
    analysisState.peakSelectionAnchor < 0 ||
    analysisState.peakSelectionAnchor >= analysisState.peaks.length
  ) {
    analysisState.peakSelectionAnchor = analysisState.selectedPeaks.length
      ? analysisState.selectedPeaks[analysisState.selectedPeaks.length - 1]
      : null;
  }
  if (!analysisState.peaks.length) {
    analysisState.peakSelectionAnchor = null;
  }
  renderPeakList();
  schedulePeakOverlay();
}

function schedulePeakFinder() {
  if (peakFinderScheduled) return;
  peakFinderScheduled = true;
  updatePeaksSectionState();
  renderPeakList();
  window.setTimeout(runPeakFinder, 0);
}

function exportPeakCsv() {
  if (!analysisState.peaks.length) return;
  const rows = ["index,x,y,intensity"];
  analysisState.peaks.forEach((peak, idx) => {
    rows.push(`${idx + 1},${peak.x},${peak.y},${peak.intensity}`);
  });
  const blob = new Blob([rows.join("\n")], { type: "text/csv;charset=utf-8" });
  const link = document.createElement("a");
  const url = URL.createObjectURL(blob);
  const base = (state.file || "peaks").split("/").pop().replace(/\.[^.]+$/, "");
  const thresholdSuffix = state.thresholdCount > 1 ? `_thr${state.thresholdIndex + 1}` : "";
  link.href = url;
  link.download = `${base}_frame_${state.frameIndex + 1}${thresholdSuffix}_peaks.csv`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}

const seriesSumController = createSeriesSumController({
  apiBase: API,
  state,
  elements: {
    seriesSumMode,
    seriesSumOperation,
    seriesSumStepField,
    seriesSumStepLabel,
    seriesSumStep,
    seriesSumRangeStartField,
    seriesSumRangeEndField,
    seriesSumRangeStart,
    seriesSumRangeEnd,
    seriesSumNormalizeEnable,
    seriesSumNormalizeFrameField,
    seriesSumNormalizeFrame,
    seriesSumOutput,
    seriesSumBrowse,
    seriesSumFormat,
    seriesSumMask,
    seriesSumStart,
    seriesSumCancel,
    seriesSumProgress,
    seriesSumProgressFill,
    seriesSumProgressText,
  },
  callbacks: {
    isHdfFile,
    validateSeriesStepInput,
    setStatus,
    ensureFileMode,
    loadAutoloadFile,
    fetchJSON,
    fetchJSONWithInit,
  },
});

function setSeriesSumProgress(progress, text) {
  seriesSumController.setSeriesSumProgress(progress, text);
}

function updateSeriesSumUi() {
  seriesSumController.updateSeriesSumUi();
}

async function startSeriesSumming() {
  await seriesSumController.startSeriesSumming();
}

async function cancelSeriesSumming() {
  await seriesSumController.cancelSeriesSumming();
}

function getPointerCanvasPos(event) {
  if (!canvasWrap) return null;
  const rect = canvasWrap.getBoundingClientRect();
  return { x: event.clientX - rect.left, y: event.clientY - rect.top };
}

function getRoiScreenGeometry() {
  if (!canvasWrap || !roiState.start || !roiState.end) return null;
  const zoom = state.zoom || 1;
  const offsetX = state.renderOffsetX || 0;
  const offsetY = state.renderOffsetY || 0;
  const viewX = getEffectiveScrollLeft() / zoom;
  const viewY = getEffectiveScrollTop() / zoom;
  const x0 = (roiState.start.x - viewX) * zoom + offsetX;
  const y0 = (roiState.start.y - viewY) * zoom + offsetY;
  const x1 = (roiState.end.x - viewX) * zoom + offsetX;
  const y1 = (roiState.end.y - viewY) * zoom + offsetY;
  return { x0, y0, x1, y1, zoom };
}

function getRoiHandleAt(event) {
  if (!roiState.enabled || !roiState.active) return null;
  const pointer = getPointerCanvasPos(event);
  const geom = getRoiScreenGeometry();
  if (!pointer || !geom) return null;
  const { x0, y0, x1, y1, zoom } = geom;
  const hit = (x, y) => Math.abs(pointer.x - x) <= 6 && Math.abs(pointer.y - y) <= 6;

  if (roiState.mode === "line") {
    if (hit(x0, y0)) return "line-start";
    if (hit(x1, y1)) return "line-end";
    return null;
  }
  if (roiState.mode === "box") {
    if (hit(x0, y0)) return "box-nw";
    if (hit(x1, y0)) return "box-ne";
    if (hit(x1, y1)) return "box-se";
    if (hit(x0, y1)) return "box-sw";
    return null;
  }
  if (roiState.mode === "circle" || roiState.mode === "annulus") {
    if (hit(x0, y0)) return "center";
    const dx = x1 - x0;
    const dy = y1 - y0;
    const outer = Math.hypot(dx, dy);
    const ux = outer > 0 ? dx / outer : 1;
    const uy = outer > 0 ? dy / outer : 0;
    if (hit(x1, y1)) return "outer";
    if (roiState.mode === "annulus" && roiState.innerRadius > 0) {
      if (hit(x0 + roiState.innerRadius * zoom * ux, y0 + roiState.innerRadius * zoom * uy)) {
        return "inner";
      }
    }
  }
  return null;
}

function isPointInRoi(point) {
  if (!point || !roiState.start || !roiState.end) return false;
  const x0 = roiState.start.x;
  const y0 = roiState.start.y;
  const x1 = roiState.end.x;
  const y1 = roiState.end.y;
  if (roiState.mode === "box") {
    const left = Math.min(x0, x1);
    const right = Math.max(x0, x1);
    const top = Math.min(y0, y1);
    const bottom = Math.max(y0, y1);
    return point.x >= left && point.x <= right && point.y >= top && point.y <= bottom;
  }
  if (roiState.mode === "circle" || roiState.mode === "annulus") {
    const dx = point.x - x0;
    const dy = point.y - y0;
    const dist = Math.hypot(dx, dy);
    const outer = Math.hypot(x1 - x0, y1 - y0);
    return dist <= outer;
  }
  if (roiState.mode === "line") {
    const zoom = state.zoom || 1;
    const tol = 6 / zoom;
    const dist = pointToSegmentDistance(point, roiState.start, roiState.end);
    return dist <= tol;
  }
  return false;
}

function pointToSegmentDistance(p, a, b) {
  const vx = b.x - a.x;
  const vy = b.y - a.y;
  const wx = p.x - a.x;
  const wy = p.y - a.y;
  const c1 = vx * wx + vy * wy;
  if (c1 <= 0) return Math.hypot(wx, wy);
  const c2 = vx * vx + vy * vy;
  if (c2 <= c1) return Math.hypot(p.x - b.x, p.y - b.y);
  const t = c1 / c2;
  const projX = a.x + t * vx;
  const projY = a.y + t * vy;
  return Math.hypot(p.x - projX, p.y - projY);
}

function clampRoiDelta(dx, dy, baseStart = roiState.start, baseEnd = roiState.end, baseOuter = roiState.outerRadius) {
  if (!baseStart || !baseEnd || !state.width || !state.height) {
    return { dx, dy };
  }
  let minX = Math.min(baseStart.x, baseEnd.x);
  let maxX = Math.max(baseStart.x, baseEnd.x);
  let minY = Math.min(baseStart.y, baseEnd.y);
  let maxY = Math.max(baseStart.y, baseEnd.y);
  if (roiState.mode === "circle" || roiState.mode === "annulus") {
    const r =
      baseOuter ||
      Math.hypot(baseEnd.x - baseStart.x, baseEnd.y - baseStart.y);
    minX = baseStart.x - r;
    maxX = baseStart.x + r;
    minY = baseStart.y - r;
    maxY = baseStart.y + r;
  }
  if (minX + dx < 0) dx = -minX;
  if (maxX + dx > state.width - 1) dx = (state.width - 1) - maxX;
  if (minY + dy < 0) dy = -minY;
  if (maxY + dy > state.height - 1) dy = (state.height - 1) - maxY;
  return { dx, dy };
}

function startRoiEdit(handle, point) {
  if (!roiState.start || !roiState.end) return;
  roiEditing = true;
  roiEditHandle = handle || "move";
  roiEditStart = point;
  roiEditSnapshot = {
    start: { ...roiState.start },
    end: { ...roiState.end },
    innerRadius: roiState.innerRadius || 0,
    outerRadius: roiState.outerRadius || Math.hypot(roiState.end.x - roiState.start.x, roiState.end.y - roiState.start.y),
  };
  canvasWrap.classList.add("is-roi");
}

function applyRoiEdit(point) {
  if (!roiEditing || !roiEditSnapshot || !roiEditStart || !point) return;
  const snap = roiEditSnapshot;
  const dxRaw = point.x - roiEditStart.x;
  const dyRaw = point.y - roiEditStart.y;
  if (roiEditHandle === "move" || roiEditHandle === "center") {
    const clamped = clampRoiDelta(
      dxRaw,
      dyRaw,
      snap.start,
      snap.end,
      snap.outerRadius
    );
    const dx = clamped.dx;
    const dy = clamped.dy;
    roiState.start = { x: snap.start.x + dx, y: snap.start.y + dy };
    roiState.end = { x: snap.end.x + dx, y: snap.end.y + dy };
    roiState.innerRadius = snap.innerRadius;
    roiState.outerRadius = snap.outerRadius;
    updateRoiCenterInputs();
  } else if (roiState.mode === "box") {
    const anchor = snap;
    if (roiEditHandle === "box-nw") {
      roiState.start = { x: point.x, y: point.y };
      roiState.end = { ...anchor.end };
    } else if (roiEditHandle === "box-ne") {
      roiState.start = { x: anchor.start.x, y: point.y };
      roiState.end = { x: point.x, y: anchor.end.y };
    } else if (roiEditHandle === "box-se") {
      roiState.start = { ...anchor.start };
      roiState.end = { x: point.x, y: point.y };
    } else if (roiEditHandle === "box-sw") {
      roiState.start = { x: point.x, y: anchor.start.y };
      roiState.end = { x: anchor.end.x, y: point.y };
    }
  } else if (roiState.mode === "line") {
    if (roiEditHandle === "line-start") {
      roiState.start = { x: point.x, y: point.y };
      roiState.end = { ...snap.end };
    } else if (roiEditHandle === "line-end") {
      roiState.start = { ...snap.start };
      roiState.end = { x: point.x, y: point.y };
    }
  } else if (roiState.mode === "circle" || roiState.mode === "annulus") {
    if (roiEditHandle === "outer") {
      roiState.start = { ...snap.start };
      roiState.end = { x: point.x, y: point.y };
      const outer = Math.max(0, Math.round(Math.hypot(point.x - snap.start.x, point.y - snap.start.y)));
      roiState.outerRadius = outer;
      if (roiState.mode === "circle") {
        roiState.innerRadius = 0;
        if (roiRadiusInput) roiRadiusInput.value = String(outer);
      } else {
        roiState.innerRadius = Math.min(snap.innerRadius, outer);
        if (roiOuterInput) roiOuterInput.value = String(outer);
        if (roiInnerInput) roiInnerInput.value = String(roiState.innerRadius);
      }
    } else if (roiEditHandle === "inner" && roiState.mode === "annulus") {
      roiState.start = { ...snap.start };
      roiState.end = { ...snap.end };
      const outer = snap.outerRadius || Math.hypot(snap.end.x - snap.start.x, snap.end.y - snap.start.y);
      const inner = Math.max(0, Math.min(Math.round(Math.hypot(point.x - snap.start.x, point.y - snap.start.y)), outer));
      roiState.outerRadius = outer;
      roiState.innerRadius = inner;
      if (roiInnerInput) roiInnerInput.value = String(inner);
      if (roiOuterInput) roiOuterInput.value = String(Math.round(outer));
    }
    updateRoiCenterInputs();
  }
  roiState.active = true;
  scheduleRoiOverlay();
  scheduleRoiUpdate();
}

function stopRoiEdit(event) {
  if (!roiEditing) return;
  roiEditing = false;
  roiEditHandle = null;
  roiEditStart = null;
  roiEditSnapshot = null;
  canvasWrap.classList.remove("is-roi");
  if (event && canvasWrap.hasPointerCapture(event.pointerId)) {
    canvasWrap.releasePointerCapture(event.pointerId);
  }
  scheduleRoiOverlay();
  scheduleRoiUpdate();
}

function scheduleRoiUpdate() {
  if (roiUpdateScheduled) return;
  roiUpdateScheduled = true;
  window.requestAnimationFrame(() => {
    roiUpdateScheduled = false;
    updateRoiStats();
  });
}

function setLoading(show) {
  if (!loadingEl) return;
  loadingEl.style.display = show ? "block" : "none";
  updateRoiSectionState();
  updateRingsSectionState();
  updatePeaksSectionState();
  if (!show && statusEl && state.hasFrame) {
    const status = (statusEl.textContent || "").trim();
    if (/^Loading (image|frame|metadata|datasets)(…|\.{3})$/i.test(status)) {
      setStatus(currentFrameStatusText());
    }
  }
}

function scheduleOverview() {
  overviewViewportController?.scheduleOverview();
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

function panToImageCenter(x, y) {
  overviewViewportController?.panToImageCenter(x, y);
}

function getOverviewHandleAt(point) {
  return overviewViewportController ? overviewViewportController.getOverviewHandleAt(point) : null;
}

function getAnchorForHandle(view, handle, keepCenter) {
  return overviewViewportController
    ? overviewViewportController.getAnchorForHandle(view, handle, keepCenter)
    : null;
}

function resizeViewFromHandle(point, handle, keepCenter) {
  overviewViewportController?.resizeViewFromHandle(point, handle, keepCenter);
}

function setZoom(value) {
  overviewViewportController?.setZoom(value);
}

function zoomAt(clientX, clientY, nextZoom) {
  overviewViewportController?.zoomAt(clientX, clientY, nextZoom);
}

function normalizeWheelDelta(event) {
  return overviewViewportController ? overviewViewportController.normalizeWheelDelta(event) : 0;
}

function startTouchGesture(touches) {
  overviewViewportController?.startTouchGesture(touches);
}

function stopTouchGesture() {
  overviewViewportController?.stopTouchGesture();
}

function updateTouchGesture(touches) {
  overviewViewportController?.updateTouchGesture(touches);
}

function queueWheelZoom(delta, clientX, clientY) {
  overviewViewportController?.queueWheelZoom(delta, clientX, clientY);
}

function isTouchGestureActive() {
  return overviewViewportController ? overviewViewportController.isTouchGestureActive() : false;
}

function fitImageToView() {
  overviewViewportController?.fitImageToView();
}

function updateFpsLabel() {
  if (fpsSelect) {
    fpsSelect.value = String(state.fps);
  }
  if (toolbarMoreFps) {
    toolbarMoreFps.value = String(state.fps);
  }
}

function setToolbarPlaybackPopoverOpen(open) {
  if (!toolbarPlaybackWrap || !toolbarPlaybackToggle || !toolbarPlaybackPopover) return;
  if (open) {
    closeToolbarMorePopover();
  }
  toolbarPlaybackWrap.classList.toggle("is-open", open);
  toolbarPlaybackToggle.setAttribute("aria-expanded", open ? "true" : "false");
  toolbarPlaybackToggle.textContent = open ? "Playback ▴" : "Playback ▾";
  toolbarPlaybackPopover.setAttribute("aria-hidden", open ? "false" : "true");
}

function closeToolbarPlaybackPopover() {
  setToolbarPlaybackPopoverOpen(false);
}

function toggleToolbarPlaybackPopover() {
  if (!toolbarPlaybackWrap || toolbarPlaybackWrap.classList.contains("is-hidden")) return;
  const hasStep = Boolean(toolbarStepWrap && !toolbarStepWrap.classList.contains("is-hidden"));
  const hasFps = Boolean(toolbarFpsWrap && !toolbarFpsWrap.classList.contains("is-hidden"));
  if (!hasStep && !hasFps) return;
  const willOpen = !toolbarPlaybackWrap.classList.contains("is-open");
  setToolbarPlaybackPopoverOpen(willOpen);
}

function setToolbarMorePopoverOpen(open) {
  if (!toolbarMoreWrap || !toolbarMoreToggle || !toolbarMorePopover) return;
  if (open) {
    closeToolbarPlaybackPopover();
  }
  toolbarMoreWrap.classList.toggle("is-open", open);
  toolbarMoreToggle.setAttribute("aria-expanded", open ? "true" : "false");
  toolbarMoreToggle.textContent = open ? "More ▴" : "More ▾";
  toolbarMorePopover.setAttribute("aria-hidden", open ? "false" : "true");
}

function closeToolbarMorePopover() {
  setToolbarMorePopoverOpen(false);
}

function toggleToolbarMorePopover() {
  if (!toolbarMoreWrap || toolbarMoreWrap.classList.contains("is-hidden")) return;
  const willOpen = !toolbarMoreWrap.classList.contains("is-open");
  setToolbarMorePopoverOpen(willOpen);
}

function syncToolbarMoreControls() {
  if (toolbarMoreStep) {
    toolbarMoreStep.value = String(state.step || FRAME_STEP_OPTIONS[0]);
  }
  if (toolbarMoreFps) {
    toolbarMoreFps.value = String(state.fps || 1);
  }
  if (toolbarMoreThreshold) {
    toolbarMoreThreshold.value = String(state.thresholdIndex || 0);
  }
  if (toolbarMorePanelToggle) {
    toolbarMorePanelToggle.textContent = state.panelCollapsed ? "Open side menu" : "Close side menu";
    toolbarMorePanelToggle.setAttribute("aria-label", state.panelCollapsed ? "Open side menu" : "Close side menu");
  }
  if (toolbarMoreFullscreen) {
    toolbarMoreFullscreen.textContent = document.fullscreenElement ? "Exit full screen" : "Full screen";
  }
}

function getThresholdDisplayOrder(count = state.thresholdCount, energies = state.thresholdEnergies) {
  const safeCount = Math.max(1, Number(count) || 1);
  const order = Array.from({ length: safeCount }, (_, i) => i);
  const energyList = Array.isArray(energies) ? energies : [];
  const hasFiniteEnergy = order.some((idx) => Number.isFinite(Number(energyList[idx])));
  if (!hasFiniteEnergy) return order;
  order.sort((a, b) => {
    const energyA = Number(energyList[a]);
    const energyB = Number(energyList[b]);
    const aFinite = Number.isFinite(energyA);
    const bFinite = Number.isFinite(energyB);
    if (aFinite && bFinite) {
      if (energyA === energyB) return a - b;
      return energyB - energyA;
    }
    if (aFinite !== bFinite) return aFinite ? -1 : 1;
    return a - b;
  });
  return order;
}

function getDefaultThresholdIndex() {
  const order = getThresholdDisplayOrder();
  return order.length ? order[order.length - 1] : 0;
}

function getThresholdIndexAtOffset(offset) {
  const order = getThresholdDisplayOrder();
  if (!order.length) return 0;
  const current = order.includes(state.thresholdIndex) ? state.thresholdIndex : getDefaultThresholdIndex();
  const currentPos = Math.max(0, order.indexOf(current));
  const nextPos = Math.max(0, Math.min(order.length - 1, currentPos + Math.round(offset)));
  return order[nextPos];
}

function updateThresholdOptions() {
  if (!thresholdSelect || !thresholdField) return;
  const count = Math.max(1, state.thresholdCount || 1);
  const show = count > 1 && state.autoload.mode === "file";
  thresholdField.classList.toggle("is-hidden", !show);
  if (toolbarThresholdWrap) {
    toolbarThresholdWrap.classList.toggle("is-hidden", !show);
  }
  thresholdSelect.innerHTML = "";
  if (toolbarThresholdSelect) {
    toolbarThresholdSelect.innerHTML = "";
  }
  if (toolbarMoreThreshold) {
    toolbarMoreThreshold.innerHTML = "";
  }
  const energies = Array.isArray(state.thresholdEnergies) ? state.thresholdEnergies : [];
  const order = getThresholdDisplayOrder(count, energies);
  order.forEach((thresholdIndex) => {
    const energy = Number(energies[thresholdIndex]);
    const energyText = Number.isFinite(energy) ? ` ${formatEnergy(energy)} eV` : "";
    const label = `Thr${thresholdIndex + 1}${energyText}`;
    thresholdSelect.appendChild(option(label, String(thresholdIndex)));
    if (toolbarThresholdSelect) {
      toolbarThresholdSelect.appendChild(option(label, String(thresholdIndex)));
    }
    if (toolbarMoreThreshold) {
      toolbarMoreThreshold.appendChild(option(label, String(thresholdIndex)));
    }
  });
  const idx = order.includes(state.thresholdIndex) ? state.thresholdIndex : getDefaultThresholdIndex();
  state.thresholdIndex = idx;
  thresholdSelect.value = String(idx);
  if (toolbarThresholdSelect) {
    toolbarThresholdSelect.value = String(idx);
  }
  if (toolbarMoreThreshold) {
    toolbarMoreThreshold.value = String(idx);
    toolbarMoreThreshold.disabled = count <= 1;
  }
  if (toolbarMoreThresholdField) {
    toolbarMoreThresholdField.classList.toggle("is-hidden", !show);
  }
  thresholdSelect.disabled = count <= 1;
  if (toolbarThresholdSelect) {
    toolbarThresholdSelect.disabled = count <= 1;
  }
  syncToolbarMoreControls();
  updateViewerFooter();
}

async function setThresholdIndex(nextIndex) {
  const count = Math.max(1, state.thresholdCount || 1);
  const parsed = Number(nextIndex);
  if (!Number.isFinite(parsed)) return;
  const clamped = Math.max(0, Math.min(count - 1, Math.round(parsed)));
  if (clamped === state.thresholdIndex) return;
  state.thresholdIndex = clamped;
  if (thresholdSelect) thresholdSelect.value = String(clamped);
  if (toolbarThresholdSelect) toolbarThresholdSelect.value = String(clamped);
  if (toolbarMoreThreshold) toolbarMoreThreshold.value = String(clamped);
  state.maskFile = "";
  await loadMask(true);
  requestFrame(state.frameIndex);
}

function setFps(value) {
  const clamped = Math.max(1, Math.min(10, Math.round(value)));
  state.fps = clamped;
  if (fpsSelect) fpsSelect.value = String(clamped);
  if (toolbarMoreFps) toolbarMoreFps.value = String(clamped);
  updateFpsLabel();
  if (state.playing) {
    stopPlayback();
    startPlayback();
  }
}

function setFrameStep(value) {
  const parsed = Math.round(Number(value || 1));
  const next = FRAME_STEP_OPTIONS.includes(parsed) ? parsed : FRAME_STEP_OPTIONS[0];
  state.step = next;
  if (frameStep) {
    frameStep.value = String(next);
  }
  if (toolbarMoreStep) {
    toolbarMoreStep.value = String(next);
  }
}

function updatePlayButtons() {
  const hasSeries = Array.isArray(state.seriesFiles) && state.seriesFiles.length > 0;
  const disabled = !state.file || (!state.dataset && !hasSeries) || state.frameCount <= 1;
  if (playBtn) {
    playBtn.classList.toggle("is-active", state.playing);
    playBtn.disabled = disabled;
    playBtn.textContent = state.playing ? "⏸" : "⏯";
  }
  if (prevBtn) prevBtn.disabled = disabled;
  if (nextBtn) nextBtn.disabled = disabled;
}

function getMaxPanelWidth() {
  return Math.max(220, Math.min(900, window.innerWidth - 24));
}

function isPhonePanelLayout() {
  return Boolean(coarsePointerQuery?.matches) && window.innerWidth < 768;
}

function clampMobilePanelSnap(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return MOBILE_PANEL_SNAP_POINTS[0];
  return Math.max(0.52, Math.min(1, numeric));
}

function nearestMobilePanelSnap(value) {
  const clamped = clampMobilePanelSnap(value);
  return MOBILE_PANEL_SNAP_POINTS.reduce((closest, candidate) =>
    Math.abs(candidate - clamped) < Math.abs(closest - clamped) ? candidate : closest
  );
}

function setMobilePanelSnap(value, snap = true, persist = false) {
  mobilePanelSnap = snap ? nearestMobilePanelSnap(value) : clampMobilePanelSnap(value);
  if (!toolsPanel) return;
  const heightPct = `${Math.round(mobilePanelSnap * 100)}vh`;
  toolsPanel.style.setProperty("--mobile-panel-height", heightPct);
  toolsPanel.dataset.mobileSnap = mobilePanelSnap >= 0.95 ? "full" : "mid";
  if (persist) {
    try {
      localStorage.setItem("albis.mobilePanelSnap", String(mobilePanelSnap));
    } catch {
      // ignore storage errors
    }
  }
}

function applyPanelState() {
  if (!toolsPanel || !appLayout) return;

  const isPhone = isPhonePanelLayout();
  document.body.classList.toggle("panel-collapsed", state.panelCollapsed);
  toolsPanel.classList.toggle("is-collapsed", state.panelCollapsed);
  toolsPanel.classList.toggle("is-mobile-sheet", isPhone);
  if (panelBody) {
    panelBody.setAttribute("aria-hidden", state.panelCollapsed ? "true" : "false");
  }

  if (isPhone) {
    setMobilePanelSnap(mobilePanelSnap, false);
    toolsPanel.classList.toggle("is-visible", !state.panelCollapsed);
    appLayout.style.removeProperty("--panel-width");
    document.documentElement.style.removeProperty("--panel-width");
  } else {
    toolsPanel.classList.remove("is-visible");
    toolsPanel.style.removeProperty("--mobile-panel-height");
    delete toolsPanel.dataset.mobileSnap;
    const maxPanelWidth = getMaxPanelWidth();
    const targetWidth = Math.max(220, Math.min(maxPanelWidth, state.panelWidth));
    const width = state.panelCollapsed ? 34 : targetWidth;
    appLayout.style.setProperty("--panel-width", `${width}px`);
    document.documentElement.style.setProperty("--panel-width", `${width}px`);
  }

  if (panelFab) {
    panelFab.classList.toggle("is-collapsed", state.panelCollapsed);
    panelFab.classList.toggle("is-open", !state.panelCollapsed);
    panelFab.dataset.state = state.panelCollapsed ? "collapsed" : "expanded";
    panelFab.textContent = state.panelCollapsed ? "Open menu ▾" : "Close menu ▴";
    panelFab.setAttribute("aria-label", state.panelCollapsed ? "Open panel" : "Collapse panel");
    panelFab.setAttribute("aria-expanded", state.panelCollapsed ? "false" : "true");
    panelFab.setAttribute("aria-keyshortcuts", "M");
    panelFab.title = state.panelCollapsed ? "Open side menu (M)" : "Close side menu (M)";
  }
  if (panelCollapseBtn) {
    panelCollapseBtn.disabled = state.panelCollapsed;
    panelCollapseBtn.setAttribute("aria-hidden", state.panelCollapsed ? "true" : "false");
    panelCollapseBtn.tabIndex = state.panelCollapsed ? -1 : 0;
  }
  syncToolbarMoreControls();
  scheduleOverview();
  scheduleHistogram();
  updateUiIdleAndAnchors();
}

function togglePanel() {
  const wasCollapsed = state.panelCollapsed;
  state.panelCollapsed = !state.panelCollapsed;
  if (isPhonePanelLayout() && wasCollapsed && !state.panelCollapsed) {
    setMobilePanelSnap(MOBILE_PANEL_SNAP_POINTS[0], true, true);
  }
  applyPanelState();
  try {
    localStorage.setItem("albis.panelCollapsed", String(state.panelCollapsed));
    localStorage.setItem("albis.panelWidth", String(state.panelWidth));
  } catch {
    // ignore storage errors
  }
}

function sanitizeIdFragment(value, fallbackPrefix = "section") {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "-")
    .replace(/^-+|-+$/g, "");
  if (normalized) return normalized;
  sectionA11yCounter += 1;
  return `${fallbackPrefix}-${sectionA11yCounter}`;
}

function syncSectionA11y(section) {
  if (!section) return;
  const title = section.querySelector(":scope > .section-title");
  const content = section.querySelector(":scope > .section-content");
  if (!title || !content) return;
  if (!content.id) {
    const sectionId = sanitizeIdFragment(section.dataset.section, "section");
    content.id = `section-content-${sectionId}`;
  }
  const collapsed = section.classList.contains("is-collapsed");
  title.setAttribute("aria-controls", content.id);
  title.setAttribute("aria-expanded", collapsed ? "false" : "true");
  content.setAttribute("aria-hidden", collapsed ? "true" : "false");
}

function initializePanelTabA11y() {
  document.querySelector(".panel-tabs[role='tablist']")?.setAttribute("aria-orientation", "horizontal");
  panelTabs.forEach((tab) => {
    tab.setAttribute("role", "tab");
    const tabId = sanitizeIdFragment(tab.dataset.panelTab, "panel-tab");
    if (!tab.id) {
      tab.id = `panel-tab-${tabId}`;
    }
    const panel = Array.from(panelTabContents).find((item) => item.dataset.panelTab === tab.dataset.panelTab);
    if (!panel) return;
    panel.setAttribute("role", "tabpanel");
    if (!panel.id) {
      panel.id = `panel-content-${tabId}`;
    }
    panel.setAttribute("aria-labelledby", tab.id);
    tab.setAttribute("aria-controls", panel.id);
  });
}

function toggleSection(event) {
  const button = event.currentTarget;
  const section = button.closest(".panel-section");
  if (!section) return;
  setSectionState(section, !section.classList.contains("is-collapsed"));
  scheduleOverview();
  scheduleHistogram();
  schedulePixelOverlay();
}

function setSectionState(section, collapsed, persist = true) {
  section.classList.toggle("is-collapsed", collapsed);
  syncSectionA11y(section);
  const id = section.dataset.section;
  if (persist && id) {
    sectionStateStore[id] = collapsed;
    try {
      localStorage.setItem("albis.sectionStates", JSON.stringify(sectionStateStore));
    } catch {
      // ignore storage errors
    }
  }
}

function initializeSectionContentWrappers() {
  const sections = document.querySelectorAll(".panel-section");
  sections.forEach((section) => {
    const title = section.querySelector(":scope > .section-title");
    if (!title) return;
    let wrapper = section.querySelector(":scope > .section-content");
    if (!wrapper) {
      wrapper = document.createElement("div");
      wrapper.className = "section-content";
      let node = title.nextSibling;
      while (node) {
        const next = node.nextSibling;
        wrapper.appendChild(node);
        node = next;
      }
      section.appendChild(wrapper);
    }
    syncSectionA11y(section);
  });
}

function setPanelWidth(width) {
  if (isPhonePanelLayout()) return;
  const maxPanelWidth = getMaxPanelWidth();
  const clamped = Math.max(220, Math.min(maxPanelWidth, Math.round(width)));
  state.panelWidth = clamped;
  state.panelCollapsed = false;
  applyPanelState();
  scheduleHistogram();
  try {
    localStorage.setItem("albis.panelWidth", String(state.panelWidth));
  } catch {
    // ignore storage errors
  }
}

function startMobilePanelDrag(event) {
  if (!isPhonePanelLayout() || state.panelCollapsed || !toolsPanel) return;
  mobilePanelDragActive = true;
  mobilePanelDragPointer = event.pointerId;
  mobilePanelDragStartY = event.clientY;
  mobilePanelDragStartSnap = mobilePanelSnap;
  toolsPanel.classList.add("is-dragging");
  if (panelSheetHandle?.setPointerCapture && Number.isInteger(event.pointerId)) {
    panelSheetHandle.setPointerCapture(event.pointerId);
  }
  event.preventDefault();
}

function updateMobilePanelDrag(event) {
  if (!mobilePanelDragActive) return;
  if (!Number.isInteger(mobilePanelDragPointer) || event.pointerId === mobilePanelDragPointer) {
    const viewportHeight = Math.max(320, window.innerHeight || 1);
    const delta = mobilePanelDragStartY - event.clientY;
    const next = mobilePanelDragStartSnap + delta / viewportHeight;
    setMobilePanelSnap(next, false);
    event.preventDefault();
  }
}

function stopMobilePanelDrag(event, cancelled = false) {
  if (!mobilePanelDragActive || !toolsPanel) return;
  if (!Number.isInteger(mobilePanelDragPointer) || event.pointerId === mobilePanelDragPointer || cancelled) {
    const dragDown = (event.clientY || mobilePanelDragStartY) - mobilePanelDragStartY;
    mobilePanelDragActive = false;
    mobilePanelDragPointer = null;
    toolsPanel.classList.remove("is-dragging");
    if (panelSheetHandle?.releasePointerCapture && Number.isInteger(event.pointerId)) {
      try {
        panelSheetHandle.releasePointerCapture(event.pointerId);
      } catch {
        // ignore release errors
      }
    }
    if (!cancelled && dragDown > 120 && mobilePanelSnap <= MOBILE_PANEL_SNAP_POINTS[0] + 0.05) {
      state.panelCollapsed = true;
      applyPanelState();
      try {
        localStorage.setItem("albis.panelCollapsed", String(state.panelCollapsed));
      } catch {
        // ignore storage errors
      }
      return;
    }
    setMobilePanelSnap(mobilePanelSnap, true, true);
    applyPanelState();
  }
}

function stopPlayback() {
  if (state.playTimer) {
    window.clearInterval(state.playTimer);
    state.playTimer = null;
  }
  state.playing = false;
  updatePlayButtons();
}

function updateFrameControls() {
  const total = Math.max(1, state.frameCount || 1);
  const displayValue = Math.max(1, Math.min(total, (state.frameIndex || 0) + 1));
  if (frameRange) {
    frameRange.min = "1";
    frameRange.max = String(total);
    frameRange.value = String(displayValue);
    frameRange.disabled = total <= 1;
  }
  if (frameIndex) {
    frameIndex.min = "1";
    frameIndex.max = String(total);
    frameIndex.value = String(displayValue);
    frameIndex.disabled = total <= 1;
  }
}

function startPlayback() {
  if (state.playing || state.frameCount <= 1) return;
  state.playing = true;
  updatePlayButtons();
  setLoading(false);
  state.playTimer = window.setInterval(() => {
    if (!state.playing) return;
    const step = Math.max(1, state.step);
    const next = (state.frameIndex + step) % state.frameCount;
    requestFrame(next);
  }, Math.max(1000 / state.fps, 50));
}

function processPendingFrameRequest(appliedFrame) {
  if (state.pendingFrame === null) return;
  const next = state.pendingFrame;
  state.pendingFrame = null;
  if (next !== state.frameIndex || !appliedFrame) {
    requestFrame(next);
  }
}

function requestFrame(index) {
  const hasSeries = Array.isArray(state.seriesFiles) && state.seriesFiles.length > 0;
  if (!state.frameCount || (!state.dataset && !hasSeries) || !state.file) return;
  const clamped = Math.max(0, Math.min(state.frameCount - 1, index));
  if (state.playing && isViewportInteractionActive()) {
    state.pendingFrame = clamped;
    return;
  }
  if (state.isLoading) {
    state.pendingFrame = clamped;
    cancelActiveFrameLoad();
    return;
  }
  state.frameIndex = clamped;
  updateFrameControls();
  updateToolbar();
  loadFrame();
}

function drawGlowDot(ctx, x, y, core, glow, rgb = "255,255,255") {
  const grad = ctx.createRadialGradient(x, y, 0, x, y, glow);
  grad.addColorStop(0, `rgba(${rgb},0.95)`);
  grad.addColorStop(1, `rgba(${rgb},0)`);
  ctx.fillStyle = grad;
  ctx.beginPath();
  ctx.arc(x, y, glow, 0, Math.PI * 2);
  ctx.fill();
  ctx.fillStyle = `rgba(${rgb},0.98)`;
  ctx.beginPath();
  ctx.arc(x, y, core, 0, Math.PI * 2);
  ctx.fill();
}

function seededRandom(seed) {
  let value = seed % 2147483647;
  if (value <= 0) value += 2147483646;
  return () => {
    value = (value * 16807) % 2147483647;
    return (value - 1) / 2147483646;
  };
}

function drawStarfield(ctx, width, height) {
  const rand = seededRandom(Math.floor(width * 13 + height * 29));
  const total = Math.round(
    Math.min(420, Math.max(180, (width * height) / 7000)),
  );

  for (let i = 0; i < total; i += 1) {
    const x = rand() * width;
    const y = rand() * height;
    const intensity = rand();
    const size = 0.4 + Math.pow(intensity, 2.1) * 1.6;
    const glow = size * (intensity > 0.82 ? 6.5 : 3.2);
    const tint = intensity > 0.75 ? "190,220,255" : "255,255,255";
    drawGlowDot(ctx, x, y, size, glow, tint);
  }

  for (let i = 0; i < 6; i += 1) {
    const cx = width * (0.15 + rand() * 0.7);
    const cy = height * (0.15 + rand() * 0.7);
    const radius = Math.min(width, height) * (0.2 + rand() * 0.25);
    const haze = ctx.createRadialGradient(cx, cy, 0, cx, cy, radius);
    haze.addColorStop(0, "rgba(80,130,200,0.12)");
    haze.addColorStop(1, "rgba(0,0,0,0)");
    ctx.fillStyle = haze;
    ctx.beginPath();
    ctx.arc(cx, cy, radius, 0, Math.PI * 2);
    ctx.fill();
  }
}

function drawSnowflake(ctx, centerX, centerY, radius) {
  const segments = [];
  const addSegment = (x1, y1, x2, y2) => {
    segments.push({ x1, y1, x2, y2 });
  };

  const branchSteps = [
    { t: 0.16, len: 0.28 },
    { t: 0.3, len: 0.26 },
    { t: 0.44, len: 0.24 },
    { t: 0.58, len: 0.22 },
    { t: 0.7, len: 0.2 },
    { t: 0.82, len: 0.16 },
    { t: 0.92, len: 0.12 },
  ];

  for (let arm = 0; arm < 6; arm += 1) {
    const angle = (arm * Math.PI * 2) / 6;
    const dx = Math.cos(angle);
    const dy = Math.sin(angle);
    addSegment(centerX, centerY, centerX + dx * radius, centerY + dy * radius);

    branchSteps.forEach((step) => {
      const baseX = centerX + dx * radius * step.t;
      const baseY = centerY + dy * radius * step.t;
      const branchLen = radius * step.len;

      [1, -1].forEach((sign) => {
        const branchAngle = angle + sign * (Math.PI / 6);
        const bx = baseX + Math.cos(branchAngle) * branchLen;
        const by = baseY + Math.sin(branchAngle) * branchLen;
        addSegment(baseX, baseY, bx, by);

        const twigBaseX =
          baseX + Math.cos(branchAngle) * branchLen * 0.62;
        const twigBaseY =
          baseY + Math.sin(branchAngle) * branchLen * 0.62;
        const twigAngle = branchAngle + sign * (Math.PI / 10);
        const twigLen = branchLen * 0.42;
        addSegment(
          twigBaseX,
          twigBaseY,
          twigBaseX + Math.cos(twigAngle) * twigLen,
          twigBaseY + Math.sin(twigAngle) * twigLen,
        );
      });
    });

    const tipX = centerX + dx * radius;
    const tipY = centerY + dy * radius;
    const tipLen = radius * 0.12;
    [1, -1].forEach((sign) => {
      const tipAngle = angle + sign * (Math.PI / 9);
      addSegment(
        tipX - dx * tipLen * 0.25,
        tipY - dy * tipLen * 0.25,
        tipX + Math.cos(tipAngle) * tipLen,
        tipY + Math.sin(tipAngle) * tipLen,
      );
    });
  }

  const strokeSegments = (width, color, blur = 0, shadow = "") => {
    ctx.save();
    ctx.lineCap = "round";
    ctx.lineJoin = "round";
    ctx.strokeStyle = color;
    ctx.lineWidth = width;
    ctx.shadowBlur = blur;
    ctx.shadowColor = shadow;
    ctx.beginPath();
    segments.forEach((seg) => {
      ctx.moveTo(seg.x1, seg.y1);
      ctx.lineTo(seg.x2, seg.y2);
    });
    ctx.stroke();
    ctx.restore();
  };

  ctx.save();
  ctx.globalCompositeOperation = "lighter";
  strokeSegments(
    Math.max(2.6, radius * 0.02),
    "rgba(120,190,255,0.2)",
    Math.max(18, radius * 0.12),
    "rgba(120,190,255,0.8)",
  );
  strokeSegments(
    Math.max(1.6, radius * 0.012),
    "rgba(200,235,255,0.85)",
  );
  strokeSegments(Math.max(0.9, radius * 0.006), "rgba(255,255,255,0.98)");

  const coreGlow = ctx.createRadialGradient(
    centerX,
    centerY,
    0,
    centerX,
    centerY,
    radius * 0.18,
  );
  coreGlow.addColorStop(0, "rgba(230,245,255,0.9)");
  coreGlow.addColorStop(1, "rgba(120,190,255,0)");
  ctx.fillStyle = coreGlow;
  ctx.beginPath();
  ctx.arc(centerX, centerY, radius * 0.18, 0, Math.PI * 2);
  ctx.fill();
  ctx.restore();
}

function drawSplash() {
  if (!splash || !splashCanvas || !splashCtx) return;
  const width = Math.max(1, Math.floor(splash.clientWidth));
  const height = Math.max(1, Math.floor(splash.clientHeight));
  const dpr = window.devicePixelRatio || 1;
  splashCanvas.width = width * dpr;
  splashCanvas.height = height * dpr;
  splashCanvas.style.width = `${width}px`;
  splashCanvas.style.height = `${height}px`;
  splashCtx.setTransform(dpr, 0, 0, dpr, 0, 0);
  splashCtx.clearRect(0, 0, width, height);
  splashCtx.fillStyle = "#000";
  splashCtx.fillRect(0, 0, width, height);

  const centerX = width / 2;
  const centerY = height / 2 - Math.min(24, height * 0.03);
  const radius = Math.min(width, height) * 0.22;

  drawStarfield(splashCtx, width, height);

  const halo = splashCtx.createRadialGradient(
    centerX,
    centerY,
    0,
    centerX,
    centerY,
    radius * 3.2,
  );
  halo.addColorStop(0, "rgba(20,40,70,0.45)");
  halo.addColorStop(1, "rgba(0,0,0,0)");
  splashCtx.fillStyle = halo;
  splashCtx.beginPath();
  splashCtx.arc(centerX, centerY, radius * 3.2, 0, Math.PI * 2);
  splashCtx.fill();

  drawSnowflake(splashCtx, centerX, centerY, radius);

  const vignette = splashCtx.createRadialGradient(
    centerX,
    centerY,
    radius * 0.6,
    centerX,
    centerY,
    Math.max(width, height) * 0.7,
  );
  vignette.addColorStop(0, "rgba(0,0,0,0)");
  vignette.addColorStop(1, "rgba(0,0,0,0.65)");
  splashCtx.fillStyle = vignette;
  splashCtx.fillRect(0, 0, width, height);
}

function updateSplashCallToAction() {
  if (!splashOpenFileBtn || !splash) return;
  const splashVisible = !splash.classList.contains("is-hidden");
  const ready = Boolean(state.backendAlive) && !state.isLoading;
  const show = splashVisible && ready && !state.hasFrame;
  if (splashActions) {
    splashActions.classList.toggle("is-hidden", !show);
  }
  splashOpenFileBtn.classList.toggle("is-hidden", !show);
  splashOpenFileBtn.disabled = !show;
}

function showSplash() {
  splash?.classList.remove("is-hidden");
  updateSplashCallToAction();
}

function setSplashStatus(text) {
  if (!splashStatus) return;
  const normalized = String(text || "").trim();
  splashStatus.textContent = normalized;
  if (!splash) return;
  const lower = normalized.toLowerCase();
  const busy = Boolean(normalized) && !/\b(ready|failed|error|done|complete)\b/.test(lower);
  splash.classList.toggle("is-busy", busy);
  updateSplashCallToAction();
}

function hideSplash() {
  splash?.classList.add("is-hidden");
  updateSplashCallToAction();
}

function updateFullscreenUi() {
  const active = Boolean(document.fullscreenElement);
  document.body.classList.toggle("is-fullscreen", active);
  if (fullscreenToggle) {
    fullscreenToggle.classList.toggle("is-active", active);
    fullscreenToggle.textContent = active ? "🗗" : "⛶";
    fullscreenToggle.setAttribute("aria-label", active ? "Exit full screen" : "Enter full screen");
    fullscreenToggle.title = active ? "Exit full screen (F)" : "Enter full screen (F)";
  }
  syncToolbarMoreControls();
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
    setStatus("Fullscreen unavailable");
  }
}

function buildViewerSourceText(maxChars = 72) {
  if (!state.file) return "No file loaded";
  const fileName = fileLabel(state.file);
  let frameLabel = "";
  if (state.frameCount > 1) {
    frameLabel = `${state.frameIndex + 1} / ${state.frameCount}`;
  } else if (state.autoload.mode !== "file" && state.autoload.lastUpdate) {
    frameLabel = formatTimeStamp(state.autoload.lastUpdate);
  }
  const datasetRaw = state.dataset ? middleTruncate(state.dataset, 38) : "";
  const datasetLabel = datasetRaw ? ` ${datasetRaw}` : "";
  const suffix = frameLabel ? `  ${frameLabel}` : "";
  const reserved = datasetLabel.length + suffix.length;
  const fileBudget = Math.max(10, maxChars - reserved);
  const fileText = middleTruncate(fileName, fileBudget);
  return `${fileText}${datasetLabel}${suffix}`;
}

function updateFooterVersions() {
  if (footerFrontendVersionEl) {
    footerFrontendVersionEl.textContent = `Frontend: ${APP_FRONTEND_BUILD}`;
    footerFrontendVersionEl.title = `Frontend build ${APP_FRONTEND_BUILD}`;
  }
  if (footerBackendVersionEl) {
    const backendVersion = state.backendVersion || "-";
    footerBackendVersionEl.textContent = `Backend: v${backendVersion}`;
    footerBackendVersionEl.title = `Backend version ${backendVersion}`;
  }
}

function updateViewerFooter() {
  if (footerFileEl) {
    const hasFile = Boolean(state.file);
    footerFileEl.textContent = hasFile ? buildViewerSourceText(78) : "";
    footerFileEl.classList.toggle("is-empty", !hasFile);
  }
  if (footerZoomEl) {
    footerZoomEl.textContent = `Zoom ${(state.zoom || 1).toFixed(1)}x`;
  }
  updateFooterVersions();
  scheduleChromeIdle();
}

function setFooterVersionPopoverOpen(open) {
  footerVersionPopoverOpen = Boolean(open);
  if (footerVersionToggleEl) {
    footerVersionToggleEl.setAttribute("aria-expanded", footerVersionPopoverOpen ? "true" : "false");
    footerVersionToggleEl.textContent = footerVersionPopoverOpen ? "Versions ▴" : "Versions ▾";
  }
  if (footerVersionPopoverEl) {
    footerVersionPopoverEl.classList.toggle("is-open", footerVersionPopoverOpen);
    footerVersionPopoverEl.setAttribute("aria-hidden", footerVersionPopoverOpen ? "false" : "true");
  }
}

function closeFooterVersionPopover() {
  setFooterVersionPopoverOpen(false);
}

function toggleFooterVersionPopover() {
  setFooterVersionPopoverOpen(!footerVersionPopoverOpen);
}

function shouldEnableChromeIdle() {
  if (!document.body.classList.contains("canvas-first")) return false;
  if (!state.hasFrame || state.isLoading) return false;
  if (!splash?.classList.contains("is-hidden")) return false;
  if (activeMenu && dropdown?.classList.contains("is-open")) return false;
  if (toolbarPlaybackWrap?.classList.contains("is-open")) return false;
  if (toolbarMoreWrap?.classList.contains("is-open")) return false;
  if (footerVersionPopoverOpen) return false;
  return true;
}

function setChromeIdle(active) {
  chromeIdleActive = Boolean(active);
  document.body.classList.toggle("chrome-idle", chromeIdleActive);
}

function clearChromeIdleTimer() {
  if (!chromeIdleTimer) return;
  window.clearTimeout(chromeIdleTimer);
  chromeIdleTimer = null;
}

function scheduleChromeIdle() {
  clearChromeIdleTimer();
  if (!shouldEnableChromeIdle()) {
    setChromeIdle(false);
    return;
  }
  chromeIdleTimer = window.setTimeout(() => {
    chromeIdleTimer = null;
    if (shouldEnableChromeIdle()) {
      setChromeIdle(true);
    }
  }, CHROME_IDLE_DELAY_MS);
}

function registerChromeActivity() {
  const now = performance.now();
  if (!chromeIdleActive && now - chromeActivityTs < 110) return;
  chromeActivityTs = now;
  setChromeIdle(false);
  scheduleChromeIdle();
}

function syncOverlayAnchors() {
  if (!document.body.classList.contains("canvas-first")) return;
  if (isPhonePanelLayout()) return;
  const toolbarEl = document.querySelector(".toolbar");
  if (!toolbarEl) return;
  const toolbarRect = toolbarEl.getBoundingClientRect();
  if (!Number.isFinite(toolbarRect.top)) return;
  const anchorTop = Math.max(0, Math.round(toolbarRect.top));
  document.documentElement.style.setProperty("--overlay-anchor-top", `${anchorTop}px`);
  const fabHeight = panelFab?.getBoundingClientRect().height || 46;
  const triggerTop = Math.max(0, Math.round(anchorTop + Math.max(0, (toolbarRect.height - fabHeight) * 0.5)));
  document.documentElement.style.setProperty("--overlay-panel-trigger-top", `${triggerTop}px`);
}

function updateUiIdleAndAnchors() {
  syncOverlayAnchors();
  scheduleChromeIdle();
}

function updateDataSourceSummary() {
  if (!dataSourceSummaryEl) return;
  const mode = (state.autoload.mode || "file").toLowerCase();
  if (mode === "file") {
    const hasFile = Boolean(state.file);
    const fileText = hasFile ? middleTruncate(fileLabel(state.file), 24) : "no file";
    setSummaryChip(dataSourceSummaryEl, `File · ${fileText}`, hasFile ? "active" : "default");
    return;
  }

  const modeLabel =
    mode === "simplon"
      ? "SIMPLON"
      : mode === "jungfraujoch"
        ? "JFJ Preview"
        : "Remote";
  const running = Boolean(state.autoload.running);
  const age = Date.now() - (state.autoload.lastUpdate || 0);
  const stale = running && (!state.autoload.lastUpdate || age > Math.max(1500, state.autoload.interval * 2));
  const streamState = !running ? "idle" : stale ? "waiting" : "live";
  const tone = stale ? "warning" : running ? "active" : "default";
  setSummaryChip(dataSourceSummaryEl, `${modeLabel} · ${streamState}`, tone);
}

function updateToolbar() {
  if (toolbarPath) {
    toolbarPath.textContent = buildViewerSourceText(estimateToolbarChars());
  }
  updateSeriesSumUi();
  updateDataSourceSummary();
  syncToolbarMoreControls();
  updateViewerFooter();
  syncOverlayAnchors();
}

function setActiveMenu(menu, anchor) {
  activeMenu = menu;
  menuButtons.forEach((btn) => {
    btn.classList.toggle("is-active", btn.dataset.menu === menu);
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
  submenuParents.forEach((parent) => parent.classList.remove("is-open"));
}

function openMenu(menu, anchor) {
  if (!dropdown) return;
  closeToolbarPlaybackPopover();
  closeToolbarMorePopover();
  closeSubmenus();
  dropdown.classList.add("is-open");
  dropdown.setAttribute("aria-hidden", "false");
  setActiveMenu(menu, anchor);
}

function closeMenu() {
  if (!dropdown) return;
  closeSubmenus();
  dropdown.classList.remove("is-open");
  dropdown.setAttribute("aria-hidden", "true");
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

function dirnameFromPath(path) {
  if (!path) return "";
  const normalized = path.replace(/\\/g, "/");
  const idx = normalized.lastIndexOf("/");
  if (idx <= 0) {
    return normalized.startsWith("/") ? "/" : "";
  }
  return normalized.slice(0, idx);
}

function syncSeriesSumOutputPath(force = false) {
  seriesSumController.syncSeriesSumOutputPath(force);
}

async function openSeriesSumOutputTarget() {
  await seriesSumController.openSeriesSumOutputTarget();
}

const fileOpenController = createFileOpenController({
  apiBase: API,
  state,
  getBackendIsLocal: () => backendIsLocal,
  elements: {
    autoloadDir,
    fileSelect,
    fileInput,
    filesystemMode,
  },
  callbacks: {
    dirnameFromPath,
    syncSeriesSumOutputPath,
    loadFiles,
    option,
    fileLabel: (...args) => fileLabel(...args),
    isHdfFile,
    loadDatasets,
    loadImageSeries,
    closeMenu,
    ensureFileMode,
    setStatus,
    openFileDialog: (...args) => openFileDialog(...args),
  },
});

async function openPathInViewer(path, options = {}) {
  await fileOpenController.openPathInViewer(path, options);
}

const uploadFlowController = createUploadFlowController({
  apiBase: API,
  state,
  elements: {
    autoloadDir,
    fileInput,
    uploadBar,
    uploadBarFill,
    uploadBarText,
  },
  callbacks: {
    ensureFileMode,
    setLoading,
    setStatus,
    loadFiles,
    openPathInViewer: (...args) => openPathInViewer(...args),
    fileLabel: (...args) => fileLabel(...args),
    fetchJSON,
  },
});

async function openFileModal() {
  await fileOpenController.openFileModal();
}

async function uploadAndOpenSelectedFiles(selectedFiles) {
  await uploadFlowController.uploadAndOpenSelectedFiles(selectedFiles);
}

function hideUploadProgress() {
  uploadFlowController.hideUploadProgress();
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
      scheduleOverview();
      scheduleHistogram();
      schedulePixelOverlay();
      scheduleResolutionOverlay();
      if (activeTabId === "analysis") {
        // Defer ROI refresh so projection canvases are visible and sized after tab switch.
        window.requestAnimationFrame(() => {
          scheduleRoiOverlay();
          scheduleRoiUpdate();
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

function formatTimeStamp(ts) {
  if (!ts) return "";
  return new Date(ts).toLocaleTimeString();
}

function setAutoloadStatus(text, markUpdate = false) {
  if (text) {
    setStatus(String(text));
  }
  if (markUpdate) {
    state.autoload.lastUpdate = Date.now();
  }
  updateAutoloadMeta();
  updateLiveBadge();
}

function setAutoloadLatest() {
  updateAutoloadMeta();
}

function updateAutoloadMeta() {
  if (autoloadStatus) {
    autoloadStatus.textContent = state.autoload.lastPoll
      ? formatTimeStamp(state.autoload.lastPoll)
      : "-";
  }
  if (autoloadLatest) {
    autoloadLatest.textContent = state.autoload.lastUpdate
      ? formatTimeStamp(state.autoload.lastUpdate)
      : "-";
  }
  updateToolbar();
}

const backendStatusController = createBackendStatusController({
  apiBase: API,
  state,
  elements: {
    liveBadge,
    backendBadge,
    aboutVersion,
  },
  callbacks: {
    updateFooterVersions,
    updateSplashCallToAction,
    setSplashStatus,
  },
});

function updateLiveBadge() {
  backendStatusController.updateLiveBadge();
}

function updateAboutVersion() {
  backendStatusController.updateAboutVersion();
}

function startBackendHeartbeat() {
  backendStatusController.startBackendHeartbeat();
}

async function waitForBackendReady(timeoutMs = 20000) {
  return backendStatusController.waitForBackendReady(timeoutMs);
}


async function fetchSettingsConfig() {
  try {
    const payload = await fetchJSON(`${API}/settings`);
    const config = payload?.config || {};
    applyUiSettings(config?.ui);
    schedulePixelOverlay();
  } catch (err) {
    console.warn("Settings fetch failed", err);
  }
}

async function bootstrapApp() {
  showSplash();
  drawSplash();
  setSplashStatus("Starting backend...");
  await waitForBackendReady();
  setSplashStatus("Loading settings...");
  await fetchSettingsConfig();
  setSplashStatus("Loading file list...");
  await loadAutoloadFolders();
  await loadFiles();
  setSplashStatus("Ready. Open a file to begin.");
}

async function loadAutoloadFolders() {
  if (!autoloadDirList) return;
  try {
    const data = await fetchJSON(`${API}/folders`);
    const folders = Array.isArray(data.folders) ? data.folders : [];
    const current = state.autoload.dir || autoloadDir.value || "";
    autoloadDirList.innerHTML = "";
    autoloadDirList.appendChild(option(".", ""));
    folders.forEach((name) => autoloadDirList.appendChild(option(name, name)));
    if (current && !folders.includes(current)) {
      autoloadDirList.appendChild(option(current, current));
    }
  } catch (err) {
    console.error(err);
  }
}

const autoloadSettingsController = createAutoloadSettingsController({
  state,
  elements: {
    autoloadMode,
    autoloadFolder,
    autoloadWatch,
    autoloadWatchEnabled,
    autoloadWatchOptions,
    autoloadTypesRow,
    autoloadSimplon,
    autoloadRemote,
    autoloadJfjoch,
    fileField,
    datasetField,
    thresholdField,
    toolbarFrameWrap,
    toolbarFrameIndexWrap,
    toolbarStepWrap,
    toolbarFpsWrap,
    toolbarPlaybackWrap,
    autoloadStatus,
    simplonMetaPanel,
    remoteMetaPanel,
    jfjochMetaPanel,
    autoloadDir,
    autoloadInterval,
    autoloadTypeHdf5,
    autoloadTypeTiff,
    autoloadTypeCbf,
    autoloadPattern,
    simplonUrl,
    simplonVersion,
    simplonTimeout,
    simplonEnable,
    remoteSourceInput,
    remoteIntervalInput,
    jfjochEndpointInput,
    jfjochSourceInput,
    jfjochTopicInput,
    jfjochChannelInput,
    jfjochIntervalInput,
  },
  callbacks: {
    closeToolbarPlaybackPopover,
    updateSimplonMetaUI,
    updateRemoteMetaUI,
    updateJfjochMetaUI,
    updateAutoloadMeta,
    updateLiveBadge,
    updateThresholdOptions,
    updateDataSourceSummary,
    setDataSourceSectionState,
    setAutoloadStatus,
    setAutoloadLatest,
    startAutoload: (...args) => startAutoload(...args),
  },
});

function persistAutoloadSettings() {
  autoloadSettingsController.persistAutoloadSettings();
}

function updateAutoloadUI() {
  autoloadSettingsController.updateAutoloadUI();
}

function loadAutoloadSettings() {
  autoloadSettingsController.loadAutoloadSettings();
}

async function setSimplonMode(enabled) {
  if (!simplonUrl || !simplonVersion) return;
  const url = simplonUrl.value.trim();
  if (!url) return;
  const version = simplonVersion.value.trim() || "1.8.0";
  const mode = enabled ? "enabled" : "disabled";
  try {
    await fetch(
      `${API}/simplon/mode?url=${encodeURIComponent(url)}&version=${encodeURIComponent(
        version
      )}&mode=${mode}`,
      { method: "POST" }
    );
  } catch (err) {
    console.error(err);
  }
}

const autoloadOrchestrationController = createAutoloadOrchestrationController({
  state,
  analysisState,
  elements: {
    autoloadMode,
    autoloadWatchEnabled,
    autoloadDir,
    autoloadInterval,
    autoloadTypeHdf5,
    autoloadTypeTiff,
    autoloadTypeCbf,
    autoloadPattern,
    simplonUrl,
    simplonVersion,
    simplonTimeout,
    simplonEnable,
    remoteSourceInput,
    remoteIntervalInput,
    jfjochEndpointInput,
    jfjochSourceInput,
    jfjochTopicInput,
    jfjochChannelInput,
    jfjochIntervalInput,
  },
  callbacks: {
    updateAutoloadUI,
    updateAutoloadMeta,
    setAutoloadStatus,
    setStatus,
    persistAutoloadSettings,
    setSimplonMode,
    fetchSimplonMask,
    updateLiveBadge,
    stopJfjochPreviewBridge: (...args) => stopJfjochPreviewBridge(...args),
    updateRemoteMetaUI,
    updateJfjochMetaUI,
    schedulePeakOverlay,
    autoloadWatchTick: (...args) => autoloadWatchTick(...args),
    autoloadSimplonTick: (...args) => autoloadSimplonTick(...args),
    autoloadJfjochTick: (...args) => autoloadJfjochTick(...args),
    autoloadRemoteTick: (...args) => autoloadRemoteTick(...args),
  },
});

async function startAutoload() {
  await autoloadOrchestrationController.startAutoload();
}

async function stopAutoload(options = {}) {
  await autoloadOrchestrationController.stopAutoload(options);
}

async function ensureFileMode() {
  await autoloadOrchestrationController.ensureFileMode();
}

async function autoloadTick() {
  await autoloadOrchestrationController.autoloadTick();
}

const autoloadModeController = createAutoloadModeController({
  apiBase: API,
  state,
  callbacks: {
    setAutoloadStatus,
    setAutoloadLatest,
    updateAutoloadMeta,
    loadAutoloadFile: (...args) => loadAutoloadFile(...args),
    fetchSimplonMask,
    parseDtype,
    parseShape,
    typedArrayFrom,
    hashBufferSample,
    applySimplonMeta,
    logClient,
    formatSimplonTimestamp,
    applyExternalFrame,
    updateLiveBadge,
  },
});

async function autoloadWatchTick() {
  await autoloadModeController.autoloadWatchTick();
}

async function autoloadSimplonTick() {
  await autoloadModeController.autoloadSimplonTick();
}

const jfjochBridgeController = createJfjochBridgeController({
  apiBase: API,
  state,
  callbacks: {
    setAutoloadStatus,
    updateJfjochMetaUI,
  },
});

async function startJfjochPreviewBridge() {
  return jfjochBridgeController.startJfjochPreviewBridge();
}

async function stopJfjochPreviewBridge() {
  return jfjochBridgeController.stopJfjochPreviewBridge();
}

async function fetchJfjochPreviewStatus() {
  return jfjochBridgeController.fetchJfjochPreviewStatus();
}

const remoteStreamController = createRemoteStreamController({
  apiBase: API,
  state,
  analysisState,
  callbacks: {
    setAutoloadStatus,
    updateLiveBadge,
    updateAutoloadMeta,
    schedulePeakOverlay,
    updateRemoteMetaUI,
    updateJfjochMetaUI,
    startJfjochPreviewBridge: (...args) => startJfjochPreviewBridge(...args),
    fetchJfjochPreviewStatus: (...args) => fetchJfjochPreviewStatus(...args),
    parseDtype,
    parseShape,
    typedArrayFrom,
    applyRemoteMeta,
    applyExternalFrame,
  },
});

async function autoloadJfjochTick() {
  await remoteStreamController.autoloadJfjochTick();
}

async function autoloadRemoteTick() {
  await remoteStreamController.autoloadRemoteTick();
}

const fileDataPipelineController = createFileDataPipelineController({
  apiBase: API,
  state,
  elements: {
    fileSelect,
    datasetSelect,
    metaShape,
    metaDtype,
  },
  callbacks: {
    fetchJSON,
    option,
    fileLabel,
    isSeriesCapable,
    isHdfFile,
    setDataControlsForHdf5,
    setDataControlsForSeries,
    loadMetadata: (...args) => loadMetadata(...args),
    loadInspectorRoot,
    updateFrameControls,
    updatePlayButtons,
    requestFrame,
    parseDtype,
    parseShape,
    typedArrayFrom,
    applyImageMeta,
    applyExternalFrame,
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
    stopPlayback,
    loadMask,
    updateToolbar,
    getActiveFrameLoadController: () => activeFrameLoadController,
    setActiveFrameLoadController: (controller) => {
      activeFrameLoadController = controller;
    },
  },
});

async function loadAutoloadFile(file) {
  await fileDataPipelineController.loadAutoloadFile(file);
}

async function loadImageSeries(file) {
  await fileDataPipelineController.loadImageSeries(file);
}

function applyExternalFrame(data, shape, dtype, label, fitView, preserveMask = false, options = {}) {
  if (!Array.isArray(shape) || shape.length < 2) return;
  const keepPlaying = Boolean(options.keepPlaying);
  if (!(keepPlaying && state.playing)) {
    stopPlayback();
  }
  const preserveSeries = Boolean(options.preserveSeries);
  if (fitView) {
    state.hasFrame = false;
  }
  if (!preserveSeries) {
    state.file = label;
    state.dataset = "";
    state.seriesFiles = [];
    state.seriesLabel = "";
    state.frameCount = 1;
    state.frameIndex = 0;
    state.thresholdCount = 1;
    state.thresholdIndex = 0;
    state.thresholdEnergies = [];
    updateFrameControls();
    updateThresholdOptions();
    datasetSelect.innerHTML = "";
    datasetSelect.appendChild(option("Single image", ""));
    datasetSelect.value = "";
    setDataControlsForImage();
  } else {
    state.dataset = "";
    state.thresholdCount = 1;
    state.thresholdIndex = 0;
    state.thresholdEnergies = [];
    updateFrameControls();
    updateThresholdOptions();
    datasetSelect.innerHTML = "";
    datasetSelect.appendChild(option("Series image", ""));
    datasetSelect.value = "";
    setDataControlsForSeries();
  }
  const height = shape[0];
  const width = shape[1];
  if (!preserveMask) {
    clearMaskState();
  }
  if (options.autoMask) {
    const autoMask = buildNegativeMask(data);
    if (autoMask) {
      state.maskRaw = autoMask;
      state.maskShape = [height, width];
      state.maskAuto = true;
      state.maskFile = options.maskKey || `auto:${label}`;
      updateMaskUI();
    }
  }
  metaShape.textContent = `${width} × ${height}`;
  metaDtype.textContent = dtype;
  applyFrame(data, width, height, dtype);
  setDataSourceSectionState("active", preserveSeries ? "Series image loaded." : "Image loaded.");
  updateToolbar();
}

async function fetchSimplonMask() {
  if (!simplonUrl) return;
  const baseUrl = simplonUrl.value.trim();
  if (!baseUrl) return;
  const version = simplonVersion?.value?.trim() || "1.8.0";
  try {
    const res = await fetch(
      `${API}/simplon/mask?url=${encodeURIComponent(baseUrl)}&version=${encodeURIComponent(version)}`
    );
    if (res.status === 204) {
      return;
    }
    if (!res.ok) {
      setAutoloadStatus("SIMPLON: mask unavailable");
      return;
    }
    const buffer = await res.arrayBuffer();
    const dtype = parseDtype(res.headers.get("X-Dtype"));
    const shape = parseShape(res.headers.get("X-Shape"));
    const data = typedArrayFrom(buffer, dtype);
    state.maskRaw = normalizeMaskData(data);
    state.maskShape = shape;
    state.maskAuto = true;
    state.maskFile = "__simplon__";
    alignMaskToFrame();
    syncMaskAvailability(true);
    if (state.hasFrame) {
      updateGlobalStats();
      redraw();
      scheduleRoiUpdate();
    }
  } catch (err) {
    console.error(err);
    setAutoloadStatus("SIMPLON: mask failed");
  }
}

function downloadCanvasImage(sourceCanvas, filename) {
  sourceCanvas.toBlob((blob) => {
    if (!blob) return;
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = filename;
    a.click();
    URL.revokeObjectURL(a.href);
  });
}

function renderRegionToCanvas(region) {
  if (!state.dataRaw || !region) return null;
  const { x, y, width, height } = region;
  if (width <= 0 || height <= 0) return null;
  const outCanvas = document.createElement("canvas");
  outCanvas.width = width;
  outCanvas.height = height;
  const ctx = outCanvas.getContext("2d");
  if (!ctx) return null;

  const imageData = ctx.createImageData(width, height);
  const out = imageData.data;
  const palette = buildPalette(state.colormap);
  const maxIdx = getPaletteColorCount(palette) - 1;
  const maskReady =
    state.maskEnabled &&
    state.maskAvailable &&
    state.maskRaw &&
    state.maskShape &&
    state.maskShape[0] === state.height &&
    state.maskShape[1] === state.width;
  const maskData = maskReady ? state.maskRaw : null;

  for (let row = 0; row < height; row += 1) {
    const imgY = y + row;
    const rowOffset = imgY * state.width;
    const outOffset = row * width * 4;
    for (let col = 0; col < width; col += 1) {
      const imgX = x + col;
      const idx = rowOffset + imgX;
      const v = state.dataRaw[idx];
      if (maskReady && maskData) {
        const maskValue = maskData[idx];
        if (maskValue & 1) {
          const j = outOffset + col * 4;
          out[j] = 0;
          out[j + 1] = 0;
          out[j + 2] = 0;
          out[j + 3] = 255;
          continue;
        } else if (maskValue & 0x1e) {
          const j = outOffset + col * 4;
          out[j] = 25;
          out[j + 1] = 50;
          out[j + 2] = 120;
          out[j + 3] = 255;
          continue;
        }
      }
      const norm = mapValueToNorm(v);
      const p = Math.floor(norm * maxIdx) * 4;
      const j = outOffset + col * 4;
      out[j] = palette[p];
      out[j + 1] = palette[p + 1];
      out[j + 2] = palette[p + 2];
      out[j + 3] = 255;
    }
  }
  ctx.putImageData(imageData, 0, 0);
  return outCanvas;
}

function getVisibleRegion() {
  if (!canvasWrap || !state.width || !state.height) return null;
  const zoom = state.zoom || 1;
  const viewX = getEffectiveScrollLeft() / zoom;
  const viewY = getEffectiveScrollTop() / zoom;
  const viewW = canvasWrap.clientWidth / zoom;
  const viewH = canvasWrap.clientHeight / zoom;
  let startX = Math.floor(viewX);
  let startY = Math.floor(viewY);
  let endX = Math.ceil(viewX + viewW);
  let endY = Math.ceil(viewY + viewH);
  startX = Math.max(0, startX);
  startY = Math.max(0, startY);
  endX = Math.min(state.width, endX);
  endY = Math.min(state.height, endY);
  const width = Math.max(0, endX - startX);
  const height = Math.max(0, endY - startY);
  return { x: startX, y: startY, width, height };
}

function exportFullImage(filenameOverride) {
  if (!state.dataRaw) return;
  const full = renderRegionToCanvas({ x: 0, y: 0, width: state.width, height: state.height });
  if (!full) return;
  const name = filenameOverride || `frame_${state.frameIndex}_full.png`;
  downloadCanvasImage(full, name);
}

function exportVisibleArea(filenameOverride) {
  const region = getVisibleRegion();
  if (!region) return;
  const image = renderRegionToCanvas(region);
  if (!image) return;
  const name = filenameOverride || `frame_${state.frameIndex}_view.png`;
  downloadCanvasImage(image, name);
}

function ensureHtml2Canvas() {
  if (typeof window.html2canvas === "function") {
    return Promise.resolve(window.html2canvas);
  }
  if (html2canvasLoadPromise) {
    return html2canvasLoadPromise;
  }
  html2canvasLoadPromise = new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.src = "vendor/html2canvas.min.js";
    script.async = true;
    script.onload = () => {
      if (typeof window.html2canvas === "function") {
        resolve(window.html2canvas);
      } else {
        reject(new Error("html2canvas loaded without global export"));
      }
    };
    script.onerror = () => {
      reject(new Error("Failed to load html2canvas"));
    };
    document.head.appendChild(script);
  }).catch((err) => {
    html2canvasLoadPromise = null;
    throw err;
  });
  return html2canvasLoadPromise;
}

async function exportViewerWindow(filenameOverride) {

  let html2canvasFn;
  try {
    html2canvasFn = await ensureHtml2Canvas();
  } catch (err) {
    console.error(err);
    setStatus("Viewer export unavailable");
    return;
  }
  const target = document.querySelector(".page");
  if (!target) return;
  try {
    const shot = await html2canvasFn(target, {
      backgroundColor: null,
      scale: window.devicePixelRatio || 1,
      useCORS: true,
    });
    const name = filenameOverride || `albis_view_${state.frameIndex + 1}.png`;
    downloadCanvasImage(shot, name);
  } catch (err) {
    console.error(err);
    setStatus("Viewer export failed");
  }
}

const settingsController = createSettingsController({
  apiBase: API,
  state,
  constants: {
    pixelLabelDefaultMinCellPx: PIXEL_LABEL_DEFAULT_MIN_CELL_PX,
    pixelLabelDefaultMaxLabels: PIXEL_LABEL_DEFAULT_MAX_LABELS,
  },
  elements: {
    settingsModal,
    settingsClose,
    settingsSave,
    settingsSaveClose,
    settingsConfigPath,
    settingsMessage,
    settingsServerExternal,
    settingsServerPort,
    settingsServerReload,
    settingsStartupTimeout,
    settingsOpenBrowser,
    settingsToolHints,
    settingsPixelLabelMin,
    settingsPixelLabelMax,
    settingsPixelLabelFormat,
    settingsPixelLabelDrag,
    settingsDataRoot,
    settingsAllowAbs,
    settingsScanCache,
    settingsMaxScanDepth,
    settingsMaxUpload,
    settingsLogLevel,
    settingsLogDir,
  },
  callbacks: {
    setToolHintsEnabled,
    openModal: (...args) => openModal(...args),
    closeModal: (...args) => closeModal(...args),
    closeMenu,
    setStatus,
    schedulePixelOverlay,
  },
});

function applyUiSettings(uiConfig) {
  settingsController.applyUiSettings(uiConfig);
}

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

function closeTopModal(options = {}) {
  const modalEl = getTopOpenModal();
  if (!modalEl) return false;
  if (modalEl === commandModal) {
    closeCommandPalette(options);
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

function closeSettingsModal({ restoreFocus = true } = {}) {
  settingsController.closeSettingsModal({ restoreFocus });
}

async function openSettingsModal() {
  await settingsController.openSettingsModal();
}

async function saveSettingsFromModal(closeAfter = false) {
  await settingsController.saveSettingsFromModal(closeAfter);
}

function isCommandPaletteOpen() {
  return Boolean(commandModal?.classList.contains("is-open"));
}

function getCommandPaletteCommands() {
  return buildCommandPaletteCommands({
    state,
    panelTabState,
    platformShortcutLabel,
    isHdfFile,
    getThresholdIndexAtOffset,
    actions: {
      openFileModal,
      closeCurrentFile,
      openSettingsModal,
      stopPlayback,
      startPlayback,
      requestFrame,
      setThresholdIndex,
      fitImageToView,
      exportFullImage,
      exportVisibleArea,
      exportViewerWindow,
      startSeriesSumming,
      openSeriesSumOutputTarget,
      cancelSeriesSumming,
      toggleFullscreen,
      togglePanel,
      setPanelTab,
      handleMenuAction,
    },
  });
}

const commandPaletteController = createCommandPaletteController({
  elements: {
    commandModal,
    commandInput,
    commandList,
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

function renderCommandPalette() {
  commandPaletteController.render();
}

function openCommandPalette(prefill = "") {
  commandPaletteController.open(prefill);
}

function closeCommandPalette(options = {}) {
  commandPaletteController.close(options);
}

function handleCommandPaletteKeydown(event) {
  return commandPaletteController.handleKeydown(event);
}

const menuActionHandler = createMenuActionHandler({
  apiBase: API,
  state,
  callbacks: {
    setStatus,
    openSettingsModal,
    openCommandPalette,
    toggleFullscreen,
    openAboutModal,
    openFileModal,
    closeCurrentFile,
    exportFullImage,
    exportVisibleArea,
    exportViewerWindow,
  },
});

async function handleMenuAction(action) {
  await menuActionHandler(action);
}

const { handleShortcut, handleNavShortcut } = createShortcutHandlers({
  state,
  elements: {
    thresholdSelect,
    toolbarThresholdSelect,
    toolbarMoreThreshold,
  },
  callbacks: {
    handleMenuAction,
    isCommandPaletteOpen,
    closeCommandPalette,
    openCommandPalette,
    openFileModal,
    closeCurrentFile,
    stopPlayback,
    startPlayback,
    setThresholdIndex,
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

function isHdfFile(path) {
  if (!path) return false;
  const lower = String(path).toLowerCase();
  return lower.endsWith(".h5") || lower.endsWith(".hdf5");
}

function isSeriesCapable(path) {
  if (!path) return false;
  const lower = String(path).toLowerCase();
  if (isHdfFile(lower)) return false;
  return (
    lower.endsWith(".cbf") ||
    lower.endsWith(".cbf.gz") ||
    lower.endsWith(".edf") ||
    lower.endsWith(".tif") ||
    lower.endsWith(".tiff")
  );
}

function fileLabel(path) {
  if (!path) return "";
  const parts = path.split(/[/\\\\]/);
  return parts[parts.length - 1] || path;
}

function hashBufferSample(buffer) {
  if (!buffer) return "";
  const bytes = new Uint8Array(buffer);
  const len = bytes.length;
  if (!len) return "0";
  const stride = Math.max(1, Math.floor(len / 2048));
  let hash = 2166136261;
  for (let i = 0; i < len; i += stride) {
    hash ^= bytes[i];
    hash = (hash * 16777619) >>> 0;
  }
  return `${len}-${hash}`;
}

let renderEngineController = null;

function initRenderer() {
  renderEngineController.initRenderer();
}

async function loadFiles() {
  setDataControlsForHdf5();
  setDataSourceSectionState("loading", "Loading files…", true);
  const folder = (autoloadDir?.value || state.autoload.dir || "").trim();
  const url = folder ? `${API}/files?folder=${encodeURIComponent(folder)}` : `${API}/files`;
  try {
    const data = await fetchJSON(url);
    fileSelect.innerHTML = "";
    const existingFile = state.file;
    if (data.files.length > 0) {
      const placeholder = option("Select file…", "");
      placeholder.disabled = true;
      placeholder.selected = true;
      fileSelect.appendChild(placeholder);
      data.files.forEach((name) => fileSelect.appendChild(option(fileLabel(name), name)));
      if (existingFile) {
        const hasExisting = data.files.includes(existingFile);
        if (!hasExisting) {
          fileSelect.appendChild(option(fileLabel(existingFile), existingFile));
        }
        fileSelect.value = existingFile;
        setDataSourceSectionState("active", "File list loaded.");
      } else {
        state.file = "";
        state.dataset = "";
        setStatus("Select a file to begin");
        updateToolbar();
        showSplash();
        setSplashStatus("Ready. Open a file to begin.");
        setLoading(false);
        setDataSourceSectionState("empty", "Select a file to begin.");
      }
      loadAutoloadFolders();
    } else {
      data.files.forEach((name) => fileSelect.appendChild(option(fileLabel(name), name)));
      if (!existingFile) {
        setStatus("No image files found");
        showSplash();
        setSplashStatus("No image files found. Open a file to begin.");
        setLoading(false);
      }
      setDataSourceSectionState("warning", "No image files found in this folder.");
      loadAutoloadFolders();
    }
  } catch (err) {
    console.error(err);
    setStatus("Failed to load files");
    setDataSourceSectionState("warning", "Failed to load file list.");
  }
}

async function loadDatasets() {
  await fileDataPipelineController.loadDatasets();
}

async function loadMetadata() {
  if (!state.file || !state.dataset) return;
  showProcessingProgress("Loading metadata…");
  setStatus("Loading metadata…");
  setDataSourceSectionState("loading", "Loading dataset metadata…", true);
  try {
    state.maskAuto = true;
    const data = await fetchJSON(
      `${API}/metadata?file=${encodeURIComponent(state.file)}&dataset=${encodeURIComponent(state.dataset)}`
    );
    state.shape = data.shape;
    state.dtype = data.dtype;
    if (data.shape.length === 4) {
      state.frameCount = data.shape[0];
      state.thresholdCount = data.shape[1];
      state.thresholdEnergies = Array.isArray(data.threshold_energies) ? data.threshold_energies : [];
    } else {
      state.frameCount = data.shape.length === 3 ? data.shape[0] : 1;
      state.thresholdCount = 1;
      state.thresholdEnergies = [];
    }
    state.thresholdIndex = getDefaultThresholdIndex();
    state.frameIndex = 0;
    syncSeriesSumOutputPath();
    updateFrameControls();
    updateThresholdOptions();
    metaShape.textContent = data.shape.join(" × ");
    metaDtype.textContent = data.dtype;
    updateToolbar();
    await loadAnalysisParams();
    await loadMask(true);
    await loadFrame();
    setDataSourceSectionState("active", "Metadata ready.");
  } catch (err) {
    console.error(err);
    setDataSourceSectionState("warning", "Failed to load metadata.");
    throw err;
  } finally {
    hideProcessingProgress();
  }
}

async function loadAnalysisParams() {
  if (!state.file || !isHdf5File(state.file)) {
    return;
  }
  try {
    const data = await fetchJSON(
      `${API}/analysis/params?file=${encodeURIComponent(state.file)}&dataset=${encodeURIComponent(
        state.dataset || ""
      )}`
    );
    if (Number.isFinite(data.distance_mm) && ringsDistance) {
      analysisState.distanceMm = data.distance_mm;
      ringsDistance.value = String(Math.round(data.distance_mm));
    }
    if (Number.isFinite(data.pixel_size_um) && ringsPixel) {
      analysisState.pixelSizeUm = data.pixel_size_um;
      ringsPixel.value = data.pixel_size_um.toFixed(2);
    }
    if (Number.isFinite(data.energy_ev) && ringsEnergy) {
      analysisState.energyEv = data.energy_ev;
      ringsEnergy.value = String(Math.round(data.energy_ev));
    }
    const fallback = getDefaultCenter();
    const centerX = Number.isFinite(data.center_x_px) ? data.center_x_px : fallback.x;
    const centerY = Number.isFinite(data.center_y_px) ? data.center_y_px : fallback.y;
    analysisState.centerX = centerX;
    analysisState.centerY = centerY;
    if (ringsCenterX) ringsCenterX.value = Math.round(centerX).toString();
    if (ringsCenterY) ringsCenterY.value = Math.round(centerY).toString();
    if (ringInputs.length && ringInputs.every((input) => !input.value)) {
      ringInputs.forEach((input, idx) => {
        const value = analysisState.rings[idx] ?? "";
        if (value) {
          input.value = String(value);
        }
      });
    }
    scheduleResolutionOverlay();
  } catch (err) {
    console.error(err);
  }
}

function parseShape(header) {
  if (!header) return [];
  return header.split(",").map((v) => parseInt(v, 10));
}

function parseHeaderFloat(headers, key) {
  if (!headers) return null;
  const raw = headers.get(key);
  if (raw === null || raw === undefined || raw === "") return null;
  const value = Number(raw);
  return Number.isFinite(value) ? value : null;
}

function parseSimplonTimestamp(raw) {
  if (!raw) return null;
  const cleaned = String(raw).replace(/\.(\d{3})\d+Z$/, ".$1Z");
  const parsed = new Date(cleaned);
  if (!Number.isNaN(parsed.valueOf())) return parsed;
  return null;
}

function formatSimplonTimestamp(raw) {
  const parsed = parseSimplonTimestamp(raw);
  if (parsed) {
    return parsed.toLocaleString();
  }
  return raw ? String(raw) : "";
}

function formatSimplonValue(value, digits = 2) {
  if (value === null || value === undefined || value === "") return "-";
  if (typeof value === "number" && Number.isFinite(value)) {
    if (Number.isInteger(value)) return String(value);
    return value.toFixed(digits);
  }
  return String(value);
}

function updateSimplonMetaUI(meta) {
  if (!simplonMetaPanel) return;
  if (state.autoload.mode !== "simplon") {
    simplonMetaPanel.classList.add("is-hidden");
    return;
  }
  simplonMetaPanel.classList.remove("is-hidden");
  if (simplonSeriesEl) simplonSeriesEl.textContent = formatSimplonValue(meta.series);
  if (simplonImageEl) simplonImageEl.textContent = formatSimplonValue(meta.image);
  if (simplonTimeEl) simplonTimeEl.textContent = formatSimplonTimestamp(meta.date) || "-";
  if (simplonEnergyEl) simplonEnergyEl.textContent = formatSimplonValue(meta.energyEv, 1);
  if (simplonThresholdEl) simplonThresholdEl.textContent = formatSimplonValue(meta.thresholdEv, 1);
  if (simplonWavelengthEl) simplonWavelengthEl.textContent = formatSimplonValue(meta.wavelengthA, 4);
  if (simplonDistanceEl) simplonDistanceEl.textContent = formatSimplonValue(meta.distanceMm, 2);
  if (simplonCenterEl) {
    if (Number.isFinite(meta.centerX) && Number.isFinite(meta.centerY)) {
      simplonCenterEl.textContent = `${Math.round(meta.centerX)}, ${Math.round(meta.centerY)}`;
    } else {
      simplonCenterEl.textContent = "-";
    }
  }
}

function updateRemoteMetaUI(meta) {
  if (!remoteMetaPanel) return;
  if (state.autoload.mode !== "remote") {
    remoteMetaPanel.classList.add("is-hidden");
    return;
  }
  remoteMetaPanel.classList.remove("is-hidden");
  if (remoteSourceEl) remoteSourceEl.textContent = formatSimplonValue(meta.source);
  if (remoteSeqEl) remoteSeqEl.textContent = formatSimplonValue(meta.seq);
  if (remoteSeriesEl) remoteSeriesEl.textContent = formatSimplonValue(meta.series);
  if (remoteImageEl) remoteImageEl.textContent = formatSimplonValue(meta.image);
  if (remoteTimeEl) remoteTimeEl.textContent = formatSimplonTimestamp(meta.date) || "-";
  if (remoteEnergyEl) remoteEnergyEl.textContent = formatSimplonValue(meta.energyEv, 1);
  if (remoteWavelengthEl) remoteWavelengthEl.textContent = formatSimplonValue(meta.wavelengthA, 4);
  if (remoteDistanceEl) remoteDistanceEl.textContent = formatSimplonValue(meta.distanceMm, 2);
  if (remoteCenterEl) {
    if (Number.isFinite(meta.centerX) && Number.isFinite(meta.centerY)) {
      remoteCenterEl.textContent = `${Math.round(meta.centerX)}, ${Math.round(meta.centerY)}`;
    } else {
      remoteCenterEl.textContent = "-";
    }
  }
  if (remotePeakSetsEl) {
    const count = Number(meta.peakSets || 0);
    remotePeakSetsEl.textContent = Number.isFinite(count) ? String(Math.max(0, Math.round(count))) : "-";
  }
}

function updateJfjochMetaUI(meta, status = {}) {
  if (!jfjochMetaPanel) return;
  if (state.autoload.mode !== "jungfraujoch") {
    jfjochMetaPanel.classList.add("is-hidden");
    return;
  }
  jfjochMetaPanel.classList.remove("is-hidden");
  if (jfjochSourceEl) jfjochSourceEl.textContent = formatSimplonValue(meta.source || state.autoload.jfjochSourceId);
  if (jfjochSeqEl) jfjochSeqEl.textContent = formatSimplonValue(meta.seq);
  if (jfjochSeriesEl) jfjochSeriesEl.textContent = formatSimplonValue(meta.series);
  if (jfjochImageEl) jfjochImageEl.textContent = formatSimplonValue(meta.image);
  if (jfjochTimeEl) jfjochTimeEl.textContent = formatSimplonTimestamp(meta.date) || "-";
  if (jfjochReflectionsEl) {
    const count = Number(meta.reflections ?? meta.peakSets ?? 0);
    jfjochReflectionsEl.textContent = Number.isFinite(count)
      ? String(Math.max(0, Math.round(count)))
      : "-";
  }
  if (jfjochChannelMetaEl) jfjochChannelMetaEl.textContent = formatSimplonValue(meta.channel || "-");
  if (jfjochBridgeStatusEl) {
    if (status?.last_error) {
      jfjochBridgeStatusEl.textContent = String(status.last_error);
    } else if (status?.running) {
      jfjochBridgeStatusEl.textContent = "running";
    } else {
      jfjochBridgeStatusEl.textContent = "stopped";
    }
  }
}

function applyImageMeta(headers) {
  if (!headers) return;
  const distanceMm = parseHeaderFloat(headers, "X-Image-DetectorDistance-MM");
  const pixelSizeUm = parseHeaderFloat(headers, "X-Image-PixelSize-UM");
  let energyEv = parseHeaderFloat(headers, "X-Image-Energy-Ev");
  const wavelengthA = parseHeaderFloat(headers, "X-Image-Wavelength-A");
  const centerX = parseHeaderFloat(headers, "X-Image-BeamCenter-X");
  const centerY = parseHeaderFloat(headers, "X-Image-BeamCenter-Y");
  if (!Number.isFinite(energyEv) && Number.isFinite(wavelengthA) && wavelengthA > 0) {
    energyEv = 12398.4193 / wavelengthA;
  }
  let updated = false;
  if (Number.isFinite(distanceMm) && ringsDistance) {
    analysisState.distanceMm = distanceMm;
    ringsDistance.value = String(Math.round(distanceMm));
    updated = true;
  }
  if (Number.isFinite(pixelSizeUm) && ringsPixel) {
    analysisState.pixelSizeUm = pixelSizeUm;
    ringsPixel.value = pixelSizeUm.toFixed(2);
    updated = true;
  }
  if (Number.isFinite(energyEv) && ringsEnergy) {
    analysisState.energyEv = energyEv;
    ringsEnergy.value = String(Math.round(energyEv));
    updated = true;
  }
  if (Number.isFinite(centerX) && ringsCenterX) {
    analysisState.centerX = centerX;
    ringsCenterX.value = Math.round(centerX).toString();
    updated = true;
  }
  if (Number.isFinite(centerY) && ringsCenterY) {
    analysisState.centerY = centerY;
    ringsCenterY.value = Math.round(centerY).toString();
    updated = true;
  }
  if (updated) {
    scheduleResolutionOverlay();
  }
}

function applySimplonMeta(headers) {
  if (!headers) return {};
  const distanceMm = parseHeaderFloat(headers, "X-Simplon-DetectorDistance-MM");
  const energyEv = parseHeaderFloat(headers, "X-Simplon-Energy-Ev");
  const thresholdEv = parseHeaderFloat(headers, "X-Simplon-Threshold-Ev");
  const wavelengthA = parseHeaderFloat(headers, "X-Simplon-Wavelength-A");
  const centerX = parseHeaderFloat(headers, "X-Simplon-BeamCenter-X");
  const centerY = parseHeaderFloat(headers, "X-Simplon-BeamCenter-Y");
  let updated = false;
  if (Number.isFinite(distanceMm) && ringsDistance) {
    analysisState.distanceMm = distanceMm;
    ringsDistance.value = String(Math.round(distanceMm));
    updated = true;
  }
  if (Number.isFinite(energyEv) && ringsEnergy) {
    analysisState.energyEv = energyEv;
    ringsEnergy.value = String(Math.round(energyEv));
    updated = true;
  }
  if (Number.isFinite(centerX) && ringsCenterX) {
    analysisState.centerX = centerX;
    ringsCenterX.value = Math.round(centerX).toString();
    updated = true;
  }
  if (Number.isFinite(centerY) && ringsCenterY) {
    analysisState.centerY = centerY;
    ringsCenterY.value = Math.round(centerY).toString();
    updated = true;
  }
  if (updated) {
    scheduleResolutionOverlay();
  }
  const meta = {
    series: headers.get("X-Simplon-Series") || "",
    image: headers.get("X-Simplon-Image") || "",
    date: headers.get("X-Simplon-Date") || "",
    energyEv,
    thresholdEv,
    wavelengthA,
    distanceMm,
    centerX,
    centerY,
  };
  state.autoload.simplonMeta = meta;
  updateSimplonMetaUI(meta);
  return meta;
}

function applyRemoteMeta(headers) {
  if (!headers) return {};
  const distanceMm = parseHeaderFloat(headers, "X-Remote-DetectorDistance-MM");
  const pixelSizeUm = parseHeaderFloat(headers, "X-Remote-PixelSize-UM");
  let energyEv = parseHeaderFloat(headers, "X-Remote-Energy-Ev");
  const wavelengthA = parseHeaderFloat(headers, "X-Remote-Wavelength-A");
  const centerX = parseHeaderFloat(headers, "X-Remote-BeamCenter-X");
  const centerY = parseHeaderFloat(headers, "X-Remote-BeamCenter-Y");
  if (!Number.isFinite(energyEv) && Number.isFinite(wavelengthA) && wavelengthA > 0) {
    energyEv = 12398.4193 / wavelengthA;
  }
  let updated = false;
  if (Number.isFinite(distanceMm) && ringsDistance) {
    analysisState.distanceMm = distanceMm;
    ringsDistance.value = String(Math.round(distanceMm));
    updated = true;
  }
  if (Number.isFinite(pixelSizeUm) && ringsPixel) {
    analysisState.pixelSizeUm = pixelSizeUm;
    ringsPixel.value = pixelSizeUm.toFixed(2);
    updated = true;
  }
  if (Number.isFinite(energyEv) && ringsEnergy) {
    analysisState.energyEv = energyEv;
    ringsEnergy.value = String(Math.round(energyEv));
    updated = true;
  }
  if (Number.isFinite(centerX) && ringsCenterX) {
    analysisState.centerX = centerX;
    ringsCenterX.value = Math.round(centerX).toString();
    updated = true;
  }
  if (Number.isFinite(centerY) && ringsCenterY) {
    analysisState.centerY = centerY;
    ringsCenterY.value = Math.round(centerY).toString();
    updated = true;
  }
  if (updated) {
    scheduleResolutionOverlay();
  }
  const meta = {
    source: headers.get("X-Remote-Source") || state.autoload.remoteSourceId || "",
    seq: Number(headers.get("X-Remote-Seq") || 0),
    displayName: headers.get("X-Remote-Display") || "",
    series: headers.get("X-Remote-Series") || "",
    image: headers.get("X-Remote-Image") || "",
    date: headers.get("X-Remote-Date") || "",
    energyEv,
    wavelengthA,
    distanceMm,
    centerX,
    centerY,
    peakSets: Number(headers.get("X-Remote-PeakSets") || 0),
  };
  state.autoload.remoteMeta = meta;
  state.autoload.remoteSeq = Number.isFinite(meta.seq) ? meta.seq : state.autoload.remoteSeq;
  updateRemoteMetaUI(meta);
  return meta;
}

function parseDtype(header) {
  return header || state.dtype;
}

function typedArrayFrom(buffer, dtype) {
  switch (dtype) {
    case "<u1":
    case "|u1":
      return new Uint8Array(buffer);
    case "<u2":
      return new Uint16Array(buffer);
    case "<u4":
      return new Uint32Array(buffer);
    case "<i2":
      return new Int16Array(buffer);
    case "<i4":
      return new Int32Array(buffer);
    case "<u8": {
      const in64 = new BigUint64Array(buffer);
      const out = new Float64Array(in64.length);
      for (let i = 0; i < in64.length; i += 1) {
        out[i] = Number(in64[i]);
      }
      return out;
    }
    case "<i8": {
      const in64 = new BigInt64Array(buffer);
      const out = new Float64Array(in64.length);
      for (let i = 0; i < in64.length; i += 1) {
        out[i] = Number(in64[i]);
      }
      return out;
    }
    case "<f4":
      return new Float32Array(buffer);
    case "<f8":
      return new Float64Array(buffer);
    default:
      return new Uint32Array(buffer);
  }
}

function toFloat32(data) {
  if (data instanceof Float32Array) return data;
  const out = new Float32Array(data.length);
  for (let i = 0; i < data.length; i += 1) {
    out[i] = data[i];
  }
  return out;
}

function getWebglUnsignedDtypeKey(dtype) {
  return getWebglUnsignedDtypeKeyUtil(dtype);
}

function isWebglUnsignedRawCandidate(dtype, data) {
  return isWebglUnsignedRawCandidateUtil(dtype, data);
}

function getWebglUnsignedUploadInfo(gl, key) {
  return getWebglUnsignedUploadInfoUtil(gl, key);
}

function getDtypeInfo(dtype) {
  return getDtypeInfoUtil(dtype);
}

function getActiveSaturationMax() {
  return Number.isFinite(state.stats?.satMax) ? state.stats.satMax : null;
}

function isSaturatedValue(value, satMax) {
  if (!Number.isFinite(value) || !Number.isFinite(satMax)) return false;
  const tolerance = Math.max(1e-9, Math.abs(satMax) * 1e-6);
  return Math.abs(value - satMax) <= tolerance;
}

function chooseHistogramBins(count) {
  return chooseHistogramBinsUtil(count);
}

function getPaletteColorCount(palette) {
  return getPaletteColorCountUtil(palette);
}

function computeHistogram(data, min, max, satMax, bins, logX) {
  return computeHistogramUtil(data, min, max, satMax, bins, logX);
}

function computeAutoLevels(data, satMaxInput) {
  return computeAutoLevelsUtil(data, satMaxInput, state.stats, state.dtype);
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

function buildPalette(name) {
  return buildPaletteUtil(name);
}

overviewViewportController = createOverviewViewportController({
  state,
  overviewState: overviewInteractionState,
  elements: {
    canvasWrap,
    canvas,
    overviewCanvas,
    overviewCtx,
    zoomRange,
    zoomValue,
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
    schedulePixelOverlay,
    scheduleRoiOverlay,
    scheduleResolutionOverlay,
    schedulePeakOverlay,
    requestFrame,
    cancelActiveFrameLoad,
    updateViewerFooter,
  },
});

renderEngineController = createRenderEngineController({
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
    scheduleOverview,
    scheduleHistogram,
    schedulePixelOverlay,
    schedulePeakOverlay,
    getRenderer: () => renderer,
    setRenderer: (nextRenderer) => {
      renderer = nextRenderer;
    },
  },
});

const overlayRenderController = createOverlayRenderController({
  state,
  analysisState,
  elements: {
    canvasWrap,
    pixelOverlay,
    pixelCtx,
    peakOverlay,
    peakCtx,
    resolutionOverlay,
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
    getRingParams,
    updateRingsSectionState,
  },
});

function deferPixelOverlayRedraw(delayMs = PIXEL_LABEL_INTERACTION_IDLE_MS) {
  overlayRenderController.deferPixelOverlayRedraw(delayMs);
}

function schedulePeakOverlay() {
  overlayRenderController.schedulePeakOverlay();
}

function scheduleResolutionOverlay() {
  overlayRenderController.scheduleResolutionOverlay();
}

function schedulePixelOverlay() {
  overlayRenderController.schedulePixelOverlay();
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

function histogramValueToX(value, width) {
  return histogramRenderController.histogramValueToX(value, width);
}

function histogramXToValue(x, width) {
  return histogramRenderController.histogramXToValue(x, width);
}

function drawHistogram(hist) {
  histogramRenderController.drawHistogram(hist);
}

function clearHistogram() {
  histogramRenderController.clearHistogram();
}

const roiStatsController = createRoiStatsController({
  state,
  roiState,
  roiCenterXInput,
  roiCenterYInput,
  roiLimitsEnable,
  roiParams,
  roiLinePlot,
  roiBoxPlotX,
  roiBoxPlotY,
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
  scheduleRoiUpdate: (...args) => scheduleRoiUpdate(...args),
  updateRoiSectionState,
  drawRoiOverlay: (...args) => drawRoiOverlay(...args),
  getActiveSaturationMax,
  isSaturatedValue,
  computeMedian,
  formatStat,
  formatRoiTick,
  PLOT_THEME,
});

function updateRoiCenterInputs() {
  roiStatsController.updateRoiCenterInputs();
}

function applyRoiCenterFromInputs() {
  return roiStatsController.applyRoiCenterFromInputs();
}

function getRoiPlotKey(canvasEl) {
  return roiStatsController.getRoiPlotKey(canvasEl);
}

function getRoiPlotLimits(plotKey) {
  return roiStatsController.getRoiPlotLimits(plotKey);
}

function syncRoiPlotLimitControls() {
  roiStatsController.syncRoiPlotLimitControls();
}

function updateRoiPlotLimitsEnabled() {
  roiStatsController.updateRoiPlotLimitsEnabled();
}

function setRoiPlotAxisLimits(plotKey, axis, minValue, maxValue) {
  roiStatsController.setRoiPlotAxisLimits(plotKey, axis, minValue, maxValue);
}

function clearRoiPlotLimitsForKey(plotKey) {
  roiStatsController.clearRoiPlotLimitsForKey(plotKey);
}

function hasAnyManualRoiPlotLimits() {
  return roiStatsController.hasAnyManualRoiPlotLimits();
}

function updateRoiModeUI() {
  roiStatsController.updateRoiModeUI();
}

function clearRoi() {
  roiStatsController.clearRoi();
}

function updateGlobalStats() {
  roiStatsController.updateGlobalStats();
}

function hideRoiTooltip(canvasEl) {
  roiStatsController.hideRoiTooltip(canvasEl);
}

function updateRoiTooltip(event, canvasEl) {
  roiStatsController.updateRoiTooltip(event, canvasEl);
}

function updateRoiStats() {
  roiStatsController.updateRoiStats();
}

function exportRoiCsv() {
  if (!roiState.enabled || !roiState.active) {
    setStatus("No ROI data to export");
    return;
  }
  const sections = [];
  const formatNum = (value) => (Number.isFinite(value) ? String(value) : "");

  const addSection = (title, data, meta, allowEmpty = false) => {
    if (!allowEmpty && (!data || !data.length)) return;
    const xLabel = meta?.xLabel || "Index";
    const yLabel = meta?.yLabel || "Value";
    const xStart = Number.isFinite(meta?.xStart) ? meta.xStart : 0;
    const xStep = Number.isFinite(meta?.xStep) && meta.xStep !== 0 ? meta.xStep : 1;
    sections.push(`# ${title}`);
    sections.push(`${xLabel},${yLabel}`);
    if (data && data.length) data.forEach((value, idx) => {
      const xVal = xStart + idx * xStep;
      sections.push(`${formatNum(xVal)},${formatNum(value)}`);
    });
    sections.push("");
  };

  if (roiState.lineProfile && roiState.lineProfile.length) {
    addSection(
      roiState.mode === "line" ? "Line Profile" : "Radial Profile",
      roiState.lineProfile,
      roiLineCanvas?._roiPlotMeta
    );
  }
  const allowBoxEmpty = roiState.mode === "box";
  if (roiState.xProjection && roiState.xProjection.length) {
    addSection("X Projection", roiState.xProjection, roiXCanvas?._roiPlotMeta, allowBoxEmpty);
  } else if (allowBoxEmpty) {
    addSection("X Projection", roiState.xProjection || [], roiXCanvas?._roiPlotMeta, true);
  }
  if (roiState.yProjection && roiState.yProjection.length) {
    addSection("Y Projection", roiState.yProjection, roiYCanvas?._roiPlotMeta, allowBoxEmpty);
  } else if (allowBoxEmpty) {
    addSection("Y Projection", roiState.yProjection || [], roiYCanvas?._roiPlotMeta, true);
  }

  if (!sections.length) {
    setStatus("No ROI projection data to export");
    return;
  }

  const base = (state.file || "roi").split("/").pop().replace(/\.[^.]+$/, "");
  const thresholdSuffix = state.thresholdCount > 1 ? `_thr${state.thresholdIndex + 1}` : "";
  const filename = `${base}_frame_${state.frameIndex + 1}${thresholdSuffix}_roi_${roiState.mode}.csv`;
  const blob = new Blob([sections.join("\n")], { type: "text/csv;charset=utf-8" });
  const link = document.createElement("a");
  const url = URL.createObjectURL(blob);
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
  setStatus(`Exported ROI CSV: ${filename}`);
}

function closeCurrentFile() {
  stopPlayback();
  state.file = "";
  state.dataset = "";
  state.shape = [];
  state.dtype = "";
  state.frameCount = 1;
  state.frameIndex = 0;
  state.thresholdCount = 1;
  state.thresholdIndex = 0;
  state.thresholdEnergies = [];
  state.dataRaw = null;
  state.dataFloat = null;
  state.histogram = null;
  state.stats = null;
  state.hasFrame = false;
  state.panOffsetX = 0;
  state.panOffsetY = 0;
  state.renderOffsetX = 0;
  state.renderOffsetY = 0;
  state.globalStats = null;
  analysisState.peaks = [];
  analysisState.selectedPeaks = [];
  analysisState.peakSelectionAnchor = null;
  clearMaskState();
  clearImageHeader();
  updateToolbar();
  setDataSourceSectionState("empty", "No file loaded.");
  setStatus("No file loaded");
  setLoading(false);
  hideUploadProgress();
  hideProcessingProgress();
  showSplash();
  setSplashStatus("Ready. Open a file to begin.");
  updateInspectorHeaderVisibility("");

  fileSelect.selectedIndex = 0;
  datasetSelect.innerHTML = "";
  updateFrameControls();
  updateThresholdOptions();
  minInput.value = "";
  maxInput.value = "";
  metaShape.textContent = "-";
  metaDtype.textContent = "-";
  metaRange.textContent = "-";

  canvas.width = 1;
  canvas.height = 1;
  const ctx = canvas.getContext("2d");
  if (ctx) {
    ctx.clearRect(0, 0, 1, 1);
  }
  applyCanvasTransform();
  updatePanCapability();
  clearHistogram();
  renderPeakList();
  schedulePeakOverlay();
  syncSeriesSumOutputPath(true);
  clearRoi();
  updateRingsSectionState();
  updatePeaksSectionState();
  updatePlayButtons();
}

function applyFrame(data, width, height, dtype) {
  state.dataRaw = data;
  state.dataFloat =
    renderer.type === "webgl" && !isWebglUnsignedRawCandidate(dtype, data) ? toFloat32(data) : null;
  state.width = width;
  state.height = height;
  state.dtype = dtype;
  state.stats = computeStats(data);
  state.histogram = state.stats.hist;
  updateGlobalStats();

  if (state.autoScale) {
    const levels = computeAutoLevels(data, state.stats.satMax ?? null);
    state.min = levels.min;
    state.max = levels.max;
    minInput.value = formatValue(state.min);
    maxInput.value = formatValue(state.max);
  }

  metaRange.textContent = `${formatValue(state.stats.min)} → ${formatValue(state.stats.max)}`;
  alignMaskToFrame();
  syncMaskAvailability(false);
  redraw();
  if (!state.hasFrame) {
    fitImageToView();
  }
  state.hasFrame = true;
  updatePanCapability();
  hideSplash();
  updatePlayButtons();
  scheduleOverview();
  scheduleRoiUpdate();
  schedulePixelOverlay();
  scheduleResolutionOverlay();
  schedulePeakFinder();
}

function redraw() {
  renderEngineController.redraw();
}

async function loadFrame() {
  await fileDataPipelineController.loadFrame();
}

function initializeMainUiBindings() {
  applyPlatformShortcutLabels();

  bindMenuAndGlobalInteractions({
    menuButtons,
    submenuParents,
    dropdown,
    menuActions,
    callbacks: {
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
      isMenuActive: (menuId) => activeMenu === menuId,
      trapModalFocus,
      isCommandPaletteOpen,
      handleCommandPaletteKeydown,
      closeTopModal,
      getTopOpenModal,
      handleNavShortcut,
      handleMenuAction,
      handleShortcut,
    },
  });
bindInspectorInteractions({
  inspectorTree,
  inspectorSearchInput,
  inspectorSearchClear,
  inspectorResults,
  inspectorStateEl,
  callbacks: {
    selectInspectorRow,
    renderInspectorLink,
    setSectionBadgeState,
    renderSkeletonBlock,
    fetchInspectorTree,
    renderInspectorTree,
    showInspectorNode,
    clearInspectorSearch,
    runInspectorSearch,
  },
});

bindFileIngress({
  fileInput,
  canvasShell,
  onFilesSelected: uploadAndOpenSelectedFiles,
});

bindChromeUiInteractions({
  elements: {
    aboutClose,
    aboutModal,
    settingsClose,
    settingsCancel,
    settingsSave,
    settingsSaveClose,
    settingsModal,
    commandInput,
    commandModal,
  },
  callbacks: {
    closeAboutModal,
    closeSettingsModal,
    saveSettingsFromModal,
    setCommandPaletteIndex: (next) => {
      commandPaletteController.setIndex(next);
    },
    renderCommandPalette,
    closeCommandPalette,
  },
});
  bindDataControlInteractions({
    state,
    elements: {
      fileSelect,
      datasetSelect,
      thresholdSelect,
      toolbarThresholdSelect,
      frameRange,
      frameIndex,
      frameStep,
      fpsSelect,
    },
    callbacks: {
      ensureFileMode,
      syncSeriesSumOutputPath,
      stopPlayback,
      isHdfFile,
      loadDatasets,
      loadImageSeries,
      loadMetadata,
      setThresholdIndex,
      requestFrame,
      setFrameStep,
      closeToolbarPlaybackPopover,
      setFps,
    },
  });
}

const {
  isBackendLocal: backendIsLocal,
  openFileBrowser,
  openFileDialog,
  closeFileBrowser,
  restoreFilesystemMode,
} = createFileBrowserController({
  apiBase: API,
  browseModal,
  browseBreadcrumb,
  browseFoldersList,
  browseFilesList,
  browsePathInput,
  browseStatus,
  browseSelectBtn,
  browseCancelBtn,
  browseCloseBtn,
  filesystemModeEl: filesystemMode,
  openModal,
  closeModal,
  setStatus,
  onPathSelected: ({ mode, selectedPath }) => {
    if (mode === "autoload") {
      if (autoloadDir) autoloadDir.value = selectedPath;
      state.autoload.dir = selectedPath;
      persistAutoloadSettings();
      if (state.autoload.mode === "file") {
        loadFiles().catch((err) => console.error(err));
      }
      if (state.autoload.running && state.autoload.mode === "file" && state.autoload.watchEnabled) {
        autoloadTick();
      }
    } else if (mode === "series-sum") {
      const picked = selectedPath.replace(/[\\/]$/, "");
      if (seriesSumOutput) {
        seriesSumOutput.value = `${picked}/series_sum`;
      }
    }
  },
});
initializeMainUiBindings();

async function handleLocalFileSelection(mode) {
  fileInput.accept = ".h5,.hdf5,.tif,.tiff,.cbf,.cbf.gz,.edf";
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
      setAutoloadStatus("Local file selected (upload endpoint needed for full support)");
    }
  };
  fileInput.click();
}

initializePostFilePickerBindings({
  autoloadBinding: {
    apiBase: API,
    state,
    analysisState,
    backendIsLocal,
    elements: {
      autoloadMode,
      autoloadWatchEnabled,
      autoloadDir,
      autoloadInterval,
      remoteIntervalInput,
      jfjochIntervalInput,
      remoteSourceInput,
      jfjochSourceInput,
      jfjochEndpointInput,
      jfjochTopicInput,
      jfjochChannelInput,
      autoloadTypeHdf5,
      autoloadTypeTiff,
      autoloadTypeCbf,
      autoloadPattern,
      autoloadBrowse,
      autoloadSelectFile,
      filesystemMode,
      simplonUrl,
      simplonVersion,
      simplonTimeout,
      simplonEnable,
    },
    callbacks: {
      stopAutoload,
      startAutoload,
      updateAutoloadUI,
      persistAutoloadSettings,
      loadFiles,
      autoloadTick,
      updateRemoteMetaUI,
      updateJfjochMetaUI,
      schedulePeakOverlay,
      setSimplonMode,
      setAutoloadStatus,
      openFileBrowser,
      openFileModal,
      handleLocalFileSelection,
    },
  },
  viewerBinding: {
    state,
    elements: {
      colormapSelect,
      autoScaleToggle,
      minInput,
      maxInput,
      maskToggle,
      maskSaturatedToggle,
      autoContrastBtn,
      invertToggle,
      histLogX,
      histLogY,
      zoomRange,
      resetView,
      prevBtn,
      nextBtn,
      playBtn,
      toolbarPlaybackToggle,
      toolbarMoreToggle,
      toolbarMoreStep,
      toolbarMoreFps,
      toolbarMoreThreshold,
      toolbarMorePanelToggle,
      toolbarMoreFullscreen,
      fullscreenToggle,
      splashOpenFileBtn,
      footerVersionToggleEl,
      panelFab,
      panelCollapseBtn,
      panelSheetHandle,
      canvasWrap,
    },
    callbacks: {
      redraw,
      scheduleHistogram,
      computeAutoLevels,
      formatValue,
      updateGlobalStats,
      scheduleRoiUpdate,
      schedulePeakFinder,
      chooseHistogramBins,
      computeHistogram,
      snapHistogramValue,
      deferViewportInteraction,
      setZoom,
      scheduleOverview,
      zoomAt,
      fitImageToView,
      stopPlayback,
      requestFrame,
      startPlayback,
      toggleToolbarPlaybackPopover,
      toggleToolbarMorePopover,
      setFrameStep,
      setFps,
      setThresholdIndex,
      togglePanel,
      closeToolbarMorePopover,
      toggleFullscreen,
      openFileModal,
      toggleFooterVersionPopover,
      registerChromeActivity,
      updateFullscreenUi,
      startMobilePanelDrag,
      updateMobilePanelDrag,
      stopMobilePanelDrag,
    },
  },
  roiControlBinding: {
    state,
    roiState,
    elements: {
      roiEnableToggle,
      roiModeSelect,
      roiLogToggle,
      roiLimitsEnable,
      roiClearBtn,
      roiExportCsvBtn,
      roiRadiusInput,
      roiInnerInput,
      roiOuterInput,
      roiCenterXInput,
      roiCenterYInput,
      canvasWrap,
    },
    callbacks: {
      setRoiDragging: (next) => {
        roiDragging = next;
      },
      stopRoiEdit,
      updateRoiModeUI,
      scheduleRoiOverlay,
      scheduleRoiUpdate,
      updateRoiPlotLimitsEnabled,
      clearRoi,
      setStatus,
      exportRoiCsv,
      applyRoiCenterFromInputs,
      updateRoiCenterInputs,
    },
  },
  panelBinding: {
    panelTabs,
    sectionToggles,
    sectionSwitches,
    callbacks: {
      initializeSectionContentWrappers,
      initializePanelTabA11y,
      setPanelTab,
      toggleSection,
      loadStoredPanelTab,
      getSectionStateStore: () => sectionStateStore,
      setSectionStateStore: (next) => {
        sectionStateStore = next;
      },
      setSectionState,
    },
  },
  viewportBinding: {
    state,
    roiState,
    constants: {
      MAX_ZOOM,
    },
    elements: {
      panelResizer,
      appLayout,
      toolsPanel,
      canvasWrap,
      roiRadiusInput,
      roiInnerInput,
      roiOuterInput,
    },
    callbacks: {
      applyPanelState,
      setPanelWidth,
      scheduleHistogram,
      deferViewportInteraction,
      normalizeWheelDelta,
      queueWheelZoom,
      scheduleOverview,
      schedulePixelOverlay,
      scheduleRoiOverlay,
      scheduleResolutionOverlay,
      schedulePeakOverlay,
      startTouchGesture,
      updateTouchGesture,
      stopTouchGesture,
      isTouchGestureActive,
      getEffectiveScrollLeft,
      getEffectiveScrollTop,
      setEffectiveScroll,
      getImagePointFromEvent,
      updateRoiCenterInputs,
      getRoiHandleAt,
      isPointInRoi,
      startRoiEdit,
      updateCursorOverlay,
      isRoiEditing: () => roiEditing,
      applyRoiEdit,
      stopRoiEdit,
      hideCursorOverlay,
      getMinZoom,
      zoomAt,
      getRoiDragging: () => roiDragging,
      setRoiDragging: (next) => {
        roiDragging = next;
      },
      scheduleRoiUpdate,
    },
  },
  roiPlotBinding: {
    roiState,
    elements: {
      roiLineCanvas,
      roiXCanvas,
      roiYCanvas,
      roiLinePlot,
      roiBoxPlotX,
      roiBoxPlotY,
      roiLimitsEnable,
    },
    callbacks: {
      updateRoiTooltip,
      hideRoiTooltip,
      getRoiPlotKey,
      getRoiPlotLimits,
      setRoiPlotAxisLimits,
      syncRoiPlotLimitControls,
      scheduleRoiUpdate,
      clearRoiPlotLimitsForKey,
      hasAnyManualRoiPlotLimits,
      normalizeWheelDelta,
    },
  },
  overviewBinding: {
    state,
    overviewState: overviewInteractionState,
    elements: {
      overviewCanvas,
    },
    callbacks: {
      overviewEventToImage,
      overviewEventToOverview,
      getViewRect,
      getOverviewHandleAt,
      getAnchorForHandle,
      resizeViewFromHandle,
      panToImageCenter,
    },
  },
  histogramDragBinding: {
    state,
    elements: {
      histCanvas,
      minInput,
      maxInput,
      autoScaleToggle,
    },
    callbacks: {
      snapHistogramValue,
      formatValue,
      redraw,
      histogramValueToX,
      histogramXToValue,
      getHistTooltipPosition,
      showHistTooltip,
      hideHistTooltip,
    },
  },
  windowUiBinding: {
    state,
    elements: {
      exportBtn,
    },
    callbacks: {
      exportFullImage,
      setZoom,
      updateToolbar,
      drawHistogram,
      drawSplash,
      applyPanelState,
      scheduleOverview,
      scheduleHistogram,
      schedulePixelOverlay,
      scheduleRoiOverlay,
      scheduleRoiUpdate,
      scheduleResolutionOverlay,
      schedulePeakOverlay,
    },
  },
});

initRenderer();

initializeUiDefaults({
  state,
  roiState,
  backendIsLocal,
  elements: {
    filesystemMode,
    fpsSelect,
    frameStep,
    histLogX,
    histLogY,
    colormapSelect,
    roiEnableToggle,
    roiModeSelect,
    roiLogToggle,
  },
  callbacks: {
    restoreFilesystemMode,
    showSplash,
    drawSplash,
    setFps,
    setFrameStep,
    updateRoiModeUI,
    updateRoiPlotLimitsEnabled,
  },
});

function validateSeriesStepInput(commit = false) {
  if (!seriesSumStep) return 1;
  const mode = (seriesSumMode?.value || "all").toLowerCase();
  if (mode === "all") {
    setFieldHint(seriesSumStep, seriesSumStepHint, "");
    return 1;
  }
  const raw = String(seriesSumStep.value || "").trim();
  if (!raw) {
    setFieldHint(seriesSumStep, seriesSumStepHint, "Enter an integer greater than or equal to 1.");
    return null;
  }
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) {
    setFieldHint(seriesSumStep, seriesSumStepHint, "Enter an integer greater than or equal to 1.");
    return null;
  }
  const normalized = Math.max(1, Math.round(parsed));
  if (commit) {
    seriesSumStep.value = String(normalized);
  }
  if (normalized !== parsed && !commit) {
    setFieldHint(seriesSumStep, seriesSumStepHint, "Using nearest integer greater than or equal to 1.");
  } else {
    setFieldHint(seriesSumStep, seriesSumStepHint, "");
  }
  return normalized;
}
bindAnalysisControlInteractions({
  apiBase: API,
  state,
  analysisState,
  backendIsLocal,
  constants: {
    defaultRingCount: DEFAULT_RING_COUNT,
  },
  elements: {
    ringsToggle,
    ringsDistance,
    ringsDistanceHint,
    ringsPixel,
    ringsPixelHint,
    ringsEnergy,
    ringsEnergyHint,
    ringsCenterX,
    ringsCenterY,
    ringInputs,
    peaksCountInput,
    peaksCountHint,
    peaksEnableToggle,
    peaksExportBtn,
    seriesSumOutput,
    seriesSumMode,
    seriesSumOperation,
    seriesSumNormalizeEnable,
    seriesSumStep,
    seriesSumStepHint,
    seriesSumRangeStart,
    seriesSumRangeEnd,
    seriesSumNormalizeFrame,
    seriesSumBrowse,
    filesystemMode,
    seriesSumProgress,
    seriesSumStart,
    seriesSumCancel,
    pixelLabelToggle,
  },
  callbacks: {
    setFieldHint,
    updateRingsSectionState,
    scheduleResolutionOverlay,
    schedulePeakFinder,
    exportPeakCsv,
    syncSeriesSumOutputPath,
    updateSeriesSumUi,
    validateSeriesStepInput,
    setStatus,
    handleLocalFileSelection,
    openFileBrowser,
    openSeriesSumOutputTarget,
    startSeriesSumming,
    cancelSeriesSumming,
    schedulePixelOverlay,
  },
});

renderPeakList();
setSeriesSumProgress(0, "Idle");
updateSeriesSumUi();
finalizeRuntimeBootstrap({
  state,
  callbacks: {
    getMaxPanelWidth,
    nearestMobilePanelSnap,
    setMobilePanelSnap: (next) => {
      mobilePanelSnap = next;
    },
    applyPanelState,
    applyCanvasTransform,
    updatePanCapability,
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
});
