/*
 * ALBIS frontend controller.
 *
 * This file drives:
 * - UI state and interactions (menus, tabs, shortcuts, gestures)
 * - Data-source orchestration (file/watch/monitor)
 * - Rendering (WebGL2 primary + CPU fallback)
 * - Overlay layers (ROI, rings, peaks, pixel labels, histogram)
 *
 * Use `docs/CODE_MAP.md` for a quick function-level navigation guide.
 */

const fileSelect = document.getElementById("file-select");
const datasetSelect = document.getElementById("dataset-select");
const fileField = document.getElementById("file-field");
const datasetField = document.getElementById("dataset-field");
const thresholdField = document.getElementById("threshold-field");
const thresholdSelect = document.getElementById("threshold-select");
const toolbarThresholdWrap = document.getElementById("toolbar-threshold-wrap");
const toolbarThresholdSelect = document.getElementById("toolbar-threshold");
const frameRange = document.getElementById("frame-range");
const frameIndex = document.getElementById("frame-index");
const frameStep = document.getElementById("frame-step");
const fpsRange = document.getElementById("fps-range");
const fpsValue = document.getElementById("fps-value");
const autoScaleToggle = document.getElementById("auto-scale");
const minInput = document.getElementById("min-input");
const maxInput = document.getElementById("max-input");
const maskToggle = document.getElementById("mask-toggle");
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
const aboutVersion = document.getElementById("about-version");
const autoContrastBtn = document.getElementById("auto-contrast");
const invertToggle = document.getElementById("invert-color");
const colormapSelect = document.getElementById("colormap-select");
const prevBtn = document.getElementById("btn-prev");
const nextBtn = document.getElementById("btn-next");
const playBtn = document.getElementById("btn-play");
const toolsPanel = document.getElementById("side-panel");
const panelResizer = document.getElementById("panel-resizer");
const panelEdgeToggle = document.getElementById("panel-edge-toggle");
const panelFab = document.getElementById("panel-fab");
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
const splash = document.getElementById("splash");
const splashCanvas = document.getElementById("splash-canvas");
const splashCtx = splashCanvas?.getContext("2d");
const splashStatus = document.getElementById("splash-status");
const resolutionOverlay = document.getElementById("resolution-overlay");
const resolutionCtx = resolutionOverlay?.getContext("2d");
const peakOverlay = document.getElementById("peak-overlay");
const peakCtx = peakOverlay?.getContext("2d");
const dataSection = document.getElementById("data-section");
const autoloadMode = document.getElementById("autoload-mode");
const filesystemMode = document.getElementById("filesystem-mode");
const autoloadDir = document.getElementById("autoload-dir");
const autoloadInterval = document.getElementById("autoload-interval");
const autoloadStatus = document.getElementById("autoload-status");
const autoloadLatest = document.getElementById("autoload-latest");
const autoloadFolder = document.getElementById("autoload-folder");
const autoloadWatch = document.getElementById("autoload-watch");
const autoloadTypesRow = document.getElementById("autoload-types");
const autoloadSimplon = document.getElementById("autoload-simplon");
const inspectorSection = document.querySelector(".panel-section.inspector");
const imageHeaderSection = document.getElementById("image-header-section");
const imageHeaderText = document.getElementById("image-header-text");
const imageHeaderEmpty = document.getElementById("image-header-empty");
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
const autoloadBrowse = document.getElementById("autoload-browse");
const autoloadDirList = document.getElementById("autoload-dir-list");
const autoloadPattern = document.getElementById("autoload-pattern");
const autoloadTypeHdf5 = document.getElementById("autoload-type-hdf5");
const autoloadTypeTiff = document.getElementById("autoload-type-tiff");
const autoloadTypeCbf = document.getElementById("autoload-type-cbf");
const simplonUrl = document.getElementById("simplon-url");
const simplonVersion = document.getElementById("simplon-version");
const simplonTimeout = document.getElementById("simplon-timeout");
const simplonEnable = document.getElementById("simplon-enable");
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
const roiStartEl = document.getElementById("roi-start");
const roiEndEl = document.getElementById("roi-end");
const roiSizeLabel = document.getElementById("roi-size-label");
const roiSizeEl = document.getElementById("roi-size");
const roiCountLabel = document.getElementById("roi-count-label");
const roiCountEl = document.getElementById("roi-count");
const roiMinEl = document.getElementById("roi-min");
const roiMaxEl = document.getElementById("roi-max");
const roiSumEl = document.getElementById("roi-sum");
const roiMedianEl = document.getElementById("roi-median");
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
const ringsToggle = document.getElementById("rings-toggle");
const ringsDistance = document.getElementById("rings-distance");
const ringsPixel = document.getElementById("rings-pixel");
const ringsEnergy = document.getElementById("rings-energy");
const ringsCenterX = document.getElementById("rings-center-x");
const ringsCenterY = document.getElementById("rings-center-y");
const ringsCount = document.getElementById("rings-count");
const ringInputs = [
  document.getElementById("ring-r1"),
  document.getElementById("ring-r2"),
  document.getElementById("ring-r3"),
  document.getElementById("ring-r4"),
].filter(Boolean);
const peaksEnableToggle = document.getElementById("peaks-enable");
const peaksCountInput = document.getElementById("peaks-count");
const peaksExportBtn = document.getElementById("peaks-export");
const peaksBody = document.getElementById("peaks-body");
const seriesSumMode = document.getElementById("series-sum-mode");
const seriesSumOperation = document.getElementById("series-sum-operation");
const seriesSumStepField = document.getElementById("series-sum-step-field");
const seriesSumStepLabel = document.getElementById("series-sum-step-label");
const seriesSumStep = document.getElementById("series-sum-step");
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
const settingsConfigPath = document.getElementById("settings-config-path");
const settingsMessage = document.getElementById("settings-message");
const settingsServerHost = document.getElementById("settings-server-host");
const settingsServerPort = document.getElementById("settings-server-port");
const settingsServerReload = document.getElementById("settings-server-reload");
const settingsLauncherPort = document.getElementById("settings-launcher-port");
const settingsStartupTimeout = document.getElementById("settings-startup-timeout");
const settingsOpenBrowser = document.getElementById("settings-open-browser");
const settingsToolHints = document.getElementById("settings-tool-hints");
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

let renderer = null;
let activeMenu = "file";
let closeTimer = null;
let overviewScheduled = false;
let histogramScheduled = false;
let overviewDragging = false;
let overviewDragOffset = { x: 0, y: 0 };
let overviewRect = null;
let overviewDragMode = null;
let overviewHandle = null;
let overviewAnchor = null;
let overviewResizeCenter = false;
let histDragging = false;
let histDragTarget = null;
let panning = false;
let panStart = { x: 0, y: 0, scrollLeft: 0, scrollTop: 0 };
let pixelOverlayScheduled = false;
let sectionStateStore = {};
let roiOverlayScheduled = false;
let roiUpdateScheduled = false;
let roiPlotResizing = null; // Track which ROI plot is being resized
let roiPlotResizeStart = { x: 0, y: 0, height: 0, container: null };
let roiDragging = false;
let roiDragPointer = null;
let roiEditing = false;
let roiEditHandle = null;
let roiEditStart = null;
let roiEditSnapshot = null;
let zoomWheelTarget = null;
let zoomWheelRaf = null;
let zoomWheelPivot = null;
let resolutionOverlayScheduled = false;
let peakOverlayScheduled = false;
let peakFinderScheduled = false;
let seriesSumPollTimer = null;
let panelTabState = "view";
let backendTimer = null;
let inspectorSelectedRow = null;
const coarsePointerQuery = window.matchMedia("(hover: none), (pointer: coarse)");
let touchGestureActive = false;
let touchGestureDistance = 0;
let touchGestureMid = null;
let touchDragActive = false;
let touchDragStart = null;

const roiState = {
  // Active ROI geometry and derived plot configuration.
  mode: "none",
  start: null,
  end: null,
  active: false,
  log: false,
  stats: null,
  lineProfile: null,
  xProjection: null,
  yProjection: null,
  innerRadius: 0,
  outerRadius: 0,
  plotLimits: {
    autoscale: true,
    line: { xMin: null, xMax: null, yMin: null, yMax: null },
    x: { xMin: null, xMax: null, yMin: null, yMax: null },
    y: { xMin: null, xMax: null, yMin: null, yMax: null },
  },
};
const analysisState = {
  // Analysis overlays rendered on top of the current frame.
  ringsEnabled: false,
  distanceMm: null,
  pixelSizeUm: null,
  energyEv: null,
  centerX: null,
  centerY: null,
  rings: [1, 2, 4, 8],
  ringCount: 3,
  peaksEnabled: false,
  peakCount: 25,
  peaks: [],
  selectedPeaks: [],
  peakSelectionAnchor: null,
};

const state = {
  // Global view/data state used across renderer + UI controls.
  file: "",
  dataset: "",
  shape: [],
  dtype: "",
  frameCount: 1,
  frameIndex: 0,
  thresholdCount: 1,
  thresholdIndex: 0,
  thresholdEnergies: [],
  seriesFiles: [],
  seriesLabel: "",
  imageHeaderFile: "",
  imageHeaderText: "",
  backendAlive: false,
  backendVersion: "0.4",
  toolHintsEnabled: false,
  isLoading: false,
  pendingFrame: null,
  playing: false,
  playTimer: null,
  fps: 1,
  step: 1,
  panelWidth: 640,
  panelCollapsed: true,
  autoScale: true,
  min: 0,
  max: 1,
  colormap: "albulaHdr",
  invert: false,
  zoom: 1,
  renderOffsetX: 0,
  renderOffsetY: 0,
  dataRaw: null,
  dataFloat: null,
  histogram: null,
  stats: null,
  histLogX: true,
  histLogY: true,
  pixelLabels: true,
  maskRaw: null,
  maskShape: null,
  maskAvailable: false,
  maskEnabled: false,
  maskAuto: true,
  maskFile: "",
  maskPath: "",
  hasFrame: false,
  width: 0,
  height: 0,
  globalStats: null,
  autoload: {
    mode: "file",
    dir: "",
    interval: 1000,
    types: {
      hdf5: true,
      tiff: true,
      cbf: true,
      edf: true,
    },
    pattern: "",
    simplonUrl: "",
    simplonVersion: "1.8.0",
    simplonTimeout: 500,
    simplonEnable: true,
    autoStart: false,
    running: false,
    timer: null,
    busy: false,
    lastFile: "",
    lastMtime: 0,
    lastUpdate: 0,
    lastPoll: 0,
    lastMonitorSig: "",
    lastMaskAttempt: 0,
  },
  seriesSum: {
    running: false,
    jobId: "",
    progress: 0,
    message: "Idle",
    outputs: [],
    openTarget: "",
    autoOutputPath: "",
  },
};

const API = "/api";
const clientLogBuffer = [];
let clientLogTimer = null;
let clientLogSending = false;

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

const originalConsoleError = console.error;
const originalConsoleWarn = console.warn;
console.error = (...args) => {
  originalConsoleError(...args);
  logClient("error", args.map(formatClientArg).join(" "));
};
console.warn = (...args) => {
  originalConsoleWarn(...args);
  logClient("warning", args.map(formatClientArg).join(" "));
};

window.addEventListener("error", (event) => {
  logClient("error", event.message || "Unhandled error", {
    source: event.filename,
    line: event.lineno,
    column: event.colno,
    stack: event.error?.stack,
  });
});

window.addEventListener("unhandledrejection", (event) => {
  logClient("error", "Unhandled promise rejection", {
    reason: formatClientArg(event.reason),
  });
});
const MIN_ZOOM = 0.1;
const MAX_ZOOM = 50;
const PIXEL_LABEL_MIN_ZOOM = 15;
const PEAK_BAD_MASK_BITS = 0x1f;

function getMinZoom() {
  if (!canvasWrap || !state.width || !state.height) {
    return MIN_ZOOM;
  }
  const scale = Math.min(
    canvasWrap.clientWidth / state.width,
    canvasWrap.clientHeight / state.height
  );
  if (!Number.isFinite(scale) || scale <= 0) {
    return MIN_ZOOM;
  }
  return Math.min(1, scale);
}
const PIXEL_LABEL_MAX_CELLS = 25000;

function setStatus(text) {
  statusEl.textContent = text;
}

function formatValue(value) {
  if (!Number.isFinite(value)) return "";
  const dtype = state.dtype || "";
  if (dtype.includes("f")) {
    return value.toFixed(3);
  }
  return Math.round(value).toString();
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



const HELP_SELECTORS = [
  "button",
  "input",
  "select",
  "textarea",
  "label.checkbox",
  ".menu-item",
  ".dropdown-item",
  ".panel-tab",
  ".section-title",
  ".panel-fab",
  ".panel-edge-toggle",
  ".panel-resizer",
  ".roi-resize-handle",
].join(",");
const HELP_DELAY_MS = 1000;
let helpTooltip = null;
let helpTimer = null;
let helpTarget = null;
let helpLastEvent = null;

function getHelpLabelText(target) {
  if (!target) return "";
  const label = target.closest("label");
  if (label) {
    const span = label.querySelector("span");
    const labelText = (span ? span.textContent : label.textContent) || "";
    return labelText.replace(/\s+/g, " ").trim();
  }
  if (target.id) {
    const labelFor = document.querySelector(`label[for="${target.id}"]`);
    if (labelFor) {
      return (labelFor.textContent || "").replace(/\s+/g, " ").trim();
    }
  }
  return "";
}

function getHelpText(target) {
  if (!target) return "";
  const dataHelp = target.dataset?.help;
  if (dataHelp) return dataHelp;
  const ariaLabel = target.getAttribute?.("aria-label");
  if (ariaLabel) return ariaLabel;
  const title = target.getAttribute?.("title");
  if (title) return title;
  if (target.classList?.contains("menu-item")) {
    const text = (target.textContent || "").replace(/\s+/g, " ").trim();
    if (text) return `Open ${text} menu`;
  }
  if (target.classList?.contains("dropdown-item")) {
    const text = target.querySelector("span")?.textContent?.trim() || "";
    if (text) return text;
  }
  const labelText = getHelpLabelText(target);
  if (labelText) return labelText;
  const text = (target.textContent || "").replace(/\s+/g, " ").trim();
  if (text) return text;
  return "";
}

function applyHelpMap() {
  const helpMap = {
    "btn-prev": "Previous frame",
    "btn-next": "Next frame",
    "btn-play": "Play/pause playback",
    "toolbar-threshold": "Select detector threshold",
    "zoom-range": "Zoom image",
    "reset-view": "Fit image to window",
    "pixel-label-toggle": "Show pixel values at high zoom",
    "mask-toggle": "Apply pixel mask (if available)",
    "colormap-select": "Choose color map",
    "invert-color": "Invert color map",
    "rings-toggle": "Show resolution rings",
    "roi-mode": "Select ROI mode",
    "roi-log": "Log-scale ROI plots",
    "roi-limits-enable": "Autoscale ROI plots",
    "roi-export-csv": "Export ROI projection data as CSV",
    "autoload-mode": "Select image source",
    "filesystem-mode": "Select filesystem source",
    "autoload-dir": "Folder to watch",
    "autoload-browse": "Browse for a folder",
    "autoload-pattern": "Filename filter (supports wildcards)",
    "autoload-interval": "Polling interval (ms)",
    "simplon-url": "SIMPLON base URL",
    "simplon-timeout": "Monitor timeout (ms)",
    "simplon-enable": "Enable live monitor",
    "panel-fab": "Toggle side panel",
    "panel-resizer": "Resize side panel",
    "inspector-search-input": "Search the HDF5 tree",
    "inspector-search-clear": "Clear search",
  };
  Object.entries(helpMap).forEach(([id, text]) => {
    const el = document.getElementById(id);
    if (el && !el.dataset.help) {
      el.dataset.help = text;
    }
  });
  document.querySelectorAll(".roi-resize-handle").forEach((el) => {
    if (!el.dataset.help) el.dataset.help = "Drag to resize plot";
  });
  document.querySelectorAll("[data-help]").forEach((el) => {
    if (el.hasAttribute("title")) {
      el.removeAttribute("title");
    }
  });
}


function setToolHintsEnabled(enabled) {
  state.toolHintsEnabled = Boolean(enabled);
  if (!state.toolHintsEnabled) {
    hideHelp();
  }
}

function positionHelpTooltip(event) {
  if (!helpTooltip || !helpLastEvent) return;
  const evt = event || helpLastEvent;
  const padding = 12;
  const offset = 14;
  let x = evt.clientX + offset;
  let y = evt.clientY + offset;
  const rect = helpTooltip.getBoundingClientRect();
  const maxX = window.innerWidth - rect.width - padding;
  const maxY = window.innerHeight - rect.height - padding;
  if (x > maxX) x = Math.max(padding, evt.clientX - rect.width - offset);
  if (y > maxY) y = Math.max(padding, evt.clientY - rect.height - offset);
  helpTooltip.style.left = `${x}px`;
  helpTooltip.style.top = `${y}px`;
}

function showHelp(target, event, immediate = false) {
  if (!helpTooltip || !state.toolHintsEnabled) return;
  const text = getHelpText(target);
  if (!text) return;
  helpTarget = target;
  helpLastEvent = event;
  if (helpTimer) {
    clearTimeout(helpTimer);
    helpTimer = null;
  }
  const reveal = () => {
    helpTooltip.textContent = text;
    helpTooltip.classList.add("is-visible");
    positionHelpTooltip(event);
  };
  if (immediate) {
    reveal();
  } else {
    helpTimer = setTimeout(reveal, HELP_DELAY_MS);
  }
}

function hideHelp() {
  if (helpTimer) {
    clearTimeout(helpTimer);
    helpTimer = null;
  }
  if (helpTooltip) {
    helpTooltip.classList.remove("is-visible");
  }
  helpTarget = null;
}

function findHelpTarget(node) {
  if (!node) return null;
  if (node.closest(".help-tooltip")) return null;
  return node.closest(HELP_SELECTORS);
}

window.addEventListener("DOMContentLoaded", initHelpTooltips, { once: true });

function initHelpTooltips() {
  if (helpTooltip) return;
  helpTooltip = document.createElement("div");
  helpTooltip.className = "help-tooltip";
  helpTooltip.setAttribute("role", "tooltip");
  document.body.appendChild(helpTooltip);
  applyHelpMap();

  document.addEventListener("pointerover", (event) => {
    const target = findHelpTarget(event.target);
    if (!target) return;
    if (target === helpTarget) return;
    showHelp(target, event, false);
  }, true);

  document.addEventListener("mouseover", (event) => {
    const target = findHelpTarget(event.target);
    if (!target) return;
    if (target === helpTarget) return;
    showHelp(target, event, false);
  });

  document.addEventListener("pointermove", (event) => {
    if (!helpTooltip) return;
    if (helpTimer) {
      helpLastEvent = event;
      return;
    }
    if (!helpTooltip.classList.contains("is-visible")) return;
    helpLastEvent = event;
    positionHelpTooltip(event);
  });

  document.addEventListener("pointerout", (event) => {
    if (!helpTarget) return;
    const related = event.relatedTarget;
    if (related && helpTarget.contains(related)) return;
    hideHelp();
  }, true);

  document.addEventListener("mouseout", (event) => {
    if (!helpTarget) return;
    const related = event.relatedTarget;
    if (related && helpTarget.contains(related)) return;
    hideHelp();
  }, true);

  document.addEventListener("focusin", (event) => {
    const target = findHelpTarget(event.target);
    if (!target) return;
    const rect = target.getBoundingClientRect();
    const fakeEvent = { clientX: rect.left + rect.width / 2, clientY: rect.top };
    showHelp(target, fakeEvent, true);
  }, true);

  document.addEventListener("focusout", () => {
    hideHelp();
  });
}

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

function setInspectorMessage(message) {
  if (!inspectorTree) return;
  inspectorSelectedRow = null;
  inspectorTree.innerHTML = `<div class="inspector-empty">${message}</div>`;
  resetInspectorDetails();
  if (inspectorResults) {
    inspectorResults.innerHTML = "";
    inspectorResults.classList.add("is-hidden");
  }
}

function setImageHeader(text) {
  if (!imageHeaderText || !imageHeaderEmpty) return;
  const headerText = typeof text === "string" ? text.trim() : "";
  const hasText = headerText.length > 0;
  imageHeaderText.textContent = hasText ? text : "";
  imageHeaderText.classList.toggle("is-hidden", !hasText);
  imageHeaderEmpty.classList.toggle("is-hidden", hasText);
}

function clearImageHeader() {
  state.imageHeaderFile = "";
  state.imageHeaderText = "";
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
    setImageHeader("");
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
    return;
  }
  inspectorResults.classList.remove("is-hidden");
  if (!Array.isArray(results) || results.length === 0) {
    inspectorResults.innerHTML = `<div class="inspector-empty">No matches.</div>`;
    return;
  }
  inspectorResults.innerHTML = "";
  results.forEach((item) => {
    const row = document.createElement("div");
    row.className = "inspector-result";
    row.dataset.path = item.path || "";
    row.dataset.type = item.type || "";
    if (item.type === "link" && item.target) {
      row.dataset.target = item.target;
    }
    const name = document.createElement("span");
    name.className = "inspector-result-name";
    name.textContent = item.path || item.name || "";
    const meta = document.createElement("span");
    meta.className = "inspector-result-meta";
    if (item.type === "dataset" && item.shape && item.dtype) {
      meta.textContent = `${item.shape.join("×")} ${item.dtype}`;
    } else if (item.type === "link" && item.target) {
      meta.textContent = item.target;
    } else {
      meta.textContent = item.type || "";
    }
    row.appendChild(name);
    row.appendChild(meta);
    inspectorResults.appendChild(row);
  });
}

async function runInspectorSearch(query) {
  if (!isHdf5File(state.file)) {
    renderInspectorResults([], query);
    return;
  }
  if (!query) {
    renderInspectorResults([], "");
    return;
  }
  try {
    const data = await fetchJSON(
      `${API}/hdf5/search?file=${encodeURIComponent(state.file)}&query=${encodeURIComponent(query)}`
    );
    renderInspectorResults(data.matches || [], query);
  } catch (err) {
    console.error(err);
    renderInspectorResults([], query);
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
  try {
    const children = await fetchInspectorTree("/");
    renderInspectorTree(children, inspectorTree);
    inspectorSelectedRow = null;
    resetInspectorDetails();
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
  } catch (err) {
    console.error(err);
    setInspectorMessage("Failed to load node details.");
  }
}

function syncOverlayCanvas(overlay, ctx) {
  if (!overlay || !ctx || !canvasWrap) return null;
  const width = canvasWrap.clientWidth || 1;
  const height = canvasWrap.clientHeight || 1;
  overlay.style.left = `${canvasWrap.offsetLeft}px`;
  overlay.style.top = `${canvasWrap.offsetTop}px`;
  overlay.style.width = `${width}px`;
  overlay.style.height = `${height}px`;
  const dpr = window.devicePixelRatio || 1;
  overlay.width = Math.max(1, Math.floor(width * dpr));
  overlay.height = Math.max(1, Math.floor(height * dpr));
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
  const imgX = (canvasWrap.scrollLeft + x - offsetX) / zoom;
  const imgY = (canvasWrap.scrollTop + y - offsetY) / zoom;
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
  if (!maskToggle) return;
  maskToggle.disabled = !state.maskAvailable;
  maskToggle.checked = Boolean(state.maskEnabled && state.maskAvailable);
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
  const imgX = (canvasWrap.scrollLeft + x - offsetX) / zoom;
  const imgY = (canvasWrap.scrollTop + y - offsetY) / zoom;
  const ix = Math.floor(imgX);
  const iy = Math.floor(imgY);
  if (ix < 0 || iy < 0 || ix >= state.width || iy >= state.height) {
    hideCursorOverlay();
    return;
  }
  const idx = iy * state.width + ix;
  let labelValue = formatValue(state.dataRaw[idx]);
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
  const resolutionValue = getResolutionAtPixel(ix, iy);
  const resolutionText = Number.isFinite(resolutionValue) ? `  d ${resolutionValue.toFixed(4)} Å` : "";
  const label = `X ${ix}  Y ${iy}  Value ${labelValue}${resolutionText}`;
  showCursorOverlay(label, event.clientX, event.clientY);
}

function clearPixelOverlay() {
  if (!pixelOverlay || !pixelCtx) return;
  pixelCtx.clearRect(0, 0, pixelOverlay.width, pixelOverlay.height);
}

function drawPixelOverlay() {
  if (!pixelOverlay || !pixelCtx || !canvasWrap) return;
  const width = canvasWrap.clientWidth || 1;
  const height = canvasWrap.clientHeight || 1;
  pixelOverlay.style.left = `${canvasWrap.offsetLeft}px`;
  pixelOverlay.style.top = `${canvasWrap.offsetTop}px`;
  pixelOverlay.style.width = `${width}px`;
  pixelOverlay.style.height = `${height}px`;
  const dpr = window.devicePixelRatio || 1;
  pixelOverlay.width = Math.max(1, Math.floor(width * dpr));
  pixelOverlay.height = Math.max(1, Math.floor(height * dpr));
  pixelCtx.setTransform(dpr, 0, 0, dpr, 0, 0);
  pixelCtx.clearRect(0, 0, width, height);

  if (!state.hasFrame || !state.dataRaw || !state.pixelLabels) return;
  const zoom = state.zoom || 1;
  if (zoom < PIXEL_LABEL_MIN_ZOOM) return;
  const offsetX = state.renderOffsetX || 0;
  const offsetY = state.renderOffsetY || 0;
  const maskReady =
    state.maskEnabled &&
    state.maskAvailable &&
    state.maskRaw &&
    state.maskShape &&
    state.maskShape[0] === state.height &&
    state.maskShape[1] === state.width;

  const viewX = canvasWrap.scrollLeft / zoom;
  const viewY = canvasWrap.scrollTop / zoom;
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

  const cells = Math.max(0, endX - startX) * Math.max(0, endY - startY);
  if (cells === 0 || cells > PIXEL_LABEL_MAX_CELLS) {
    return;
  }

  const fontSize = Math.min(14, Math.max(6, zoom * 0.9));
  pixelCtx.font = `${fontSize}px "Lucida Grande", "Helvetica Neue", Arial, sans-serif`;
  pixelCtx.textAlign = "center";
  pixelCtx.textBaseline = "middle";
  pixelCtx.fillStyle = "rgba(255, 255, 255, 0.9)";
  pixelCtx.shadowColor = "rgba(0, 0, 0, 0.7)";
  pixelCtx.shadowBlur = 2;

  for (let y = startY; y < endY; y += 1) {
    const rowOffset = y * state.width;
    const screenY = (y - viewY) * zoom + zoom / 2 + offsetY;
    for (let x = startX; x < endX; x += 1) {
      const idx = rowOffset + x;
      let text = formatValue(state.dataRaw[idx]);
      if (maskReady && state.maskRaw) {
        const maskValue = state.maskRaw[idx];
        if (maskValue & 1) {
          text = "G";
        } else if (maskValue & 0x1e) {
          text = "D";
        }
      }
      const screenX = (x - viewX) * zoom + zoom / 2 + offsetX;
      pixelCtx.fillText(text, screenX, screenY);
    }
  }
  pixelCtx.shadowBlur = 0;
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
  if (!roiState.active || roiState.mode === "none" || !roiState.start || !roiState.end) return;
  const zoom = state.zoom || 1;
  const offsetX = state.renderOffsetX || 0;
  const offsetY = state.renderOffsetY || 0;
  const viewX = canvasWrap.scrollLeft / zoom;
  const viewY = canvasWrap.scrollTop / zoom;
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

function scheduleResolutionOverlay() {
  if (!resolutionOverlay || !resolutionCtx) return;
  if (resolutionOverlayScheduled) return;
  resolutionOverlayScheduled = true;
  window.requestAnimationFrame(() => {
    resolutionOverlayScheduled = false;
    drawResolutionOverlay();
  });
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
  const count = Number(ringsCount?.value || analysisState.ringCount || 3);
  const maxCount = Math.max(1, ringInputs.length);
  const ringLimit = Number.isFinite(count)
    ? Math.max(1, Math.min(maxCount, Math.round(count)))
    : Math.min(3, maxCount);
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

function renderPeakList() {
  if (!peaksBody) return;
  peaksBody.innerHTML = "";
  if (!analysisState.peaksEnabled) {
    const empty = document.createElement("div");
    empty.className = "peaks-empty";
    empty.textContent = "Enable \"Find peaks\" to detect diffraction peaks.";
    peaksBody.appendChild(empty);
    return;
  }
  if (!analysisState.peaks.length) {
    const empty = document.createElement("div");
    empty.className = "peaks-empty";
    empty.textContent = state.hasFrame ? "No peaks detected." : "Load a frame to detect peaks.";
    peaksBody.appendChild(empty);
    return;
  }
  analysisState.peaks.forEach((peak, idx) => {
    const row = document.createElement("button");
    row.type = "button";
    row.className = "peaks-row";
    if (analysisState.selectedPeaks.includes(idx)) {
      row.classList.add("is-selected");
    }
    row.innerHTML = `<span>${peak.x}</span><span>${peak.y}</span><span>${formatStat(peak.intensity)}</span>`;
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
      } else {
        analysisState.selectedPeaks = [idx];
        analysisState.peakSelectionAnchor = idx;
      }
      if (!Number.isInteger(analysisState.peakSelectionAnchor)) {
        analysisState.peakSelectionAnchor = idx;
      }
      renderPeakList();
      schedulePeakOverlay();
    });
    peaksBody.appendChild(row);
  });
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
      if (mask && (mask[idx] & PEAK_BAD_MASK_BITS)) continue;

      let left = data[idx - 1];
      let right = data[idx + 1];
      let up = data[idx - width];
      let down = data[idx + width];
      if (mask) {
        if (mask[idx - 1] & PEAK_BAD_MASK_BITS) left = Number.NEGATIVE_INFINITY;
        if (mask[idx + 1] & PEAK_BAD_MASK_BITS) right = Number.NEGATIVE_INFINITY;
        if (mask[idx - width] & PEAK_BAD_MASK_BITS) up = Number.NEGATIVE_INFINITY;
        if (mask[idx + width] & PEAK_BAD_MASK_BITS) down = Number.NEGATIVE_INFINITY;
      }
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
  const requested = Math.max(1, Math.min(500, Math.round(Number(peaksCountInput?.value || analysisState.peakCount || 25))));
  analysisState.peakCount = requested;
  if (peaksCountInput) {
    peaksCountInput.value = String(requested);
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
  if (!analysisState.selectedPeaks.length && analysisState.peaks.length) {
    analysisState.selectedPeaks = [0];
    analysisState.peakSelectionAnchor = 0;
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
  window.setTimeout(runPeakFinder, 0);
}

function exportPeakCsv() {
  if (!analysisState.peaks.length) return;
  const rows = ["x,y,intensity"];
  analysisState.peaks.forEach((peak) => {
    rows.push(`${peak.x},${peak.y},${peak.intensity}`);
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

function schedulePeakOverlay() {
  if (!peakOverlay || !peakCtx) return;
  if (peakOverlayScheduled) return;
  peakOverlayScheduled = true;
  window.requestAnimationFrame(() => {
    peakOverlayScheduled = false;
    drawPeakOverlay();
  });
}

function drawPeakOverlay() {
  if (!peakOverlay || !peakCtx || !canvasWrap) return;
  const metrics = syncOverlayCanvas(peakOverlay, peakCtx);
  if (!metrics) return;
  const { width, height } = metrics;
  peakCtx.clearRect(0, 0, width, height);
  if (!state.hasFrame || !analysisState.peaksEnabled || !analysisState.peaks.length) return;

  const zoom = state.zoom || 1;
  const offsetX = state.renderOffsetX || 0;
  const offsetY = state.renderOffsetY || 0;
  const viewX = canvasWrap.scrollLeft / zoom;
  const viewY = canvasWrap.scrollTop / zoom;

  analysisState.peaks.forEach((peak, index) => {
    const sx = (peak.x + 0.5 - viewX) * zoom + offsetX;
    const sy = (peak.y + 0.5 - viewY) * zoom + offsetY;
    if (sx < -20 || sy < -20 || sx > width + 20 || sy > height + 20) return;
    const selected = analysisState.selectedPeaks.includes(index);
    const zoomScale = Math.max(0, Math.log2(Math.max(1, zoom)));
    const radius = selected
      ? Math.max(14, Math.min(34, 16 + zoomScale * 2.2))
      : Math.max(8, Math.min(16, 9 + zoomScale * 0.6));

    if (selected) {
      peakCtx.setLineDash([]);
      peakCtx.beginPath();
      peakCtx.arc(sx, sy, radius, 0, Math.PI * 2);
      peakCtx.lineWidth = 3.8;
      peakCtx.strokeStyle = "rgba(18, 18, 18, 0.92)";
      peakCtx.stroke();

      peakCtx.beginPath();
      peakCtx.arc(sx, sy, radius - 1.5, 0, Math.PI * 2);
      peakCtx.lineWidth = 2.6;
      peakCtx.strokeStyle = "rgba(72, 255, 105, 0.98)";
      peakCtx.stroke();

      const cross = radius + 5;
      peakCtx.beginPath();
      peakCtx.moveTo(sx - cross, sy);
      peakCtx.lineTo(sx + cross, sy);
      peakCtx.moveTo(sx, sy - cross);
      peakCtx.lineTo(sx, sy + cross);
      peakCtx.lineWidth = 5.2;
      peakCtx.strokeStyle = "rgba(0, 0, 0, 0.8)";
      peakCtx.stroke();

      peakCtx.beginPath();
      peakCtx.moveTo(sx - cross, sy);
      peakCtx.lineTo(sx + cross, sy);
      peakCtx.moveTo(sx, sy - cross);
      peakCtx.lineTo(sx, sy + cross);
      peakCtx.lineWidth = 2.8;
      peakCtx.strokeStyle = "rgba(72, 255, 105, 0.98)";
      peakCtx.stroke();
    } else {
      peakCtx.setLineDash([5, 4]);
      peakCtx.beginPath();
      peakCtx.arc(sx, sy, radius, 0, Math.PI * 2);
      peakCtx.lineWidth = 1.8;
      peakCtx.strokeStyle = "rgba(255, 255, 255, 0.55)";
      peakCtx.stroke();

      peakCtx.beginPath();
      peakCtx.arc(sx, sy, Math.max(3, radius - 2), 0, Math.PI * 2);
      peakCtx.lineWidth = 1.2;
      peakCtx.strokeStyle = "rgba(70, 155, 255, 0.72)";
      peakCtx.stroke();
    }
  });
  peakCtx.setLineDash([]);
}

function setSeriesSumProgress(progress, text) {
  const value = Number.isFinite(progress) ? Math.max(0, Math.min(1, progress)) : 0;
  state.seriesSum.progress = value;
  state.seriesSum.message = text || state.seriesSum.message || "Idle";
  if (seriesSumProgressFill) {
    seriesSumProgressFill.style.width = `${(value * 100).toFixed(1)}%`;
  }
  const canOpen = !state.seriesSum.running && Boolean(state.seriesSum.openTarget);
  if (seriesSumProgressText) {
    if (state.seriesSum.running) {
      const pct = `${Math.round(value * 100)}%`;
      seriesSumProgressText.textContent = `${pct}  ${state.seriesSum.message || "Running…"}`;
    } else {
      let message = state.seriesSum.message || "Idle";
      if (canOpen && !/click to open/i.test(message)) {
        message = `${message} — click to open`;
      }
      seriesSumProgressText.textContent = message;
    }
  }
  updateSeriesSumProgressOpenState();
}

function updateSeriesSumUi() {
  const mode = (seriesSumMode?.value || "all").toLowerCase();
  const isNth = mode === "nth";
  const isRange = mode === "range";
  const normalizeEnabled = Boolean(seriesSumNormalizeEnable?.checked);
  if (seriesSumStepField) {
    seriesSumStepField.classList.toggle("is-hidden", mode === "all");
  }
  if (seriesSumStepLabel) {
    seriesSumStepLabel.textContent = isNth ? "Nth interval (N)" : "Chunk size (N)";
  }
  if (seriesSumRangeStartField) {
    seriesSumRangeStartField.classList.toggle("is-hidden", !isRange);
  }
  if (seriesSumRangeEndField) {
    seriesSumRangeEndField.classList.toggle("is-hidden", !isRange);
  }
  if (seriesSumNormalizeFrameField) {
    seriesSumNormalizeFrameField.classList.toggle("is-hidden", !normalizeEnabled);
  }
  const totalFrames = Math.max(1, Number(state.frameCount || 1));
  if (seriesSumRangeStart) {
    seriesSumRangeStart.min = "1";
    seriesSumRangeStart.max = String(totalFrames);
    const nextStart = Math.max(1, Math.min(totalFrames, Math.round(Number(seriesSumRangeStart.value || 1))));
    seriesSumRangeStart.value = String(nextStart);
  }
  if (seriesSumRangeEnd) {
    seriesSumRangeEnd.min = "1";
    seriesSumRangeEnd.max = String(totalFrames);
    const nextEnd = Math.max(1, Math.min(totalFrames, Math.round(Number(seriesSumRangeEnd.value || totalFrames))));
    seriesSumRangeEnd.value = String(nextEnd);
  }
  if (seriesSumNormalizeFrame) {
    seriesSumNormalizeFrame.min = "1";
    seriesSumNormalizeFrame.max = String(totalFrames);
    const nextNorm = Math.max(1, Math.min(totalFrames, Math.round(Number(seriesSumNormalizeFrame.value || 1))));
    seriesSumNormalizeFrame.value = String(nextNorm);
  }
  const ready = Boolean(state.file && (isHdfFile(state.file) ? state.dataset : true));
  if (seriesSumStart) {
    seriesSumStart.disabled = !ready || state.seriesSum.running;
    seriesSumStart.textContent = state.seriesSum.running ? "Summing…" : "Start Summing";
  }
  if (seriesSumBrowse) {
    seriesSumBrowse.disabled = state.seriesSum.running || !ready;
  }
  if (seriesSumMode) {
    seriesSumMode.disabled = state.seriesSum.running || !ready;
  }
  if (seriesSumOperation) {
    seriesSumOperation.disabled = state.seriesSum.running || !ready;
  }
  if (seriesSumStep) {
    seriesSumStep.disabled = state.seriesSum.running || mode === "all" || !ready;
  }
  if (seriesSumRangeStart) {
    seriesSumRangeStart.disabled = state.seriesSum.running || !isRange || !ready;
  }
  if (seriesSumRangeEnd) {
    seriesSumRangeEnd.disabled = state.seriesSum.running || !isRange || !ready;
  }
  if (seriesSumNormalizeEnable) {
    seriesSumNormalizeEnable.disabled = state.seriesSum.running || !ready;
  }
  if (seriesSumNormalizeFrame) {
    seriesSumNormalizeFrame.disabled = state.seriesSum.running || !normalizeEnabled || !ready;
  }
  if (seriesSumOutput) {
    seriesSumOutput.disabled = state.seriesSum.running || !ready;
  }
  if (seriesSumFormat) {
    seriesSumFormat.disabled = state.seriesSum.running || !ready;
  }
  if (seriesSumMask) {
    seriesSumMask.disabled = state.seriesSum.running || !ready;
  }
}

function stopSeriesSumPolling() {
  if (seriesSumPollTimer) {
    window.clearTimeout(seriesSumPollTimer);
    seriesSumPollTimer = null;
  }
}

function resolveSeriesOpenTarget(outputs) {
  if (!Array.isArray(outputs) || !outputs.length) return "";
  return String(outputs[0]);
}

async function pollSeriesSumStatus() {
  if (!state.seriesSum.jobId) {
    state.seriesSum.running = false;
    updateSeriesSumUi();
    return;
  }
  try {
    const data = await fetchJSON(
      `${API}/analysis/series-sum/status?job_id=${encodeURIComponent(state.seriesSum.jobId)}`
    );
    const status = data.status || "running";
    const progress = Number.isFinite(data.progress) ? Number(data.progress) : state.seriesSum.progress;
    const message = data.message || state.seriesSum.message || "Running…";
    const outputs = Array.isArray(data.outputs) ? data.outputs : [];
    state.seriesSum.running = status === "queued" || status === "running";
    state.seriesSum.outputs = outputs;
    state.seriesSum.openTarget = state.seriesSum.running ? "" : resolveSeriesOpenTarget(outputs);
    setSeriesSumProgress(progress, message);
    updateSeriesSumUi();
    if (state.seriesSum.running) {
      seriesSumPollTimer = window.setTimeout(pollSeriesSumStatus, 500);
      return;
    }
    if (status === "done") {
      const count = state.seriesSum.outputs.length;
      setStatus(`Series summing done (${count} file${count === 1 ? "" : "s"})`);
    } else if (status === "error") {
      setStatus(`Series summing failed`);
    }
  } catch (err) {
    console.error(err);
    state.seriesSum.running = false;
    state.seriesSum.openTarget = "";
    setSeriesSumProgress(1, "Failed to query status");
    updateSeriesSumUi();
    setStatus("Series summing status failed");
  }
}

async function startSeriesSumming() {
  if (!state.file || (isHdfFile(state.file) && !state.dataset) || state.seriesSum.running) return;
  const mode = (seriesSumMode?.value || "all").toLowerCase();
  const operation = (seriesSumOperation?.value || "sum").toLowerCase();
  const normalizeEnabled = Boolean(seriesSumNormalizeEnable?.checked);
  const totalFrames = Math.max(1, Math.round(Number(state.frameCount || 1)));

  const step = Math.max(1, Math.round(Number(seriesSumStep?.value || 10)));
  if (seriesSumStep) {
    seriesSumStep.value = String(step);
  }

  const rangeStart = Math.max(1, Math.round(Number(seriesSumRangeStart?.value || 1)));
  const rangeEnd = Math.max(1, Math.round(Number(seriesSumRangeEnd?.value || totalFrames)));
  if (mode === "range" && rangeStart > rangeEnd) {
    setStatus("Range start must be <= range end");
    return;
  }

  let normalizeFrame = null;
  if (normalizeEnabled) {
    normalizeFrame = Math.max(1, Math.min(totalFrames, Math.round(Number(seriesSumNormalizeFrame?.value || 1))));
    if (seriesSumNormalizeFrame) {
      seriesSumNormalizeFrame.value = String(normalizeFrame);
    }
  }

  const payload = {
    file: state.file,
    dataset: state.dataset,
    mode,
    step,
    operation,
    normalize_frame: normalizeFrame,
    range_start: mode === "range" ? rangeStart : null,
    range_end: mode === "range" ? rangeEnd : null,
    output_path: (seriesSumOutput?.value || "").trim(),
    format: (seriesSumFormat?.value || "hdf5").toLowerCase(),
    apply_mask: Boolean(seriesSumMask?.checked),
  };
  try {
    stopSeriesSumPolling();
    state.seriesSum.running = true;
    state.seriesSum.jobId = "";
    state.seriesSum.outputs = [];
    state.seriesSum.openTarget = "";
    setSeriesSumProgress(0, "Submitting job…");
    updateSeriesSumUi();
    const data = await fetchJSONWithInit(`${API}/analysis/series-sum/start`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    state.seriesSum.jobId = String(data.job_id || "");
    state.seriesSum.running = true;
    setSeriesSumProgress(0.01, "Queued");
    setStatus("Series summing started");
    updateSeriesSumUi();
    pollSeriesSumStatus();
  } catch (err) {
    console.error(err);
    state.seriesSum.running = false;
    setSeriesSumProgress(0, "Start failed");
    updateSeriesSumUi();
    setStatus("Failed to start series summing");
  }
}

function drawResolutionOverlay() {
  if (!resolutionOverlay || !resolutionCtx || !canvasWrap) return;
  const metrics = syncOverlayCanvas(resolutionOverlay, resolutionCtx);
  if (!metrics) return;
  const { width, height } = metrics;
  resolutionCtx.clearRect(0, 0, width, height);
  if (!analysisState.ringsEnabled || !state.hasFrame) return;
  const params = getRingParams();
  if (!params.distanceMm || !params.pixelSizeUm || !params.energyEv) return;
  const lambda = 12398.4193 / params.energyEv;
  if (!Number.isFinite(lambda) || lambda <= 0) return;
  const zoom = state.zoom || 1;
  const offsetX = state.renderOffsetX || 0;
  const offsetY = state.renderOffsetY || 0;
  const viewX = canvasWrap.scrollLeft / zoom;
  const viewY = canvasWrap.scrollTop / zoom;
  const centerX = (params.centerX - viewX) * zoom + offsetX;
  const centerY = (params.centerY - viewY) * zoom + offsetY;
  const pixelSizeMm = params.pixelSizeUm / 1000;
  if (!Number.isFinite(pixelSizeMm) || pixelSizeMm <= 0) return;

  resolutionCtx.save();
  resolutionCtx.setLineDash([6, 6]);
  resolutionCtx.lineJoin = "round";
  resolutionCtx.lineCap = "round";
  const fontSize = 14;
  resolutionCtx.font = `${fontSize}px 'Avenir', 'Segoe UI', sans-serif`;
  resolutionCtx.textBaseline = "middle";
  const labelAngle = -Math.PI / 6;
  params.rings.forEach((d) => {
    const sinArg = lambda / (2 * d);
    if (!Number.isFinite(sinArg) || sinArg <= 0 || sinArg >= 1) return;
    const twoTheta = 2 * Math.asin(sinArg);
    const radiusMm = params.distanceMm * Math.tan(twoTheta);
    const radiusPx = radiusMm / pixelSizeMm;
    if (!Number.isFinite(radiusPx) || radiusPx <= 0) return;
    const screenRadius = radiusPx * zoom;
    if (screenRadius < 5) return;
    resolutionCtx.beginPath();
    resolutionCtx.arc(centerX, centerY, screenRadius, 0, Math.PI * 2);
    resolutionCtx.lineWidth = 3.5;
    resolutionCtx.strokeStyle = "rgba(255, 255, 255, 0.45)";
    resolutionCtx.stroke();
    resolutionCtx.lineWidth = 2;
    resolutionCtx.strokeStyle = "rgba(20, 80, 170, 0.95)";
    resolutionCtx.stroke();

    const labelX = centerX + Math.cos(labelAngle) * screenRadius;
    const labelY = centerY + Math.sin(labelAngle) * screenRadius;
    const label = Number.isFinite(d) ? `${d.toFixed(2).replace(/\.00$/, "")} Å` : "Å";
    const textX = labelX + 8;
    const textY = labelY;
    const textWidth = resolutionCtx.measureText(label).width;
    const padX = 6;
    const padY = 3;
    resolutionCtx.fillStyle = "rgba(10, 20, 40, 0.55)";
    resolutionCtx.fillRect(textX - padX, textY - fontSize / 2 - padY, textWidth + padX * 2, fontSize + padY * 2);
    resolutionCtx.lineWidth = 3;
    resolutionCtx.strokeStyle = "rgba(0, 0, 0, 0.7)";
    resolutionCtx.strokeText(label, textX, textY);
    resolutionCtx.fillStyle = "rgba(230, 240, 255, 0.98)";
    resolutionCtx.fillText(label, textX, textY);
  });

  if (params.centerKnown) {
    const arm = Math.max(10, Math.min(22, 10 + Math.log2(Math.max(1, zoom)) * 4));
    resolutionCtx.setLineDash([]);
    resolutionCtx.beginPath();
    resolutionCtx.moveTo(centerX - arm, centerY);
    resolutionCtx.lineTo(centerX + arm, centerY);
    resolutionCtx.moveTo(centerX, centerY - arm);
    resolutionCtx.lineTo(centerX, centerY + arm);
    resolutionCtx.lineWidth = 4;
    resolutionCtx.strokeStyle = "rgba(0, 0, 0, 0.72)";
    resolutionCtx.stroke();
    resolutionCtx.beginPath();
    resolutionCtx.moveTo(centerX - arm, centerY);
    resolutionCtx.lineTo(centerX + arm, centerY);
    resolutionCtx.moveTo(centerX, centerY - arm);
    resolutionCtx.lineTo(centerX, centerY + arm);
    resolutionCtx.lineWidth = 2.2;
    resolutionCtx.strokeStyle = "rgba(255, 65, 65, 0.96)";
    resolutionCtx.stroke();
  }
  resolutionCtx.restore();
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
  const viewX = canvasWrap.scrollLeft / zoom;
  const viewY = canvasWrap.scrollTop / zoom;
  const x0 = (roiState.start.x - viewX) * zoom + offsetX;
  const y0 = (roiState.start.y - viewY) * zoom + offsetY;
  const x1 = (roiState.end.x - viewX) * zoom + offsetX;
  const y1 = (roiState.end.y - viewY) * zoom + offsetY;
  return { x0, y0, x1, y1, zoom };
}

function getRoiHandleAt(event) {
  if (!roiState.active || roiState.mode === "none") return null;
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
}

function scheduleOverview() {
  if (overviewScheduled) return;
  overviewScheduled = true;
  window.requestAnimationFrame(() => {
    overviewScheduled = false;
    drawOverview();
  });
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

function schedulePixelOverlay() {
  if (pixelOverlayScheduled) return;
  pixelOverlayScheduled = true;
  window.requestAnimationFrame(() => {
    pixelOverlayScheduled = false;
    drawPixelOverlay();
  });
}

function getOverviewMetrics() {
  if (!overviewCanvas) return null;
  const wrap = overviewCanvas.parentElement;
  const width = wrap?.clientWidth || 1;
  const height = wrap?.clientHeight || 1;
  const imgW = state.width || 1;
  const imgH = state.height || 1;
  const scale = Math.min(width / imgW, height / imgH);
  const drawW = imgW * scale;
  const drawH = imgH * scale;
  const offsetX = (width - drawW) / 2;
  const offsetY = (height - drawH) / 2;
  return { width, height, imgW, imgH, scale, offsetX, offsetY };
}

function getViewRect() {
  const imgW = state.width;
  const imgH = state.height;
  if (!imgW || !imgH) return null;
  const zoom = state.zoom || 1;
  const scaleX = zoom;
  const scaleY = zoom;
  const viewW = canvasWrap.clientWidth / scaleX;
  const viewH = canvasWrap.clientHeight / scaleY;
  const viewWClamped = Math.min(viewW, imgW);
  const viewHClamped = Math.min(viewH, imgH);
  let viewX = canvasWrap.scrollLeft / scaleX;
  let viewY = canvasWrap.scrollTop / scaleY;
  viewX = Math.max(0, Math.min(imgW - viewWClamped, viewX));
  viewY = Math.max(0, Math.min(imgH - viewHClamped, viewY));
  return { viewX, viewY, viewW: viewWClamped, viewH: viewHClamped, scaleX, scaleY };
}

function overviewEventToImage(event) {
  if (!overviewCanvas || !state.hasFrame || !state.width || !state.height) return null;
  const metrics = getOverviewMetrics();
  if (!metrics) return null;
  const rect = overviewCanvas.getBoundingClientRect();
  const x = (event.clientX - rect.left) * (metrics.width / rect.width);
  const y = (event.clientY - rect.top) * (metrics.height / rect.height);
  const imgX = (x - metrics.offsetX) / metrics.scale;
  const imgY = (y - metrics.offsetY) / metrics.scale;
  return {
    x: Math.max(0, Math.min(metrics.imgW, imgX)),
    y: Math.max(0, Math.min(metrics.imgH, imgY)),
  };
}

function overviewEventToOverview(event) {
  if (!overviewCanvas) return null;
  const metrics = getOverviewMetrics();
  if (!metrics) return null;
  const rect = overviewCanvas.getBoundingClientRect();
  const x = (event.clientX - rect.left) * (metrics.width / rect.width);
  const y = (event.clientY - rect.top) * (metrics.height / rect.height);
  return { x, y, metrics };
}

function panToImageCenter(x, y) {
  const view = getViewRect();
  if (!view) return;
  const maxX = Math.max(0, state.width - view.viewW);
  const maxY = Math.max(0, state.height - view.viewH);
  const targetX = Math.max(0, Math.min(maxX, x - view.viewW / 2));
  const targetY = Math.max(0, Math.min(maxY, y - view.viewH / 2));
  canvasWrap.scrollLeft = targetX * view.scaleX;
  canvasWrap.scrollTop = targetY * view.scaleY;
  scheduleOverview();
}

function scrollToView(viewX, viewY) {
  if (!state.width || !state.height) return;
  const zoom = state.zoom || 1;
  canvasWrap.scrollLeft = viewX * zoom;
  canvasWrap.scrollTop = viewY * zoom;
}

function getOverviewHandleAt(point) {
  if (!overviewRect) return null;
  const handleSize = overviewRect.handleSize || 8;
  const threshold = handleSize;
  for (const handle of overviewRect.handles) {
    if (Math.abs(point.x - handle.x) <= threshold && Math.abs(point.y - handle.y) <= threshold) {
      return handle.name;
    }
  }
  return null;
}

function getAnchorForHandle(view, handle, keepCenter) {
  if (keepCenter) {
    return { x: view.viewX + view.viewW / 2, y: view.viewY + view.viewH / 2 };
  }
  switch (handle) {
    case "nw":
      return { x: view.viewX + view.viewW, y: view.viewY + view.viewH };
    case "ne":
      return { x: view.viewX, y: view.viewY + view.viewH };
    case "se":
      return { x: view.viewX, y: view.viewY };
    case "sw":
      return { x: view.viewX + view.viewW, y: view.viewY };
    default:
      return null;
  }
}

function resizeViewFromHandle(point, handle, keepCenter) {
  if (!overviewAnchor || !state.width || !state.height) return;
  const anchor = overviewAnchor;
  const aspect = canvasWrap.clientWidth / canvasWrap.clientHeight || 1;
  let width;
  let height;

  if (keepCenter) {
    const dx = Math.abs(point.x - anchor.x);
    const dy = Math.abs(point.y - anchor.y);
    if (handle === "n" || handle === "s") {
      height = dy * 2;
      width = height * aspect;
    } else if (handle === "e" || handle === "w") {
      width = dx * 2;
      height = width / aspect;
    } else {
      width = dx * 2;
      height = dy * 2;
      if (width / height > aspect) {
        height = width / aspect;
      } else {
        width = height * aspect;
      }
    }
  } else if (handle === "n" || handle === "s") {
    height = Math.abs(point.y - anchor.y);
    width = height * aspect;
  } else if (handle === "e" || handle === "w") {
    width = Math.abs(point.x - anchor.x);
    height = width / aspect;
  } else {
    width = Math.abs(point.x - anchor.x);
    height = Math.abs(point.y - anchor.y);
    if (width / height > aspect) {
      height = width / aspect;
    } else {
      width = height * aspect;
    }
  }

  if (!isFinite(width) || !isFinite(height) || width <= 0 || height <= 0) return;
  const minViewW = Math.max(30, state.width * 0.02);
  if (width < minViewW) {
    width = minViewW;
    height = width / aspect;
  }
  width = Math.min(width, state.width);
  height = Math.min(height, state.height);

  let viewX;
  let viewY;
  if (keepCenter) {
    viewX = anchor.x - width / 2;
    viewY = anchor.y - height / 2;
  } else {
    switch (handle) {
      case "nw":
        viewX = anchor.x - width;
        viewY = anchor.y - height;
        break;
      case "ne":
        viewX = anchor.x;
        viewY = anchor.y - height;
        break;
      case "se":
        viewX = anchor.x;
        viewY = anchor.y;
        break;
      case "sw":
        viewX = anchor.x - width;
        viewY = anchor.y;
        break;
      case "n":
        viewX = anchor.x - width / 2;
        viewY = anchor.y - height;
        break;
      case "s":
        viewX = anchor.x - width / 2;
        viewY = anchor.y;
        break;
      case "e":
        viewX = anchor.x;
        viewY = anchor.y - height / 2;
        break;
      case "w":
        viewX = anchor.x - width;
        viewY = anchor.y - height / 2;
        break;
      default:
        return;
    }
  }

  viewX = Math.max(0, Math.min(state.width - width, viewX));
  viewY = Math.max(0, Math.min(state.height - height, viewY));

  const zoomX = canvasWrap.clientWidth / width;
  const zoomY = canvasWrap.clientHeight / height;
  const zoom = Math.min(6, Math.max(0.5, Math.min(zoomX, zoomY)));
  setZoom(zoom);
  window.requestAnimationFrame(() => {
    scrollToView(viewX, viewY);
    scheduleOverview();
  });
}

function drawOverview() {
  if (!overviewCanvas || !overviewCtx) return;
  const metrics = getOverviewMetrics();
  if (!metrics) return;
  const { width, height, imgW, imgH, scale, offsetX, offsetY } = metrics;
  const dpr = window.devicePixelRatio || 1;
  overviewCanvas.width = Math.max(1, Math.floor(width * dpr));
  overviewCanvas.height = Math.max(1, Math.floor(height * dpr));
  overviewCtx.setTransform(dpr, 0, 0, dpr, 0, 0);
  overviewCtx.fillStyle = "#1e1e1e";
  overviewCtx.fillRect(0, 0, width, height);

  if (!state.hasFrame || !state.width || !state.height) {
    overviewCtx.strokeStyle = "rgba(255,255,255,0.2)";
    overviewCtx.strokeRect(0.5, 0.5, width - 1, height - 1);
    overviewCtx.fillStyle = "rgba(200,200,200,0.6)";
    overviewCtx.font = "10px \"Lucida Grande\", \"Helvetica Neue\", Arial, sans-serif";
    overviewCtx.textAlign = "center";
    overviewCtx.textBaseline = "middle";
    overviewCtx.fillText("No image", width / 2, height / 2);
    overviewRect = null;
    return;
  }

  const drawW = imgW * scale;
  const drawH = imgH * scale;
  overviewCtx.drawImage(canvas, 0, 0, imgW, imgH, offsetX, offsetY, drawW, drawH);

  const view = getViewRect();
  if (!view) {
    overviewRect = null;
    return;
  }
  const rectX = offsetX + view.viewX * scale;
  const rectY = offsetY + view.viewY * scale;
  const rectW = view.viewW * scale;
  const rectH = view.viewH * scale;

  overviewCtx.fillStyle = "rgba(0, 0, 0, 0.35)";
  overviewCtx.fillRect(offsetX, offsetY, drawW, drawH);
  overviewCtx.drawImage(
    canvas,
    view.viewX,
    view.viewY,
    view.viewW,
    view.viewH,
    rectX,
    rectY,
    rectW,
    rectH
  );

  overviewCtx.strokeStyle = "rgba(0, 0, 0, 0.65)";
  overviewCtx.lineWidth = 3;
  overviewCtx.strokeRect(rectX, rectY, rectW, rectH);
  overviewCtx.strokeStyle = "rgba(140, 210, 255, 0.95)";
  overviewCtx.lineWidth = 1.5;
  overviewCtx.strokeRect(rectX, rectY, rectW, rectH);
  overviewCtx.fillStyle = "rgba(110, 181, 255, 0.12)";
  overviewCtx.fillRect(rectX, rectY, rectW, rectH);

  const handleSize = 7;
  const half = handleSize / 2;
  const handles = [
    { name: "nw", x: rectX, y: rectY },
    { name: "ne", x: rectX + rectW, y: rectY },
    { name: "se", x: rectX + rectW, y: rectY + rectH },
    { name: "sw", x: rectX, y: rectY + rectH },
  ];
  overviewCtx.fillStyle = "rgba(220, 245, 255, 0.95)";
  overviewCtx.strokeStyle = "rgba(10, 20, 30, 0.8)";
  overviewCtx.lineWidth = 1;
  handles.forEach((handle) => {
    overviewCtx.fillRect(handle.x - half, handle.y - half, handleSize, handleSize);
    overviewCtx.strokeRect(handle.x - half, handle.y - half, handleSize, handleSize);
  });

  overviewRect = { rectX, rectY, rectW, rectH, handles, handleSize, view, metrics };
}

function setZoom(value) {
  const minZoom = getMinZoom();
  const clamped = Math.max(minZoom, Math.min(MAX_ZOOM, Number(value)));
  state.zoom = clamped;
  const offsetX =
    canvasWrap && state.width
      ? Math.max(0, (canvasWrap.clientWidth - state.width * clamped) / 2)
      : 0;
  state.renderOffsetX = Number.isFinite(offsetX) ? offsetX : 0;
  state.renderOffsetY = 0;
  canvas.style.transform = `translate(${state.renderOffsetX}px, ${state.renderOffsetY}px) scale(${clamped})`;
  if (zoomRange) {
    zoomRange.min = String(minZoom);
    zoomRange.value = String(clamped);
  }
  if (zoomValue) {
    zoomValue.textContent = `${clamped.toFixed(1)}x`;
  }
  schedulePixelOverlay();
  scheduleRoiOverlay();
  scheduleResolutionOverlay();
  schedulePeakOverlay();
}

function zoomAt(clientX, clientY, nextZoom) {
  if (!canvasWrap) {
    setZoom(nextZoom);
    return;
  }
  const rect = canvasWrap.getBoundingClientRect();
  const x = clientX - rect.left;
  const y = clientY - rect.top;
  const prevZoom = state.zoom || 1;
  const prevOffsetX = state.renderOffsetX || 0;
  const prevOffsetY = state.renderOffsetY || 0;
  const worldX = (canvasWrap.scrollLeft + x - prevOffsetX) / prevZoom;
  const worldY = (canvasWrap.scrollTop + y - prevOffsetY) / prevZoom;

  setZoom(nextZoom);

  const newOffsetX = state.renderOffsetX || 0;
  const newOffsetY = state.renderOffsetY || 0;
  const newScrollLeft = worldX * state.zoom - x + newOffsetX;
  const newScrollTop = worldY * state.zoom - y + newOffsetY;
  const maxScrollLeft = Math.max(0, canvasWrap.scrollWidth - canvasWrap.clientWidth);
  const maxScrollTop = Math.max(0, canvasWrap.scrollHeight - canvasWrap.clientHeight);
  canvasWrap.scrollLeft = Math.max(0, Math.min(maxScrollLeft, newScrollLeft));
  canvasWrap.scrollTop = Math.max(0, Math.min(maxScrollTop, newScrollTop));
  scheduleOverview();
}

function normalizeWheelDelta(event) {
  let delta = event.deltaY;
  if (event.deltaMode === 1) {
    delta *= 16;
  } else if (event.deltaMode === 2) {
    delta *= 120;
  }
  if (!Number.isFinite(delta)) return 0;
  return Math.max(-200, Math.min(200, delta));
}

function touchDistance(t0, t1) {
  const dx = t1.clientX - t0.clientX;
  const dy = t1.clientY - t0.clientY;
  return Math.hypot(dx, dy);
}

function touchMidpoint(t0, t1) {
  return {
    x: (t0.clientX + t1.clientX) * 0.5,
    y: (t0.clientY + t1.clientY) * 0.5,
  };
}

function startTouchGesture(touches) {
  if (!canvasWrap || touches.length < 2) return;
  const t0 = touches[0];
  const t1 = touches[1];
  touchGestureDistance = touchDistance(t0, t1);
  touchGestureMid = touchMidpoint(t0, t1);
  touchGestureActive = true;
  canvasWrap.classList.add("is-panning");
}

function stopTouchGesture() {
  touchGestureActive = false;
  touchGestureDistance = 0;
  touchGestureMid = null;
  if (canvasWrap) {
    canvasWrap.classList.remove("is-panning");
  }
}

function updateTouchGesture(touches) {
  if (!canvasWrap || touches.length < 2) return;
  const t0 = touches[0];
  const t1 = touches[1];
  const nextDistance = touchDistance(t0, t1);
  const nextMid = touchMidpoint(t0, t1);
  if (!Number.isFinite(nextDistance) || nextDistance <= 0) return;

  if (!touchGestureActive || !touchGestureMid || touchGestureDistance <= 0) {
    touchGestureDistance = nextDistance;
    touchGestureMid = nextMid;
    touchGestureActive = true;
    return;
  }

  const minZoom = getMinZoom();
  const scale = Math.max(0.25, Math.min(4, nextDistance / touchGestureDistance));
  const nextZoom = Math.max(minZoom, Math.min(MAX_ZOOM, (state.zoom || 1) * scale));
  zoomAt(nextMid.x, nextMid.y, nextZoom);

  const dx = nextMid.x - touchGestureMid.x;
  const dy = nextMid.y - touchGestureMid.y;
  if (dx || dy) {
    const maxScrollLeft = Math.max(0, canvasWrap.scrollWidth - canvasWrap.clientWidth);
    const maxScrollTop = Math.max(0, canvasWrap.scrollHeight - canvasWrap.clientHeight);
    canvasWrap.scrollLeft = Math.max(0, Math.min(maxScrollLeft, canvasWrap.scrollLeft - dx));
    canvasWrap.scrollTop = Math.max(0, Math.min(maxScrollTop, canvasWrap.scrollTop - dy));
  }

  touchGestureDistance = nextDistance;
  touchGestureMid = nextMid;
}

function stepWheelZoom() {
  if (zoomWheelTarget === null || !zoomWheelPivot) {
    zoomWheelRaf = null;
    return;
  }
  const current = state.zoom || 1;
  const minZoom = getMinZoom();
  const target = Math.max(minZoom, Math.min(MAX_ZOOM, zoomWheelTarget));
  const next = current + (target - current) * 0.35;
  zoomAt(zoomWheelPivot.x, zoomWheelPivot.y, next);
  if (Math.abs(target - next) < 0.001) {
    zoomWheelTarget = null;
    zoomWheelRaf = null;
    return;
  }
  zoomWheelRaf = window.requestAnimationFrame(stepWheelZoom);
}

function fitImageToView() {
  if (!canvasWrap || !state.width || !state.height) return;
  const scale = Math.min(
    canvasWrap.clientWidth / state.width,
    canvasWrap.clientHeight / state.height
  );
  if (!Number.isFinite(scale) || scale <= 0) return;
  setZoom(scale);
  canvasWrap.scrollLeft = 0;
  canvasWrap.scrollTop = 0;
  scheduleOverview();
  schedulePixelOverlay();
}

function updateFpsLabel() {
  if (fpsValue) {
    fpsValue.textContent = `${state.fps} fps`;
  }
}

function updateThresholdOptions() {
  if (!thresholdSelect || !thresholdField) return;
  const count = Math.max(1, state.thresholdCount || 1);
  const show = count > 1 && state.autoload.mode !== "simplon";
  thresholdField.classList.toggle("is-hidden", !show);
  if (toolbarThresholdWrap) {
    toolbarThresholdWrap.classList.toggle("is-hidden", !show);
  }
  thresholdSelect.innerHTML = "";
  if (toolbarThresholdSelect) {
    toolbarThresholdSelect.innerHTML = "";
  }
  const energies = Array.isArray(state.thresholdEnergies) ? state.thresholdEnergies : [];
  for (let i = 0; i < count; i += 1) {
    const energy = energies[i];
    const energyText = Number.isFinite(energy) ? ` ${formatEnergy(energy)} eV` : "";
    const label = `Thr${i + 1}${energyText}`;
    thresholdSelect.appendChild(option(label, String(i)));
    if (toolbarThresholdSelect) {
      toolbarThresholdSelect.appendChild(option(label, String(i)));
    }
  }
  const idx = Math.max(0, Math.min(count - 1, state.thresholdIndex || 0));
  state.thresholdIndex = idx;
  thresholdSelect.value = String(idx);
  if (toolbarThresholdSelect) {
    toolbarThresholdSelect.value = String(idx);
  }
  thresholdSelect.disabled = count <= 1;
  if (toolbarThresholdSelect) {
    toolbarThresholdSelect.disabled = count <= 1;
  }
}

async function setThresholdIndex(nextIndex) {
  const count = Math.max(1, state.thresholdCount || 1);
  const clamped = Math.max(0, Math.min(count - 1, Math.round(nextIndex)));
  if (clamped === state.thresholdIndex) return;
  state.thresholdIndex = clamped;
  if (thresholdSelect) thresholdSelect.value = String(clamped);
  if (toolbarThresholdSelect) toolbarThresholdSelect.value = String(clamped);
  state.maskFile = "";
  await loadMask(true);
  requestFrame(state.frameIndex);
}

function setFps(value) {
  const clamped = Math.max(1, Math.min(10, Math.round(value)));
  state.fps = clamped;
  if (fpsRange) {
    fpsRange.value = String(clamped);
  }
  updateFpsLabel();
  if (state.playing) {
    stopPlayback();
    startPlayback();
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

function applyPanelState() {
  if (!toolsPanel || !appLayout) return;
  
  const isMobile = window.matchMedia("(hover: none) and (pointer: coarse)").matches;
  
  toolsPanel.classList.toggle("is-collapsed", state.panelCollapsed);
  
  // Mobile: show/hide panel with translation
  if (isMobile && window.innerWidth < 768) {
    toolsPanel.classList.toggle("is-visible", !state.panelCollapsed);
  } else {
    // Desktop/tablet: use panel width
    const maxPanelWidth =
      window.innerWidth < 900 ? Math.max(220, Math.floor(window.innerWidth * 0.7)) : 900;
    const targetWidth = Math.max(220, Math.min(maxPanelWidth, state.panelWidth));
    const width = state.panelCollapsed ? 28 : targetWidth;
    appLayout.style.setProperty("--panel-width", `${width}px`);
    document.documentElement.style.setProperty("--panel-width", `${width}px`);
  }
  
  if (panelEdgeToggle) {
    panelEdgeToggle.textContent = state.panelCollapsed ? "◀" : "▶";
    panelEdgeToggle.setAttribute("aria-label", state.panelCollapsed ? "Expand panel" : "Collapse panel");
  }
  if (panelFab) {
    panelFab.classList.toggle("is-collapsed", state.panelCollapsed);
    panelFab.textContent = state.panelCollapsed ? "◀" : "▶";
    panelFab.setAttribute("aria-label", state.panelCollapsed ? "Open panel" : "Collapse panel");
  }
  scheduleOverview();
  scheduleHistogram();
}

function togglePanel() {
  state.panelCollapsed = !state.panelCollapsed;
  applyPanelState();
  try {
    localStorage.setItem("albis.panelCollapsed", String(state.panelCollapsed));
    localStorage.setItem("albis.panelWidth", String(state.panelWidth));
  } catch {
    // ignore storage errors
  }
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

function setPanelWidth(width) {
  const maxPanelWidth =
    window.innerWidth < 900 ? Math.max(220, Math.floor(window.innerWidth * 0.7)) : 900;
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
    const next = state.frameIndex + step >= state.frameCount ? 0 : state.frameIndex + step;
    requestFrame(next);
  }, Math.max(1000 / state.fps, 50));
}

function requestFrame(index) {
  const hasSeries = Array.isArray(state.seriesFiles) && state.seriesFiles.length > 0;
  if (!state.frameCount || (!state.dataset && !hasSeries) || !state.file) return;
  const clamped = Math.max(0, Math.min(state.frameCount - 1, index));
  state.frameIndex = clamped;
  updateFrameControls();
  updateToolbar();
  if (state.isLoading) {
    state.pendingFrame = clamped;
    return;
  }
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

function showSplash() {
  splash?.classList.remove("is-hidden");
}

function setSplashStatus(text) {
  if (!splashStatus) return;
  splashStatus.textContent = text || "";
}

function hideSplash() {
  splash?.classList.add("is-hidden");
}

function updateToolbar() {
  if (!toolbarPath) return;
  if (!state.file) {
    toolbarPath.textContent = "No file loaded";
    updateSeriesSumUi();
    return;
  }
  let frameLabel = "";
  if (state.frameCount > 1) {
    frameLabel = `${state.frameIndex + 1} / ${state.frameCount}`;
  } else if (state.autoload.mode !== "file" && state.autoload.lastUpdate) {
    frameLabel = formatTimeStamp(state.autoload.lastUpdate);
  }
  const datasetLabel = state.dataset ? ` ${state.dataset}` : "";
  const suffix = frameLabel ? `  ${frameLabel}` : "";
  toolbarPath.textContent = `${state.file}${datasetLabel}${suffix}`;
  updateSeriesSumUi();
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

function closeSubmenus() {
  submenuParents.forEach((parent) => parent.classList.remove("is-open"));
}

function openMenu(menu, anchor) {
  if (!dropdown) return;
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

function defaultSeriesSumOutputPath(filePath) {
  if (!filePath) return "output/series_sum";
  const normalized = String(filePath).replace(/\\/g, "/");
  const lastSlash = normalized.lastIndexOf("/");
  const lastDot = normalized.lastIndexOf(".");
  const base = lastDot > lastSlash ? normalized.slice(0, lastDot) : normalized;
  return `${base}_series_sum`;
}

function syncSeriesSumOutputPath(force = false) {
  const autoPath = defaultSeriesSumOutputPath(state.file);
  if (!seriesSumOutput) {
    state.seriesSum.autoOutputPath = autoPath;
    return;
  }
  const current = (seriesSumOutput.value || "").trim();
  if (force || !current || current === state.seriesSum.autoOutputPath) {
    seriesSumOutput.value = autoPath;
  }
  state.seriesSum.autoOutputPath = autoPath;
}

function updateSeriesSumProgressOpenState() {
  if (!seriesSumProgress) return;
  const canOpen = !state.seriesSum.running && Boolean(state.seriesSum.openTarget);
  seriesSumProgress.classList.toggle("is-clickable", canOpen);
  seriesSumProgress.title = canOpen ? "Click to open output" : "";
}

async function openSeriesSumOutputTarget() {
  if (state.seriesSum.running || !state.seriesSum.openTarget) return;
  try {
    await ensureFileMode();
    await loadAutoloadFile(state.seriesSum.openTarget);
    setStatus("Opened series output in ALBIS");
  } catch (err) {
    console.error(err);
    setStatus("Failed to open series output");
  }
}

async function openFileModal() {
  closeMenu();
  await ensureFileMode();

  // If backend is local, use native dialog
  if (backendIsLocal) {
    try {
      const res = await fetch(`${API}/choose-file`);
      if (res.status === 204) return;
      if (!res.ok) {
        throw new Error(`Picker failed: ${res.status}`);
      }
      const data = await res.json();
      const path = data?.path;
      if (!path) return;
      const folder = dirnameFromPath(path);
      if (autoloadDir) autoloadDir.value = folder;
      state.autoload.dir = folder;
      state.file = path;
      syncSeriesSumOutputPath();
      await loadFiles();
      if (fileSelect) {
        const existing = Array.from(fileSelect.options).some((opt) => opt.value === path);
        if (!existing) {
          fileSelect.appendChild(option(fileLabel(path), path));
        }
        fileSelect.value = path;
      }
      if (isHdfFile(path)) {
        await loadDatasets();
      } else {
        await loadImageSeries(path);
      }
      return;
    } catch (err) {
      console.error(err);
    }
  } else if (filesystemMode?.value === "local") {
    // Use HTML5 file input for local filesystem on remote backend
    fileInput.accept = ".h5,.hdf5,.tif,.tiff,.cbf,.cbf.gz,.edf";
    fileInput.onchange = async (event) => {
      const file = event.target.files?.[0];
      if (!file) return;
      
      // For local files, we store the filename but note that actual file reading
      // from user's machine requires a backend upload endpoint
      try {
        const folder = dirnameFromPath(file.name);
        if (autoloadDir) autoloadDir.value = folder;
        state.autoload.dir = folder;
        state.file = file.name;
        syncSeriesSumOutputPath();
        setStatus(`Local file selected: ${file.name} (upload endpoint needed for data access)`);
        return;
      } catch (err) {
        console.error(err);
        setStatus("Failed to select local file");
      }
    };
    fileInput.click();
  } else {
    // Use web file browser for remote filesystem
    try {
      const selectedFile = await openFileDialog();
      if (!selectedFile) return;
      
      const folder = dirnameFromPath(selectedFile);
      if (autoloadDir) autoloadDir.value = folder;
      state.autoload.dir = folder;
      state.file = selectedFile;
      syncSeriesSumOutputPath();
      await loadFiles();
      if (fileSelect) {
        const existing = Array.from(fileSelect.options).some((opt) => opt.value === selectedFile);
        if (!existing) {
          fileSelect.appendChild(option(fileLabel(selectedFile), selectedFile));
        }
        fileSelect.value = selectedFile;
      }
      if (isHdfFile(selectedFile)) {
        await loadDatasets();
      } else {
        await loadImageSeries(selectedFile);
      }
      return;
    } catch (err) {
      console.error(err);
    }
  }

  // Fallback to HTML5 file input
  fileInput?.click();
}

function showUploadProgress() {
  if (!uploadBar) return;
  uploadBar.classList.add("is-active");
  uploadBar.classList.remove("is-processing");
  uploadBar.setAttribute("aria-hidden", "false");
  if (uploadBarFill) uploadBarFill.style.width = "0%";
  if (uploadBarText) uploadBarText.textContent = "Uploading 0%";
}

function updateUploadProgress(percent) {
  if (!uploadBar) return;
  const value = Math.max(0, Math.min(100, percent));
  if (uploadBarFill) uploadBarFill.style.width = `${value}%`;
  if (uploadBarText) uploadBarText.textContent = `Uploading ${value}%`;
}

function hideUploadProgress() {
  if (!uploadBar) return;
  uploadBar.classList.remove("is-active");
  uploadBar.classList.remove("is-processing");
  uploadBar.setAttribute("aria-hidden", "true");
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

function flashDataSection() {
  if (!dataSection) return;
  setPanelTab("data");
  dataSection.scrollIntoView({ behavior: "smooth", block: "start" });
  dataSection.classList.add("flash");
  window.setTimeout(() => dataSection.classList.remove("flash"), 800);
}

function setPanelTab(tabId, persist = true) {
  panelTabState = tabId;
  panelTabs.forEach((tab) => {
    tab.classList.toggle("is-active", tab.dataset.panelTab === tabId);
  });
  panelTabContents.forEach((panel) => {
    panel.classList.toggle("is-active", panel.dataset.panelTab === tabId);
  });
  if (persist) {
    try {
      localStorage.setItem("albis.panelTab", tabId);
    } catch {
      // ignore storage errors
    }
  }
  scheduleOverview();
  scheduleHistogram();
  schedulePixelOverlay();
  scheduleResolutionOverlay();
}

function setDataControlsForHdf5() {
  if (datasetSelect) datasetSelect.disabled = false;
  if (thresholdSelect) thresholdSelect.disabled = false;
  if (toolbarThresholdSelect) toolbarThresholdSelect.disabled = false;
  if (frameRange) frameRange.disabled = false;
  if (frameIndex) frameIndex.disabled = false;
  if (frameStep) frameStep.disabled = false;
  if (fpsRange) fpsRange.disabled = false;
  updateInspectorHeaderVisibility(state.file);
}

function setDataControlsForImage() {
  if (datasetSelect) datasetSelect.disabled = true;
  if (thresholdSelect) thresholdSelect.disabled = true;
  if (toolbarThresholdSelect) toolbarThresholdSelect.disabled = true;
  if (frameRange) frameRange.disabled = true;
  if (frameIndex) frameIndex.disabled = true;
  if (frameStep) frameStep.disabled = true;
  if (fpsRange) fpsRange.disabled = true;
  updateInspectorHeaderVisibility(state.file);
}

function setDataControlsForSeries() {
  if (datasetSelect) datasetSelect.disabled = true;
  if (thresholdSelect) thresholdSelect.disabled = true;
  if (toolbarThresholdSelect) toolbarThresholdSelect.disabled = true;
  if (frameRange) frameRange.disabled = false;
  if (frameIndex) frameIndex.disabled = false;
  if (frameStep) frameStep.disabled = false;
  if (fpsRange) fpsRange.disabled = false;
  updateInspectorHeaderVisibility(state.file);
}

function formatTimeStamp(ts) {
  if (!ts) return "";
  return new Date(ts).toLocaleTimeString();
}

function setAutoloadStatus(text, markUpdate = false) {
  if (markUpdate) {
    state.autoload.lastUpdate = Date.now();
  }
  updateAutoloadMeta();
  updateLiveBadge();
}

function setAutoloadLatest(text) {
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

function updateLiveBadge() {
  if (!liveBadge) return;
  if (!state.autoload.running || state.autoload.mode !== "simplon") {
    liveBadge.classList.remove("is-active", "is-wait");
    liveBadge.setAttribute("aria-hidden", "true");
    return;
  }
  liveBadge.classList.add("is-active");
  const age = Date.now() - (state.autoload.lastUpdate || 0);
  const wait = !state.autoload.lastUpdate || age > state.autoload.interval * 2;
  liveBadge.classList.toggle("is-wait", wait);
  liveBadge.textContent = wait ? "WAIT" : "LIVE";
  liveBadge.setAttribute("aria-hidden", "false");
}

function updateBackendBadge() {
  if (!backendBadge) return;
  backendBadge.classList.toggle("is-off", !state.backendAlive);
  backendBadge.classList.toggle("is-active", true);
  backendBadge.textContent = state.backendAlive ? "SERVER" : "OFFLINE";
  backendBadge.setAttribute("aria-hidden", "false");
}

function updateAboutVersion() {
  if (!aboutVersion) return;
  aboutVersion.textContent = `Version ${state.backendVersion || "0.4"}`;
}

async function checkBackendHealth() {
  let alive = false;
  let version = state.backendVersion || "0.4";
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), 1500);
  try {
    const res = await fetch(`${API}/health`, { signal: controller.signal, cache: "no-store" });
    if (res.ok) {
      alive = true;
      try {
        const data = await res.json();
        if (data?.version) {
          version = String(data.version);
        }
      } catch {
        // ignore parse errors
      }
    }
  } catch {
    alive = false;
  } finally {
    window.clearTimeout(timer);
  }
  if (state.backendAlive !== alive) {
    state.backendAlive = alive;
  }
  if (state.backendVersion !== version) {
    state.backendVersion = version;
    updateAboutVersion();
  }
  updateBackendBadge();
  return alive;
}

function startBackendHeartbeat() {
  if (backendTimer) {
    window.clearInterval(backendTimer);
  }
  void checkBackendHealth();
  backendTimer = window.setInterval(() => {
    void checkBackendHealth();
  }, 4000);
}

function sleep(ms) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

async function waitForBackendReady(timeoutMs = 20000) {
  const deadline = Date.now() + timeoutMs;
  let attempts = 0;
  while (Date.now() < deadline) {
    attempts += 1;
    setSplashStatus(`Starting backend... (${attempts})`);
    const alive = await checkBackendHealth();
    if (alive) {
      setSplashStatus(`Backend ready (v${state.backendVersion || "0.4"})`);
      return true;
    }
    await sleep(250);
  }
  setSplashStatus("Backend startup is taking longer than expected...");
  return false;
}


async function fetchSettingsConfig() {
  try {
    const payload = await fetchJSON(`${API}/settings`);
    const config = payload?.config || {};
    if (typeof config?.ui?.tool_hints !== "undefined") {
      setToolHintsEnabled(Boolean(config.ui.tool_hints));
    }
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
  setSplashStatus("Ready");
}

function persistAutoloadSettings() {
  try {
    const payload = {
      mode: state.autoload.mode,
      dir: state.autoload.dir,
      interval: state.autoload.interval,
      types: state.autoload.types,
      pattern: state.autoload.pattern,
      simplonUrl: state.autoload.simplonUrl,
      simplonVersion: state.autoload.simplonVersion,
      simplonTimeout: state.autoload.simplonTimeout,
      simplonEnable: state.autoload.simplonEnable,
      autoStart: state.autoload.autoStart,
    };
    localStorage.setItem("albis.autoload", JSON.stringify(payload));
  } catch {
    // ignore storage errors
  }
}

function updateAutoloadUI() {
  if (autoloadMode) autoloadMode.value = state.autoload.mode;
  if (autoloadFolder) {
    autoloadFolder.classList.toggle("is-hidden", state.autoload.mode === "simplon");
  }
  if (autoloadWatch) autoloadWatch.classList.toggle("is-hidden", state.autoload.mode !== "watch");
  if (autoloadTypesRow) autoloadTypesRow.classList.toggle("is-hidden", state.autoload.mode === "watch");
  if (autoloadSimplon) autoloadSimplon.classList.toggle("is-hidden", state.autoload.mode !== "simplon");
  if (fileField) fileField.classList.toggle("is-hidden", state.autoload.mode === "simplon");
  if (datasetField) datasetField.classList.toggle("is-hidden", state.autoload.mode === "simplon");
  if (thresholdField) thresholdField.classList.toggle("is-hidden", state.autoload.mode === "simplon");
  if (frameRange) frameRange.closest(".field")?.classList.toggle("is-hidden", state.autoload.mode !== "file");
  if (frameStep) frameStep.closest(".field")?.classList.toggle("is-hidden", state.autoload.mode !== "file");
  if (fpsRange) fpsRange.closest(".field")?.classList.toggle("is-hidden", state.autoload.mode !== "file");
  if (autoloadStatus) {
    const meta = autoloadStatus.closest(".autoload-meta");
    if (meta) meta.classList.toggle("is-hidden", state.autoload.mode === "file");
  }
  updateAutoloadMeta();
  updateLiveBadge();
  updateThresholdOptions();
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

function loadAutoloadSettings() {
  try {
    const raw = localStorage.getItem("albis.autoload");
    if (raw) {
      const stored = JSON.parse(raw);
      if (stored && typeof stored === "object") {
        const storedMode = stored.mode || state.autoload.mode;
        state.autoload.mode = storedMode === "off" ? "file" : storedMode;
        state.autoload.dir = stored.dir || "";
        state.autoload.interval = Number(stored.interval || state.autoload.interval);
        if (stored.types && typeof stored.types === "object") {
          state.autoload.types = {
            hdf5: stored.types.hdf5 !== false,
            tiff: stored.types.tiff !== false,
            cbf: stored.types.cbf !== false,
          };
        }
        state.autoload.pattern = stored.pattern || "";
        state.autoload.simplonUrl = stored.simplonUrl || "";
        state.autoload.simplonVersion = stored.simplonVersion || state.autoload.simplonVersion;
        state.autoload.simplonTimeout = Number(stored.simplonTimeout || state.autoload.simplonTimeout);
        state.autoload.simplonEnable =
          stored.simplonEnable !== undefined ? Boolean(stored.simplonEnable) : state.autoload.simplonEnable;
        state.autoload.autoStart = Boolean(stored.autoStart);
      }
    }
  } catch {
    // ignore storage errors
  }
  if (autoloadMode) autoloadMode.value = state.autoload.mode;
  if (autoloadDir) autoloadDir.value = state.autoload.dir;
  if (autoloadInterval) autoloadInterval.value = String(state.autoload.interval || 1000);
  if (autoloadTypeHdf5) autoloadTypeHdf5.checked = state.autoload.types.hdf5;
  if (autoloadTypeTiff) autoloadTypeTiff.checked = state.autoload.types.tiff;
  if (autoloadTypeCbf) autoloadTypeCbf.checked = state.autoload.types.cbf;
  if (autoloadPattern) autoloadPattern.value = state.autoload.pattern;
  if (simplonUrl) simplonUrl.value = state.autoload.simplonUrl;
  if (simplonVersion) simplonVersion.value = state.autoload.simplonVersion;
  if (simplonTimeout) simplonTimeout.value = String(state.autoload.simplonTimeout || 500);
  if (simplonEnable) simplonEnable.checked = Boolean(state.autoload.simplonEnable);
  updateAutoloadUI();
  setAutoloadStatus("Idle");
  setAutoloadLatest("-");
  if (state.autoload.mode !== "file") {
    startAutoload();
  }
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

async function startAutoload() {
  state.autoload.mode = autoloadMode?.value || state.autoload.mode;
  state.autoload.dir = autoloadDir?.value?.trim() || "";
  state.autoload.interval = Math.max(200, Number(autoloadInterval?.value || 1000));
  state.autoload.types = {
    hdf5: autoloadTypeHdf5?.checked ?? true,
    tiff: autoloadTypeTiff?.checked ?? true,
    cbf: autoloadTypeCbf?.checked ?? true,
  };
  if (state.autoload.mode === "watch") {
    state.autoload.types = { hdf5: true, tiff: true, cbf: true };
    if (autoloadTypeHdf5) autoloadTypeHdf5.checked = true;
    if (autoloadTypeTiff) autoloadTypeTiff.checked = true;
    if (autoloadTypeCbf) autoloadTypeCbf.checked = true;
  }
  state.autoload.pattern = autoloadPattern?.value?.trim() || "";
  state.autoload.simplonUrl = simplonUrl?.value?.trim() || "";
  state.autoload.simplonVersion = simplonVersion?.value?.trim() || "1.8.0";
  state.autoload.simplonTimeout = Math.max(100, Number(simplonTimeout?.value || 500));
  state.autoload.simplonEnable = simplonEnable?.checked ?? true;
  if (state.autoload.mode === "file") {
    await stopAutoload({ keepMode: false });
    return;
  }
  await stopAutoload({ keepMode: true, disableMonitor: false });
  state.autoload.running = true;
  state.autoload.autoStart = true;
  state.autoload.lastFile = "";
  state.autoload.lastMtime = 0;
  state.autoload.lastUpdate = 0;
  state.autoload.lastPoll = 0;
  state.autoload.lastMonitorSig = "";
  updateAutoloadUI();
  updateAutoloadMeta();
  setAutoloadStatus(`Running (${state.autoload.mode === "watch" ? "Watch folder" : "SIMPLON monitor"})`);
  persistAutoloadSettings();
  if (state.autoload.mode === "simplon" && state.autoload.simplonEnable) {
    setStatus("SIMPLON monitor");
    await setSimplonMode(true);
    state.autoload.lastMaskAttempt = Date.now();
    await fetchSimplonMask();
  }
  updateLiveBadge();
  autoloadTick();
  state.autoload.timer = window.setInterval(autoloadTick, state.autoload.interval);
}

async function stopAutoload({ keepMode = true, disableMonitor = true } = {}) {
  if (state.autoload.timer) {
    window.clearInterval(state.autoload.timer);
    state.autoload.timer = null;
  }
  if (state.autoload.running && state.autoload.mode === "simplon" && disableMonitor) {
    await setSimplonMode(false);
  }
  state.autoload.running = false;
  state.autoload.busy = false;
  state.autoload.autoStart = keepMode ? state.autoload.autoStart : false;
  if (!keepMode) {
    state.autoload.mode = "file";
  }
  updateAutoloadUI();
  setAutoloadStatus(state.autoload.mode === "file" ? "Idle" : "Stopped");
  updateLiveBadge();
  persistAutoloadSettings();
}

async function ensureFileMode() {
  if (state.autoload.running || state.autoload.mode !== "file") {
    await stopAutoload({ keepMode: false, disableMonitor: true });
  }
}

async function autoloadTick() {
  if (!state.autoload.running || state.autoload.busy) return;
  if (state.isLoading) return;
  state.autoload.busy = true;
  state.autoload.lastPoll = Date.now();
  updateAutoloadMeta();
  try {
    if (state.autoload.mode === "watch") {
      await autoloadWatchTick();
    } else if (state.autoload.mode === "simplon") {
      await autoloadSimplonTick();
    }
  } finally {
    state.autoload.busy = false;
  }
}

async function autoloadWatchTick() {
  const folder = state.autoload.dir || "";
  const exts = [];
  if (state.autoload.types.hdf5) exts.push("h5", "hdf5");
  if (state.autoload.types.tiff) exts.push("tif", "tiff");
  if (state.autoload.types.cbf) exts.push("cbf", "cbf.gz");
  if (state.autoload.types.edf) exts.push("edf");
  if (exts.length === 0) {
    setAutoloadStatus("Watch: no types selected");
    return;
  }
  const pattern = state.autoload.pattern || "";
  const url = `${API}/autoload/latest?folder=${encodeURIComponent(folder)}&exts=${encodeURIComponent(
    exts.join(",")
  )}&pattern=${encodeURIComponent(pattern)}`;
  const res = await fetch(url);
  if (res.status === 204) {
    setAutoloadStatus("Watch: no files");
    setAutoloadLatest("-");
    return;
  }
  if (!res.ok) {
    setAutoloadStatus("Watch: error");
    return;
  }
  const payload = await res.json();
  if (!payload?.file) {
    setAutoloadStatus("Watch: no files");
    return;
  }
  const mtime = Number(payload.mtime || 0);
  if (payload.file === state.autoload.lastFile && mtime <= state.autoload.lastMtime) {
    return;
  }
  const previousFile = state.autoload.lastFile;
  state.autoload.lastFile = payload.file;
  const previousMtime = state.autoload.lastMtime;
  state.autoload.lastMtime = mtime;
  await loadAutoloadFile(payload.file);
  const changed = payload.file !== previousFile || mtime > previousMtime;
  if (changed) {
    state.autoload.lastUpdate = Date.now();
    updateAutoloadMeta();
  }
  setAutoloadStatus(payload.file === previousFile ? "Watch: updated" : "Watch: loaded");
}

async function autoloadSimplonTick() {
  const baseUrl = state.autoload.simplonUrl || "";
  if (!baseUrl) {
    setAutoloadStatus("SIMPLON: set base URL");
    return;
  }
  if (!state.maskAvailable) {
    const now = Date.now();
    const lastAttempt = state.autoload.lastMaskAttempt || 0;
    if (now - lastAttempt > 5000) {
      state.autoload.lastMaskAttempt = now;
      await fetchSimplonMask();
    }
  }
  const version = state.autoload.simplonVersion || "1.8.0";
  const timeout = state.autoload.simplonTimeout || 500;
  const enable = state.autoload.simplonEnable ? "1" : "0";
  const url = `${API}/simplon/monitor?url=${encodeURIComponent(baseUrl)}&version=${encodeURIComponent(
    version
  )}&timeout=${encodeURIComponent(timeout)}&enable=${enable}`;
  const res = await fetch(url);
  if (res.status === 204) {
    setAutoloadStatus("SIMPLON: no frame");
    updateLiveBadge();
    return;
  }
  if (!res.ok) {
    setAutoloadStatus("SIMPLON: error");
    updateLiveBadge();
    return;
  }
  const buffer = await res.arrayBuffer();
  const dtype = parseDtype(res.headers.get("X-Dtype"));
  const shape = parseShape(res.headers.get("X-Shape"));
  const data = typedArrayFrom(buffer, dtype);
  const sig = hashBufferSample(buffer);
  const changed = sig && sig !== state.autoload.lastMonitorSig;
  if (changed) {
    state.autoload.lastMonitorSig = sig;
    state.autoload.lastUpdate = Date.now();
    updateAutoloadMeta();
  }
  const simplonMeta = applySimplonMeta(res.headers);
  if (!state.autoload.loggedSimplonHeaders) {
    state.autoload.loggedSimplonHeaders = true;
    logClient("info", "SIMPLON response headers", {
      headers: Object.fromEntries(res.headers.entries()),
    });
  }
  let label = "SIMPLON monitor";
  let hostLabel = "";
  try {
    hostLabel = new URL(baseUrl).host;
  } catch {
    hostLabel = baseUrl || "";
  }
  if (hostLabel) {
    label = `${label} (${hostLabel})`;
  }
  const detailParts = [];
  if (simplonMeta.series !== '' && simplonMeta.series != null) detailParts.push(`S${simplonMeta.series}`);
  if (simplonMeta.image !== '' && simplonMeta.image != null) detailParts.push(`Img${simplonMeta.image}`);
  const timeLabel = formatSimplonTimestamp(simplonMeta.date);
  if (timeLabel) detailParts.push(timeLabel);
  if (detailParts.length) {
    label = `${label} ${detailParts.join(" ")}`;
  }
  applyExternalFrame(data, shape, dtype, label, false, true);
  setAutoloadStatus("SIMPLON: updated");
  updateLiveBadge();
}

async function loadAutoloadFile(file) {
  const lower = file.toLowerCase();
  if (lower.endsWith(".h5") || lower.endsWith(".hdf5")) {
    const wasFile = state.file;
    state.file = file;
    if (fileSelect) {
      const existing = Array.from(fileSelect.options).some((opt) => opt.value === file);
      if (!existing) {
        fileSelect.appendChild(option(fileLabel(file), file));
      }
      fileSelect.value = file;
    }
    setDataControlsForHdf5();
    await loadDatasets();
    if (state.frameCount > 1) {
      requestFrame(state.frameCount - 1);
    }
    return;
  }
  await loadImageSeries(file);
}

async function loadImageSeries(file) {
  if (!file) return;
  if (!isSeriesCapable(file)) {
    state.seriesFiles = [];
    state.seriesLabel = "";
    await loadImageFile(file);
    return;
  }
  try {
    const data = await fetchJSON(`${API}/series?file=${encodeURIComponent(file)}`);
    const files = Array.isArray(data.files) ? data.files : [file];
    if (data.series && files.length > 1) {
      state.seriesFiles = files;
      state.seriesLabel = fileLabel(file);
      state.file = file;
      state.dataset = "";
      state.frameCount = files.length;
      state.frameIndex = Math.max(0, Math.min(files.length - 1, Number(data.index || 0)));
      state.hasFrame = false;
      updateFrameControls();
      updatePlayButtons();
      setDataControlsForSeries();
      await loadSeriesFrame();
      return;
    }
  } catch (err) {
    console.warn(err);
  }
  state.seriesFiles = [];
  state.seriesLabel = "";
  await loadImageFile(file);
}

async function loadImageFile(file) {
  stopPlayback();
  setLoading(true);
  setStatus("Loading image…");
  const res = await fetch(`${API}/image?file=${encodeURIComponent(file)}`);
  if (!res.ok) {
    setStatus("Failed to load image");
    setLoading(false);
    return;
  }
  const buffer = await res.arrayBuffer();
  const dtype = parseDtype(res.headers.get("X-Dtype"));
  const shape = parseShape(res.headers.get("X-Shape"));
  const data = typedArrayFrom(buffer, dtype);
  applyImageMeta(res.headers);
  applyExternalFrame(data, shape, dtype, file, true, false, {
    autoMask: true,
    maskKey: `auto:${file}`,
  });
  setLoading(false);
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

function exportFrame(filenameOverride) {
  canvas.toBlob((blob) => {
    if (!blob) return;
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    const name = filenameOverride || `frame_${state.frameIndex}.png`;
    a.download = name;
    a.click();
    URL.revokeObjectURL(a.href);
  });
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
      let v = state.dataRaw[idx];
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
  const viewX = canvasWrap.scrollLeft / zoom;
  const viewY = canvasWrap.scrollTop / zoom;
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

async function exportViewerWindow(filenameOverride) {
  if (typeof window.html2canvas !== "function") {
    setStatus("Viewer export unavailable");
    return;
  }
  const target = document.querySelector(".page");
  if (!target) return;
  try {
    const shot = await window.html2canvas(target, {
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

function setSettingsMessage(text, isError = false) {
  if (!settingsMessage) return;
  settingsMessage.textContent = text || "";
  settingsMessage.classList.toggle("is-error", Boolean(isError));
}

function closeSettingsModal() {
  settingsModal?.classList.remove("is-open");
  setSettingsMessage("");
}

function fillSettingsForm(config, configPath = "") {
  if (!config || !settingsServerHost) return;
  settingsServerHost.value = String(config?.server?.host ?? "127.0.0.1");
  settingsServerPort.value = String(Number(config?.server?.port ?? 8000));
  settingsServerReload.checked = Boolean(config?.server?.reload);

  settingsLauncherPort.value = String(Number(config?.launcher?.port ?? 0));
  settingsStartupTimeout.value = String(Number(config?.launcher?.startup_timeout_sec ?? 5.0));
  settingsOpenBrowser.checked = Boolean(config?.launcher?.open_browser ?? true);
  if (settingsToolHints) {
    const toolHints = config?.ui?.tool_hints;
    settingsToolHints.checked = Boolean(toolHints ?? state.toolHintsEnabled);
  }

  settingsDataRoot.value = String(config?.data?.root ?? "");
  settingsAllowAbs.checked = Boolean(config?.data?.allow_abs_paths ?? true);
  settingsScanCache.value = String(Number(config?.data?.scan_cache_sec ?? 2.0));
  settingsMaxScanDepth.value = String(Number(config?.data?.max_scan_depth ?? -1));
  settingsMaxUpload.value = String(Number(config?.data?.max_upload_mb ?? 0));

  settingsLogLevel.value = String(config?.logging?.level ?? "INFO").toUpperCase();
  settingsLogDir.value = String(config?.logging?.dir ?? "");
  if (settingsConfigPath) {
    settingsConfigPath.textContent = configPath || "-";
  }
}

function collectSettingsForm() {
  const asInt = (value, fallback) => {
    const num = Number(value);
    if (!Number.isFinite(num)) return fallback;
    return Math.round(num);
  };
  const asFloat = (value, fallback) => {
    const num = Number(value);
    if (!Number.isFinite(num)) return fallback;
    return num;
  };

  return {
    server: {
      host: (settingsServerHost?.value || "127.0.0.1").trim() || "127.0.0.1",
      port: Math.max(0, Math.min(65535, asInt(settingsServerPort?.value, 8000))),
      reload: Boolean(settingsServerReload?.checked),
    },
    launcher: {
      port: Math.max(0, Math.min(65535, asInt(settingsLauncherPort?.value, 0))),
      startup_timeout_sec: Math.max(0.1, asFloat(settingsStartupTimeout?.value, 5.0)),
      open_browser: Boolean(settingsOpenBrowser?.checked),
    },
    data: {
      root: (settingsDataRoot?.value || "").trim(),
      allow_abs_paths: Boolean(settingsAllowAbs?.checked),
      scan_cache_sec: Math.max(0, asFloat(settingsScanCache?.value, 2.0)),
      max_scan_depth: Math.max(-1, asInt(settingsMaxScanDepth?.value, -1)),
      max_upload_mb: Math.max(0, asInt(settingsMaxUpload?.value, 0)),
    },
    logging: {
      level: (settingsLogLevel?.value || "INFO").toUpperCase(),
      dir: (settingsLogDir?.value || "").trim(),
    },
    ui: {
      tool_hints: Boolean(settingsToolHints?.checked),
    },
  };
}

async function openSettingsModal() {
  closeMenu();
  if (!settingsModal) return;
  settingsModal.classList.add("is-open");
  setSettingsMessage("Loading settings...");
  try {
    const res = await fetch(`${API}/settings`);
    if (!res.ok) {
      throw new Error(`Settings request failed: ${res.status}`);
    }
    const payload = await res.json();
    const config = payload?.config || {};
    fillSettingsForm(config, payload?.path || "");
    if (settingsToolHints) settingsToolHints.checked = Boolean(config?.ui?.tool_hints ?? state.toolHintsEnabled);
    if (typeof config?.ui?.tool_hints !== "undefined") {
      setToolHintsEnabled(Boolean(config.ui.tool_hints));
    }
    setSettingsMessage("Edit values and click Save.");
  } catch (err) {
    console.error(err);
    setSettingsMessage("Failed to load settings", true);
  }
}

async function saveSettingsFromModal() {
  if (!settingsSave) return;
  const config = collectSettingsForm();
  settingsSave.disabled = true;
  setSettingsMessage("Saving settings...");
  try {
    const res = await fetch(`${API}/settings`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ config }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(data?.detail || `Save failed (${res.status})`);
    }
    fillSettingsForm(data?.config || config, data?.path || "");
    if (settingsToolHints) {
      setToolHintsEnabled(settingsToolHints.checked);
    }
    setSettingsMessage("Saved. Restart ALBIS to apply all settings.");
    setStatus("Settings saved. Restart ALBIS to apply all settings.");
  } catch (err) {
    console.error(err);
    setSettingsMessage(String(err?.message || "Failed to save settings"), true);
  } finally {
    settingsSave.disabled = false;
  }
}

async function handleMenuAction(action) {
  switch (action) {
    case "help-docs":
      window.open("docs.html", "_blank");
      break;
    case "help-log":
      try {
        const res = await fetch(`${API}/open-log`, { method: "POST" });
        if (!res.ok) {
          setStatus("Failed to open log file");
        }
      } catch (err) {
        console.error(err);
        setStatus("Failed to open log file");
      }
      break;
    case "settings-open":
      openSettingsModal();
      break;
    case "help-about":
      if (aboutModal) {
        aboutModal.classList.add("is-open");
      }
      break;
    case "new-window":
      window.open(window.location.href, "_blank");
      break;
    case "open":
      openFileModal();
      break;
    case "close-file":
      closeCurrentFile();
      break;
    case "save-full": {
      const base = state.file ? state.file.replace(/\.[^.]+$/, "") : "frame";
      const suggested = `${base}_frame_${state.frameIndex + 1}.png`;
      const name = window.prompt("Save As (Full Image)", suggested);
      if (name) {
        exportFullImage(name);
      }
      break;
    }
    case "save-visible": {
      const base = state.file ? state.file.replace(/\.[^.]+$/, "") : "frame";
      const suggested = `${base}_view_${state.frameIndex + 1}.png`;
      const name = window.prompt("Save As (Visible Area)", suggested);
      if (name) {
        exportVisibleArea(name);
      }
      break;
    }
    case "save-window": {
      const suggested = `albis_view_${state.frameIndex + 1}.png`;
      const name = window.prompt("Save As (Viewer Window)", suggested);
      if (name) {
        exportViewerWindow(name);
      }
      break;
    }
    case "export-full":
      exportFullImage();
      break;
    case "export-visible":
      exportVisibleArea();
      break;
    case "export-window":
      exportViewerWindow();
      break;
    default:
      break;
  }
}

function handleShortcut(event) {
  const isMod = event.metaKey || event.ctrlKey;
  if (!isMod) return;
  const key = event.key.toLowerCase();
  const isShift = event.shiftKey;
  const isAlt = event.altKey;
  if (["o", "s", "e", "n", "w", ","].includes(key)) {
    event.preventDefault();
  }
  switch (key) {
    case "o":
      openFileModal();
      break;
    case "w":
      closeCurrentFile();
      break;
    case "s":
      if (isAlt) {
        handleMenuAction("save-window");
      } else if (isShift) {
        handleMenuAction("save-visible");
      } else {
        handleMenuAction("save-full");
      }
      break;
    case "e":
      if (isAlt) {
        handleMenuAction("export-window");
      } else if (isShift) {
        handleMenuAction("export-visible");
      } else {
        handleMenuAction("export-full");
      }
      break;
    case "n":
      handleMenuAction("new-window");
      break;
    case ",":
      handleMenuAction("settings-open");
      break;
    default:
      break;
  }
}

function isFormElement(target) {
  if (!target) return false;
  if (target.isContentEditable) return true;
  const tag = target.tagName ? target.tagName.toLowerCase() : "";
  if (["input", "textarea", "select", "option"].includes(tag)) return true;
  return Boolean(target.closest?.("input, textarea, select, [contenteditable='true']"));
}

function handleNavShortcut(event) {
  if (event.metaKey || event.ctrlKey || event.altKey) return false;
  if (event.key === "Tab" || event.keyCode === 9) {
    event.preventDefault();
    if (state.playing) {
      stopPlayback();
    } else {
      startPlayback();
    }
    return true;
  }
  const hasThresholds = state.thresholdCount > 1 && state.autoload.mode !== "simplon";
  const isThresholdTarget =
    event.target === thresholdSelect || event.target === toolbarThresholdSelect;
  if (hasThresholds && (event.key === "ArrowUp" || event.key === "ArrowDown")) {
    if (!isThresholdTarget && isFormElement(event.target)) return false;
    event.preventDefault();
    stopPlayback();
    const delta = event.key === "ArrowUp" ? -1 : 1;
    void setThresholdIndex(state.thresholdIndex + delta);
    return true;
  }
  if (isFormElement(event.target)) return false;
  switch (event.key) {
    case "ArrowLeft":
      event.preventDefault();
      stopPlayback();
      requestFrame(state.frameIndex - 1);
      return true;
    case "ArrowRight":
      event.preventDefault();
      stopPlayback();
      requestFrame(state.frameIndex + 1);
      return true;
    case "ArrowUp": {
      event.preventDefault();
      stopPlayback();
      const step = Math.max(1, state.step || 1);
      requestFrame(state.frameIndex - step);
      return true;
    }
    case "ArrowDown": {
      event.preventDefault();
      stopPlayback();
      const step = Math.max(1, state.step || 1);
      requestFrame(state.frameIndex + step);
      return true;
    }
    default:
      return false;
  }
}

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`Request failed: ${res.status}`);
  }
  return res.json();
}

async function fetchJSONWithInit(url, init) {
  const res = await fetch(url, init);
  if (!res.ok) {
    let detail = "";
    try {
      const body = await res.json();
      detail = body?.detail ? `: ${body.detail}` : "";
    } catch {
      // ignore parse errors
    }
    throw new Error(`Request failed: ${res.status}${detail}`);
  }
  return res.json();
}

async function findExistingFile(filename, folder = "") {
  if (!filename) return null;
  try {
    const url = folder ? `${API}/files?folder=${encodeURIComponent(folder)}` : `${API}/files`;
    const data = await fetchJSON(url);
    const matches = data.files.filter((file) => file === filename || file.endsWith(`/${filename}`));
    if (matches.length === 0) return null;
    const exact = matches.find((file) => file === filename);
    if (exact) return exact;
    matches.sort((a, b) => a.length - b.length);
    return matches[0];
  } catch (err) {
    console.error(err);
    return null;
  }
}

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

function createShader(gl, type, source) {
  const shader = gl.createShader(type);
  gl.shaderSource(shader, source);
  gl.compileShader(shader);
  if (!gl.getShaderParameter(shader, gl.COMPILE_STATUS)) {
    const info = gl.getShaderInfoLog(shader);
    gl.deleteShader(shader);
    throw new Error(info || "Shader compile failed");
  }
  return shader;
}

function createProgram(gl, vertexSource, fragmentSource) {
  const vs = createShader(gl, gl.VERTEX_SHADER, vertexSource);
  const fs = createShader(gl, gl.FRAGMENT_SHADER, fragmentSource);
  const program = gl.createProgram();
  gl.attachShader(program, vs);
  gl.attachShader(program, fs);
  gl.linkProgram(program);
  gl.deleteShader(vs);
  gl.deleteShader(fs);
  if (!gl.getProgramParameter(program, gl.LINK_STATUS)) {
    const info = gl.getProgramInfoLog(program);
    gl.deleteProgram(program);
    throw new Error(info || "Program link failed");
  }
  return program;
}

function createWebGLRenderer() {
  // WebGL2 renderer is the primary path for large image performance.
  // It handles contrast mapping and masking directly in the fragment shader.
  const gl = canvas.getContext("webgl2", {
    antialias: false,
    preserveDrawingBuffer: true,
  });
  if (!gl) {
    return null;
  }

  const vertexSource = `#version 300 es
    in vec2 a_position;
    out vec2 v_tex;
    void main() {
      v_tex = a_position * 0.5 + 0.5;
      gl_Position = vec4(a_position, 0.0, 1.0);
    }
  `;

  const fragmentSource = `#version 300 es
    precision highp float;
    precision highp int;
    uniform sampler2D u_data;
    uniform sampler2D u_lut;
    uniform sampler2D u_mask;
    uniform float u_mask_enabled;
    uniform float u_min;
    uniform float u_max;
    uniform float u_invert;
    uniform float u_hdr;
    uniform float u_lut_size;
    in vec2 v_tex;
    out vec4 outColor;
    void main() {
      float value = texture(u_data, v_tex).r;
      if (u_mask_enabled > 0.5) {
        float maskClass = texture(u_mask, v_tex).r;
        if (maskClass > 0.75) {
          outColor = vec4(0.0, 0.0, 0.0, 1.0);
          return;
        } else if (maskClass > 0.25) {
          outColor = vec4(0.1, 0.2, 0.47, 1.0);
          return;
        }
      }
      float norm = 0.0;
      if (u_hdr > 0.5) {
        const float linSize = 256.0;
        const float logSize = 768.0;
        float bg = u_min;
        float fg = u_max;
        float lfg = fg * 10000.0;
        float idx = 0.0;
        if (value <= bg) {
          idx = 0.0;
        } else if (value >= lfg) {
          idx = linSize + logSize - 1.0;
        } else if (value < fg && fg > bg) {
          float linSlope = linSize / (fg - bg);
          idx = floor((value - bg) * linSlope);
          idx = clamp(idx, 0.0, linSize - 1.0);
        } else if (fg > bg && lfg > fg && value > bg) {
          float denom = log((lfg - bg) / (fg - bg));
          if (denom > 0.0) {
            float logSlope = (logSize - 1.0) / denom;
            float logOffset = -log(max(fg - bg, 1e-12)) * logSlope;
            float x = log(max(value - bg, 1e-12)) * logSlope + logOffset;
            idx = linSize + floor(x);
            idx = clamp(idx, linSize, linSize + logSize - 1.0);
          } else {
            idx = linSize;
          }
        }
        norm = idx / (linSize + logSize - 1.0);
      } else {
        float denom = max(u_max - u_min, 1.0);
        float t = (value - u_min) / denom;
        norm = clamp(t, 0.0, 1.0);
      }
      if (u_invert > 0.5) {
        norm = 1.0 - norm;
      }
      float lutSize = max(u_lut_size, 2.0);
      float lutIndex = floor(norm * (lutSize - 1.0));
      float lutU = (lutIndex + 0.5) / lutSize;
      outColor = texture(u_lut, vec2(lutU, 0.5));
    }
  `;

  let program;
  try {
    program = createProgram(gl, vertexSource, fragmentSource);
  } catch (err) {
    console.error(err);
    setStatus("WebGL shader error");
    return {
      type: "webgl",
      render: () => {},
    };
  }

  const vao = gl.createVertexArray();
  gl.bindVertexArray(vao);
  const buffer = gl.createBuffer();
  gl.bindBuffer(gl.ARRAY_BUFFER, buffer);
  gl.bufferData(gl.ARRAY_BUFFER, new Float32Array([-1, -1, 1, -1, -1, 1, 1, 1]), gl.STATIC_DRAW);
  const positionLoc = gl.getAttribLocation(program, "a_position");
  gl.enableVertexAttribArray(positionLoc);
  gl.vertexAttribPointer(positionLoc, 2, gl.FLOAT, false, 0, 0);
  gl.bindVertexArray(null);

  const dataTex = gl.createTexture();
  gl.bindTexture(gl.TEXTURE_2D, dataTex);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.NEAREST);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.NEAREST);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);

  const lutTex = gl.createTexture();
  gl.bindTexture(gl.TEXTURE_2D, lutTex);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.NEAREST);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.NEAREST);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);

  const maskTex = gl.createTexture();
  gl.bindTexture(gl.TEXTURE_2D, maskTex);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.NEAREST);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.NEAREST);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
  gl.texImage2D(
    gl.TEXTURE_2D,
    0,
    gl.R8,
    1,
    1,
    0,
    gl.RED,
    gl.UNSIGNED_BYTE,
    new Uint8Array([0])
  );

  const uniforms = {
    data: gl.getUniformLocation(program, "u_data"),
    lut: gl.getUniformLocation(program, "u_lut"),
    mask: gl.getUniformLocation(program, "u_mask"),
    maskEnabled: gl.getUniformLocation(program, "u_mask_enabled"),
    min: gl.getUniformLocation(program, "u_min"),
    max: gl.getUniformLocation(program, "u_max"),
    invert: gl.getUniformLocation(program, "u_invert"),
    hdr: gl.getUniformLocation(program, "u_hdr"),
    lutSize: gl.getUniformLocation(program, "u_lut_size"),
  };

  const maxTextureSize = gl.getParameter(gl.MAX_TEXTURE_SIZE);
  let maskTexWidth = 1;
  let maskTexHeight = 1;
  let lastMaskData = null;
  let maskClassData = null;

  return {
    type: "webgl",
    maxTextureSize,
    render({
      floatData,
      width,
      height,
      min,
      max,
      palette,
      invert,
      mask,
      maskWidth,
      maskHeight,
      maskEnabled,
      colormap,
    }) {
      if (!floatData) return;
      if (width > maxTextureSize || height > maxTextureSize) {
        setStatus(`Frame exceeds max texture size ${maxTextureSize}px`);
        return;
      }
      if (canvas.width !== width || canvas.height !== height) {
        canvas.width = width;
        canvas.height = height;
      }

      gl.viewport(0, 0, width, height);
      gl.useProgram(program);
      gl.bindVertexArray(vao);

      gl.activeTexture(gl.TEXTURE0);
      gl.bindTexture(gl.TEXTURE_2D, dataTex);
      gl.pixelStorei(gl.UNPACK_ALIGNMENT, 1);
      gl.pixelStorei(gl.UNPACK_FLIP_Y_WEBGL, true);
      gl.texImage2D(gl.TEXTURE_2D, 0, gl.R32F, width, height, 0, gl.RED, gl.FLOAT, floatData);
      gl.uniform1i(uniforms.data, 0);

      gl.activeTexture(gl.TEXTURE1);
      gl.bindTexture(gl.TEXTURE_2D, lutTex);
      const lutSize = getPaletteColorCount(palette);
      gl.texImage2D(
        gl.TEXTURE_2D,
        0,
        gl.RGBA,
        lutSize,
        1,
        0,
        gl.RGBA,
        gl.UNSIGNED_BYTE,
        palette
      );
      gl.uniform1i(uniforms.lut, 1);

      const useMask = Boolean(
        maskEnabled &&
          mask &&
          maskWidth === width &&
          maskHeight === height &&
          mask.length === width * height
      );
      gl.activeTexture(gl.TEXTURE2);
      gl.bindTexture(gl.TEXTURE_2D, maskTex);
      gl.uniform1i(uniforms.mask, 2);
      gl.uniform1f(uniforms.maskEnabled, useMask ? 1.0 : 0.0);
      if (useMask && (mask !== lastMaskData || maskTexWidth !== width || maskTexHeight !== height)) {
        if (!maskClassData || maskClassData.length !== width * height) {
          maskClassData = new Uint8Array(width * height);
        }
        for (let i = 0; i < mask.length; i += 1) {
          const bits = mask[i];
          if (bits & 1) {
            maskClassData[i] = 255;
          } else if (bits & 0x1e) {
            maskClassData[i] = 128;
          } else {
            maskClassData[i] = 0;
          }
        }
        gl.pixelStorei(gl.UNPACK_ALIGNMENT, 1);
        gl.texImage2D(
          gl.TEXTURE_2D,
          0,
          gl.R8,
          width,
          height,
          0,
          gl.RED,
          gl.UNSIGNED_BYTE,
          maskClassData
        );
        maskTexWidth = width;
        maskTexHeight = height;
        lastMaskData = mask;
      }

      gl.uniform1f(uniforms.min, min);
      gl.uniform1f(uniforms.max, max);
      gl.uniform1f(uniforms.invert, invert ? 1.0 : 0.0);
      gl.uniform1f(uniforms.hdr, colormap === "albulaHdr" ? 1.0 : 0.0);
      gl.uniform1f(uniforms.lutSize, lutSize);
      gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
    },
  };
}

function createCpuRenderer() {
  const ctx = canvas.getContext("2d");
  return {
    type: "cpu",
    render({ data, width, height, palette, mask, maskEnabled }) {
      if (!data) return;
      if (canvas.width !== width || canvas.height !== height) {
        canvas.width = width;
        canvas.height = height;
      }
      const imageData = ctx.createImageData(width, height);
      const out = imageData.data;
      const maxIdx = getPaletteColorCount(palette) - 1;
      for (let i = 0; i < data.length; i += 1) {
        let v = data[i];
        if (maskEnabled && mask && mask.length === data.length) {
          const maskValue = mask[i];
          if (maskValue & 1) {
            const j = i * 4;
            out[j] = 0;
            out[j + 1] = 0;
            out[j + 2] = 0;
            out[j + 3] = 255;
            continue;
          } else if (maskValue & 0x1e) {
            const j = i * 4;
            out[j] = 25;
            out[j + 1] = 50;
            out[j + 2] = 120;
            out[j + 3] = 255;
            continue;
          }
        }
        const norm = mapValueToNorm(v);
        const idx = Math.floor(norm * maxIdx) * 4;
        const j = i * 4;
        out[j] = palette[idx];
        out[j + 1] = palette[idx + 1];
        out[j + 2] = palette[idx + 2];
        out[j + 3] = 255;
      }
      ctx.putImageData(imageData, 0, 0);
    },
  };
}

function initRenderer() {
  renderer = createWebGLRenderer();
  if (!renderer) {
    renderer = createCpuRenderer();
  }
  metaRenderer.textContent = renderer.type === "webgl" ? "WebGL2" : "CPU";
}

async function loadFiles() {
  setDataControlsForHdf5();
  const folder = (autoloadDir?.value || state.autoload.dir || "").trim();
  const url = folder ? `${API}/files?folder=${encodeURIComponent(folder)}` : `${API}/files`;
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
    } else {
      state.file = "";
      state.dataset = "";
      setStatus("Select a file to begin");
      updateToolbar();
      showSplash();
      setLoading(false);
    }
    loadAutoloadFolders();
  } else {
    data.files.forEach((name) => fileSelect.appendChild(option(fileLabel(name), name)));
    if (!existingFile) {
      setStatus("No image files found");
      showSplash();
      setLoading(false);
    }
    loadAutoloadFolders();
  }
}

function sortDatasets(datasets) {
  const linkedStack = datasets.find((d) => d.path === "/entry/data");
  if (linkedStack) {
    return [linkedStack, ...datasets.filter((d) => d !== linkedStack)];
  }
  const primary = datasets.find((d) => d.path.includes("/entry/data/data"));
  if (primary) {
    return [primary, ...datasets.filter((d) => d !== primary)];
  }
  return datasets;
}

async function loadDatasets() {
  if (!state.file) return;
  if (!isHdfFile(state.file)) {
    await loadImageSeries(state.file);
    return;
  }
  state.hasFrame = false;
  stopPlayback();
  state.seriesFiles = [];
  state.seriesLabel = "";
  setDataControlsForHdf5();
  await loadMask(true);
  showProcessingProgress("Scanning datasets…");
  setLoading(true);
  setStatus("Scanning datasets…");
  try {
    const data = await fetchJSON(`${API}/datasets?file=${encodeURIComponent(state.file)}`);
    const candidates = data.datasets
      .filter((d) => d.image)
      .sort((a, b) => b.size - a.size);

    datasetSelect.innerHTML = "";
    const ordered = sortDatasets(candidates);
    ordered.forEach((d) => datasetSelect.appendChild(option(`${d.path} (${d.shape.join("x")})`, d.path)));
    await loadInspectorRoot();

    if (ordered.length > 0) {
      state.dataset = ordered[0].path;
      datasetSelect.value = state.dataset;
      await loadMetadata();
    } else {
      setStatus("No image datasets found");
      showSplash();
      setLoading(false);
    }
  } catch (err) {
    console.error(err);
    setStatus("Failed to scan datasets");
    showSplash();
    setLoading(false);
  } finally {
    hideProcessingProgress();
  }
}

async function loadMetadata() {
  if (!state.file || !state.dataset) return;
  showProcessingProgress("Loading metadata…");
  setStatus("Loading metadata…");
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
    state.thresholdIndex = Math.max(0, Math.min(state.thresholdIndex, state.thresholdCount - 1));
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
  return {
    series: headers.get("X-Simplon-Series") || "",
    image: headers.get("X-Simplon-Image") || "",
    date: headers.get("X-Simplon-Date") || "",
  };
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

function getDtypeInfo(dtype) {
  if (!dtype) return null;
  if (dtype.length >= 3 && (dtype[0] === "<" || dtype[0] === ">" || dtype[0] === "|")) {
    const kind = dtype[1];
    const bytes = Number.parseInt(dtype.slice(2), 10);
    if (Number.isFinite(bytes) && bytes > 0) {
      return { kind, bits: bytes * 8 };
    }
    return null;
  }
  const lower = dtype.toLowerCase();
  if (lower.startsWith("uint")) {
    const bits = Number.parseInt(lower.slice(4), 10);
    if (Number.isFinite(bits)) {
      return { kind: "u", bits };
    }
  }
  if (lower.startsWith("int")) {
    const bits = Number.parseInt(lower.slice(3), 10);
    if (Number.isFinite(bits)) {
      return { kind: "i", bits };
    }
  }
  if (lower.startsWith("float")) {
    const bits = Number.parseInt(lower.slice(5), 10);
    if (Number.isFinite(bits)) {
      return { kind: "f", bits };
    }
  }
  return null;
}

function getSaturationMax(rawMax) {
  const info = getDtypeInfo(state.dtype);
  if (!info || info.kind === "f") return null;
  if (!Number.isFinite(rawMax)) return null;
  const bits = info.bits;
  if (!Number.isFinite(bits) || bits <= 0 || bits > 52) return null;
  const dtypeMax = info.kind === "u" ? 2 ** bits - 1 : 2 ** (bits - 1) - 1;
  const candidates = [4, 8, 12, 16, 32];
  for (const candBits of candidates) {
    if (candBits > bits) continue;
    const candMax = 2 ** candBits - 1;
    if (rawMax === candMax) {
      return candMax;
    }
  }
  return dtypeMax;
}

function chooseHistogramBins(count) {
  if (!Number.isFinite(count) || count <= 0) return 256;
  const bins = Math.round(Math.sqrt(count) * 0.5);
  return Math.max(32, Math.min(256, bins));
}

const AUTO_CONTRAST_LOW = 0.001;
const AUTO_CONTRAST_HIGH = 0.999;
const AUTO_CONTRAST_BINS = 4096;
const ALBULA_LIN_SIZE = 256;
const ALBULA_LOG_SIZE = 768;
const ALBULA_LUT_SIZE = ALBULA_LIN_SIZE + ALBULA_LOG_SIZE;
const ALBULA_LOG_FOREGROUND_FACTOR = 10000;

function getPaletteColorCount(palette) {
  if (!palette || !palette.length) return 1;
  return Math.max(1, Math.floor(palette.length / 4));
}

function mapAlbulaHdrToNorm(value, bg, fg) {
  // Emulate ALBULA HDR transfer:
  // linear ramp until FG, then logarithmic compression for brighter peaks.
  if (!Number.isFinite(value) || !Number.isFinite(bg) || !Number.isFinite(fg)) {
    return 0;
  }
  const lfg = fg * ALBULA_LOG_FOREGROUND_FACTOR;
  let idx = 0;
  if (value <= bg) {
    idx = 0;
  } else if (value >= lfg) {
    idx = ALBULA_LUT_SIZE - 1;
  } else if (value < fg && fg > bg) {
    const linSlope = ALBULA_LIN_SIZE / (fg - bg);
    idx = Math.floor((value - bg) * linSlope);
    idx = Math.max(0, Math.min(ALBULA_LIN_SIZE - 1, idx));
  } else if (fg > bg && lfg > fg && value > bg) {
    const denom = Math.log((lfg - bg) / (fg - bg));
    if (Number.isFinite(denom) && denom > 0) {
      const logSlope = (ALBULA_LOG_SIZE - 1) / denom;
      const logOffset = -Math.log(fg - bg) * logSlope;
      const x = Math.log(Math.max(value - bg, Number.EPSILON)) * logSlope + logOffset;
      idx = ALBULA_LIN_SIZE + Math.floor(x);
      idx = Math.max(ALBULA_LIN_SIZE, Math.min(ALBULA_LUT_SIZE - 1, idx));
    } else {
      idx = ALBULA_LIN_SIZE;
    }
  }
  return idx / (ALBULA_LUT_SIZE - 1);
}

function computeHistogram(data, min, max, satMax, bins, logX) {
  const hist = new Uint32Array(bins);
  if (!Number.isFinite(min) || !Number.isFinite(max) || bins <= 0) {
    return hist;
  }
  const range = max - min || 1;
  let mapValue = (v) => (v - min) / range;
  if (logX) {
    const symlog = (v) => Math.sign(v) * Math.log10(1 + Math.abs(v));
    const minMap = symlog(min);
    const maxMap = symlog(max);
    const mapRange = maxMap - minMap || 1;
    mapValue = (v) => (symlog(v) - minMap) / mapRange;
  }

  for (let i = 0; i < data.length; i += 1) {
    const v = data[i];
    if (!Number.isFinite(v)) continue;
    if (v < 0) continue;
    if (satMax !== null && v === satMax) continue;
    const t = mapValue(v);
    if (!Number.isFinite(t)) continue;
    const idx = Math.min(bins - 1, Math.max(0, Math.floor(t * (bins - 1))));
    hist[idx] += 1;
  }
  return hist;
}

function computeAutoLevels(data, satMaxInput) {
  let rawMax = Number.NEGATIVE_INFINITY;
  for (let i = 0; i < data.length; i += 1) {
    const v = data[i];
    if (!Number.isFinite(v)) continue;
    if (v < 0) continue;
    if (v > rawMax) rawMax = v;
  }

  const satMax = satMaxInput ?? getSaturationMax(rawMax);
  let minLog = Number.POSITIVE_INFINITY;
  let maxLog = Number.NEGATIVE_INFINITY;
  let count = 0;

  for (let i = 0; i < data.length; i += 1) {
    const v = data[i];
    if (!Number.isFinite(v)) continue;
    if (v < 0) continue;
    if (satMax !== null && v === satMax) continue;
    const lv = Math.log1p(v);
    if (lv < minLog) minLog = lv;
    if (lv > maxLog) maxLog = lv;
    count += 1;
  }

  if (!Number.isFinite(minLog) || !Number.isFinite(maxLog) || count === 0) {
    return { min: state.stats?.min ?? 0, max: state.stats?.max ?? 1 };
  }

  const bins = Math.max(256, Math.min(AUTO_CONTRAST_BINS, Math.round(Math.sqrt(count)) * 4));
  const hist = new Uint32Array(bins);
  const range = maxLog - minLog || 1;

  for (let i = 0; i < data.length; i += 1) {
    const v = data[i];
    if (!Number.isFinite(v)) continue;
    if (v < 0) continue;
    if (satMax !== null && v === satMax) continue;
    const lv = Math.log1p(v);
    const idx = Math.min(bins - 1, Math.max(0, Math.floor(((lv - minLog) / range) * (bins - 1))));
    hist[idx] += 1;
  }

  const lowTarget = count * AUTO_CONTRAST_LOW;
  const highTarget = count * AUTO_CONTRAST_HIGH;
  let cumulative = 0;
  let lowBin = 0;
  for (let i = 0; i < bins; i += 1) {
    cumulative += hist[i];
    if (cumulative >= lowTarget) {
      lowBin = i;
      break;
    }
  }
  cumulative = 0;
  let highBin = bins - 1;
  for (let i = 0; i < bins; i += 1) {
    cumulative += hist[i];
    if (cumulative >= highTarget) {
      highBin = i;
      break;
    }
  }
  if (highBin <= lowBin) {
    highBin = Math.min(bins - 1, lowBin + 1);
  }

  const lowLog = minLog + (lowBin / (bins - 1)) * range;
  const highLog = minLog + (highBin / (bins - 1)) * range;
  const minVal = Math.expm1(lowLog);
  const maxVal = Math.expm1(highLog);
  if (!Number.isFinite(minVal) || !Number.isFinite(maxVal) || minVal >= maxVal) {
    return { min: state.stats?.min ?? 0, max: state.stats?.max ?? 1 };
  }
  return { min: minVal, max: maxVal };
}

function computeStats(data) {
  let rawMax = Number.NEGATIVE_INFINITY;

  for (let i = 0; i < data.length; i += 1) {
    const v = data[i];
    if (!Number.isFinite(v)) continue;
    if (v < 0) continue;
    if (v > rawMax) rawMax = v;
  }

  const satMax = getSaturationMax(rawMax);
  let min = Number.POSITIVE_INFINITY;
  let max = Number.NEGATIVE_INFINITY;

  for (let i = 0; i < data.length; i += 1) {
    const v = data[i];
    if (!Number.isFinite(v)) continue;
    if (v < 0) continue;
    if (satMax !== null && v === satMax) continue;
    if (v < min) min = v;
    if (v > max) max = v;
  }

  if (!Number.isFinite(min) || !Number.isFinite(max)) {
    return { min: 0, max: 1, hist: new Uint32Array(0), satMax, bins: 0 };
  }

  const bins = chooseHistogramBins(data.length);
  const hist = computeHistogram(data, min, max, satMax, bins, state.histLogX);
  return { min, max, hist, satMax, bins };
}

function histogramValueToX(value, width) {
  const minVal = state.stats?.min ?? 0;
  const maxVal = state.stats?.max ?? 1;
  if (!Number.isFinite(value) || !Number.isFinite(minVal) || !Number.isFinite(maxVal)) {
    return 0;
  }
  const range = maxVal - minVal || 1;
  if (!state.histLogX) {
    return ((value - minVal) / range) * width;
  }
  const symlog = (v) => Math.sign(v) * Math.log10(1 + Math.abs(v));
  const minMap = symlog(minVal);
  const maxMap = symlog(maxVal);
  const mapRange = maxMap - minMap || 1;
  const mapped = (symlog(value) - minMap) / mapRange;
  return Math.min(width, Math.max(0, mapped * width));
}

function histogramXToValue(x, width) {
  const minVal = state.stats?.min ?? 0;
  const maxVal = state.stats?.max ?? 1;
  const clampedX = Math.min(width, Math.max(0, x));
  const t = width ? clampedX / width : 0;
  if (!state.histLogX) {
    return minVal + t * (maxVal - minVal);
  }
  const symlog = (v) => Math.sign(v) * Math.log10(1 + Math.abs(v));
  const invSymlog = (v) => Math.sign(v) * (10 ** Math.abs(v) - 1);
  const minMap = symlog(minVal);
  const maxMap = symlog(maxVal);
  const mapRange = maxMap - minMap || 1;
  const mapped = minMap + t * mapRange;
  return invSymlog(mapped);
}

function mapValueToNorm(value) {
  if (!Number.isFinite(value)) return 0;
  const minVal = Number.isFinite(state.min) ? state.min : 0;
  const maxVal = Number.isFinite(state.max) ? state.max : minVal + 1;
  const range = maxVal - minVal || 1;
  let t = (value - minVal) / range;
  let norm = 0;
  if (state.colormap === "albulaHdr") {
    norm = mapAlbulaHdrToNorm(value, minVal, maxVal);
  } else {
    norm = Math.min(1, Math.max(0, t));
  }
  if (state.invert) {
    norm = 1 - norm;
  }
  return Math.min(1, Math.max(0, norm));
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
  const paletteSize = name === "albulaHdr" ? ALBULA_LUT_SIZE : 256;
  const palette = new Uint8Array(paletteSize * 4);
  const mixStops = (stops, t) => {
    const scaled = t * (stops.length - 1);
    const idx = Math.floor(scaled);
    const frac = scaled - idx;
    const a = stops[idx];
    const b = stops[Math.min(idx + 1, stops.length - 1)];
    return [
      Math.round(a[0] + (b[0] - a[0]) * frac),
      Math.round(a[1] + (b[1] - a[1]) * frac),
      Math.round(a[2] + (b[2] - a[2]) * frac),
    ];
  };
  for (let i = 0; i < paletteSize; i += 1) {
    const t = paletteSize > 1 ? i / (paletteSize - 1) : 0;
    let r = 0;
    let g = 0;
    let b = 0;
    if (name === "gray") {
      r = g = b = Math.round(t * 255);
    } else if (name === "heat") {
      const tt = t * 3;
      r = Math.min(255, Math.round(255 * Math.min(tt, 1)));
      g = Math.min(255, Math.round(255 * Math.max(0, tt - 1)));
      b = Math.min(255, Math.round(255 * Math.max(0, tt - 2)));
    } else if (name === "viridis") {
      [r, g, b] = mixStops(
        [
          [68, 1, 84],
          [59, 82, 139],
          [33, 145, 140],
          [94, 201, 97],
          [253, 231, 37],
        ],
        t
      );
    } else if (name === "magma") {
      [r, g, b] = mixStops(
        [
          [0, 0, 4],
          [53, 15, 83],
          [132, 32, 102],
          [196, 66, 74],
          [251, 135, 53],
          [252, 253, 191],
        ],
        t
      );
    } else if (name === "inferno") {
      [r, g, b] = mixStops(
        [
          [0, 0, 4],
          [51, 13, 81],
          [120, 28, 109],
          [190, 55, 84],
          [249, 101, 49],
          [252, 255, 164],
        ],
        t
      );
    } else if (name === "cividis") {
      [r, g, b] = mixStops(
        [
          [0, 32, 76],
          [40, 77, 117],
          [92, 125, 127],
          [147, 173, 112],
          [207, 223, 108],
          [253, 231, 37],
        ],
        t
      );
    } else if (name === "turbo") {
      [r, g, b] = mixStops(
        [
          [48, 18, 59],
          [50, 127, 216],
          [63, 195, 160],
          [189, 211, 57],
          [249, 143, 8],
          [179, 21, 22],
        ],
        t
      );
    } else if (name === "blueYellowRed") {
      r = Math.round(255 * Math.min(1, Math.max(0, t * 1.2)));
      g = Math.round(255 * Math.min(1, Math.max(0, 1.2 - Math.abs(t - 0.5) * 2)));
      b = Math.round(255 * Math.min(1, Math.max(0, 1 - t * 1.2)));
    } else if (name === "albisHdr") {
      const gamma = Math.pow(t, 0.7);
      r = Math.round(255 * Math.min(1, gamma * 1.1));
      g = Math.round(255 * Math.min(1, gamma * 0.9 + t * 0.3));
      b = Math.round(255 * Math.min(1, (1 - gamma) * 0.4 + t * 0.6));
    } else if (name === "albulaHdr") {
      if (i < ALBULA_LIN_SIZE) {
        const v = 255 - i;
        r = v;
        g = v;
        b = v;
      } else {
        const logIndex = i - ALBULA_LIN_SIZE;
        if (logIndex < 256) {
          r = logIndex;
          g = 0;
          b = 0;
        } else if (logIndex < 512) {
          r = 255;
          g = logIndex - 256;
          b = 0;
        } else {
          r = 255;
          g = 255;
          b = logIndex - 512;
        }
      }
    }
    const base = i * 4;
    palette[base] = r;
    palette[base + 1] = g;
    palette[base + 2] = b;
    palette[base + 3] = 255;
  }
  return palette;
}

function drawHistogram(hist) {
  const width = histCanvas.clientWidth;
  const height = histCanvas.clientHeight;
  if (width < 4 || height < 4) {
    return;
  }
  histCanvas.width = width * window.devicePixelRatio;
  histCanvas.height = height * window.devicePixelRatio;
  histCtx.setTransform(window.devicePixelRatio, 0, 0, window.devicePixelRatio, 0, 0);
  histCtx.clearRect(0, 0, width, height);
  histCtx.fillStyle = "#2b2b2b";
  histCtx.fillRect(0, 0, width, height);
  if (!hist || hist.length === 0) {
    histCtx.strokeStyle = "rgba(0,0,0,0.5)";
    histCtx.strokeRect(0.5, 0.5, width - 1, height - 1);
    return;
  }
  const maxCount = Math.max(...hist);
  const bins = hist.length;
  const pad = 10;
  const drawableHeight = Math.max(4, height - pad);
  const logY = state.histLogY;
  const yDenom = logY ? Math.log10(1 + maxCount) : maxCount;

  const barWidth = width / bins;
  histCtx.fillStyle = "#dcdcdc";
  for (let i = 0; i < bins; i += 1) {
    const count = hist[i];
    const norm = yDenom ? (logY ? Math.log10(1 + count) / yDenom : count / yDenom) : 0;
    const h = norm * drawableHeight;
    histCtx.fillRect(i * barWidth, height - h, Math.max(1, barWidth), h);
  }

  const minVal = state.min;
  const maxVal = state.max;
  if (Number.isFinite(minVal) && Number.isFinite(maxVal)) {
    const minX = histogramValueToX(minVal, width);
    const maxX = histogramValueToX(maxVal, width);
    const markerTop = 2;
    const markerBottom = height - 2;

    const drawMarker = (x, color, label, options = {}) => {
      const preferRight = options.preferRight !== false;
      const labelY = Number.isFinite(options.labelY) ? options.labelY : markerTop + 10;
      histCtx.strokeStyle = color;
      histCtx.lineWidth = 1.5;
      histCtx.beginPath();
      histCtx.moveTo(x, markerTop);
      histCtx.lineTo(x, markerBottom);
      histCtx.stroke();

      histCtx.fillStyle = color;
      histCtx.fillRect(x - 3, markerTop, 6, 8);
      histCtx.strokeStyle = "rgba(0,0,0,0.6)";
      histCtx.strokeRect(x - 3, markerTop, 6, 8);

      if (label) {
        histCtx.font = "10px \"Lucida Grande\", \"Helvetica Neue\", Arial, sans-serif";
        histCtx.textBaseline = "top";
        histCtx.fillStyle = "#f2f2f2";
        const metrics = histCtx.measureText(label);
        let textX;
        if (preferRight) {
          textX = Math.min(width - metrics.width - 4, Math.max(4, x + 6));
        } else {
          textX = Math.max(4, Math.min(width - metrics.width - 4, x - metrics.width - 6));
        }
        histCtx.fillText(label, textX, labelY);
      }
    };

    const labelsClose = Math.abs(maxX - minX) < 120;
    const labelTop = markerTop + 10;
    const labelBottom = markerTop + 24;
    drawMarker(minX, "#6eb5ff", `BG ${formatValue(minVal)}`, {
      preferRight: true,
      labelY: labelTop,
    });
    drawMarker(maxX, "#ffd166", `FG ${formatValue(maxVal)}`, {
      preferRight: false,
      labelY: labelsClose ? labelBottom : labelTop,
    });
  }

  histCtx.strokeStyle = "rgba(0,0,0,0.5)";
  histCtx.strokeRect(0.5, 0.5, width - 1, height - 1);
  drawColorbar();
}

function clearHistogram() {
  const width = histCanvas.clientWidth || 1;
  const height = histCanvas.clientHeight || 1;
  histCanvas.width = width * window.devicePixelRatio;
  histCanvas.height = height * window.devicePixelRatio;
  histCtx.setTransform(window.devicePixelRatio, 0, 0, window.devicePixelRatio, 0, 0);
  histCtx.fillStyle = "#2b2b2b";
  histCtx.fillRect(0, 0, width, height);
  histCtx.strokeStyle = "rgba(0,0,0,0.5)";
  histCtx.strokeRect(0.5, 0.5, width - 1, height - 1);
  drawColorbar();
}

function drawColorbar() {
  if (!histColorbar || !histColorCtx) return;
  const width = histColorbar.clientWidth || 1;
  const height = histColorbar.clientHeight || 1;
  const dpr = window.devicePixelRatio || 1;
  histColorbar.width = Math.max(1, Math.floor(width * dpr));
  histColorbar.height = Math.max(1, Math.floor(height * dpr));
  histColorCtx.setTransform(dpr, 0, 0, dpr, 0, 0);
  histColorCtx.clearRect(0, 0, width, height);

  const palette = buildPalette(state.colormap);
  const statsMin = Number.isFinite(state.stats?.min) ? state.stats.min : state.min;
  const statsMax = Number.isFinite(state.stats?.max) ? state.stats.max : state.max;
  const statsRange = statsMax - statsMin || 1;
  const useLogX = Boolean(state.histLogX);
  const symlog = (v) => Math.sign(v) * Math.log10(1 + Math.abs(v));
  const invSymlog = (v) => Math.sign(v) * (10 ** Math.abs(v) - 1);
  const minMap = useLogX ? symlog(statsMin) : 0;
  const maxMap = useLogX ? symlog(statsMax) : 0;
  const mapRange = useLogX ? maxMap - minMap || 1 : 1;
  const imageData = histColorCtx.createImageData(width, height);
  const data = imageData.data;
  const maxIdx = getPaletteColorCount(palette) - 1;
  for (let x = 0; x < width; x += 1) {
    const t = width > 1 ? x / (width - 1) : 0;
    const value = useLogX
      ? invSymlog(minMap + t * mapRange)
      : statsMin + t * statsRange;
    const norm = mapValueToNorm(value);
    const idx = Math.min(maxIdx, Math.max(0, Math.round(norm * maxIdx)));
    const p = idx * 4;
    const r = palette[p];
    const g = palette[p + 1];
    const b = palette[p + 2];
    for (let y = 0; y < height; y += 1) {
      const i = (y * width + x) * 4;
      data[i] = r;
      data[i + 1] = g;
      data[i + 2] = b;
      data[i + 3] = 255;
    }
  }
  histColorCtx.putImageData(imageData, 0, 0);
  histColorCtx.strokeStyle = "rgba(0,0,0,0.5)";
  histColorCtx.strokeRect(0.5, 0.5, width - 1, height - 1);
}

function setRoiText(el, value) {
  if (!el) return;
  el.textContent = value;
}

function updateRoiCenterInputs() {
  if (!roiCenterXInput || !roiCenterYInput) return;
  if (!roiState.start || (roiState.mode !== "circle" && roiState.mode !== "annulus")) {
    roiCenterXInput.value = "";
    roiCenterYInput.value = "";
    return;
  }
  roiCenterXInput.value = String(Math.round(roiState.start.x));
  roiCenterYInput.value = String(Math.round(roiState.start.y));
}

function applyRoiCenterFromInputs() {
  if (!roiCenterXInput || !roiCenterYInput) return null;
  const xVal = Number(roiCenterXInput.value);
  const yVal = Number(roiCenterYInput.value);
  if (!Number.isFinite(xVal) || !Number.isFinite(yVal)) return null;
  const x = Math.max(0, Math.min(state.width - 1, Math.round(xVal)));
  const y = Math.max(0, Math.min(state.height - 1, Math.round(yVal)));
  return { x, y };
}

function getRoiPlotKey(canvasEl) {
  const id = canvasEl?.id || "";
  if (id === "roi-line-canvas") return "line";
  if (id === "roi-x-canvas") return "x";
  if (id === "roi-y-canvas") return "y";
  return "line";
}

function getRoiPlotLimits(plotKey) {
  return roiState.plotLimits[plotKey] || roiState.plotLimits.line;
}

function clearRoiPlotLimits() {
  ["line", "x", "y"].forEach((key) => {
    const limits = roiState.plotLimits[key];
    if (!limits) return;
    limits.xMin = null;
    limits.xMax = null;
    limits.yMin = null;
    limits.yMax = null;
  });
}

function syncRoiPlotLimitControls() {
  if (roiLimitsEnable) {
    roiLimitsEnable.checked = roiState.plotLimits.autoscale;
  }
}

function updateRoiPlotLimitsEnabled() {
  roiState.plotLimits.autoscale = Boolean(roiLimitsEnable?.checked);
  if (roiState.plotLimits.autoscale) {
    clearRoiPlotLimits();
  }
  syncRoiPlotLimitControls();
  scheduleRoiUpdate();
}

function setRoiPlotAxisLimits(plotKey, axis, minValue, maxValue) {
  if (axis !== "x" && axis !== "y") return;
  const limits = getRoiPlotLimits(plotKey);
  const minKey = axis === "x" ? "xMin" : "yMin";
  const maxKey = axis === "x" ? "xMax" : "yMax";
  let lo = Number.isFinite(minValue) ? minValue : null;
  let hi = Number.isFinite(maxValue) ? maxValue : null;
  if (lo !== null && hi !== null && lo > hi) {
    [lo, hi] = [hi, lo];
  }
  limits[minKey] = lo;
  limits[maxKey] = hi;
}

function updateRoiModeUI() {
  const mode = roiState.mode;
  const showPlots = mode !== "none";
  if (roiParams) {
    roiParams.classList.toggle("is-circle", mode === "circle");
    roiParams.classList.toggle("is-annulus", mode === "annulus");
  }
  if (roiLinePlot) {
    const showLine = mode === "line" || mode === "circle" || mode === "annulus";
    roiLinePlot.classList.toggle("is-hidden", !showLine);
    const title = roiLinePlot.querySelector(".roi-plot-title");
    if (title) {
      title.textContent = mode === "line" ? "Line Profile" : "Radial Profile";
    }
  }
  if (roiBoxPlotX) {
    roiBoxPlotX.classList.toggle("is-hidden", mode !== "box");
  }
  if (roiBoxPlotY) {
    roiBoxPlotY.classList.toggle("is-hidden", mode !== "box");
  }
  if (roiPlotControls) {
    roiPlotControls.classList.toggle("is-hidden", !showPlots);
  }
  if (roiRadiusField) {
    roiRadiusField.classList.toggle("is-hidden", mode !== "circle");
  }
  if (roiCenterFields) {
    roiCenterFields.classList.toggle("is-hidden", mode !== "circle" && mode !== "annulus");
  }
  if (roiRingFields) {
    roiRingFields.classList.toggle("is-hidden", mode !== "annulus");
  }
  if (roiSizeLabel) {
    if (mode === "line") {
      roiSizeLabel.textContent = "Length (px)";
    } else if (mode === "box") {
      roiSizeLabel.textContent = "Size (WxH)";
    } else if (mode === "circle") {
      roiSizeLabel.textContent = "Radius (px)";
    } else if (mode === "annulus") {
      roiSizeLabel.textContent = "Rin → Rout";
    } else {
      roiSizeLabel.textContent = "Image";
    }
  }
  if (roiCountLabel) {
    roiCountLabel.textContent = mode === "line" ? "Samples" : "Pixels";
  }
  if (roiHelp) {
    if (mode === "annulus") {
      roiHelp.textContent = "Right‑drag to set outer radius. Adjust inner radius below.";
    } else if (mode === "circle") {
      roiHelp.textContent = "Right‑drag from center to set radius.";
    } else {
      roiHelp.textContent = "Right‑drag on the image to define the ROI.";
    }
  }
  updateRoiCenterInputs();
  syncRoiPlotLimitControls();
}

function clearRoi() {
  roiState.start = null;
  roiState.end = null;
  roiState.active = false;
  roiState.stats = null;
  roiState.lineProfile = null;
  roiState.xProjection = null;
  roiState.yProjection = null;
  roiState.innerRadius = 0;
  roiState.outerRadius = 0;
  if (roiRadiusInput) roiRadiusInput.value = "";
  if (roiCenterXInput) roiCenterXInput.value = "";
  if (roiCenterYInput) roiCenterYInput.value = "";
  if (roiInnerInput) roiInnerInput.value = "";
  if (roiOuterInput) roiOuterInput.value = "";
  setRoiText(roiStartEl, "-");
  setRoiText(roiEndEl, "-");
  setRoiText(roiSizeEl, "-");
  setRoiText(roiCountEl, "-");
  setRoiText(roiMinEl, "-");
  setRoiText(roiMaxEl, "-");
  setRoiText(roiSumEl, "-");
  setRoiText(roiMedianEl, "-");
  setRoiText(roiStdEl, "-");
  drawRoiPlot(roiLineCanvas, roiLineCtx, null, roiState.log);
  drawRoiPlot(roiXCanvas, roiXCtx, null, roiState.log);
  drawRoiPlot(roiYCanvas, roiYCtx, null, roiState.log);
  drawRoiOverlay();
}

function applyMaskToValue(value, maskValue) {
  if (!Number.isFinite(maskValue)) return { value, skip: false };
  if (maskValue & 1) {
    return { value: 0, skip: false };
  }
  if (maskValue & 0x1e) {
    return { value: 0, skip: true };
  }
  return { value, skip: false };
}

function sampleValue(ix, iy) {
  if (!state.dataRaw) return null;
  const idx = iy * state.width + ix;
  const raw = state.dataRaw[idx];
  if (
    state.maskEnabled &&
    state.maskAvailable &&
    state.maskRaw &&
    state.maskShape &&
    state.maskShape[0] === state.height &&
    state.maskShape[1] === state.width
  ) {
    const maskValue = state.maskRaw[idx];
    const masked = applyMaskToValue(raw, maskValue);
    return { value: masked.value, skip: masked.skip };
  }
  return { value: raw, skip: false };
}

function computeGlobalStats() {
  if (!state.dataRaw) return null;
  let min = Number.POSITIVE_INFINITY;
  let max = Number.NEGATIVE_INFINITY;
  let sum = 0;
  let count = 0;
  let mean = 0;
  let m2 = 0;
  const useMask =
    state.maskEnabled &&
    state.maskAvailable &&
    state.maskRaw &&
    state.maskShape &&
    state.maskShape[0] === state.height &&
    state.maskShape[1] === state.width;

  for (let i = 0; i < state.dataRaw.length; i += 1) {
    let v = state.dataRaw[i];
    if (!Number.isFinite(v)) continue;
    if (useMask) {
      const maskValue = state.maskRaw[i];
      const masked = applyMaskToValue(v, maskValue);
      if (masked.skip) continue;
      v = masked.value;
    }
    count += 1;
    sum += v;
    min = Math.min(min, v);
    max = Math.max(max, v);
    const delta = v - mean;
    mean += delta / count;
    m2 += delta * (v - mean);
  }

  if (count === 0) {
    return { count: 0, sum: 0, mean: 0, min: 0, max: 0, std: 0 };
  }
  const std = count > 1 ? Math.sqrt(m2 / (count - 1)) : 0;
  return { count, sum, mean, min, max, std };
}

function updateGlobalStats() {
  state.globalStats = computeGlobalStats();
}

function showRoiTooltip(canvasEl, text, clientX, clientY) {
  if (!canvasEl) return;
  const container = canvasEl.parentElement;
  if (!container) return;
  const tooltip = container.querySelector(".roi-tooltip");
  if (!tooltip) return;
  tooltip.textContent = text;
  tooltip.classList.add("is-visible");
  tooltip.setAttribute("aria-hidden", "false");
  const rect = container.getBoundingClientRect();
  let left = clientX - rect.left + 8;
  let top = clientY - rect.top + 8;
  const maxLeft = rect.width - tooltip.offsetWidth - 6;
  const maxTop = rect.height - tooltip.offsetHeight - 6;
  left = Math.min(maxLeft, Math.max(6, left));
  top = Math.min(maxTop, Math.max(6, top));
  tooltip.style.left = `${left}px`;
  tooltip.style.top = `${top}px`;
}

function hideRoiTooltip(canvasEl) {
  if (!canvasEl) return;
  const container = canvasEl.parentElement;
  const tooltip = container?.querySelector(".roi-tooltip");
  if (!tooltip) return;
  tooltip.classList.remove("is-visible");
  tooltip.setAttribute("aria-hidden", "true");
}

function updateRoiTooltip(event, canvasEl) {
  const plot = canvasEl?._roiPlot;
  if (!plot || !plot.data || plot.data.length === 0) {
    hideRoiTooltip(canvasEl);
    return;
  }
  const rect = canvasEl.getBoundingClientRect();
  const x = event.clientX - rect.left;
  const y = event.clientY - rect.top;
  const plotX = x - plot.padL;
  const plotW = plot.width - plot.padL - plot.padR;
  const plotH = plot.height - plot.padT - plot.padB;
  if (plotX < 0 || plotX > plotW || y < plot.padT || y > plot.padT + plotH) {
    hideRoiTooltip(canvasEl);
    return;
  }
  const t = plotW ? plotX / plotW : 0;
  const idx = Math.max(0, Math.min(plot.data.length - 1, Math.round(t * (plot.data.length - 1))));
  const xValue = plot.xStart + idx * plot.xStep;
  const value = plot.data[idx];
  const label = `${plot.xLabel} ${xValue}  Value ${formatStat(value)}`;
  showRoiTooltip(canvasEl, label, event.clientX, event.clientY);
}

function updateRoiStats() {
  // This function is intentionally central: it computes ROI statistics and
  // updates all derived plots/labels in one pass to keep UI state consistent.
  if (!state.hasFrame) {
    if (roiState.active) {
      clearRoi();
    }
    return;
  }
  if (roiState.mode === "none") {
    roiState.active = false;
    const stats = state.globalStats;
    setRoiText(roiStartEl, "-");
    setRoiText(roiEndEl, "-");
    if (roiSizeLabel) {
      roiSizeLabel.textContent = "Image";
    }
    if (roiCountLabel) {
      roiCountLabel.textContent = "Pixels";
    }
    setRoiText(roiSizeEl, state.width && state.height ? `${state.width} × ${state.height}` : "-");
    setRoiText(roiCountEl, stats ? `${stats.count}` : "-");
    setRoiText(roiMinEl, stats ? formatStat(stats.min) : "-");
    setRoiText(roiMaxEl, stats ? formatStat(stats.max) : "-");
    setRoiText(roiSumEl, stats ? formatStat(stats.sum) : "-");
    setRoiText(roiMedianEl, stats && Number.isFinite(stats.median) ? formatStat(stats.median) : "-");
    setRoiText(roiStdEl, stats ? formatStat(stats.std) : "-");
    if (roiLineCanvas) {
      roiLineCanvas._roiPlotMeta = null;
    }
    if (roiXCanvas) {
      roiXCanvas._roiPlotMeta = null;
    }
    if (roiYCanvas) {
      roiYCanvas._roiPlotMeta = null;
    }
    drawRoiPlot(roiLineCanvas, roiLineCtx, null, roiState.log);
    drawRoiPlot(roiXCanvas, roiXCtx, null, roiState.log);
    drawRoiPlot(roiYCanvas, roiYCtx, null, roiState.log);
    drawRoiOverlay();
    return;
  }
  if (!roiState.start || !roiState.end) {
    const stats = state.globalStats;
    setRoiText(roiStartEl, "-");
    setRoiText(roiEndEl, "-");
    setRoiText(roiSizeEl, "-");
    setRoiText(roiCountEl, stats ? `${stats.count}` : "-");
    setRoiText(roiMinEl, stats ? formatStat(stats.min) : "-");
    setRoiText(roiMaxEl, stats ? formatStat(stats.max) : "-");
    setRoiText(roiSumEl, stats ? formatStat(stats.sum) : "-");
    setRoiText(roiMedianEl, stats && Number.isFinite(stats.median) ? formatStat(stats.median) : "-");
    setRoiText(roiStdEl, stats ? formatStat(stats.std) : "-");
    drawRoiPlot(roiLineCanvas, roiLineCtx, null, roiState.log);
    drawRoiPlot(roiXCanvas, roiXCtx, null, roiState.log);
    drawRoiPlot(roiYCanvas, roiYCtx, null, roiState.log);
    drawRoiOverlay();
    return;
  }
  const x0 = Math.max(0, Math.min(state.width - 1, roiState.start.x));
  const y0 = Math.max(0, Math.min(state.height - 1, roiState.start.y));
  const x1 = Math.max(0, Math.min(state.width - 1, roiState.end.x));
  const y1 = Math.max(0, Math.min(state.height - 1, roiState.end.y));
  setRoiText(roiStartEl, `${x0}, ${y0}`);
  setRoiText(roiEndEl, `${x1}, ${y1}`);

  let min = Number.POSITIVE_INFINITY;
  let max = Number.NEGATIVE_INFINITY;
  let sum = 0;
  let count = 0;
  let mean = 0;
  let m2 = 0;
  const statsValues = [];

  if (roiState.mode === "line") {
    const dx = x1 - x0;
    const dy = y1 - y0;
    const steps = Math.max(Math.abs(dx), Math.abs(dy));
    const values = [];
    for (let i = 0; i <= steps; i += 1) {
      const t = steps === 0 ? 0 : i / steps;
      const ix = Math.max(0, Math.min(state.width - 1, Math.round(x0 + dx * t)));
      const iy = Math.max(0, Math.min(state.height - 1, Math.round(y0 + dy * t)));
      const sampled = sampleValue(ix, iy);
      if (!sampled) continue;
      const v = sampled.value;
      values.push(Number.isFinite(v) ? v : 0);
      if (sampled.skip || !Number.isFinite(v)) {
        continue;
      }
      statsValues.push(v);
      count += 1;
      sum += v;
      min = Math.min(min, v);
      max = Math.max(max, v);
      const delta = v - mean;
      mean += delta / count;
      m2 += delta * (v - mean);
    }
    const length = Math.hypot(dx, dy);
    roiState.lineProfile = values;
    roiState.xProjection = null;
    roiState.yProjection = null;
    setRoiText(roiSizeEl, formatStat(length));
    setRoiText(roiCountEl, count ? `${count}` : "0");
    const std = count > 1 ? Math.sqrt(m2 / (count - 1)) : 0;
    const median = statsValues.length ? computeMedian(statsValues) : Number.NaN;
    setRoiText(roiMinEl, count ? formatStat(min) : "-");
    setRoiText(roiMaxEl, count ? formatStat(max) : "-");
    setRoiText(roiSumEl, count ? formatStat(sum) : "-");
    setRoiText(roiMedianEl, count ? formatStat(median) : "-");
    setRoiText(roiStdEl, count ? formatStat(std) : "-");
    if (roiLineCanvas) {
      roiLineCanvas._roiPlotMeta = {
        xLabel: "Sample",
        yLabel: "Intensity",
        xStart: 0,
        xStep: 1,
      };
    }
    drawRoiPlot(roiLineCanvas, roiLineCtx, values, roiState.log);
    drawRoiPlot(roiXCanvas, roiXCtx, null, roiState.log);
    drawRoiPlot(roiYCanvas, roiYCtx, null, roiState.log);
  } else if (roiState.mode === "box") {
    const left = Math.min(x0, x1);
    const right = Math.max(x0, x1);
    const top = Math.min(y0, y1);
    const bottom = Math.max(y0, y1);
    const width = right - left + 1;
    const height = bottom - top + 1;
    const xProj = new Float64Array(width);
    const yProj = new Float64Array(height);
    const xCounts = new Int32Array(width);
    const yCounts = new Int32Array(height);

    for (let y = top; y <= bottom; y += 1) {
      const rowIndex = y - top;
      for (let x = left; x <= right; x += 1) {
        const colIndex = x - left;
        const sampled = sampleValue(x, y);
        if (!sampled) continue;
        const v = sampled.value;
        if (!sampled.skip && Number.isFinite(v)) {
          statsValues.push(v);
          count += 1;
          sum += v;
          min = Math.min(min, v);
          max = Math.max(max, v);
          const delta = v - mean;
          mean += delta / count;
          m2 += delta * (v - mean);
          xProj[colIndex] += v;
          yProj[rowIndex] += v;
          xCounts[colIndex] += 1;
          yCounts[rowIndex] += 1;
        }
      }
    }
    for (let i = 0; i < xProj.length; i += 1) {
      xProj[i] = xCounts[i] > 0 ? xProj[i] / xCounts[i] : 0;
    }
    for (let i = 0; i < yProj.length; i += 1) {
      yProj[i] = yCounts[i] > 0 ? yProj[i] / yCounts[i] : 0;
    }
    roiState.xProjection = Array.from(xProj);
    roiState.yProjection = Array.from(yProj);
    roiState.lineProfile = null;
    setRoiText(roiSizeEl, `${width} × ${height}`);
    setRoiText(roiCountEl, count ? `${count}` : "0");
    const std = count > 1 ? Math.sqrt(m2 / (count - 1)) : 0;
    const median = statsValues.length ? computeMedian(statsValues) : Number.NaN;
    setRoiText(roiMinEl, count ? formatStat(min) : "-");
    setRoiText(roiMaxEl, count ? formatStat(max) : "-");
    setRoiText(roiSumEl, count ? formatStat(sum) : "-");
    setRoiText(roiMedianEl, count ? formatStat(median) : "-");
    setRoiText(roiStdEl, count ? formatStat(std) : "-");
    drawRoiPlot(roiLineCanvas, roiLineCtx, null, roiState.log);
    if (roiXCanvas) {
      roiXCanvas._roiPlotMeta = {
        xLabel: "X Pixel",
        yLabel: "Mean",
        xStart: left,
        xStep: 1,
      };
    }
    if (roiYCanvas) {
      roiYCanvas._roiPlotMeta = {
        xLabel: "Y Pixel",
        yLabel: "Mean",
        xStart: top,
        xStep: 1,
      };
    }
    drawRoiPlot(roiXCanvas, roiXCtx, roiState.xProjection, roiState.log);
    drawRoiPlot(roiYCanvas, roiYCtx, roiState.yProjection, roiState.log);
  } else if (roiState.mode === "circle" || roiState.mode === "annulus") {
    const dx = x1 - x0;
    const dy = y1 - y0;
    const outerRadius = Math.max(1, Math.round(Math.hypot(dx, dy)));
    if (roiState.mode === "circle") {
      roiState.innerRadius = 0;
      roiState.outerRadius = outerRadius;
      if (roiRadiusInput) roiRadiusInput.value = String(outerRadius);
    } else {
      roiState.outerRadius = outerRadius;
      let inner = Math.max(0, Math.round(roiState.innerRadius || 0));
      if (!inner || inner >= outerRadius) {
        inner = Math.max(0, Math.round(outerRadius * 0.5));
      }
      roiState.innerRadius = inner;
      if (roiInnerInput) roiInnerInput.value = String(inner);
      if (roiOuterInput) roiOuterInput.value = String(outerRadius);
    }

    const left = Math.max(0, Math.floor(x0 - outerRadius));
    const right = Math.min(state.width - 1, Math.ceil(x0 + outerRadius));
    const top = Math.max(0, Math.floor(y0 - outerRadius));
    const bottom = Math.min(state.height - 1, Math.ceil(y0 + outerRadius));
    const innerR2 = roiState.innerRadius * roiState.innerRadius;
    const outerR2 = outerRadius * outerRadius;
    const radialSum = new Float64Array(outerRadius + 1);
    const radialCount = new Uint32Array(outerRadius + 1);

    for (let y = top; y <= bottom; y += 1) {
      const dyPix = y - y0;
      for (let x = left; x <= right; x += 1) {
        const dxPix = x - x0;
        const r2 = dxPix * dxPix + dyPix * dyPix;
        if (r2 > outerR2 || r2 < innerR2) continue;
        const sampled = sampleValue(x, y);
        if (!sampled) continue;
        const v = sampled.value;
        if (!sampled.skip && Number.isFinite(v)) {
          statsValues.push(v);
          count += 1;
          sum += v;
          min = Math.min(min, v);
          max = Math.max(max, v);
          const delta = v - mean;
          mean += delta / count;
          m2 += delta * (v - mean);
          const r = Math.min(outerRadius, Math.floor(Math.sqrt(r2)));
          radialSum[r] += v;
          radialCount[r] += 1;
        }
      }
    }

    const profile = Array.from(radialSum, (v, i) => (radialCount[i] ? v / radialCount[i] : 0));
    let displayProfile = profile;
    let displayStart = 0;
    if (roiState.mode === "annulus" && roiState.innerRadius > 0) {
      displayStart = Math.min(roiState.innerRadius, profile.length - 1);
      displayProfile = profile.slice(displayStart);
    }
    roiState.lineProfile = displayProfile;
    roiState.xProjection = null;
    roiState.yProjection = null;
    if (roiState.mode === "circle") {
      setRoiText(roiSizeEl, `${outerRadius}`);
    } else {
      setRoiText(roiSizeEl, `${roiState.innerRadius} → ${outerRadius}`);
    }
    setRoiText(roiCountEl, count ? `${count}` : "0");
    const std = count > 1 ? Math.sqrt(m2 / (count - 1)) : 0;
    const median = statsValues.length ? computeMedian(statsValues) : Number.NaN;
    setRoiText(roiMinEl, count ? formatStat(min) : "-");
    setRoiText(roiMaxEl, count ? formatStat(max) : "-");
    setRoiText(roiSumEl, count ? formatStat(sum) : "-");
    setRoiText(roiMedianEl, count ? formatStat(median) : "-");
    setRoiText(roiStdEl, count ? formatStat(std) : "-");
    if (roiLineCanvas) {
      roiLineCanvas._roiPlotMeta = {
        xLabel: "Radius (px)",
        yLabel: "Intensity",
        xStart: displayStart,
        xStep: 1,
      };
    }
    drawRoiPlot(roiLineCanvas, roiLineCtx, displayProfile, roiState.log);
    drawRoiPlot(roiXCanvas, roiXCtx, null, roiState.log);
    drawRoiPlot(roiYCanvas, roiYCtx, null, roiState.log);
  }
  drawRoiOverlay();
}

function drawRoiPlot(canvasEl, ctx, data, logScale) {
  if (!canvasEl || !ctx) return;
  const width = canvasEl.clientWidth || 1;
  const height = canvasEl.clientHeight || 1;
  canvasEl.width = Math.max(1, Math.floor(width * window.devicePixelRatio));
  canvasEl.height = Math.max(1, Math.floor(height * window.devicePixelRatio));
  ctx.setTransform(window.devicePixelRatio, 0, 0, window.devicePixelRatio, 0, 0);
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = "#2b2b2b";
  ctx.fillRect(0, 0, width, height);
  if (!data || data.length === 0) {
    canvasEl._roiPlot = null;
    ctx.strokeStyle = "rgba(0,0,0,0.5)";
    ctx.strokeRect(0.5, 0.5, width - 1, height - 1);
    return;
  }
  const plotMeta = canvasEl._roiPlotMeta || {};
  const plotKey = getRoiPlotKey(canvasEl);
  const limits = getRoiPlotLimits(plotKey);
  const autoscale = roiState.plotLimits.autoscale;
  const xStepRaw = Number(plotMeta.xStep ?? 1);
  const xStep = Number.isFinite(xStepRaw) && xStepRaw !== 0 ? xStepRaw : 1;
  let xStart = Number(plotMeta.xStart ?? 0) || 0;
  let visibleData = data;
  if (!autoscale && data.length > 0) {
    const totalMinX = xStart;
    const totalMaxX = xStart + (data.length - 1) * xStep;
    const lo = Number.isFinite(limits.xMin) ? Math.max(totalMinX, limits.xMin) : totalMinX;
    const hi = Number.isFinite(limits.xMax) ? Math.min(totalMaxX, limits.xMax) : totalMaxX;
    if (hi < lo) {
      visibleData = [];
    } else {
      const firstIdx = Math.max(0, Math.ceil((lo - xStart) / xStep));
      const lastIdx = Math.min(data.length - 1, Math.floor((hi - xStart) / xStep));
      if (lastIdx >= firstIdx) {
        visibleData = data.slice(firstIdx, lastIdx + 1);
        xStart += firstIdx * xStep;
      } else {
        visibleData = [];
      }
    }
  }
  if (!visibleData || visibleData.length === 0) {
    canvasEl._roiPlot = null;
    ctx.strokeStyle = "rgba(0,0,0,0.5)";
    ctx.strokeRect(0.5, 0.5, width - 1, height - 1);
    return;
  }

  const valuesRaw = visibleData;
  const values = logScale ? valuesRaw.map((v) => Math.log10(1 + Math.max(0, v))) : valuesRaw;
  let minValue = 0;
  if (!autoscale && Number.isFinite(limits.yMin)) {
    minValue = logScale ? Math.log10(1 + Math.max(0, limits.yMin)) : limits.yMin;
  }
  let maxValue = Math.max(...values);
  if (!autoscale && Number.isFinite(limits.yMax)) {
    maxValue = logScale ? Math.log10(1 + Math.max(0, limits.yMax)) : limits.yMax;
  }
  if (!Number.isFinite(minValue)) minValue = 0;
  if (!Number.isFinite(maxValue) || maxValue <= minValue) {
    maxValue = minValue + 1;
  }
  const yRange = maxValue - minValue;
  const padL = 40;
  const padR = 8;
  const padT = 8;
  const padB = 30;
  const drawableHeight = Math.max(4, height - padT - padB);
  const drawableWidth = Math.max(4, width - padL - padR);
  ctx.strokeStyle = "rgba(90, 90, 90, 0.9)";
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(padL, padT);
  ctx.lineTo(padL, padT + drawableHeight);
  ctx.lineTo(padL + drawableWidth, padT + drawableHeight);
  ctx.stroke();

  ctx.strokeStyle = "rgba(120, 120, 120, 0.8)";
  ctx.fillStyle = "#cfcfcf";
  ctx.font = "10px \"Lucida Grande\", \"Helvetica Neue\", Arial, sans-serif";

  const measureMaxLabel = (labels) =>
    labels.reduce((max, label) => Math.max(max, ctx.measureText(label).width), 0);

  let xTickCount = Math.max(2, Math.min(4, Math.floor(drawableWidth / 90)));
  while (xTickCount > 1) {
    const labels = [];
    for (let i = 0; i <= xTickCount; i += 1) {
      const t = i / xTickCount;
      const xValue = xStart + t * (values.length - 1) * xStep;
      labels.push(formatRoiTick(xValue));
    }
    const maxLabel = measureMaxLabel(labels);
    const spacing = drawableWidth / xTickCount;
    if (maxLabel + 6 <= spacing) break;
    xTickCount -= 1;
  }

  ctx.textAlign = "center";
  ctx.textBaseline = "top";
  for (let i = 0; i <= xTickCount; i += 1) {
    const t = i / xTickCount;
    const x = padL + t * drawableWidth;
    ctx.beginPath();
    ctx.moveTo(x, padT + drawableHeight);
    ctx.lineTo(x, padT + drawableHeight + 4);
    ctx.stroke();
    const xValue = xStart + t * (values.length - 1) * xStep;
    ctx.fillText(formatRoiTick(xValue), x, padT + drawableHeight + 6);
  }

  let yTickCount = Math.max(2, Math.min(4, Math.floor(drawableHeight / 50)));
  while (yTickCount > 1) {
    const labels = [];
    for (let i = 0; i <= yTickCount; i += 1) {
      const t = i / yTickCount;
      const displayVal = minValue + t * yRange;
      const actualVal = logScale ? Math.pow(10, displayVal) - 1 : displayVal;
      labels.push(formatRoiTick(actualVal));
    }
    const maxLabel = measureMaxLabel(labels);
    const spacing = drawableHeight / yTickCount;
    if (maxLabel + 6 <= spacing) break;
    yTickCount -= 1;
  }

  ctx.textAlign = "right";
  ctx.textBaseline = "middle";
  for (let i = 0; i <= yTickCount; i += 1) {
    const t = i / yTickCount;
    const y = padT + drawableHeight - t * drawableHeight;
    ctx.beginPath();
    ctx.moveTo(padL - 4, y);
    ctx.lineTo(padL, y);
    ctx.stroke();
    const displayVal = minValue + t * yRange;
    const actualVal = logScale ? Math.pow(10, displayVal) - 1 : displayVal;
    ctx.fillText(formatRoiTick(actualVal), padL - 8, y);
  }

  ctx.strokeStyle = "#e5e5e5";
  ctx.lineWidth = 1;
  ctx.beginPath();
  values.forEach((v, i) => {
    const x = padL + (i / Math.max(1, values.length - 1)) * drawableWidth;
    const yNorm = yRange ? (v - minValue) / yRange : 0;
    const y = padT + drawableHeight - Math.max(0, Math.min(1, yNorm)) * drawableHeight;
    if (i === 0) {
      ctx.moveTo(x, y);
    } else {
      ctx.lineTo(x, y);
    }
  });
  ctx.stroke();

  const xLabel = plotMeta.xLabel || "Index";
  const yLabel = plotMeta.yLabel || "Value";
  const xMinActual = xStart;
  const xMaxActual = xStart + (values.length - 1) * xStep;
  const yMinActual = logScale ? Math.max(0, Math.pow(10, minValue) - 1) : minValue;
  const yMaxActual = logScale ? Math.max(0, Math.pow(10, maxValue) - 1) : maxValue;
  ctx.fillStyle = "#cfcfcf";
  ctx.font = "10px \"Lucida Grande\", \"Helvetica Neue\", Arial, sans-serif";
  ctx.textAlign = "center";
  ctx.fillText(xLabel, padL + drawableWidth / 2, height - 4);
  ctx.save();
  ctx.translate(Math.max(12, padL - 26), padT + drawableHeight / 2);
  ctx.rotate(-Math.PI / 2);
  ctx.fillText(yLabel, 0, 0);
  ctx.restore();

  canvasEl._roiPlot = {
    data: visibleData,
    log: logScale,
    xLabel,
    yLabel,
    padL,
    padR,
    padT,
    padB,
    width,
    height,
    xStart,
    xStep,
    xMin: xMinActual,
    xMax: xMaxActual,
    yMin: yMinActual,
    yMax: yMaxActual,
  };
  ctx.strokeStyle = "rgba(0,0,0,0.5)";
  ctx.strokeRect(0.5, 0.5, width - 1, height - 1);
}

function exportRoiCsv() {
  if (!roiState.active || roiState.mode === "none") {
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
  state.globalStats = null;
  analysisState.peaks = [];
  analysisState.selectedPeaks = [];
  analysisState.peakSelectionAnchor = null;
  clearMaskState();
  clearImageHeader();
  updateToolbar();
  setStatus("No file loaded");
  setLoading(false);
  hideUploadProgress();
  hideProcessingProgress();
  showSplash();
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
  clearHistogram();
  renderPeakList();
  schedulePeakOverlay();
  syncSeriesSumOutputPath(true);
  clearRoi();
  updatePlayButtons();
}

function applyFrame(data, width, height, dtype) {
  state.dataRaw = data;
  state.dataFloat = renderer.type === "webgl" ? toFloat32(data) : null;
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
  hideSplash();
  updatePlayButtons();
  scheduleOverview();
  scheduleRoiUpdate();
  schedulePixelOverlay();
  scheduleResolutionOverlay();
  schedulePeakFinder();
}

function redraw() {
  if (!state.dataRaw) return;
  const palette = buildPalette(state.colormap);
  const maskReady =
    state.maskEnabled &&
    state.maskAvailable &&
    state.maskRaw &&
    state.maskShape &&
    state.maskShape[0] === state.height &&
    state.maskShape[1] === state.width;
  const maskData = maskReady ? state.maskRaw : null;
  const maskWidth = maskReady ? state.maskShape[1] : 0;
  const maskHeight = maskReady ? state.maskShape[0] : 0;
  if (renderer.type === "webgl") {
    renderer.render({
      floatData: state.dataFloat,
      width: state.width,
      height: state.height,
      min: state.min,
      max: state.max,
      palette,
      invert: state.invert,
      colormap: state.colormap,
      mask: maskData,
      maskWidth,
      maskHeight,
      maskEnabled: maskReady,
    });
  } else {
    renderer.render({
      data: state.dataRaw,
      width: state.width,
      height: state.height,
      min: state.min,
      max: state.max,
      palette,
      mask: maskData,
      maskEnabled: maskReady,
    });
  }
  if (state.histogram) {
    drawHistogram(state.histogram);
  }
  scheduleOverview();
  scheduleHistogram();
  schedulePixelOverlay();
  schedulePeakOverlay();
}

async function loadSeriesFrame() {
  const files = Array.isArray(state.seriesFiles) ? state.seriesFiles : [];
  if (!files.length) return;
  const file = files[state.frameIndex];
  if (!file) return;
  if (state.isLoading) return;
  state.isLoading = true;
  if (!state.playing) {
    setLoading(true);
    setStatus("Loading frame…");
  } else {
    setLoading(false);
  }
  const res = await fetch(`${API}/image?file=${encodeURIComponent(file)}`);
  if (!res.ok) {
    setStatus("Failed to load image");
    setLoading(false);
    state.isLoading = false;
    if (!state.hasFrame) {
      showSplash();
    }
    return;
  }
  const buffer = await res.arrayBuffer();
  const dtype = parseDtype(res.headers.get("X-Dtype"));
  const shape = parseShape(res.headers.get("X-Shape"));
  const data = typedArrayFrom(buffer, dtype);
  applyImageMeta(res.headers);

  const height = shape[0];
  const width = shape[1];
  metaShape.textContent = `${width} × ${height}`;
  metaDtype.textContent = dtype;

  const seriesKey = state.seriesLabel || state.file || file;
  const reuseMask = Boolean(state.maskRaw && state.maskFile === `auto:${seriesKey}`);
  applyExternalFrame(data, shape, dtype, state.file || file, false, reuseMask, {
    preserveSeries: true,
    keepPlaying: true,
    autoMask: !reuseMask,
    maskKey: `auto:${seriesKey}`,
  });
  setStatus(`Frame ${state.frameIndex + 1} / ${state.frameCount}`);
  updateToolbar();
  setLoading(false);
  state.isLoading = false;
  if (state.pendingFrame !== null && state.pendingFrame !== state.frameIndex) {
    const next = state.pendingFrame;
    state.pendingFrame = null;
    requestFrame(next);
  } else {
    state.pendingFrame = null;
  }
}

async function loadFrame() {
  if (Array.isArray(state.seriesFiles) && state.seriesFiles.length > 0) {
    await loadSeriesFrame();
    return;
  }
  if (!state.file || !state.dataset) return;
  if (state.isLoading) return;
  state.isLoading = true;
  if (!state.playing) {
    setLoading(true);
    setStatus("Loading frame…");
  } else {
    setLoading(false);
  }
  const url = `${API}/frame?file=${encodeURIComponent(state.file)}&dataset=${encodeURIComponent(
    state.dataset
  )}&index=${state.frameIndex}${
    state.thresholdCount > 1 ? `&threshold=${state.thresholdIndex}` : ""
  }`;
  const res = await fetch(url);
  if (!res.ok) {
    setStatus("Failed to load frame");
    setLoading(false);
    state.isLoading = false;
    if (!state.hasFrame) {
      showSplash();
    }
    return;
  }
  const buffer = await res.arrayBuffer();
  const dtype = parseDtype(res.headers.get("X-Dtype"));
  const shape = parseShape(res.headers.get("X-Shape"));
  const data = typedArrayFrom(buffer, dtype);

  const height = shape[0];
  const width = shape[1];
  metaShape.textContent = `${width} × ${height}`;
  metaDtype.textContent = dtype;

  applyFrame(data, width, height, dtype);
  setStatus(`Frame ${state.frameIndex + 1} / ${state.frameCount}`);
  updateToolbar();
  setLoading(false);
  state.isLoading = false;
  if (state.pendingFrame !== null && state.pendingFrame !== state.frameIndex) {
    const next = state.pendingFrame;
    state.pendingFrame = null;
    requestFrame(next);
  } else {
    state.pendingFrame = null;
  }
}

menuButtons.forEach((btn) => {
  btn.addEventListener("mouseenter", () => {
    cancelClose();
    openMenu(btn.dataset.menu, btn);
  });
  btn.addEventListener("click", () => {
    cancelClose();
    if (dropdown.classList.contains("is-open") && activeMenu === btn.dataset.menu) {
      closeMenu();
    } else {
      openMenu(btn.dataset.menu, btn);
    }
  });
});

submenuParents.forEach((parent) => {
  parent.addEventListener("click", (event) => {
    if (!isCoarsePointerDevice()) return;
    if (event.target.closest(".dropdown-submenu")) return;
    const alreadyOpen = parent.classList.contains("is-open");
    closeSubmenus();
    if (!alreadyOpen) {
      parent.classList.add("is-open");
    }
    event.preventDefault();
    event.stopPropagation();
  });
});

dropdown?.addEventListener("mouseenter", cancelClose);
dropdown?.addEventListener("mouseleave", scheduleClose);

document.addEventListener("click", (event) => {
  if (!dropdown) return;
  const withinMenu = event.target.closest(".menu-bar") || event.target.closest(".menu-dropdown");
  if (!withinMenu) {
    closeMenu();
  }
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") {
    closeMenu();
    aboutModal?.classList.remove("is-open");
    settingsModal?.classList.remove("is-open");
    if (browseModal?.classList.contains("is-open")) {
      closeFileBrowser();
    }
    return;
  }
  if (handleNavShortcut(event)) {
    return;
  }
  if (event.key === "F1") {
    event.preventDefault();
    handleMenuAction("help-docs");
    return;
  }
  handleShortcut(event);
});

menuActions.forEach((item) => {
  item.addEventListener("click", () => {
    if (item.classList.contains("is-disabled")) return;
    handleMenuAction(item.dataset.action);
    closeMenu();
  });
});

inspectorTree?.addEventListener("click", async (event) => {
  const row = event.target.closest(".inspector-row");
  if (!row) return;
  const node = row.parentElement;
  if (!node) return;
  const nodeType = node.dataset.type || "";
  const nodePath = node.dataset.path || "";
  selectInspectorRow(row);
  if (nodeType === "link") {
    renderInspectorLink(nodePath || "-", node.dataset.target || "-");
    return;
  }
  if (nodeType === "group") {
    const toggle = node.querySelector(".inspector-toggle");
    const willOpen = !node.classList.contains("is-open");
    node.classList.toggle("is-open", willOpen);
    if (toggle) {
      toggle.textContent = willOpen ? "▾" : "▸";
    }
    if (willOpen && node.dataset.loaded !== "true") {
      try {
        const children = await fetchInspectorTree(nodePath);
        const container = node.querySelector(".inspector-children");
        if (container) {
          renderInspectorTree(children, container);
        }
        node.dataset.loaded = "true";
      } catch (err) {
        console.error(err);
      }
    }
  }
  if (nodePath) {
    await showInspectorNode(nodePath);
  }
});

let inspectorSearchTimer = null;
inspectorSearchInput?.addEventListener("input", () => {
  if (inspectorSearchTimer) {
    window.clearTimeout(inspectorSearchTimer);
  }
  const query = inspectorSearchInput.value.trim();
  inspectorSearchTimer = window.setTimeout(() => {
    runInspectorSearch(query);
  }, 250);
});

inspectorSearchClear?.addEventListener("click", () => {
  clearInspectorSearch();
  runInspectorSearch("");
});

inspectorResults?.addEventListener("click", async (event) => {
  const row = event.target.closest(".inspector-result");
  if (!row) return;
  const nodePath = row.dataset.path || "";
  const nodeType = row.dataset.type || "";
  if (!nodePath) return;
  if (nodeType === "link") {
    renderInspectorLink(nodePath, row.dataset.target || "-");
    return;
  }
  await showInspectorNode(nodePath);
});

if (fileInput) {
  fileInput.addEventListener("change", async () => {
    const file = fileInput.files?.[0];
    if (!file) return;
    await ensureFileMode();
    const uploadFolder = (autoloadDir?.value || state.autoload.dir || "").trim();
    setLoading(true);
    setStatus("Checking for existing file…");
    try {
      const existing = await findExistingFile(file.name, uploadFolder);
      if (existing) {
        setStatus(`Using existing file: ${existing}`);
        await loadFiles();
        state.file = existing;
        fileSelect.value = existing;
        await loadDatasets();
        setLoading(false);
        return;
      }
      setStatus("Uploading file…");
      showUploadProgress();
      const payload = await new Promise((resolve, reject) => {
        const xhr = new XMLHttpRequest();
        const uploadUrl = uploadFolder
          ? `${API}/upload?folder=${encodeURIComponent(uploadFolder)}`
          : `${API}/upload`;
        xhr.open("POST", uploadUrl, true);
        xhr.responseType = "json";
        xhr.upload.addEventListener("progress", (event) => {
          if (!event.lengthComputable) {
            updateUploadProgress(0);
            return;
          }
          const percent = Math.round((event.loaded / event.total) * 100);
          updateUploadProgress(percent);
        });
        xhr.addEventListener("load", () => {
          if (xhr.status >= 200 && xhr.status < 300) {
            resolve(xhr.response);
          } else {
            reject(new Error(xhr.response?.detail || "Upload failed"));
          }
        });
        xhr.addEventListener("error", () => reject(new Error("Upload failed")));
        const form = new FormData();
        form.append("file", file);
        xhr.send(form);
      });
      updateUploadProgress(100);
      await loadFiles();
      if (payload?.filename) {
        state.file = payload.filename;
        fileSelect.value = payload.filename;
        await loadDatasets();
      }
    } catch (err) {
      console.error(err);
      setStatus("Failed to upload file");
      setLoading(false);
    } finally {
      hideUploadProgress();
      fileInput.value = "";
    }
  });
}

if (aboutClose) {
  aboutClose.addEventListener("click", () => {
    aboutModal?.classList.remove("is-open");
  });
}

settingsClose?.addEventListener("click", closeSettingsModal);
settingsCancel?.addEventListener("click", closeSettingsModal);
settingsSave?.addEventListener("click", () => {
  void saveSettingsFromModal();
});
settingsModal?.querySelector(".modal-backdrop")?.addEventListener("click", closeSettingsModal);
fileSelect.addEventListener("change", async (event) => {
  await ensureFileMode();
  state.file = event.target.value;
  if (!state.file) return;
  syncSeriesSumOutputPath();
  stopPlayback();
  if (isHdfFile(state.file)) {
    await loadDatasets();
  } else {
    await loadImageSeries(state.file);
  }
});

datasetSelect.addEventListener("change", async (event) => {
  state.dataset = event.target.value;
  stopPlayback();
  await loadMetadata();
});

thresholdSelect?.addEventListener("change", async (event) => {
  const value = Math.max(0, Number(event.target.value || 0));
  await setThresholdIndex(value);
});

toolbarThresholdSelect?.addEventListener("change", async (event) => {
  const value = Math.max(0, Number(event.target.value || 0));
  await setThresholdIndex(value);
});

frameRange.addEventListener("input", async (event) => {
  stopPlayback();
  const value = Math.round(Number(event.target.value || 1));
  requestFrame(value - 1);
});

frameIndex.addEventListener("change", async (event) => {
  stopPlayback();
  const value = Math.round(Number(event.target.value || 1));
  requestFrame(value - 1);
});

frameStep?.addEventListener("change", () => {
  const value = Math.max(1, Math.round(Number(frameStep.value || 1)));
  state.step = value;
  frameStep.value = String(value);
});

fpsRange?.addEventListener("input", () => {
  setFps(Number(fpsRange.value));
});

autoloadMode?.addEventListener("change", () => {
  const nextMode = autoloadMode.value;
  if (state.autoload.running) {
    stopAutoload();
  }
  state.autoload.mode = nextMode;
  updateAutoloadUI();
  persistAutoloadSettings();
  if (state.autoload.mode === "file") {
    loadFiles().catch((err) => console.error(err));
  } else {
    startAutoload();
  }
});

autoloadDir?.addEventListener("change", () => {
  state.autoload.dir = autoloadDir.value.trim();
  persistAutoloadSettings();
  if (state.autoload.mode === "file") {
    loadFiles().catch((err) => console.error(err));
  }
  if (state.autoload.running && state.autoload.mode === "watch") {
    autoloadTick();
  }
});

autoloadInterval?.addEventListener("change", () => {
  state.autoload.interval = Math.max(200, Number(autoloadInterval.value || 1000));
  persistAutoloadSettings();
  if (state.autoload.running) {
    startAutoload();
  }
});

[autoloadTypeHdf5, autoloadTypeTiff, autoloadTypeCbf].forEach((input) => {
  input?.addEventListener("change", () => {
    state.autoload.types = {
      hdf5: autoloadTypeHdf5?.checked ?? true,
      tiff: autoloadTypeTiff?.checked ?? true,
      cbf: autoloadTypeCbf?.checked ?? true,
    };
    persistAutoloadSettings();
    if (state.autoload.running && state.autoload.mode === "watch") {
      autoloadTick();
    }
  });
});

autoloadPattern?.addEventListener("change", () => {
  state.autoload.pattern = autoloadPattern.value.trim();
  persistAutoloadSettings();
  if (state.autoload.running && state.autoload.mode === "watch") {
    autoloadTick();
  }
});

// File browser modal state and logic
let fileBrowserState = {
  currentPath: "",
  selectedPath: "",
  mode: null, // "autoload" or "series-sum"
  inputElement: null,
  filesystemMode: "local", // "local" or "remote" (default to local)
};

// Detect if backend and frontend are on the same machine
function isBackendLocal() {
  try {
    const url = new URL(API, window.location.href);
    const hostname = url.hostname.toLowerCase();
    return (
      hostname === "localhost" ||
      hostname === "127.0.0.1" ||
      hostname === "[::1]" ||
      hostname === "::1"
    );
  } catch {
    return false;
  }
}

const backendIsLocal = isBackendLocal();

const browseModal = document.getElementById("browse-modal");
const browseBreadcrumb = document.getElementById("browse-breadcrumb");
const browseFoldersList = document.getElementById("browse-folders-list");
const browseFilesList = document.getElementById("browse-files-list");
const browsePathInput = document.getElementById("browse-path-input");
const browseSelectBtn = document.getElementById("browse-select");
const browseCancelBtn = document.getElementById("browse-cancel");
const browseCloseBtn = document.getElementById("browse-close");

async function loadBrowseDirectory(path) {
  try {
    const query = path ? `?path=${encodeURIComponent(path)}` : "";
    const res = await fetch(`${API}/browse${query}`);
    if (!res.ok) {
      console.error("Failed to browse directory:", res.status);
      return null;
    }
    const data = await res.json();
    return data;
  } catch (err) {
    console.error("Browse directory error:", err);
    return null;
  }
}

function renderBrowseContent(data) {
  if (!data) return;

  fileBrowserState.currentPath = data.currentPath || "";
  fileBrowserState.selectedPath = fileBrowserState.currentPath;
  browsePathInput.value = fileBrowserState.selectedPath;

  // Update breadcrumb
  browseBreadcrumb.innerHTML = "";
  const rootBtn = document.createElement("button");
  rootBtn.className = "breadcrumb-btn";
  rootBtn.textContent = "Root";
  rootBtn.dataset.path = "";
  if (data.currentPath === "") {
    rootBtn.classList.add("is-active");
  }
  rootBtn.addEventListener("click", () => loadAndRenderBrowser(""));
  browseBreadcrumb.appendChild(rootBtn);

  if (data.currentPath && data.currentPath !== "") {
    const parts = data.currentPath.split("/");
    let accumulated = "";
    for (const part of parts) {
      accumulated = accumulated ? `${accumulated}/${part}` : part;
      const btn = document.createElement("button");
      btn.className = "breadcrumb-btn";
      btn.textContent = part;
      btn.dataset.path = accumulated;
      if (accumulated === data.currentPath) {
        btn.classList.add("is-active");
      }
      btn.addEventListener("click", () => loadAndRenderBrowser(accumulated));
      browseBreadcrumb.appendChild(btn);
    }
  }

  // Render folders
  browseFoldersList.innerHTML = "";
  if (data.folders && data.folders.length > 0) {
    for (const folder of data.folders) {
      const btn = document.createElement("button");
      btn.className = "browse-item";
      btn.textContent = folder;
      btn.addEventListener("click", () => {
        const newPath = fileBrowserState.currentPath
          ? `${fileBrowserState.currentPath}/${folder}`
          : folder;
        loadAndRenderBrowser(newPath);
      });
      browseFoldersList.appendChild(btn);
    }
  } else {
    const empty = document.createElement("div");
    empty.style.padding = "8px";
    empty.style.color = "#999";
    empty.style.fontSize = "11px";
    empty.textContent = "No folders";
    browseFoldersList.appendChild(empty);
  }

  // Render files
  browseFilesList.innerHTML = "";
  if (data.files && data.files.length > 0) {
    for (const file of data.files) {
      const btn = document.createElement("button");
      btn.className = "browse-item";
      btn.textContent = file;
      btn.addEventListener("click", () => {
        fileBrowserState.selectedPath = fileBrowserState.currentPath
          ? `${fileBrowserState.currentPath}/${file}`
          : file;
        browsePathInput.value = fileBrowserState.selectedPath;
        document.querySelectorAll(".browse-item.is-selected").forEach((el) => {
          el.classList.remove("is-selected");
        });
        btn.classList.add("is-selected");
      });
      browseFilesList.appendChild(btn);
    }
  } else {
    const empty = document.createElement("div");
    empty.style.padding = "8px";
    empty.style.color = "#999";
    empty.style.fontSize = "11px";
    empty.textContent = "No image files";
    browseFilesList.appendChild(empty);
  }
}

async function loadAndRenderBrowser(path) {
  const data = await loadBrowseDirectory(path);
  if (data) {
    renderBrowseContent(data);
  }
}

let fileDialogPromise = null;
let fileDialogResolve = null;

function openFileBrowser(mode, inputElement) {
  fileBrowserState.mode = mode;
  fileBrowserState.inputElement = inputElement;
  fileBrowserState.currentPath = "";
  fileBrowserState.selectedPath = "";
  browseModal.classList.add("is-open");
  loadAndRenderBrowser("").catch((err) => console.error(err));
}

function openFileDialog() {
  return new Promise((resolve, reject) => {
    fileDialogPromise = { resolve, reject };
    fileBrowserState.mode = "file-open";
    fileBrowserState.inputElement = null;
    fileBrowserState.currentPath = "";
    fileBrowserState.selectedPath = "";
    browseModal.classList.add("is-open");
    loadAndRenderBrowser("").catch((err) => {
      fileDialogPromise = null;
      reject(err);
    });
  });
}

function closeFileBrowser() {
  browseModal.classList.remove("is-open");
}

browseSelectBtn?.addEventListener("click", () => {
  const selected = fileBrowserState.selectedPath;

  // Handle file open dialog mode
  if (fileBrowserState.mode === "file-open") {
    closeFileBrowser();
    if (fileDialogPromise) {
      if (selected) {
        fileDialogPromise.resolve(selected);
      } else {
        fileDialogPromise.reject(new Error("No file selected"));
      }
      fileDialogPromise = null;
    }
    return;
  }

  // Handle folder/path selection modes
  if (!fileBrowserState.inputElement) {
    closeFileBrowser();
    return;
  }

  if (!selected) {
    setStatus("No file selected");
    return;
  }

  if (fileBrowserState.mode === "autoload") {
    autoloadDir.value = selected;
    state.autoload.dir = selected;
    persistAutoloadSettings();
    if (state.autoload.mode === "file") {
      loadFiles().catch((err) => console.error(err));
    }
    if (state.autoload.running && state.autoload.mode === "watch") {
      autoloadTick();
    }
  } else if (fileBrowserState.mode === "series-sum") {
    const picked = selected.replace(/[\\/]$/, "");
    seriesSumOutput.value = `${picked}/series_sum`;
  }

  closeFileBrowser();
});

browseCancelBtn?.addEventListener("click", closeFileBrowser);
browseCloseBtn?.addEventListener("click", closeFileBrowser);

browseModal?.addEventListener("click", (event) => {
  if (event.target === browseModal) {
    closeFileBrowser();
  }
});

// Handle filesystem mode selection
filesystemMode?.addEventListener("change", () => {
  fileBrowserState.filesystemMode = filesystemMode.value;
});

async function handleLocalFileSelection(mode, inputElement) {
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

autoloadBrowse?.addEventListener("click", async () => {
  // If backend is local, always use native dialog
  if (backendIsLocal) {
    try {
      const res = await fetch(`${API}/choose-folder`);
      if (res.status === 204) {
        return;
      }
      if (!res.ok) {
        setAutoloadStatus("Folder picker unavailable");
        return;
      }
      const data = await res.json();
      if (data?.path && autoloadDir) {
        autoloadDir.value = data.path;
        state.autoload.dir = data.path;
        persistAutoloadSettings();
        if (state.autoload.mode === "file") {
          loadFiles().catch((err) => console.error(err));
        }
        if (state.autoload.running && state.autoload.mode === "watch") {
          autoloadTick();
        }
      }
    } catch (err) {
      console.error(err);
      setAutoloadStatus("Folder picker failed");
    }
  } else if (filesystemMode?.value === "local") {
    // Use HTML5 file input for local filesystem on remote backend
    handleLocalFileSelection("autoload", autoloadDir);
  } else {
    // Use web browser for remote filesystem
    openFileBrowser("autoload", autoloadDir);
  }
});

simplonUrl?.addEventListener("change", () => {
  state.autoload.simplonUrl = simplonUrl.value.trim();
  persistAutoloadSettings();
});

simplonVersion?.addEventListener("change", () => {
  state.autoload.simplonVersion = simplonVersion.value.trim() || "1.8.0";
  persistAutoloadSettings();
});

simplonTimeout?.addEventListener("change", () => {
  state.autoload.simplonTimeout = Math.max(100, Number(simplonTimeout.value || 500));
  persistAutoloadSettings();
});

simplonEnable?.addEventListener("change", async () => {
  state.autoload.simplonEnable = simplonEnable.checked;
  persistAutoloadSettings();
  if (state.autoload.running && state.autoload.mode === "simplon") {
    await setSimplonMode(state.autoload.simplonEnable);
  }
});

colormapSelect?.addEventListener("change", () => {
  const value = colormapSelect.value;
  if (value) {
    state.colormap = value;
    redraw();
    scheduleHistogram();
  }
});

autoScaleToggle.addEventListener("change", () => {
  state.autoScale = autoScaleToggle.checked;
  if (state.autoScale && state.dataRaw && state.stats) {
    const levels = computeAutoLevels(state.dataRaw, state.stats.satMax ?? null);
    state.min = levels.min;
    state.max = levels.max;
    minInput.value = formatValue(state.min);
    maxInput.value = formatValue(state.max);
  }
  redraw();
  scheduleHistogram();
});

roiModeSelect?.addEventListener("change", () => {
  roiState.mode = roiModeSelect.value;
  updateRoiModeUI();
  if (roiState.mode === "none") {
    clearRoi();
  } else {
    if (roiState.mode === "circle") {
      roiState.innerRadius = 0;
    }
    roiState.active = Boolean(roiState.start && roiState.end);
    scheduleRoiOverlay();
    scheduleRoiUpdate();
  }
});

roiLogToggle?.addEventListener("change", () => {
  roiState.log = roiLogToggle.checked;
  scheduleRoiUpdate();
});

roiLimitsEnable?.addEventListener("change", updateRoiPlotLimitsEnabled);
roiExportCsvBtn?.addEventListener("click", exportRoiCsv);

roiRadiusInput?.addEventListener("change", () => {
  if (roiState.mode !== "circle") return;
  if (!roiState.start) {
    const center = applyRoiCenterFromInputs();
    if (center) {
      roiState.start = center;
      roiState.end = center;
    }
  }
  if (!roiState.start) return;
  const radius = Math.max(0, Math.round(Number(roiRadiusInput.value || 0)));
  roiState.outerRadius = radius;
  roiState.end = { x: Math.min(state.width - 1, roiState.start.x + radius), y: roiState.start.y };
  roiState.active = true;
  updateRoiCenterInputs();
  scheduleRoiOverlay();
  scheduleRoiUpdate();
});

roiInnerInput?.addEventListener("change", () => {
  if (roiState.mode !== "annulus") return;
  if (!roiState.start) {
    const center = applyRoiCenterFromInputs();
    if (center) {
      roiState.start = center;
      roiState.end = center;
    }
  }
  if (!roiState.start) return;
  const inner = Math.max(0, Math.round(Number(roiInnerInput.value || 0)));
  roiState.innerRadius = inner;
  roiState.active = true;
  updateRoiCenterInputs();
  scheduleRoiOverlay();
  scheduleRoiUpdate();
});

roiOuterInput?.addEventListener("change", () => {
  if (roiState.mode !== "annulus") return;
  if (!roiState.start) {
    const center = applyRoiCenterFromInputs();
    if (center) {
      roiState.start = center;
      roiState.end = center;
    }
  }
  if (!roiState.start) return;
  const outer = Math.max(0, Math.round(Number(roiOuterInput.value || 0)));
  roiState.outerRadius = outer;
  roiState.end = { x: Math.min(state.width - 1, roiState.start.x + outer), y: roiState.start.y };
  roiState.active = true;
  updateRoiCenterInputs();
  scheduleRoiOverlay();
  scheduleRoiUpdate();
});

[roiCenterXInput, roiCenterYInput].forEach((input) => {
  input?.addEventListener("change", () => {
    if (roiState.mode !== "circle" && roiState.mode !== "annulus") return;
    const center = applyRoiCenterFromInputs();
    if (!center) return;
    roiState.start = center;
    const outer =
      roiState.mode === "circle"
        ? Math.max(0, Math.round(Number(roiRadiusInput?.value || roiState.outerRadius || 0)))
        : Math.max(0, Math.round(Number(roiOuterInput?.value || roiState.outerRadius || 0)));
    roiState.outerRadius = outer;
    roiState.end = { x: Math.min(state.width - 1, center.x + outer), y: center.y };
    roiState.active = true;
    updateRoiCenterInputs();
    scheduleRoiOverlay();
    scheduleRoiUpdate();
  });
});

maskToggle?.addEventListener("change", () => {
  state.maskEnabled = maskToggle.checked;
  state.maskAuto = false;
  updateGlobalStats();
  redraw();
  scheduleRoiUpdate();
  schedulePeakFinder();
});

autoContrastBtn.addEventListener("click", () => {
  if (!state.stats || !state.dataRaw) return;
  state.autoScale = true;
  autoScaleToggle.checked = true;
  const levels = computeAutoLevels(state.dataRaw, state.stats.satMax ?? null);
  state.min = levels.min;
  state.max = levels.max;
  minInput.value = formatValue(state.min);
  maxInput.value = formatValue(state.max);
  redraw();
  scheduleHistogram();
});

invertToggle.addEventListener("change", () => {
  state.invert = invertToggle.checked;
  redraw();
  scheduleHistogram();
});

[histLogX, histLogY].forEach((toggle) => {
  if (!toggle) return;
  toggle.addEventListener("change", () => {
    state.histLogX = histLogX?.checked ?? state.histLogX;
    state.histLogY = histLogY?.checked ?? state.histLogY;
    if (state.dataRaw && state.stats) {
      const bins = state.stats.bins || chooseHistogramBins(state.dataRaw.length);
      state.histogram = computeHistogram(
        state.dataRaw,
        state.stats.min,
        state.stats.max,
        state.stats.satMax ?? null,
        bins,
        state.histLogX
      );
    }
    scheduleHistogram();
  });
});

[minInput, maxInput].forEach((input) => {
  input.addEventListener("change", () => {
    if (!state.dataRaw) return;
    const statsMin = Number.isFinite(state.stats?.min) ? state.stats.min : Math.min(state.min, state.max);
    const statsMax = Number.isFinite(state.stats?.max) ? state.stats.max : Math.max(state.min, state.max);
    let nextMin = snapHistogramValue(Number(minInput.value || state.min));
    let nextMax = snapHistogramValue(Number(maxInput.value || state.max));
    nextMin = Math.max(statsMin, Math.min(statsMax, nextMin));
    nextMax = Math.max(statsMin, Math.min(statsMax, nextMax));
    if (input === minInput && nextMin > nextMax) {
      nextMin = nextMax;
    } else if (input === maxInput && nextMax < nextMin) {
      nextMax = nextMin;
    } else if (nextMin > nextMax) {
      nextMax = nextMin;
    }
    state.min = nextMin;
    state.max = nextMax;
    minInput.value = formatValue(state.min);
    maxInput.value = formatValue(state.max);
    state.autoScale = false;
    autoScaleToggle.checked = false;
    redraw();
    scheduleHistogram();
  });
});

zoomRange.addEventListener("input", () => {
  if (!canvasWrap) {
    setZoom(zoomRange.value);
    scheduleOverview();
    return;
  }
  const rect = canvasWrap.getBoundingClientRect();
  zoomAt(rect.left + rect.width / 2, rect.top + rect.height / 2, Number(zoomRange.value));
});

resetView.addEventListener("click", () => {
  fitImageToView();
});

prevBtn?.addEventListener("click", () => {
  stopPlayback();
  requestFrame(state.frameIndex - Math.max(1, state.step));
});

nextBtn?.addEventListener("click", () => {
  stopPlayback();
  requestFrame(state.frameIndex + Math.max(1, state.step));
});

playBtn?.addEventListener("click", () => {
  if (state.playing) {
    stopPlayback();
  } else {
    startPlayback();
  }
});

panelEdgeToggle?.addEventListener("click", () => {
  togglePanel();
});
panelFab?.addEventListener("click", () => {
  togglePanel();
});

panelTabs.forEach((tab) => {
  tab.addEventListener("click", () => {
    const target = tab.dataset.panelTab;
    if (!target) return;
    setPanelTab(target);
  });
});

sectionToggles.forEach((btn) => {
  btn.addEventListener("click", toggleSection);
});

try {
  const storedSections = localStorage.getItem("albis.sectionStates");
  if (storedSections) {
    sectionStateStore = JSON.parse(storedSections) || {};
  }
} catch {
  sectionStateStore = {};
}

sectionToggles.forEach((btn) => {
  const section = btn.closest(".panel-section");
  const id = section?.dataset.section;
  if (id && sectionStateStore[id]) {
    setSectionState(section, true, false);
  }
});
try {
  const storedPanelTab = localStorage.getItem("albis.panelTab");
  if (storedPanelTab) {
    const normalized = storedPanelTab === "tools" ? "view" : storedPanelTab;
    setPanelTab(normalized, false);
  } else {
    setPanelTab("view", false);
  }
} catch {
  setPanelTab("view", false);
}

panelResizer?.addEventListener("mousedown", (event) => {
  if (!appLayout) return;
  if (state.panelCollapsed) {
    state.panelCollapsed = false;
    applyPanelState();
  }
  const startX = event.clientX;
  const startWidth = toolsPanel?.getBoundingClientRect().width || state.panelWidth;
  function onMove(e) {
    const delta = startX - e.clientX;
    setPanelWidth(startWidth + delta);
    scheduleHistogram();
  }
  function onUp(e) {
    const delta = startX - e.clientX;
    const finalWidth = startWidth + delta;
    if (finalWidth < 140) {
      state.panelCollapsed = true;
    }
    applyPanelState();
    window.removeEventListener("mousemove", onMove);
    window.removeEventListener("mouseup", onUp);
    document.body.style.cursor = "";
  }
  document.body.style.cursor = "col-resize";
  window.addEventListener("mousemove", onMove);
  window.addEventListener("mouseup", onUp);
});

canvasWrap.addEventListener(
  "wheel",
  (event) => {
    event.preventDefault();
    const delta = normalizeWheelDelta(event);
    if (!delta) return;
    const zoomBase = zoomWheelTarget ?? state.zoom ?? 1;
    const factor = Math.exp(-delta * 0.002);
    const minZoom = getMinZoom();
    zoomWheelTarget = Math.max(minZoom, Math.min(MAX_ZOOM, zoomBase * factor));
    zoomWheelPivot = { x: event.clientX, y: event.clientY };
    if (!zoomWheelRaf) {
      zoomWheelRaf = window.requestAnimationFrame(stepWheelZoom);
    }
  },
  { passive: false }
);

canvasWrap.addEventListener("scroll", () => {
  scheduleOverview();
  schedulePixelOverlay();
  scheduleRoiOverlay();
  scheduleResolutionOverlay();
  schedulePeakOverlay();
});

canvasWrap.addEventListener("contextmenu", (event) => {
  event.preventDefault();
});

canvasWrap.addEventListener(
  "touchstart",
  (event) => {
    if (event.touches.length >= 2) {
      // Multi-touch: pinch/zoom gesture
      stopTouchDrag();
      startTouchGesture(event.touches);
      event.preventDefault();
    } else if (event.touches.length === 1) {
      // Single touch: prepare for dragging
      const touch = event.touches[0];
      touchDragStart = {
        x: touch.clientX,
        y: touch.clientY,
        scrollLeft: canvasWrap.scrollLeft,
        scrollTop: canvasWrap.scrollTop,
      };
      touchDragActive = true;
    }
  },
  { passive: false }
);

canvasWrap.addEventListener(
  "touchmove",
  (event) => {
    if (event.touches.length >= 2) {
      // Multi-touch: continue zoom gesture
      if (!touchGestureActive) return;
      updateTouchGesture(event.touches);
      event.preventDefault();
      return;
    }
    
    if (event.touches.length === 1 && touchDragActive && touchDragStart) {
      // Single touch drag: pan the canvas
      const touch = event.touches[0];
      const dx = touch.clientX - touchDragStart.x;
      const dy = touch.clientY - touchDragStart.y;
      canvasWrap.scrollLeft = touchDragStart.scrollLeft - dx;
      canvasWrap.scrollTop = touchDragStart.scrollTop - dy;
      event.preventDefault();
    }
  },
  { passive: false }
);

canvasWrap.addEventListener("touchend", (event) => {
  if (event.touches.length >= 2) {
    startTouchGesture(event.touches);
    return;
  }
  stopTouchGesture();
  stopTouchDrag();
});

canvasWrap.addEventListener("touchcancel", () => {
  stopTouchGesture();
  stopTouchDrag();
});

function stopTouchDrag() {
  touchDragActive = false;
  touchDragStart = null;
}

canvasWrap.addEventListener("pointerdown", (event) => {
  if (event.pointerType === "touch") return;
  const isRightClick = event.button === 2 || event.buttons === 2 || event.which === 3;
  const isCtrlClick = event.button === 0 && event.ctrlKey;
  const roiTrigger = roiState.mode !== "none" && (isRightClick || isCtrlClick);
  if (roiTrigger) {
    const point = getImagePointFromEvent(event);
    if (!point) return;
    roiDragging = true;
    roiDragPointer = event.pointerId;
    roiState.active = true;
    roiState.start = point;
    roiState.end = point;
    if (roiState.mode === "circle" || roiState.mode === "annulus") {
      updateRoiCenterInputs();
      roiState.outerRadius = 0;
      if (roiState.mode === "circle") {
        roiState.innerRadius = 0;
        if (roiRadiusInput) roiRadiusInput.value = "0";
      } else {
        if (!roiState.innerRadius) {
          roiState.innerRadius = 0;
        }
        if (roiInnerInput) roiInnerInput.value = String(roiState.innerRadius || 0);
        if (roiOuterInput) roiOuterInput.value = "0";
      }
    }
    canvasWrap.classList.add("is-roi");
    canvasWrap.setPointerCapture(event.pointerId);
    event.preventDefault();
    scheduleRoiOverlay();
    scheduleRoiUpdate();
    return;
  }
  if (event.button !== 0) return;
  if (event.target.closest(".loading")) return;
  if (roiState.active && roiState.mode !== "none") {
    const point = getImagePointFromEvent(event);
    if (point) {
      const handle = getRoiHandleAt(event);
      if (handle || isPointInRoi(point)) {
        startRoiEdit(handle || "move", point);
        canvasWrap.setPointerCapture(event.pointerId);
        event.preventDefault();
        return;
      }
    }
  }
  panning = true;
  panStart = {
    x: event.clientX,
    y: event.clientY,
    scrollLeft: canvasWrap.scrollLeft,
    scrollTop: canvasWrap.scrollTop,
  };
  canvasWrap.classList.add("is-panning");
  canvasWrap.setPointerCapture(event.pointerId);
  event.preventDefault();
});

canvasWrap.addEventListener("pointermove", (event) => {
  if (event.pointerType === "touch") return;
  updateCursorOverlay(event);
  if (roiEditing) {
    const point = getImagePointFromEvent(event);
    if (!point) return;
    applyRoiEdit(point);
    return;
  }
  if (roiDragging) {
    const point = getImagePointFromEvent(event);
    if (!point) return;
    updateRoiDrag(point);
    return;
  }
  if (!panning) return;
  const dx = event.clientX - panStart.x;
  const dy = event.clientY - panStart.y;
  canvasWrap.scrollLeft = panStart.scrollLeft - dx;
  canvasWrap.scrollTop = panStart.scrollTop - dy;
  scheduleOverview();
});

function stopPan(event) {
  if (!panning) return;
  panning = false;
  canvasWrap.classList.remove("is-panning");
  schedulePixelOverlay();
  if (event && canvasWrap.hasPointerCapture(event.pointerId)) {
    canvasWrap.releasePointerCapture(event.pointerId);
  }
}

function stopRoi(event) {
  if (!roiDragging) return;
  roiDragging = false;
  roiDragPointer = null;
  canvasWrap.classList.remove("is-roi");
  if (event && canvasWrap.hasPointerCapture(event.pointerId)) {
    canvasWrap.releasePointerCapture(event.pointerId);
  }
  scheduleRoiOverlay();
  scheduleRoiUpdate();
}

function updateRoiDrag(point) {
  roiState.end = point;
  if (roiState.mode === "circle" || roiState.mode === "annulus") {
    const dx = roiState.end.x - roiState.start.x;
    const dy = roiState.end.y - roiState.start.y;
    const outer = Math.max(0, Math.round(Math.hypot(dx, dy)));
    roiState.outerRadius = outer;
    if (roiState.mode === "circle") {
      if (roiRadiusInput) roiRadiusInput.value = String(outer);
    } else {
      if (roiOuterInput) roiOuterInput.value = String(outer);
      if (!roiState.innerRadius || roiState.innerRadius >= outer) {
        roiState.innerRadius = Math.max(0, Math.round(outer * 0.5));
        if (roiInnerInput) roiInnerInput.value = String(roiState.innerRadius);
      }
    }
  }
  scheduleRoiOverlay();
  scheduleRoiUpdate();
}

canvasWrap.addEventListener("pointerup", (event) => {
  stopRoiEdit(event);
  stopRoi(event);
  stopPan(event);
});

canvasWrap.addEventListener("pointercancel", (event) => {
  stopRoiEdit(event);
  stopRoi(event);
  stopPan(event);
});

window.addEventListener("mousemove", (event) => {
  if (roiEditing) {
    const point = getImagePointFromEvent(event);
    if (!point) return;
    applyRoiEdit(point);
    return;
  }
  if (!roiDragging) return;
  const point = getImagePointFromEvent(event);
  if (!point) return;
  updateRoiDrag(point);
});

window.addEventListener("mouseup", (event) => {
  if (roiEditing) {
    stopRoiEdit(event);
    return;
  }
  if (!roiDragging) return;
  stopRoi(event);
});

canvasWrap.addEventListener("pointerleave", () => {
  stopRoi();
  hideCursorOverlay();
});

canvasWrap.addEventListener("dblclick", (event) => {
  event.preventDefault();
  const minZoom = getMinZoom();
  const next = Math.min(MAX_ZOOM, Math.max(minZoom, state.zoom * 2));
  zoomAt(event.clientX, event.clientY, next);
});

[roiLineCanvas, roiXCanvas, roiYCanvas].forEach((canvasEl) => {
  if (!canvasEl) return;
  canvasEl.addEventListener("mousemove", (event) => updateRoiTooltip(event, canvasEl));
  canvasEl.addEventListener("mouseleave", () => hideRoiTooltip(canvasEl));
  canvasEl.addEventListener(
    "wheel",
    (event) => {
      event.preventDefault();
      const plot = canvasEl._roiPlot;
      if (!plot) return;
      const rect = canvasEl.getBoundingClientRect();
      const x = event.clientX - rect.left;
      const y = event.clientY - rect.top;
      const inYAxis = x <= plot.padL;
      const inXAxis = y >= plot.height - plot.padB;
      if (!inYAxis && !inXAxis) return;
      const delta = normalizeWheelDelta(event);
      if (!delta) return;
      const factor = Math.exp(-delta * 0.002);
      if (inYAxis) {
        const yRange = plot.yMax - plot.yMin;
        const cursorFrac = (plot.height - plot.padB - y) / (plot.height - plot.padB - plot.padT);
        const cursorValue = plot.yMin + cursorFrac * yRange;
        const newRange = yRange / factor;
        const newMin = cursorValue - cursorFrac * newRange;
        const newMax = cursorValue + (1 - cursorFrac) * newRange;
        if (roiState.plotLimits.autoscale) {
          roiState.plotLimits.autoscale = false;
          if (roiLimitsEnable) roiLimitsEnable.checked = false;
        }
        const plotKey = getRoiPlotKey(canvasEl);
        setRoiPlotAxisLimits(plotKey, "y", newMin, newMax);
      } else {
        const xRange = plot.xMax - plot.xMin;
        const cursorFrac = (x - plot.padL) / (plot.width - plot.padL - plot.padR);
        const cursorValue = plot.xMin + cursorFrac * xRange;
        const newRange = xRange / factor;
        const newMin = cursorValue - cursorFrac * newRange;
        const newMax = cursorValue + (1 - cursorFrac) * newRange;
        if (roiState.plotLimits.autoscale) {
          roiState.plotLimits.autoscale = false;
          if (roiLimitsEnable) roiLimitsEnable.checked = false;
        }
        const plotKey = getRoiPlotKey(canvasEl);
        setRoiPlotAxisLimits(plotKey, "x", newMin, newMax);
      }
      syncRoiPlotLimitControls();
      scheduleRoiUpdate();
    },
    { passive: false }
  );
});

// ROI plot resize handles
[roiLinePlot, roiBoxPlotX, roiBoxPlotY].forEach((plotContainer) => {
  if (!plotContainer) return;
  const resizeHandle = plotContainer.querySelector(".roi-resize-handle");
  if (!resizeHandle) return;
  
  resizeHandle.addEventListener("pointerdown", (event) => {
    if (event.button !== 0) return; // Only left mouse button
    event.preventDefault();
    roiPlotResizing = plotContainer;
    const rect = plotContainer.getBoundingClientRect();
    roiPlotResizeStart = {
      x: event.clientX,
      y: event.clientY,
      height: rect.height,
      container: plotContainer,
    };
    document.addEventListener("pointermove", onRoiPlotResizeMove);
    document.addEventListener("pointerup", onRoiPlotResizeEnd);
  });
});

function onRoiPlotResizeMove(event) {
  if (!roiPlotResizing) return;
  const dy = event.clientY - roiPlotResizeStart.y;
  const newHeight = Math.max(80, roiPlotResizeStart.height + dy);
  roiPlotResizing.style.height = `${newHeight}px`;
}

function onRoiPlotResizeEnd() {
  if (roiPlotResizing) {
    const canvas = roiPlotResizing.querySelector("canvas");
    if (canvas) {
      // Trigger a redraw due to canvas size change
      scheduleRoiUpdate();
    }
  }
  roiPlotResizing = null;
  document.removeEventListener("pointermove", onRoiPlotResizeMove);
  document.removeEventListener("pointerup", onRoiPlotResizeEnd);
}

overviewCanvas?.addEventListener("pointerdown", (event) => {
  if (!state.hasFrame) return;
  const point = overviewEventToImage(event);
  const overviewPoint = overviewEventToOverview(event);
  const view = getViewRect();
  if (!point || !overviewPoint || !view) return;

  const handle = getOverviewHandleAt(overviewPoint);
  if (handle) {
    overviewDragMode = "resize";
    overviewHandle = handle;
    overviewResizeCenter = event.altKey;
    overviewAnchor = getAnchorForHandle(view, handle, overviewResizeCenter);
    overviewDragOffset = { x: 0, y: 0 };
  } else {
    overviewDragMode = "move";
    overviewResizeCenter = false;
    const inView =
      point.x >= view.viewX &&
      point.x <= view.viewX + view.viewW &&
      point.y >= view.viewY &&
      point.y <= view.viewY + view.viewH;
    if (inView) {
      const centerX = view.viewX + view.viewW / 2;
      const centerY = view.viewY + view.viewH / 2;
      overviewDragOffset = { x: point.x - centerX, y: point.y - centerY };
    } else {
      overviewDragOffset = { x: 0, y: 0 };
    }
  }

  overviewDragging = true;
  overviewCanvas.style.cursor = "";
  overviewCanvas.classList.add("is-dragging");
  overviewCanvas.setPointerCapture(event.pointerId);
  if (overviewDragMode === "resize") {
    resizeViewFromHandle(point, overviewHandle, overviewResizeCenter);
  } else {
    panToImageCenter(point.x - overviewDragOffset.x, point.y - overviewDragOffset.y);
  }
});

overviewCanvas?.addEventListener("pointermove", (event) => {
  if (!state.hasFrame) return;
  if (!overviewDragging) {
    const overviewPoint = overviewEventToOverview(event);
    if (!overviewPoint) return;
    const handle = getOverviewHandleAt(overviewPoint);
    if (handle) {
      overviewCanvas.style.cursor =
        handle === "nw" || handle === "se" ? "nwse-resize" : "nesw-resize";
    } else {
      overviewCanvas.style.cursor = "";
    }
    return;
  }
  const point = overviewEventToImage(event);
  if (!point) return;
  if (overviewDragMode === "resize") {
    resizeViewFromHandle(point, overviewHandle, overviewResizeCenter);
  } else {
    panToImageCenter(point.x - overviewDragOffset.x, point.y - overviewDragOffset.y);
  }
});

function stopOverviewDrag(event) {
  if (!overviewDragging) return;
  overviewDragging = false;
  overviewDragMode = null;
  overviewHandle = null;
  overviewAnchor = null;
  overviewResizeCenter = false;
  overviewCanvas?.classList.remove("is-dragging");
  if (overviewCanvas) {
    overviewCanvas.style.cursor = "";
  }
  if (event && overviewCanvas && overviewCanvas.hasPointerCapture(event.pointerId)) {
    overviewCanvas.releasePointerCapture(event.pointerId);
  }
}

overviewCanvas?.addEventListener("pointerup", (event) => {
  stopOverviewDrag(event);
});

overviewCanvas?.addEventListener("pointercancel", (event) => {
  stopOverviewDrag(event);
});

overviewCanvas?.addEventListener("pointerleave", () => {
  if (!overviewDragging && overviewCanvas) {
    overviewCanvas.style.cursor = "";
  }
});

histCanvas.addEventListener("pointerdown", (event) => {
  if (!state.stats) return;
  const rect = histCanvas.getBoundingClientRect();
  const x = event.clientX - rect.left;
  const width = histCanvas.clientWidth;
  const minX = histogramValueToX(state.min, width);
  const maxX = histogramValueToX(state.max, width);
  const threshold = 6;
  const distMin = Math.abs(x - minX);
  const distMax = Math.abs(x - maxX);
  if (Math.min(distMin, distMax) > threshold) return;
  histDragTarget = distMin <= distMax ? "min" : "max";
  histDragging = true;
  histCanvas.setPointerCapture(event.pointerId);
  histCanvas.style.cursor = "ew-resize";
  hideHistTooltip();
  event.preventDefault();
});

histCanvas.addEventListener("pointermove", (event) => {
  if (!state.stats) return;
  const rect = histCanvas.getBoundingClientRect();
  const x = event.clientX - rect.left;
  const width = histCanvas.clientWidth;
  if (!histDragging) {
    const minX = histogramValueToX(state.min, width);
    const maxX = histogramValueToX(state.max, width);
    const threshold = 6;
    const distMin = Math.abs(x - minX);
    const distMax = Math.abs(x - maxX);
    if (Math.min(distMin, distMax) <= threshold) {
      histCanvas.style.cursor = "ew-resize";
    } else {
      histCanvas.style.cursor = "";
    }
    const value = snapHistogramValue(histogramXToValue(x, width));
    if (Number.isFinite(value)) {
      const { left, top } = getHistTooltipPosition(rect, x);
      showHistTooltip(`Value ${formatValue(value)}`, left, top);
    } else {
      hideHistTooltip();
    }
    return;
  }

  const value = snapHistogramValue(histogramXToValue(x, width));
  if (!Number.isFinite(value)) return;
  const minVal = state.stats.min;
  const maxVal = state.stats.max;
  if (histDragTarget === "min") {
    const clamped = Math.max(minVal, Math.min(value, state.max));
    state.min = clamped;
    minInput.value = formatValue(state.min);
  } else if (histDragTarget === "max") {
    const clamped = Math.min(maxVal, Math.max(value, state.min));
    state.max = clamped;
    maxInput.value = formatValue(state.max);
  }
  state.autoScale = false;
  autoScaleToggle.checked = false;
  redraw();
  scheduleHistogram();
  hideHistTooltip();
  event.preventDefault();
});

function stopHistDrag(event) {
  if (!histDragging) return;
  histDragging = false;
  histDragTarget = null;
  histCanvas.style.cursor = "";
  hideHistTooltip();
  if (event && histCanvas.hasPointerCapture(event.pointerId)) {
    histCanvas.releasePointerCapture(event.pointerId);
  }
}

histCanvas.addEventListener("pointerup", (event) => {
  stopHistDrag(event);
});

histCanvas.addEventListener("pointercancel", (event) => {
  stopHistDrag(event);
});

histCanvas.addEventListener("pointerleave", () => {
  if (!histDragging) {
    histCanvas.style.cursor = "";
    hideHistTooltip();
  }
});

exportBtn?.addEventListener("click", () => {
  exportFullImage();
});

window.addEventListener("resize", () => {
  if (state.hasFrame) {
    setZoom(state.zoom);
  }
  if (state.histogram) drawHistogram(state.histogram);
  drawSplash();
  applyPanelState();
  scheduleOverview();
  scheduleHistogram();
  schedulePixelOverlay();
  scheduleRoiOverlay();
  scheduleRoiUpdate();
  scheduleResolutionOverlay();
  schedulePeakOverlay();
});

initRenderer();

// Hide filesystem mode selector if backend is local
if (backendIsLocal && filesystemMode) {
  filesystemMode.parentElement?.classList.add("is-hidden");
}

showSplash();
drawSplash();
if (fpsRange) {
  setFps(Number(fpsRange.value));
}
if (frameStep) {
  const stepValue = Math.max(1, Math.round(Number(frameStep.value || 1)));
  state.step = stepValue;
  frameStep.value = String(stepValue);
}
if (histLogX) {
  state.histLogX = histLogX.checked;
}
if (histLogY) {
  state.histLogY = histLogY.checked;
}
if (colormapSelect) {
  colormapSelect.value = state.colormap;
}
if (roiModeSelect) {
  roiState.mode = roiModeSelect.value;
  roiState.active = false;
  updateRoiModeUI();
}
if (roiLogToggle) {
  roiState.log = roiLogToggle.checked;
}
updateRoiPlotLimitsEnabled();
updateRingsFromInputs();

function updateRingsFromInputs() {
  if (ringsToggle) {
    analysisState.ringsEnabled = ringsToggle.checked;
  }
  if (ringsCount) {
    const count = Number(ringsCount.value);
    const maxCount = Math.max(1, ringInputs.length);
    analysisState.ringCount = Number.isFinite(count)
      ? Math.max(1, Math.min(maxCount, Math.round(count)))
      : Math.min(3, maxCount);
    ringsCount.value = String(analysisState.ringCount);
  }
  if (ringsDistance) {
    const value = Number(ringsDistance.value);
    analysisState.distanceMm = Number.isFinite(value) && value > 0 ? value : null;
  }
  if (ringsPixel) {
    const value = Number(ringsPixel.value);
    analysisState.pixelSizeUm = Number.isFinite(value) && value > 0 ? value : null;
  }
  if (ringsEnergy) {
    const value = Number(ringsEnergy.value);
    analysisState.energyEv = Number.isFinite(value) && value > 0 ? value : null;
  }
  if (ringsCenterX) {
    const value = Number(ringsCenterX.value);
    analysisState.centerX = Number.isFinite(value) ? value : analysisState.centerX;
  }
  if (ringsCenterY) {
    const value = Number(ringsCenterY.value);
    analysisState.centerY = Number.isFinite(value) ? value : analysisState.centerY;
  }
  if (ringInputs.length) {
    analysisState.rings = ringInputs
      .map((input, idx) => {
        const value = Number(input.value || analysisState.rings[idx]);
        return Number.isFinite(value) && value > 0 ? value : null;
      })
      .filter((value) => value !== null);
  }
  ringInputs.forEach((input, idx) => {
    if (!input) return;
    const visible = idx < analysisState.ringCount;
    input.style.display = visible ? "" : "none";
  });
  scheduleResolutionOverlay();
}

[ringsToggle, ringsDistance, ringsPixel, ringsEnergy, ringsCenterX, ringsCenterY, ringsCount, ...ringInputs]
  .filter(Boolean)
  .forEach((input) => {
    const eventName = input.type === "checkbox" ? "change" : "input";
    input.addEventListener(eventName, updateRingsFromInputs);
  });

if (peaksCountInput) {
  const initial = Math.max(1, Math.min(500, Math.round(Number(peaksCountInput.value || 25))));
  analysisState.peakCount = initial;
  peaksCountInput.value = String(initial);
  peaksCountInput.addEventListener("change", () => {
    const next = Math.max(1, Math.min(500, Math.round(Number(peaksCountInput.value || analysisState.peakCount))));
    analysisState.peakCount = next;
    peaksCountInput.value = String(next);
    schedulePeakFinder();
  });
}

if (peaksEnableToggle) {
  analysisState.peaksEnabled = peaksEnableToggle.checked;
  peaksEnableToggle.addEventListener("change", () => {
    analysisState.peaksEnabled = peaksEnableToggle.checked;
    if (!analysisState.peaksEnabled) {
      analysisState.peakSelectionAnchor = null;
    }
    schedulePeakFinder();
  });
}

peaksExportBtn?.addEventListener("click", () => {
  exportPeakCsv();
});

if (seriesSumOutput && !seriesSumOutput.value.trim()) {
  syncSeriesSumOutputPath(true);
}

seriesSumMode?.addEventListener("change", () => {
  updateSeriesSumUi();
});

seriesSumOperation?.addEventListener("change", () => {
  updateSeriesSumUi();
});

seriesSumNormalizeEnable?.addEventListener("change", () => {
  updateSeriesSumUi();
});

seriesSumStep?.addEventListener("change", () => {
  const value = Math.max(1, Math.round(Number(seriesSumStep.value || 10)));
  seriesSumStep.value = String(value);
});

seriesSumRangeStart?.addEventListener("change", () => {
  const total = Math.max(1, Number(state.frameCount || 1));
  const value = Math.max(1, Math.min(total, Math.round(Number(seriesSumRangeStart.value || 1))));
  seriesSumRangeStart.value = String(value);
});

seriesSumRangeEnd?.addEventListener("change", () => {
  const total = Math.max(1, Number(state.frameCount || 1));
  const value = Math.max(1, Math.min(total, Math.round(Number(seriesSumRangeEnd.value || total))));
  seriesSumRangeEnd.value = String(value);
});

seriesSumNormalizeFrame?.addEventListener("change", () => {
  const total = Math.max(1, Number(state.frameCount || 1));
  const value = Math.max(1, Math.min(total, Math.round(Number(seriesSumNormalizeFrame.value || 1))));
  seriesSumNormalizeFrame.value = String(value);
});

seriesSumBrowse?.addEventListener("click", async () => {
  if (state.seriesSum.running) return;
  
  // If backend is local, always use native dialog
  if (backendIsLocal) {
    try {
      const res = await fetch(`${API}/choose-folder`);
      if (res.status === 204) return;
      if (!res.ok) {
        setStatus("Series output picker unavailable");
        return;
      }
      const data = await res.json();
      if (data?.path && seriesSumOutput) {
        const picked = String(data.path).replace(/[\\/]$/, "");
        seriesSumOutput.value = `${picked}/series_sum`;
      }
    } catch (err) {
      console.error(err);
      setStatus("Series output picker failed");
    }
  } else if (filesystemMode?.value === "local") {
    // Use HTML5 file input for local filesystem on remote backend
    handleLocalFileSelection("series-sum", seriesSumOutput);
  } else {
    // Use web browser for remote filesystem
    openFileBrowser("series-sum", seriesSumOutput);
  }
});

seriesSumProgress?.addEventListener("click", () => {
  openSeriesSumOutputTarget();
});

seriesSumStart?.addEventListener("click", () => {
  startSeriesSumming();
});

renderPeakList();
setSeriesSumProgress(0, "Idle");
updateSeriesSumUi();
if (pixelLabelToggle) {
  state.pixelLabels = pixelLabelToggle.checked;
  pixelLabelToggle.addEventListener("change", () => {
    state.pixelLabels = pixelLabelToggle.checked;
    schedulePixelOverlay();
  });
}
try {
  const storedWidth = Number(localStorage.getItem("albis.panelWidth"));
  const storedCollapsed = localStorage.getItem("albis.panelCollapsed");
  if (storedWidth) {
    state.panelWidth = Math.max(220, Math.min(900, storedWidth));
  }
  if (storedCollapsed !== null) {
    state.panelCollapsed = storedCollapsed === "true";
  } else if (window.innerWidth < 900) {
    state.panelCollapsed = true;
  }
} catch {
  // ignore storage errors
}
applyPanelState();
loadAutoloadSettings();
updatePlayButtons();
updateAboutVersion();
initHelpTooltips();
startBackendHeartbeat();

void bootstrapApp().catch((err) => {
  console.error(err);
  setSplashStatus("Initialization failed");
  setStatus("Failed to initialize");
  showSplash();
  setLoading(false);
});
