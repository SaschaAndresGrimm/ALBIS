const fileSelect = document.getElementById("file-select");
const datasetSelect = document.getElementById("dataset-select");
const fileField = document.getElementById("file-field");
const datasetField = document.getElementById("dataset-field");
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
const dataSection = document.getElementById("data-section");
const autoloadMode = document.getElementById("autoload-mode");
const autoloadDir = document.getElementById("autoload-dir");
const autoloadInterval = document.getElementById("autoload-interval");
const autoloadStatus = document.getElementById("autoload-status");
const autoloadLatest = document.getElementById("autoload-latest");
const autoloadFolder = document.getElementById("autoload-folder");
const autoloadWatch = document.getElementById("autoload-watch");
const autoloadSimplon = document.getElementById("autoload-simplon");
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
const roiParams = document.getElementById("roi-params");
const roiRadiusField = document.getElementById("roi-radius-field");
const roiRadiusInput = document.getElementById("roi-radius");
const roiCenterFields = document.getElementById("roi-center-fields");
const roiCenterXInput = document.getElementById("roi-center-x");
const roiCenterYInput = document.getElementById("roi-center-y");
const roiRingFields = document.getElementById("roi-ring-fields");
const roiInnerInput = document.getElementById("roi-inner-radius");
const roiOuterInput = document.getElementById("roi-outer-radius");
const roiStartEl = document.getElementById("roi-start");
const roiEndEl = document.getElementById("roi-end");
const roiSizeLabel = document.getElementById("roi-size-label");
const roiSizeEl = document.getElementById("roi-size");
const roiCountLabel = document.getElementById("roi-count-label");
const roiCountEl = document.getElementById("roi-count");
const roiMeanEl = document.getElementById("roi-mean");
const roiMinEl = document.getElementById("roi-min");
const roiMaxEl = document.getElementById("roi-max");
const roiSumEl = document.getElementById("roi-sum");
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
const menuButtons = document.querySelectorAll(".menu-item[data-menu]");
const dropdown = document.getElementById("menu-dropdown");
const dropdownPanels = document.querySelectorAll(".dropdown-panel");
const menuActions = document.querySelectorAll(".dropdown-item[data-action]");
const aboutModal = document.getElementById("about-modal");
const aboutClose = document.getElementById("about-close");
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
let roiDragging = false;
let roiDragPointer = null;
let panelTabState = "view";

const roiState = {
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
};

const state = {
  file: "",
  dataset: "",
  shape: [],
  dtype: "",
  frameCount: 1,
  frameIndex: 0,
  isLoading: false,
  pendingFrame: null,
  playing: false,
  playTimer: null,
  fps: 5,
  step: 1,
  panelWidth: 640,
  panelCollapsed: true,
  autoScale: true,
  min: 0,
  max: 1,
  colormap: "albulaHdr",
  invert: false,
  zoom: 1,
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
  const imgX = (canvasWrap.scrollLeft + x) / zoom;
  const imgY = (canvasWrap.scrollTop + y) / zoom;
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
  if (!state.file) {
    clearMaskState();
    return;
  }
  if (state.maskFile === state.file && state.maskRaw) {
    syncMaskAvailability(forceEnable);
    return;
  }
  state.maskFile = state.file;
  state.maskRaw = null;
  state.maskShape = null;
  state.maskAvailable = false;
  if (forceEnable) {
    state.maskEnabled = true;
  }
  updateMaskUI();
  try {
    const res = await fetch(`${API}/mask?file=${encodeURIComponent(state.file)}`);
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
  const imgX = (canvasWrap.scrollLeft + x) / zoom;
  const imgY = (canvasWrap.scrollTop + y) / zoom;
  const ix = Math.floor(imgX);
  const iy = Math.floor(imgY);
  if (ix < 0 || iy < 0 || ix >= state.width || iy >= state.height) {
    hideCursorOverlay();
    return;
  }
  const dataY = renderer?.type === "webgl" ? state.height - 1 - iy : iy;
  const idx = dataY * state.width + ix;
  const value = state.dataRaw[idx];
  const label = `X ${ix}  Y ${iy}  Value ${formatValue(value)}`;
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
    const dataY = renderer?.type === "webgl" ? state.height - 1 - y : y;
    const rowOffset = dataY * state.width;
    const screenY = (y - viewY) * zoom + zoom / 2;
    for (let x = startX; x < endX; x += 1) {
      const value = state.dataRaw[rowOffset + x];
      const screenX = (x - viewX) * zoom + zoom / 2;
      pixelCtx.fillText(formatValue(value), screenX, screenY);
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
  const viewX = canvasWrap.scrollLeft / zoom;
  const viewY = canvasWrap.scrollTop / zoom;
  const x0 = (roiState.start.x - viewX) * zoom;
  const y0 = (roiState.start.y - viewY) * zoom;
  const x1 = (roiState.end.x - viewX) * zoom;
  const y1 = (roiState.end.y - viewY) * zoom;

  roiCtx.save();
  roiCtx.lineWidth = 2;
  roiCtx.strokeStyle = "rgba(255, 255, 255, 0.95)";
  roiCtx.setLineDash([6, 4]);
  if (roiState.mode === "line") {
    roiCtx.beginPath();
    roiCtx.moveTo(x0, y0);
    roiCtx.lineTo(x1, y1);
    roiCtx.stroke();
  } else if (roiState.mode === "box") {
    const left = Math.min(x0, x1);
    const top = Math.min(y0, y1);
    const w = Math.abs(x1 - x0);
    const h = Math.abs(y1 - y0);
    roiCtx.strokeRect(left, top, w, h);
  } else if (roiState.mode === "circle" || roiState.mode === "annulus") {
    const radius = Math.hypot(x1 - x0, y1 - y0);
    roiCtx.beginPath();
    roiCtx.arc(x0, y0, radius, 0, Math.PI * 2);
    roiCtx.stroke();
    if (roiState.mode === "annulus" && roiState.innerRadius > 0) {
      roiCtx.beginPath();
      roiCtx.arc(x0, y0, roiState.innerRadius * zoom, 0, Math.PI * 2);
      roiCtx.stroke();
    }
  }
  roiCtx.restore();
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
    case "n":
      return { x: view.viewX + view.viewW / 2, y: view.viewY + view.viewH };
    case "s":
      return { x: view.viewX + view.viewW / 2, y: view.viewY };
    case "e":
      return { x: view.viewX, y: view.viewY + view.viewH / 2 };
    case "w":
      return { x: view.viewX + view.viewW, y: view.viewY + view.viewH / 2 };
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
    { name: "n", x: rectX + rectW / 2, y: rectY },
    { name: "e", x: rectX + rectW, y: rectY + rectH / 2 },
    { name: "s", x: rectX + rectW / 2, y: rectY + rectH },
    { name: "w", x: rectX, y: rectY + rectH / 2 },
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
  const clamped = Math.max(MIN_ZOOM, Math.min(MAX_ZOOM, Number(value)));
  state.zoom = clamped;
  canvas.style.transform = `scale(${clamped})`;
  if (zoomRange) {
    zoomRange.value = String(clamped);
  }
  if (zoomValue) {
    zoomValue.textContent = `${clamped.toFixed(1)}x`;
  }
  schedulePixelOverlay();
  scheduleRoiOverlay();
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
  const worldX = (canvasWrap.scrollLeft + x) / prevZoom;
  const worldY = (canvasWrap.scrollTop + y) / prevZoom;

  setZoom(nextZoom);

  const newScrollLeft = worldX * state.zoom - x;
  const newScrollTop = worldY * state.zoom - y;
  const maxScrollLeft = Math.max(0, canvasWrap.scrollWidth - canvasWrap.clientWidth);
  const maxScrollTop = Math.max(0, canvasWrap.scrollHeight - canvasWrap.clientHeight);
  canvasWrap.scrollLeft = Math.max(0, Math.min(maxScrollLeft, newScrollLeft));
  canvasWrap.scrollTop = Math.max(0, Math.min(maxScrollTop, newScrollTop));
  scheduleOverview();
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

function setFps(value) {
  const clamped = Math.max(1, Math.min(60, Math.round(value)));
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
  const disabled = !state.file || !state.dataset || state.frameCount <= 1;
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
  toolsPanel.classList.toggle("is-collapsed", state.panelCollapsed);
  const maxPanelWidth =
    window.innerWidth < 900 ? Math.max(220, Math.floor(window.innerWidth * 0.7)) : 900;
  const targetWidth = Math.max(220, Math.min(maxPanelWidth, state.panelWidth));
  const width = state.panelCollapsed ? 28 : targetWidth;
  appLayout.style.setProperty("--panel-width", `${width}px`);
  document.documentElement.style.setProperty("--panel-width", `${width}px`);
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
  if (!state.frameCount || !state.dataset || !state.file) return;
  const clamped = Math.max(0, Math.min(state.frameCount - 1, index));
  state.frameIndex = clamped;
  frameRange.value = String(clamped);
  frameIndex.value = String(clamped);
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

function hideSplash() {
  splash?.classList.add("is-hidden");
}

function updateToolbar() {
  if (!toolbarPath) return;
  if (!state.file) {
    toolbarPath.textContent = "No file loaded";
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

function openMenu(menu, anchor) {
  if (!dropdown) return;
  dropdown.classList.add("is-open");
  dropdown.setAttribute("aria-hidden", "false");
  setActiveMenu(menu, anchor);
}

function closeMenu() {
  if (!dropdown) return;
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

function openFileModal() {
  closeMenu();
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
}

function setDataControlsForHdf5() {
  if (datasetSelect) datasetSelect.disabled = false;
  if (frameRange) frameRange.disabled = false;
  if (frameIndex) frameIndex.disabled = false;
  if (frameStep) frameStep.disabled = false;
  if (fpsRange) fpsRange.disabled = false;
}

function setDataControlsForImage() {
  if (datasetSelect) datasetSelect.disabled = true;
  if (frameRange) frameRange.disabled = true;
  if (frameIndex) frameIndex.disabled = true;
  if (frameStep) frameStep.disabled = true;
  if (fpsRange) fpsRange.disabled = true;
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
  if (autoloadSimplon) autoloadSimplon.classList.toggle("is-hidden", state.autoload.mode !== "simplon");
  if (fileField) fileField.classList.toggle("is-hidden", state.autoload.mode === "simplon");
  if (datasetField) datasetField.classList.toggle("is-hidden", state.autoload.mode === "simplon");
  if (frameRange) frameRange.closest(".field")?.classList.toggle("is-hidden", state.autoload.mode !== "file");
  if (frameStep) frameStep.closest(".field")?.classList.toggle("is-hidden", state.autoload.mode !== "file");
  if (fpsRange) fpsRange.closest(".field")?.classList.toggle("is-hidden", state.autoload.mode !== "file");
  if (autoloadStatus) {
    const meta = autoloadStatus.closest(".autoload-meta");
    if (meta) meta.classList.toggle("is-hidden", state.autoload.mode === "file");
  }
  updateAutoloadMeta();
  updateLiveBadge();
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
    await fetchSimplonMask();
    await setSimplonMode(true);
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
  if (state.autoload.types.cbf) exts.push("cbf");
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
  let label = "SIMPLON monitor";
  try {
    label = `SIMPLON monitor (${new URL(baseUrl).host})`;
  } catch {
    if (baseUrl) {
      label = `SIMPLON monitor (${baseUrl})`;
    }
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
  applyExternalFrame(data, shape, dtype, file, true);
  setLoading(false);
}

function applyExternalFrame(data, shape, dtype, label, fitView, preserveMask = false) {
  if (!Array.isArray(shape) || shape.length < 2) return;
  stopPlayback();
  if (fitView) {
    state.hasFrame = false;
  }
  state.file = label;
  state.dataset = "";
  state.frameCount = 1;
  state.frameIndex = 0;
  frameRange.max = "0";
  frameRange.value = "0";
  frameIndex.value = "0";
  datasetSelect.innerHTML = "";
  datasetSelect.appendChild(option("Single image", ""));
  datasetSelect.value = "";
  setDataControlsForImage();
  if (!preserveMask) {
    clearMaskState();
  }

  const height = shape[0];
  const width = shape[1];
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
  const range = state.max - state.min || 1;
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
    const dataY = renderer?.type === "webgl" ? state.height - 1 - imgY : imgY;
    const rowOffset = dataY * state.width;
    const outOffset = row * width * 4;
    for (let col = 0; col < width; col += 1) {
      const imgX = x + col;
      const idx = rowOffset + imgX;
      let v = state.dataRaw[idx];
      if (maskReady && maskData) {
        const maskValue = maskData[idx];
        if (maskValue & 1) {
          v = 0;
        } else if (maskValue & 0x1e) {
          const j = outOffset + col * 4;
          out[j] = 51;
          out[j + 1] = 153;
          out[j + 2] = 255;
          out[j + 3] = 255;
          continue;
        }
      }
      let norm = Math.min(1, Math.max(0, (v - state.min) / range));
      if (state.invert) {
        norm = 1 - norm;
      }
      const p = Math.floor(norm * 255) * 4;
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
  if (["o", "s", "e", "n", "w"].includes(key)) {
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
    precision highp usampler2D;
    uniform sampler2D u_data;
    uniform sampler2D u_lut;
    uniform usampler2D u_mask;
    uniform float u_mask_enabled;
    uniform float u_min;
    uniform float u_max;
    uniform float u_invert;
    uniform float u_data_max;
    uniform float u_hdr;
    in vec2 v_tex;
    out vec4 outColor;
    void main() {
      float value = texture(u_data, v_tex).r;
      if (u_mask_enabled > 0.5) {
        uint mask = texture(u_mask, v_tex).r;
        if ((mask & 1u) != 0u) {
          value = 0.0;
        } else if ((mask & 0x1Eu) != 0u) {
          outColor = vec4(0.2, 0.6, 1.0, 1.0);
          return;
        }
      }
      float denom = max(u_max - u_min, 1.0);
      float t = (value - u_min) / denom;
      float norm = 0.0;
      if (u_hdr > 0.5) {
        if (t <= 0.0) {
          norm = 0.0;
        } else if (t <= 1.0) {
          const float HDR_GAMMA = 1.0;
          norm = 0.5 * pow(t, HDR_GAMMA);
        } else {
          const float HDR_OVER = 50.0;
          const float HDR_OVER_GAMMA = 0.28;
          float overRange = max(u_data_max - u_max, 1.0);
          float overRatio = max(0.0, (value - u_max) / overRange);
          float over = log(1.0 + overRatio * HDR_OVER) / log(1.0 + HDR_OVER);
          over = pow(over, HDR_OVER_GAMMA);
          norm = 0.5 + 0.5 * clamp(over, 0.0, 1.0);
        }
      } else {
        norm = clamp(t, 0.0, 1.0);
      }
      if (u_invert > 0.5) {
        norm = 1.0 - norm;
      }
      outColor = texture(u_lut, vec2(norm, 0.5));
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
    gl.R32UI,
    1,
    1,
    0,
    gl.RED_INTEGER,
    gl.UNSIGNED_INT,
    new Uint32Array([0])
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
    dataMax: gl.getUniformLocation(program, "u_data_max"),
  };

  const maxTextureSize = gl.getParameter(gl.MAX_TEXTURE_SIZE);
  let maskTexWidth = 1;
  let maskTexHeight = 1;
  let lastMaskData = null;

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
      dataMax,
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
      gl.texImage2D(gl.TEXTURE_2D, 0, gl.R32F, width, height, 0, gl.RED, gl.FLOAT, floatData);
      gl.uniform1i(uniforms.data, 0);

      gl.activeTexture(gl.TEXTURE1);
      gl.bindTexture(gl.TEXTURE_2D, lutTex);
      gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, 256, 1, 0, gl.RGBA, gl.UNSIGNED_BYTE, palette);
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
        gl.pixelStorei(gl.UNPACK_ALIGNMENT, 1);
        gl.texImage2D(
          gl.TEXTURE_2D,
          0,
          gl.R32UI,
          width,
          height,
          0,
          gl.RED_INTEGER,
          gl.UNSIGNED_INT,
          mask
        );
        maskTexWidth = width;
        maskTexHeight = height;
        lastMaskData = mask;
      }

      gl.uniform1f(uniforms.min, min);
      gl.uniform1f(uniforms.max, max);
      gl.uniform1f(uniforms.invert, invert ? 1.0 : 0.0);
      gl.uniform1f(uniforms.hdr, colormap === "albulaHdr" ? 1.0 : 0.0);
      gl.uniform1f(uniforms.dataMax, Number.isFinite(dataMax) ? dataMax : max);
      gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
    },
  };
}

function createCpuRenderer() {
  const ctx = canvas.getContext("2d");
  return {
    type: "cpu",
    render({ data, width, height, min, max, palette, invert, mask, maskEnabled, colormap, dataMax }) {
      if (!data) return;
      if (canvas.width !== width || canvas.height !== height) {
        canvas.width = width;
        canvas.height = height;
      }
      const imageData = ctx.createImageData(width, height);
      const out = imageData.data;
      for (let i = 0; i < data.length; i += 1) {
        let v = data[i];
        if (maskEnabled && mask && mask.length === data.length) {
          const maskValue = mask[i];
          if (maskValue & 1) {
            v = 0;
          } else if (maskValue & 0x1e) {
            const j = i * 4;
            out[j] = 51;
            out[j + 1] = 153;
            out[j + 2] = 255;
            out[j + 3] = 255;
            continue;
          }
        }
        const norm = mapValueToNorm(v);
        const idx = Math.floor(norm * 255) * 4;
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
      setStatus("No HDF5 files found");
      showSplash();
      setLoading(false);
    }
    loadAutoloadFolders();
  }
}

function sortDatasets(datasets) {
  const primary = datasets.find((d) => d.path.includes("/entry/data/data"));
  if (primary) {
    return [primary, ...datasets.filter((d) => d !== primary)];
  }
  return datasets;
}

async function loadDatasets() {
  if (!state.file) return;
  state.hasFrame = false;
  stopPlayback();
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
    state.frameCount = data.shape.length === 3 ? data.shape[0] : 1;
    state.frameIndex = 0;
    frameRange.max = Math.max(state.frameCount - 1, 0);
    frameRange.value = "0";
    frameIndex.value = "0";
    metaShape.textContent = data.shape.join(" × ");
    metaDtype.textContent = data.dtype;
    updateToolbar();
    await loadFrame();
  } finally {
    hideProcessingProgress();
  }
}

function parseShape(header) {
  if (!header) return [];
  return header.split(",").map((v) => parseInt(v, 10));
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
  const bins = Math.round(Math.sqrt(count));
  return Math.max(64, Math.min(1024, bins));
}

const AUTO_CONTRAST_LOW = 0.001;
const AUTO_CONTRAST_HIGH = 0.999;
const AUTO_CONTRAST_BINS = 4096;
const ALBULA_HDR_OVER = 50;
const ALBULA_HDR_GAMMA = 1.0;
const ALBULA_HDR_OVER_GAMMA = 0.3;

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
  const dataMax = Number.isFinite(state.stats?.max) ? state.stats.max : maxVal;
  const range = maxVal - minVal || 1;
  let t = (value - minVal) / range;
  let norm = 0;
  if (state.colormap === "albulaHdr") {
    if (t <= 0) {
      norm = 0;
    } else if (t <= 1) {
      norm = 0.5 * Math.pow(t, ALBULA_HDR_GAMMA);
    } else {
      const overRange = Math.max(1e-6, dataMax - maxVal);
      const overRatio = Math.max(0, (value - maxVal) / overRange);
      let over =
        Math.log(1 + overRatio * ALBULA_HDR_OVER) / Math.log(1 + ALBULA_HDR_OVER);
      over = Math.pow(over, ALBULA_HDR_OVER_GAMMA);
      norm = 0.5 + 0.5 * Math.min(1, Math.max(0, over));
    }
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
  const palette = new Uint8Array(256 * 4);
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
  const mixStopsAt = (stops, t) => {
    let idx = 0;
    while (idx < stops.length - 1 && t > stops[idx + 1].t) {
      idx += 1;
    }
    const a = stops[idx];
    const b = stops[Math.min(idx + 1, stops.length - 1)];
    const span = b.t - a.t || 1;
    const frac = Math.min(1, Math.max(0, (t - a.t) / span));
    return [
      Math.round(a.c[0] + (b.c[0] - a.c[0]) * frac),
      Math.round(a.c[1] + (b.c[1] - a.c[1]) * frac),
      Math.round(a.c[2] + (b.c[2] - a.c[2]) * frac),
    ];
  };
  for (let i = 0; i < 256; i += 1) {
    const t = i / 255;
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
      [r, g, b] = mixStopsAt(
        [
          { t: 0.0, c: [255, 255, 255] },
          { t: 0.51, c: [0, 0, 0] },
          { t: 0.54, c: [255, 0, 0] },
          { t: 0.58, c: [255, 255, 0] },
          { t: 1.0, c: [255, 255, 255] },
        ],
        t
      );
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

    const drawMarker = (x, color, label) => {
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
        const textX = Math.min(width - metrics.width - 4, Math.max(4, x + 6));
        histCtx.fillText(label, textX, markerTop + 10);
      }
    };

    drawMarker(minX, "#6eb5ff", `BG ${formatValue(minVal)}`);
    drawMarker(maxX, "#ffd166", `FG ${formatValue(maxVal)}`);
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
  const maxIdx = 255;
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
  const logToggleEl = roiLogToggle?.closest(".roi-log-toggle");
  if (logToggleEl) {
    logToggleEl.classList.toggle("is-hidden", !showPlots);
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
  setRoiText(roiMeanEl, "-");
  setRoiText(roiMinEl, "-");
  setRoiText(roiMaxEl, "-");
  setRoiText(roiSumEl, "-");
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
  const dataY = renderer?.type === "webgl" ? state.height - 1 - iy : iy;
  const idx = dataY * state.width + ix;
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
    setRoiText(roiMeanEl, stats ? formatStat(stats.mean) : "-");
    setRoiText(roiMinEl, stats ? formatStat(stats.min) : "-");
    setRoiText(roiMaxEl, stats ? formatStat(stats.max) : "-");
    setRoiText(roiSumEl, stats ? formatStat(stats.sum) : "-");
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
    setRoiText(roiMeanEl, stats ? formatStat(stats.mean) : "-");
    setRoiText(roiMinEl, stats ? formatStat(stats.min) : "-");
    setRoiText(roiMaxEl, stats ? formatStat(stats.max) : "-");
    setRoiText(roiSumEl, stats ? formatStat(stats.sum) : "-");
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
    setRoiText(roiMeanEl, count ? formatStat(mean) : "-");
    setRoiText(roiMinEl, count ? formatStat(min) : "-");
    setRoiText(roiMaxEl, count ? formatStat(max) : "-");
    setRoiText(roiSumEl, count ? formatStat(sum) : "-");
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

    for (let y = top; y <= bottom; y += 1) {
      const rowIndex = y - top;
      for (let x = left; x <= right; x += 1) {
        const colIndex = x - left;
        const sampled = sampleValue(x, y);
        if (!sampled) continue;
        const v = sampled.value;
        if (!sampled.skip && Number.isFinite(v)) {
          count += 1;
          sum += v;
          min = Math.min(min, v);
          max = Math.max(max, v);
          const delta = v - mean;
          mean += delta / count;
          m2 += delta * (v - mean);
          xProj[colIndex] += v;
          yProj[rowIndex] += v;
        }
      }
    }
    roiState.xProjection = Array.from(xProj);
    roiState.yProjection = Array.from(yProj);
    roiState.lineProfile = null;
    setRoiText(roiSizeEl, `${width} × ${height}`);
    setRoiText(roiCountEl, count ? `${count}` : "0");
    const std = count > 1 ? Math.sqrt(m2 / (count - 1)) : 0;
    setRoiText(roiMeanEl, count ? formatStat(mean) : "-");
    setRoiText(roiMinEl, count ? formatStat(min) : "-");
    setRoiText(roiMaxEl, count ? formatStat(max) : "-");
    setRoiText(roiSumEl, count ? formatStat(sum) : "-");
    setRoiText(roiStdEl, count ? formatStat(std) : "-");
    drawRoiPlot(roiLineCanvas, roiLineCtx, null, roiState.log);
    if (roiXCanvas) {
      roiXCanvas._roiPlotMeta = {
        xLabel: "X Pixel",
        yLabel: "Sum",
        xStart: left,
        xStep: 1,
      };
    }
    if (roiYCanvas) {
      roiYCanvas._roiPlotMeta = {
        xLabel: "Y Pixel",
        yLabel: "Sum",
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

    const profile = Array.from(radialSum, (v, i) =>
      radialCount[i] ? v / radialCount[i] : 0
    );
    roiState.lineProfile = profile;
    roiState.xProjection = null;
    roiState.yProjection = null;
    if (roiState.mode === "circle") {
      setRoiText(roiSizeEl, `${outerRadius}`);
    } else {
      setRoiText(roiSizeEl, `${roiState.innerRadius} → ${outerRadius}`);
    }
    setRoiText(roiCountEl, count ? `${count}` : "0");
    const std = count > 1 ? Math.sqrt(m2 / (count - 1)) : 0;
    setRoiText(roiMeanEl, count ? formatStat(mean) : "-");
    setRoiText(roiMinEl, count ? formatStat(min) : "-");
    setRoiText(roiMaxEl, count ? formatStat(max) : "-");
    setRoiText(roiSumEl, count ? formatStat(sum) : "-");
    setRoiText(roiStdEl, count ? formatStat(std) : "-");
    if (roiLineCanvas) {
      roiLineCanvas._roiPlotMeta = {
        xLabel: "Radius (px)",
        yLabel: "Intensity",
        xStart: 0,
        xStep: 1,
      };
    }
    drawRoiPlot(roiLineCanvas, roiLineCtx, profile, roiState.log);
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
  const values = logScale ? data.map((v) => Math.log10(1 + Math.max(0, v))) : data;
  const maxValue = Math.max(...values);
  const padL = 34;
  const padR = 8;
  const padT = 8;
  const padB = 22;
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
      const xValue = plotMeta.xStart + t * (values.length - 1) * (plotMeta.xStep ?? 1);
      labels.push(formatStat(xValue));
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
    const xValue = plotMeta.xStart + t * (values.length - 1) * (plotMeta.xStep ?? 1);
    ctx.fillText(formatStat(xValue), x, padT + drawableHeight + 6);
  }

  let yTickCount = Math.max(2, Math.min(4, Math.floor(drawableHeight / 50)));
  while (yTickCount > 1) {
    const labels = [];
    for (let i = 0; i <= yTickCount; i += 1) {
      const t = i / yTickCount;
      const displayVal = t * maxValue;
      const actualVal = logScale ? Math.pow(10, displayVal) - 1 : displayVal;
      labels.push(formatStat(actualVal));
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
    const displayVal = t * maxValue;
    const actualVal = logScale ? Math.pow(10, displayVal) - 1 : displayVal;
    ctx.fillText(formatStat(actualVal), padL - 6, y);
  }

  ctx.strokeStyle = "#e5e5e5";
  ctx.lineWidth = 1;
  ctx.beginPath();
  values.forEach((v, i) => {
    const x = padL + (i / Math.max(1, values.length - 1)) * drawableWidth;
    const y = padT + drawableHeight - (maxValue ? (v / maxValue) * drawableHeight : 0);
    if (i === 0) {
      ctx.moveTo(x, y);
    } else {
      ctx.lineTo(x, y);
    }
  });
  ctx.stroke();

  const xLabel = plotMeta.xLabel || "Index";
  const yLabel = plotMeta.yLabel || "Value";
  ctx.fillStyle = "#cfcfcf";
  ctx.font = "10px \"Lucida Grande\", \"Helvetica Neue\", Arial, sans-serif";
  ctx.textAlign = "center";
  ctx.fillText(xLabel, padL + drawableWidth / 2, height - 6);
  ctx.save();
  ctx.translate(10, padT + drawableHeight / 2);
  ctx.rotate(-Math.PI / 2);
  ctx.fillText(yLabel, 0, 0);
  ctx.restore();

  canvasEl._roiPlot = {
    data,
    log: logScale,
    xLabel,
    yLabel,
    padL,
    padR,
    padT,
    padB,
    width,
    height,
    xStart: plotMeta.xStart ?? 0,
    xStep: plotMeta.xStep ?? 1,
  };
  ctx.strokeStyle = "rgba(0,0,0,0.5)";
  ctx.strokeRect(0.5, 0.5, width - 1, height - 1);
}

function closeCurrentFile() {
  stopPlayback();
  state.file = "";
  state.dataset = "";
  state.shape = [];
  state.dtype = "";
  state.frameCount = 1;
  state.frameIndex = 0;
  state.dataRaw = null;
  state.dataFloat = null;
  state.histogram = null;
  state.stats = null;
  state.hasFrame = false;
  state.globalStats = null;
  clearMaskState();
  updateToolbar();
  setStatus("No file loaded");
  setLoading(false);
  hideUploadProgress();
  hideProcessingProgress();
  showSplash();

  fileSelect.selectedIndex = 0;
  datasetSelect.innerHTML = "";
  frameRange.max = "0";
  frameRange.value = "0";
  frameIndex.value = "0";
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
      dataMax: state.stats?.max ?? state.max,
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
      invert: state.invert,
      colormap: state.colormap,
      dataMax: state.stats?.max ?? state.max,
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
}

async function loadFrame() {
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
  )}&index=${state.frameIndex}`;
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
    if (aboutModal) {
      aboutModal.classList.remove("is-open");
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

if (fileInput) {
  fileInput.addEventListener("change", async () => {
    const file = fileInput.files?.[0];
    if (!file) return;
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
fileSelect.addEventListener("change", async (event) => {
  state.file = event.target.value;
  if (!state.file) return;
  stopPlayback();
  await loadDatasets();
});

datasetSelect.addEventListener("change", async (event) => {
  state.dataset = event.target.value;
  stopPlayback();
  await loadMetadata();
});

frameRange.addEventListener("input", async (event) => {
  stopPlayback();
  const value = Number(event.target.value);
  requestFrame(value);
});

frameIndex.addEventListener("change", async (event) => {
  stopPlayback();
  const value = Math.max(0, Math.min(state.frameCount - 1, Number(event.target.value)));
  requestFrame(value);
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

autoloadBrowse?.addEventListener("click", async () => {
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
    state.min = snapHistogramValue(Number(minInput.value || state.min));
    state.max = snapHistogramValue(Number(maxInput.value || state.max));
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
    const delta = Math.sign(event.deltaY);
    const next = Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, state.zoom - delta * 0.2));
    zoomAt(event.clientX, event.clientY, next);
  },
  { passive: false }
);

canvasWrap.addEventListener("scroll", () => {
  scheduleOverview();
  schedulePixelOverlay();
  scheduleRoiOverlay();
});

canvasWrap.addEventListener("contextmenu", (event) => {
  event.preventDefault();
});

canvasWrap.addEventListener("pointerdown", (event) => {
  if (event.button === 2 && roiState.mode !== "none") {
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
  updateCursorOverlay(event);
  if (roiDragging) {
    const point = getImagePointFromEvent(event);
    if (!point) return;
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

canvasWrap.addEventListener("pointerup", (event) => {
  stopRoi(event);
  stopPan(event);
});

canvasWrap.addEventListener("pointercancel", (event) => {
  stopRoi(event);
  stopPan(event);
});

canvasWrap.addEventListener("pointerleave", () => {
  stopRoi();
  hideCursorOverlay();
});

canvasWrap.addEventListener("dblclick", (event) => {
  event.preventDefault();
  const next = Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, state.zoom * 2));
  zoomAt(event.clientX, event.clientY, next);
});

[roiLineCanvas, roiXCanvas, roiYCanvas].forEach((canvasEl) => {
  if (!canvasEl) return;
  canvasEl.addEventListener("mousemove", (event) => updateRoiTooltip(event, canvasEl));
  canvasEl.addEventListener("mouseleave", () => hideRoiTooltip(canvasEl));
});

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
      if (handle === "n" || handle === "s") {
        overviewCanvas.style.cursor = "ns-resize";
      } else if (handle === "e" || handle === "w") {
        overviewCanvas.style.cursor = "ew-resize";
      } else {
        overviewCanvas.style.cursor =
          handle === "nw" || handle === "se" ? "nwse-resize" : "nesw-resize";
      }
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
  if (state.histogram) drawHistogram(state.histogram);
  drawSplash();
  applyPanelState();
  scheduleOverview();
  scheduleHistogram();
  schedulePixelOverlay();
  scheduleRoiOverlay();
  scheduleRoiUpdate();
});

initRenderer();
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
loadAutoloadFolders();
loadFiles().catch((err) => {
  console.error(err);
  setStatus("Failed to initialize");
  showSplash();
  setLoading(false);
});
updatePlayButtons();
