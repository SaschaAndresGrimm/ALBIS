/**
 * The box ROI's 3D view: a card under the box's profiles that shows the
 * pixels of the box as a surface whose height is their value, like adxv's
 * 3-D magnify window. For a single peak it shows its shape; for a box over
 * the whole frame, the detector at a glance, in blocks (roi_surface_engine.js).
 *
 * Same colour map, contrast and mask colours as the image, and the frame as
 * shown (flatfield-corrected, inpainted). Drag turns it, Shift-drag or
 * right-drag moves it, the wheel zooms and Shift+wheel stretches the
 * heights; a double-click puts the view back. The card's title is its on/off
 * box, like the histogram's; what it shows, wire mesh and log height are
 * remembered.
 *
 * The card is built here rather than in index.html, which is at its line
 * baseline (tests/code_size_guard.test.js).
 */

import { applyI18nToDom, t } from "./i18n.js";
import { currentMaskColors } from "./mask_colors.js";
import { usesInDataPixelFlags } from "./roi_stats_engine.js";
import {
  DEFAULT_SURFACE_CAMERA,
  SURFACE_KIND,
  buildSurfaceGeometry,
  buildSurfaceGrid,
  clampSurfaceCamera,
  orbitSurfaceCamera,
  panSurfaceCamera,
  parseCssRgb,
  pickSurfaceVertex,
  surfaceBoxBounds,
  surfaceCellPixel,
  surfaceVertexColors,
  surfaceViewMatrix,
} from "./roi_surface_engine.js";
import { createSurfaceRenderer } from "./roi_surface_renderer.js";

const STORAGE_KEY = "albis.roiSurface";
// Grid lines over the surface while a cell is still a few screen pixels wide.
const GRID_LINES_MAX_CELLS = 96;
const KEY_TURN_PX = 12;
const ZOOM_PER_WHEEL_PIXEL = 0.0015;
const MIN_CARD_HEIGHT = 120;

const CARD_HTML = `
  <div class="roi-plot-header">
    <label class="checkbox roi-histogram-toggle roi-plot-title"><input type="checkbox" data-surface-toggle /><span data-i18n="analysis.roi.plot.surface">3D View</span></label>
    <div class="roi-plot-actions">
      <button type="button" class="roi-plot-icon-btn" data-surface-reset title="Reset the view" aria-label="Reset the view" data-i18n-title="roi.surface.reset" data-i18n-aria-label="roi.surface.reset">↺</button><button type="button" class="roi-plot-icon-btn" data-surface-png title="Export plot as PNG" aria-label="Export plot as PNG" data-i18n-title="roi.plot.export_png" data-i18n-aria-label="roi.plot.export_png"><svg class="roi-plot-png-icon" viewBox="0 0 16 16" aria-hidden="true" focusable="false"><path d="M8 1.75v9M4 7l4 4 4-4M2.5 14h11" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" /></svg></button>
    </div>
  </div>
  <div class="roi-surface-options">
    <label class="checkbox"><input type="checkbox" data-surface-wire /><span data-i18n="roi.surface.wire">Wire mesh</span></label>
    <label class="checkbox"><input type="checkbox" data-surface-log /><span data-i18n="roi.surface.log">Log height</span></label>
    <button type="button" class="info-tip" data-info-key="info.roi.surface" aria-label="More information" data-i18n-aria-label="info.button_label"></button>
  </div>
  <div class="roi-surface-stage">
    <canvas class="roi-surface-canvas" tabindex="0" role="img" aria-label="3D view of the box ROI" data-i18n-aria-label="roi.surface.canvas_label"></canvas>
    <div class="roi-surface-message" aria-live="polite"></div>
    <div class="roi-tooltip" aria-hidden="true"></div>
  </div>
  <div class="roi-surface-note" aria-live="polite"></div>
  <div class="roi-resize-handle"></div>
`;

function readSettings() {
  try {
    const stored = JSON.parse(window.localStorage?.getItem(STORAGE_KEY) || "{}") || {};
    return { open: stored.open === true, wire: stored.wire === true, log: stored.log === true };
  } catch {
    return { open: false, wire: false, log: false };
  }
}

function sameKey(a, b) {
  return Boolean(a && b && a.length === b.length && a.every((value, i) => Object.is(value, b[i])));
}

export function createRoiSurfaceController({ state, roiState, elements = {}, callbacks = {} }) {
  const { anchor } = elements;
  const {
    buildPalette,
    getPaletteColorCount,
    mapValueToNorm,
    getActiveSaturationMax = () => Number.NaN,
    isSaturatedValue = () => false,
    getInpaintedFrame = () => null,
    formatStat = (value) => String(value),
    showTooltip = () => {},
    hideTooltip = () => {},
    exportPng = () => {},
  } = callbacks;

  const card = document.createElement("div");
  card.className = "roi-plot roi-surface-plot is-hidden is-collapsed";
  card.id = "roi-surface-plot";
  card.innerHTML = CARD_HTML;
  if (anchor?.parentElement) anchor.parentElement.insertBefore(card, anchor);
  applyI18nToDom(card);

  const toggle = card.querySelector("[data-surface-toggle]");
  const wireBox = card.querySelector("[data-surface-wire]");
  const logBox = card.querySelector("[data-surface-log]");
  const resetBtn = card.querySelector("[data-surface-reset]");
  const pngBtn = card.querySelector("[data-surface-png]");
  const canvas = card.querySelector("canvas");
  const message = card.querySelector(".roi-surface-message");
  const note = card.querySelector(".roi-surface-note");
  const resizeHandle = card.querySelector(".roi-resize-handle");

  let settings = readSettings();
  let camera = { ...DEFAULT_SURFACE_CAMERA };
  let renderer = null;
  let rendererTried = false;
  let grid = null;
  let gridKey = null;
  let geometry = null;
  let geometryKey = null;
  let colors = null;
  let uploadedGeometry = null;
  let uploadedColors = null;
  let matrix = null;
  let frameRequest = 0;
  let drag = null;

  function saveSettings() {
    try {
      window.localStorage?.setItem(STORAGE_KEY, JSON.stringify(settings));
    } catch {
      // Remembered for this session only.
    }
  }

  function visible() {
    return Boolean(roiState.enabled && roiState.mode === "box");
  }

  function showing() {
    return visible() && settings.open;
  }

  function syncCard() {
    card.classList.toggle("is-hidden", !visible());
    card.classList.toggle("is-collapsed", !settings.open);
    // A height dragged with the corner handle would hold the folded card open.
    if (!settings.open) card.style.removeProperty("height");
    if (toggle) toggle.checked = settings.open;
    if (wireBox) wireBox.checked = settings.wire;
    if (logBox) logBox.checked = settings.log;
  }

  /** The grid for the box on the frame as shown; rebuilt only when one of them changed. */
  function refreshGrid() {
    const hasBox = Boolean(state.hasFrame && state.dataRaw && roiState.active);
    const bounds = hasBox ? surfaceBoxBounds(roiState, state.width, state.height) : null;
    if (!bounds) {
      grid = null;
      gridKey = null;
      return;
    }
    const fill = getInpaintedFrame();
    const shown = fill?.data || null;
    const holes = shown ? fill.holes || null : null;
    const maskApplied =
      state.maskEnabled &&
      state.maskAvailable &&
      state.maskRaw &&
      state.maskShape?.[0] === state.height &&
      state.maskShape?.[1] === state.width;
    const mask = maskApplied ? state.maskRaw : null;
    const inDataFlags = !state.maskAvailable && usesInDataPixelFlags(state.dataRaw);
    const satMax = getActiveSaturationMax();
    const markSaturated = Boolean(state.maskSaturatedEnabled && Number.isFinite(satMax));
    const key = [
      state.dataRaw, shown, holes, mask, inDataFlags, markSaturated ? satMax : null,
      bounds.left, bounds.top, bounds.width, bounds.height,
    ];
    if (sameKey(key, gridKey)) return;
    grid = buildSurfaceGrid({
      data: state.dataRaw,
      shown,
      holes,
      mask,
      inDataFlags,
      isSaturated: markSaturated ? (raw) => isSaturatedValue(raw, satMax) : null,
      frameWidth: state.width,
      ...bounds,
    });
    gridKey = key;
    geometry = null;
    colors = null;
  }

  function refreshGeometry() {
    if (!grid) {
      geometry = null;
      return;
    }
    const key = [grid, settings.log, state.pixelAspect || 1, camera.heightScale];
    if (geometry && sameKey(key, geometryKey)) return;
    geometry = buildSurfaceGeometry(grid, { log: settings.log, pixelAspect: state.pixelAspect || 1, heightScale: camera.heightScale });
    geometryKey = key;
  }

  /** Vertex colours as the image paints its pixels: the colour map at the contrast, mask colours for the rest. */
  function refreshColors() {
    if (!grid) {
      colors = null;
      return;
    }
    const palette = buildPalette(state.colormap);
    const maxIndex = Math.max(0, getPaletteColorCount(palette) - 1);
    const maskColors = currentMaskColors(state);
    const paletteColor = (value) => {
      const norm = Math.min(1, Math.max(0, Number(mapValueToNorm(value)) || 0));
      const p = Math.floor(norm * maxIndex) * 4;
      return [palette[p], palette[p + 1], palette[p + 2]];
    };
    const floor = paletteColor(grid.min);
    colors = surfaceVertexColors(grid, (i) => {
      switch (grid.kinds[i]) {
        case SURFACE_KIND.value:
          return paletteColor(grid.values[i]);
        case SURFACE_KIND.saturated:
          return maskColors.saturated;
        case SURFACE_KIND.gap:
          return maskColors.gap;
        case SURFACE_KIND.defective:
          return maskColors.defective;
        default:
          return floor;
      }
    });
  }

  function setMessage(text) {
    if (message) {
      message.textContent = text || "";
      message.classList.toggle("is-visible", Boolean(text));
      message.parentElement?.classList.toggle("has-message", Boolean(text));
    }
  }

  function sizeCanvas() {
    const dpr = Math.max(1, Math.min(2, window.devicePixelRatio || 1));
    const width = Math.max(1, Math.round((canvas.clientWidth || 1) * dpr));
    const height = Math.max(1, Math.round((canvas.clientHeight || 1) * dpr));
    if (canvas.width !== width) canvas.width = width;
    if (canvas.height !== height) canvas.height = height;
  }

  function background() {
    return parseCssRgb(window.getComputedStyle?.(canvas)?.backgroundColor) || [0, 0, 0];
  }

  function draw() {
    if (!showing() || !canvas) return;
    if (!rendererTried) {
      rendererTried = true;
      renderer = createSurfaceRenderer(canvas);
    }
    const blocks = grid && grid.step > 1 ? t("roi.surface.blocks", { size: grid.step }) : "";
    if (note) note.textContent = blocks;
    canvas._roiPlotMeta = null;
    matrix = null;
    if (!renderer) {
      setMessage(t("roi.surface.no_webgl"));
      return;
    }
    sizeCanvas();
    let problem = "";
    if (!grid) problem = t("roi.surface.draw_box");
    else if (grid.cols < 2 || grid.rows < 2) problem = t("roi.surface.too_small");
    setMessage(problem);
    if (problem) {
      renderer.draw({ empty: true, background: background() });
      return;
    }
    refreshGeometry();
    if (!colors) refreshColors();
    if (uploadedGeometry !== geometry) {
      renderer.setGeometry(geometry);
      uploadedGeometry = geometry;
    }
    if (uploadedColors !== colors) {
      renderer.setColors(colors);
      uploadedColors = colors;
    }
    matrix = surfaceViewMatrix(camera, canvas.width / canvas.height, geometry.zTop);
    renderer.draw({
      matrix,
      wire: settings.wire,
      grid: grid.cols <= GRID_LINES_MAX_CELLS && grid.rows <= GRID_LINES_MAX_CELLS,
      background: background(),
    });
    // What the PNG export looks for (roi_plot_controller.js).
    canvas._roiPlotMeta = { kind: "surface" };
  }

  function schedule() {
    if (frameRequest || !showing()) return;
    frameRequest = window.requestAnimationFrame(() => {
      frameRequest = 0;
      draw();
    });
  }

  /** After the ROI or the frame changed (updateRoiStats). */
  function update() {
    syncCard();
    if (!showing()) return;
    refreshGrid();
    schedule();
  }

  /** After the contrast or colour map changed: the same mesh, new colours. */
  function refresh() {
    if (!showing()) return;
    refreshGrid();
    refreshColors();
    schedule();
  }

  function setCamera(next) {
    camera = clampSurfaceCamera(next);
    schedule();
  }

  function resetView() {
    setCamera({ ...DEFAULT_SURFACE_CAMERA });
  }

  function hoverText(index) {
    const cell = surfaceCellPixel(grid, index);
    const kind = grid.kinds[index];
    let value = "–";
    if (kind === SURFACE_KIND.value) value = formatStat(grid.values[index]);
    else if (kind === SURFACE_KIND.saturated) value = t("legend.mask.saturated");
    else if (kind === SURFACE_KIND.gap) value = t("legend.mask.gap");
    else if (kind === SURFACE_KIND.defective) value = t("legend.mask.defective");
    if (grid.step === 1) return t("roi.surface.tooltip_pixel", { x: cell.x, y: cell.y, value });
    return t("roi.surface.tooltip_block", { x: cell.x, y: cell.y, columns: cell.columns, rows: cell.rows, value });
  }

  function hover(event) {
    if (!matrix || !geometry) {
      hideTooltip(canvas);
      return;
    }
    const rect = canvas.getBoundingClientRect();
    if (!(rect.width > 0) || !(rect.height > 0)) return;
    const index = pickSurfaceVertex(
      geometry.positions,
      matrix,
      event.clientX - rect.left,
      event.clientY - rect.top,
      rect.width,
      rect.height,
    );
    if (index < 0) {
      hideTooltip(canvas);
      return;
    }
    showTooltip(canvas, hoverText(index), event.clientX, event.clientY);
  }

  toggle?.addEventListener("change", () => {
    settings = { ...settings, open: Boolean(toggle.checked) };
    saveSettings();
    update();
  });
  wireBox?.addEventListener("change", () => {
    settings = { ...settings, wire: Boolean(wireBox.checked) };
    saveSettings();
    schedule();
  });
  logBox?.addEventListener("change", () => {
    settings = { ...settings, log: Boolean(logBox.checked) };
    saveSettings();
    schedule();
  });
  resetBtn?.addEventListener("click", resetView);
  pngBtn?.addEventListener("click", () => exportPng());

  canvas?.addEventListener("pointerdown", (event) => {
    if (!matrix || (event.button !== 0 && event.button !== 2)) return;
    event.preventDefault();
    // For the arrow keys; the ring is for keyboard focus only.
    canvas.focus({ preventScroll: true, focusVisible: false });
    drag = { x: event.clientX, y: event.clientY, pan: event.button === 2 || event.shiftKey };
    canvas.setPointerCapture?.(event.pointerId);
    hideTooltip(canvas);
  });
  canvas?.addEventListener("pointermove", (event) => {
    if (!drag) {
      hover(event);
      return;
    }
    const dx = event.clientX - drag.x;
    const dy = event.clientY - drag.y;
    drag.x = event.clientX;
    drag.y = event.clientY;
    if (drag.pan) setCamera(panSurfaceCamera(camera, dx, dy, canvas.clientWidth, canvas.clientHeight));
    else setCamera(orbitSurfaceCamera(camera, dx, dy));
  });
  const endDrag = () => {
    drag = null;
  };
  canvas?.addEventListener("pointerup", endDrag);
  canvas?.addEventListener("pointercancel", endDrag);
  canvas?.addEventListener("pointerleave", () => hideTooltip(canvas));
  canvas?.addEventListener("contextmenu", (event) => event.preventDefault());
  canvas?.addEventListener("dblclick", resetView);
  canvas?.addEventListener(
    "wheel",
    (event) => {
      if (!matrix) return;
      event.preventDefault();
      // Shift turns a vertical wheel into a horizontal one on macOS.
      const delta = event.deltaY || event.deltaX;
      const factor = Math.exp(-delta * ZOOM_PER_WHEEL_PIXEL);
      if (event.shiftKey || event.altKey) setCamera({ ...camera, heightScale: camera.heightScale * factor });
      else setCamera({ ...camera, zoom: camera.zoom * factor });
    },
    { passive: false },
  );
  canvas?.addEventListener("keydown", (event) => {
    if (event.metaKey || event.ctrlKey || event.altKey) return;
    let next = null;
    if (event.key === "ArrowLeft") next = orbitSurfaceCamera(camera, -KEY_TURN_PX, 0);
    else if (event.key === "ArrowRight") next = orbitSurfaceCamera(camera, KEY_TURN_PX, 0);
    else if (event.key === "ArrowUp") next = orbitSurfaceCamera(camera, 0, -KEY_TURN_PX);
    else if (event.key === "ArrowDown") next = orbitSurfaceCamera(camera, 0, KEY_TURN_PX);
    else if (event.key === "+" || event.key === "=") next = { ...camera, zoom: camera.zoom * 1.2 };
    else if (event.key === "-") next = { ...camera, zoom: camera.zoom / 1.2 };
    else if (event.key === "r" || event.key === "R" || event.key === "0") next = { ...DEFAULT_SURFACE_CAMERA };
    if (!next) return;
    // The arrows step through frames on the image; here they turn the view.
    event.preventDefault();
    event.stopPropagation();
    setCamera(next);
  });

  resizeHandle?.addEventListener("pointerdown", (event) => {
    if (event.button !== 0) return;
    event.preventDefault();
    const startY = event.clientY;
    const startHeight = card.getBoundingClientRect().height;
    const move = (moveEvent) => {
      card.style.height = `${Math.max(MIN_CARD_HEIGHT, startHeight + moveEvent.clientY - startY)}px`;
      schedule();
    };
    const end = () => {
      document.removeEventListener("pointermove", move);
      document.removeEventListener("pointerup", end);
    };
    document.addEventListener("pointermove", move);
    document.addEventListener("pointerup", end);
  });

  if (canvas && typeof window.ResizeObserver === "function") {
    new window.ResizeObserver(() => schedule()).observe(canvas);
  }
  canvas?.addEventListener("webglcontextlost", (event) => {
    event.preventDefault();
    uploadedGeometry = null;
    uploadedColors = null;
  });
  canvas?.addEventListener("webglcontextrestored", () => {
    rendererTried = false;
    renderer = null;
    schedule();
  });

  syncCard();

  return {
    update,
    refresh,
    resetView,
    card,
    get settings() {
      return { ...settings };
    },
    get camera() {
      return { ...camera };
    },
    get grid() {
      return grid;
    },
  };
}
