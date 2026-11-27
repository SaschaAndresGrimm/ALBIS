import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("../modules/i18n.js", () => ({
  t: (key, vars) => (vars ? `${key} ${JSON.stringify(vars)}` : key),
  applyI18nToDom: () => {},
}));

const renderer = vi.hoisted(() => ({ current: null }));
vi.mock("../modules/roi_surface_renderer.js", () => ({
  createSurfaceRenderer: () => renderer.current,
}));

import { createRoiSurfaceController } from "../modules/roi_surface_controller.js";

function fakeRenderer() {
  return { setGeometry: vi.fn(), setColors: vi.fn(), draw: vi.fn() };
}

function setup({ roi = {}, frame = {} } = {}) {
  document.body.innerHTML = `<div class="roi-plots"><div id="hist"></div></div>`;
  const anchor = document.getElementById("hist");
  const width = 8;
  const height = 6;
  const state = {
    hasFrame: true,
    dataRaw: new Float64Array(width * height).map((_, i) => i),
    width,
    height,
    colormap: "gray",
    maskEnabled: false,
    maskAvailable: false,
    ...frame,
  };
  const roiState = { enabled: true, mode: "box", active: true, start: { x: 1, y: 1 }, end: { x: 4, y: 3 }, ...roi };
  const callbacks = {
    buildPalette: () => new Uint8Array(256 * 4).fill(200),
    getPaletteColorCount: () => 256,
    mapValueToNorm: vi.fn(() => 0.5),
    getActiveSaturationMax: () => Number.NaN,
    isSaturatedValue: () => false,
    formatStat: (value) => `v${value}`,
    showTooltip: vi.fn(),
    hideTooltip: vi.fn(),
    exportPng: vi.fn(),
  };
  const controller = createRoiSurfaceController({ state, roiState, elements: { anchor }, callbacks });
  return { controller, state, roiState, callbacks, card: controller.card };
}

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

describe("ROI 3D view", () => {
  beforeEach(() => {
    localStorage.clear();
    renderer.current = fakeRenderer();
    vi.stubGlobal("requestAnimationFrame", (cb) => setTimeout(cb, 0));
  });
  afterEach(() => vi.unstubAllGlobals());

  it("is a card above the histogram's, shown for a box ROI only, folded until switched on", () => {
    const { controller, roiState, card } = setup();
    expect(card.nextElementSibling?.id).toBe("hist");
    controller.update();
    expect(card.classList.contains("is-hidden")).toBe(false);
    expect(card.classList.contains("is-collapsed")).toBe(true);
    roiState.mode = "line";
    controller.update();
    expect(card.classList.contains("is-hidden")).toBe(true);
  });

  it("measures nothing while folded", () => {
    const { controller } = setup();
    controller.update();
    expect(controller.grid).toBeNull();
  });

  it("switched on, draws the box's pixels and remembers that it is on", async () => {
    const { controller, card } = setup();
    const toggle = card.querySelector("[data-surface-toggle]");
    toggle.checked = true;
    toggle.dispatchEvent(new Event("change"));
    await flush();
    expect(controller.grid).toMatchObject({ cols: 4, rows: 3, step: 1, min: 9, max: 28 });
    expect(renderer.current.setGeometry).toHaveBeenCalledTimes(1);
    expect(renderer.current.draw).toHaveBeenLastCalledWith(expect.objectContaining({ wire: false, grid: true }));
    expect(JSON.parse(localStorage.getItem("albis.roiSurface")).open).toBe(true);
    expect(card.querySelector("canvas")._roiPlotMeta).toBeTruthy();
  });

  it("draws the wire mesh, and the log heights as a new mesh", async () => {
    localStorage.setItem("albis.roiSurface", JSON.stringify({ open: true }));
    const { controller, card } = setup();
    controller.update();
    await flush();
    const wire = card.querySelector("[data-surface-wire]");
    wire.checked = true;
    wire.dispatchEvent(new Event("change"));
    await flush();
    expect(renderer.current.draw).toHaveBeenLastCalledWith(expect.objectContaining({ wire: true }));
    expect(renderer.current.setGeometry).toHaveBeenCalledTimes(1);
    const log = card.querySelector("[data-surface-log]");
    log.checked = true;
    log.dispatchEvent(new Event("change"));
    await flush();
    expect(renderer.current.setGeometry).toHaveBeenCalledTimes(2);
  });

  it("recolours for a new contrast without measuring again", async () => {
    localStorage.setItem("albis.roiSurface", JSON.stringify({ open: true }));
    const { controller, callbacks } = setup();
    controller.update();
    await flush();
    const grid = controller.grid;
    callbacks.mapValueToNorm.mockClear();
    controller.refresh();
    await flush();
    expect(controller.grid).toBe(grid);
    expect(callbacks.mapValueToNorm).toHaveBeenCalled();
    expect(renderer.current.setColors).toHaveBeenCalledTimes(2);
    expect(renderer.current.setGeometry).toHaveBeenCalledTimes(1);
  });

  it("measures again for a new frame", async () => {
    localStorage.setItem("albis.roiSurface", JSON.stringify({ open: true }));
    const { controller, state } = setup();
    controller.update();
    const first = controller.grid;
    state.dataRaw = new Float64Array(state.width * state.height).fill(2);
    controller.update();
    expect(controller.grid).not.toBe(first);
    expect(controller.grid.max).toBe(2);
  });

  it("applies the pixel mask as the image does: only while it is on", () => {
    localStorage.setItem("albis.roiSurface", JSON.stringify({ open: true }));
    const maskRaw = new Uint32Array(48);
    maskRaw[1 * 8 + 1] = 1;
    const { controller, state } = setup({ frame: { maskRaw, maskShape: [6, 8], maskAvailable: true, maskEnabled: true } });
    controller.update();
    expect(controller.grid.kinds[0]).toBe(1);
    state.maskEnabled = false;
    controller.update();
    expect(controller.grid.kinds[0]).toBe(0);
  });

  it("says what to do when there is no box, or it is too small", async () => {
    localStorage.setItem("albis.roiSurface", JSON.stringify({ open: true }));
    const { controller, roiState, card } = setup({ roi: { active: false } });
    const message = card.querySelector(".roi-surface-message");
    controller.update();
    await flush();
    expect(message.textContent).toBe("roi.surface.draw_box");
    expect(message.parentElement.classList.contains("has-message")).toBe(true);
    roiState.active = true;
    roiState.end = { x: 1, y: 5 };
    controller.update();
    await flush();
    expect(message.textContent).toBe("roi.surface.too_small");
    expect(renderer.current.draw).toHaveBeenLastCalledWith(expect.objectContaining({ empty: true }));
  });

  it("says so where the browser has no WebGL2", async () => {
    renderer.current = null;
    localStorage.setItem("albis.roiSurface", JSON.stringify({ open: true }));
    const { controller, card } = setup();
    controller.update();
    await flush();
    expect(card.querySelector(".roi-surface-message").textContent).toBe("roi.surface.no_webgl");
  });

  it("says when a large box is shown in blocks", async () => {
    localStorage.setItem("albis.roiSurface", JSON.stringify({ open: true }));
    const width = 600;
    const height = 20;
    const { controller, card } = setup({
      frame: { width, height, dataRaw: new Uint16Array(width * height) },
      roi: { start: { x: 0, y: 0 }, end: { x: width - 1, y: height - 1 } },
    });
    controller.update();
    await flush();
    expect(controller.grid.step).toBe(3);
    expect(card.querySelector(".roi-surface-note").textContent).toBe('roi.surface.blocks {"size":3}');
    // Too many cells for grid lines over the surface.
    expect(renderer.current.draw).toHaveBeenLastCalledWith(expect.objectContaining({ grid: false }));
  });

  it("turns with the arrow keys, which then do not reach the frame stepping", async () => {
    localStorage.setItem("albis.roiSurface", JSON.stringify({ open: true }));
    const { controller, card } = setup();
    controller.update();
    await flush();
    const canvas = card.querySelector("canvas");
    const before = controller.camera.yaw;
    const onWindow = vi.fn();
    window.addEventListener("keydown", onWindow);
    canvas.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowLeft", bubbles: true, cancelable: true }));
    window.removeEventListener("keydown", onWindow);
    expect(controller.camera.yaw).toBeGreaterThan(before);
    expect(onWindow).not.toHaveBeenCalled();
    canvas.dispatchEvent(new KeyboardEvent("keydown", { key: "r", bubbles: true }));
    expect(controller.camera.yaw).toBe(before);
  });

  it("exports through the ROI plots' PNG export", () => {
    const { card, callbacks } = setup();
    card.querySelector("[data-surface-png]").click();
    expect(callbacks.exportPng).toHaveBeenCalled();
  });
});
