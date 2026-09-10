import fs from "node:fs";
import path from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import { OPAQUE_OVERLAY_RGB } from "../modules/overlay_painters.js";
import { SATURATED_PIXEL_RGBA } from "../modules/viewer_overlay_colors.js";

// The real English catalogue, so a renamed or deleted status key fails here
// instead of silently degrading to a raw key in the UI.
const EN = JSON.parse(
  fs.readFileSync(path.join(process.cwd(), "frontend", "locales", "en.json"), "utf8")
);

function makeState(overrides = {}) {
  return {
    file: "/data/single_frame.cbf",
    dataset: "",
    seriesFiles: [],
    frameCount: 1,
    fps: 5,
    width: 64,
    height: 64,
    animationExport: { running: false, cancelling: false },
    ...overrides,
  };
}

async function createController(state) {
  vi.resetModules();
  global.fetch = vi.fn(async () => ({ ok: true, json: async () => EN }));
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  const { createAnimationExportController } = await import(
    "../modules/animation_export_controller.js"
  );
  const setStatus = vi.fn();
  const openModal = vi.fn();
  const controller = createAnimationExportController({
    apiBase: "/api",
    state,
    elements: {},
    callbacks: { setStatus, openModal, closeModal: vi.fn() },
  });
  return { controller, setStatus, openModal };
}

describe("animation_export_controller", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.doUnmock("../modules/gif_encoder.js");
    delete global.fetch;
  });

  it("refuses a single frame with a toast rather than silently", async () => {
    const { controller, setStatus, openModal } = await createController(makeState());
    controller.openDialog();
    expect(openModal).not.toHaveBeenCalled();
    expect(setStatus).toHaveBeenCalledWith(EN["status.animation_export.not_series"], {
      tone: "warning",
    });
  });

  it("refuses with no file open with a toast rather than silently", async () => {
    const { controller, setStatus, openModal } = await createController(makeState({ file: "" }));
    controller.openDialog();
    expect(openModal).not.toHaveBeenCalled();
    expect(setStatus).toHaveBeenCalledWith(EN["status.animation_export.no_file"], {
      tone: "warning",
    });
  });

  it("opens the dialog for an exportable series without a warning", async () => {
    const { controller, setStatus, openModal } = await createController(
      makeState({ file: "/data/series_0001.cbf", seriesFiles: ["a.cbf", "b.cbf"], frameCount: 2 })
    );
    controller.openDialog();
    expect(openModal).toHaveBeenCalledTimes(1);
    expect(setStatus).not.toHaveBeenCalled();
  });
});

/* ---------------------------------------------------------------- overlays */

const FRAME_W = 8;
const FRAME_H = 8;

function seriesState(overrides = {}) {
  return makeState({
    file: "/data/series_0001.cbf",
    seriesFiles: ["a.cbf", "b.cbf", "c.cbf"],
    frameCount: 3,
    width: FRAME_W,
    height: FRAME_H,
    colormap: "grayscale",
    pixelAspect: 1,
    ...overrides,
  });
}

function checkbox(checked) {
  const el = document.createElement("input");
  el.type = "checkbox";
  el.checked = checked;
  return el;
}

/**
 * A canvas whose 2D context records nothing and hands back one opaque white
 * pixel at the top left. That is enough to prove the overlay reached the
 * palette indices, without pulling a real canvas into jsdom.
 */
function stubOverlayCanvas(ow, oh) {
  const rgba = new Uint8ClampedArray(ow * oh * 4);
  rgba[0] = 255;
  rgba[1] = 255;
  rgba[2] = 255;
  rgba[3] = 255;
  const noop = () => {};
  const ctx = new Proxy(
    { getImageData: () => ({ data: rgba }), measureText: () => ({ width: 10 }) },
    { get: (target, key) => (key in target ? target[key] : noop), set: () => true }
  );
  return { width: ow, height: oh, getContext: () => ctx };
}

async function runExport({ overlays, overlayState, detectPeaksInFrame } = {}) {
  vi.resetModules();
  const encoded = { palette: null, frames: [], size: null };
  vi.doMock("../modules/gif_encoder.js", () => ({
    GifWriter: class {
      constructor({ width, height, palette }) {
        encoded.size = { width, height };
        encoded.palette = palette;
      }
      // The controller reuses one index buffer across frames, so copy.
      addFrame(indices) {
        encoded.frames.push(Uint8Array.from(indices));
      }
      finish() {
        return new Uint8Array([0x47]);
      }
    },
  }));

  const catalogue = { ...EN };
  global.fetch = vi.fn(async (url) => {
    if (String(url).includes("locales")) return { ok: true, json: async () => catalogue };
    // Each frame carries its own index in every pixel, so a marker derived
    // from the frame's own data identifies which frame produced it.
    const which = String(url).includes("b.cbf") ? 1 : String(url).includes("c.cbf") ? 2 : 0;
    const data = new Uint8Array(FRAME_W * FRAME_H).fill(which);
    return {
      ok: true,
      json: async () => catalogue,
      arrayBuffer: async () => data.buffer,
      headers: { get: (name) => (name === "X-Dtype" ? "uint8" : `${FRAME_H},${FRAME_W}`) },
    };
  });

  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  const { createAnimationExportController } = await import(
    "../modules/animation_export_controller.js"
  );

  const state = seriesState();
  const overlaysCheckbox = checkbox(Boolean(overlays));
  const detect = detectPeaksInFrame || vi.fn((frame) => [{ x: frame.data[0], y: 1 }]);
  const controller = createAnimationExportController({
    apiBase: "/api",
    state,
    elements: { overlaysCheckbox, overlaysField: document.createElement("label") },
    callbacks: {
      buildPalette: () => new Uint8Array(256 * 4).fill(200),
      getPaletteColorCount: () => 256,
      mapValueToNorm: (v) => v / 255,
      getActiveSaturationMax: () => null,
      isSaturatedValue: () => false,
      parseDtype: () => "uint8",
      parseShape: () => [FRAME_H, FRAME_W],
      typedArrayFrom: (buffer) => new Uint8Array(buffer),
      getOverlayState: () =>
        overlayState || {
          ringsEnabled: false,
          ringParams: null,
          peaksEnabled: true,
          pixelAspect: 1,
        },
      detectPeaksInFrame: detect,
      createOverlayCanvas: stubOverlayCanvas,
      openModal: vi.fn(),
      closeModal: vi.fn(),
      setStatus: vi.fn(),
    },
  });

  // Take the save-picker path browsers actually use; the anchor fallback
  // navigates, which jsdom only warns about.
  const written = [];
  window.showSaveFilePicker = vi.fn(async () => ({
    name: "export.gif",
    createWritable: async () => ({
      write: (blob) => written.push(blob),
      close: async () => {},
    }),
  }));
  controller.openDialog();
  await controller.startExport();
  return { encoded, detect, controller, overlaysCheckbox, state, written };
}

describe("GIF export with overlays", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.doUnmock("../modules/gif_encoder.js");
    delete global.fetch;
    delete window.showSaveFilePicker;
  });

  it("writes the file it encoded", async () => {
    const { written } = await runExport({ overlays: true });
    expect(written).toHaveLength(1);
    expect(written[0].type).toBe("image/gif");
  });

  it("keeps the colour table inside the 256 entries a GIF allows", async () => {
    const off = await runExport({ overlays: false });
    const on = await runExport({ overlays: true });
    expect(off.encoded.palette.length / 3).toBeLessThanOrEqual(256);
    expect(on.encoded.palette.length / 3).toBeLessThanOrEqual(256);
    // Overlay entries come out of the colormap's share, not out of thin air.
    expect(on.encoded.palette.length).toBe(off.encoded.palette.length);
  });

  it("puts the reserved colours where the frame writer expects them", async () => {
    const steps = 252 - OPAQUE_OVERLAY_RGB.length;
    const { encoded } = await runExport({ overlays: true });
    const at = (index) => Array.from(encoded.palette.slice(index * 3, index * 3 + 3));
    expect(at(steps + 2)).toEqual(Array.from(SATURATED_PIXEL_RGBA.byte.slice(0, 3)));
    OPAQUE_OVERLAY_RGB.forEach((rgb, i) => {
      expect(at(steps + 3 + i)).toEqual(rgb);
    });
  });

  it("keeps the full colormap ramp when overlays are off", async () => {
    const { encoded } = await runExport({ overlays: false });
    const at = (index) => Array.from(encoded.palette.slice(index * 3, index * 3 + 3));
    expect(at(252 + 2)).toEqual(Array.from(SATURATED_PIXEL_RGBA.byte.slice(0, 3)));
  });

  it("stamps the overlay into the exported pixels", async () => {
    const steps = 252 - OPAQUE_OVERLAY_RGB.length;
    const on = await runExport({ overlays: true });
    const off = await runExport({ overlays: false });
    // The stub paints one opaque white pixel; white is overlay colour 0.
    expect(on.encoded.frames[0][0]).toBe(steps + 3);
    expect(on.encoded.frames[0][1]).not.toBe(steps + 3);
    expect(off.encoded.frames[0][0]).not.toBe(steps + 3);
  });

  it("finds the spots of the frame being exported, not the one on screen", async () => {
    const { detect, encoded } = await runExport({ overlays: true });
    expect(encoded.frames).toHaveLength(3);
    expect(detect).toHaveBeenCalledTimes(3);
    // Each frame's pixels are filled with its own ordinal (see the fetch stub).
    expect(detect.mock.calls.map(([frame]) => frame.data[0])).toEqual([0, 1, 2]);
  });

  it("runs the spot finder once when only rings are exported", async () => {
    const detect = vi.fn(() => []);
    const { encoded } = await runExport({
      overlays: true,
      detectPeaksInFrame: detect,
      overlayState: {
        ringsEnabled: true,
        ringParams: { mode: "circle", rings: [], energyEv: 0 },
        peaksEnabled: false,
        pixelAspect: 1,
      },
    });
    expect(encoded.frames).toHaveLength(3);
    expect(detect).not.toHaveBeenCalled();
  });

  it("greys the checkbox out, and unticks it, when there is nothing to overlay", async () => {
    const { overlaysCheckbox } = await runExport({
      overlays: true,
      overlayState: {
        ringsEnabled: false,
        ringParams: null,
        peaksEnabled: false,
        pixelAspect: 1,
      },
    });
    expect(overlaysCheckbox.disabled).toBe(true);
    expect(overlaysCheckbox.checked).toBe(false);
  });
});
