import fs from "node:fs";
import path from "node:path";

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { GEOMETRY_OVERRIDE_STORAGE_KEY } from "../modules/geometry_params.js";
import { createAnalysisState } from "../modules/state.js";

const EN = JSON.parse(fs.readFileSync(path.join(process.cwd(), "frontend", "locales", "en.json"), "utf8"));

const HEADER = {
  distanceMm: 200,
  pixelSizeXUm: 172,
  pixelSizeYUm: 172,
  energyEv: 12000,
  centerX: 1000,
  centerY: 1100,
};

function memoryStorage() {
  const map = new Map();
  return {
    getItem: (key) => (map.has(key) ? map.get(key) : null),
    setItem: (key, value) => map.set(key, String(value)),
    map,
  };
}

function buildElements() {
  const input = () => document.createElement("input");
  const div = () => document.createElement("div");
  const inline = document.createElement("div");
  inline.innerHTML = `
    <span data-geometry-inline-values></span>
    <span data-geometry-inline-manual class="is-hidden"></span>
    <button data-geometry-edit></button>`;
  const overrideToggle = input();
  overrideToggle.type = "checkbox";
  return {
    sectionStateEl: div(),
    summaryChipEl: div(),
    overrideToggle,
    inputs: {
      distanceMm: input(),
      pixelSizeXUm: input(),
      pixelSizeYUm: input(),
      energyEv: input(),
      centerX: input(),
      centerY: input(),
    },
    hints: { distanceMm: div(), pixelSize: div(), energyEv: div(), center: div() },
    resetButton: document.createElement("button"),
    geometryFile: input(),
    geometryFileHint: div(),
    geometryBrowse: document.createElement("button"),
    geometryClear: document.createElement("button"),
    geometryStatusEl: div(),
    inlineSummaries: [inline],
  };
}

async function setup({ storage = memoryStorage(), state = { file: "a.cbf", hasFrame: false } } = {}) {
  vi.resetModules();
  global.fetch = vi.fn(async () => ({ ok: true, json: async () => EN }));
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  const { createGeometryParamsController } = await import("../modules/geometry_params_controller.js");
  const elements = buildElements();
  const analysisState = createAnalysisState();
  const callbacks = {
    onParamsChanged: vi.fn(),
    redraw: vi.fn(),
    reloadGeometry: vi.fn(),
    revealSection: vi.fn(),
    setSectionBadgeState: (el, tone, message) => {
      el.dataset.tone = tone;
      el.textContent = message;
    },
    setSummaryChip: (el, text, tone = "default") => {
      el.dataset.tone = tone;
      el.textContent = text;
    },
  };
  const controller = createGeometryParamsController({
    apiBase: "/api",
    state,
    analysisState,
    elements,
    callbacks,
    storage,
  });
  return { controller, elements, analysisState, state, callbacks, storage };
}

function type(input, value) {
  input.value = value;
  input.dispatchEvent(new Event("input"));
}

describe("geometry_params_controller", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("notifies nothing while it is being built, yet a saved override is already in effect", async () => {
    // app.js builds this controller before the overlays exist. A notification
    // from the first recompute reached code that was not initialised yet and
    // stopped the whole app from starting.
    const storage = memoryStorage();
    storage.setItem(
      GEOMETRY_OVERRIDE_STORAGE_KEY,
      JSON.stringify({ v: 1, enabled: true, values: { distanceMm: 305 }, geometryFile: "" }),
    );
    const { analysisState, callbacks } = await setup({ storage });

    expect(callbacks.onParamsChanged).not.toHaveBeenCalled();
    expect(callbacks.redraw).not.toHaveBeenCalled();
    expect(analysisState.distanceMm).toBe(305);
  });

  it("shows the file's values read-only while the override is off", async () => {
    const { controller, elements, analysisState } = await setup();
    controller.setSource(HEADER, "image");

    expect(elements.inputs.distanceMm.readOnly).toBe(true);
    expect(elements.inputs.distanceMm.value).toBe("200");
    expect(elements.summaryChipEl.textContent).toBe("From metadata");

    type(elements.inputs.distanceMm, "305");
    expect(analysisState.distanceMm).toBe(200);
  });

  it("applies a typed value, marks it, and says what the file had", async () => {
    const { controller, elements, analysisState, storage } = await setup();
    controller.setSource(HEADER, "image");
    elements.overrideToggle.checked = true;
    elements.overrideToggle.dispatchEvent(new Event("change"));

    type(elements.inputs.distanceMm, "305");

    expect(analysisState.distanceMm).toBe(305);
    expect(elements.inputs.distanceMm.classList.contains("is-overridden")).toBe(true);
    expect(elements.hints.distanceMm.textContent).toBe("Metadata: 200");
    expect(elements.summaryChipEl.textContent).toBe("Manual");
    expect(JSON.parse(storage.getItem(GEOMETRY_OVERRIDE_STORAGE_KEY)).values.distanceMm).toBe(305);
  });

  it("keeps the override for the next file, while the rest follows that file", async () => {
    const { controller, analysisState } = await setup();
    controller.setSource(HEADER, "image");
    controller.setOverrideValues({ distanceMm: 305 });

    controller.setSource({ ...HEADER, distanceMm: 100, energyEv: 8048 }, "image");

    expect(analysisState.distanceMm).toBe(305);
    expect(analysisState.energyEv).toBe(8048);
  });

  it("supplies what a file lacks, and reports the file as incomplete without it", async () => {
    const { controller, elements, analysisState } = await setup();
    controller.setSource({ energyEv: 8048 }, "image");

    expect(elements.summaryChipEl.textContent).toBe("Incomplete");
    expect(elements.hints.distanceMm.textContent).toBe("Not in the metadata");

    controller.setOverrideValues({ distanceMm: 120, pixelSizeXUm: 75, centerX: 500, centerY: 510 });
    expect(analysisState.distanceMm).toBe(120);
    expect(analysisState.pixelSizeUm).toBe(75);
    expect(analysisState.pixelSizeYUm).toBe(75);
  });

  it("returns an emptied field to the file's value", async () => {
    const { controller, elements, analysisState } = await setup();
    controller.setSource(HEADER, "image");
    controller.setOverrideValues({ distanceMm: 305 });

    type(elements.inputs.distanceMm, "");

    expect(analysisState.distanceMm).toBe(200);
  });

  it("rejects an impossible value with a hint and keeps the previous one", async () => {
    const { controller, elements, analysisState } = await setup();
    controller.setSource(HEADER, "image");
    controller.setOverrideEnabled(true);

    type(elements.inputs.distanceMm, "-3");

    expect(analysisState.distanceMm).toBe(200);
    expect(elements.inputs.distanceMm.classList.contains("is-invalid")).toBe(true);
    expect(elements.hints.distanceMm.textContent).toBe(EN["validation.rings.distance_positive"]);
  });

  it("comes back in the next session", async () => {
    const storage = memoryStorage();
    const first = await setup({ storage });
    first.controller.setOverrideValues({ energyEv: 8048 });

    const second = await setup({ storage });
    second.controller.setSource(HEADER, "image");

    expect(second.analysisState.energyEv).toBe(8048);
    expect(second.elements.overrideToggle.checked).toBe(true);
  });

  it("applies a chosen geometry file only while the override is on", async () => {
    const { controller, callbacks } = await setup();

    controller.setGeometryFile("/data/refined.expt");
    expect(controller.getActiveGeometryFile()).toBe("/data/refined.expt");
    expect(callbacks.reloadGeometry).toHaveBeenCalledTimes(1);

    controller.setOverrideEnabled(false);
    expect(controller.getActiveGeometryFile()).toBe("");
    expect(callbacks.reloadGeometry).toHaveBeenCalledTimes(2);
  });

  it("resets to the metadata, dropping the geometry file too", async () => {
    const { controller, analysisState, callbacks } = await setup();
    controller.setSource(HEADER, "image");
    controller.setOverrideValues({ distanceMm: 305 });
    controller.setGeometryFile("/data/refined.expt");
    callbacks.reloadGeometry.mockClear();

    controller.resetOverride();

    expect(analysisState.distanceMm).toBe(200);
    expect(controller.getActiveGeometryFile()).toBe("");
    expect(callbacks.reloadGeometry).toHaveBeenCalledTimes(1);
    expect(controller.overrideInEffect()).toBe(false);
  });

  it("treats a dragged beam centre as an override", async () => {
    const { controller, analysisState, elements } = await setup();
    controller.setSource(HEADER, "image");

    controller.setBeamCenter(1080, 2595);

    expect(analysisState.centerX).toBe(1080);
    expect(analysisState.centerY).toBe(2595);
    expect(elements.overrideToggle.checked).toBe(true);
  });

  it("summarises the values in effect where the rings and peaks are, and links to the section", async () => {
    const { controller, elements, callbacks } = await setup();
    controller.setSource(HEADER, "image");
    const inline = elements.inlineSummaries[0];

    expect(inline.querySelector("[data-geometry-inline-values]").textContent).toBe(
      "200 mm · 172 µm · 12000 eV · beam 1000, 1100",
    );
    expect(inline.querySelector("[data-geometry-inline-manual]").classList.contains("is-hidden")).toBe(true);

    controller.setOverrideValues({ distanceMm: 305 });
    expect(inline.querySelector("[data-geometry-inline-manual]").classList.contains("is-hidden")).toBe(false);

    inline.querySelector("[data-geometry-edit]").click();
    expect(callbacks.revealSection).toHaveBeenCalledTimes(1);
  });

  it("redraws the image when a pixel size changes its aspect", async () => {
    const { controller, state, callbacks } = await setup({ state: { file: "a.h5", hasFrame: true } });
    controller.setSource({ ...HEADER, pixelSizeXUm: 75, pixelSizeYUm: 75 }, "hdf5");
    callbacks.redraw.mockClear();

    controller.setOverrideValues({ pixelSizeYUm: 225 });

    expect(state.pixelAspect).toBe(3);
    expect(callbacks.redraw).toHaveBeenCalledTimes(1);
  });

  it("follows an override changed in another window", async () => {
    const { controller, analysisState, storage } = await setup();
    controller.setSource(HEADER, "image");

    storage.setItem(
      GEOMETRY_OVERRIDE_STORAGE_KEY,
      JSON.stringify({ v: 1, enabled: true, values: { distanceMm: 410 }, geometryFile: "" }),
    );
    window.dispatchEvent(new StorageEvent("storage", { key: GEOMETRY_OVERRIDE_STORAGE_KEY }));

    expect(analysisState.distanceMm).toBe(410);
  });
});
