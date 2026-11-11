import fs from "node:fs";
import path from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import { createAnalysisState } from "../modules/state.js";

// Data → Detector Geometry's Detector 2θ row: built after the beam-centre
// field (index.html is past its line budget), and then a field like the rest.
const EN = JSON.parse(fs.readFileSync(path.join(process.cwd(), "frontend", "locales", "en.json"), "utf8"));
const MASTER = {
  distanceMm: 134.25,
  pixelSizeXUm: 75,
  pixelSizeYUm: 75,
  energyEv: 12650,
  centerX: 1607.4,
  centerY: 1586.4,
};

function memoryStorage() {
  const map = new Map();
  return { getItem: (key) => (map.has(key) ? map.get(key) : null), setItem: (key, value) => map.set(key, String(value)) };
}

function buildElements() {
  document.body.innerHTML = `
    <div class="geometry-grid">
      <label class="field"><input id="cx" type="number" /><input id="cy" type="number" /></label>
      <label class="field rings-geometry-file-field"><input id="file" /></label>
    </div>
    <div id="inline"><span data-geometry-inline-values></span><span data-geometry-inline-manual></span></div>`;
  const input = () => document.createElement("input");
  const div = () => document.createElement("div");
  const overrideToggle = input();
  overrideToggle.type = "checkbox";
  return {
    sectionStateEl: div(),
    summaryChipEl: div(),
    overrideToggle,
    overrideHint: div(),
    inputs: {
      distanceMm: input(),
      pixelSizeXUm: input(),
      pixelSizeYUm: input(),
      energyEv: input(),
      centerX: document.getElementById("cx"),
      centerY: document.getElementById("cy"),
    },
    hints: { distanceMm: div(), pixelSize: div(), energyEv: div(), center: div() },
    resetButton: document.createElement("button"),
    geometryFile: document.getElementById("file"),
    geometryFileHint: div(),
    geometryBrowse: document.createElement("button"),
    geometryClear: document.createElement("button"),
    geometryStatusEl: div(),
    inlineSummaries: [document.getElementById("inline")],
  };
}

async function setup() {
  vi.resetModules();
  global.fetch = vi.fn(async () => ({ ok: true, json: async () => EN }));
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  const { createGeometryParamsController } = await import("../modules/geometry_params_controller.js");
  const analysisState = createAnalysisState();
  const onParamsChanged = vi.fn();
  const controller = createGeometryParamsController({
    apiBase: "/api",
    state: { file: "scan_master.h5", hasFrame: true },
    analysisState,
    elements: buildElements(),
    callbacks: { onParamsChanged, setSectionBadgeState: () => {}, setSummaryChip: () => {} },
    storage: memoryStorage(),
  });
  return {
    controller,
    analysisState,
    onParamsChanged,
    angle: document.getElementById("geometry-two-theta"),
    direction: document.getElementById("geometry-two-theta-direction"),
    hint: document.getElementById("geometry-two-theta-hint"),
    inline: document.querySelector("[data-geometry-inline-values]"),
  };
}

afterEach(() => {
  vi.restoreAllMocks();
  delete global.fetch;
});

describe("the Detector 2θ row", () => {
  it("is placed right after the beam-centre field, translated", async () => {
    const { angle, direction } = await setup();
    const row = angle.closest(".field");
    expect(row.previousElementSibling.contains(document.getElementById("cx"))).toBe(true);
    expect(row.textContent).toContain(EN["geometry.two_theta.label"]);
    expect([...direction.options].map((o) => o.value)).toEqual(["", "+x", "-x", "+y", "-y"]);
    expect(angle.closest(".num-stepper")).toBeNull();
  });

  it("shows a file's angle and asks for the direction before applying it", async () => {
    // An older DECTRIS master states two_theta, never which way the arm swings.
    const { controller, analysisState, angle, hint } = await setup();
    controller.setSource({ ...MASTER, twoThetaDeg: 20 }, "hdf5");

    expect(angle.value).toBe("20");
    expect(analysisState.twoThetaDeg).toBe(20);
    expect(analysisState.twoThetaDirection).toBe("");
    expect(hint.textContent).toBe(EN["geometry.two_theta.direction_needed"]);
  });

  it("applies the angle once a direction is chosen, as a manual edit", async () => {
    const { controller, analysisState, direction, hint, inline, onParamsChanged } = await setup();
    controller.setSource({ ...MASTER, twoThetaDeg: 20 }, "hdf5");
    onParamsChanged.mockClear();

    direction.value = "+x";
    direction.dispatchEvent(new Event("change"));

    expect(analysisState.twoThetaDirection).toBe("+x");
    expect(analysisState.geometryOverride.enabled).toBe(true);
    expect(onParamsChanged).toHaveBeenCalled();
    expect(hint.classList.contains("is-hidden")).toBe(true);
    expect(inline.textContent).toContain("2θ 20° toward +X (right)");
  });

  it("refuses an angle the arm cannot reach", async () => {
    const { controller, analysisState, angle, hint } = await setup();
    controller.setOverrideEnabled(true);
    angle.value = "95";
    angle.dispatchEvent(new Event("input"));

    expect(analysisState.twoThetaDeg).toBeNull();
    expect(hint.textContent).toBe(EN["validation.rings.two_theta_range"]);
  });

  it("stands aside for a file geometry that states no arm", async () => {
    const { controller, analysisState, angle, direction } = await setup();
    analysisState.ringMode = "geometry";
    analysisState.ringGeometry = { panels: [{}] };
    controller.render();

    expect(angle.disabled).toBe(true);
    expect(direction.disabled).toBe(true);
  });

  it("lets the angle of a file's own arm be corrected, about the file's axis", async () => {
    // A NeXus file that places its detector on a 2θ arm states the angle and
    // the axis. The angle shows as the file's, and the override may change it
    // when the metadata is wrong; the direction is the file's.
    const { controller, analysisState, angle, direction, inline } = await setup();
    analysisState.ringMode = "geometry";
    analysisState.ringGeometry = { panels: [{}], two_theta_deg: 20, two_theta_axis: [-1, 0, 0] };
    controller.setSource({ ...MASTER }, "hdf5");
    controller.setGeometryReference({ centerX: 985, centerY: 1419, distanceMm: 85, twoThetaDeg: 20 });

    expect(angle.disabled).toBe(false);
    expect(angle.value).toBe("20");
    expect(analysisState.twoThetaDeg).toBe(20);
    expect(direction.disabled).toBe(true);
    expect(direction.options[0].textContent).toBe(EN["geometry.two_theta.direction.from_file"]);

    controller.setOverrideEnabled(true);
    angle.value = "25";
    angle.dispatchEvent(new Event("input"));

    expect(analysisState.twoThetaDeg).toBe(25);
    expect(inline.textContent).toContain("2θ 25°");
  });

  it("has nothing to change for a geometry with no arm", async () => {
    const { controller, analysisState, angle } = await setup();
    analysisState.ringMode = "geometry";
    analysisState.ringGeometry = { panels: [{}, {}] };
    controller.render();
    expect(angle.disabled).toBe(true);
  });
});
