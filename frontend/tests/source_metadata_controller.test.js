import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { getGeometryReferencePose, prepareRingGeometry } from "../modules/ring_geometry_utils.js";
import { createAnalysisState } from "../modules/state.js";

function buildFetchMock() {
  return vi.fn(async () => ({
    ok: true,
    json: async () => ({
      "common.ready": "Ready",
      "rings.geometry.status_auto": "Auto geometry: {{source}}.",
      "rings.geometry.status_manual": "Manual geometry: {{source}}.",
      "rings.lock.live": "Live geometry",
      "rings.lock.locked": "Geometry locked",
    }),
  }));
}

function buildHeaders(map) {
  return { get: (key) => (key in map ? map[key] : null) };
}

const NO_META_ELEMENTS = {
  simplonMetaPanel: null,
  remoteMetaPanel: null,
  jfjochMetaPanel: null,
};

async function setup({ state, analysisState }) {
  vi.resetModules();
  global.fetch = buildFetchMock();
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  const { createSourceMetadataController } = await import("../modules/source_metadata_controller.js");
  const { createGeometryParamsController } = await import("../modules/geometry_params_controller.js");
  const geometryStatusEl = document.createElement("div");
  const geometryParams = createGeometryParamsController({
    apiBase: "/api",
    state,
    analysisState,
    elements: { geometryStatusEl },
    callbacks: {},
  });
  const controller = createSourceMetadataController({
    state,
    analysisState,
    elements: NO_META_ELEMENTS,
    callbacks: { geometryParams },
  });
  return { controller, geometryParams, geometryStatusEl };
}

function createGapGeometryPayload() {
  return {
    mode: "geometry",
    detector: "test-gap",
    source: "P12M_geometry/imported.expt",
    panels: [
      {
        name: "upper",
        origin_mm: [-60, -80, 100],
        fast_axis: [1, 0, 0],
        slow_axis: [0, 1, 0],
        pixel_size_mm: [1, 1],
        image_size_px: [120, 50],
        raw_offset_px: [0, 0],
      },
      {
        name: "lower",
        origin_mm: [-60, 30, 100],
        fast_axis: [1, 0, 0],
        slow_axis: [0, 1, 0],
        pixel_size_mm: [1, 1],
        image_size_px: [120, 50],
        raw_offset_px: [0, 70],
      },
    ],
  };
}

describe("source_metadata_controller", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("takes the pose from a geometry file the user chose, over the file's metadata", async () => {
    // A hand-loaded .expt is a calibration: its distance and beam centre win
    // over what the HDF5 file states, as they did before the override moved.
    const state = { file: "sum_0001.h5", seriesFiles: [], autoload: { mode: "file" } };
    const analysisState = createAnalysisState();
    const { controller, geometryParams, geometryStatusEl } = await setup({ state, analysisState });
    const payload = createGapGeometryPayload();
    const reference = getGeometryReferencePose(prepareRingGeometry(payload));

    geometryParams.setSource(
      { distanceMm: 0, pixelSizeXUm: 172, pixelSizeYUm: 172, energyEv: 7118, centerX: 1219.7, centerY: 2538.5 },
      "hdf5",
    );
    controller.applyImageGeometry(payload, "file:sum_0001.h5|geometry:refined.expt", { overrideActive: true });

    expect(reference).toBeTruthy();
    expect(analysisState.distanceMm).toBeCloseTo(reference.distanceMm, 6);
    expect(analysisState.centerX).toBeCloseTo(reference.centerX, 6);
    expect(analysisState.centerY).toBeCloseTo(reference.centerY, 6);
    expect(analysisState.energyEv).toBe(7118);
    expect(geometryStatusEl.textContent).toContain("Manual geometry");
  });

  it("fills only what the metadata lacks from a geometry ALBIS found by itself", async () => {
    const state = { file: "thau_00001.cbf", seriesFiles: [], autoload: { mode: "file" } };
    const analysisState = createAnalysisState();
    const { controller, geometryParams } = await setup({ state, analysisState });
    const payload = createGapGeometryPayload();
    const reference = getGeometryReferencePose(prepareRingGeometry(payload));

    geometryParams.setSource({ distanceMm: 260.13, energyEv: 4500 }, "image");
    controller.applyImageGeometry(payload, "file:thau_00001.cbf");

    expect(analysisState.distanceMm).toBe(260.13);
    expect(analysisState.centerX).toBeCloseTo(reference.centerX, 6);
    expect(analysisState.centerY).toBeCloseTo(reference.centerY, 6);
  });

  it("keeps an override across live SIMPLON frames, and switching it off resumes the stream", async () => {
    const state = {
      file: "",
      seriesFiles: [],
      autoload: { mode: "simplon", running: true, simplonUrl: "http://det:80", simplonMeta: {} },
    };
    const analysisState = createAnalysisState();
    const { controller, geometryParams } = await setup({ state, analysisState });
    const frame = buildHeaders({
      "X-Simplon-DetectorDistance-MM": "250",
      "X-Simplon-Energy-Ev": "12000",
      "X-Simplon-BeamCenter-X": "1000",
      "X-Simplon-BeamCenter-Y": "1100",
    });

    controller.applySimplonMeta(frame);
    expect(analysisState.distanceMm).toBe(250);

    geometryParams.setOverrideValues({ distanceMm: 305 });
    controller.applySimplonMeta(frame);
    expect(analysisState.distanceMm).toBe(305);
    expect(analysisState.energyEv).toBe(12000);

    geometryParams.setOverrideEnabled(false);
    expect(analysisState.distanceMm).toBe(250);
  });

  it("keeps a live stream's earlier value for a field a frame does not carry", async () => {
    const state = { file: "", seriesFiles: [], autoload: { mode: "simplon", running: true, simplonMeta: {} } };
    const analysisState = createAnalysisState();
    const { controller } = await setup({ state, analysisState });

    controller.applySimplonMeta(buildHeaders({ "X-Simplon-Energy-Ev": "12000", "X-Simplon-DetectorDistance-MM": "250" }));
    controller.applySimplonMeta(buildHeaders({ "X-Simplon-DetectorDistance-MM": "251" }));

    expect(analysisState.distanceMm).toBe(251);
    expect(analysisState.energyEv).toBe(12000);
  });

  it("replaces a previous file's metadata entirely rather than keeping what the next lacks", async () => {
    const state = { file: "a.cbf", seriesFiles: [], autoload: { mode: "file" } };
    const analysisState = createAnalysisState();
    const { controller } = await setup({ state, analysisState });

    controller.applyImageMeta(
      buildHeaders({ "X-Image-DetectorDistance-MM": "200", "X-Image-PixelSize-UM": "172", "X-Image-Energy-Ev": "12000" }),
    );
    controller.applyImageMeta(buildHeaders({ "X-Image-Energy-Ev": "8048" }));

    expect(analysisState.energyEv).toBe(8048);
    expect(analysisState.distanceMm).toBeNull();
    expect(analysisState.pixelSizeUm).toBeNull();
  });
});
