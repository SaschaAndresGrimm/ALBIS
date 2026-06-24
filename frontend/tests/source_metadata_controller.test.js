import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { getGeometryReferencePose, prepareRingGeometry } from "../modules/ring_geometry_utils.js";

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

function buildLiveControllerElements() {
  const make = () => document.createElement("input");
  return {
    ringsDistance: make(),
    ringsPixel: make(),
    ringsEnergy: make(),
    ringsCenterX: make(),
    ringsCenterY: make(),
    ringsGeometryFile: make(),
    ringsGeometryFileHint: document.createElement("div"),
    ringsGeometryBrowse: document.createElement("button"),
    ringsGeometryClear: document.createElement("button"),
    ringsGeometryStatusEl: document.createElement("div"),
    ringsGeometryLockEl: document.createElement("div"),
    ringsGeometryLockLabel: document.createElement("span"),
    ringsGeometryLockReset: document.createElement("button"),
  };
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

  it("seeds manual override geometry from the reference pose for HDF5 inputs", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock();
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createSourceMetadataController } = await import("../modules/source_metadata_controller.js");

    const ringsDistance = document.createElement("input");
    const ringsPixel = document.createElement("input");
    const ringsEnergy = document.createElement("input");
    const ringsCenterX = document.createElement("input");
    const ringsCenterY = document.createElement("input");
    const ringsGeometryFile = document.createElement("input");
    const ringsGeometryFileHint = document.createElement("div");
    const ringsGeometryBrowse = document.createElement("button");
    const ringsGeometryClear = document.createElement("button");
    const ringsGeometryStatusEl = document.createElement("div");
    const payload = createGapGeometryPayload();
    const reference = getGeometryReferencePose(prepareRingGeometry(payload));

    const analysisState = {
      ringMode: "planar",
      ringGeometry: null,
      ringGeometrySource: "",
      ringGeometryKey: "",
      geometryOverridePath: "12m_data_albis/P12M_geometry/imported.expt",
      geometryOverrideScopeKey: "file:sum_0001.h5",
      geometryOverrideActive: false,
      geometryManualKey: "",
      geometryDistanceManual: false,
      geometryCenterXManual: false,
      geometryCenterYManual: false,
      distanceMm: 0,
      pixelSizeUm: 172,
      energyEv: 7118,
      centerX: 1219.7,
      centerY: 2538.5,
    };

    const controller = createSourceMetadataController({
      state: {
        file: "sum_0001.h5",
        seriesFiles: [],
        autoload: { mode: "file" },
      },
      analysisState,
      elements: {
        simplonMetaPanel: null,
        simplonSeriesEl: null,
        simplonImageEl: null,
        simplonTimeEl: null,
        simplonEnergyEl: null,
        simplonThresholdEl: null,
        simplonWavelengthEl: null,
        simplonDistanceEl: null,
        simplonCenterEl: null,
        remoteMetaPanel: null,
        remoteSourceEl: null,
        remoteSeqEl: null,
        remoteSeriesEl: null,
        remoteImageEl: null,
        remoteTimeEl: null,
        remoteEnergyEl: null,
        remoteWavelengthEl: null,
        remoteDistanceEl: null,
        remoteCenterEl: null,
        remotePeakSetsEl: null,
        jfjochMetaPanel: null,
        jfjochSourceEl: null,
        jfjochSeqEl: null,
        jfjochSeriesEl: null,
        jfjochImageEl: null,
        jfjochTimeEl: null,
        jfjochReflectionsEl: null,
        jfjochChannelMetaEl: null,
        jfjochBridgeStatusEl: null,
        ringsDistance,
        ringsPixel,
        ringsEnergy,
        ringsCenterX,
        ringsCenterY,
        ringsGeometryFile,
        ringsGeometryFileHint,
        ringsGeometryBrowse,
        ringsGeometryClear,
        ringsGeometryStatusEl,
      },
      callbacks: {
        scheduleResolutionOverlay: () => {},
      },
    });

    controller.applyImageGeometry(
      payload,
      "file:sum_0001.h5|geometry:12m_data_albis/P12M_geometry/imported.expt",
      { overrideActive: true },
    );

    expect(reference).toBeTruthy();
    expect(analysisState.distanceMm).toBeCloseTo(reference.distanceMm, 6);
    expect(analysisState.centerX).toBeCloseTo(reference.centerX, 6);
    expect(analysisState.centerY).toBeCloseTo(reference.centerY, 6);
    expect(ringsDistance.value).toBe(String(reference.distanceMm));
    expect(ringsGeometryStatusEl.textContent).toContain("Manual geometry");
  });

  it("honours the geometry lock for live SIMPLON metadata and reset restores live values", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock();
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createSourceMetadataController } = await import("../modules/source_metadata_controller.js");

    const elements = buildLiveControllerElements();
    const analysisState = {
      ringMode: "planar",
      ringGeometry: null,
      ringGeometryKey: "",
      geometryManualKey: "",
      geometryDistanceManual: false,
      geometryCenterXManual: false,
      geometryCenterYManual: false,
      geometryLocked: false,
      geometryLockKey: "",
      distanceMm: null,
      pixelSizeUm: null,
      energyEv: null,
      centerX: null,
      centerY: null,
      externalPeakSets: [],
    };
    const state = {
      file: "",
      seriesFiles: [],
      autoload: { mode: "simplon", running: true, simplonUrl: "http://det:80", simplonMeta: {} },
    };

    const controller = createSourceMetadataController({
      state,
      analysisState,
      elements,
      callbacks: { scheduleResolutionOverlay: () => {} },
    });

    // First frame seeds geometry from headers.
    controller.applySimplonMeta(
      buildHeaders({
        "X-Simplon-DetectorDistance-MM": "250",
        "X-Simplon-Energy-Ev": "12000",
        "X-Simplon-BeamCenter-X": "1000",
        "X-Simplon-BeamCenter-Y": "1100",
      }),
    );
    expect(analysisState.distanceMm).toBeCloseTo(250, 6);
    expect(elements.ringsGeometryLockEl.classList.contains("is-hidden")).toBe(false);
    expect(elements.ringsGeometryLockEl.classList.contains("is-locked")).toBe(false);

    // User corrects + locks geometry for the active source scope.
    analysisState.distanceMm = 305;
    analysisState.geometryLocked = true;
    analysisState.geometryLockKey = "simplon:http://det:80";

    // Next frame must NOT overwrite the corrected distance.
    controller.applySimplonMeta(
      buildHeaders({
        "X-Simplon-DetectorDistance-MM": "250",
        "X-Simplon-BeamCenter-X": "1000",
        "X-Simplon-BeamCenter-Y": "1100",
      }),
    );
    expect(analysisState.distanceMm).toBe(305);
    expect(elements.ringsGeometryLockEl.classList.contains("is-locked")).toBe(true);

    // Reset clears the lock and snaps back to the latest live metadata.
    controller.resetGeometryLock();
    expect(analysisState.geometryLocked).toBe(false);
    expect(analysisState.distanceMm).toBeCloseTo(250, 6);
    expect(elements.ringsGeometryLockEl.classList.contains("is-locked")).toBe(false);
  });
});
