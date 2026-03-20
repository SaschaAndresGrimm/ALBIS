import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { getGeometryReferencePose, prepareRingGeometry } from "../modules/ring_geometry_utils.js";

function buildFetchMock() {
  return vi.fn(async () => ({
    ok: true,
    json: async () => ({
      "common.ready": "Ready",
      "rings.geometry.status_auto": "Auto geometry: {{source}}.",
      "rings.geometry.status_manual": "Manual geometry: {{source}}.",
    }),
  }));
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
});
