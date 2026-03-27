import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

function buildFetchMock(dictionaries) {
  return vi.fn(async (url) => {
    const text = String(url);
    const match = text.match(/locales\/([^/]+)\.json/);
    if (!match) {
      throw new Error(`Unexpected fetch: ${text}`);
    }
    const language = decodeURIComponent(match[1]);
    return {
      ok: true,
      json: async () => dictionaries[language] || {},
    };
  });
}

describe("file_session_controller", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("clears transient frame-load state when closing the current file", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock({
      en: {
        "status.file.no_file_loaded": "No file loaded",
        "splash.status.ready_open_file": "Ready to open file",
      },
    });
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createFileSessionController } = await import("../modules/file_session_controller.js");

    const state = {
      file: "scan_0001.h5",
      dataset: "/entry/data/data",
      shape: [12, 2167, 2070],
      dtype: "uint32",
      frameCount: 12,
      frameIndex: 5,
      thresholdCount: 2,
      thresholdIndex: 1,
      thresholdEnergies: [7118, 9000],
      dataRaw: new Uint32Array([1, 2, 3]),
      dataFloat: new Float32Array([1, 2, 3]),
      histogram: { bins: [] },
      stats: { min: 1, max: 3 },
      hasFrame: true,
      pendingFrame: 8,
      isLoading: true,
      playing: true,
      panOffsetX: 5,
      panOffsetY: 6,
      renderOffsetX: 7,
      renderOffsetY: 8,
      globalStats: { min: 1, max: 3 },
    };
    const analysisState = {
      peaks: [{ x: 1, y: 2 }],
      selectedPeaks: [{ x: 1, y: 2 }],
      peakSelectionAnchor: { x: 1, y: 2 },
      ringMode: "refined",
      ringGeometry: { panels: [] },
      ringGeometrySource: "test",
      ringGeometryKey: "scan",
      geometryOverridePath: "override.json",
      geometryOverrideScopeKey: "file:scan_0001.h5",
      geometryOverrideActive: true,
      geometryManualKey: "manual",
      geometryDistanceManual: true,
      geometryCenterXManual: true,
      geometryCenterYManual: true,
    };
    const stopPlayback = vi.fn(() => {
      state.playing = false;
    });
    const resetTransientFrameLoadState = vi.fn(() => {
      state.pendingFrame = null;
      state.isLoading = false;
    });

    const controller = createFileSessionController({
      state,
      analysisState,
      elements: {
        fileSelect: document.createElement("select"),
        datasetSelect: document.createElement("select"),
        minInput: document.createElement("input"),
        maxInput: document.createElement("input"),
        metaShape: document.createElement("div"),
        metaDtype: document.createElement("div"),
        metaRange: document.createElement("div"),
        canvas: null,
      },
      callbacks: {
        stopPlayback,
        resetTransientFrameLoadState,
        clearImageGeometry: vi.fn(),
        clearMaskState: vi.fn(),
        clearImageHeader: vi.fn(),
        updateToolbar: vi.fn(),
        setDataSourceSectionState: vi.fn(),
        setStatus: vi.fn(),
        setLoading: vi.fn(),
        hideUploadProgress: vi.fn(),
        hideProcessingProgress: vi.fn(),
        showSplash: vi.fn(),
        setSplashStatus: vi.fn(),
        updateInspectorHeaderVisibility: vi.fn(),
        updateFrameControls: vi.fn(),
        updateThresholdOptions: vi.fn(),
        applyCanvasTransform: vi.fn(),
        updatePanCapability: vi.fn(),
        clearHistogram: vi.fn(),
        renderPeakList: vi.fn(),
        schedulePeakOverlay: vi.fn(),
        syncSeriesSumOutputPath: vi.fn(),
        clearRoi: vi.fn(),
        updateRingsSectionState: vi.fn(),
        updatePeaksSectionState: vi.fn(),
        updatePlayButtons: vi.fn(),
        option: () => document.createElement("option"),
        setDataControlsForImage: vi.fn(),
        setDataControlsForSeries: vi.fn(),
        buildNegativeMask: vi.fn(),
        updateMaskUI: vi.fn(),
        getRenderer: () => null,
        isWebglUnsignedRawCandidate: vi.fn(() => false),
        toFloat32: vi.fn((data) => data),
        computeStats: vi.fn(() => ({ min: 1, max: 3 })),
        updateGlobalStats: vi.fn(),
        computeAutoLevels: vi.fn(() => ({ min: 1, max: 3 })),
        formatValue: vi.fn((value) => String(value)),
        alignMaskToFrame: vi.fn(),
        syncMaskAvailability: vi.fn(),
        redraw: vi.fn(),
        fitImageToView: vi.fn(),
        hideSplash: vi.fn(),
        scheduleOverview: vi.fn(),
        scheduleRoiUpdate: vi.fn(),
        schedulePixelOverlay: vi.fn(),
        scheduleResolutionOverlay: vi.fn(),
        schedulePeakFinder: vi.fn(),
        scheduleHistogram: vi.fn(),
      },
    });

    controller.closeCurrentFile();

    expect(stopPlayback).toHaveBeenCalledTimes(1);
    expect(resetTransientFrameLoadState).toHaveBeenCalledTimes(1);
    expect(state.pendingFrame).toBeNull();
    expect(state.isLoading).toBe(false);
    expect(state.file).toBe("");
    expect(state.dataset).toBe("");
    expect(state.frameCount).toBe(1);
    expect(state.thresholdCount).toBe(1);
  });
});
