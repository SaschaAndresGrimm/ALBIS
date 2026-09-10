import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

function buildFetchMock() {
  return vi.fn(async () => ({
    ok: true,
    json: async () => ({
      "status.frame.loading_metadata": "Loading metadata",
      "status.data.loading_dataset_metadata": "Loading dataset metadata",
      "status.data.metadata_ready": "Metadata ready",
      "status.frame.loading_frame": "Loading frame",
    }),
  }));
}

describe("frame_metadata_controller", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("loads geometry for HDF5 files using the current scope key", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock();
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createFrameMetadataController } = await import("../modules/frame_metadata_controller.js");

    const analysisState = {
      rings: [1, 3.67, 11.01],
      distanceMm: null,
      pixelSizeUm: null,
      energyEv: null,
      centerX: null,
      centerY: null,
    };
    const loadImageGeometry = vi.fn(async () => {});
    const ringsDistance = document.createElement("input");
    const ringsPixel = document.createElement("input");
    const ringsEnergy = document.createElement("input");
    const ringsCenterX = document.createElement("input");
    const ringsCenterY = document.createElement("input");
    const ringInputs = [document.createElement("input"), document.createElement("input"), document.createElement("input")];

    const controller = createFrameMetadataController({
      apiBase: "/api",
      state: {
        autoload: { dir: "" },
        file: "sum_0001.h5",
        dataset: "/entry/data/data",
        seriesFiles: [],
      },
      analysisState,
      elements: {
        autoloadDir: null,
        autoloadDirList: null,
        fileSelect: null,
        metaShape: null,
        metaDtype: null,
        ringsDistance,
        ringsPixel,
        ringsEnergy,
        ringsCenterX,
        ringsCenterY,
        ringInputs,
      },
      callbacks: {
        fetchJSON: vi.fn(async () => ({
          distance_mm: 240,
          pixel_size_um: 172,
          energy_ev: 7118,
          center_x_px: 1080,
          center_y_px: 2603,
        })),
        option: () => document.createElement("option"),
        fileLabel: (value) => String(value || ""),
        setDataControlsForHdf5: () => {},
        setDataSourceSectionState: () => {},
        setStatus: () => {},
        stopPlayback: () => {},
        updateToolbar: () => {},
        showSplash: () => {},
        setSplashStatus: () => {},
        setLoading: () => {},
        showProcessingProgress: () => {},
        hideProcessingProgress: () => {},
        getDefaultThresholdIndex: () => 0,
        syncSeriesSumOutputPath: () => {},
        updateFrameControls: () => {},
        updateThresholdOptions: () => {},
        loadMask: async () => {},
        loadFrame: async () => {},
        isHdf5File: () => true,
        getDefaultCenter: () => ({ x: 0, y: 0 }),
        loadImageGeometry,
        resetTransientFrameLoadState: () => {},
        scheduleResolutionOverlay: () => {},
      },
    });

    await controller.loadAnalysisParams();

    expect(loadImageGeometry).toHaveBeenCalledWith("sum_0001.h5", "file:sum_0001.h5");
    expect(analysisState.distanceMm).toBe(240);
    expect(analysisState.centerX).toBe(1080);
    expect(ringsCenterY.value).toBe("2603");
  });

  it("stops playback and clears transient frame-load state before loading dataset metadata", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock();
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createFrameMetadataController } = await import("../modules/frame_metadata_controller.js");

    const state = {
      autoload: { dir: "" },
      file: "scan_0001.h5",
      dataset: "/entry/data/data",
      seriesFiles: [],
      pendingFrame: 7,
      isLoading: true,
      playing: true,
      maskAuto: false,
      frameIndex: 5,
    };
    const stopPlayback = vi.fn(() => {
      state.playing = false;
    });
    const resetTransientFrameLoadState = vi.fn(() => {
      state.pendingFrame = null;
      state.isLoading = false;
    });
    const loadFrame = vi.fn(async () => {
      expect(state.playing).toBe(false);
      expect(state.pendingFrame).toBeNull();
      expect(state.isLoading).toBe(false);
      expect(state.frameIndex).toBe(0);
      return true;
    });

    const controller = createFrameMetadataController({
      apiBase: "/api",
      state,
      analysisState: {
        rings: [],
      },
      elements: {
        autoloadDir: null,
        autoloadDirList: null,
        fileSelect: null,
        metaShape: document.createElement("div"),
        metaDtype: document.createElement("div"),
        ringsDistance: null,
        ringsPixel: null,
        ringsEnergy: null,
        ringsCenterX: null,
        ringsCenterY: null,
        ringInputs: [],
      },
      callbacks: {
        fetchJSON: vi.fn(async (url) => {
          if (String(url).includes("/metadata?")) {
            return {
              shape: [12, 2167, 2070],
              dtype: "uint32",
            };
          }
          return {};
        }),
        option: () => document.createElement("option"),
        fileLabel: (value) => String(value || ""),
        setDataControlsForHdf5: () => {},
        setDataSourceSectionState: () => {},
        setStatus: () => {},
        stopPlayback,
        updateToolbar: () => {},
        showSplash: () => {},
        setSplashStatus: () => {},
        setLoading: () => {},
        showProcessingProgress: () => {},
        hideProcessingProgress: () => {},
        getDefaultThresholdIndex: () => 0,
        syncSeriesSumOutputPath: () => {},
        updateFrameControls: () => {},
        updateThresholdOptions: () => {},
        loadMask: async () => {},
        loadFrame,
        isHdf5File: () => true,
        getDefaultCenter: () => ({ x: 0, y: 0 }),
        loadImageGeometry: async () => {},
        resetTransientFrameLoadState,
        scheduleResolutionOverlay: () => {},
      },
    });

    const loaded = await controller.loadMetadata();

    expect(loaded).toBe(true);
    expect(stopPlayback).toHaveBeenCalledTimes(1);
    expect(resetTransientFrameLoadState).toHaveBeenCalledTimes(1);
    expect(loadFrame).toHaveBeenCalledTimes(1);
  });

  it("does not reset transient frame-load state during the initial hdf5 metadata load", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock();
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createFrameMetadataController } = await import("../modules/frame_metadata_controller.js");

    const resetTransientFrameLoadState = vi.fn(() => {});
    const stopPlayback = vi.fn(() => {});

    const controller = createFrameMetadataController({
      apiBase: "/api",
      state: {
        autoload: { dir: "" },
        file: "initial_open.h5",
        dataset: "/entry/data",
        seriesFiles: [],
        hasFrame: false,
        pendingFrame: null,
        isLoading: false,
        playing: false,
      },
      analysisState: {
        rings: [],
      },
      elements: {
        autoloadDir: null,
        autoloadDirList: null,
        fileSelect: null,
        metaShape: document.createElement("div"),
        metaDtype: document.createElement("div"),
        ringsDistance: null,
        ringsPixel: null,
        ringsEnergy: null,
        ringsCenterX: null,
        ringsCenterY: null,
        ringInputs: [],
      },
      callbacks: {
        fetchJSON: vi.fn(async (url) => {
          if (String(url).includes("/metadata?")) {
            return {
              shape: [300, 2180, 2073],
              dtype: "uint32",
            };
          }
          return {};
        }),
        option: () => document.createElement("option"),
        fileLabel: (value) => String(value || ""),
        setDataControlsForHdf5: () => {},
        setDataSourceSectionState: () => {},
        setStatus: () => {},
        stopPlayback,
        updateToolbar: () => {},
        showSplash: () => {},
        setSplashStatus: () => {},
        setLoading: () => {},
        showProcessingProgress: () => {},
        hideProcessingProgress: () => {},
        getDefaultThresholdIndex: () => 0,
        syncSeriesSumOutputPath: () => {},
        updateFrameControls: () => {},
        updateThresholdOptions: () => {},
        loadMask: async () => {},
        loadFrame: async () => true,
        isHdf5File: () => true,
        getDefaultCenter: () => ({ x: 0, y: 0 }),
        loadImageGeometry: async () => {},
        resetTransientFrameLoadState,
        scheduleResolutionOverlay: () => {},
      },
    });

    const loaded = await controller.loadMetadata();

    expect(loaded).toBe(true);
    expect(stopPlayback).not.toHaveBeenCalled();
    expect(resetTransientFrameLoadState).not.toHaveBeenCalled();
  });


  it("clears the loading state and reports the failure when metadata cannot be read", async () => {
    // setLoading(true) was switched on before the fetch and only ever switched
    // off on the success path, so a dataset the backend cannot read left the
    // spinner and "Loading metadata…" on screen for the rest of the session.
    // The catch also rethrew, and the dataset dropdown awaits loadMetadata()
    // with no try/catch, so it became an unhandled rejection as well.
    vi.resetModules();
    global.fetch = buildFetchMock();
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createFrameMetadataController } = await import("../modules/frame_metadata_controller.js");

    const setLoading = vi.fn();
    const setStatus = vi.fn();
    const setDataSourceSectionState = vi.fn();

    const controller = createFrameMetadataController({
      apiBase: "/api",
      state: {
        autoload: { dir: "" },
        file: "broken.h5",
        dataset: "/entry/data/data",
        seriesFiles: [],
      },
      analysisState: { rings: [], distanceMm: null, pixelSizeUm: null, energyEv: null, centerX: null, centerY: null },
      elements: {
        autoloadDir: null, autoloadDirList: null, fileSelect: null,
        metaShape: document.createElement("span"),
        metaDtype: document.createElement("span"),
        ringsDistance: document.createElement("input"),
        ringsPixel: document.createElement("input"),
        ringsEnergy: document.createElement("input"),
        ringsCenterX: document.createElement("input"),
        ringsCenterY: document.createElement("input"),
        ringInputs: [],
      },
      callbacks: {
        fetchJSON: vi.fn(async () => { throw new Error("422 Cannot read data"); }),
        option: () => document.createElement("option"),
        fileLabel: (value) => String(value || ""),
        setDataControlsForHdf5: () => {},
        setDataSourceSectionState,
        setStatus,
        stopPlayback: () => {},
        updateToolbar: () => {},
        showSplash: () => {},
        setSplashStatus: () => {},
        setLoading,
        showProcessingProgress: () => {},
        hideProcessingProgress: () => {},
        getDefaultThresholdIndex: () => 0,
        syncSeriesSumOutputPath: () => {},
        updateFrameControls: () => {},
        updateThresholdOptions: () => {},
        loadMask: async () => {},
        loadFrame: async () => {},
        isHdf5File: () => true,
        getDefaultCenter: () => ({ x: 0, y: 0 }),
        loadImageGeometry: async () => {},
        resetTransientFrameLoadState: () => {},
        scheduleResolutionOverlay: () => {},
      },
    });

    // Must not reject: the dataset dropdown awaits this without a catch.
    const result = await controller.loadMetadata();

    expect(result).toBe(false);
    expect(setLoading).toHaveBeenCalledWith(false);
    // Toned, so it is raised as a toast instead of only landing in the footer
    // pill where the next ambient update overwrites it.
    const toned = setStatus.mock.calls.find(([, opts]) => opts && opts.tone === "error");
    expect(toned, "the failure was never reported with an error tone").toBeTruthy();
    expect(setDataSourceSectionState).toHaveBeenCalledWith("warning", expect.anything());
  });
});
