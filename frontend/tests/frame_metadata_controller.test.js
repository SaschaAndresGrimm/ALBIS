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
        scheduleResolutionOverlay: () => {},
      },
    });

    await controller.loadAnalysisParams();

    expect(loadImageGeometry).toHaveBeenCalledWith("sum_0001.h5", "file:sum_0001.h5");
    expect(analysisState.distanceMm).toBe(240);
    expect(analysisState.centerX).toBe(1080);
    expect(ringsCenterY.value).toBe("2603");
  });
});
