import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

function buildFetchMock(dictionaries, handlers = {}) {
  return vi.fn((url, init = {}) => {
    const text = String(url);
    const match = text.match(/locales\/([^/]+)\.json/);
    if (match) {
      const language = decodeURIComponent(match[1]);
      return Promise.resolve({
        ok: true,
        json: async () => dictionaries[language] || {},
      });
    }
    if (handlers.frame && text.includes("/frame?")) {
      return handlers.frame(url, init);
    }
    throw new Error(`Unexpected fetch: ${text}`);
  });
}

describe("file_data_pipeline_controller", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("aborts in-flight frame loads and clears stale pending state before opening a new hdf5 file", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock({
      en: {
        "status.data.scanning_datasets": "Scanning datasets",
        "status.data.dataset_metadata_loaded": "Dataset metadata loaded",
        "status.data.loading_frame": "Loading frame",
      },
    }, {
      frame: (_, init) =>
        new Promise((resolve, reject) => {
          init.signal?.addEventListener("abort", () => {
            const abortError = new Error("Aborted");
            abortError.name = "AbortError";
            reject(abortError);
          });
        }),
    });
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createFileDataPipelineController } = await import(
      "../modules/file_data_pipeline_controller.js"
    );

    const state = {
      file: "source_0001.h5",
      dataset: "/entry/data/data",
      frameCount: 12,
      frameIndex: 7,
      thresholdCount: 1,
      thresholdIndex: 0,
      seriesFiles: [],
      seriesLabel: "",
      hasFrame: true,
      isLoading: false,
      playing: false,
      pendingFrame: null,
      maskRaw: null,
      maskFile: "",
    };
    const fileSelect = document.createElement("select");
    const datasetSelect = document.createElement("select");
    const metaShape = document.createElement("div");
    const metaDtype = document.createElement("div");
    const loadMetadata = vi.fn(async () => {
      expect(state.pendingFrame).toBeNull();
      expect(state.isLoading).toBe(false);
      state.frameCount = 1;
      return true;
    });

    const controller = createFileDataPipelineController({
      apiBase: "/api",
      state,
      elements: {
        fileSelect,
        datasetSelect,
        metaShape,
        metaDtype,
      },
      callbacks: {
        fetchJSON: vi.fn(async () => ({
          datasets: [
            {
              path: "/entry/data/data",
              image: true,
              size: 6,
              shape: [1, 2, 3],
            },
          ],
        })),
        option: (label, value) => {
          const opt = document.createElement("option");
          opt.textContent = label;
          opt.value = value;
          return opt;
        },
        fileLabel: (value) => String(value || ""),
        isSeriesCapable: () => false,
        isHdfFile: (file) => String(file || "").endsWith(".h5"),
        setDataControlsForHdf5: vi.fn(),
        setDataControlsForSeries: vi.fn(),
        loadMetadata,
        loadImageGeometry: vi.fn(async () => {}),
        loadInspectorRoot: vi.fn(async () => {}),
        updateFrameControls: vi.fn(),
        updatePlayButtons: vi.fn(),
        requestFrame: vi.fn(),
        parseDtype: vi.fn(),
        parseShape: vi.fn(),
        typedArrayFrom: vi.fn(),
        applyImageMeta: vi.fn(),
        applyExternalFrame: vi.fn(),
        processPendingFrameRequest: vi.fn(),
        currentFrameStatusText: vi.fn(() => "Ready"),
        setLoading: vi.fn(),
        setStatus: vi.fn(),
        showSplash: vi.fn(),
        setSplashStatus: vi.fn(),
        setDataSourceSectionState: vi.fn(),
        showProcessingProgress: vi.fn(),
        hideProcessingProgress: vi.fn(),
        stopPlayback: vi.fn(),
        loadMask: vi.fn(async () => {}),
        updateToolbar: vi.fn(),
      },
    });

    const inFlightLoad = controller.loadFrame();
    expect(controller.isFrameLoading()).toBe(true);
    controller.queuePendingFrame(11);
    const loaded = await controller.loadAutoloadFile("sum_output.h5");

    expect(loaded).toBe(true);
    await expect(inFlightLoad).resolves.toBe(false);
    expect(loadMetadata).toHaveBeenCalledTimes(1);
    expect(state.file).toBe("sum_output.h5");
    expect(state.dataset).toBe("/entry/data/data");
    expect(state.pendingFrame).toBeNull();
    expect(controller.isFrameLoading()).toBe(false);
  });
});
