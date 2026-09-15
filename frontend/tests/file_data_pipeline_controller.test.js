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

// A controller whose dataset scan always fails with the given error shape.
// Only the reporting callbacks matter here, so the rest are stubs.
async function makeScanFailureController({ detail, detailData, setStatus }) {
  const { createFileDataPipelineController } = await import(
    "../modules/file_data_pipeline_controller.js"
  );
  const refusal = new Error("Request failed (422)");
  refusal.status = 422;
  if (detail) refusal.detail = detail;
  if (detailData) refusal.detailData = detailData;

  return createFileDataPipelineController({
    apiBase: "/api",
    state: {
      file: "master.h5",
      dataset: "",
      seriesFiles: [],
      hasFrame: false,
      isLoading: false,
      playing: false,
      pendingFrame: null,
      maskRaw: null,
      maskFile: "",
    },
    elements: {
      fileSelect: document.createElement("select"),
      datasetSelect: document.createElement("select"),
      metaShape: document.createElement("div"),
      metaDtype: document.createElement("div"),
    },
    callbacks: {
      fetchJSON: vi.fn(async () => {
        throw refusal;
      }),
      option: (label, value) => {
        const opt = document.createElement("option");
        opt.value = value;
        return opt;
      },
      fileLabel: (value) => String(value || ""),
      isSeriesCapable: () => false,
      isHdfFile: () => true,
      setDataControlsForHdf5: vi.fn(),
      setDataControlsForSeries: vi.fn(),
      loadMetadata: vi.fn(async () => true),
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
      setStatus,
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

describe("file_data_pipeline_controller frame cache", () => {
  async function setup({ frameCacheMb = 256, autoload = {} } = {}) {
    vi.resetModules();
    const requested = [];
    global.fetch = vi.fn((url) => {
      const text = String(url);
      const match = text.match(/locales\/([^/]+)\.json/);
      if (match) return Promise.resolve({ ok: true, json: async () => ({}) });
      if (!text.includes("/frame?")) throw new Error(`Unexpected fetch: ${text}`);
      requested.push(text);
      return Promise.resolve({
        ok: true,
        headers: {
          get: (name) => ({ "X-Dtype": "<u2", "X-Shape": "8,8" })[name] ?? null,
        },
        arrayBuffer: async () => new ArrayBuffer(128),
      });
    });

    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createFileDataPipelineController } = await import(
      "../modules/file_data_pipeline_controller.js"
    );

    const state = {
      file: "run.h5",
      dataset: "/entry/data/data",
      frameCount: 20,
      frameIndex: 0,
      thresholdCount: 1,
      thresholdIndex: 0,
      seriesFiles: [],
      seriesLabel: "",
      hasFrame: false,
      isLoading: false,
      playing: false,
      pendingFrame: null,
      maskRaw: null,
      maskFile: "",
      frameCacheMb,
      autoload: { watchEnabled: false, running: false, ...autoload },
    };

    const applyFrame = vi.fn();
    const setLoading = vi.fn();
    const controller = createFileDataPipelineController({
      apiBase: "/api",
      state,
      elements: {
        fileSelect: document.createElement("select"),
        datasetSelect: document.createElement("select"),
        metaShape: document.createElement("div"),
        metaDtype: document.createElement("div"),
      },
      callbacks: {
        fetchJSON: vi.fn(async () => ({ datasets: [] })),
        option: () => document.createElement("option"),
        fileLabel: (value) => String(value || ""),
        isSeriesCapable: () => false,
        isHdfFile: () => true,
        setDataControlsForHdf5: vi.fn(),
        setDataControlsForSeries: vi.fn(),
        loadMetadata: vi.fn(async () => true),
        loadImageGeometry: vi.fn(async () => {}),
        loadInspectorRoot: vi.fn(async () => {}),
        updateFrameControls: vi.fn(),
        updatePlayButtons: vi.fn(),
        requestFrame: vi.fn(),
        parseDtype: (header) => header,
        parseShape: (header) => String(header).split(",").map(Number),
        typedArrayFrom: (buffer) => new Uint16Array(buffer),
        applyImageMeta: vi.fn(),
        applyExternalFrame: vi.fn(),
        applyFrame,
        processPendingFrameRequest: vi.fn(),
        currentFrameStatusText: () => "Ready",
        setLoading,
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

    const visit = async (index) => {
      state.frameIndex = index;
      return controller.loadFrame();
    };

    return { controller, state, requested, applyFrame, setLoading, visit };
  }

  it("does not refetch a frame it has already loaded", async () => {
    const { visit, requested } = await setup();

    await visit(3);
    await visit(4);
    await visit(3);

    expect(requested).toHaveLength(2);
    expect(requested.filter((url) => url.includes("index=3"))).toHaveLength(1);
  });

  it("still displays the frame on a cache hit", async () => {
    const { visit, applyFrame } = await setup();

    await visit(2);
    const fromNetwork = applyFrame.mock.calls[0];
    await visit(5);
    await visit(2);

    expect(applyFrame).toHaveBeenCalledTimes(3);
    const fromCache = applyFrame.mock.calls[2];
    expect(fromCache[1]).toBe(fromNetwork[1]); // width
    expect(fromCache[2]).toBe(fromNetwork[2]); // height
    expect(fromCache[3]).toBe(fromNetwork[3]); // dtype
    expect(fromCache[0]).toBe(fromNetwork[0]); // same buffer, not a copy
  });

  it("shows no loading spinner for a cache hit", async () => {
    const { visit, setLoading } = await setup();

    await visit(1);
    setLoading.mockClear();
    await visit(1);

    expect(setLoading.mock.calls.every(([value]) => value === false)).toBe(true);
  });

  it("keys on the threshold so multi-threshold data cannot cross over", async () => {
    // Getting this wrong would silently display the wrong energy channel, which
    // looks like corrupt data rather than a bug.
    const { visit, state, requested } = await setup();
    state.thresholdCount = 2;

    state.thresholdIndex = 0;
    await visit(7);
    state.thresholdIndex = 1;
    await visit(7);

    expect(requested).toHaveLength(2);
    expect(requested[0]).toContain("threshold=0");
    expect(requested[1]).toContain("threshold=1");
  });

  it("keys on the dataset so switching datasets cannot serve the wrong image", async () => {
    const { visit, state, requested } = await setup();

    await visit(4);
    state.dataset = "/entry/data/other";
    await visit(4);

    expect(requested).toHaveLength(2);
  });

  it("never caches while autoload is running", async () => {
    // The file may still be growing under the filewriter, so a frame read before
    // it was fully flushed must not be pinned for the rest of the session.
    const { visit, requested } = await setup({ autoload: { running: true } });

    await visit(2);
    await visit(2);
    await visit(2);

    expect(requested).toHaveLength(3);
  });

  it("never caches while an autoload watch is armed", async () => {
    const { visit, requested } = await setup({ autoload: { watchEnabled: true } });

    await visit(2);
    await visit(2);

    expect(requested).toHaveLength(2);
  });

  it("refetches every frame when the cache is disabled", async () => {
    const { visit, requested } = await setup({ frameCacheMb: 0 });

    await visit(6);
    await visit(6);

    expect(requested).toHaveLength(2);
  });

  it("drops cached frames when the data source changes", async () => {
    const { visit, controller, requested } = await setup();

    await visit(8);
    controller.resetTransientLoadState();
    await visit(8);

    expect(requested).toHaveLength(2);
  });

  it("says a refused dataset scan in the user's own language", async () => {
    // A master file whose data files are missing is answered with a 422 code
    // plus counts, which this composes into the active locale. "Failed to scan
    // datasets" is what the user used to get, and it is exactly the message
    // that made an incomplete download look like a corrupt file.
    vi.resetModules();
    global.fetch = buildFetchMock({
      en: {
        "status.data.failed_scan_datasets": "Failed to scan datasets",
        "error.master_data_missing":
          "{{count}} linked data files are missing{{example}}. Copy them into this folder.",
        "error.master_data_example_suffix": " (e.g. {{name}})",
      },
    });
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createFileDataPipelineController } = await import(
      "../modules/file_data_pipeline_controller.js"
    );

    const EXPECTED =
      "5,000,000 linked data files are missing (e.g. tem_burnin_data_000001.h5). " +
      "Copy them into this folder.";
    const refusal = new Error("Request failed (422)");
    refusal.status = 422;
    refusal.detail = "the server's English, which must not be what is shown";
    refusal.detailData = {
      code: "master_data_missing",
      group: "/entry/data",
      count: 5000000,
      example: "tem_burnin_data_000001.h5",
      unreadable: 0,
    };

    const state = {
      file: "tem_burnin_master.h5",
      dataset: "",
      seriesFiles: [],
      hasFrame: false,
      isLoading: false,
      playing: false,
      pendingFrame: null,
      maskRaw: null,
      maskFile: "",
    };
    const setStatus = vi.fn();
    const setSplashStatus = vi.fn();
    const setDataSourceSectionState = vi.fn();

    const controller = createFileDataPipelineController({
      apiBase: "/api",
      state,
      elements: {
        fileSelect: document.createElement("select"),
        datasetSelect: document.createElement("select"),
        metaShape: document.createElement("div"),
        metaDtype: document.createElement("div"),
      },
      callbacks: {
        fetchJSON: vi.fn(async () => {
          throw refusal;
        }),
        option: (label, value) => {
          const opt = document.createElement("option");
          opt.textContent = label;
          opt.value = value;
          return opt;
        },
        fileLabel: (value) => String(value || ""),
        isSeriesCapable: () => false,
        isHdfFile: () => true,
        setDataControlsForHdf5: vi.fn(),
        setDataControlsForSeries: vi.fn(),
        loadMetadata: vi.fn(async () => true),
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
        setStatus,
        showSplash: vi.fn(),
        setSplashStatus,
        setDataSourceSectionState,
        showProcessingProgress: vi.fn(),
        hideProcessingProgress: vi.fn(),
        stopPlayback: vi.fn(),
        loadMask: vi.fn(async () => {}),
        updateToolbar: vi.fn(),
      },
    });

    vi.spyOn(console, "error").mockImplementation(() => {});
    await expect(controller.loadDatasets()).resolves.toBe(false);

    // Localized and short enough to read on the splash, and the count carries
    // the locale's own grouping rather than the server's.
    expect(setStatus).toHaveBeenCalledWith(EXPECTED, { tone: "error" });
    // Free text, not an i18n key: `setSplashStatus` renders it verbatim. The
    // explicit `busy: false` stops the loading spinner, which the English word
    // list in the splash controller cannot infer from a translated sentence.
    expect(setSplashStatus).toHaveBeenCalledWith(EXPECTED, {}, { busy: false });
    // The badge stays short and localized -- a whole sentence does not fit it.
    expect(setDataSourceSectionState).toHaveBeenLastCalledWith(
      "warning",
      "Failed to scan datasets",
    );
  });

  it("falls back to the localized message when the server gives no reason", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock({
      en: { "status.data.failed_scan_datasets": "Failed to scan datasets" },
    });
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createFileDataPipelineController } = await import(
      "../modules/file_data_pipeline_controller.js"
    );

    const setStatus = vi.fn();
    const setSplashStatus = vi.fn();
    const controller = createFileDataPipelineController({
      apiBase: "/api",
      state: {
        file: "x.h5",
        dataset: "",
        seriesFiles: [],
        hasFrame: false,
        isLoading: false,
        playing: false,
        pendingFrame: null,
        maskRaw: null,
        maskFile: "",
      },
      elements: {
        fileSelect: document.createElement("select"),
        datasetSelect: document.createElement("select"),
        metaShape: document.createElement("div"),
        metaDtype: document.createElement("div"),
      },
      callbacks: {
        fetchJSON: vi.fn(async () => {
          throw new Error("network down");
        }),
        option: (label, value) => {
          const opt = document.createElement("option");
          opt.value = value;
          return opt;
        },
        fileLabel: (value) => String(value || ""),
        isSeriesCapable: () => false,
        isHdfFile: () => true,
        setDataControlsForHdf5: vi.fn(),
        setDataControlsForSeries: vi.fn(),
        loadMetadata: vi.fn(async () => true),
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
        setStatus,
        showSplash: vi.fn(),
        setSplashStatus,
        setDataSourceSectionState: vi.fn(),
        showProcessingProgress: vi.fn(),
        hideProcessingProgress: vi.fn(),
        stopPlayback: vi.fn(),
        loadMask: vi.fn(async () => {}),
        updateToolbar: vi.fn(),
      },
    });

    vi.spyOn(console, "error").mockImplementation(() => {});
    await expect(controller.loadDatasets()).resolves.toBe(false);

    expect(setStatus).toHaveBeenCalledWith("Failed to scan datasets", { tone: "error" });
    expect(setSplashStatus).toHaveBeenCalledWith(
      "splash.status.dataset_scan_failed",
      {},
      { busy: false },
    );
  });

  it("omits the example clause when the server names no file", async () => {
    // A group of broken soft links reaches the 422 with a count but no
    // filename. The sentence has to still read as a sentence.
    vi.resetModules();
    global.fetch = buildFetchMock({
      en: {
        "status.data.failed_scan_datasets": "Failed to scan datasets",
        "error.master_data_missing":
          "{{count}} linked data files are missing{{example}}. Copy them into this folder.",
        "error.master_data_example_suffix": " (e.g. {{name}})",
      },
    });
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });

    const setStatus = vi.fn();
    const controller = await makeScanFailureController({
      detailData: { code: "master_data_missing", count: 12, example: "", unreadable: 0 },
      setStatus,
    });

    vi.spyOn(console, "error").mockImplementation(() => {});
    await controller.loadDatasets();

    expect(setStatus).toHaveBeenCalledWith(
      "12 linked data files are missing. Copy them into this folder.",
      { tone: "error" },
    );
  });

  it("shows the server's English for a code it does not know", async () => {
    // The map is explicit, so a code added server-side degrades to the
    // sentence the server already wrote rather than rendering a raw key.
    vi.resetModules();
    global.fetch = buildFetchMock({
      en: { "status.data.failed_scan_datasets": "Failed to scan datasets" },
    });
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });

    const setStatus = vi.fn();
    const controller = await makeScanFailureController({
      detail: "Something new the server explains in English",
      detailData: { code: "some_future_code", count: 3 },
      setStatus,
    });

    vi.spyOn(console, "error").mockImplementation(() => {});
    await controller.loadDatasets();

    expect(setStatus).toHaveBeenCalledWith("Something new the server explains in English", {
      tone: "error",
    });
  });
});

