import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

function buildFetchMock() {
  return vi.fn(async () => ({
    ok: true,
    json: async () => ({
      "series.progress.idle": "Idle",
      "series.progress.submitting": "Submitting",
      "series.progress.queued": "Queued",
      "series.progress.running": "Running",
      "series.progress.click_to_open": "Click to open",
      "series.progress.click_to_open_inline": "click to open",
      "series.button.summing": "Summing",
      "series.button.cancel": "Cancel",
      "series.button.start": "Start",
      "status.series.started": "Started",
      "status.series.done": "Done",
    }),
  }));
}

function createController({ geometryContext = null } = {}) {
  const payloads = [];
  const state = {
    file: "scan_0001.cbf",
    dataset: "",
    frameCount: 2,
    shape: [2, 2],
    width: 2,
    height: 2,
    thresholdCount: 1,
    seriesSum: {
      running: false,
      cancelling: false,
      jobId: "",
      outputs: [],
      openTarget: "",
      progress: 0,
      message: "",
      autoOutputPath: "",
    },
  };
  const elements = {
    seriesSumMode: Object.assign(document.createElement("input"), { value: "all" }),
    seriesSumOperation: Object.assign(document.createElement("input"), { value: "sum" }),
    seriesSumStepField: null,
    seriesSumStepLabel: null,
    seriesSumStep: Object.assign(document.createElement("input"), { value: "1" }),
    seriesSumRangeStartField: null,
    seriesSumRangeEndField: null,
    seriesSumRangeStart: Object.assign(document.createElement("input"), { value: "1" }),
    seriesSumRangeEnd: Object.assign(document.createElement("input"), { value: "2" }),
    seriesSumNormalizeMethod: Object.assign(document.createElement("input"), { value: "none" }),
    seriesSumNormalizeFrameField: null,
    seriesSumNormalizeFrame: Object.assign(document.createElement("input"), { value: "1" }),
    seriesSumNormalizeScalarField: null,
    seriesSumNormalizeScalar: Object.assign(document.createElement("input"), { value: "1" }),
    seriesSumNormalizeImageField: null,
    seriesSumNormalizeImage: Object.assign(document.createElement("input"), { value: "" }),
    seriesSumNormalizeImageBrowse: null,
    seriesSumMedianEstimate: null,
    seriesSumOutput: Object.assign(document.createElement("input"), { value: "output/series_sum" }),
    seriesSumBrowse: null,
    seriesSumFormat: Object.assign(document.createElement("input"), { value: "hdf5" }),
    seriesSumMask: Object.assign(document.createElement("input"), { checked: false }),
    seriesSumStart: document.createElement("button"),
    seriesSumCancel: document.createElement("button"),
    seriesSumProgress: document.createElement("div"),
    seriesSumProgressFill: document.createElement("div"),
    seriesSumProgressText: document.createElement("div"),
  };
  const callbacks = {
    isHdfFile: () => false,
    validateSeriesStepInput: () => 1,
    setStatus: vi.fn(),
    ensureFileMode: async () => {},
    loadAutoloadFile: async () => {},
    fetchJSON: vi.fn(async () => ({ status: "done", progress: 1, outputs: [] })),
    fetchJSONWithInit: vi.fn(async (_url, init) => {
      payloads.push(JSON.parse(init.body));
      return { job_id: "job-1" };
    }),
    getSeriesSumGeometryContext: () => geometryContext,
  };
  return { state, elements, callbacks, payloads };
}

describe("series_sum_controller", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("includes effective geometry payload when geometry mode is active", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock();
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createSeriesSumController } = await import("../modules/series_sum_controller.js");

    const geometryContext = {
      geometry: {
        mode: "geometry",
        detector: "pilatus-12m-dls-cshape",
        source: "P12M_geometry/imported.expt",
        panels: [
          {
            name: "row-00",
            origin_mm: [-184.9, -245.0, 250.13],
            fast_axis: [1, 0, 0],
            slow_axis: [0, 0, 1],
            pixel_size_mm: [0.172, 0.172],
            image_size_px: [2463, 195],
            raw_offset_px: [3.4, -5.2],
          },
        ],
      },
      distanceMm: 250.13,
      pixelSizeUm: 172,
      energyEv: 7118,
      centerX: 1083.9,
      centerY: 2593.48,
    };
    const { state, elements, callbacks, payloads } = createController({ geometryContext });
    const controller = createSeriesSumController({
      apiBase: "/api",
      state,
      elements,
      callbacks,
    });

    await controller.startSeriesSumming();

    expect(payloads).toHaveLength(1);
    expect(payloads[0].geometry).toEqual(geometryContext.geometry);
    expect(payloads[0].distance_mm).toBe(250.13);
    expect(payloads[0].center_x_px).toBe(1083.9);
    expect(payloads[0].center_y_px).toBe(2593.48);
  });

  it("omits geometry fields for planar datasets", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock();
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createSeriesSumController } = await import("../modules/series_sum_controller.js");

    const { state, elements, callbacks, payloads } = createController();
    const controller = createSeriesSumController({
      apiBase: "/api",
      state,
      elements,
      callbacks,
    });

    await controller.startSeriesSumming();

    expect(payloads).toHaveLength(1);
    expect("geometry" in payloads[0]).toBe(false);
    expect("distance_mm" in payloads[0]).toBe(false);
  });
});
