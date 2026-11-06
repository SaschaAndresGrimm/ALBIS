import { afterEach, describe, expect, it, vi } from "vitest";

import { createFileDataPipelineController } from "../modules/file_data_pipeline_controller.js";

// The splash covers the viewer until the first image arrives, so while that
// image loads the splash itself has to say so -- and must not keep saying it
// once the load is over.

const LOADING = "splash.status.loading_file";
const READY = "splash.status.ready_open_file";

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function imageResponse() {
  return {
    ok: true,
    status: 200,
    headers: new Headers({ "X-Dtype": "uint8", "X-Shape": "1,1" }),
    arrayBuffer: async () => new ArrayBuffer(1),
  };
}

function makeController({ hasFrame = false, file = "", fetchJSON = vi.fn() } = {}) {
  const state = {
    file,
    dataset: "",
    seriesFiles: [],
    hasFrame,
    isLoading: false,
    playing: false,
    pendingFrame: null,
    maskRaw: null,
    maskFile: "",
  };
  const setSplashStatus = vi.fn();
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
      fetchJSON,
      option: (label, value) => {
        const opt = document.createElement("option");
        opt.value = value;
        return opt;
      },
      fileLabel: (value) => String(value || ""),
      isSeriesCapable: () => false,
      isHdfFile: (name) => String(name).endsWith(".h5"),
      setDataControlsForHdf5: vi.fn(),
      setDataControlsForSeries: vi.fn(),
      loadImageGeometry: vi.fn(async () => {}),
      updateFrameControls: vi.fn(),
      updatePlayButtons: vi.fn(),
      requestFrame: vi.fn(),
      parseDtype: () => "uint8",
      parseShape: () => [1, 1],
      typedArrayFrom: (buffer) => new Uint8Array(buffer),
      applyImageMeta: vi.fn(),
      processPendingFrameRequest: vi.fn(),
      currentFrameStatusText: vi.fn(() => "Ready"),
      setLoading: vi.fn(),
      setStatus: vi.fn(),
      showSplash: vi.fn(),
      setSplashStatus,
      setDataSourceSectionState: vi.fn(),
      showProcessingProgress: vi.fn(),
      hideProcessingProgress: vi.fn(),
    },
    core: {
      inspectorPanel: { loadInspectorRoot: vi.fn(async () => {}) },
      chromeToolbar: { updateToolbar: vi.fn() },
      frameMetadata: { loadMetadata: vi.fn(async () => false) },
      maskCursor: { loadMask: vi.fn(async () => {}) },
      // The real one sets hasFrame and hides the splash.
      fileSession: {
        applyExternalFrame: vi.fn(() => {
          state.hasFrame = true;
        }),
      },
      framePlayback: { stopPlayback: vi.fn() },
    },
  });
  const splashKeys = () => setSplashStatus.mock.calls.map(([key]) => key);
  return { controller, state, setSplashStatus, splashKeys };
}

describe("file_data_pipeline_controller first load splash", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("says the first file is loading while it loads", async () => {
    const image = deferred();
    global.fetch = vi.fn(() => image.promise);
    const { controller, splashKeys } = makeController();

    const loading = controller.loadImageSeries("frame_0001.cbf");
    expect(splashKeys()).toEqual([LOADING]);

    image.resolve(imageResponse());
    await expect(loading).resolves.toBe(true);
    // Success hides the splash; nothing is written to it afterwards.
    expect(splashKeys()).toEqual([LOADING]);
  });

  it("leaves the splash alone when an image is already on screen", async () => {
    global.fetch = vi.fn(async () => imageResponse());
    const { controller, setSplashStatus } = makeController({ hasFrame: true });

    await controller.loadImageSeries("frame_0002.cbf");

    expect(setSplashStatus).not.toHaveBeenCalled();
  });

  it("gives the splash back its Open file line when the first file fails", async () => {
    global.fetch = vi.fn(async () => {
      throw new TypeError("Failed to fetch");
    });
    const { controller, splashKeys } = makeController();

    await expect(controller.loadImageSeries("broken.cbf")).resolves.toBe(false);

    expect(splashKeys()).toEqual([LOADING, READY]);
  });

  it("keeps a failure's own splash line rather than overwriting it", async () => {
    const fetchJSON = vi.fn(async () => {
      throw Object.assign(new Error("Request failed (500)"), { status: 500 });
    });
    const { controller, splashKeys } = makeController({ file: "master.h5", fetchJSON });

    await expect(controller.loadDatasets()).resolves.toBe(false);

    expect(splashKeys()).toEqual([LOADING, "splash.status.dataset_scan_failed"]);
  });

  it("does not stop the spinner when the load that ended was not the latest", async () => {
    const first = deferred();
    const second = deferred();
    global.fetch = vi.fn().mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise);
    const { controller, splashKeys } = makeController();

    const older = controller.loadImageSeries("a.cbf");
    const newer = controller.loadImageSeries("b.cbf");
    first.reject(new TypeError("Failed to fetch"));
    await older;

    expect(splashKeys()).toEqual([LOADING, LOADING]);

    second.resolve(imageResponse());
    await expect(newer).resolves.toBe(true);
  });

  it("does nothing when there is no file to load", async () => {
    const { controller, setSplashStatus } = makeController();

    await controller.loadImageSeries("");
    await controller.loadDatasets();

    expect(setSplashStatus).not.toHaveBeenCalled();
  });
});
