import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe("file_open_flow", () => {
  beforeEach(() => {
    vi.resetModules();
    document.body.innerHTML = "";
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("deduplicates concurrent native picker requests", async () => {
    const fetchPending = deferred();
    global.fetch = vi.fn(() => fetchPending.promise);

    const { createFileOpenController } = await import("../modules/file_open_flow.js");

    const autoloadDir = document.createElement("input");
    const fileSelect = document.createElement("select");
    const fileInput = document.createElement("input");
    const loadFiles = vi.fn(async () => {});
    const loadImageSeries = vi.fn(async () => {});

    const controller = createFileOpenController({
      apiBase: "/api",
      state: {
        autoload: {},
        frameIndex: 0,
      },
      getBackendIsLocal: () => true,
      elements: {
        autoloadDir,
        fileSelect,
        fileInput,
        filesystemMode: null,
      },
      callbacks: {
        dirnameFromPath: (value) => String(value).replace(/\/[^/]+$/, ""),
        syncSeriesSumOutputPath: vi.fn(),
        loadFiles,
        option: (label, value) => {
          const opt = document.createElement("option");
          opt.textContent = label;
          opt.value = value;
          return opt;
        },
        fileLabel: (value) => String(value),
        isHdfFile: () => false,
        loadDatasets: vi.fn(async () => {}),
        loadImageSeries,
        closeMenu: vi.fn(),
        ensureFileMode: vi.fn(async () => {}),
        setStatus: vi.fn(),
        openFileDialog: vi.fn(async () => null),
      },
    });

    const first = controller.openFileModal();
    const second = controller.openFileModal();

    await vi.waitFor(() => {
      expect(global.fetch).toHaveBeenCalledTimes(1);
    });

    fetchPending.resolve({
      status: 200,
      ok: true,
      json: async () => ({ path: "/tmp/example/frame_0001.cbf" }),
    });

    await Promise.all([first, second]);

    expect(loadFiles).toHaveBeenCalledTimes(1);
    expect(loadImageSeries).toHaveBeenCalledTimes(1);
    expect(fileSelect.value).toBe("/tmp/example/frame_0001.cbf");
    expect(autoloadDir.value).toBe("/tmp/example");
  });
});
