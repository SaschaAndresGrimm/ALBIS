import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

function buildFetchMock(dictionaries, routeHandler) {
  return vi.fn(async (url, init) => {
    const text = String(url);
    const match = text.match(/locales\/([^/]+)\.json/);
    if (match) {
      const language = decodeURIComponent(match[1]);
      return {
        ok: true,
        json: async () => dictionaries[language] || {},
      };
    }
    return routeHandler(text, init);
  });
}

describe("autoload_mode_controller", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("does not advance the watched file cursor when loading the new file fails", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock(
      {
        en: {
          "autoload.status.watch.error": "Watch error",
          "autoload.status.watch.no_files": "No files",
          "autoload.status.watch.loaded": "Loaded",
          "autoload.status.watch.updated": "Updated",
          "autoload.status.watch.no_types_selected": "No types",
        },
      },
      async (url) => {
        if (url.includes("/autoload/latest")) {
          return {
            ok: true,
            status: 200,
            headers: new Headers(),
            json: async () => ({
              file: "scan_0002.cbf",
              mtime: 200,
            }),
          };
        }
        throw new Error(`Unexpected fetch: ${url}`);
      },
    );
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createAutoloadModeController } = await import("../modules/autoload_mode_controller.js");

    const state = {
      autoload: {
        dir: "/data",
        pattern: "",
        lastFile: "scan_0001.cbf",
        lastMtime: 100,
        lastUpdate: 0,
        types: {
          hdf5: false,
          tiff: false,
          cbf: true,
          edf: false,
        },
      },
    };
    const setAutoloadStatus = vi.fn();
    const updateAutoloadMeta = vi.fn();
    const controller = createAutoloadModeController({
      apiBase: "/api",
      state,
      callbacks: {
        setAutoloadStatus,
        setAutoloadLatest: vi.fn(),
        updateAutoloadMeta,
        loadAutoloadFile: vi.fn(async () => false),
        fetchSimplonMask: vi.fn(),
        parseDtype: vi.fn(),
        parseShape: vi.fn(),
        typedArrayFrom: vi.fn(),
        hashBufferSample: vi.fn(),
        parseSimplonMeta: vi.fn(() => ({ analysis: {}, meta: {} })),
        createLiveSourceSnapshot: vi.fn((value) => value),
        appendLiveFrame: vi.fn(() => ({ appended: false, rendered: false })),
        logClient: vi.fn(),
        formatSimplonTimestamp: vi.fn(),
        updateLiveBadge: vi.fn(),
      },
    });

    await controller.autoloadWatchTick();

    expect(state.autoload.lastFile).toBe("scan_0001.cbf");
    expect(state.autoload.lastMtime).toBe(100);
    expect(updateAutoloadMeta).not.toHaveBeenCalled();
    expect(setAutoloadStatus).toHaveBeenCalledWith("Watch error");
  });

  it("reports a folder too large to scan instead of an empty one", async () => {
    // A truncated search that reached no file answers 204, which has no body:
    // without the header this would be reported as "no files" and the user
    // would have no way to tell a big folder from an empty one.
    vi.resetModules();
    global.fetch = buildFetchMock(
      {
        en: {
          "autoload.status.watch.no_files": "No files",
          "autoload.status.watch.truncated": "Folder too large",
        },
      },
      async (url) => {
        if (url.includes("/autoload/latest")) {
          return {
            ok: false,
            status: 204,
            headers: new Headers({ "X-Scan-Truncated": "1" }),
            json: async () => ({}),
          };
        }
        throw new Error(`Unexpected fetch: ${url}`);
      },
    );
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createAutoloadModeController } = await import("../modules/autoload_mode_controller.js");

    const setAutoloadStatus = vi.fn();
    const controller = createAutoloadModeController({
      apiBase: "/api",
      state: {
        autoload: {
          dir: "/data",
          pattern: "",
          lastFile: "",
          lastMtime: 0,
          lastUpdate: 0,
          types: { hdf5: false, tiff: false, cbf: true, edf: false },
        },
      },
      callbacks: {
        setAutoloadStatus,
        setAutoloadLatest: vi.fn(),
        updateAutoloadMeta: vi.fn(),
        loadAutoloadFile: vi.fn(async () => true),
        fetchSimplonMask: vi.fn(),
        parseDtype: vi.fn(),
        parseShape: vi.fn(),
        typedArrayFrom: vi.fn(),
        hashBufferSample: vi.fn(),
        parseSimplonMeta: vi.fn(() => ({ analysis: {}, meta: {} })),
        createLiveSourceSnapshot: vi.fn((value) => value),
        appendLiveFrame: vi.fn(() => ({ appended: false, rendered: false })),
        logClient: vi.fn(),
        formatSimplonTimestamp: vi.fn(),
        updateLiveBadge: vi.fn(),
      },
    });

    await controller.autoloadWatchTick();

    expect(setAutoloadStatus).toHaveBeenCalledWith("Folder too large");
    expect(setAutoloadStatus).not.toHaveBeenCalledWith("No files");
  });

  it("keeps saying the view is partial while a truncated watch loads files", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock(
      {
        en: {
          "autoload.status.watch.loaded": "Loaded",
          "autoload.status.watch.truncated": "Folder too large",
        },
      },
      async (url) => {
        if (url.includes("/autoload/latest")) {
          return {
            ok: true,
            status: 200,
            headers: new Headers({ "X-Scan-Truncated": "1" }),
            json: async () => ({ file: "scan_0002.cbf", mtime: 200, truncated: true }),
          };
        }
        throw new Error(`Unexpected fetch: ${url}`);
      },
    );
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createAutoloadModeController } = await import("../modules/autoload_mode_controller.js");

    const setAutoloadStatus = vi.fn();
    const controller = createAutoloadModeController({
      apiBase: "/api",
      state: {
        autoload: {
          dir: "/data",
          pattern: "",
          lastFile: "",
          lastMtime: 0,
          lastUpdate: 0,
          types: { hdf5: false, tiff: false, cbf: true, edf: false },
        },
      },
      callbacks: {
        setAutoloadStatus,
        setAutoloadLatest: vi.fn(),
        updateAutoloadMeta: vi.fn(),
        loadAutoloadFile: vi.fn(async () => true),
        fetchSimplonMask: vi.fn(),
        parseDtype: vi.fn(),
        parseShape: vi.fn(),
        typedArrayFrom: vi.fn(),
        hashBufferSample: vi.fn(),
        parseSimplonMeta: vi.fn(() => ({ analysis: {}, meta: {} })),
        createLiveSourceSnapshot: vi.fn((value) => value),
        appendLiveFrame: vi.fn(() => ({ appended: false, rendered: false })),
        logClient: vi.fn(),
        formatSimplonTimestamp: vi.fn(),
        updateLiveBadge: vi.fn(),
      },
    });

    await controller.autoloadWatchTick();

    expect(setAutoloadStatus).toHaveBeenLastCalledWith("Folder too large");
  });

  it("does not append duplicate SIMPLON frames when the monitor hash stays the same", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock(
      {
        en: {
          "autoload.status.simplon.updated": "Updated",
          "autoload.status.simplon.no_frame": "No frame",
          "autoload.status.simplon.error": "Error",
          "autoload.status.simplon.set_base_url": "Set URL",
          "source.label.simplon_monitor": "SIMPLON monitor",
        },
      },
      async (url) => {
        if (url.includes("/simplon/monitor")) {
          return {
            ok: true,
            status: 200,
            headers: {
              get(name) {
                const values = {
                  "X-Dtype": "<u2",
                  "X-Shape": "1,1",
                  "X-Simplon-Series": "7",
                  "X-Simplon-Image": "8",
                  "X-Simplon-Date": "2026-04-02T12:00:00Z",
                };
                return values[name] ?? null;
              },
              entries() {
                return [
                  ["X-Dtype", "<u2"],
                  ["X-Shape", "1,1"],
                ][Symbol.iterator]();
              },
            },
            arrayBuffer: async () => new Uint16Array([5]).buffer,
          };
        }
        throw new Error(`Unexpected fetch: ${url}`);
      },
    );
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createAutoloadModeController } = await import("../modules/autoload_mode_controller.js");

    const appendLiveFrame = vi.fn(() => ({ appended: true, rendered: true }));
    const updateAutoloadMeta = vi.fn();
    const state = {
      maskAvailable: true,
      autoload: {
        simplonUrl: "http://simplon.example",
        simplonVersion: "1.8.0",
        simplonTimeout: 500,
        simplonEnable: true,
        livePaused: false,
        lastMaskAttempt: 0,
        lastMonitorSig: "",
        lastUpdate: 0,
      },
    };
    const controller = createAutoloadModeController({
      apiBase: "/api",
      state,
      callbacks: {
        setAutoloadStatus: vi.fn(),
        setAutoloadLatest: vi.fn(),
        updateAutoloadMeta,
        loadAutoloadFile: vi.fn(),
        fetchSimplonMask: vi.fn(),
        parseDtype: (value) => value,
        parseShape: (value) => String(value).split(",").map((item) => Number.parseInt(item, 10)),
        typedArrayFrom: (buffer) => new Uint16Array(buffer),
        hashBufferSample: vi.fn(() => "same-hash"),
        parseSimplonMeta: vi.fn(() => ({
          analysis: {},
          meta: { series: "7", image: "8", date: "2026-04-02T12:00:00Z" },
        })),
        createLiveSourceSnapshot: vi.fn((value) => value),
        appendLiveFrame,
        logClient: vi.fn(),
        formatSimplonTimestamp: vi.fn(() => "12:00"),
        updateLiveBadge: vi.fn(),
      },
    });

    await controller.autoloadSimplonTick();
    await controller.autoloadSimplonTick();

    expect(appendLiveFrame).toHaveBeenCalledTimes(1);
    expect(updateAutoloadMeta).toHaveBeenCalledTimes(1);
  });

  it("drops an in-flight SIMPLON frame when live browsing is paused", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock(
      {
        en: {
          "autoload.status.simplon.updated": "Updated",
          "autoload.status.simplon.no_frame": "No frame",
          "autoload.status.simplon.error": "Error",
          "autoload.status.simplon.set_base_url": "Set URL",
          "source.label.simplon_monitor": "SIMPLON monitor",
        },
      },
      async (url) => {
        if (url.includes("/simplon/monitor")) {
          return {
            ok: true,
            status: 200,
            headers: {
              get(name) {
                const values = {
                  "X-Dtype": "<u2",
                  "X-Shape": "1,1",
                  "X-Simplon-Series": "7",
                  "X-Simplon-Image": "8",
                  "X-Simplon-Date": "2026-04-02T12:00:00Z",
                };
                return values[name] ?? null;
              },
              entries() {
                return [
                  ["X-Dtype", "<u2"],
                  ["X-Shape", "1,1"],
                ][Symbol.iterator]();
              },
            },
            arrayBuffer: async () => new Uint16Array([5]).buffer,
          };
        }
        throw new Error(`Unexpected fetch: ${url}`);
      },
    );
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createAutoloadModeController } = await import("../modules/autoload_mode_controller.js");

    const appendLiveFrame = vi.fn(() => ({ appended: true, rendered: true }));
    const updateAutoloadMeta = vi.fn();
    const updateLiveBadge = vi.fn();
    const state = {
      maskAvailable: true,
      autoload: {
        simplonUrl: "http://simplon.example",
        simplonVersion: "1.8.0",
        simplonTimeout: 500,
        simplonEnable: true,
        livePaused: true,
        lastMaskAttempt: 0,
        lastMonitorSig: "",
        lastUpdate: 0,
      },
    };
    const controller = createAutoloadModeController({
      apiBase: "/api",
      state,
      callbacks: {
        setAutoloadStatus: vi.fn(),
        setAutoloadLatest: vi.fn(),
        updateAutoloadMeta,
        loadAutoloadFile: vi.fn(),
        fetchSimplonMask: vi.fn(),
        parseDtype: (value) => value,
        parseShape: (value) => String(value).split(",").map((item) => Number.parseInt(item, 10)),
        typedArrayFrom: (buffer) => new Uint16Array(buffer),
        hashBufferSample: vi.fn(() => "new-hash"),
        parseSimplonMeta: vi.fn(() => ({
          analysis: {},
          meta: { series: "7", image: "8", date: "2026-04-02T12:00:00Z" },
        })),
        createLiveSourceSnapshot: vi.fn((value) => value),
        appendLiveFrame,
        logClient: vi.fn(),
        formatSimplonTimestamp: vi.fn(() => "12:00"),
        updateLiveBadge,
      },
    });

    await controller.autoloadSimplonTick();

    expect(appendLiveFrame).not.toHaveBeenCalled();
    expect(updateAutoloadMeta).not.toHaveBeenCalled();
    expect(state.autoload.lastMonitorSig).toBe("");
    expect(updateLiveBadge).toHaveBeenCalledTimes(1);
  });
});
