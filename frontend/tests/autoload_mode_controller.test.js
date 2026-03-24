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
        applySimplonMeta: vi.fn(),
        logClient: vi.fn(),
        formatSimplonTimestamp: vi.fn(),
        applyExternalFrame: vi.fn(),
        updateLiveBadge: vi.fn(),
      },
    });

    await controller.autoloadWatchTick();

    expect(state.autoload.lastFile).toBe("scan_0001.cbf");
    expect(state.autoload.lastMtime).toBe(100);
    expect(updateAutoloadMeta).not.toHaveBeenCalled();
    expect(setAutoloadStatus).toHaveBeenCalledWith("Watch error");
  });
});
