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

function createHeaders(values) {
  return {
    get(name) {
      return values[name] ?? null;
    },
    entries() {
      return Object.entries(values)[Symbol.iterator]();
    },
  };
}

describe("remote_stream_controller", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("clears stale peak overlays when remote metadata conflicts with a newer sequence", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock(
      {
        en: {
          "autoload.status.remote.updated": "Updated",
          "autoload.status.remote.waiting": "Waiting",
          "autoload.status.remote.error": "Error",
          "source.label.remote_stream_with_seq": "Remote {{sourceId}}{{seqSuffix}}",
        },
      },
      async (url) => {
        if (url.includes("/remote/v1/latest")) {
          return {
            ok: true,
            status: 200,
            headers: createHeaders({
              "X-Dtype": "<u2",
              "X-Shape": "1,1",
            }),
            arrayBuffer: async () => new Uint16Array([9]).buffer,
          };
        }
        if (url.includes("/remote/v1/meta")) {
          return {
            ok: false,
            status: 409,
            json: async () => ({ detail: "Requested sequence is no longer current", current_seq: 3 }),
          };
        }
        throw new Error(`Unexpected fetch: ${url}`);
      },
    );
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createRemoteStreamController } = await import("../modules/remote_stream_controller.js");

    const schedulePeakOverlay = vi.fn();
    const applyExternalFrame = vi.fn();
    const analysisState = {
      externalPeakSets: [
        { name: "stale", color: "#4aa3ff", style: "", points: [{ x: 1, y: 2, intensity: 3 }] },
      ],
    };
    const state = {
      autoload: {
        remoteSourceId: "default",
        lastRemoteSeq: 1,
        remoteMeta: {},
        lastUpdate: 0,
      },
    };
    const controller = createRemoteStreamController({
      apiBase: "/api",
      state,
      analysisState,
      callbacks: {
        setAutoloadStatus: vi.fn(),
        updateLiveBadge: vi.fn(),
        updateAutoloadMeta: vi.fn(),
        schedulePeakOverlay,
        updateRemoteMetaUI: vi.fn(),
        updateJfjochMetaUI: vi.fn(),
        startJfjochPreviewBridge: vi.fn(),
        fetchJfjochPreviewStatus: vi.fn(),
        parseDtype: (value) => value,
        parseShape: (value) => String(value).split(",").map((item) => Number.parseInt(item, 10)),
        typedArrayFrom: (buffer) => new Uint16Array(buffer),
        applyRemoteMeta: () => ({
          seq: 2,
          displayName: "",
          series: "12",
          image: "34",
          date: "2026-03-24T12:00:00Z",
        }),
        applyExternalFrame,
      },
    });

    await controller.autoloadRemoteTick();

    expect(applyExternalFrame).toHaveBeenCalledTimes(1);
    expect(analysisState.externalPeakSets).toEqual([]);
    expect(state.autoload.lastRemoteSeq).toBe(2);
    expect(schedulePeakOverlay).toHaveBeenCalled();
    expect(controller).toBeTruthy();
  });
});
