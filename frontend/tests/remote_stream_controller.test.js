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

  it("appends a new remote sequence to live history and patches metadata", async () => {
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
            ok: true,
            status: 200,
            json: async () => ({
              peak_sets: [
                {
                  name: "set-1",
                  color: "#4aa3ff",
                  style: "",
                  points: [[1, 2, 3]],
                },
              ],
            }),
          };
        }
        throw new Error(`Unexpected fetch: ${url}`);
      },
    );
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createRemoteStreamController } = await import("../modules/remote_stream_controller.js");

    const appendLiveFrame = vi.fn(() => ({ appended: true, rendered: true }));
    const updateLiveHistoryEntry = vi.fn();
    const applyExternalFrame = vi.fn();
    const state = {
      autoload: {
        remoteSourceId: "default",
        lastRemoteSeq: 1,
        remoteMeta: {},
        livePaused: false,
        lastUpdate: 0,
      },
    };
    const controller = createRemoteStreamController({
      apiBase: "/api",
      state,
      callbacks: {
        setAutoloadStatus: vi.fn(),
        updateLiveBadge: vi.fn(),
        updateAutoloadMeta: vi.fn(),
        startJfjochPreviewBridge: vi.fn(),
        fetchJfjochPreviewStatus: vi.fn(),
        parseDtype: (value) => value,
        parseShape: (value) => String(value).split(",").map((item) => Number.parseInt(item, 10)),
        typedArrayFrom: (buffer) => new Uint16Array(buffer),
        parseRemoteMeta: () => ({
          analysis: {},
          meta: {
            seq: 2,
            displayName: "",
            series: "12",
            image: "34",
            date: "2026-03-24T12:00:00Z",
          },
        }),
        createLiveSourceSnapshot: vi.fn((value) => value),
        applyLiveSourceSnapshot: vi.fn(),
        appendLiveFrame,
        updateLiveHistoryEntry,
        resetLiveHistory: vi.fn(),
        applyExternalFrame,
      },
    });

    await controller.autoloadRemoteTick();

    expect(appendLiveFrame).toHaveBeenCalledTimes(1);
    expect(applyExternalFrame).not.toHaveBeenCalled();
    expect(state.autoload.lastRemoteSeq).toBe(2);
    expect(updateLiveHistoryEntry).toHaveBeenCalledTimes(1);
    expect(controller).toBeTruthy();
  });

  it("drops an in-flight remote frame when live browsing is paused", async () => {
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
        throw new Error(`Unexpected fetch: ${url}`);
      },
    );
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createRemoteStreamController } = await import("../modules/remote_stream_controller.js");

    const appendLiveFrame = vi.fn(() => ({ appended: true, rendered: true }));
    const updateLiveBadge = vi.fn();
    const state = {
      autoload: {
        remoteSourceId: "default",
        lastRemoteSeq: 1,
        remoteMeta: {},
        livePaused: true,
        lastUpdate: 0,
      },
    };
    const controller = createRemoteStreamController({
      apiBase: "/api",
      state,
      callbacks: {
        setAutoloadStatus: vi.fn(),
        updateLiveBadge,
        updateAutoloadMeta: vi.fn(),
        startJfjochPreviewBridge: vi.fn(),
        fetchJfjochPreviewStatus: vi.fn(),
        parseDtype: (value) => value,
        parseShape: (value) => String(value).split(",").map((item) => Number.parseInt(item, 10)),
        typedArrayFrom: (buffer) => new Uint16Array(buffer),
        parseRemoteMeta: () => ({
          analysis: {},
          meta: {
            seq: 2,
            displayName: "",
            series: "12",
            image: "34",
            date: "2026-03-24T12:00:00Z",
          },
        }),
        createLiveSourceSnapshot: vi.fn((value) => value),
        applyLiveSourceSnapshot: vi.fn(),
        appendLiveFrame,
        updateLiveHistoryEntry: vi.fn(),
        resetLiveHistory: vi.fn(),
        applyExternalFrame: vi.fn(),
      },
    });

    await controller.autoloadRemoteTick();

    expect(appendLiveFrame).not.toHaveBeenCalled();
    expect(state.autoload.lastRemoteSeq).toBe(1);
    expect(updateLiveBadge).toHaveBeenCalledTimes(1);
  });
});
