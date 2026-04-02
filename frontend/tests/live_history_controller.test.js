import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

function buildFetchMock(dictionaries) {
  return vi.fn(async (url) => {
    const text = String(url);
    const match = text.match(/locales\/([^/]+)\.json/);
    if (!match) {
      throw new Error(`Unexpected fetch: ${text}`);
    }
    const language = decodeURIComponent(match[1]);
    return {
      ok: true,
      json: async () => dictionaries[language] || {},
    };
  });
}

function createState() {
  return {
    frameCount: 1,
    frameIndex: 0,
    autoload: {
      mode: "remote",
      historyEntries: [],
      historyCapacity: 8,
      historyCursor: 0,
      followingLatest: true,
      livePaused: false,
      pendingNewFrames: 0,
    },
  };
}

function createEntry(seq) {
  return {
    sourceKey: "remote:default",
    sourceKind: "remote",
    dedupeKey: String(seq),
    label: `Remote #${seq}`,
    data: new Uint16Array([seq]),
    shape: [1, 1],
    dtype: "<u2",
    snapshot: {
      sourceKind: "remote",
      analysis: {},
      remoteMeta: { seq },
      externalPeakSets: [],
    },
  };
}

describe("live_history_controller", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("appends and renders the newest live frame while following latest", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock({
      en: {
        "status.frame.position": "Frame {{current}} / {{total}}",
        "status.live_history.paused": "Paused on history",
        "status.live_history.paused_new_frames": "Paused on history · {{count}} newer frames",
      },
    });
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createLiveHistoryController } = await import("../modules/live_history_controller.js");

    const state = createState();
    const applyExternalFrame = vi.fn();
    const controller = createLiveHistoryController({
      state,
      callbacks: {
        applyExternalFrame,
        applyLiveSourceSnapshot: vi.fn(),
        updateFrameControls: vi.fn(),
        updatePlayButtons: vi.fn(),
        updateToolbar: vi.fn(),
        updateAutoloadUI: vi.fn(),
        setStatus: vi.fn(),
      },
    });

    controller.appendLiveFrame(createEntry(1));
    controller.appendLiveFrame(createEntry(2));

    expect(applyExternalFrame).toHaveBeenCalledTimes(2);
    expect(state.frameCount).toBe(2);
    expect(state.frameIndex).toBe(1);
    expect(state.autoload.followingLatest).toBe(true);
    expect(state.autoload.pendingNewFrames).toBe(0);
  });

  it("pauses live browsing on history and goLive returns to the newest frame", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock({
      en: {
        "status.frame.position": "Frame {{current}} / {{total}}",
        "status.live_history.paused": "Paused on history",
        "status.live_history.paused_new_frames": "Paused on history · {{count}} newer frames",
      },
    });
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createLiveHistoryController } = await import("../modules/live_history_controller.js");

    const state = createState();
    const applyExternalFrame = vi.fn();
    const setStatus = vi.fn();
    const controller = createLiveHistoryController({
      state,
      callbacks: {
        applyExternalFrame,
        applyLiveSourceSnapshot: vi.fn(),
        updateFrameControls: vi.fn(),
        updatePlayButtons: vi.fn(),
        updateToolbar: vi.fn(),
        updateAutoloadUI: vi.fn(),
        setStatus,
      },
    });

    controller.appendLiveFrame(createEntry(1));
    controller.appendLiveFrame(createEntry(2));
    controller.appendLiveFrame(createEntry(3));
    controller.showLiveHistoryFrame(0);
    expect(state.frameCount).toBe(3);
    expect(state.frameIndex).toBe(0);
    expect(state.autoload.followingLatest).toBe(false);
    expect(state.autoload.livePaused).toBe(true);
    expect(state.autoload.pendingNewFrames).toBe(0);
    expect(setStatus).toHaveBeenLastCalledWith("Paused on history");

    applyExternalFrame.mockClear();
    controller.goLive();

    expect(applyExternalFrame).toHaveBeenCalledTimes(1);
    expect(state.frameIndex).toBe(2);
    expect(state.autoload.followingLatest).toBe(true);
    expect(state.autoload.livePaused).toBe(false);
    expect(state.autoload.pendingNewFrames).toBe(0);
  });

  it("truncates to the last eight frames while following latest", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock({
      en: {
        "status.frame.position": "Frame {{current}} / {{total}}",
        "status.live_history.paused": "Paused on history",
        "status.live_history.paused_new_frames": "Paused on history · {{count}} newer frames",
      },
    });
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createLiveHistoryController } = await import("../modules/live_history_controller.js");

    const state = createState();
    const applyExternalFrame = vi.fn();
    const controller = createLiveHistoryController({
      state,
      callbacks: {
        applyExternalFrame,
        applyLiveSourceSnapshot: vi.fn(),
        updateFrameControls: vi.fn(),
        updatePlayButtons: vi.fn(),
        updateToolbar: vi.fn(),
        updateAutoloadUI: vi.fn(),
        setStatus: vi.fn(),
      },
    });

    for (let seq = 1; seq <= 8; seq += 1) {
      controller.appendLiveFrame(createEntry(seq));
    }
    controller.appendLiveFrame(createEntry(9));

    expect(state.autoload.historyEntries).toHaveLength(8);
    expect(state.autoload.historyEntries[0].dedupeKey).toBe("2");
    expect(state.frameIndex).toBe(7);
    expect(applyExternalFrame).toHaveBeenCalledTimes(9);
  });

  it("resets live history back to single-frame navigation", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock({
      en: {
        "status.frame.position": "Frame {{current}} / {{total}}",
        "status.live_history.paused": "Paused on history",
        "status.live_history.paused_new_frames": "Paused on history · {{count}} newer frames",
      },
    });
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createLiveHistoryController } = await import("../modules/live_history_controller.js");

    const state = createState();
    const controller = createLiveHistoryController({
      state,
      callbacks: {
        applyExternalFrame: vi.fn(),
        applyLiveSourceSnapshot: vi.fn(),
        updateFrameControls: vi.fn(),
        updatePlayButtons: vi.fn(),
        updateToolbar: vi.fn(),
        updateAutoloadUI: vi.fn(),
        setStatus: vi.fn(),
      },
    });

    controller.appendLiveFrame(createEntry(1));
    controller.appendLiveFrame(createEntry(2));
    controller.resetLiveHistory();

    expect(state.autoload.historyEntries).toEqual([]);
    expect(state.frameCount).toBe(1);
    expect(state.frameIndex).toBe(0);
    expect(state.autoload.followingLatest).toBe(true);
    expect(state.autoload.livePaused).toBe(false);
  });
});
