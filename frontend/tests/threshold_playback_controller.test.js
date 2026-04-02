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

describe("threshold_playback_controller", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("switches the play button between stop-live and go-live semantics in live-history mode", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock({
      en: {
        "backend.live.live": "LIVE",
        "backend.live.stop": "STOP",
        "toolbar.play.go_live_aria": "Go live",
        "toolbar.play.go_live_title": "Go live title",
        "toolbar.play.stop_live_aria": "Stop live",
        "toolbar.play.stop_live_title": "Stop live title",
        "toolbar.play.toggle": "Play or pause",
        "hint.frame.go_live": "Go live hint",
        "hint.frame.stop_live": "Stop live hint",
        "hint.frame.play_pause": "Play hint",
      },
    });
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createThresholdPlaybackController } = await import("../modules/threshold_playback_controller.js");

    const playBtn = document.createElement("button");
    const prevBtn = document.createElement("button");
    const nextBtn = document.createElement("button");
    const state = {
      file: "Remote #3",
      dataset: "",
      frameCount: 3,
      frameIndex: 2,
      thresholdCount: 1,
      thresholdIndex: 0,
      thresholdEnergies: [],
      seriesFiles: [],
      playing: false,
      fps: 1,
      step: 1,
      autoload: {
        mode: "remote",
        historyEntries: [{}, {}, {}],
        followingLatest: true,
        livePaused: false,
      },
    };
    const controller = createThresholdPlaybackController({
      state,
      constants: {
        frameStepOptions: [1, 10, 100],
      },
      elements: {
        thresholdSelect: document.createElement("select"),
        thresholdField: document.createElement("div"),
        toolbarThresholdWrap: document.createElement("div"),
        toolbarThresholdSelect: document.createElement("select"),
        toolbarMoreThreshold: document.createElement("select"),
        toolbarMoreThresholdField: document.createElement("div"),
        fpsSelect: document.createElement("select"),
        toolbarMoreFps: document.createElement("select"),
        frameStep: document.createElement("select"),
        toolbarMoreStep: document.createElement("select"),
        playBtn,
        prevBtn,
        nextBtn,
      },
      callbacks: {
        formatEnergy: vi.fn(),
        option: vi.fn(() => document.createElement("option")),
        syncToolbarMoreControls: vi.fn(),
        updateViewerFooter: vi.fn(),
        updateFpsLabel: vi.fn(),
        stopPlayback: vi.fn(),
        startPlayback: vi.fn(),
        loadMask: vi.fn(),
        requestFrame: vi.fn(),
      },
    });

    controller.updatePlayButtons();

    expect(playBtn.textContent).toBe("STOP");
    expect(playBtn.disabled).toBe(false);
    expect(playBtn.getAttribute("aria-label")).toBe("Stop live");
    expect(playBtn.title).toBe("Stop live title");
    expect(playBtn.dataset.help).toBe("Stop live hint");
    expect(prevBtn.disabled).toBe(false);
    expect(nextBtn.disabled).toBe(true);

    state.autoload.followingLatest = false;
    state.autoload.livePaused = true;
    state.frameIndex = 1;
    controller.updatePlayButtons();

    expect(playBtn.textContent).toBe("LIVE");
    expect(playBtn.disabled).toBe(false);
    expect(playBtn.getAttribute("aria-label")).toBe("Go live");
    expect(playBtn.title).toBe("Go live title");
    expect(playBtn.dataset.help).toBe("Go live hint");
    expect(prevBtn.disabled).toBe(false);
    expect(nextBtn.disabled).toBe(false);
  });
});
