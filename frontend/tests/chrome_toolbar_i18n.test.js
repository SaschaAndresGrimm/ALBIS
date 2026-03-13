import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

function buildFetchMock(dictionaries) {
  return vi.fn(async (url) => {
    const match = String(url).match(/locales\/([^/]+)\.json/);
    const language = match ? decodeURIComponent(match[1]) : "en";
    const payload = dictionaries[language] || {};
    return {
      ok: true,
      json: async () => payload,
    };
  });
}

describe("chrome toolbar i18n", () => {
  beforeEach(() => {
    document.body.innerHTML = `
      <div id="toolbar-playback-wrap" class="toolbar-playback">
        <button id="toolbar-playback-toggle" type="button">Playback ▾</button>
        <div id="toolbar-playback-popover" aria-hidden="true"></div>
      </div>
    `;
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("updates the playback toggle label on language change without requiring interaction", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock({
      en: {
        "toolbar.playback.closed": "Playback ▾",
        "toolbar.playback.open": "Playback ▴",
      },
      "zh-CN": {
        "toolbar.playback.closed": "播放 ▾",
        "toolbar.playback.open": "播放 ▴",
      },
      ja: {},
      fr: {},
      es: {},
      it: {},
      pt: {},
    });

    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });

    const { createChromeToolbarController } = await import("../modules/chrome_toolbar_controller.js");
    const toolbarPlaybackWrap = document.getElementById("toolbar-playback-wrap");
    const toolbarPlaybackToggle = document.getElementById("toolbar-playback-toggle");
    const toolbarPlaybackPopover = document.getElementById("toolbar-playback-popover");

    const controller = createChromeToolbarController({
      state: {
        fps: 1,
        step: 1,
        thresholdIndex: 0,
        panelCollapsed: true,
        zoom: 1,
        file: "",
        dataset: "",
        frameCount: 0,
        frameIndex: 0,
        backendVersion: "",
        autoload: {
          mode: "file",
          running: false,
          lastUpdate: 0,
          interval: 1000,
        },
      },
      constants: {
        appFrontendBuild: "local",
        frameStepOptions: [1],
        chromeIdleDelayMs: 2000,
      },
      elements: {
        toolbarPlaybackWrap,
        toolbarPlaybackToggle,
        toolbarPlaybackPopover,
      },
      callbacks: {
        middleTruncate: (value) => String(value || ""),
        fileLabel: (value) => String(value || ""),
        formatTimeStamp: () => "",
        setSummaryChip: () => {},
        estimateToolbarChars: () => 72,
        updateSeriesSumUi: () => {},
        isPhonePanelLayout: () => false,
        isMenuOpen: () => false,
      },
    });

    controller.updateToolbar();
    expect(toolbarPlaybackToggle?.textContent).toBe("Playback ▾");

    i18n.setLanguage("zh-CN", { persist: false, applyDom: false });
    controller.updateToolbar();

    expect(toolbarPlaybackToggle?.textContent).toBe("播放 ▾");
  });
});
