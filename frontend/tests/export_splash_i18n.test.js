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

describe("splash status i18n", () => {
  beforeEach(() => {
    document.body.innerHTML = `
      <div id="splash" class="splash">
        <div
          id="splash-status"
          class="splash-sub"
          data-i18n="splash.status.starting_backend"
        >
          Starting backend...
        </div>
        <div id="splash-actions" class="splash-actions"></div>
        <button id="splash-open-file"></button>
      </div>
    `;
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("keeps the current splash status when the language changes", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock({
      en: {
        "splash.status.starting_backend": "Starting backend...",
        "splash.status.ready_open_file": "Ready. Open a file to begin.",
      },
      "zh-CN": {
        "splash.status.starting_backend": "正在启动后端...",
        "splash.status.ready_open_file": "准备就绪。打开文件以开始。",
      },
      ja: {},
      fr: {},
      es: {},
      it: {},
      pt: {},
    });

    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });

    const { createExportSplashController } = await import("../modules/export_splash_controller.js");
    const splashStatus = document.getElementById("splash-status");
    const splash = document.getElementById("splash");

    const controller = createExportSplashController({
      state: {
        backendAlive: true,
        isLoading: false,
        hasFrame: false,
      },
      elements: {
        canvasWrap: null,
        splash,
        splashCanvas: null,
        splashCtx: null,
        splashActions: document.getElementById("splash-actions"),
        splashOpenFileBtn: document.getElementById("splash-open-file"),
        splashStatus,
      },
      callbacks: {
        buildPalette: () => new Uint8ClampedArray(0),
        getPaletteColorCount: () => 0,
        mapValueToNorm: () => 0,
        getEffectiveScrollLeft: () => 0,
        getEffectiveScrollTop: () => 0,
        setStatus: () => {},
      },
    });

    controller.setSplashStatus("splash.status.ready_open_file");
    expect(splashStatus?.textContent).toBe("Ready. Open a file to begin.");
    expect(splashStatus?.dataset.i18n).toBe("splash.status.ready_open_file");

    i18n.setLanguage("zh-CN", { persist: false, applyDom: true });

    expect(splashStatus?.textContent).toBe("准备就绪。打开文件以开始。");
    expect(splashStatus?.dataset.i18n).toBe("splash.status.ready_open_file");
  });
});
