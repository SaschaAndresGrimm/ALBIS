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

  async function splashController() {
    const { createExportSplashController } = await import(
      "../modules/export_splash_controller.js"
    );
    return createExportSplashController({
      state: { backendAlive: true, isLoading: false, hasFrame: false },
      elements: {
        canvasWrap: null,
        splash: document.getElementById("splash"),
        splashCanvas: null,
        splashCtx: null,
        splashActions: document.getElementById("splash-actions"),
        splashOpenFileBtn: document.getElementById("splash-open-file"),
        splashStatus: document.getElementById("splash-status"),
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
  }

  it("stops the spinner for a localized failure the word list cannot recognize", async () => {
    // Free text is classified by searching it for "ready", "failed", "error",
    // "done" or "complete". No translation carries those, so a German or
    // Japanese failure message used to keep the loading spinner turning -- and
    // even the English "5,000,000 linked data files are missing." does not
    // match. The caller knows it is reporting a failure, so it says so.
    vi.resetModules();
    global.fetch = buildFetchMock({ en: {} });
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const controller = await splashController();
    const splash = document.getElementById("splash");

    const GERMAN = "5.000.000 verknüpfte Datendateien fehlen. Kopieren Sie sie in diesen Ordner.";
    controller.setSplashStatus(GERMAN);
    expect(splash.classList.contains("is-busy")).toBe(true);

    controller.setSplashStatus(GERMAN, {}, { busy: false });
    expect(splash.classList.contains("is-busy")).toBe(false);
    expect(document.getElementById("splash-status").textContent).toBe(GERMAN);
  });

  it("still guesses when the caller says nothing", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock({ en: {} });
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const controller = await splashController();
    const splash = document.getElementById("splash");

    controller.setSplashStatus("Converting frame 3 of 40");
    expect(splash.classList.contains("is-busy")).toBe(true);

    controller.setSplashStatus("Export complete");
    expect(splash.classList.contains("is-busy")).toBe(false);
  });
});

