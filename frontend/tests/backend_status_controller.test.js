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

describe("backend_status_controller", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("shows a paused live badge when the viewer is frozen on live history", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock({
      en: {
        "backend.live.live": "LIVE",
        "backend.live.wait": "WAIT",
        "backend.live.paused": "PAUSED",
        "backend.live.aria.live": "Stream live",
        "backend.live.aria.wait": "Stream waiting",
        "backend.live.aria.paused": "Stream paused",
        "backend.live.title.live": "Live title",
        "backend.live.title.wait": "Wait title",
        "backend.live.title.paused": "Paused title",
      },
    });
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createBackendStatusController } = await import("../modules/backend_status_controller.js");

    const liveBadge = document.createElement("div");
    const controller = createBackendStatusController({
      apiBase: "/api",
      state: {
        autoload: {
          mode: "remote",
          running: true,
          interval: 250,
          lastUpdate: Date.now(),
          historyEntries: [{}, {}],
          followingLatest: false,
          livePaused: true,
        },
      },
      elements: {
        liveBadge,
        backendBadge: null,
        aboutVersion: null,
      },
      callbacks: {
        updateFooterVersions: vi.fn(),
        updateSplashCallToAction: vi.fn(),
        setSplashStatus: vi.fn(),
      },
    });

    controller.updateLiveBadge();

    expect(liveBadge.textContent).toBe("PAUSED");
    expect(liveBadge.classList.contains("is-active")).toBe(true);
    expect(liveBadge.classList.contains("is-paused")).toBe(true);
    expect(liveBadge.classList.contains("is-wait")).toBe(false);
    expect(liveBadge.getAttribute("aria-label")).toBe("Stream paused");
    expect(liveBadge.title).toBe("Paused title");
  });
});
