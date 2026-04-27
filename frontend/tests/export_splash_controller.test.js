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

function createMockCanvas() {
  const ctx = {
    lastImageData: null,
    createImageData: (width, height) => ({
      width,
      height,
      data: new Uint8ClampedArray(width * height * 4),
    }),
    putImageData: (imageData) => {
      ctx.lastImageData = imageData;
    },
  };
  return {
    width: 0,
    height: 0,
    getContext: (kind) => (kind === "2d" ? ctx : null),
    toBlob: (callback) => callback({ size: 4 }),
    _ctx: ctx,
  };
}

describe("export_splash_controller", () => {
  let originalUrl;

  beforeEach(() => {
    localStorage.clear();
    originalUrl = global.URL;
  });

  afterEach(() => {
    vi.restoreAllMocks();
    global.URL = originalUrl;
    delete global.fetch;
  });

  it("exports saturated pixels with the same overlay color used by the viewer", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock({
      en: {},
    });
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createExportSplashController } = await import("../modules/export_splash_controller.js");

    const originalCreateElement = document.createElement.bind(document);
    const canvases = [];
    vi.spyOn(document, "createElement").mockImplementation((tagName, options) => {
      if (String(tagName).toLowerCase() === "canvas") {
        const canvas = createMockCanvas();
        canvases.push(canvas);
        return canvas;
      }
      return originalCreateElement(tagName, options);
    });
    vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});
    global.URL = {
      createObjectURL: vi.fn(() => "blob:mock"),
      revokeObjectURL: vi.fn(),
    };

    const controller = createExportSplashController({
      state: {
        dataRaw: new Uint16Array([5]),
        colormap: "gray",
        maskEnabled: false,
        maskAvailable: false,
        maskRaw: null,
        maskShape: null,
        maskSaturatedEnabled: true,
        width: 1,
        height: 1,
        frameIndex: 0,
      },
      elements: {
        canvasWrap: null,
        splash: null,
        splashCanvas: null,
        splashCtx: null,
        splashActions: null,
        splashOpenFileBtn: null,
        splashStatus: null,
      },
      callbacks: {
        buildPalette: () => new Uint8Array([10, 20, 30, 255, 40, 50, 60, 255]),
        getPaletteColorCount: () => 2,
        mapValueToNorm: () => 0,
        getActiveSaturationMax: () => 5,
        getEffectiveScrollLeft: () => 0,
        getEffectiveScrollTop: () => 0,
        isSaturatedValue: (value, satMax) => value === satMax,
        setStatus: () => {},
      },
    });

    controller.exportFullImage("frame.png");

    expect(canvases).toHaveLength(1);
    expect(Array.from(canvases[0]._ctx.lastImageData.data)).toEqual([88, 183, 198, 255]);
  });
});
