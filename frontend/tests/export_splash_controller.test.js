import fs from "node:fs";
import path from "node:path";

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// The real English catalogue, so a renamed status key fails here instead of
// quietly degrading to a raw key in the UI.
const EN = JSON.parse(
  fs.readFileSync(path.join(process.cwd(), "frontend", "locales", "en.json"), "utf8")
);

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
        hasFrame: true,
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

describe("export_splash_controller availability", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  async function build(stateOverrides) {
    vi.resetModules();
    global.fetch = vi.fn(async () => ({ ok: true, json: async () => EN }));
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createExportSplashController } = await import(
      "../modules/export_splash_controller.js"
    );
    const setStatus = vi.fn();
    const controller = createExportSplashController({
      state: { width: 4, height: 4, zoom: 1, frameIndex: 0, ...stateOverrides },
      elements: { canvasWrap: { clientWidth: 4, clientHeight: 4 } },
      callbacks: {
        getEffectiveScrollLeft: () => 0,
        getEffectiveScrollTop: () => 0,
        setStatus,
      },
    });
    return { controller, setStatus };
  }

  it("says why a full-image save is not possible instead of returning silently", async () => {
    const { controller, setStatus } = await build({ hasFrame: false, dataRaw: null });
    expect(controller.exportFullImage({ saveAs: true })).toBeUndefined();
    expect(setStatus).toHaveBeenCalledWith(EN["status.export.no_image"], { tone: "warning" });
  });

  it("refuses a viewer-window screenshot with nothing on screen", async () => {
    const { controller, setStatus } = await build({ hasFrame: false, dataRaw: null });
    await controller.exportViewerWindow({ saveAs: true });
    expect(setStatus).toHaveBeenCalledWith(EN["status.export.no_image"], { tone: "warning" });
  });

  it("says why a visible-area save is not possible instead of returning silently", async () => {
    const { controller, setStatus } = await build({ hasFrame: false, dataRaw: null });
    expect(controller.exportVisibleArea({ saveAs: true })).toBeUndefined();
    expect(setStatus).toHaveBeenCalledWith(EN["status.export.no_image"], { tone: "warning" });
  });
});

describe("Viewer Window capture keeps enlarged pixels sharp", () => {
  function fakeContextProto() {
    const calls = [];
    const proto = {
      imageSmoothingEnabled: true,
      drawImage(source) {
        calls.push({ id: source.id, smoothing: this.imageSmoothingEnabled });
      },
    };
    return { proto, calls };
  }

  it("draws pixelated canvases without smoothing, and others as before", async () => {
    const { withPixelatedCanvasesSharp } = await import("../modules/export_splash_controller.js");
    const { proto, calls } = fakeContextProto();
    const original = proto.drawImage;
    const image = Object.assign(document.createElement("canvas"), { id: "image" });
    image.style.imageRendering = "pixelated";
    const histogram = Object.assign(document.createElement("canvas"), { id: "histogram" });
    document.body.append(image, histogram);
    const ctx = Object.create(proto);

    const result = await withPixelatedCanvasesSharp(async () => {
      ctx.drawImage(image, 0, 0);
      ctx.drawImage(histogram, 0, 0);
      return "shot";
    }, proto);

    expect(result).toBe("shot");
    expect(calls).toEqual([
      { id: "image", smoothing: false },
      { id: "histogram", smoothing: true },
    ]);
    // Smoothing is restored after the call, and drawImage after the capture.
    expect(ctx.imageSmoothingEnabled).toBe(true);
    expect(proto.drawImage).toBe(original);
    image.remove();
    histogram.remove();
  });

  it("restores drawImage when the capture fails", async () => {
    const { withPixelatedCanvasesSharp } = await import("../modules/export_splash_controller.js");
    const { proto } = fakeContextProto();
    const original = proto.drawImage;
    await expect(
      withPixelatedCanvasesSharp(async () => {
        throw new Error("tainted");
      }, proto),
    ).rejects.toThrow("tainted");
    expect(proto.drawImage).toBe(original);
  });
});

describe("Save As -> Visible Area", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  async function saveVisible(stateOverrides) {
    vi.resetModules();
    global.fetch = buildFetchMock({ en: {} });
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createExportSplashController } = await import("../modules/export_splash_controller.js");
    const originalCreateElement = document.createElement.bind(document);
    const canvases = [];
    vi.spyOn(document, "createElement").mockImplementation((tagName, options) => {
      if (String(tagName).toLowerCase() !== "canvas") return originalCreateElement(tagName, options);
      const canvas = createMockCanvas();
      canvas._ctx.drawImage = (...args) => {
        canvas.drawn = { args: args.slice(1), smoothing: canvas._ctx.imageSmoothingEnabled };
      };
      canvases.push(canvas);
      return canvas;
    });
    vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});
    const url = global.URL;
    global.URL = { createObjectURL: vi.fn(() => "blob:mock"), revokeObjectURL: vi.fn() };
    try {
      const controller = createExportSplashController({
        state: {
          hasFrame: true,
          dataRaw: new Uint16Array(100),
          colormap: "gray",
          width: 10,
          height: 10,
          frameIndex: 0,
          ...stateOverrides,
        },
        // 40 x 80 CSS px of viewport.
        elements: { canvasWrap: { clientWidth: 40, clientHeight: 80 } },
        callbacks: {
          buildPalette: () => new Uint8Array([0, 0, 0, 255, 255, 255, 255, 255]),
          getPaletteColorCount: () => 2,
          mapValueToNorm: () => 0,
          getActiveSaturationMax: () => null,
          getEffectiveScrollLeft: () => 0,
          getEffectiveScrollTop: () => 0,
          isSaturatedValue: () => false,
          setStatus: () => {},
        },
      });
      controller.exportVisibleArea();
    } finally {
      global.URL = url;
    }
    return canvases;
  }

  it("saves what is on screen: the viewer's zoom and pixel aspect, without smoothing", async () => {
    // At 20x with pixels twice as tall, 40 x 80 px show 2 x 2 detector pixels.
    const canvases = await saveVisible({ zoom: 20, pixelAspect: 2 });
    const out = canvases[canvases.length - 1];
    expect([out.width, out.height]).toEqual([40, 80]);
    expect(out.drawn).toEqual({ args: [0, 0, 40, 80], smoothing: false });
  });

  it("keeps one pixel per detector pixel when zoomed out", async () => {
    const canvases = await saveVisible({ zoom: 0.5, pixelAspect: 1 });
    expect(canvases).toHaveLength(1);
    expect([canvases[0].width, canvases[0].height]).toEqual([10, 10]);
  });
});
