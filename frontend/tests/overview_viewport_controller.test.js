import { afterEach, describe, expect, it, vi } from "vitest";

function createCanvasWrap(size) {
  const canvasWrap = document.createElement("div");
  let left = 0;
  let top = 0;

  Object.defineProperties(canvasWrap, {
    clientWidth: {
      configurable: true,
      get: () => size.clientWidth,
    },
    clientHeight: {
      configurable: true,
      get: () => size.clientHeight,
    },
    scrollLeft: {
      configurable: true,
      get: () => left,
      set: (value) => {
        left = Number(value) || 0;
      },
    },
    scrollTop: {
      configurable: true,
      get: () => top,
      set: (value) => {
        top = Number(value) || 0;
      },
    },
  });

  canvasWrap.getBoundingClientRect = () => ({
    left: 0,
    top: 0,
    width: size.clientWidth,
    height: size.clientHeight,
    right: size.clientWidth,
    bottom: size.clientHeight,
  });

  return canvasWrap;
}

async function createController({
  imageWidth = 200,
  imageHeight = 200,
  zoom = 1,
  viewWidth = 400,
  viewHeight = 300,
  footerHeight = 0,
  footerTop = null,
  panelRect = null,
  chromeBottom = 0,
  onViewportChanged = vi.fn(),
} = {}) {
  const { createOverviewViewportController } = await import("../modules/overview_viewport_controller.js");
  const state = {
    playing: false,
    hasFrame: true,
    width: imageWidth,
    height: imageHeight,
    zoom,
    renderOffsetX: 0,
    renderOffsetY: 0,
  };
  const size = { clientWidth: viewWidth, clientHeight: viewHeight };
  const canvasWrap = createCanvasWrap(size);
  const canvas = document.createElement("canvas");
  let viewerFooterEl = null;
  if (footerHeight) {
    viewerFooterEl = document.createElement("div");
    viewerFooterEl.getBoundingClientRect = () =>
      footerTop === null ? { height: footerHeight } : { top: footerTop, height: footerHeight };
  }
  // A side panel floating over the canvas, and the menu bar and toolbar
  // floating over its top (the canvas-first layout).
  let toolsPanel = null;
  if (panelRect) {
    toolsPanel = document.createElement("div");
    toolsPanel.style.position = "absolute";
    // In the document, as the real panel is: jsdom 31 gives a detached
    // element an empty computed style, which would read as a docked panel.
    document.body.append(toolsPanel);
    toolsPanel.getBoundingClientRect = () => ({ ...panelRect, width: panelRect.right - panelRect.left });
  }
  let appChrome = null;
  if (chromeBottom) {
    appChrome = document.createElement("div");
    appChrome.getBoundingClientRect = () => ({
      left: 0, top: 0, right: viewWidth, bottom: chromeBottom, width: viewWidth, height: chromeBottom,
    });
  }
  const overviewState = {};

  const controller = createOverviewViewportController({
    state,
    overviewState,
    elements: {
      canvasWrap,
      canvas,
      overviewCanvas: null,
      overviewCtx: null,
      zoomRange: null,
      zoomValue: null,
      viewerFooterEl,
      toolsPanel,
      appChrome,
    },
    constants: {
      MIN_ZOOM: 0.1,
      MAX_ZOOM: 20,
      VIEWPORT_INTERACTION_IDLE_MS: 60,
    },
    theme: {
      PLOT_THEME: {},
    },
    callbacks: {
      scheduleRoiOverlay: vi.fn(),
      requestFrame: vi.fn(),
      hasPendingFrameRequest: () => false,
      consumePendingFrameRequest: () => null,
      isFrameLoading: () => false,
      updateViewerFooter: vi.fn(),
      onViewportChanged,
    },
    core: {
      overlayRender: { deferPixelOverlayRedraw: vi.fn(), schedulePixelOverlay: vi.fn(), scheduleResolutionOverlay: vi.fn(), schedulePeakOverlay: vi.fn() },
      fileDataPipeline: { cancelActiveLoad: vi.fn() },
    },
  });

  return { controller, state, canvasWrap, canvas, size, overviewState, onViewportChanged };
}

// Where the image's edges are on screen, in canvas pixels.
function imageEdges(controller, state) {
  // "+ 0" turns -0 into 0, so toEqual compares positions, not signs.
  const left = -controller.getEffectiveScrollLeft() + 0;
  const top = -controller.getEffectiveScrollTop() + 0;
  return {
    left,
    top,
    right: left + state.width * state.zoom + 0,
    bottom: top + state.height * state.zoom + 0,
  };
}

function getScreenPoint(controller, state, worldX, worldY) {
  return {
    x: (state.renderOffsetX || 0) + worldX * (state.zoom || 1) - controller.getEffectiveScrollLeft(),
    y: (state.renderOffsetY || 0) + worldY * (state.zoom || 1) - controller.getEffectiveScrollTop(),
  };
}

describe("overview_viewport_controller", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("flushes only the latest queued frame after viewport defer completes", async () => {
    vi.useFakeTimers();
    const { createOverviewViewportController } = await import("../modules/overview_viewport_controller.js");

    let pendingFrame = 2;
    let frameLoading = false;
    const requestFrame = vi.fn();
    const controller = createOverviewViewportController({
      state: {
        playing: true,
        hasFrame: true,
        width: 100,
        height: 100,
        zoom: 1,
        renderOffsetX: 0,
        renderOffsetY: 0,
      },
      overviewState: {},
      elements: {
        canvasWrap: null,
        canvas: null,
        overviewCanvas: null,
        overviewCtx: null,
        zoomRange: null,
        zoomValue: null,
        viewerFooterEl: null,
      },
      constants: {
        MIN_ZOOM: 0.1,
        MAX_ZOOM: 20,
        VIEWPORT_INTERACTION_IDLE_MS: 60,
      },
      theme: {
        PLOT_THEME: {},
      },
      callbacks: {
        scheduleRoiOverlay: vi.fn(),
        requestFrame,
        hasPendingFrameRequest: () => pendingFrame !== null,
        consumePendingFrameRequest: () => {
          const next = pendingFrame;
          pendingFrame = null;
          return next;
        },
        isFrameLoading: () => frameLoading,
        updateViewerFooter: vi.fn(),
      },
      core: {
        overlayRender: { deferPixelOverlayRedraw: vi.fn(), schedulePixelOverlay: vi.fn(), scheduleResolutionOverlay: vi.fn(), schedulePeakOverlay: vi.fn() },
        fileDataPipeline: { cancelActiveLoad: vi.fn() },
      },
    });

    controller.deferViewportInteraction(60);
    pendingFrame = 9;
    frameLoading = false;
    await vi.advanceTimersByTimeAsync(72);

    expect(requestFrame).toHaveBeenCalledTimes(1);
    expect(requestFrame).toHaveBeenCalledWith(9);
  });

  it("keeps the pixel under the cursor where it is while zooming in and out", async () => {
    const { controller, state } = await createController({ imageWidth: 2000, imageHeight: 2000 });

    controller.setZoom(1);
    for (const [screenX, screenY, zoom] of [[120, 80, 2.5], [300, 220, 0.8], [50, 260, 4], [200, 150, 1]]) {
      const imageX = controller.getEffectiveScrollLeft() / state.zoom + screenX / state.zoom;
      const imageY = controller.getEffectiveScrollTop() / state.zoom + screenY / state.zoom;
      controller.zoomAt(screenX, screenY, zoom);
      const after = getScreenPoint(controller, state, imageX, imageY);
      expect(after.x).toBeCloseTo(screenX, 6);
      expect(after.y).toBeCloseTo(screenY, 6);
    }
  });

  it("zooms and unzooms back to exactly the same view", async () => {
    const { controller, state } = await createController({ imageWidth: 2000, imageHeight: 2000 });

    controller.setZoom(1);
    controller.setEffectiveScroll(700, 900);
    const before = { x: controller.getEffectiveScrollLeft(), y: controller.getEffectiveScrollTop() };
    controller.zoomAt(130, 70, 3);
    controller.zoomAt(130, 70, 1);

    expect(state.zoom).toBe(1);
    expect(controller.getEffectiveScrollLeft()).toBeCloseTo(before.x, 6);
    expect(controller.getEffectiveScrollTop()).toBeCloseTo(before.y, 6);
  });

  it("pans until only a strip of the image is left in view", async () => {
    const { controller, state } = await createController({ imageWidth: 2000, imageHeight: 2000 });

    controller.setZoom(1);
    controller.setEffectiveScroll(-99999, -99999);
    expect(imageEdges(controller, state)).toMatchObject({ left: 400 - 48, top: 300 - 48 });

    controller.setEffectiveScroll(99999, 99999);
    expect(imageEdges(controller, state)).toMatchObject({ right: 48, bottom: 48 });
  });

  it("keeps the strip of image above the footer floating over it", async () => {
    const { controller, state } = await createController({
      imageWidth: 2000,
      imageHeight: 2000,
      footerHeight: 40,
    });

    controller.setZoom(1);
    controller.setEffectiveScroll(0, -99999);

    expect(imageEdges(controller, state).top).toBe(300 - 48 - 48);
  });

  it("moves a zoomed-out image freely, short of losing it", async () => {
    const { controller, state } = await createController({ imageWidth: 200, imageHeight: 1000 });

    controller.setZoom(1);
    controller.setEffectiveScroll(-30, 400);
    expect(imageEdges(controller, state)).toMatchObject({ left: 30, right: 230, top: -400 });

    controller.setEffectiveScroll(-9999, 400);
    expect(imageEdges(controller, state)).toMatchObject({ left: 400 - 48 });

    controller.setEffectiveScroll(9999, 400);
    expect(imageEdges(controller, state)).toMatchObject({ right: 48 });
  });

  it("does not move a small, moved image when its minimap outline is grabbed", async () => {
    const { controller } = await createController({ imageWidth: 200, imageHeight: 150 });

    controller.setZoom(1);
    controller.setEffectiveScroll(-150, -120);
    const before = controller.getViewCenter();
    // The overview bindings grab relative to the real view centre.
    controller.panToImageCenter(before.centerX, before.centerY);

    expect(controller.getViewCenter()).toEqual(before);
  });

  it("comes back to the centred image when zooming out from a panned corner", async () => {
    const { controller, state } = await createController({ imageWidth: 400, imageHeight: 400 });

    controller.fitImageToView();
    const fitted = imageEdges(controller, state);
    controller.zoomAt(20, 20, 4);
    controller.setEffectiveScroll(99999, 99999);
    controller.zoomAt(390, 290, 0.5);
    controller.fitImageToView();

    expect(imageEdges(controller, state)).toEqual(fitted);
    expect(fitted.left).toBe(50);
    expect(fitted.top).toBe(0);
  });

  it("fits the image clear of a floating panel, the gutter beside it included", async () => {
    // The panel stands 10 px in from the right edge; the image may not reach
    // under it, and stops FIT_GAP_PX (8) short of it.
    const { controller, state } = await createController({
      imageWidth: 200,
      imageHeight: 100,
      viewWidth: 600,
      viewHeight: 400,
      panelRect: { left: 300, right: 590, top: 0, bottom: 400 },
    });

    controller.fitImageToView();

    expect(imageEdges(controller, state)).toMatchObject({ left: 0, right: 292 });
  });

  it("fits the image below the menu bar and toolbar floating over the canvas", async () => {
    const { controller, state } = await createController({
      imageWidth: 200,
      imageHeight: 200,
      viewWidth: 400,
      viewHeight: 300,
      chromeBottom: 60,
    });

    controller.fitImageToView();

    // 300 - (60 + 8) = 232 px tall, centred across the 400 px width.
    const edges = imageEdges(controller, state);
    for (const [side, at] of Object.entries({ left: 84, top: 68, right: 316, bottom: 300 })) {
      expect(edges[side]).toBeCloseTo(at, 9);
    }
  });

  it("fits the image above a footer floating short of the canvas's bottom edge", async () => {
    // Footer 250..290 in a 300 px canvas: it covers 50 px, not its 40 px height.
    const { controller, state } = await createController({
      imageWidth: 200,
      imageHeight: 200,
      footerHeight: 40,
      footerTop: 250,
    });

    controller.fitImageToView();

    expect(imageEdges(controller, state).bottom).toBeCloseTo(300 - 50 - 8, 9);
  });

  it("keeps the image's corner under the cursor while zooming in from zoomed out", async () => {
    const { controller, state } = await createController({
      imageWidth: 487,
      imageHeight: 619,
      viewWidth: 1900,
      viewHeight: 1100,
      footerHeight: 40,
    });

    controller.fitImageToView();
    controller.setZoom(0.8);
    const corner = getScreenPoint(controller, state, 487, 619);
    for (let step = 0; step < 12; step += 1) {
      controller.zoomAt(corner.x, corner.y, state.zoom * 1.22);
      const after = getScreenPoint(controller, state, 487, 619);
      expect(after.x).toBeCloseTo(corner.x, 6);
      expect(after.y).toBeCloseTo(corner.y, 6);
    }
  });

  it("keeps a pixel near the edge under the cursor when zooming in on it", async () => {
    const { controller, state } = await createController({
      imageWidth: 2048,
      imageHeight: 2048,
      viewWidth: 1900,
      viewHeight: 1200,
    });

    controller.setZoom(0.3);
    const before = getScreenPoint(controller, state, 1979, 1424);
    controller.zoomAt(before.x, before.y, 3.3);
    const after = getScreenPoint(controller, state, 1979, 1424);

    expect(after.x).toBeCloseTo(before.x, 6);
    expect(after.y).toBeCloseTo(before.y, 6);
  });

  it("keeps the pixel under the cursor while zooming a small image inside the view", async () => {
    const { controller, state } = await createController({ imageWidth: 100, imageHeight: 100 });

    controller.setZoom(1);
    const before = getScreenPoint(controller, state, 80, 30);
    controller.zoomAt(before.x, before.y, 1.5);
    const after = getScreenPoint(controller, state, 80, 30);

    expect(after.x).toBeCloseTo(before.x, 6);
    expect(after.y).toBeCloseTo(before.y, 6);
  });

  it("keeps a strip of the image in view when zooming over the background beside it", async () => {
    const { controller, state } = await createController({
      imageWidth: 2000,
      imageHeight: 2000,
      viewWidth: 2400,
      viewHeight: 2000,
    });

    controller.setZoom(1);
    for (let step = 0; step < 10; step += 1) controller.zoomAt(2390, 1000, state.zoom * 1.5);

    expect(imageEdges(controller, state).right).toBeGreaterThanOrEqual(48 - 1e-6);
  });

  it("keeps the image point in the middle when the view is resized", async () => {
    const { controller, size } = await createController({ imageWidth: 2000, imageHeight: 2000 });

    controller.setZoom(2);
    controller.panToImageCenter(800, 600);
    size.clientWidth = 250;
    size.clientHeight = 500;
    controller.setZoom(2);

    expect(controller.getViewCenter()).toMatchObject({ centerX: 800, centerY: 600, zoom: 2 });
  });

  it("zooms with the slider about the middle of the view", async () => {
    const { controller } = await createController({ imageWidth: 2000, imageHeight: 2000 });

    controller.setZoom(1);
    controller.panToImageCenter(900, 1300);
    controller.setZoom(5);

    expect(controller.getViewCenter()).toMatchObject({ centerX: 900, centerY: 1300 });
  });

  it("outlines on the minimap exactly the part of the image in view", async () => {
    const { controller, state } = await createController({ imageWidth: 2000, imageHeight: 2000 });

    controller.setZoom(2);
    controller.setEffectiveScroll(1000, 2000);

    const rect = controller.getViewRect();
    expect(rect.viewX).toBeCloseTo(controller.getEffectiveScrollLeft() / state.zoom, 6);
    expect(rect.viewY).toBeCloseTo(controller.getEffectiveScrollTop() / state.zoom, 6);
    expect(rect.viewW).toBeCloseTo(200, 6);
    expect(rect.viewH).toBeCloseTo(150, 6);
  });



  it("centres a clicked minimap point, also near the image's edges", async () => {
    const { controller } = await createController({ imageWidth: 2000, imageHeight: 2000 });

    controller.setZoom(2);
    controller.panToImageCenter(700, 1200);
    expect(controller.getViewCenter()).toMatchObject({ centerX: 700, centerY: 1200 });

    controller.panToImageCenter(10, 1990);
    expect(controller.getViewCenter()).toMatchObject({ centerX: 10, centerY: 1990 });
  });

  it("resizes from a minimap handle with the full zoom range, in one step", async () => {
    const { controller, state, overviewState } = await createController({ imageWidth: 2000, imageHeight: 2000 });

    controller.setZoom(1);
    overviewState.anchor = { x: 100, y: 100 };
    controller.resizeViewFromHandle({ x: 140, y: 130 }, "se", false);

    expect(state.zoom).toBeCloseTo(10, 6);
    const center = controller.getViewCenter();
    expect(center.centerX).toBeCloseTo(120, 6);
    expect(center.centerY).toBeCloseTo(115, 6);
  });

  it("carries the view to a linked window as a centre and a zoom", async () => {
    const { controller, state } = await createController({ imageWidth: 2000, imageHeight: 2000 });

    controller.setViewCenter(1500, 400, 3);

    expect(state.zoom).toBe(3);
    expect(controller.getViewCenter()).toMatchObject({ centerX: 1500, centerY: 400, zoom: 3 });
  });

  it("zooms about the middle of the view when the wheel has no pointer position", async () => {
    const { controller, state } = await createController({ imageWidth: 2000, imageHeight: 2000 });

    controller.setZoom(1);
    controller.panToImageCenter(600, 900);
    controller.queueWheelZoom(-100);

    expect(state.zoom).toBeGreaterThan(1);
    expect(controller.getViewCenter()).toMatchObject({ centerX: 600, centerY: 900 });
  });

  it("applies wheel zoom immediately instead of waiting for a deferred animation step", async () => {
    const { controller, state } = await createController();

    controller.queueWheelZoom(-100, 200, 150);

    expect(state.zoom).toBeGreaterThan(1);
  });

  it("notifies callers when the viewport changes", async () => {
    const { controller, onViewportChanged } = await createController();

    controller.setEffectiveScroll(20, 30, false);

    expect(onViewportChanged).toHaveBeenCalledWith({ reason: "pan" });
  });

  it("resizes the overview's view from an edge about the middle of the side across", async () => {
    const { controller } = await createController();
    const view = { viewX: 100, viewY: 50, viewW: 200, viewH: 120 };
    expect(controller.getAnchorForHandle(view, "n", false)).toEqual({ x: 200, y: 170 });
    expect(controller.getAnchorForHandle(view, "s", false)).toEqual({ x: 200, y: 50 });
    expect(controller.getAnchorForHandle(view, "e", false)).toEqual({ x: 100, y: 110 });
    expect(controller.getAnchorForHandle(view, "w", false)).toEqual({ x: 300, y: 110 });
    expect(controller.getAnchorForHandle(view, "e", true)).toEqual({ x: 200, y: 110 });
  });
});
