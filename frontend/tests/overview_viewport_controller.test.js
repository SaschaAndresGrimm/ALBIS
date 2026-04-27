import { afterEach, describe, expect, it, vi } from "vitest";

function createCanvasWrap({ clientWidth, clientHeight, scrollWidth, scrollHeight }) {
  const canvasWrap = document.createElement("div");
  let left = 0;
  let top = 0;

  Object.defineProperties(canvasWrap, {
    clientWidth: {
      configurable: true,
      get: () => clientWidth,
    },
    clientHeight: {
      configurable: true,
      get: () => clientHeight,
    },
    scrollWidth: {
      configurable: true,
      get: () => scrollWidth,
    },
    scrollHeight: {
      configurable: true,
      get: () => scrollHeight,
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
    width: clientWidth,
    height: clientHeight,
    right: clientWidth,
    bottom: clientHeight,
  });

  return canvasWrap;
}

async function createController({
  imageWidth = 200,
  imageHeight = 200,
  zoom = 1,
  viewWidth = 400,
  viewHeight = 300,
  scrollWidth = 400,
  scrollHeight = 400,
} = {}) {
  const { createOverviewViewportController } = await import("../modules/overview_viewport_controller.js");
  const state = {
    playing: false,
    hasFrame: true,
    width: imageWidth,
    height: imageHeight,
    zoom,
    panOffsetX: 0,
    panOffsetY: 0,
    renderOffsetX: 0,
    renderOffsetY: 0,
  };
  const canvasWrap = createCanvasWrap({
    clientWidth: viewWidth,
    clientHeight: viewHeight,
    scrollWidth,
    scrollHeight,
  });
  const canvas = document.createElement("canvas");

  const controller = createOverviewViewportController({
    state,
    overviewState: {},
    elements: {
      canvasWrap,
      canvas,
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
      deferPixelOverlayRedraw: vi.fn(),
      schedulePixelOverlay: vi.fn(),
      scheduleRoiOverlay: vi.fn(),
      scheduleResolutionOverlay: vi.fn(),
      schedulePeakOverlay: vi.fn(),
      requestFrame: vi.fn(),
      cancelActiveFrameLoad: vi.fn(),
      hasPendingFrameRequest: () => false,
      consumePendingFrameRequest: () => null,
      isFrameLoading: () => false,
      updateViewerFooter: vi.fn(),
    },
  });

  return { controller, state, canvasWrap, canvas };
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
        panOffsetX: 0,
        panOffsetY: 0,
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
        deferPixelOverlayRedraw: vi.fn(),
        schedulePixelOverlay: vi.fn(),
        scheduleRoiOverlay: vi.fn(),
        scheduleResolutionOverlay: vi.fn(),
        schedulePeakOverlay: vi.fn(),
        requestFrame,
        cancelActiveFrameLoad: vi.fn(),
        hasPendingFrameRequest: () => pendingFrame !== null,
        consumePendingFrameRequest: () => {
          const next = pendingFrame;
          pendingFrame = null;
          return next;
        },
        isFrameLoading: () => frameLoading,
        updateViewerFooter: vi.fn(),
      },
    });

    controller.deferViewportInteraction(60);
    pendingFrame = 9;
    frameLoading = false;
    await vi.advanceTimersByTimeAsync(72);

    expect(requestFrame).toHaveBeenCalledTimes(1);
    expect(requestFrame).toHaveBeenCalledWith(9);
  });

  it("allows edge overscroll even after the image becomes taller than the viewport", async () => {
    const { controller, state } = await createController();

    controller.setZoom(2);
    controller.setEffectiveScroll(0, -50, false);

    expect(controller.getEffectiveScrollTop()).toBe(-50);
    expect(state.panOffsetY).toBe(50);
  });

  it("does not snap the viewport back to the image edge when crossing the free-pan threshold", async () => {
    const { controller, state } = await createController();

    controller.setZoom(1.4);
    controller.setEffectiveScroll(0, -40, false);
    expect(controller.getEffectiveScrollTop()).toBe(-40);

    controller.setZoom(2);

    expect(controller.getEffectiveScrollTop()).toBe(-40);
    expect(state.panOffsetY).toBe(40);
  });

  it("does not let a large zoom-scaled image force the top edge to snap into view", async () => {
    const { controller, state } = await createController({
      imageWidth: 2000,
      imageHeight: 2000,
      viewWidth: 400,
      viewHeight: 300,
      scrollWidth: 2000,
      scrollHeight: 2000,
    });

    controller.setZoom(1);
    controller.setEffectiveScroll(0, -120, false);

    expect(controller.getEffectiveScrollTop()).toBe(-120);
    expect(state.panOffsetY).toBe(120);
  });
});
