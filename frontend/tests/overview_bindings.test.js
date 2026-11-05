import { describe, expect, it, vi } from "vitest";
import { bindOverviewInteractions } from "../modules/overview_bindings.js";

function setup({ viewCenter, viewRect, point }) {
  const overviewCanvas = document.createElement("canvas");
  overviewCanvas.setPointerCapture = vi.fn();
  overviewCanvas.hasPointerCapture = () => false;
  overviewCanvas.releasePointerCapture = vi.fn();
  const panToImageCenter = vi.fn();
  const viewport = {
    getViewCenter: () => viewCenter,
    deferViewportInteraction: vi.fn(),
    normalizeWheelDelta: (event) => event.deltaY,
    queueWheelZoom: vi.fn(),
  };
  let current = point;
  bindOverviewInteractions({
    state: { hasFrame: true },
    overviewState: {},
    core: { overviewViewport: viewport },
    elements: { overviewCanvas },
    callbacks: {
      overviewEventToImage: () => current,
      overviewEventToOverview: () => ({ x: 0, y: 0 }),
      getViewRect: () => viewRect,
      getOverviewHandleAt: () => null,
      getAnchorForHandle: () => null,
      resizeViewFromHandle: vi.fn(),
      panToImageCenter,
    },
  });
  const fire = (type, init = {}) => {
    const event = new window.MouseEvent(type, { bubbles: true, cancelable: true, ...init });
    Object.defineProperty(event, "pointerId", { value: 1 });
    overviewCanvas.dispatchEvent(event);
    return event;
  };
  return { overviewCanvas, panToImageCenter, viewport, fire, moveTo: (next) => { current = next; } };
}

describe("overview_bindings", () => {
  it("grabs the outline without moving the view, also when the view is off the image's middle", () => {
    // A zoomed-out image moved aside: the outline is the whole image, the view centre is elsewhere.
    const { panToImageCenter, fire, moveTo } = setup({
      viewCenter: { centerX: -150, centerY: 40, zoom: 0.5 },
      viewRect: { viewX: 0, viewY: 0, viewW: 200, viewH: 150 },
      point: { x: 120, y: 90 },
    });

    fire("pointerdown");
    expect(panToImageCenter).toHaveBeenLastCalledWith(-150, 40);

    moveTo({ x: 130, y: 95 });
    fire("pointermove");
    expect(panToImageCenter).toHaveBeenLastCalledWith(-140, 45);
  });

  it("centres a spot clicked outside the outline", () => {
    const { panToImageCenter, fire } = setup({
      viewCenter: { centerX: 500, centerY: 500, zoom: 4 },
      viewRect: { viewX: 450, viewY: 460, viewW: 100, viewH: 80 },
      point: { x: 100, y: 900 },
    });

    fire("pointerdown");
    expect(panToImageCenter).toHaveBeenLastCalledWith(100, 900);
  });

  it("zooms about the middle of the view when scrolled over", () => {
    const { overviewCanvas, viewport } = setup({
      viewCenter: { centerX: 0, centerY: 0, zoom: 1 },
      viewRect: { viewX: 0, viewY: 0, viewW: 10, viewH: 10 },
      point: { x: 0, y: 0 },
    });

    const event = new window.WheelEvent("wheel", { deltaY: -100, bubbles: true, cancelable: true });
    overviewCanvas.dispatchEvent(event);

    expect(event.defaultPrevented).toBe(true);
    expect(viewport.queueWheelZoom).toHaveBeenCalledWith(-100);
  });
});
