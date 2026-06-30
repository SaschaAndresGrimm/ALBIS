import { describe, expect, it, vi } from "vitest";

vi.mock("../modules/i18n.js", () => ({
  t: (_key, vars = {}) => `X ${vars.x} Y ${vars.y} Value ${vars.value}${vars.resolutionText || ""}`,
}));

import { createMaskCursorController } from "../modules/mask_cursor_controller.js";

function makeController(state) {
  const canvasWrap = document.createElement("div");
  canvasWrap.getBoundingClientRect = () => ({ left: 0, top: 0, width: 1000, height: 1000 });
  return createMaskCursorController({
    apiBase: "/api",
    state: {
      hasFrame: true,
      width: 1000,
      height: 1000,
      renderOffsetX: 0,
      renderOffsetY: 0,
      ...state,
    },
    elements: {
      canvasWrap,
      canvasShell: document.createElement("div"),
      cursorOverlay: document.createElement("div"),
      histTooltip: document.createElement("div"),
      maskToggle: document.createElement("input"),
      maskSaturatedToggle: document.createElement("input"),
      simplonUrl: document.createElement("input"),
      simplonVersion: document.createElement("input"),
    },
    callbacks: {
      isHdfFile: vi.fn(),
      parseDtype: vi.fn(),
      parseShape: vi.fn(),
      typedArrayFrom: vi.fn(),
      getActiveSaturationMax: vi.fn(() => null),
      updateGlobalStats: vi.fn(),
      redraw: vi.fn(),
      scheduleRoiUpdate: vi.fn(),
      getDtypeInfo: vi.fn(),
      formatValue: vi.fn(),
      isSaturatedValue: vi.fn(() => false),
      getResolutionAtPixel: vi.fn(() => null),
      getEffectiveScrollLeft: vi.fn(() => 0),
      getEffectiveScrollTop: vi.fn(() => 0),
      setAutoloadStatus: vi.fn(),
    },
  });
}

describe("non-square (strixel) pixel aspect", () => {
  it("compresses the Y data coordinate by pixelAspect while leaving X as the reference axis", () => {
    // pixelAspect = y_pixel_size / x_pixel_size. With aspect = 2 the Y axis is
    // stretched 2x on screen, so a given screen offset maps to half as many
    // data pixels in Y as in X.
    const controller = makeController({ zoom: 10, pixelAspect: 2 });
    const point = controller.getImagePointFromEvent(
      { clientX: 37, clientY: 37 },
      { allowOutside: true },
    );
    // X: 37 / 10 = 3.7 -> 3 ; Y: 37 / (10 * 2) = 1.85 -> 1
    expect(point).toEqual({ x: 3, y: 1 });
  });

  it("is identical to the isotropic case when pixelAspect is 1", () => {
    const controller = makeController({ zoom: 10, pixelAspect: 1 });
    const point = controller.getImagePointFromEvent(
      { clientX: 37, clientY: 37 },
      { allowOutside: true },
    );
    expect(point).toEqual({ x: 3, y: 3 });
  });

  it("defaults to square pixels when pixelAspect is missing", () => {
    const controller = makeController({ zoom: 10 });
    const point = controller.getImagePointFromEvent(
      { clientX: 37, clientY: 37 },
      { allowOutside: true },
    );
    expect(point).toEqual({ x: 3, y: 3 });
  });
});
