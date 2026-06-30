import { describe, expect, it, vi } from "vitest";

vi.mock("../modules/i18n.js", () => ({
  t: (_key, vars = {}) => `X ${vars.x} Y ${vars.y} Value ${vars.value}${vars.resolutionText || ""}`,
}));

import { createMaskCursorController } from "../modules/mask_cursor_controller.js";
import {
  getCircularRoiOuterRadius,
  physicalRoiRadius,
} from "../modules/roi_geometry_utils.js";

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

describe("physical resolution-shell ROI radius", () => {
  it("reduces to the ordinary pixel radius for square pixels", () => {
    expect(physicalRoiRadius(3, 4, 1)).toBe(5);
    expect(physicalRoiRadius(3, 4)).toBe(5); // default aspect
  });

  it("scales the Y offset by aspect so equal physical radii match across axes", () => {
    // With 4x-taller pixels, 10 px in Y spans the same physical distance as
    // 40 px in X, so both yield the same X-pixel-equivalent radius.
    expect(physicalRoiRadius(40, 0, 4)).toBe(40);
    expect(physicalRoiRadius(0, 10, 4)).toBe(40);
    // Mixed offset: sqrt(9 + (4*4)^2) = sqrt(265) ~= 16.28 -> 16
    expect(physicalRoiRadius(3, 4, 4)).toBe(16);
  });

  it("derives the outer radius from the drag endpoint using aspect", () => {
    const roiState = { start: { x: 0, y: 0 }, end: { x: 0, y: 10 } };
    expect(getCircularRoiOuterRadius(roiState, 4)).toBe(40);
    expect(getCircularRoiOuterRadius(roiState, 1)).toBe(10);
  });

  it("honours an explicit stored radius regardless of aspect", () => {
    const roiState = { start: { x: 0, y: 0 }, end: { x: 0, y: 10 }, outerRadius: 7 };
    expect(getCircularRoiOuterRadius(roiState, 4)).toBe(7);
  });
});
