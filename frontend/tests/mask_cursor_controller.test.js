import { describe, expect, it, vi } from "vitest";

vi.mock("../modules/i18n.js", () => ({
  t: (_key, vars = {}) => `X ${vars.x} Y ${vars.y} Value ${vars.value}${vars.resolutionText || ""}`,
}));

import { createMaskCursorController } from "../modules/mask_cursor_controller.js";

describe("mask_cursor_controller", () => {
  it("returns off-image coordinates for circular ROI drags when explicitly allowed", () => {
    const canvasWrap = document.createElement("div");
    canvasWrap.getBoundingClientRect = () => ({ left: 20, top: 30, width: 100, height: 100 });

    const controller = createMaskCursorController({
      apiBase: "/api",
      state: {
        hasFrame: true,
        width: 100,
        height: 100,
        zoom: 1,
        renderOffsetX: 0,
        renderOffsetY: 0,
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

    const point = controller.getImagePointFromEvent(
      { clientX: 10, clientY: 25 },
      { allowOutside: true, allowOutsideViewport: true },
    );

    expect(point).toEqual({ x: -10, y: -5 });
  });

  it("renders whole-number float samples without trailing decimals in the cursor readout", () => {
    const canvasWrap = document.createElement("div");
    canvasWrap.getBoundingClientRect = () => ({ left: 20, top: 30, width: 100, height: 100 });
    const canvasShell = document.createElement("div");
    canvasShell.getBoundingClientRect = () => ({ left: 20, top: 30, width: 100, height: 100 });
    const cursorOverlay = document.createElement("div");

    const controller = createMaskCursorController({
      apiBase: "/api",
      state: {
        hasFrame: true,
        width: 1,
        height: 1,
        zoom: 1,
        renderOffsetX: 0,
        renderOffsetY: 0,
        dataRaw: new Float32Array([24]),
        dtype: "<f4",
        maskEnabled: false,
        maskAvailable: false,
        maskRaw: null,
        maskShape: null,
        maskSaturatedEnabled: false,
      },
      elements: {
        canvasWrap,
        canvasShell,
        cursorOverlay,
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
        getDtypeInfo: vi.fn(() => ({ kind: "f", bits: 32 })),
        formatValue: vi.fn(() => "24.000"),
        isSaturatedValue: vi.fn(() => false),
        getResolutionAtPixel: vi.fn(() => null),
        getEffectiveScrollLeft: vi.fn(() => 0),
        getEffectiveScrollTop: vi.fn(() => 0),
        setAutoloadStatus: vi.fn(),
      },
    });

    controller.updateCursorOverlay({ clientX: 20.5, clientY: 30.5 });

    expect(cursorOverlay.textContent).toBe("X 0 Y 0 Value 24");
  });
});
