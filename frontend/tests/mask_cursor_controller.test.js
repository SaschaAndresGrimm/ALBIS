import { describe, expect, it, vi } from "vitest";

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
});
