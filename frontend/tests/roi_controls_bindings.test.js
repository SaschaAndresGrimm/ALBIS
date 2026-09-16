import { describe, expect, it, vi } from "vitest";

import { bindRoiControlInteractions } from "../modules/roi_controls_bindings.js";

describe("roi_controls_bindings", () => {
  it("keeps circle radius when numeric input extends beyond the detector edge", () => {
    const roiRadiusInput = document.createElement("input");
    roiRadiusInput.value = "40";
    const roiCenterXInput = document.createElement("input");
    const roiCenterYInput = document.createElement("input");

    const roiState = {
      enabled: true,
      active: true,
      mode: "circle",
      start: { x: 90, y: 10 },
      end: { x: 90, y: 10 },
      innerRadius: 0,
      outerRadius: 0,
    };

    bindRoiControlInteractions({
      state: { width: 100, height: 100 },
      roiState,
      elements: {
        roiEnableToggle: document.createElement("input"),
        roiModeSelect: document.createElement("select"),
        roiHistogramToggle: document.createElement("input"),
        roiClearBtn: document.createElement("button"),
        roiExportCsvBtn: document.createElement("button"),
        roiRadiusInput,
        roiInnerInput: document.createElement("input"),
        roiOuterInput: document.createElement("input"),
        roiCenterXInput,
        roiCenterYInput,
        canvasWrap: document.createElement("div"),
      },
      callbacks: {
        setRoiDragging: vi.fn(),
        stopRoiEdit: vi.fn(),
        updateRoiModeUI: vi.fn(),
        scheduleRoiOverlay: vi.fn(),
        scheduleRoiUpdate: vi.fn(),
        clearRoi: vi.fn(),
        setStatus: vi.fn(),
        exportRoiCsv: vi.fn(),
        applyRoiCenterFromInputs: vi.fn(() => null),
        updateRoiCenterInputs: vi.fn(),
      },
    });

    roiRadiusInput.dispatchEvent(new window.Event("change"));

    expect(roiState.outerRadius).toBe(40);
    expect(roiState.end.x).toBe(130);
    expect(roiState.end.y).toBe(10);
  });

  it("accepts off-image circle centers from numeric inputs", () => {
    const roiCenterXInput = document.createElement("input");
    const roiCenterYInput = document.createElement("input");
    const roiRadiusInput = document.createElement("input");
    roiCenterXInput.value = "-15";
    roiCenterYInput.value = "125";
    roiRadiusInput.value = "20";

    const roiState = {
      enabled: true,
      active: true,
      mode: "circle",
      start: { x: 10, y: 10 },
      end: { x: 30, y: 10 },
      innerRadius: 0,
      outerRadius: 20,
    };

    bindRoiControlInteractions({
      state: { width: 100, height: 100 },
      roiState,
      elements: {
        roiEnableToggle: document.createElement("input"),
        roiModeSelect: document.createElement("select"),
        roiHistogramToggle: document.createElement("input"),
        roiClearBtn: document.createElement("button"),
        roiExportCsvBtn: document.createElement("button"),
        roiRadiusInput,
        roiInnerInput: document.createElement("input"),
        roiOuterInput: document.createElement("input"),
        roiCenterXInput,
        roiCenterYInput,
        canvasWrap: document.createElement("div"),
      },
      callbacks: {
        setRoiDragging: vi.fn(),
        stopRoiEdit: vi.fn(),
        updateRoiModeUI: vi.fn(),
        scheduleRoiOverlay: vi.fn(),
        scheduleRoiUpdate: vi.fn(),
        clearRoi: vi.fn(),
        setStatus: vi.fn(),
        exportRoiCsv: vi.fn(),
        applyRoiCenterFromInputs: vi.fn(() => ({ x: -15, y: 125 })),
        updateRoiCenterInputs: vi.fn(),
      },
    });

    roiCenterXInput.dispatchEvent(new window.Event("change"));

    expect(roiState.start).toEqual({ x: -15, y: 125 });
    expect(roiState.end).toEqual({ x: 5, y: 125 });
    expect(roiState.outerRadius).toBe(20);
  });

  it("refreshes the statistics after clearing the ROI so whole-image values return", () => {
    const roiClearBtn = document.createElement("button");
    const roiState = {
      enabled: true,
      active: true,
      mode: "rect",
      start: { x: 10, y: 10 },
      end: { x: 30, y: 30 },
      innerRadius: 0,
      outerRadius: 0,
    };
    const scheduleRoiUpdate = vi.fn();
    const clearRoi = vi.fn(() => {
      roiState.start = null;
      roiState.end = null;
      roiState.active = false;
    });

    bindRoiControlInteractions({
      state: { width: 100, height: 100 },
      roiState,
      elements: {
        roiEnableToggle: document.createElement("input"),
        roiModeSelect: document.createElement("select"),
        roiHistogramToggle: document.createElement("input"),
        roiClearBtn,
        roiExportCsvBtn: document.createElement("button"),
        roiRadiusInput: document.createElement("input"),
        roiInnerInput: document.createElement("input"),
        roiOuterInput: document.createElement("input"),
        roiCenterXInput: document.createElement("input"),
        roiCenterYInput: document.createElement("input"),
        canvasWrap: document.createElement("div"),
      },
      callbacks: {
        setRoiDragging: vi.fn(),
        stopRoiEdit: vi.fn(),
        updateRoiModeUI: vi.fn(),
        scheduleRoiOverlay: vi.fn(),
        scheduleRoiUpdate,
        clearRoi,
        setStatus: vi.fn(),
        exportRoiCsv: vi.fn(),
        applyRoiCenterFromInputs: vi.fn(() => null),
        updateRoiCenterInputs: vi.fn(),
      },
    });

    roiClearBtn.dispatchEvent(new window.Event("click"));

    expect(clearRoi).toHaveBeenCalledTimes(1);
    expect(scheduleRoiUpdate).toHaveBeenCalledTimes(1);
  });
});
