import { describe, expect, it } from "vitest";

import { createOverlayRenderController } from "../modules/overlay_render_controller.js";
import { prepareRingGeometry } from "../modules/ring_geometry_utils.js";

function createResolutionContext() {
  const operations = [];
  return {
    operations,
    clearRect: (...args) => operations.push(["clearRect", ...args]),
    save: () => operations.push(["save"]),
    restore: () => operations.push(["restore"]),
    setLineDash: (...args) => operations.push(["setLineDash", ...args]),
    beginPath: () => operations.push(["beginPath"]),
    moveTo: (...args) => operations.push(["moveTo", ...args]),
    lineTo: (...args) => operations.push(["lineTo", ...args]),
    stroke: () => operations.push(["stroke"]),
    fillRect: (...args) => operations.push(["fillRect", ...args]),
    strokeText: (...args) => operations.push(["strokeText", ...args]),
    fillText: (...args) => operations.push(["fillText", ...args]),
    measureText: (text) => ({ width: String(text).length * 8 }),
    font: "",
    textBaseline: "middle",
    lineJoin: "round",
    lineCap: "round",
    lineWidth: 1,
    strokeStyle: "",
    fillStyle: "",
  };
}

function createGeometry() {
  return prepareRingGeometry({
    mode: "geometry",
    detector: "pilatus-12m-dls-cshape",
    source: "P12M_geometry/imported.expt",
    panels: [
      {
        name: "row-00",
        origin_mm: [-100, -90, 100],
        fast_axis: [1, 0, 0],
        slow_axis: [0, 1, 0],
        pixel_size_mm: [1, 1],
        image_size_px: [200, 80],
        raw_offset_px: [0, 0],
      },
      {
        name: "row-01",
        origin_mm: [-100, 10, 100],
        fast_axis: [1, 0, 0],
        slow_axis: [0, 1, 0],
        pixel_size_mm: [1, 1],
        image_size_px: [200, 80],
        raw_offset_px: [0, 100],
      },
    ],
  });
}

describe("overlay_render_controller", () => {
  it("renders geometry-mode rings when planar distance is zero", () => {
    const resolutionCtx = createResolutionContext();
    const controller = createOverlayRenderController({
      state: {
        hasFrame: true,
        zoom: 1,
        renderOffsetX: 0,
        renderOffsetY: 0,
      },
      analysisState: {
        ringsEnabled: true,
      },
      elements: {
        canvasWrap: { clientWidth: 400, clientHeight: 400 },
        pixelOverlay: null,
        pixelCtx: null,
        peakOverlay: null,
        peakCtx: null,
        resolutionOverlay: { width: 400, height: 400 },
        resolutionCtx,
      },
      constants: {
        pixelLabelDefaultMinCellPx: 12,
        pixelLabelDefaultMaxLabels: 1000,
        pixelLabelDenseZoomPx: 18,
        pixelLabelInteractionIdleMs: 120,
        pixelLabelHaloMaxLabels: 2000,
      },
      callbacks: {
        syncOverlayCanvas: () => ({ width: 400, height: 400 }),
        getActiveSaturationMax: () => null,
        getEffectiveScrollLeft: () => 0,
        getEffectiveScrollTop: () => 0,
        formatPixelLabelValue: () => "",
        isSaturatedValue: () => false,
        getRingParams: () => ({
          mode: "geometry",
          geometry: createGeometry(),
          geometrySource: "P12M_geometry/imported.expt",
          distanceMm: 0,
          pixelSizeUm: 172,
          energyEv: 12398.4193,
          centerX: 100,
          centerY: 100,
          centerKnown: true,
          rings: [5],
        }),
        updateRingsSectionState: () => {},
      },
    });

    controller.drawResolutionOverlay();

    expect(resolutionCtx.operations.some(([name]) => name === "lineTo")).toBe(true);
    expect(resolutionCtx.operations.some(([name]) => name === "strokeText")).toBe(true);
  });
});
