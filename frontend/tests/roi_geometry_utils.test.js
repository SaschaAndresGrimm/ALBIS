import { describe, expect, it } from "vitest";

import {
  applyCircularRoiGeometry,
  clampCircularRoiCenterDelta,
  clampCircularRoiInnerRadius,
  getCircularRoiOuterRadius,
  getVisibleCircularHandlePoint,
} from "../modules/roi_geometry_utils.js";

describe("roi_geometry_utils", () => {
  it("preserves circular ROI radius when the center moves to the image edge", () => {
    const roiState = {
      start: { x: 50, y: 50 },
      end: { x: 80, y: 50 },
      outerRadius: 30,
    };

    applyCircularRoiGeometry(roiState, { x: 50, y: 0 }, 30);

    expect(roiState.start).toEqual({ x: 50, y: 0 });
    expect(roiState.end).toEqual({ x: 80, y: 0 });
    expect(getCircularRoiOuterRadius(roiState)).toBe(30);
  });

  it("allows circular ROI centers to move beyond the detector bounds", () => {
    expect(clampCircularRoiCenterDelta(0, -80, { x: 50, y: 50 }, 200, 200)).toEqual({
      dx: 0,
      dy: -80,
    });
  });

  it("keeps annulus inner radius within the outer radius", () => {
    expect(clampCircularRoiInnerRadius(42, 18)).toBe(18);
  });

  it("projects the resize handle onto the visible detector arc when the preferred endpoint is off-frame", () => {
    const point = getVisibleCircularHandlePoint(
      { x: 50, y: -10 },
      30,
      { x: 1, y: 0 },
      { left: 0, top: 0, right: 100, bottom: 100 },
    );

    expect(point).toBeTruthy();
    expect(point.x).toBeCloseTo(78.284, 3);
    expect(point.y).toBeCloseTo(0, 6);
  });
});
