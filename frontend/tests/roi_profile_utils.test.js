import { describe, expect, it } from "vitest";

import {
  detectProfilePeaks,
  formatArea,
  formatMm,
  isResolutionCalibrated,
} from "../modules/roi_profile_utils.js";

describe("roi_profile_utils", () => {
  it("formats millimetres with precision that follows the magnitude", () => {
    expect(formatMm(123.456)).toBe("123.5");
    expect(formatMm(1.2345)).toBe("1.23");
    expect(formatMm(0.12345)).toBe("0.123");
    expect(formatMm(Number.NaN)).toBeNull();
  });

  it("formats areas and rejects negative or non-finite ones", () => {
    expect(formatArea(250.4)).toBe("250");
    expect(formatArea(2.54)).toBe("2.5");
    expect(formatArea(0.5)).toBe("0.500");
    expect(formatArea(0.001)).toBe("0.00100");
    expect(formatArea(-1)).toBeNull();
    expect(formatArea(Infinity)).toBeNull();
  });

  it("needs a known centre, distance, pixel size and energy in planar mode", () => {
    const planar = { centerKnown: true, distanceMm: 100, pixelSizeUm: 75, energyEv: 12000 };
    expect(isResolutionCalibrated(planar)).toBe(true);
    expect(isResolutionCalibrated({ ...planar, centerKnown: false })).toBe(false);
    expect(isResolutionCalibrated({ ...planar, distanceMm: 0 })).toBe(false);
    expect(isResolutionCalibrated(null)).toBe(false);
  });

  it("needs only energy and a geometry in geometry mode", () => {
    expect(isResolutionCalibrated({ mode: "geometry", energyEv: 12000, geometry: {} })).toBe(true);
    expect(isResolutionCalibrated({ mode: "geometry", energyEv: 12000, geometry: null })).toBe(false);
  });

  it("finds prominent peaks in axis units and ignores flat or short profiles", () => {
    const values = [0, 1, 10, 1, 0, 0, 0, 1, 5, 1, 0];
    expect(detectProfilePeaks(values, 100, 2)).toEqual([
      { x: 104, value: 10 },
      { x: 116, value: 5 },
    ]);
    expect(detectProfilePeaks([3, 3, 3, 3])).toEqual([]);
    expect(detectProfilePeaks([1, 2])).toEqual([]);
  });

  it("skips non-finite samples and drops peaks below the prominence floor", () => {
    const values = [0, Number.NaN, 100, 0, 0.5, 0, 0];
    expect(detectProfilePeaks(values)).toEqual([{ x: 2, value: 100 }]);
  });
});
