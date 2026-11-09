import { describe, expect, it } from "vitest";
import {
  axisScroll,
  centerFromScroll,
  centerInBox,
  clampAxisCenter,
  fitZoom,
  visibleAxisSpan,
  zoomAxisAbout,
} from "../modules/viewport_math.js";

const axis = { start: 0, end: 400 };

describe("viewport_math", () => {
  it("lets the image move until only a strip of it is left in view", () => {
    // 1000 px image at 1x in 400 px, 48 px kept: centres from 48 - 200 to 1000 + 200 - 48.
    expect(clampAxisCenter(500, { size: 1000, scale: 1, axis })).toBe(500);
    expect(clampAxisCenter(-9999, { size: 1000, scale: 1, axis })).toBe(-152);
    expect(clampAxisCenter(9999, { size: 1000, scale: 1, axis })).toBe(1152);
  });

  it("keeps the whole of an image smaller than the strip in view", () => {
    // 20 px image: its far edge no further left than 20, near edge no further right than 380.
    expect(clampAxisCenter(-9999, { size: 20, scale: 1, axis })).toBe(-180);
    expect(clampAxisCenter(9999, { size: 20, scale: 1, axis })).toBe(200);
  });

  it("changes the limits smoothly as the image grows", () => {
    const at = (scale) => clampAxisCenter(-1e9, { size: 200, scale, axis });
    for (const scale of [0.24, 0.5, 1.99, 2, 2.01, 5]) {
      expect(at(scale * 1.00001)).toBeCloseTo(at(scale), 1);
    }
  });

  it("never moves an image that is in view", () => {
    for (const center of [-100, 0, 250, 700, 1100]) {
      expect(clampAxisCenter(center, { size: 1000, scale: 1, axis })).toBe(center);
    }
  });

  it("keeps the strip above a control floating over the image", () => {
    const withFooter = { start: 0, end: 300, edgeEnd: 252 };
    const center = clampAxisCenter(-99999, { size: 1000, scale: 1, axis: withFooter });
    // Near edge on screen: mid - center * scale.
    expect(150 - center).toBe(252 - 48);
  });

  it("uses only the part of the canvas a floating panel leaves visible", () => {
    const besidePanel = { start: 0, end: 300 };
    // Near edge at most 300 - 48 = 252 on screen: centre at least (150 - 252) / 1.
    expect(clampAxisCenter(-9999, { size: 200, scale: 1, axis: besidePanel })).toBe(-102);
    expect(axisScroll(100, { scale: 1, axis: besidePanel })).toBe(-50);
  });

  it("keeps the point under the pointer while the scale changes", () => {
    const center = zoomAxisAbout(500, { point: 50, fromScale: 1, toScale: 4, axis });
    // Image point under screen x = 50 before: 500 + (50 - 200) / 1 = 350.
    expect(center + (50 - 200) / 4).toBeCloseTo(350, 9);
  });

  it("converts between a centre and the scroll the overlays use", () => {
    const scroll = axisScroll(640, { scale: 2.5, axis });
    expect(scroll).toBe(640 * 2.5 - 200);
    expect(centerFromScroll(scroll, { scale: 2.5, axis })).toBeCloseTo(640, 9);
  });

  it("reports the visible part of the image, clipped to the image", () => {
    expect(visibleAxisSpan(500, { size: 1000, scale: 2, axis })).toEqual({ from: 400, length: 200 });
    expect(visibleAxisSpan(100, { size: 200, scale: 1, axis })).toEqual({ from: 0, length: 200 });
  });

  it("fits the whole image, allowing for stretched pixels", () => {
    expect(fitZoom({ imageWidth: 400, imageHeight: 200, width: 800, height: 800 })).toBe(2);
    expect(fitZoom({ imageWidth: 400, imageHeight: 200, pixelAspect: 4, width: 800, height: 800 })).toBe(1);
    expect(fitZoom({ imageWidth: 0, imageHeight: 0, width: 800, height: 800 })).toBeNull();
  });

  it("centres the image in the clear part of the view", () => {
    // 200 px image at 1x; the clear box 100..300 of a 0..300 axis has its middle at 200.
    const center = centerInBox({ size: 200, scale: 1, axis: { start: 0, end: 300 }, box: { start: 100, end: 300 } });
    // The image middle (100) lands on screen at mid + (100 - center) * scale.
    expect(150 + (100 - center)).toBe(200);
    expect(centerInBox({ size: 200, scale: 2, axis, box: axis })).toBe(100);
  });
});
