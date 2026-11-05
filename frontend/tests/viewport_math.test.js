import { describe, expect, it } from "vitest";
import {
  axisScroll,
  centerFromScroll,
  clampAxisCenter,
  fitZoom,
  visibleAxisSpan,
  zoomAxisAbout,
} from "../modules/viewport_math.js";

const axis = { start: 0, end: 400 };

describe("viewport_math", () => {
  it("lets an image that fits the span move, but not out of it", () => {
    // 200 px image in 400 px: centres from 0 (left edge at 0) to 200 (right edge at 400).
    expect(clampAxisCenter(30, { size: 200, scale: 1, axis })).toBe(30);
    expect(clampAxisCenter(-80, { size: 200, scale: 1, axis })).toBe(0);
    expect(clampAxisCenter(500, { size: 200, scale: 1, axis })).toBe(200);
    // Exactly as large as the span: one position, from either rule.
    expect(clampAxisCenter(30, { size: 200, scale: 2, axis })).toBe(100);
  });

  it("changes the limits without a jump as the image grows past the span", () => {
    const at = (scale) => clampAxisCenter(-1e9, { size: 200, scale, axis });
    expect(at(1.99999)).toBeCloseTo(at(2), 2);
    expect(at(2.00001)).toBeCloseTo(at(2), 2);
  });

  it("keeps a larger image's edges at the sides of the span", () => {
    // 1000 px image at 1x in 400 px: centres from 200 to 800.
    expect(clampAxisCenter(50, { size: 1000, scale: 1, axis })).toBe(200);
    expect(clampAxisCenter(950, { size: 1000, scale: 1, axis })).toBe(800);
    expect(clampAxisCenter(500, { size: 1000, scale: 1, axis })).toBe(500);
  });

  it("lets the far edge stop before a control floating over the image", () => {
    const withFooter = { start: 0, end: 300, edgeEnd: 252 };
    const center = clampAxisCenter(99999, { size: 1000, scale: 1, axis: withFooter });
    // Far edge on screen: mid + (size - center) * scale.
    expect(150 + (1000 - center)).toBe(252);
  });

  it("uses only the part of the canvas a floating panel leaves visible", () => {
    const besidePanel = { start: 0, end: 300 };
    // Right edge at most at 300: centre at least 200 - (300 - 150).
    expect(clampAxisCenter(0, { size: 200, scale: 1, axis: besidePanel })).toBe(50);
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
});
