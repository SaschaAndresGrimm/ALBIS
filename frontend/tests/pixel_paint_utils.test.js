import { describe, expect, it } from "vitest";

import { MARKER_RING_LIMIT, markersAsRings, paintPixels, visibleImageArea } from "../modules/pixel_paint_utils.js";

const RED = [255, 0, 0, 200];
const alphaAt = (buf, w, x, y) => buf[(y * w + x) * 4 + 3];

describe("pixel_paint_utils", () => {
  it("rings markers up to the limit and paints them past it", () => {
    const indices = (n) => ({ indices: new Array(n).fill(0) });
    expect(markersAsRings([indices(MARKER_RING_LIMIT)])).toBe(true);
    expect(markersAsRings([indices(MARKER_RING_LIMIT - 5), indices(6)])).toBe(false);
    expect(markersAsRings(null)).toBe(true);
  });

  it("paints every pixel found, the last rows as well as the first", () => {
    // More than the ring limit, spread over the whole image: the rings used to
    // stop after the first 20,000 in row order, so only the top was marked.
    const width = 400;
    const height = 200;
    const indices = [];
    for (let i = 0; i < width * height; i += 2) indices.push(i);
    expect(indices.length).toBeGreaterThan(MARKER_RING_LIMIT);
    const buf = new Uint8ClampedArray(width * height * 4);
    const view = { scaleX: 1, scaleY: 1, offsetX: 0, offsetY: 0 };

    const shown = paintPixels(buf, width, height, { indices, width, view, colorOf: () => RED });

    expect(shown).toBe(indices.length);
    expect(alphaAt(buf, width, 0, 0)).toBe(200);
    expect(alphaAt(buf, width, 1, 0)).toBe(0);
    expect(alphaAt(buf, width, width - 2, height - 1)).toBe(200);
  });

  it("covers a pixel's whole area when zoomed in, and skips what is out of view", () => {
    const buf = new Uint8ClampedArray(20 * 20 * 4);
    const view = { scaleX: 5, scaleY: 5, offsetX: -5, offsetY: 0 };
    // Image 4 wide: pixel 1 is (1, 0) -> screen 0..5; pixel 0 is left of the view.
    const shown = paintPixels(buf, 20, 20, { indices: [0, 1], width: 4, view, colorOf: () => RED });

    expect(shown).toBe(1);
    expect(alphaAt(buf, 20, 0, 0)).toBe(200);
    expect(alphaAt(buf, 20, 4, 4)).toBe(200);
    expect(alphaAt(buf, 20, 5, 0)).toBe(0);
  });

  it("still gives a pixel smaller than a device pixel one to show in", () => {
    const buf = new Uint8ClampedArray(10 * 10 * 4);
    const view = { scaleX: 0.25, scaleY: 0.25, offsetX: 0, offsetY: 0 };
    paintPixels(buf, 10, 10, { indices: [40 * 9 + 9], width: 40, view, dpr: 2, colorOf: () => RED });
    // (9, 9) at 0.25 x 2 = 4.5 -> device pixel (4, 4).
    expect(alphaAt(buf, 10, 4, 4)).toBe(200);
  });

  it("knows which image pixels a canvas shows", () => {
    // Zoomed in 4x, panned 100 css px left and 40 up, on a 200 x 120 canvas.
    const view = { scaleX: 4, scaleY: 4, offsetX: -100, offsetY: -40 };
    expect(visibleImageArea(view, 200, 120, 1000, 1000)).toEqual({ x0: 25, y0: 10, x1: 75, y1: 40 });
    // Clamped to the image, and nothing when it is out of view.
    expect(visibleImageArea({ scaleX: 1, scaleY: 1, offsetX: 50, offsetY: 0 }, 200, 100, 20, 20)).toEqual({ x0: 0, y0: 0, x1: 19, y1: 19 });
    expect(visibleImageArea({ scaleX: 1, scaleY: 1, offsetX: 500, offsetY: 0 }, 200, 100, 20, 20)).toBeNull();
  });
});
