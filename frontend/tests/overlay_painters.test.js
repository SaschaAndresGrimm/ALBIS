import { describe, expect, it } from "vitest";

import { canvasFont } from "../modules/canvas_fonts.js";
import {
  OPAQUE_OVERLAY_RGB,
  nearestOverlayColorIndex,
  paintPeakMarkers,
  paintResolutionRings,
  regionView,
  screenView,
  viewX,
} from "../modules/overlay_painters.js";

function recordingContext() {
  const ops = [];
  return {
    ops,
    of: (name) => ops.filter(([op]) => op === name),
    save: () => ops.push(["save"]),
    restore: () => ops.push(["restore"]),
    setLineDash: (...a) => ops.push(["setLineDash", ...a]),
    beginPath: () => ops.push(["beginPath"]),
    moveTo: (...a) => ops.push(["moveTo", ...a]),
    lineTo: (...a) => ops.push(["lineTo", ...a]),
    arc: (...a) => ops.push(["arc", ...a]),
    ellipse: (...a) => ops.push(["ellipse", ...a]),
    fill: () => ops.push(["fill"]),
    stroke: () => ops.push(["stroke"]),
    fillRect: (...a) => ops.push(["fillRect", ...a]),
    strokeText: (...a) => ops.push(["strokeText", ...a]),
    fillText: (...a) => ops.push(["fillText", ...a]),
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

const RING_PARAMS = {
  mode: "circle",
  distanceMm: 100,
  pixelSizeUm: 100, // 0.1 mm, so 1 mm of radius is 10 image pixels
  energyEv: 12398.4193, // 1 Angstrom
  centerX: 500,
  centerY: 500,
  centerKnown: true,
  rings: [3],
};

describe("the export and the image sampler agree on where a pixel lands", () => {
  // frameToIndices picks the source pixel for output column ox as
  //   srcX = region.x + floor(ox * region.width / ow)
  // The overlay has to land inside the same output column for the image pixel
  // it is marking, or a spot marker sits next to its spot.
  const region = { x: 37, y: 11, width: 160, height: 120 };
  const ow = 320;
  const oh = 240;
  const view = regionView(region, ow, oh);

  const sampledColumn = (ox) => region.x + Math.floor((ox * region.width) / ow);

  it.each([0, 1, 40, 99, 159])("keeps image column %i inside its own output column", (offset) => {
    const imageX = region.x + offset;
    const ox = Math.floor(viewX(view, imageX + 0.5));
    expect(sampledColumn(ox)).toBe(imageX);
  });

  it("puts the crop's top-left corner at the output origin", () => {
    expect(viewX(view, region.x)).toBe(0);
    expect(regionView(region, ow, oh).offsetY).toBeCloseTo(-region.y * (oh / region.height));
  });
});

describe("anisotropic pixels", () => {
  function ringGeometry(ctx, { view, pixelAspect }) {
    paintResolutionRings(ctx, { params: RING_PARAMS, view, pixelAspect });
    const [, , , radiusX, radiusY] = ctx.of("ellipse")[0];
    return { radiusX, radiusY };
  }

  it("draws a true circle on screen, where the view already stretches them", () => {
    // The viewport applies the aspect stretch itself (zoomY = zoom * aspect),
    // so a ring at a constant radius in mm is round on screen.
    const ctx = recordingContext();
    const { radiusX, radiusY } = ringGeometry(ctx, {
      view: screenView({ zoom: 2, zoomY: 2 * 3, scrollX: 0, scrollY: 0, offsetX: 0, offsetY: 0 }),
      pixelAspect: 3,
    });
    expect(radiusY).toBeCloseTo(radiusX);
  });

  it("draws the same ring as an ellipse in an unstretched export", () => {
    // An exported frame is raw pixels: three times as tall a pixel means a
    // third as many rows to the same physical radius.
    const ctx = recordingContext();
    const { radiusX, radiusY } = ringGeometry(ctx, {
      view: regionView({ x: 0, y: 0, width: 1000, height: 1000 }, 1000, 1000),
      pixelAspect: 3,
    });
    expect(radiusY).toBeCloseTo(radiusX / 3);
  });
});

describe("paintResolutionRings", () => {
  const view = regionView({ x: 0, y: 0, width: 1000, height: 1000 }, 1000, 1000);

  it("refuses geometry it cannot turn into a radius", () => {
    expect(paintResolutionRings(recordingContext(), { params: null, view })).toBe(false);
    expect(
      paintResolutionRings(recordingContext(), {
        params: { ...RING_PARAMS, energyEv: 0 },
        view,
      })
    ).toBe(false);
    expect(
      paintResolutionRings(recordingContext(), {
        params: { ...RING_PARAMS, distanceMm: 0 },
        view,
      })
    ).toBe(false);
  });

  it("scales line widths and the label with uiScale", () => {
    const plain = recordingContext();
    const scaled = recordingContext();
    paintResolutionRings(plain, { params: RING_PARAMS, view, uiScale: 1 });
    paintResolutionRings(scaled, { params: RING_PARAMS, view, uiScale: 4 });
    // Through canvasFont, so a change to the shared stack does not have to be
    // transcribed here -- only the size, which is what this test is about.
    expect(scaled.font).toBe(canvasFont(56));
    expect(plain.font).toBe(canvasFont(14));
    // The ring itself keeps its radius; only the ink around it grows.
    expect(scaled.of("ellipse")[0][3]).toBeCloseTo(plain.of("ellipse")[0][3]);
  });

  it("leaves the beam centre unmarked when the centre is a guess", () => {
    const known = recordingContext();
    const guessed = recordingContext();
    paintResolutionRings(known, { params: RING_PARAMS, view });
    paintResolutionRings(guessed, { params: { ...RING_PARAMS, centerKnown: false }, view });
    // The centre marker is the only thing here drawn with moveTo/lineTo.
    expect(known.of("moveTo").length).toBeGreaterThan(0);
    expect(guessed.of("moveTo")).toHaveLength(0);
  });
});

describe("paintPeakMarkers", () => {
  const view = regionView({ x: 0, y: 0, width: 100, height: 100 }, 100, 100);

  it("marks a peak at its pixel centre", () => {
    const ctx = recordingContext();
    paintPeakMarkers(ctx, { peaks: [{ x: 10, y: 20 }], view, width: 100, height: 100 });
    const [, cx, cy] = ctx.of("arc")[0];
    expect(cx).toBe(10.5);
    expect(cy).toBe(20.5);
  });

  it("skips peaks that fall outside the crop", () => {
    const ctx = recordingContext();
    paintPeakMarkers(ctx, {
      peaks: [{ x: 10, y: 20 }, { x: 400, y: 20 }],
      view,
      width: 100,
      height: 100,
    });
    // Two arcs per marker (halo + ring), so one marker survived.
    expect(ctx.of("arc")).toHaveLength(2);
  });

  it("draws nothing when there are no peaks and no external sets", () => {
    const ctx = recordingContext();
    paintPeakMarkers(ctx, { peaks: [], view, width: 100, height: 100 });
    expect(ctx.of("arc")).toHaveLength(0);
  });
});

describe("nearestOverlayColorIndex", () => {
  it("returns each reserved colour exactly", () => {
    OPAQUE_OVERLAY_RGB.forEach((rgb, index) => {
      expect(nearestOverlayColorIndex(...rgb)).toBe(index);
    });
  });

  it("snaps an anti-aliased blend to the nearer of the two it came from", () => {
    // Where a ring's blue meets its white halo, canvas hands back the mix.
    expect(nearestOverlayColorIndex(232, 238, 246)).toBe(0); // 90% white
    expect(nearestOverlayColorIndex(43, 97, 178)).toBe(1); // 10% white
  });

  it("holds no two colours close enough to pick between by accident", () => {
    // A near-duplicate would spend a palette slot on an invisible difference
    // and make anti-aliased edges land on either of the pair at random.
    for (let i = 0; i < OPAQUE_OVERLAY_RGB.length; i += 1) {
      for (let j = i + 1; j < OPAQUE_OVERLAY_RGB.length; j += 1) {
        const [a, b] = [OPAQUE_OVERLAY_RGB[i], OPAQUE_OVERLAY_RGB[j]];
        const distance = Math.hypot(a[0] - b[0], a[1] - b[1], a[2] - b[2]);
        expect(distance).toBeGreaterThan(96);
      }
    }
  });
});
