import { describe, expect, it } from "vitest";

import { canvasFont } from "../modules/canvas_fonts.js";
import {
  OPAQUE_OVERLAY_COLORS,
  OPAQUE_OVERLAY_RGB,
  markerInkScale,
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
    // The width and colour travel with the stroke: nothing could otherwise
    // observe them, so the whole ink-scaling feature was untestable.
    stroke() {
      ops.push(["stroke", this.lineWidth, this.strokeStyle]);
    },
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


describe("marker ink thins with density, radius does not", () => {
  const view = regionView({ x: 0, y: 0, width: 600, height: 600 }, 600, 600);
  const peaks = (n) =>
    Array.from({ length: n }, (_, i) => ({ x: 20 + (i % 20) * 12, y: 20 + Math.floor(i / 20) * 12 }));

  it("leaves the default peak count at full weight", () => {
    expect(markerInkScale(25)).toBe(1);
    expect(markerInkScale(1)).toBe(1);
    expect(markerInkScale(0)).toBe(1);
  });

  it("thins meaningfully by a hundred markers", () => {
    const ink = markerInkScale(100);
    expect(ink).toBeGreaterThan(0.6);
    expect(ink).toBeLessThan(0.75);
  });

  it("stops thinning before the ring becomes a hairline", () => {
    // 1.7px is the amber ring's full weight; it must stay drawable.
    expect(markerInkScale(100000) * 1.7).toBeGreaterThan(0.9);
    expect(markerInkScale(1e9)).toBe(markerInkScale(500));
  });

  it("survives a nonsense count rather than producing NaN line widths", () => {
    for (const bad of [NaN, undefined, null, -5, "many"]) {
      expect(Number.isFinite(markerInkScale(bad))).toBe(true);
    }
  });

  it("keeps the marker radius identical at 25 and at 200 markers", () => {
    const few = recordingContext();
    const many = recordingContext();
    paintPeakMarkers(few, { peaks: peaks(25), view, width: 600, height: 600 });
    paintPeakMarkers(many, { peaks: peaks(200), view, width: 600, height: 600 });
    // arc args are (x, y, radius, ...); the first marker is at the same place.
    expect(many.of("arc")[0][2]).toBe(few.of("arc")[0][2]);
  });

  it("draws a thinner stroke for the same marker when there are more of them", () => {
    // Reproduce what the painter does, since lineWidth is not recorded per op.
    expect(3.4 * markerInkScale(200)).toBeLessThan(3.4 * markerInkScale(25));
  });
});

describe("ring labels keep off the ROI", () => {
  const view = regionView({ x: 0, y: 0, width: 1000, height: 1000 }, 1000, 1000);

  function labelBoxes(avoidImageRects) {
    const ctx = recordingContext();
    paintResolutionRings(ctx, { params: RING_PARAMS, view, avoidImageRects });
    // drawRingLabel fills its scrim with fillRect before writing the text.
    return ctx.of("fillRect").map(([, x, y, w, h]) => ({ x, y, w, h }));
  }

  const overlaps = (a, b) =>
    a.x < b.x + b.w && a.x + a.w > b.x && a.y < b.y + b.h && a.y + a.h > b.y;

  it("places its label somewhere with no ROI in the way", () => {
    expect(labelBoxes([]).length).toBeGreaterThan(0);
  });

  it("moves the label off a box ROI sitting where it would have gone", () => {
    const unobstructed = labelBoxes([])[0];
    expect(unobstructed).toBeTruthy();
    // An ROI covering exactly that spot, in image pixels.
    const roi = {
      x: unobstructed.x - 20,
      y: unobstructed.y - 20,
      width: unobstructed.w + 40,
      height: unobstructed.h + 40,
    };
    const moved = labelBoxes([roi]);
    for (const box of moved) {
      expect(overlaps(box, { x: roi.x, y: roi.y, w: roi.width, h: roi.height })).toBe(false);
    }
  });

  it("reserves something for a zero-height line ROI", () => {
    const unobstructed = labelBoxes([])[0];
    const line = { x: unobstructed.x - 40, y: unobstructed.y + unobstructed.h / 2, width: 200, height: 0 };
    const moved = labelBoxes([line]);
    // The inflation gives a flat ROI real extent, so the label has to move.
    if (moved.length) {
      expect(moved[0].y).not.toBeCloseTo(unobstructed.y);
    }
  });

  it("ignores malformed entries instead of throwing", () => {
    expect(() =>
      paintResolutionRings(recordingContext(), {
        params: RING_PARAMS,
        view,
        avoidImageRects: [null, undefined, {}, { x: NaN, y: 0, width: 10, height: 10 }],
      })
    ).not.toThrow();
  });
});


describe("the ink scaling actually reaches the canvas", () => {
  const view = regionView({ x: 0, y: 0, width: 600, height: 600 }, 600, 600);
  const peaks = (n) =>
    Array.from({ length: n }, (_, i) => ({ x: 20 + (i % 20) * 12, y: 20 + Math.floor(i / 20) * 12 }));

  function strokeWidths(opts) {
    const ctx = recordingContext();
    paintPeakMarkers(ctx, { view, width: 600, height: 600, ...opts });
    return ctx.of("stroke").map(([, lineWidth]) => lineWidth);
  }

  it("draws a thinner ring for 200 markers than for 25", () => {
    const few = strokeWidths({ peaks: peaks(25) });
    const many = strokeWidths({ peaks: peaks(200) });
    // Second stroke of each marker is the amber ring.
    expect(many[1]).toBeLessThan(few[1]);
    expect(few[1]).toBeCloseTo(1.7);
  });

  it("keeps an absolute dark fringe on the halo at the floor", () => {
    const many = strokeWidths({ peaks: peaks(400) });
    // Proportional part at the 0.55 floor, plus the fixed 1.7 fringe.
    expect(many[0]).toBeCloseTo(1.7 * 0.55 + 1.7, 5);
    // ... and strictly more than thinning the whole halo would have left.
    expect(many[0]).toBeGreaterThan(3.4 * 0.55);
  });

  it("leaves a selected marker at full weight however many others there are", () => {
    const widths = strokeWidths({ peaks: peaks(400), selectedPeaks: [0] });
    // The selected branch strokes halo 3.8 then ring 2.6, both unscaled.
    expect(widths[0]).toBeCloseTo(3.8);
    expect(widths[1]).toBeCloseTo(2.6);
  });

  it("does not thin at all in an export, however many markers there are", () => {
    // A GIF has no partial alpha to carry a thinned stroke, so the exporter
    // opts out and gets exactly the weights a 25-marker screen view gets.
    const exported = strokeWidths({
      peaks: peaks(400),
      uiScale: 1,
      colors: OPAQUE_OVERLAY_COLORS,
      thinWithDensity: false,
    });
    const screenFew = strokeWidths({ peaks: peaks(25) });
    expect(exported[0]).toBeCloseTo(screenFew[0], 5);
    expect(exported[1]).toBeCloseTo(screenFew[1], 5);
    expect(exported[0]).toBeCloseTo(3.4, 5);
  });

  it("scales an export's weights with uiScale and nothing else", () => {
    const [halo, ring] = strokeWidths({
      peaks: peaks(400),
      uiScale: 2,
      thinWithDensity: false,
    });
    expect(halo).toBeCloseTo(6.8, 5);
    expect(ring).toBeCloseTo(3.4, 5);
  });
});

describe("a concentric ROI never costs a ring its d-spacing", () => {
  const view = regionView({ x: 0, y: 0, width: 1000, height: 1000 }, 1000, 1000);
  const MANY_RINGS = { ...RING_PARAMS, rings: [8, 4, 3, 2, 1.5] };

  function labelCount(avoidImageRects) {
    const ctx = recordingContext();
    paintResolutionRings(ctx, { params: MANY_RINGS, view, avoidImageRects });
    return ctx.of("fillText").length;
  }

  it("labels every ring it strokes with no ROI present", () => {
    const ctx = recordingContext();
    paintResolutionRings(ctx, { params: MANY_RINGS, view });
    expect(ctx.of("fillText").length).toBeGreaterThan(1);
  });

  /**
   * The bug this exists for: the ROI was a hard veto, and a circular ROI
   * snapped to the beam centre (a one-click action) encloses whole rings, so
   * every label inside it was silently dropped however far it slid. An
   * unlabelled ring beside a labelled one invites reading the wrong d-spacing.
   */
  it.each([100, 300, 600, 900])(
    "keeps its labels with a concentric ROI of half-size %i",
    (half) => {
      const centred = {
        x: MANY_RINGS.centerX - half,
        y: MANY_RINGS.centerY - half,
        width: half * 2,
        height: half * 2,
      };
      expect(labelCount([centred])).toBe(labelCount([]));
    }
  );

  it("still refuses to stack two labels on top of each other", () => {
    // Hard boxes keep their veto: sibling labels are not negotiable.
    const ctx = recordingContext();
    // Collapse every ring onto nearly the same radius so their anchors collide.
    paintResolutionRings(ctx, {
      params: { ...RING_PARAMS, rings: [3, 3.001, 3.002, 3.003, 3.004] },
      view,
    });
    const boxes = ctx.of("fillRect").map(([, x, y, w, h]) => ({ x, y, w, h }));
    for (let i = 0; i < boxes.length; i += 1) {
      for (let j = i + 1; j < boxes.length; j += 1) {
        const [a, b] = [boxes[i], boxes[j]];
        expect(a.x < b.x + b.w && a.x + a.w > b.x && a.y < b.y + b.h && a.y + a.h > b.y).toBe(false);
      }
    }
  });
});
