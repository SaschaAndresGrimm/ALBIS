import { describe, expect, it } from "vitest";

import {
  USER_MASK_BIT,
  countEdits,
  createChange,
  createChangedPixels,
  createHistory,
  editedValue,
  forEachInBox,
  forEachInBrush,
  forEachInCircle,
  forEachInPolygon,
  linePoints,
} from "../modules/pixel_mask_edit.js";

// The frame past 2^24 pixels takes about 2 s here and over 5 s, the default
// limit, on GitHub's runners, where it failed main's CI now and then.
const LARGE_FRAME_TIMEOUT_MS = 30_000;

function covered(width, height, cover) {
  const out = [];
  cover(width, height, (index) => out.push([index % width, Math.floor(index / width)]));
  return out.sort((a, b) => a[1] - b[1] || a[0] - b[0]);
}

describe("shapes", () => {
  it("covers a box between any two corners, both included, clipped to the image", () => {
    expect(covered(10, 10, (w, h, v) => forEachInBox({ x: 3, y: 2 }, { x: 1, y: 1 }, w, h, v))).toEqual([
      [1, 1], [2, 1], [3, 1],
      [1, 2], [2, 2], [3, 2],
    ]);
    expect(covered(4, 4, (w, h, v) => forEachInBox({ x: -5, y: 2 }, { x: 1, y: 9 }, w, h, v))).toEqual([
      [0, 2], [1, 2], [0, 3], [1, 3],
    ]);
  });

  it("covers the pixels whose centres are in a circle", () => {
    expect(covered(9, 9, (w, h, v) => forEachInCircle({ x: 4, y: 4 }, 0, w, h, v))).toEqual([[4, 4]]);
    const disc = covered(9, 9, (w, h, v) => forEachInCircle({ x: 4, y: 4 }, 1, w, h, v));
    expect(disc).toEqual([[4, 3], [3, 4], [4, 4], [5, 4], [4, 5]]);
    // Clipped at the edge, without wrapping into the next row.
    const edge = covered(5, 5, (w, h, v) => forEachInCircle({ x: 0, y: 2 }, 1.5, w, h, v));
    expect(edge.every(([x]) => x <= 1)).toBe(true);
  });

  it("covers the pixels whose centres are inside a polygon", () => {
    const square = [{ x: 1, y: 1 }, { x: 3, y: 1 }, { x: 3, y: 3 }, { x: 1, y: 3 }];
    // The same pixels as the box between those corners.
    const unique = (list) => Array.from(new Set(list.map(String))).map((p) => p.split(",").map(Number));
    expect(unique(covered(6, 6, (w, h, v) => forEachInPolygon(square, w, h, v)))).toEqual(
      covered(6, 6, (w, h, v) => forEachInBox({ x: 1, y: 1 }, { x: 3, y: 3 }, w, h, v)),
    );
    // A triangle: its corners and inside, nothing beyond.
    const triangle = [{ x: 0, y: 0 }, { x: 4, y: 0 }, { x: 0, y: 4 }];
    const cells = unique(covered(6, 6, (w, h, v) => forEachInPolygon(triangle, w, h, v)));
    expect(cells).toContainEqual([4, 0]);
    expect(cells).toContainEqual([0, 4]);
    expect(cells.every(([x, y]) => x + y <= 4)).toBe(true);
    expect(covered(6, 6, (w, h, v) => forEachInPolygon(square.slice(0, 2), w, h, v))).toEqual([]);
  });

  it("brushes a square around the pixel", () => {
    expect(covered(9, 9, (w, h, v) => forEachInBrush({ x: 4, y: 4 }, 1, w, h, v))).toEqual([[4, 4]]);
    expect(covered(9, 9, (w, h, v) => forEachInBrush({ x: 4, y: 4 }, 3, w, h, v))).toHaveLength(9);
  });

  it("joins the points of a fast drag", () => {
    expect(linePoints({ x: 0, y: 0 }, { x: 3, y: 1 })).toEqual([
      { x: 0, y: 0 }, { x: 1, y: 0 }, { x: 2, y: 1 }, { x: 3, y: 1 },
    ]);
    expect(linePoints({ x: 2, y: 2 }, { x: 2, y: 2 })).toEqual([{ x: 2, y: 2 }]);
  });
});

describe("editing values", () => {
  it("masks a clear pixel with the user bit and leaves a masked one as it is", () => {
    expect(editedValue(0, "mask")).toBe(USER_MASK_BIT);
    expect(editedValue(8, "mask")).toBe(8);
  });

  it("unmasks everything but a gap", () => {
    expect(editedValue(8, "unmask")).toBe(0);
    expect(editedValue(1 | 2, "unmask")).toBe(1);
    expect(editedValue(0, "unmask")).toBe(0);
  });

  it("records each pixel once, with the value from before the stroke", () => {
    const data = new Uint32Array([0, 4, 0]);
    const change = createChange();
    expect(change.apply(data, 0, "mask")).toBe(true);
    expect(change.apply(data, 0, "mask")).toBe(false);
    change.apply(data, 1, "unmask");
    change.apply(data, 1, "mask");
    const record = change.finish(data);
    expect(Array.from(record.idx)).toEqual([0, 1]);
    expect(Array.from(record.was)).toEqual([0, 4]);
    expect(Array.from(record.now)).toEqual([USER_MASK_BIT, USER_MASK_BIT]);
  });

  it("finishes a stroke that changed nothing as nothing", () => {
    const data = new Uint32Array([8]);
    const change = createChange();
    change.apply(data, 0, "mask");
    expect(change.finish(data)).toBeNull();
  });

  it("undoes and redoes, and a new change drops what was undone", () => {
    const data = new Uint32Array(4);
    const history = createHistory();
    const first = createChange();
    first.apply(data, 0, "mask");
    history.push(first.finish(data));
    const second = createChange();
    second.apply(data, 1, "mask");
    history.push(second.finish(data));

    history.undo(data);
    expect(Array.from(data)).toEqual([USER_MASK_BIT, 0, 0, 0]);
    expect(history.canRedo).toBe(true);
    history.redo(data);
    expect(data[1]).toBe(USER_MASK_BIT);

    history.undo(data);
    const third = createChange();
    third.apply(data, 3, "mask");
    history.push(third.finish(data));
    expect(history.canRedo).toBe(false);
    history.undo(data);
    history.undo(data);
    expect(Array.from(data)).toEqual([0, 0, 0, 0]);
    expect(history.canUndo).toBe(false);
  });

  it("counts what was masked and unmasked by hand", () => {
    const base = new Uint32Array([0, 8, 3, 0]);
    const data = new Uint32Array([USER_MASK_BIT, 0, 1, 0]);
    expect(countEdits(base, data)).toEqual({ masked: 1, unmasked: 2 });
  });
});

describe("pixels changed by hand", () => {
  it("counts them, knows where they are, and forgets one set back", () => {
    // 4 x 3 image; pixels 1 (1, 0) and 10 (2, 2) differ from the start.
    const base = new Uint32Array(12);
    const data = base.slice();
    data[1] = USER_MASK_BIT;
    data[10] = USER_MASK_BIT;
    const changed = createChangedPixels();
    changed.rebuild(data, base, 4);
    expect(changed.size).toBe(2);
    const seen = [];
    changed.forEachIn({ x0: 0, y0: 0, x1: 3, y1: 2 }, (i) => seen.push(i));
    expect(seen).toEqual([1, 10]);

    // Only the area asked for is looked at.
    seen.length = 0;
    changed.forEachIn({ x0: 2, y0: 1, x1: 3, y1: 2 }, (i) => seen.push(i));
    expect(seen).toEqual([10]);

    data[1] = 0;
    changed.track(data, base, [1, 1]);
    expect(changed.size).toBe(1);

    changed.clear();
    expect(changed.size).toBe(0);
  });

  it("takes an edit over a frame larger than a Set or a Map can hold, and undoes it", () => {
    // 4097 x 4097 is past 2^24 pixels, the most a Set or Map holds; an EIGER2
    // 16M frame (4148 x 4362) is too. A box over all of it used to throw.
    const width = 4097;
    const n = width * width;
    const base = new Uint8Array(n);
    const data = base.slice();
    const change = createChange();
    for (let i = 0; i < n; i += 1) change.apply(data, i, "mask");
    const record = change.finish(data);
    expect(record.idx).toHaveLength(n);

    const changed = createChangedPixels();
    changed.track(data, base, record.idx);
    expect(changed.size).toBe(n);

    const history = createHistory();
    history.push(record);
    history.undo(data);
    expect(data[n - 1]).toBe(0);
    changed.track(data, base, record.idx);
    expect(changed.size).toBe(0);
  }, LARGE_FRAME_TIMEOUT_MS);
});
