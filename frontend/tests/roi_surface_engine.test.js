import { describe, expect, it } from "vitest";
import {
  DEFAULT_SURFACE_CAMERA,
  SURFACE_KIND,
  SURFACE_MAX_CELLS,
  buildSurfaceGeometry,
  buildSurfaceGrid,
  clampSurfaceCamera,
  orbitSurfaceCamera,
  panSurfaceCamera,
  parseCssRgb,
  pickSurfaceVertex,
  projectSurfacePoint,
  surfaceBoxBounds,
  surfaceCellHeights,
  surfaceCellPixel,
  surfaceGridStep,
  surfaceHeightOf,
  surfaceVertexColors,
  surfaceViewMatrix,
} from "../modules/roi_surface_engine.js";

/** A frame `width` wide filled by `valueAt(x, y)`. */
function frame(width, height, valueAt, Type = Float64Array) {
  const data = new Type(width * height);
  for (let y = 0; y < height; y += 1) for (let x = 0; x < width; x += 1) data[y * width + x] = valueAt(x, y);
  return data;
}

describe("surfaceBoxBounds", () => {
  it("rounds the corners, puts them in order and keeps them on the frame", () => {
    const roi = { start: { x: 12.6, y: 40.2 }, end: { x: 3.4, y: -5 } };
    expect(surfaceBoxBounds(roi, 20, 30)).toEqual({ left: 3, top: 0, width: 11, height: 30 });
  });

  it("is null without both corners or a frame", () => {
    expect(surfaceBoxBounds({ start: { x: 1, y: 1 } }, 10, 10)).toBeNull();
    expect(surfaceBoxBounds({ start: { x: 1, y: 1 }, end: { x: 2, y: 2 } }, 0, 10)).toBeNull();
  });
});

describe("surfaceGridStep", () => {
  it("shows every pixel up to SURFACE_MAX_CELLS on the longer side, then blocks", () => {
    expect(surfaceGridStep(SURFACE_MAX_CELLS, 10)).toBe(1);
    expect(surfaceGridStep(10, SURFACE_MAX_CELLS + 1)).toBe(2);
    // An EIGER2 16M frame across: 4148 / 256 -> 17-pixel blocks.
    expect(surfaceGridStep(4148, 4362)).toBe(18);
    expect(surfaceGridStep(4148, 100)).toBe(17);
  });
});

describe("buildSurfaceGrid", () => {
  it("is the box's pixels one to one when it is small", () => {
    const data = frame(6, 5, (x, y) => x + 10 * y);
    const grid = buildSurfaceGrid({ data, frameWidth: 6, left: 1, top: 2, width: 3, height: 2 });
    expect(grid).toMatchObject({ cols: 3, rows: 2, step: 1, min: 21, max: 33 });
    expect(Array.from(grid.values)).toEqual([21, 22, 23, 31, 32, 33]);
    expect(Array.from(grid.kinds)).toEqual(Array(6).fill(SURFACE_KIND.value));
  });

  it("keeps the highest pixel of each block, so one strong spot still stands out", () => {
    const data = frame(10, 10, () => 1);
    data[7 * 10 + 8] = 500;
    const grid = buildSurfaceGrid({ data, frameWidth: 10, left: 0, top: 0, width: 10, height: 10, maxCells: 3 });
    // 10 pixels in at most 3 cells: blocks of 4, the last one 2 wide.
    expect(grid).toMatchObject({ cols: 3, rows: 3, step: 4, max: 500 });
    expect(grid.values[1 * 3 + 2]).toBe(500);
    expect(grid.values[0]).toBe(1);
  });

  it("marks gaps and broken pixels from the mask, and leaves them out of the heights", () => {
    const data = frame(3, 1, () => 7);
    const mask = new Uint32Array([1, 4, 0]);
    const grid = buildSurfaceGrid({ data, mask, frameWidth: 3, left: 0, top: 0, width: 3, height: 1 });
    expect(Array.from(grid.kinds)).toEqual([SURFACE_KIND.gap, SURFACE_KIND.defective, SURFACE_KIND.value]);
    expect(Number.isNaN(grid.values[0])).toBe(true);
    expect([grid.min, grid.max]).toEqual([7, 7]);
  });

  it("reads -1 and -2 as a gap and a broken pixel only when the data carries the flags", () => {
    const data = new Int32Array([-1, -2, 3]);
    const flagged = buildSurfaceGrid({ data, inDataFlags: true, frameWidth: 3, left: 0, top: 0, width: 3, height: 1 });
    expect(Array.from(flagged.kinds)).toEqual([SURFACE_KIND.gap, SURFACE_KIND.defective, SURFACE_KIND.value]);
    const measured = buildSurfaceGrid({ data, frameWidth: 3, left: 0, top: 0, width: 3, height: 1 });
    expect(Array.from(measured.values)).toEqual([-1, -2, 3]);
  });

  it("makes a block with a saturated pixel saturated, the highest thing in it", () => {
    const data = new Uint32Array([5, 65535, 9, 4]);
    const grid = buildSurfaceGrid({
      data,
      isSaturated: (raw) => raw >= 65535,
      frameWidth: 4,
      left: 0,
      top: 0,
      width: 4,
      height: 1,
      maxCells: 2,
    });
    expect(Array.from(grid.kinds)).toEqual([SURFACE_KIND.saturated, SURFACE_KIND.value]);
    // The saturated value does not set the scale for the others.
    expect(grid.max).toBe(9);
  });

  it("measures the frame as shown: corrected values, and inpainted pixels as filled", () => {
    const data = new Float64Array([10, 20, 30]);
    const shown = new Float64Array([1, 2, Number.NaN]);
    const holes = new Uint8Array([0, 0, 0]);
    const mask = new Uint32Array([0, 1, 0]);
    holes[1] = 1;
    const grid = buildSurfaceGrid({ data, shown, holes, mask, frameWidth: 3, left: 0, top: 0, width: 3, height: 1 });
    // The gap pixel was filled with 2 and shows; the flat had nothing for the third.
    expect(Array.from(grid.kinds)).toEqual([SURFACE_KIND.value, SURFACE_KIND.value, SURFACE_KIND.empty]);
    expect([grid.values[0], grid.values[1]]).toEqual([1, 2]);
  });

  it("handles a box over a whole EIGER2 16M frame in blocks", () => {
    const width = 4148;
    const height = 4362;
    const data = new Uint16Array(width * height);
    data[4000 * width + 4100] = 900;
    const grid = buildSurfaceGrid({ data, frameWidth: width, left: 0, top: 0, width, height });
    expect(grid.step).toBe(18);
    expect(grid.cols).toBe(231);
    expect(grid.rows).toBe(243);
    expect(grid.max).toBe(900);
    expect(grid.values[Math.floor(4000 / 18) * grid.cols + Math.floor(4100 / 18)]).toBe(900);
  });
});

describe("heights", () => {
  it("runs from the box's lowest value to its highest, linear or log", () => {
    expect(surfaceHeightOf(5, 0, 10)).toBe(0.5);
    expect(surfaceHeightOf(-3, 0, 10)).toBe(0);
    expect(surfaceHeightOf(30, 0, 10)).toBe(1);
    expect(surfaceHeightOf(9, 0, 99, true)).toBeCloseTo(0.5, 10);
    expect(surfaceHeightOf(5, 5, 5)).toBe(0);
  });

  it("puts saturated cells at the top and the rest on the floor", () => {
    const grid = {
      cols: 4,
      rows: 1,
      values: new Float64Array([0, 10, Number.NaN, Number.NaN]),
      kinds: new Uint8Array([SURFACE_KIND.value, SURFACE_KIND.value, SURFACE_KIND.saturated, SURFACE_KIND.gap]),
      min: 0,
      max: 10,
    };
    expect(Array.from(surfaceCellHeights(grid))).toEqual([0, 1, 1, 0]);
  });
});

describe("buildSurfaceGeometry", () => {
  const flat = (cols, rows) => buildSurfaceGrid({ data: new Float64Array(cols * rows).fill(3), frameWidth: cols, left: 0, top: 0, width: cols, height: rows });

  it("has a vertex per cell, two triangles per square and the lines between neighbours", () => {
    const geometry = buildSurfaceGeometry(flat(4, 3));
    expect(geometry.vertexCount).toBe(12);
    expect(geometry.triangles.length).toBe(3 * 2 * 6);
    expect(geometry.lines.length).toBe((3 * 3 + 4 * 2) * 2);
    // Flat: every normal points straight up.
    expect(Array.from(geometry.normals.slice(0, 3), (v) => v + 0)).toEqual([0, 0, 1]);
  });

  it("keeps the box's shape, the longer side from -1 to 1, with the image's top row at the back", () => {
    const geometry = buildSurfaceGeometry(flat(4, 2), { pixelAspect: 1 });
    expect([geometry.halfX, geometry.halfY]).toEqual([1, 0.5]);
    const [x0, y0] = geometry.positions;
    expect(x0).toBeCloseTo(-0.75);
    expect(y0).toBeCloseTo(0.25);
    const tall = buildSurfaceGeometry(flat(4, 2), { pixelAspect: 2 });
    expect([tall.halfX, tall.halfY]).toEqual([1, 1]);
  });

  it("stretches the heights with heightScale", () => {
    const data = new Float64Array([0, 10, 0, 10]);
    const grid = buildSurfaceGrid({ data, frameWidth: 2, left: 0, top: 0, width: 2, height: 2 });
    const one = buildSurfaceGeometry(grid);
    const two = buildSurfaceGeometry(grid, { heightScale: 2 });
    expect(two.zTop).toBeCloseTo(2 * one.zTop);
    expect(two.positions[5]).toBeCloseTo(two.zTop);
  });
});

describe("surfaceVertexColors", () => {
  it("takes each vertex's colour from colorOf, opaque", () => {
    const grid = { cols: 2, rows: 1 };
    expect(Array.from(surfaceVertexColors(grid, (i) => [i, 2, 3]))).toEqual([0, 2, 3, 255, 1, 2, 3, 255]);
  });
});

describe("camera", () => {
  it("keeps the pitch between level and straight down and the zoom in range", () => {
    const camera = clampSurfaceCamera({ ...DEFAULT_SURFACE_CAMERA, pitch: 5, zoom: 1000, yaw: 4 * Math.PI + 0.5 });
    expect(camera.pitch).toBeLessThan(Math.PI / 2);
    expect(camera.zoom).toBe(40);
    expect(camera.yaw).toBeCloseTo(0.5);
  });

  it("turns with a drag and moves with a pan", () => {
    const turned = orbitSurfaceCamera(DEFAULT_SURFACE_CAMERA, 100, 0);
    expect(turned.yaw).toBeCloseTo(DEFAULT_SURFACE_CAMERA.yaw - 1);
    const moved = panSurfaceCamera(DEFAULT_SURFACE_CAMERA, 50, 25, 100, 100);
    expect([moved.panX, moved.panY]).toEqual([1, -0.5]);
  });

  it("puts the middle of the surface in the middle of the view", () => {
    const matrix = surfaceViewMatrix(DEFAULT_SURFACE_CAMERA, 1.5, 0.8);
    const centre = projectSurfacePoint(matrix, 0, 0, 0.4, 300, 200);
    expect(centre.x).toBeCloseTo(150, 5);
    expect(centre.y).toBeCloseTo(100, 5);
  });

  it("looking straight down, shows the box as the image: top left at the top left", () => {
    const matrix = surfaceViewMatrix({ ...DEFAULT_SURFACE_CAMERA, yaw: 0, pitch: Math.PI / 2 }, 1, 0);
    const topLeft = projectSurfacePoint(matrix, -1, 1, 0, 100, 100);
    expect(topLeft.x).toBeLessThan(50);
    expect(topLeft.y).toBeLessThan(50);
  });

  it("picks the vertex under the mouse nearest the camera", () => {
    const matrix = surfaceViewMatrix({ ...DEFAULT_SURFACE_CAMERA, yaw: 0, pitch: Math.PI / 2 }, 1, 0);
    // Two points on the same line of sight from above: the higher one wins.
    const positions = new Float32Array([0, 0, 0, 0, 0, 0.5, 0.9, 0.9, 0]);
    expect(pickSurfaceVertex(positions, matrix, 50, 50, 100, 100)).toBe(1);
    expect(pickSurfaceVertex(positions, matrix, 5, 95, 100, 100)).toBe(-1);
  });
});

describe("surfaceCellPixel", () => {
  it("is the cell's first pixel and how many pixels it spans, short at the box's edge", () => {
    const grid = { cols: 3, step: 4, left: 100, top: 50, width: 10, height: 10 };
    expect(surfaceCellPixel(grid, 0)).toEqual({ x: 100, y: 50, columns: 4, rows: 4 });
    expect(surfaceCellPixel(grid, 8)).toEqual({ x: 108, y: 58, columns: 2, rows: 2 });
  });
});

describe("parseCssRgb", () => {
  it("reads a computed colour as 0..1", () => {
    expect(parseCssRgb("rgb(255, 0, 51)")).toEqual([1, 0, 0.2]);
    expect(parseCssRgb("")).toBeNull();
  });
});
