/**
 * The box ROI's 3D view, without the DOM: the pixels of the box as a grid of
 * heights, the mesh drawn from it, and the camera that looks at it.
 *
 * Each pixel is a point whose height is its value, as in adxv's 3-D magnify
 * window. A box larger than SURFACE_MAX_CELLS pixels on a side (the whole
 * detector) is shown as blocks of step × step pixels, each at the height of
 * its highest pixel, so a single strong spot still stands out; the view says
 * so under the plot.
 */

import { DATA_DEFECTIVE_VALUE, DATA_GAP_VALUE } from "./roi_stats_engine.js";

/** Points along the longer side of the box, at most. 256² cells is 65k points. */
export const SURFACE_MAX_CELLS = 256;

/** What a cell of the grid shows. A cell with no pixel left to show is EMPTY. */
export const SURFACE_KIND = Object.freeze({ value: 0, gap: 1, defective: 2, saturated: 3, empty: 4 });

// The surface's height at the box's highest value, in the units where the
// longer side of the box is 2.
const Z_BASE = 0.8;
const FIELD_OF_VIEW = (36 * Math.PI) / 180;
const CAMERA_DISTANCE = 4.2;
const PITCH_MIN = 0.02;
const PITCH_MAX = Math.PI / 2 - 0.001;

export const SURFACE_ZOOM_RANGE = Object.freeze([0.25, 40]);
export const SURFACE_HEIGHT_RANGE = Object.freeze([0.05, 20]);

/** Looking from in front of the box's bottom edge, a little to the left, 35° up. */
export const DEFAULT_SURFACE_CAMERA = Object.freeze({ yaw: -0.5, pitch: 0.62, zoom: 1.25, panX: 0, panY: 0, heightScale: 1 });

const MASK_GAP_BIT = 1;
const MASK_DEFECTIVE_BITS = 0x1e;
const SEEN_GAP = 1;
const SEEN_DEFECTIVE = 2;
const SEEN_SATURATED = 4;

/** The box the statistics measure: `roiState`'s corners, rounded and kept inside the frame. */
export function surfaceBoxBounds(roiState, frameWidth, frameHeight) {
  if (!roiState?.start || !roiState?.end || !(frameWidth > 0) || !(frameHeight > 0)) return null;
  const clampX = (v) => Math.max(0, Math.min(frameWidth - 1, Math.round(v)));
  const clampY = (v) => Math.max(0, Math.min(frameHeight - 1, Math.round(v)));
  const x0 = clampX(roiState.start.x);
  const x1 = clampX(roiState.end.x);
  const y0 = clampY(roiState.start.y);
  const y1 = clampY(roiState.end.y);
  if (![x0, x1, y0, y1].every(Number.isFinite)) return null;
  const left = Math.min(x0, x1);
  const top = Math.min(y0, y1);
  return { left, top, width: Math.abs(x1 - x0) + 1, height: Math.abs(y1 - y0) + 1 };
}

/** Pixels per cell side: 1 up to `maxCells` pixels, then as few as keep the grid within it. */
export function surfaceGridStep(width, height, maxCells = SURFACE_MAX_CELLS) {
  const longer = Math.max(1, Math.floor(width) || 1, Math.floor(height) || 1);
  return Math.max(1, Math.ceil(longer / Math.max(1, maxCells)));
}

/**
 * The box as a grid of cells: per cell the highest value shown in it and what
 * it is (SURFACE_KIND).
 *
 * The same pixels count as in the statistics: `data` is the raw frame,
 * `shown` the frame as displayed (flatfield-corrected, inpainted) or null,
 * `holes` the pixels inpainting filled (shown as their filled value), `mask`
 * the pixel mask when it is applied (bit 1 gap, bits 2-5 defective), and
 * `inDataFlags` whether -1 and -2 in the data mark gaps and broken pixels.
 * `isSaturated(raw)` is given only while saturated pixels are marked; a cell
 * holding one is saturated, since it is the highest thing in it.
 */
export function buildSurfaceGrid({
  data,
  shown = null,
  holes = null,
  mask = null,
  inDataFlags = false,
  isSaturated = null,
  frameWidth,
  left,
  top,
  width,
  height,
  maxCells = SURFACE_MAX_CELLS,
}) {
  if (!data || !(frameWidth > 0) || !(width > 0) || !(height > 0)) return null;
  const step = surfaceGridStep(width, height, maxCells);
  const cols = Math.ceil(width / step);
  const rows = Math.ceil(height / step);
  const count = cols * rows;
  const values = new Float64Array(count).fill(Number.NEGATIVE_INFINITY);
  const seen = new Uint8Array(count);
  const cellOfColumn = new Int32Array(width);
  for (let x = 0; x < width; x += 1) cellOfColumn[x] = Math.floor(x / step);

  for (let y = 0; y < height; y += 1) {
    const rowCell = Math.floor(y / step) * cols;
    const rowStart = (top + y) * frameWidth + left;
    for (let x = 0; x < width; x += 1) {
      const index = rowStart + x;
      const cell = rowCell + cellOfColumn[x];
      let value;
      if (holes !== null && holes[index]) {
        value = shown[index];
      } else {
        const raw = data[index];
        if (mask !== null) {
          const bits = mask[index];
          if (bits & MASK_GAP_BIT) {
            seen[cell] |= SEEN_GAP;
            continue;
          }
          if (bits & MASK_DEFECTIVE_BITS) {
            seen[cell] |= SEEN_DEFECTIVE;
            continue;
          }
        } else if (inDataFlags) {
          if (raw === DATA_GAP_VALUE) {
            seen[cell] |= SEEN_GAP;
            continue;
          }
          if (raw === DATA_DEFECTIVE_VALUE) {
            seen[cell] |= SEEN_DEFECTIVE;
            continue;
          }
        }
        if (isSaturated !== null && isSaturated(raw)) {
          seen[cell] |= SEEN_SATURATED;
          continue;
        }
        if (!Number.isFinite(raw)) continue;
        value = shown !== null ? shown[index] : raw;
      }
      if (value > values[cell]) values[cell] = value;
    }
  }

  const kinds = new Uint8Array(count);
  let min = Number.POSITIVE_INFINITY;
  let max = Number.NEGATIVE_INFINITY;
  for (let i = 0; i < count; i += 1) {
    const flags = seen[i];
    const value = values[i];
    if (flags & SEEN_SATURATED) {
      kinds[i] = SURFACE_KIND.saturated;
    } else if (Number.isFinite(value)) {
      kinds[i] = SURFACE_KIND.value;
      if (value < min) min = value;
      if (value > max) max = value;
      continue;
    } else if (flags & SEEN_GAP) {
      kinds[i] = SURFACE_KIND.gap;
    } else if (flags & SEEN_DEFECTIVE) {
      kinds[i] = SURFACE_KIND.defective;
    } else {
      kinds[i] = SURFACE_KIND.empty;
    }
    if (!Number.isFinite(value)) values[i] = Number.NaN;
  }
  if (!(min <= max)) {
    min = 0;
    max = 0;
  }
  return { cols, rows, step, left, top, width, height, values, kinds, min, max };
}

/** 0 at `lo`, 1 at `hi`; with `log`, log(1 + v - lo) over log(1 + hi - lo), so faint signal under a strong spot keeps some height. */
export function surfaceHeightOf(value, lo, hi, log = false) {
  if (!Number.isFinite(value) || !(hi > lo)) return 0;
  const span = hi - lo;
  const above = Math.min(Math.max(value - lo, 0), span);
  return log ? Math.log1p(above) / Math.log1p(span) : above / span;
}

/** The height of each cell, 0..1: values by surfaceHeightOf, saturated cells at the top, the rest on the floor. */
export function surfaceCellHeights(grid, log = false) {
  const count = grid.cols * grid.rows;
  const heights = new Float32Array(count);
  for (let i = 0; i < count; i += 1) {
    const kind = grid.kinds[i];
    if (kind === SURFACE_KIND.saturated) heights[i] = 1;
    else if (kind === SURFACE_KIND.value) heights[i] = surfaceHeightOf(grid.values[i], grid.min, grid.max, log);
  }
  return heights;
}

/**
 * The mesh: one vertex per cell, at the cell's centre, x to the right and y
 * up the image (the image's top row furthest back), the longer side from -1
 * to 1 and `pixelAspect` keeping a pixel's shape. `heightScale` stretches z.
 * Returns positions and normals (3 floats per vertex), the triangles of the
 * surface and the line pairs of the wire mesh (vertex indices), and zTop,
 * the height of the highest point.
 */
export function buildSurfaceGeometry(grid, { log = false, pixelAspect = 1, heightScale = 1 } = {}) {
  const { cols, rows } = grid;
  const count = cols * rows;
  const aspect = pixelAspect > 0 ? pixelAspect : 1;
  const extentX = cols;
  const extentY = rows * aspect;
  const longer = Math.max(extentX, extentY);
  const halfX = extentX / longer;
  const halfY = extentY / longer;
  const dx = (2 * halfX) / cols;
  const dy = (2 * halfY) / rows;
  const zTop = Z_BASE * heightScale;
  const heights = surfaceCellHeights(grid, log);

  const positions = new Float32Array(count * 3);
  for (let r = 0; r < rows; r += 1) {
    const y = halfY - (r + 0.5) * dy;
    for (let c = 0; c < cols; c += 1) {
      const i = r * cols + c;
      positions[i * 3] = -halfX + (c + 0.5) * dx;
      positions[i * 3 + 1] = y;
      positions[i * 3 + 2] = heights[i] * zTop;
    }
  }

  const normals = new Float32Array(count * 3);
  for (let r = 0; r < rows; r += 1) {
    for (let c = 0; c < cols; c += 1) {
      const i = r * cols + c;
      const cl = Math.max(0, c - 1);
      const cr = Math.min(cols - 1, c + 1);
      const ru = Math.max(0, r - 1);
      const rd = Math.min(rows - 1, r + 1);
      const dzdx = cr > cl ? (positions[(r * cols + cr) * 3 + 2] - positions[(r * cols + cl) * 3 + 2]) / ((cr - cl) * dx) : 0;
      // Row r - 1 lies further up y than row r + 1.
      const dzdy = rd > ru ? (positions[(ru * cols + c) * 3 + 2] - positions[(rd * cols + c) * 3 + 2]) / ((rd - ru) * dy) : 0;
      const length = Math.hypot(dzdx, dzdy, 1);
      normals[i * 3] = -dzdx / length;
      normals[i * 3 + 1] = -dzdy / length;
      normals[i * 3 + 2] = 1 / length;
    }
  }

  const quads = Math.max(0, cols - 1) * Math.max(0, rows - 1);
  const triangles = new Uint32Array(quads * 6);
  let t = 0;
  for (let r = 0; r + 1 < rows; r += 1) {
    for (let c = 0; c + 1 < cols; c += 1) {
      const a = r * cols + c;
      const b = a + 1;
      const d = a + cols;
      const e = d + 1;
      triangles[t++] = a;
      triangles[t++] = d;
      triangles[t++] = b;
      triangles[t++] = b;
      triangles[t++] = d;
      triangles[t++] = e;
    }
  }

  const lines = new Uint32Array((rows * Math.max(0, cols - 1) + cols * Math.max(0, rows - 1)) * 2);
  let l = 0;
  for (let r = 0; r < rows; r += 1) {
    for (let c = 0; c + 1 < cols; c += 1) {
      lines[l++] = r * cols + c;
      lines[l++] = r * cols + c + 1;
    }
  }
  for (let c = 0; c < cols; c += 1) {
    for (let r = 0; r + 1 < rows; r += 1) {
      lines[l++] = r * cols + c;
      lines[l++] = (r + 1) * cols + c;
    }
  }
  return { vertexCount: count, positions, normals, triangles, lines, halfX, halfY, zTop };
}

/** RGBA bytes per vertex, from `colorOf(index)` -> [r, g, b] (0-255). */
export function surfaceVertexColors(grid, colorOf) {
  const count = grid.cols * grid.rows;
  const colors = new Uint8Array(count * 4);
  for (let i = 0; i < count; i += 1) {
    const rgb = colorOf(i) || [0, 0, 0];
    colors[i * 4] = rgb[0];
    colors[i * 4 + 1] = rgb[1];
    colors[i * 4 + 2] = rgb[2];
    colors[i * 4 + 3] = 255;
  }
  return colors;
}

/** The camera with pitch kept between level and straight down, zoom and height in range. */
export function clampSurfaceCamera(camera) {
  const clamp = (v, [lo, hi]) => Math.min(hi, Math.max(lo, v));
  const turn = 2 * Math.PI;
  const yaw = ((((camera.yaw + Math.PI) % turn) + turn) % turn) - Math.PI;
  return {
    yaw: Number.isFinite(yaw) ? yaw : DEFAULT_SURFACE_CAMERA.yaw,
    pitch: clamp(Number.isFinite(camera.pitch) ? camera.pitch : DEFAULT_SURFACE_CAMERA.pitch, [PITCH_MIN, PITCH_MAX]),
    zoom: clamp(Number.isFinite(camera.zoom) ? camera.zoom : 1, SURFACE_ZOOM_RANGE),
    panX: Number.isFinite(camera.panX) ? camera.panX : 0,
    panY: Number.isFinite(camera.panY) ? camera.panY : 0,
    heightScale: clamp(Number.isFinite(camera.heightScale) ? camera.heightScale : 1, SURFACE_HEIGHT_RANGE),
  };
}

/** Dragging by (dx, dy) screen pixels turns the surface with the mouse and tips it towards or away. */
export function orbitSurfaceCamera(camera, dx, dy) {
  return clampSurfaceCamera({ ...camera, yaw: camera.yaw - dx * 0.01, pitch: camera.pitch + dy * 0.01 });
}

/** Moving the picture by (dx, dy) screen pixels on a view `width` × `height`. */
export function panSurfaceCamera(camera, dx, dy, width, height) {
  if (!(width > 0) || !(height > 0)) return camera;
  return clampSurfaceCamera({ ...camera, panX: camera.panX + (2 * dx) / width, panY: camera.panY - (2 * dy) / height });
}

function multiply(a, b) {
  const out = new Float32Array(16);
  for (let col = 0; col < 4; col += 1) {
    for (let row = 0; row < 4; row += 1) {
      let sum = 0;
      for (let k = 0; k < 4; k += 1) sum += a[k * 4 + row] * b[col * 4 + k];
      out[col * 4 + row] = sum;
    }
  }
  return out;
}

function lookAt(eye, target, up) {
  const fx = target[0] - eye[0];
  const fy = target[1] - eye[1];
  const fz = target[2] - eye[2];
  const fl = Math.hypot(fx, fy, fz) || 1;
  const f = [fx / fl, fy / fl, fz / fl];
  let sx = f[1] * up[2] - f[2] * up[1];
  let sy = f[2] * up[0] - f[0] * up[2];
  let sz = f[0] * up[1] - f[1] * up[0];
  const sl = Math.hypot(sx, sy, sz) || 1;
  sx /= sl;
  sy /= sl;
  sz /= sl;
  const ux = sy * f[2] - sz * f[1];
  const uy = sz * f[0] - sx * f[2];
  const uz = sx * f[1] - sy * f[0];
  const dot = (ax, ay, az) => ax * eye[0] + ay * eye[1] + az * eye[2];
  // Column-major, as WebGL takes it.
  return new Float32Array([
    sx, ux, -f[0], 0,
    sy, uy, -f[1], 0,
    sz, uz, -f[2], 0,
    -dot(sx, sy, sz), -dot(ux, uy, uz), dot(f[0], f[1], f[2]), 1,
  ]);
}

/**
 * Model, view and projection in one column-major matrix, for a view `aspect`
 * (width / height) wide. The camera circles the middle of the surface
 * (`zTop` its highest point); zoom narrows the field of view and pan moves
 * the picture on the screen, so neither distorts the perspective.
 */
export function surfaceViewMatrix(camera, aspect, zTop = Z_BASE) {
  const { yaw, pitch, zoom, panX, panY } = clampSurfaceCamera(camera);
  const target = [0, 0, zTop / 2];
  const eye = [
    target[0] + CAMERA_DISTANCE * Math.cos(pitch) * Math.sin(yaw),
    target[1] - CAMERA_DISTANCE * Math.cos(pitch) * Math.cos(yaw),
    target[2] + CAMERA_DISTANCE * Math.sin(pitch),
  ];
  // Looking straight down, "up" on the screen is the image's top.
  const up = [-Math.sin(yaw) * Math.sin(pitch), Math.cos(yaw) * Math.sin(pitch), Math.cos(pitch)];
  const view = lookAt(eye, target, up);
  const f = zoom / Math.tan(FIELD_OF_VIEW / 2);
  const safeAspect = aspect > 0 ? aspect : 1;
  const near = 0.05;
  const far = 50;
  const projection = new Float32Array([
    f / safeAspect, 0, 0, 0,
    0, f, 0, 0,
    0, 0, (far + near) / (near - far), -1,
    0, 0, (2 * far * near) / (near - far), 0,
  ]);
  const clip = multiply(projection, view);
  // Pan in clip space: x += panX * w, so the shift is the same on screen at every depth.
  for (let col = 0; col < 4; col += 1) {
    clip[col * 4] += panX * clip[col * 4 + 3];
    clip[col * 4 + 1] += panY * clip[col * 4 + 3];
  }
  return clip;
}

/** Where (x, y, z) lands on a `width` × `height` view, in pixels from the top left, with its depth w; null behind the camera. */
export function projectSurfacePoint(matrix, x, y, z, width, height) {
  const cx = matrix[0] * x + matrix[4] * y + matrix[8] * z + matrix[12];
  const cy = matrix[1] * x + matrix[5] * y + matrix[9] * z + matrix[13];
  const w = matrix[3] * x + matrix[7] * y + matrix[11] * z + matrix[15];
  if (!(w > 0)) return null;
  return { x: ((cx / w + 1) / 2) * width, y: ((1 - cy / w) / 2) * height, w };
}

/**
 * The vertex under the mouse at (sx, sy): of those within `radius` pixels,
 * the one nearest the camera, so a peak hides what is behind it. -1 if none.
 */
export function pickSurfaceVertex(positions, matrix, sx, sy, width, height, radius = 10) {
  let best = -1;
  let bestW = Number.POSITIVE_INFINITY;
  const r2 = radius * radius;
  const count = positions.length / 3;
  for (let i = 0; i < count; i += 1) {
    const p = projectSurfacePoint(matrix, positions[i * 3], positions[i * 3 + 1], positions[i * 3 + 2], width, height);
    if (!p) continue;
    const ddx = p.x - sx;
    const ddy = p.y - sy;
    if (ddx * ddx + ddy * ddy > r2) continue;
    if (p.w < bestW) {
      bestW = p.w;
      best = i;
    }
  }
  return best;
}

/** The image pixel at the middle of cell `index`, or its top-left pixel and block size when cells are blocks. */
export function surfaceCellPixel(grid, index) {
  const c = index % grid.cols;
  const r = Math.floor(index / grid.cols);
  const x = grid.left + c * grid.step;
  const y = grid.top + r * grid.step;
  return {
    x,
    y,
    columns: Math.min(grid.step, grid.left + grid.width - x),
    rows: Math.min(grid.step, grid.top + grid.height - y),
  };
}

/** An "r, g, b" colour from getComputedStyle as [r, g, b] in 0..1, or null. */
export function parseCssRgb(text) {
  const numbers = String(text || "").match(/-?\d+(\.\d+)?/g);
  if (!numbers || numbers.length < 3) return null;
  return numbers.slice(0, 3).map((n) => Math.min(1, Math.max(0, Number(n) / 255)));
}
