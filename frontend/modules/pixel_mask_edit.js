/**
 * Editing a pixel mask by hand (beta): which pixels a stroke or shape covers,
 * what masking or unmasking them changes, and undo.
 *
 * A pixel masked here gets USER_MASK_BIT. It is one of the bits every part of
 * ALBIS already treats as "defective" (bits 1-4), so it is drawn, left out of
 * the statistics and filled by inpainting like any defective pixel, and a
 * detector that applies the mask treats it the same way. Unmasking clears every
 * bit except a gap's: a module gap is where the detector has no pixel, and a
 * box dragged across one should not make it look like it has.
 *
 * Shapes cover a pixel when its centre lies inside them, so a box from one
 * pixel to another covers both, and a circle is round at any size.
 */

export const USER_MASK_BIT = 0x10;
const GAP_BIT = 0x1;
const HISTORY_LIMIT = 100;
// Past this many pixels touched, a stroke's duplicates are found with a flag
// per image pixel rather than a Set, which cannot hold past 2^24.
const SMALL_CHANGE = 1 << 20;

/** Call visit(index) for each pixel of the box between two corners. */
export function forEachInBox(a, b, width, height, visit) {
  const x0 = Math.max(0, Math.min(a.x, b.x));
  const x1 = Math.min(width - 1, Math.max(a.x, b.x));
  const y0 = Math.max(0, Math.min(a.y, b.y));
  const y1 = Math.min(height - 1, Math.max(a.y, b.y));
  for (let y = y0; y <= y1; y += 1) {
    const row = y * width;
    for (let x = x0; x <= x1; x += 1) visit(row + x);
  }
}

/** Each pixel whose centre is within `radius` of the centre of pixel `c`. */
export function forEachInCircle(c, radius, width, height, visit) {
  const r = Math.max(0, radius);
  const cx = c.x + 0.5;
  const cy = c.y + 0.5;
  const y0 = Math.max(0, Math.floor(cy - r));
  const y1 = Math.min(height - 1, Math.ceil(cy + r));
  for (let y = y0; y <= y1; y += 1) {
    const dy = y + 0.5 - cy;
    const half = r * r - dy * dy;
    if (half < 0) continue;
    const span = Math.sqrt(half);
    const x0 = Math.max(0, Math.ceil(cx - span - 0.5));
    const x1 = Math.min(width - 1, Math.floor(cx + span - 0.5));
    const row = y * width;
    for (let x = x0; x <= x1; x += 1) visit(row + x);
  }
}

/**
 * Each pixel whose centre lies inside the polygon (even-odd rule), and the
 * pixels on its outline: the corners clicked are covered, as a box's are. A
 * pixel may be visited twice.
 */
export function forEachInPolygon(points, width, height, visit) {
  if (!Array.isArray(points) || points.length < 3) return;
  const ys = points.map((p) => p.y + 0.5);
  const y0 = Math.max(0, Math.floor(Math.min(...ys)));
  const y1 = Math.min(height - 1, Math.ceil(Math.max(...ys)));
  const n = points.length;
  for (let y = y0; y <= y1; y += 1) {
    const sy = y + 0.5;
    const crossings = [];
    for (let i = 0; i < n; i += 1) {
      const a = points[i];
      const b = points[(i + 1) % n];
      const ay = a.y + 0.5;
      const by = b.y + 0.5;
      if ((ay <= sy && by > sy) || (by <= sy && ay > sy)) {
        crossings.push(a.x + 0.5 + ((sy - ay) / (by - ay)) * (b.x - a.x));
      }
    }
    crossings.sort((p, q) => p - q);
    const row = y * width;
    for (let k = 0; k + 1 < crossings.length; k += 2) {
      const x0 = Math.max(0, Math.ceil(crossings[k] - 0.5));
      const x1 = Math.min(width - 1, Math.floor(crossings[k + 1] - 0.5));
      for (let x = x0; x <= x1; x += 1) visit(row + x);
    }
  }
  for (let i = 0; i < n; i += 1) {
    for (const p of linePoints(points[i], points[(i + 1) % n])) {
      if (p.x >= 0 && p.y >= 0 && p.x < width && p.y < height) visit(p.y * width + p.x);
    }
  }
}

/** A square brush of `size` pixels centred on `p` (size 1: the pixel itself). */
export function forEachInBrush(p, size, width, height, visit) {
  const s = Math.max(1, Math.round(size));
  const lo = Math.floor((s - 1) / 2);
  forEachInBox({ x: p.x - lo, y: p.y - lo }, { x: p.x - lo + s - 1, y: p.y - lo + s - 1 }, width, height, visit);
}

/** The pixels from `a` to `b`, so a fast drag leaves no gaps (Bresenham). */
export function linePoints(a, b) {
  const points = [];
  let x = a.x;
  let y = a.y;
  const dx = Math.abs(b.x - x);
  const dy = -Math.abs(b.y - y);
  const sx = x < b.x ? 1 : -1;
  const sy = y < b.y ? 1 : -1;
  let err = dx + dy;
  for (;;) {
    points.push({ x, y });
    if (x === b.x && y === b.y) break;
    const e2 = 2 * err;
    if (e2 >= dy) {
      err += dy;
      x += sx;
    }
    if (e2 <= dx) {
      err += dx;
      y += sy;
    }
  }
  return points;
}

/** The value a pixel takes when masked or unmasked; unchanged if it already is. */
export function editedValue(value, mode) {
  if (mode === "unmask") return value & GAP_BIT;
  return value ? value : USER_MASK_BIT;
}

/**
 * One undoable change: the pixels it touched with their values before and
 * after, built while a stroke runs.
 *
 * Kept as two lists, not a Map from pixel to old value: a Map holds at most
 * 2^24 entries, and a box over a whole EIGER2 16M frame (18 million pixels)
 * threw. A pixel changed twice in a stroke is listed twice; finish keeps its
 * first entry, which has the value from before the stroke.
 */
export function createChange() {
  const touched = [];
  const old = [];
  return {
    /** Edit pixel `index` of `data`; returns whether its value changed. */
    apply(data, index, mode) {
      const value = data[index];
      const next = editedValue(value, mode);
      if (next === value) return false;
      touched.push(index);
      old.push(value);
      data[index] = next;
      return true;
    },
    /** Freeze into the record history keeps, or null if nothing changed. */
    finish(data) {
      // Seen pixels: a Set while it is small, a flag per pixel past that.
      const small = touched.length < SMALL_CHANGE;
      const seenSet = small ? new Set() : null;
      const seenFlags = small ? null : new Uint8Array(data.length);
      const indices = [];
      const was = [];
      for (let k = 0; k < touched.length; k += 1) {
        const index = touched[k];
        if (small ? seenSet.has(index) : seenFlags[index]) continue;
        if (small) seenSet.add(index);
        else seenFlags[index] = 1;
        if (data[index] === old[k]) continue;
        indices.push(index);
        was.push(old[k]);
      }
      if (!indices.length) return null;
      const idx = Uint32Array.from(indices);
      const now = new Uint32Array(idx.length);
      for (let k = 0; k < idx.length; k += 1) now[k] = data[idx[k]];
      return { idx, was: Uint32Array.from(was), now };
    },
  };
}

export function createHistory(limit = HISTORY_LIMIT) {
  let done = [];
  let undone = [];
  function put(data, record, values) {
    for (let k = 0; k < record.idx.length; k += 1) data[record.idx[k]] = values[k];
  }
  return {
    push(record) {
      if (!record) return;
      done.push(record);
      if (done.length > limit) done.shift();
      undone = [];
    },
    /** Undo the last change into `data`; returns its record, or null. */
    undo(data) {
      const record = done.pop();
      if (!record) return null;
      put(data, record, record.was);
      undone.push(record);
      return record;
    },
    redo(data) {
      const record = undone.pop();
      if (!record) return null;
      put(data, record, record.now);
      done.push(record);
      return record;
    },
    clear() {
      done = [];
      undone = [];
    },
    get canUndo() {
      return done.length > 0;
    },
    get canRedo() {
      return undone.length > 0;
    },
  };
}

/**
 * Which pixels differ from what editing started from, for the overlay: a flag
 * per pixel, how many are set, and the box round them all.
 *
 * Not a Set of indices: a Set holds at most 2^24, fewer than an EIGER2 16M
 * frame has pixels. Flags alone would make the overlay scan the whole frame on
 * every pointer move; the box keeps that scan as small as the edits, and it
 * only grows -- shrinking it would need a scan of its own.
 */
export function createChangedPixels() {
  let flags = new Uint8Array(0);
  let width = 0;
  let size = 0;
  let box = null;

  function grow(index) {
    const x = index % width;
    const y = (index - x) / width;
    if (!box) box = { x0: x, y0: y, x1: x, y1: y };
    else {
      if (x < box.x0) box.x0 = x;
      if (x > box.x1) box.x1 = x;
      if (y < box.y0) box.y0 = y;
      if (y > box.y1) box.y1 = y;
    }
  }

  return {
    /** Start over for `data` against `base`, an image `imageWidth` wide. */
    rebuild(data, base, imageWidth) {
      width = imageWidth;
      flags = new Uint8Array(data?.length || 0);
      size = 0;
      box = null;
      for (let i = 0; i < flags.length; i += 1) {
        if (data[i] === base[i]) continue;
        flags[i] = 1;
        size += 1;
        grow(i);
      }
    },
    /** Note what `indices` are now: changed or back to how they began. */
    track(data, base, indices) {
      for (const index of indices) {
        const now = data[index] !== base[index] ? 1 : 0;
        if (now === flags[index]) continue;
        flags[index] = now;
        size += now ? 1 : -1;
        if (now) grow(index);
      }
    },
    clear() {
      flags = new Uint8Array(0);
      size = 0;
      box = null;
    },
    get size() {
      return size;
    },
    /** Call visit(index) for each changed pixel inside `area` ({ x0, y0, x1, y1 }, inclusive). */
    forEachIn(area, visit) {
      if (!box || !size) return;
      const x0 = Math.max(box.x0, area.x0);
      const x1 = Math.min(box.x1, area.x1);
      const y0 = Math.max(box.y0, area.y0);
      const y1 = Math.min(box.y1, area.y1);
      for (let y = y0; y <= y1; y += 1) {
        const row = y * width;
        for (let x = x0; x <= x1; x += 1) if (flags[row + x]) visit(row + x);
      }
    },
  };
}

/** Pixels where `data` differs from `base`, as masked and unmasked counts. */
export function countEdits(base, data) {
  let masked = 0;
  let unmasked = 0;
  const n = Math.min(base?.length || 0, data?.length || 0);
  for (let i = 0; i < n; i += 1) {
    if (base[i] === data[i]) continue;
    // A gap stays a gap when unmasked, so only the other bits say which way.
    if (!(data[i] & ~GAP_BIT) && base[i] & ~GAP_BIT) unmasked += 1;
    else masked += 1;
  }
  return { masked, unmasked };
}
