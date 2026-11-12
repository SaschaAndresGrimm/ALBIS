/**
 * Where the histogram's BG and FG captions go.
 *
 * Each caption sits beside its marker line. With the levels far apart, BG
 * reads to the right of its line and FG to the left of its own, both on the
 * first row. Close together they turn outwards -- BG to the left, FG to the
 * right -- so the two captions do not cover each other or the gap between the
 * lines. Only when they still collide (both levels pinned at one edge, say)
 * does FG drop to a second row, a clear row below rather than touching.
 */

const EDGE = 8; // keep a caption this far from the plot's sides
const GAP = 6; // between a marker line and its caption
const SPACING = 6; // the least room between the two captions on one row

function clampX(x, labelWidth, width) {
  return Math.max(EDGE, Math.min(width - labelWidth - EDGE, x));
}

/**
 * @param {{minX: number, maxX: number, bgWidth: number, fgWidth: number, width: number}} input
 *   marker positions and caption widths in CSS pixels, and the plot's width
 * @returns {{bg: {x: number, row: number}, fg: {x: number, row: number}}}
 *   the left edge of each caption and its row (0 or 1)
 */
export function layoutLevelLabels({ minX, maxX, bgWidth, fgWidth, width }) {
  const inward = {
    bg: clampX(minX + GAP, bgWidth, width),
    fg: clampX(maxX - GAP - fgWidth, fgWidth, width),
  };
  if (inward.bg + bgWidth + SPACING <= inward.fg) {
    return { bg: { x: inward.bg, row: 0 }, fg: { x: inward.fg, row: 0 } };
  }
  const outward = {
    bg: clampX(minX - GAP - bgWidth, bgWidth, width),
    fg: clampX(maxX + GAP, fgWidth, width),
  };
  if (outward.bg + bgWidth + SPACING <= outward.fg) {
    return { bg: { x: outward.bg, row: 0 }, fg: { x: outward.fg, row: 0 } };
  }
  return { bg: { x: inward.bg, row: 0 }, fg: { x: clampX(maxX + GAP, fgWidth, width), row: 1 } };
}
