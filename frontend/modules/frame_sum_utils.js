/**
 * Several consecutive frames shown as one (the toolbar's Sum control).
 *
 * The frame shown is the sum (or mean) of frames i to i+N-1, with i the frame
 * index as before, so a weak signal shows and a long series can be looked
 * through N frames at a time. Pure: the toolbar, the loader and the labels all
 * ask these helpers, so they agree on what is on screen.
 */

// Any count from 1 (a single frame: the control off) to what the endpoint
// takes. A fixed list of sums left out 3, 7, 15 ...: the count is typed or
// stepped, and these are the one-click presets beside it.
export const MAX_FRAME_SUM = 1000;
export const FRAME_SUM_PRESETS = [2, 5, 10, 20, 50, 100];

// What `[` and `]` step through: roughly doubling, so 1 to 1000 is nine keys.
export const FRAME_SUM_LADDER = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000];

// The steps always offered. The sum in effect is added to them (stepOptionsFor),
// so the step can follow any sum: window after window, none skipped and none
// shown twice.
export const FRAME_STEP_OPTIONS = [1, 2, 5, 10, 20, 50, 100, 1000];

export const FRAME_SUM_OPERATIONS = ["sum", "mean"];

/** A whole count from 1 to MAX_FRAME_SUM; anything unreadable is 1. */
export function normalizeFrameSum(value) {
  const parsed = Math.round(Number(value));
  if (!Number.isFinite(parsed)) return 1;
  return Math.max(1, Math.min(MAX_FRAME_SUM, parsed));
}

/** The next count up (+1) or down (-1) the ladder from `current`. */
export function nextFrameSum(current, direction) {
  const count = normalizeFrameSum(current);
  if (direction > 0) return FRAME_SUM_LADDER.find((value) => value > count) ?? MAX_FRAME_SUM;
  return [...FRAME_SUM_LADDER].reverse().find((value) => value < count) ?? 1;
}

/** The steps to offer: the usual ones, the sum and the step in use, in order. */
export function stepOptionsFor(sum, step) {
  const extra = [normalizeFrameSum(sum), normalizeFrameSum(step)];
  return [...new Set([...FRAME_STEP_OPTIONS, ...extra])].sort((a, b) => a - b);
}

export function normalizeFrameSumOp(value) {
  return FRAME_SUM_OPERATIONS.includes(value) ? value : "sum";
}

/**
 * The frames the view covers: `{ start, end, count }`, 0-based and inclusive.
 * At the end of the series the window holds the frames left, as the backend
 * does, so the last frames are still shown.
 */
export function frameSumWindow(index, sum, total) {
  const frames = Math.max(1, Math.round(Number(total)) || 1);
  const start = Math.max(0, Math.min(frames - 1, Math.round(Number(index)) || 0));
  const count = Math.max(1, Math.min(normalizeFrameSum(sum), frames - start));
  return { start, end: start + count - 1, count };
}

/**
 * Whether the Sum control applies to what is open: a stack of frames in one
 * HDF5 dataset, or a series of one file per frame. The live sources go frame
 * by frame: their history is not a series the backend can read again.
 */
export function frameSumAvailable({ frameCount, liveMode }) {
  return !liveMode && Number(frameCount) > 1;
}

/** The sum in effect: the chosen one where it applies, else 1. */
export function effectiveFrameSum(sum, available) {
  return available ? normalizeFrameSum(sum) : 1;
}

/** The query that asks `/api/frame` for the window; empty for one frame. */
export function frameSumQuery(sum, op) {
  const count = normalizeFrameSum(sum);
  if (count <= 1) return "";
  return `&count=${count}&op=${normalizeFrameSumOp(op)}`;
}

/** Part of the frame cache key: a summed frame is not the plain one. */
export function frameSumCacheSuffix(sum, op) {
  const count = normalizeFrameSum(sum);
  return count <= 1 ? "" : `\u0000${normalizeFrameSumOp(op)}${count}`;
}

/**
 * The step after the sum changes from `previousSum` to `nextSum`.
 *
 * A step that matched the old sum follows the new one, so stepping and
 * playback go window by window. A step the user set to something else is
 * theirs and stays: a step of 1 over a sum of 10 is a sliding window.
 */
export function stepForFrameSum(step, previousSum, nextSum) {
  const current = Math.max(1, Math.round(Number(step)) || 1);
  if (current !== normalizeFrameSum(previousSum)) return current;
  return normalizeFrameSum(nextSum);
}

const LIVE_MODES = new Set(["simplon", "remote", "jungfraujoch"]);

/** Whether the Sum control applies to what `state` has open. */
export function frameSumAvailableFor(state) {
  return frameSumAvailable({
    frameCount: state?.frameCount,
    liveMode: LIVE_MODES.has(state?.autoload?.mode),
  });
}

/** The sum the view shows now: `state.frameSum` where it applies, else 1. */
export function activeFrameSum(state) {
  return effectiveFrameSum(state?.frameSum, frameSumAvailableFor(state));
}

/**
 * The frame position as shown in the toolbar: "121 / 3600", or with a sum,
 * "121–130 / 3600". The sum itself is said by the footer pill and the status.
 */
export function framePositionLabel(index, total, sum) {
  const frames = Math.max(1, Math.round(Number(total)) || 1);
  const { start, end, count } = frameSumWindow(index, sum, frames);
  if (count <= 1) return `${start + 1} / ${frames}`;
  return `${start + 1}–${end + 1} / ${frames}`;
}

/**
 * How many files an export writes with the sum on: one per window, as the
 * backend cuts them (the current frame's window, or a range in windows).
 */
export function frameSumExportCount(mode, frames, sum) {
  const count = normalizeFrameSum(sum);
  if (mode === "current") return 1;
  return Math.ceil(Math.max(0, frames) / count);
}

/** The part of an export's file names that says it is a sum: "_sum10". */
export function frameSumNameTag(sum, op) {
  const count = normalizeFrameSum(sum);
  return count > 1 ? `_${normalizeFrameSumOp(op)}${count}` : "";
}
