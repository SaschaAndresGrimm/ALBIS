/**
 * The axis chip on an ROI plot: one click returns the plot to auto axes and
 * remembers the manual limits, the next click brings them back.
 */

const EMPTY_LIMITS = { xMin: null, xMax: null, yMin: null, yMax: null };

function isManual(limits) {
  return ["xMin", "xMax", "yMin", "yMax"].some((key) => Number.isFinite(limits?.[key]));
}

/** What a chip click does: `{ limits, stash }` to apply, or null if there is nothing to toggle. */
export function toggleAxisChip(limits, stash) {
  if (isManual(limits)) return { limits: { ...EMPTY_LIMITS }, stash: { ...limits } };
  if (stash) return { limits: { ...EMPTY_LIMITS, ...stash }, stash: null };
  return null;
}

/** "manual" while limits are set, "auto" while a stash waits to be restored, else "none" (chip hidden). */
export function axisChipState(limits, stash) {
  if (isManual(limits)) return "manual";
  return stash ? "auto" : "none";
}
