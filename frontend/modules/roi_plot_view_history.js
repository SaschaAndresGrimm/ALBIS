/**
 * The views an ROI plot has been through, per plot, for its Previous view
 * button: a double-click reset or a zoom went straight to the new range, and
 * getting back to a range you had zoomed into took the whole zoom again.
 *
 * A view is the plot's axis limits ({ xMin, xMax, yMin, yMax }, null for an
 * axis that follows the data). Pure state, no DOM.
 */

const DEFAULT_LIMIT = 20;

function sameView(a, b) {
  return a && b && a.xMin === b.xMin && a.xMax === b.xMax && a.yMin === b.yMin && a.yMax === b.yMax;
}

export function createRoiPlotViewHistory({ limit = DEFAULT_LIMIT } = {}) {
  const stacks = new Map();

  function stackFor(key) {
    if (!stacks.has(key)) stacks.set(key, []);
    return stacks.get(key);
  }

  return {
    /** Remember `view` before a change; a repeat of the last one is skipped. */
    push(key, view) {
      if (!view) return;
      const stack = stackFor(key);
      const copy = { xMin: view.xMin ?? null, xMax: view.xMax ?? null, yMin: view.yMin ?? null, yMax: view.yMax ?? null };
      if (sameView(stack[stack.length - 1], copy)) return;
      stack.push(copy);
      if (stack.length > limit) stack.shift();
    },
    /** The last view remembered, removed from the history, or null. */
    pop(key) {
      return stackFor(key).pop() ?? null;
    },
    size(key) {
      return stacks.get(key)?.length ?? 0;
    },
  };
}
