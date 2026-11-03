/**
 * Keyboard movement in the inspector tree, after the WAI-ARIA tree pattern.
 *
 * Pure: the caller lists the visible items in order, each as
 * `{ level, expandable, expanded }`, and gets back what the key should do.
 * Until this existed only groups could take focus (through their toggle
 * button), so a dataset or link could not be reached without a mouse.
 */

function parentIndex(items, index) {
  const level = items[index].level;
  for (let i = index - 1; i >= 0; i -= 1) {
    if (items[i].level < level) return i;
  }
  return -1;
}

/**
 * `{ action: "focus" | "expand" | "collapse" | "activate", index }`, or null
 * when the key means nothing here (so the caller leaves the event alone).
 */
export function treeKeyAction(items, index, key) {
  if (!Array.isArray(items) || index < 0 || index >= items.length) return null;
  const item = items[index];
  const last = items.length - 1;
  switch (key) {
    case "ArrowDown":
      return index < last ? { action: "focus", index: index + 1 } : null;
    case "ArrowUp":
      return index > 0 ? { action: "focus", index: index - 1 } : null;
    case "Home":
      return index > 0 ? { action: "focus", index: 0 } : null;
    case "End":
      return index < last ? { action: "focus", index: last } : null;
    case "ArrowRight":
      if (!item.expandable) return null;
      if (!item.expanded) return { action: "expand", index };
      return index < last && items[index + 1].level > item.level ? { action: "focus", index: index + 1 } : null;
    case "ArrowLeft": {
      if (item.expandable && item.expanded) return { action: "collapse", index };
      const parent = parentIndex(items, index);
      return parent >= 0 ? { action: "focus", index: parent } : null;
    }
    case "Enter":
    case " ":
      return { action: "activate", index };
    default:
      return null;
  }
}
