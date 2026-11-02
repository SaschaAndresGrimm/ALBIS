/**
 * What the file inspector writes beside a node: its value, shape and dtype.
 *
 * Pure: the caller passes the translated labels it needs.
 */

// Long enough for a timestamp with its zone or a short sample name; the whole
// value is in the row's tooltip and in the details below the tree.
export const INSPECTOR_VALUE_CHARS = 40;

export function formatInspectorShape(shape, scalarLabel = "") {
  if (!Array.isArray(shape)) return "";
  if (shape.length === 0) return scalarLabel;
  return shape.join("×");
}

export function formatInspectorScalar(value, units = "") {
  if (value === null || value === undefined) return "";
  const text = String(value);
  return units ? `${text} ${units}` : text;
}

function clip(text, limit) {
  return text.length > limit ? `${text.slice(0, limit - 1)}…` : text;
}

/**
 * The right-hand text of a tree row or search result, and its tooltip.
 *
 * A one-element dataset shows its value (`NXmx`, `12.4 keV`): that is what a
 * reader opens a NeXus master to look up. Other datasets show shape and
 * dtype; a scalar the backend did not summarise shows its dtype alone, never
 * an empty shape.
 */
export function inspectorRowMeta(node, { typeLabel = "", scalarLabel = "" } = {}) {
  const type = String(node?.type || "").toLowerCase();
  if (type === "dataset") {
    const shapeText = formatInspectorShape(node.shape, scalarLabel);
    const dtype = node.dtype || "";
    const layout = [shapeText, dtype].filter(Boolean).join(" ");
    const value = formatInspectorScalar(node.value, node.units);
    if (value) {
      return { text: clip(value, INSPECTOR_VALUE_CHARS), title: layout ? `${value}\n${layout}` : value };
    }
    const bare = Array.isArray(node.shape) && node.shape.length === 0 ? dtype : layout;
    return { text: bare || typeLabel, title: layout };
  }
  if (type === "link" && node.target) {
    return { text: node.target, title: node.target };
  }
  return { text: typeLabel, title: "" };
}
