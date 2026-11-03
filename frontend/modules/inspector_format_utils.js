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
  // A group's icon says what it is; the column stays empty rather than
  // repeating "group" down the tree.
  if (type === "group") return { text: "", title: "" };
  return { text: typeLabel, title: "" };
}

/** `/entry/data/start_time` → `{ name: "start_time", parent: "/entry/data" }`. */
export function splitInspectorPath(path) {
  const parts = String(path || "").split("/").filter(Boolean);
  const name = parts.pop() || "/";
  return { name, parent: parts.length ? `/${parts.join("/")}` : "/" };
}

/**
 * `text` cut into `{ text, match }` pieces around every case-insensitive
 * occurrence of `query`, for highlighting what a search found. The pieces
 * join back to `text` exactly.
 */
export function splitMatches(text, query) {
  const source = String(text ?? "");
  const needle = String(query ?? "").trim().toLowerCase();
  if (!needle) return [{ text: source, match: false }];
  const lower = source.toLowerCase();
  const pieces = [];
  let from = 0;
  for (let at = lower.indexOf(needle); at !== -1; at = lower.indexOf(needle, from)) {
    if (at > from) pieces.push({ text: source.slice(from, at), match: false });
    pieces.push({ text: source.slice(at, at + needle.length), match: true });
    from = at + needle.length;
  }
  if (from < source.length || !pieces.length) pieces.push({ text: source.slice(from), match: false });
  return pieces;
}
