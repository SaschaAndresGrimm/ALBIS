/**
 * Geometry for the inspector's line chart of a 1D array.
 *
 * Pure: takes the backend's outline (each point the lowest and highest value
 * of `step` consecutive values) and returns SVG path data and the lookups
 * the hover caption needs. With `step` 1 every value is its own point and the
 * chart is a line; with more, low and high differ and it is a band, so one
 * spike among 40,000 values still shows.
 */

function finite(values) {
  return values.filter((v) => typeof v === "number" && Number.isFinite(v));
}

/**
 * `{ line, band, yMin, yMax }` for a chart `width` × `height` in its own
 * units, or null when the outline holds no finite value. Gaps (a stretch of
 * NaN) break the path instead of being drawn through.
 */
export function outlineGeometry(outline, { width, height, pad = 2 }) {
  const low = Array.isArray(outline?.low) ? outline.low : [];
  const high = Array.isArray(outline?.high) ? outline.high : [];
  const lows = finite(low);
  const highs = finite(high);
  if (!lows.length || !highs.length) return null;
  let yMin = Math.min(...lows);
  let yMax = Math.max(...highs);
  const flat = yMin === yMax;
  // A constant array is drawn as a level line in the middle, not on an edge.
  const span = flat ? 1 : yMax - yMin;
  const n = high.length;
  const x = (i) => (n === 1 ? width / 2 : pad + (i * (width - 2 * pad)) / (n - 1));
  const y = (v) => (flat ? height / 2 : pad + ((yMax - v) * (height - 2 * pad)) / span);
  const fmt = (v) => Number(v.toFixed(2));

  const runs = [];
  let run = [];
  for (let i = 0; i < n; i += 1) {
    const ok = Number.isFinite(low[i]) && Number.isFinite(high[i]);
    if (ok) run.push(i);
    if ((!ok || i === n - 1) && run.length) {
      runs.push(run);
      run = [];
    }
  }
  const isBand = Number(outline.step) > 1 && low.some((v, i) => v !== high[i]);
  const line = isBand
    ? ""
    : runs.map((r) => r.map((i, k) => `${k ? "L" : "M"}${fmt(x(i))} ${fmt(y(high[i]))}`).join(" ")).join(" ");
  const band = isBand
    ? runs
        .map((r) => {
          const top = r.map((i, k) => `${k ? "L" : "M"}${fmt(x(i))} ${fmt(y(high[i]))}`);
          const bottom = [...r].reverse().map((i) => `L${fmt(x(i))} ${fmt(y(low[i]))}`);
          return `${top.join(" ")} ${bottom.join(" ")} Z`;
        })
        .join(" ")
    : "";
  if (flat) {
    yMin = lows[0];
    yMax = highs[0];
  }
  return { line, band, yMin, yMax };
}

/**
 * The point under a pointer at `fraction` (0–1) across the chart:
 * `{ index, from, to, low, high }`, where `from`–`to` are the array indices
 * that point covers (equal when `step` is 1).
 */
export function outlinePointAt(outline, fraction, count) {
  const n = Array.isArray(outline?.high) ? outline.high.length : 0;
  if (!n) return null;
  const index = Math.min(n - 1, Math.max(0, Math.round(fraction * (n - 1))));
  const step = Math.max(1, Number(outline.step) || 1);
  const from = index * step;
  const to = Math.min(from + step, Math.max(Number(count) || 0, from + 1)) - 1;
  return { index, from, to, low: outline.low[index], high: outline.high[index] };
}
