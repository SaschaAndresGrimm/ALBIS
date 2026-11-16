/**
 * Fitting a long name into a narrow box without losing the part that tells
 * it apart. Paths and file names share their start (the volume, the folder,
 * the run) and differ at the end (the frame number, the extension), so the
 * text is cut in the middle, not at its end. No DOM here: the caller says
 * whether a length fits.
 */

export const ELLIPSIS = "…";

// `text` cut to `keep` characters plus "…" in its middle, a little more of
// the end kept than of the start; `text` itself when it is short enough.
export function middleEllipsis(text, keep) {
  const value = String(text ?? "");
  const count = Math.max(0, Math.floor(Number(keep) || 0));
  if (value.length <= count) return value;
  const tail = Math.ceil(count / 2);
  const head = count - tail;
  return `${value.slice(0, head)}${ELLIPSIS}${value.slice(value.length - tail)}`;
}

// The largest n in [0, max] for which fits(n) holds, given that it holds for
// every n below one that does; 0 when none does. A binary search: a handful of
// layouts for a name of any length.
export function longestFitting(max, fits) {
  let low = 0;
  let high = Math.max(0, Math.floor(max));
  if (fits(high)) return high;
  while (high - low > 1) {
    const mid = Math.floor((low + high) / 2);
    if (fits(mid)) low = mid;
    else high = mid;
  }
  return low;
}
