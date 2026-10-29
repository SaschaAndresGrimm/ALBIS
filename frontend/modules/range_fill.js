/**
 * The filled part of a slider's track, up to its thumb.
 *
 * Firefox draws it from `::-moz-range-progress`; WebKit and Chromium have no
 * such part, so the track paints it from `--range-fill`, which has to follow
 * the value. A user's drag is caught by one `input` listener on the document;
 * code that sets a slider's value or bounds itself calls `syncRangeFill`,
 * since assigning `value` fires no event.
 */

export function rangeFillPercent(input) {
  const min = Number(input.min) || 0;
  const max = input.max === "" ? 100 : Number(input.max);
  const value = Number(input.value);
  if (!Number.isFinite(value) || !Number.isFinite(max) || max <= min) return 0;
  return Math.max(0, Math.min(100, ((value - min) / (max - min)) * 100));
}

export function syncRangeFill(input) {
  if (!input || input.type !== "range") return;
  input.style.setProperty("--range-fill", `${rangeFillPercent(input)}%`);
}

export function attachRangeFill(root = document) {
  root.querySelectorAll('input[type="range"]').forEach(syncRangeFill);
  root.addEventListener("input", (event) => syncRangeFill(event.target));
}
