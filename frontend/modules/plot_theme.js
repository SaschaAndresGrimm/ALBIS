/**
 * The colours of the plots drawn on canvas -- the histogram, the ROI profiles
 * and the overview -- read from the --plot-* tokens in style.css, so they
 * follow the light and dark theme like the rest of the interface.
 *
 * The returned object keeps the shape the renderers already use
 * (PLOT_THEME.bg, PLOT_THEME.frame, ...), with a getter per colour. Reading
 * computed styles on every stroke would be slow, so the values are cached and
 * read again only when the theme changes.
 */

import { THEME_CHANGE_EVENT } from "./theme.js";

const TOKENS = {
  bg: "--plot-bg",
  frame: "--plot-frame",
  axis: "--plot-axis",
  grid: "--plot-grid",
  text: "--plot-text",
  bar: "--plot-bar",
  line: "--plot-line",
  lineGlow: "--plot-line-glow",
  markerOutline: "--plot-marker-outline",
  peak: "--plot-peak",
};

// The dark values, for when no stylesheet answers (tests, a failed load).
const FALLBACK = {
  bg: "rgba(12, 18, 27, 0.96)",
  frame: "rgba(78, 100, 137, 0.45)",
  axis: "rgba(132, 156, 196, 0.82)",
  grid: "rgba(88, 112, 152, 0.24)",
  text: "#dce8ff",
  bar: "rgba(236, 243, 255, 0.88)",
  line: "rgba(236, 243, 255, 0.95)",
  lineGlow: "rgba(102, 178, 255, 0.2)",
  markerOutline: "rgba(8, 14, 22, 0.85)",
  peak: "rgba(255, 196, 110, 0.95)",
};

export function createPlotTheme({
  readToken = (name) => globalThis.getComputedStyle?.(document.documentElement).getPropertyValue(name),
  target = globalThis,
} = {}) {
  let cache = null;

  function load() {
    cache = {};
    for (const [key, token] of Object.entries(TOKENS)) {
      const value = String(readToken(token) ?? "").trim();
      cache[key] = value || FALLBACK[key];
    }
  }

  target.addEventListener?.(THEME_CHANGE_EVENT, () => {
    cache = null;
  });

  const theme = {};
  for (const key of Object.keys(TOKENS)) {
    Object.defineProperty(theme, key, {
      enumerable: true,
      get() {
        if (!cache) load();
        return cache[key];
      },
    });
  }
  return theme;
}
