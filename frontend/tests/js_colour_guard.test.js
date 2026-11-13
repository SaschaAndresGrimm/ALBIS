/**
 * Colours written into the JavaScript, per file, may not grow.
 *
 * ALBIS has a dark and a light theme. The stylesheets reach both through
 * tokens (style_guard.test.js holds them at zero literals), but a colour a
 * module writes itself -- a fillStyle on a canvas, a style set from code --
 * is the same in both, and on a light panel a plot drawn in the dark theme's
 * pale text is unreadable. That is how the histogram's BG and FG captions
 * came out as dark boxes in the first light build.
 *
 * Interface colours come from modules/plot_theme.js, which reads the
 * --plot-* tokens (frontend/AGENTS.md). What is left here is mostly data:
 * ring, peak and mask overlay colours, and things drawn over the image,
 * which stays dark in both themes. Lower a baseline when you remove literals;
 * a new file with colours, or more in an old one, fails until a reviewer has
 * seen why the colour cannot be a token.
 */

import { describe, expect, it } from "vitest";
import fs from "node:fs";
import path from "node:path";

const FRONTEND = path.join(process.cwd(), "frontend");

// plot_theme.js is where the tokens are read; its literals are the dark
// fallbacks for when no stylesheet answers.
const EXEMPT = new Set(["modules/plot_theme.js"]);

const BASELINE = {
  "modules/overlay_painters.js": 46,
  "modules/export_splash_controller.js": 16,
  "modules/roi_interaction_controller.js": 9,
  "modules/overview_viewport_controller.js": 6,
  "modules/minmax_finder_painter.js": 4,
  "modules/magnifier_controller.js": 4,
  "modules/pixel_mask_edit_controller.js": 3,
  "modules/histogram_render_controller.js": 2,
  "modules/roi_plot_renderer.js": 1,
  "modules/remote_stream_controller.js": 1,
};

function sources() {
  const modules = fs
    .readdirSync(path.join(FRONTEND, "modules"))
    .filter((name) => name.endsWith(".js"))
    .map((name) => `modules/${name}`);
  return ["app.js", ...modules].filter((file) => !EXEMPT.has(file)).sort();
}

/** Colour literals in a file, comments left out: a comment may name one. */
export function countColourLiterals(text) {
  const code = text.replace(/\/\*[\s\S]*?\*\//g, "").replace(/(^|[^:"'`])\/\/.*$/gm, "$1");
  return (code.match(/#[0-9a-fA-F]{6}\b|#[0-9a-fA-F]{3}\b|rgba?\(/g) || []).length;
}

const counts = Object.fromEntries(
  sources().map((file) => [file, countColourLiterals(fs.readFileSync(path.join(FRONTEND, file), "utf8"))]),
);

describe("js colour guard", () => {
  it("adds no colour literals to the JavaScript; use plot_theme.js or a token", () => {
    const grown = Object.entries(counts)
      .filter(([file, count]) => count > (BASELINE[file] ?? 0))
      .map(([file, count]) => `${file}: ${count} (baseline ${BASELINE[file] ?? 0})`);
    expect(grown, "colours that should follow the theme come from PLOT_THEME (modules/plot_theme.js) or a CSS token").toEqual([]);
  });

  it("keeps the baselines current, so a file that lost literals cannot grow back", () => {
    const stale = Object.entries(BASELINE)
      .filter(([file, budget]) => (counts[file] ?? 0) < budget)
      .map(([file]) => `${file}: ${counts[file] ?? 0}`);
    expect(stale, "lower BASELINE to these (or drop the entry at 0)").toEqual([]);
  });

  it("counts literals in code, not in comments", () => {
    expect(countColourLiterals('ctx.fillStyle = "#ffd166"; // was #000\n/* rgba(1,2,3) */')).toBe(1);
    expect(countColourLiterals('const url = "http://x"; ctx.strokeStyle = "rgba(0, 0, 0, 0.5)";')).toBe(1);
  });
});
