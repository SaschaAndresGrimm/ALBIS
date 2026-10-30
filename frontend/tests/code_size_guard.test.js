import { describe, expect, it } from "vitest";
import fs from "node:fs";
import path from "node:path";

/**
 * Two ratchets on the shape of the frontend code, in the manner of
 * style_guard.test.js: what is there may stay, but it may not grow, and
 * nothing new may start out the same way.
 *
 * File length. frontend/AGENTS.md says not to copy app.js's size, and the
 * composition root kept growing anyway, to 5,659 lines by 0.38.1, with
 * style.css at 9,241 until it was split into frontend/styles/. A file that
 * size cannot be read whole -- by a person scrolling or by a coding agent
 * with a context window -- so changes land where the reader happened to
 * look, and the file grows again. A new file
 * stays under MAX_LINES; one already over it may not get longer.
 *
 * Direct fetch. http.js's fetchJSON / fetchJSONWithInit turn a failed request
 * into a translated message; a bare fetch() shows the user whatever the
 * browser says, in English. 24 modules still called fetch() directly, and new
 * code copies its neighbours.
 *
 * When one of these fails because you made a file shorter or removed a fetch,
 * lower its baseline to the new number in the same change, so the gain stays.
 */

const ROOT = process.cwd();
const MAX_LINES = 1000;

// Files over MAX_LINES, at the length they may not exceed.
const LINE_BASELINE = {
  "frontend/app.js": 5659,
  // +16 in the stylesheet split: one <link> per part in frontend/styles/.
  "frontend/index.html": 2599,
  "frontend/modules/detector_control_controller.js": 2903,
  "frontend/modules/file_browser.js": 1680,
  "frontend/modules/pixel_mask_controller.js": 1066,
  "frontend/modules/roi_stats_controller.js": 1263,
};

// Direct fetch() calls per file. http.js is where they belong; i18n.js loads
// the locale files before any translated message exists to show.
const FETCH_ALLOWED = new Set(["frontend/modules/http.js", "frontend/modules/i18n.js"]);
const FETCH_BASELINE = {
  "frontend/app.js": 2,
  "frontend/modules/analysis_controls_bindings.js": 1,
  "frontend/modules/animation_export_controller.js": 1,
  "frontend/modules/autoload_controls.js": 1,
  "frontend/modules/autoload_mode_controller.js": 2,
  "frontend/modules/autoload_status_controller.js": 1,
  "frontend/modules/backend_status_controller.js": 1,
  "frontend/modules/data_export_controller.js": 1,
  "frontend/modules/detector_control_controller.js": 1,
  "frontend/modules/file_browser.js": 1,
  "frontend/modules/file_data_pipeline_controller.js": 3,
  "frontend/modules/file_open_flow.js": 1,
  "frontend/modules/flatfield_controller.js": 3,
  "frontend/modules/geometry_params_controller.js": 1,
  "frontend/modules/jfjoch_bridge_controller.js": 3,
  "frontend/modules/jfjoch_probe_controller.js": 1,
  "frontend/modules/known_detectors.js": 1,
  "frontend/modules/mask_cursor_controller.js": 2,
  "frontend/modules/pixel_mask_controller.js": 5,
  "frontend/modules/quit_controller.js": 1,
  "frontend/modules/remote_stream_controller.js": 3,
  "frontend/modules/settings_controller.js": 3,
  "frontend/modules/simplon_probe_controller.js": 1,
};

function sourceFiles() {
  const modules = fs
    .readdirSync(path.join(ROOT, "frontend", "modules"))
    .filter((name) => name.endsWith(".js"))
    .map((name) => `frontend/modules/${name}`);
  const styles = fs
    .readdirSync(path.join(ROOT, "frontend", "styles"))
    .filter((name) => name.endsWith(".css"))
    .map((name) => `frontend/styles/${name}`);
  return ["frontend/app.js", "frontend/index.html", "frontend/style.css", ...styles, ...modules].sort();
}

function read(file) {
  return fs.readFileSync(path.join(ROOT, file), "utf8");
}

function lineCount(file) {
  const text = read(file);
  return text.split("\n").length - (text.endsWith("\n") ? 1 : 0);
}

function directFetchCount(file) {
  // Comments mention fetch() when explaining why; only calls count.
  const code = read(file)
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .replace(/(^|[^:])\/\/.*$/gm, "$1");
  return (code.match(/(?<![\w.])fetch\(|\b(?:window|globalThis)\.fetch\(/g) || []).length;
}

describe("code size guard: no file grows past what it is", () => {
  it.each(sourceFiles())("%s stays within its line budget", (file) => {
    const lines = lineCount(file);
    const budget = LINE_BASELINE[file] ?? MAX_LINES;
    expect(
      lines,
      LINE_BASELINE[file]
        ? `${file} has ${lines} lines, baseline ${budget}. Move the new code into a module of its own and wire it in.`
        : `${file} has ${lines} lines, over ${MAX_LINES}. Split it by what each part owns (see frontend/AGENTS.md).`,
    ).toBeLessThanOrEqual(budget);
  });

  it("keeps the baselines current, so a file that shrank cannot grow back", () => {
    const stale = Object.entries(LINE_BASELINE)
      .map(([file, budget]) => ({ file, budget, lines: lineCount(file) }))
      .filter(({ budget, lines }) => lines < budget)
      .map(({ file, lines }) => `${file}: ${lines}`);
    expect(stale, "lower LINE_BASELINE to these (or drop the entry if under MAX_LINES)").toEqual([]);
  });
});

describe("code size guard: backend calls go through http.js", () => {
  it.each(sourceFiles().filter((file) => file.endsWith(".js") && !FETCH_ALLOWED.has(file)))(
    "%s adds no direct fetch() calls",
    (file) => {
      const count = directFetchCount(file);
      const baseline = FETCH_BASELINE[file] ?? 0;
      expect(
        count,
        `${file} calls fetch() ${count} time(s), baseline ${baseline}. Use fetchJSON / fetchJSONWithInit from modules/http.js; ` +
          "for a response that is not JSON, add a helper there. If you removed calls, lower FETCH_BASELINE.",
      ).toBe(baseline);
    },
  );
});
