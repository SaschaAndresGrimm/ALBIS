import { describe, expect, it } from "vitest";
import fs from "node:fs";
import path from "node:path";

/**
 * Two checks on the shape of the frontend code. File length is a ratchet, in
 * the manner of style_guard.test.js: what is there may stay, but it may not
 * grow, and nothing new may start out the same way. Direct fetch is a rule.
 *
 * File length. frontend/AGENTS.md says not to copy app.js's size, and the
 * composition root kept growing anyway, to 5,659 lines by 0.38.1, with
 * style.css at 9,241 until it was split into frontend/styles/. A file that
 * size cannot be read whole -- by a person scrolling or by a coding agent
 * with a context window -- so changes land where the reader happened to
 * look, and the file grows again. A new file stays under MAX_LINES; one
 * already over it may not get longer.
 *
 * Direct fetch. http.js's helpers turn a failed request into a translated
 * message; a bare fetch() shows the user whatever the browser says, in
 * English. 24 modules called fetch() directly until they were moved onto
 * http.js, and new code copies its neighbours, so no module but http.js and
 * i18n.js may.
 *
 * When the line check fails because you made a file shorter, lower its
 * baseline to the new number in the same change, so the gain stays. A line
 * baseline may go up only when nothing smaller will do, with the reason in
 * the pull request.
 */

const ROOT = process.cwd();
const MAX_LINES = 1000;

// Files over MAX_LINES, at the length they may not exceed.
const LINE_BASELINE = {
  "frontend/app.js": 3551,
  // +17 in the stylesheet split: one <link> per part in frontend/styles/.
  "frontend/index.html": 2600,
};

// The only modules that call fetch() themselves: http.js, which every other
// call goes through, and i18n.js, which loads the locale files before any
// translated message exists to show.
const FETCH_ALLOWED = new Set(["frontend/modules/http.js", "frontend/modules/i18n.js"]);

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
    "%s calls fetch() only through http.js",
    (file) => {
      expect(
        directFetchCount(file),
        `${file} calls fetch() directly. Use fetchJSON / fetchJSONWithInit, or fetchResponse for bytes, headers ` +
          "or a 204, from modules/http.js.",
      ).toBe(0);
    },
  );
});
