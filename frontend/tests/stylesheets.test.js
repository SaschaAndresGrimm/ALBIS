import { describe, expect, it } from "vitest";
import fs from "node:fs";
import path from "node:path";

import { readStylesheets, stylesheetHrefs } from "./stylesheets.js";

/**
 * The stylesheet is style.css plus the parts in frontend/styles/, linked from
 * index.html in the order of their numbers; that order is the cascade.
 *
 * A part that is not linked is dead weight nobody notices, and one linked out
 * of order lets an earlier rule win where a later one used to. And a url() in
 * a stylesheet resolves against that file, not the page: moving a rule from
 * style.css into styles/ turned url("resources/splash.webp") into a request
 * for styles/resources/, and the start screen lost its picture with no test
 * failing.
 */

const FRONTEND = path.join(process.cwd(), "frontend");

describe("stylesheets", () => {
  it("links style.css first, then every part in styles/ in the order of its number", () => {
    const parts = fs
      .readdirSync(path.join(FRONTEND, "styles"))
      .filter((name) => name.endsWith(".css"))
      .sort()
      .map((name) => `styles/${name}`);
    expect(stylesheetHrefs()).toEqual(["style.css", ...parts]);
  });

  it("numbers the parts without gaps or repeats, so a new one has an obvious place", () => {
    const numbers = stylesheetHrefs()
      .slice(1)
      .map((href) => Number(path.basename(href).split("-")[0]));
    expect(numbers).toEqual(numbers.map((_, i) => i + 1));
  });

  it("points every url() at a file that exists, relative to the stylesheet", () => {
    const missing = readStylesheets().flatMap(({ href, css }) =>
      [...css.replace(/\/\*[\s\S]*?\*\//g, "").matchAll(/url\(\s*["']?([^"')]+)["']?\s*\)/g)]
        .map((match) => match[1])
        .filter((target) => !/^(data:|#|https?:)/.test(target))
        .filter((target) => !fs.existsSync(path.join(FRONTEND, path.dirname(href), target)))
        .map((target) => `${href}: ${target}`),
    );
    expect(missing).toEqual([]);
  });
});
