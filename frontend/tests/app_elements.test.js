import { describe, expect, it } from "vitest";
import fs from "node:fs";
import path from "node:path";

/**
 * modules/app_elements.js is app.js's table of the elements index.html declares
 * by id. These keep the table and its two ends in step: an id that drifts out
 * of index.html, or an entry app.js no longer takes, fails here rather than
 * leaving a control silently null.
 */

const root = path.join(process.cwd(), "frontend");
const read = (file) => fs.readFileSync(path.join(root, file), "utf8");
const html = read("index.html");
const table = read("modules/app_elements.js");
const entries = [...table.matchAll(/^ {4}(\w+): byId\("([^"]+)"\),/gm)].map(([, name, id]) => ({ name, id }));
const taken = read("app.js")
  .match(/const \{\n([\s\S]*?)\n\} = queryAppElements\(\);/)[1]
  .split(/[\s,]+/)
  .filter(Boolean);

describe("app_elements", () => {
  it("lists the elements app.js looks up by id", () => {
    expect(entries.length).toBeGreaterThan(400);
  });

  it.each(entries)("#$id ($name) exists in index.html", ({ id }) => {
    expect(html).toContain(`id="${id}"`);
  });

  it("is taken whole by app.js, each name once", () => {
    expect(taken).toEqual(entries.map((entry) => entry.name));
  });
});
