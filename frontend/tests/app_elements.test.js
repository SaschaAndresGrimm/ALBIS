import { describe, expect, it } from "vitest";
import fs from "node:fs";
import path from "node:path";

/**
 * modules/app_elements.js is app.js's table of the elements index.html declares
 * by id. app.js passes the whole table to its controllers (`...appElements`)
 * and names only the elements it works with itself. These keep the table and
 * its ends in step: an id that drifts out of index.html, or an entry nothing
 * reads any more, fails here rather than leaving a control silently null or a
 * dead lookup behind -- three of those were found when the table was made.
 */

const root = path.join(process.cwd(), "frontend");
const read = (file) => fs.readFileSync(path.join(root, file), "utf8");
const html = read("index.html");
const table = read("modules/app_elements.js");
const entries = [...table.matchAll(/^ {4}(\w+): byId\("([^"]+)"\),/gm)].map(([, name, id]) => ({ name, id }));
const app = read("app.js");
const taken = app
  .match(/const \{\n([\s\S]*?)\n\} = appElements;/)[1]
  .split(/[\s,]+/)
  .filter(Boolean);
const modules = fs
  .readdirSync(path.join(root, "modules"))
  .filter((name) => name.endsWith(".js") && name !== "app_elements.js")
  .map((name) => read(`modules/${name}`))
  .join("\n");

describe("app_elements", () => {
  it("lists the elements app.js looks up by id", () => {
    expect(entries.length).toBeGreaterThan(400);
  });

  it.each(entries)("#$id ($name) exists in index.html", ({ id }) => {
    expect(html).toContain(`id="${id}"`);
  });

  it("is what app.js takes its own elements from, each name once", () => {
    const names = new Set(entries.map((entry) => entry.name));
    expect(taken.filter((name) => !names.has(name))).toEqual([]);
    expect(new Set(taken).size).toBe(taken.length);
  });

  it("holds no element that nothing reads", () => {
    const unread = entries
      .map((entry) => entry.name)
      .filter((name) => !taken.includes(name) && !new RegExp(`\\b${name}\\b`).test(modules));
    expect(unread).toEqual([]);
  });
});
