import fs from "node:fs";
import path from "node:path";

/**
 * The interface's stylesheets, as index.html loads them: style.css (tokens and
 * base rules) and then the parts in frontend/styles/. Tests that read "the
 * stylesheet" read all of them, in this order, which is the cascade order.
 */

const FRONTEND = path.join(process.cwd(), "frontend");

/** The stylesheet paths index.html links, relative to frontend/, in order. */
export function stylesheetHrefs() {
  const html = fs.readFileSync(path.join(FRONTEND, "index.html"), "utf8");
  return [...html.matchAll(/<link rel="stylesheet" href="([^"]+)"/g)].map((match) => match[1]);
}

/** Each linked stylesheet's text, keyed by its path relative to frontend/. */
export function readStylesheets() {
  return stylesheetHrefs().map((href) => ({ href, css: fs.readFileSync(path.join(FRONTEND, href), "utf8") }));
}

/** All of them as one text, in link order. */
export function readAllCss() {
  return readStylesheets()
    .map((sheet) => sheet.css)
    .join("\n");
}
