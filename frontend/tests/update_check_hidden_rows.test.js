import fs from "node:fs";
import path from "node:path";

import { describe, expect, it } from "vitest";

/**
 * The update dialog hides rows with the `hidden` attribute, which is a
 * user-agent `display: none` -- and an author-level `display: grid` or `flex`
 * on the same element silently outranks it. The element then stays on screen
 * while the controller believes it is gone, and no DOM test notices, because
 * `element.hidden` is perfectly true. jsdom has no layout, so the only place
 * this can be checked is the stylesheet itself.
 */
// Comments are stripped before parsing: a comma inside one would otherwise be
// read as a selector separator.
const CSS = fs
  .readFileSync(path.resolve(__dirname, "..", "style.css"), "utf8")
  .replace(/\/\*[\s\S]*?\*\//g, "");

// Containers that hold the dialog together and are never hidden on their own.
// A new `.update-check-*` class that carries a display must either guard
// itself against `[hidden]` or be listed here, deliberately.
const NEVER_HIDDEN = new Set([
  ".update-check-modal-body",
  ".update-check-version-grid",
  // Centres the label inside the progress bar. It is shown and hidden with
  // its parent, never on its own.
  ".update-check-progress-text",
]);

function rulesInStylesheet() {
  const rules = [];
  const pattern = /([^{}]+)\{([^{}]*)\}/g;
  let match = pattern.exec(CSS);
  while (match !== null) {
    rules.push({ selectors: match[1], body: match[2] });
    match = pattern.exec(CSS);
  }
  return rules;
}

function declaredDisplay(body) {
  const match = /(?:^|;)\s*display\s*:\s*([^;]+)/.exec(body);
  return match ? match[1].trim() : "";
}

describe("update dialog rows that the controller hides", () => {
  it("opt out of their own display when the hidden attribute is set", () => {
    const rules = rulesInStylesheet();

    // Every `.update-check-*` class given a display other than `none`.
    const needsGuard = new Set();
    for (const { selectors, body } of rules) {
      const display = declaredDisplay(body);
      if (!display || display === "none") continue;
      for (const selector of selectors.split(",")) {
        const trimmed = selector.trim();
        if (/^\.update-check-[a-z-]+$/.test(trimmed) && !NEVER_HIDDEN.has(trimmed)) {
          needsGuard.add(trimmed);
        }
      }
    }
    expect(needsGuard.size).toBeGreaterThan(0);

    // Every `.update-check-*[hidden]` class that is set back to `none`.
    const guarded = new Set();
    for (const { selectors, body } of rules) {
      if (declaredDisplay(body) !== "none") continue;
      for (const selector of selectors.split(",")) {
        const match = /^(\.update-check-[a-z-]+)\[hidden\]$/.exec(selector.trim());
        if (match) guarded.add(match[1]);
      }
    }

    const unguarded = [...needsGuard].filter((selector) => !guarded.has(selector)).sort();
    expect(unguarded).toEqual([]);
  });
});
