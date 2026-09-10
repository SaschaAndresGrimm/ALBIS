import { describe, expect, it } from "vitest";
import fs from "node:fs";
import path from "node:path";

/**
 * A role that is not a role is worse than no role at all: it replaces whatever
 * the element would have been announced as with nothing. The settings dialog
 * carried `role="tabpage"` on all four panels -- a plausible-looking name that
 * does not exist -- so a screen reader saw a tablist whose tabs pointed at
 * nothing, in the one dialog where the rest of the accessibility work was
 * careful.
 */

const HTML_FILES = ["index.html", "docs.html"].map((name) =>
  path.join(process.cwd(), "frontend", name),
);

// WAI-ARIA 1.2 roles. Abstract roles are deliberately absent: they are not
// allowed in markup.
const VALID_ROLES = new Set([
  "alert", "alertdialog", "application", "article", "banner", "blockquote", "button",
  "caption", "cell", "checkbox", "code", "columnheader", "combobox", "command",
  "complementary", "contentinfo", "definition", "deletion", "dialog", "directory",
  "document", "emphasis", "feed", "figure", "form", "generic", "grid", "gridcell",
  "group", "heading", "img", "insertion", "link", "list", "listbox", "listitem",
  "log", "main", "marquee", "math", "menu", "menubar", "menuitem", "menuitemcheckbox",
  "menuitemradio", "meter", "navigation", "none", "note", "option", "paragraph",
  "presentation", "progressbar", "radio", "radiogroup", "region", "row", "rowgroup",
  "rowheader", "scrollbar", "search", "searchbox", "separator", "slider", "spinbutton",
  "status", "strong", "subscript", "superscript", "switch", "tab", "table", "tablist",
  "tabpanel", "term", "textbox", "time", "timer", "toolbar", "tooltip", "tree",
  "treegrid", "treeitem",
]);

function readHtml(file) {
  return fs.readFileSync(file, "utf8");
}

describe("aria roles", () => {
  it.each(HTML_FILES)("only uses roles that exist in %s", (file) => {
    const html = readHtml(file);
    const used = [...html.matchAll(/\srole="([^"]+)"/g)].map((match) => match[1].trim());

    const invalid = [...new Set(used)].filter((role) => !VALID_ROLES.has(role)).sort();

    expect(invalid).toEqual([]);
  });

  it("pairs every tab with the panel it controls", () => {
    const html = readHtml(HTML_FILES[0]);
    const tabs = [...html.matchAll(/<button[^>]*role="tab"[^>]*>/g)].map((match) => match[0]);
    const panels = [...html.matchAll(/<div[^>]*role="tabpanel"[^>]*>/g)].map((match) => match[0]);

    expect(tabs.length).toBeGreaterThan(0);
    expect(panels.length).toBe(tabs.length);

    const attribute = (tag, name) => tag.match(new RegExp(`${name}="([^"]+)"`))?.[1] ?? "";

    const panelIds = new Set(panels.map((panel) => attribute(panel, "id")));
    const tabIds = new Set(tabs.map((tab) => attribute(tab, "id")));

    tabs.forEach((tab) => {
      const controls = attribute(tab, "aria-controls");
      expect(controls, `tab ${attribute(tab, "id") || tab} has no aria-controls`).not.toBe("");
      expect(panelIds.has(controls), `aria-controls="${controls}" names no tabpanel`).toBe(true);
    });

    panels.forEach((panel) => {
      const labelledBy = attribute(panel, "aria-labelledby");
      expect(labelledBy, `panel ${attribute(panel, "id") || panel} has no aria-labelledby`).not.toBe(
        "",
      );
      expect(tabIds.has(labelledBy), `aria-labelledby="${labelledBy}" names no tab`).toBe(true);
    });
  });

  /**
   * The transport buttons beside them have carried names all along; these two
   * were announced as a bare "slider" and "spin button", which is the primary
   * way through a stack. A visible label is not an option -- they sit in a
   * toolbar with no room for one -- so the name has to be on the control.
   */
  it("names the frame controls, in every language", () => {
    const html = readHtml(HTML_FILES[0]);
    const en = JSON.parse(
      fs.readFileSync(path.join(process.cwd(), "frontend", "locales", "en.json"), "utf8"),
    );

    ["frame-range", "frame-index"].forEach((id) => {
      const tag = html.match(new RegExp(`<input[^>]*id="${id}"[^>]*>`, "s"))?.[0];
      expect(tag, `no <input id="${id}">`).toBeTruthy();

      const key = tag.match(/data-i18n-aria-label="([^"]+)"/)?.[1] ?? "";
      expect(key, `${id} has no data-i18n-aria-label`).not.toBe("");
      expect(en[key], `${key} is missing from en.json`).toBeTruthy();

      // The static attribute is what a screen reader sees before the first
      // localization pass, so it has to be there too.
      expect(tag.match(/\saria-label="([^"]+)"/)?.[1] ?? "", `${id} has no aria-label`).not.toBe("");
    });
  });
});

/**
 * A control with no accessible name is announced as its role and nothing else
 * -- "checkbox, checked" -- which is no help at all when it is the on/off
 * switch for one of the three headline analysis features. Each switch is a
 * <label> containing only the input and an aria-hidden track, so the name
 * computed from its contents was the empty string.
 */
describe("the analysis feature switches carry an accessible name", () => {
  const html = fs.readFileSync(path.join(process.cwd(), "frontend", "index.html"), "utf8");

  it.each([
    ["roi-enable", "analysis.roi.section_title"],
    ["rings-toggle", "analysis.rings.section_title"],
    ["peaks-enable", "analysis.peaks.section_title"],
  ])("%s is named from %s", (id, key) => {
    const tag = html.match(new RegExp(`<input[^>]*id="${id}"[^>]*>`, "s"));

    expect(tag, `no <input id="${id}"> in index.html`).not.toBeNull();
    expect(tag[0]).toContain('aria-label="');
    // Translated, not hardcoded: the name must follow the interface language.
    expect(tag[0]).toContain(`data-i18n-aria-label="${key}"`);
  });

  it("names them from keys that exist in every locale", () => {
    const dir = path.join(process.cwd(), "frontend", "locales");
    const keys = [
      "analysis.roi.section_title",
      "analysis.rings.section_title",
      "analysis.peaks.section_title",
    ];
    const missing = [];
    for (const file of fs.readdirSync(dir).filter((f) => f.endsWith(".json"))) {
      const dict = JSON.parse(fs.readFileSync(path.join(dir, file), "utf8"));
      for (const key of keys) {
        if (!dict[key]) missing.push(`${file}:${key}`);
      }
    }

    expect(missing).toEqual([]);
  });
});
