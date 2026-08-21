import { describe, expect, it } from "vitest";
import fs from "node:fs";
import path from "node:path";

const root = path.join(process.cwd(), "frontend");
const read = (file) => fs.readFileSync(path.join(root, file), "utf8");

// settings_controller.test.js drives a stand-in shell, not the real dialog, so a
// field can be wired end to end in the tests and still be missing from the app.
// That failure mode is silent in the worst way: the key has no control, so
// saving drops it back to its default -- the 0.10.9 bug.
describe("settings dialog wiring", () => {
  const html = read("index.html");
  const app = read("app.js");
  const controller = read("modules/settings_controller.js");

  const destructured = controller
    .match(/const \{\n(.*?)\n {2}\} = elements;/s)[1]
    .split("\n")
    .map((line) => line.trim().replace(/,$/, ""))
    .filter((name) => name.startsWith("settings"));

  it("destructures a meaningful number of settings elements", () => {
    expect(destructured.length).toBeGreaterThan(20);
  });

  it.each(destructured)("%s resolves to an element that exists in index.html", (name) => {
    const byId = app.match(new RegExp(`const ${name} = document\\.getElementById\\("([^"]+)"\\)`));
    if (byId) {
      expect(html, `id="${byId[1]}" is missing from index.html`).toContain(`id="${byId[1]}"`);
      return;
    }
    // A few are class-based, so accept querySelector too rather than forcing an
    // id purely to satisfy this check.
    const bySelector = app.match(
      new RegExp(`const ${name} = document\\.querySelector\\("\\.([^"]+)"\\)`),
    );
    expect(bySelector, `${name} is never looked up in app.js`).not.toBeNull();
    expect(html, `class "${bySelector[1]}" is missing from index.html`).toContain(bySelector[1]);
  });

  it("gives every config section written on save a control in the dialog", () => {
    // Keys the save payload sets explicitly must have somewhere to come from;
    // otherwise the value is invented rather than edited.
    for (const key of ["allowed_hosts", "compression", "startup_health_timeout_sec"]) {
      expect(controller, `${key} is not written on save`).toContain(key);
    }
    for (const id of ["settings-allowed-hosts", "settings-compression", "settings-startup-health-timeout"]) {
      expect(html, `${id} is missing from the dialog`).toContain(`id="${id}"`);
    }
  });
});
