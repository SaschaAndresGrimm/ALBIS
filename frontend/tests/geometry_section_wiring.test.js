import fs from "node:fs";
import path from "node:path";

import { describe, expect, it } from "vitest";

const root = path.join(process.cwd(), "frontend");
const read = (file) => fs.readFileSync(path.join(root, file), "utf8");

// geometry_params_controller.test.js drives stand-in elements, so the section
// can work in the tests and still be unreachable in the app if an id drifts.
describe("Detector Geometry wiring", () => {
  const html = read("index.html");
  const app = read("app.js");
  const ids = [...app.matchAll(/document\.getElementById\("(geometry-[^"]+|detector-geometry-section|summary-geometry)"\)/g)].map(
    (match) => match[1],
  );

  it("looks up the section's controls", () => {
    expect(ids.length).toBeGreaterThanOrEqual(18);
  });

  it.each(ids)("#%s exists in index.html", (id) => {
    expect(html).toContain(`id="${id}"`);
  });

  it("sits in the Data tab", () => {
    const dataTab = html.slice(html.indexOf('data-panel-tab="data">'), html.indexOf('data-panel-tab="analysis">'));
    expect(dataTab).toContain('id="detector-geometry-section"');
  });

  it("shows the values in effect in both Resolution Rings and Peak Finder", () => {
    const rings = html.slice(html.indexOf('data-section="resolution-rings"'), html.indexOf('data-section="peak-finder"'));
    const peaks = html.slice(html.indexOf('data-section="peak-finder"'));
    expect(rings).toContain("data-geometry-inline");
    expect(peaks).toContain("data-geometry-inline");
    // The old per-section fields must not come back beside the new ones.
    expect(html).not.toContain('id="rings-distance"');
    expect(html).not.toContain('id="rings-geometry-lock"');
  });
});
