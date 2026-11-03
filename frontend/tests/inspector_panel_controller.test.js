/**
 * How the inspector's tree rows and details read.
 *
 * Two faults this guards: a leaf's toggle was hidden with `.is-hidden`
 * (`display: none`), so leaf names sat a column left of their sibling groups';
 * and a scalar string read "object" in the tree and nothing at all as its
 * shape in the details.
 */

import fs from "node:fs";
import path from "node:path";

import { beforeEach, describe, expect, it, vi } from "vitest";

const EN = JSON.parse(
  fs.readFileSync(path.join(process.cwd(), "frontend", "locales", "en.json"), "utf8")
);

async function setup(responses) {
  vi.resetModules();
  global.fetch = vi.fn(async () => ({ ok: true, json: async () => EN }));
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  const { createInspectorPanelController } = await import(
    "../modules/inspector_panel_controller.js"
  );
  document.body.innerHTML = `
    <div id="tree"></div>
    <div id="details">
      <strong id="path"></strong><strong id="type"></strong>
      <strong id="shape"></strong><strong id="dtype"></strong>
      <div id="attrs"></div><div id="preview"></div>
    </div>`;
  const byId = (id) => document.getElementById(id);
  const fetchJSON = vi.fn(async (url) => {
    const route = Object.keys(responses).find((key) => url.includes(key));
    return responses[route];
  });
  const controller = createInspectorPanelController({
    apiBase: "/api",
    state: { file: "/data/master.h5", dataset: "" },
    elements: {
      inspectorTree: byId("tree"),
      inspectorDetails: byId("details"),
      inspectorPath: byId("path"),
      inspectorType: byId("type"),
      inspectorShape: byId("shape"),
      inspectorDtype: byId("dtype"),
      inspectorAttrs: byId("attrs"),
      inspectorPreview: byId("preview"),
    },
    callbacks: {
      fetchJSON,
      isHdf5File: () => true,
      isHeaderCapableFile: () => false,
      setSectionBadgeState: () => {},
      renderSkeletonBlock: () => {},
      formatInspectorValue: (v) => String(v),
      resetInspectorDetails: () => {},
    },
  });
  return { controller, byId };
}

const ROOT = {
  path: "/entry",
  children: [
    { name: "data", path: "/entry/data", type: "group", hasChildren: true },
    { name: "definition", path: "/entry/definition", type: "dataset", shape: [], dtype: "string", value: "NXmx" },
    { name: "energy", path: "/entry/energy", type: "dataset", shape: [1], dtype: "float64", value: 12.4, units: "keV" },
  ],
};

describe("inspector tree rows", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
  });

  it("keeps a leaf's toggle column so names line up", async () => {
    const { controller, byId } = await setup({ "/hdf5/tree": ROOT });
    await controller.loadInspectorRoot();

    const toggles = [...byId("tree").querySelectorAll(".inspector-toggle")];
    expect(toggles).toHaveLength(3);
    const leafToggles = toggles.slice(1);
    leafToggles.forEach((toggle) => {
      expect(toggle.classList.contains("is-hidden")).toBe(false);
      expect(toggle.classList.contains("is-placeholder")).toBe(true);
    });
  });

  it("shows a scalar's value, not its dtype", async () => {
    const { controller, byId } = await setup({ "/hdf5/tree": ROOT });
    await controller.loadInspectorRoot();

    const metas = [...byId("tree").querySelectorAll(".inspector-meta")].map((m) => m.textContent);
    expect(metas).toEqual(["group", "NXmx", "12.4 keV"]);
    expect(byId("tree").textContent).not.toContain("object");
  });

  it("names a scalar's shape in the details", async () => {
    const { controller, byId } = await setup({
      "/hdf5/node": { path: "/entry/definition", type: "dataset", shape: [], dtype: "string", attrs: [] },
      "/hdf5/value": { path: "/entry/definition", type: "dataset", shape: [], dtype: "string", preview: "NXmx", truncated: false },
    });
    await controller.showInspectorNode("/entry/definition");

    expect(byId("shape").textContent).toBe("scalar");
    expect(byId("dtype").textContent).toBe("string");
  });
});
