/**
 * A group with more children than the backend will list must say so.
 *
 * An ARINA master written one companion file per frame put 5,000,000 external
 * links in `/entry/data`. The tree route now stops at 10,000 of them, which
 * leaves the inspector showing a listing that looks complete and is not: the
 * user scrolls 10,000 rows and has no way to know 4,990,000 are missing, nor
 * that the ones shown are an arbitrary sample rather than the first by name.
 *
 * The notice rides along as a trailing row so that both callers which render
 * children -- the root load and the expand-a-group handler -- show it without
 * either of them knowing it exists.
 */

import fs from "node:fs";
import path from "node:path";

import { beforeEach, describe, expect, it, vi } from "vitest";

// The real English catalogue, so a renamed or deleted key fails here rather
// than degrading to a raw key in the UI.
const EN = JSON.parse(
  fs.readFileSync(path.join(process.cwd(), "frontend", "locales", "en.json"), "utf8")
);

async function controllerWith(response) {
  vi.resetModules();
  global.fetch = vi.fn(async () => ({ ok: true, json: async () => EN }));
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  const { createInspectorPanelController } = await import(
    "../modules/inspector_panel_controller.js"
  );

  const inspectorTree = document.createElement("ul");
  inspectorTree.className = "inspector-children";
  document.body.appendChild(inspectorTree);

  const controller = createInspectorPanelController({
    apiBase: "/api",
    state: { file: "/data/master.h5", dataset: "" },
    elements: { inspectorTree },
    callbacks: {
      fetchJSON: vi.fn().mockResolvedValue(response),
      isHdf5File: () => true,
      isHeaderCapableFile: () => false,
      setSectionBadgeState: () => {},
      renderSkeletonBlock: () => {},
      formatInspectorValue: (v) => String(v),
      resetInspectorDetails: () => {},
    },
  });
  return { controller, inspectorTree };
}

const BIG = (5000000).toLocaleString();

describe("inspector tree truncation", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
  });

  it("appends a note naming the real size when the listing is capped", async () => {
    const { controller } = await controllerWith({
      path: "/entry/data",
      children: [
        { name: "data_000001", path: "/entry/data/data_000001", type: "link" },
        { name: "data_000002", path: "/entry/data/data_000002", type: "link" },
      ],
      childCount: 5000000,
      truncated: true,
    });

    const children = await controller.fetchInspectorTree("/entry/data");

    expect(children).toHaveLength(3);
    const note = children[2];
    expect(note.type).toBe("truncated");
    // The count the user reads must be the group's size, not the page's.
    expect(note.name).toContain(BIG);
    expect(note.name).not.toContain("inspector.tree");
  });

  it("adds nothing to a group that was listed in full", async () => {
    const { controller } = await controllerWith({
      path: "/entry",
      children: [{ name: "data", path: "/entry/data", type: "group" }],
      childCount: 1,
      truncated: false,
    });

    const children = await controller.fetchInspectorTree("/entry");

    expect(children).toHaveLength(1);
    expect(children.every((c) => c.type !== "truncated")).toBe(true);
  });

  it("tolerates a backend that reports no counts at all", async () => {
    // The fields are optional in the response model, so a non-group path
    // returns neither. That must not invent a notice.
    const { controller } = await controllerWith({ path: "/entry", children: [] });

    await expect(controller.fetchInspectorTree("/entry")).resolves.toEqual([]);
  });

  it("renders the note as a plain row with nothing to select or expand", async () => {
    const { controller, inspectorTree } = await controllerWith({
      path: "/entry/data",
      children: [{ name: "data_000001", path: "/entry/data/data_000001", type: "link" }],
      childCount: 5000000,
      truncated: true,
    });

    const children = await controller.fetchInspectorTree("/entry/data");
    controller.renderInspectorTree(children, inspectorTree);

    const rows = inspectorTree.querySelectorAll("li.inspector-node");
    expect(rows).toHaveLength(2);
    const noteRow = rows[1];
    expect(noteRow.dataset.type).toBe("truncated");
    // No path: the click handler keys off this to leave the row alone, and
    // `showInspectorNode` would 404 on a path that names no node.
    expect(noteRow.dataset.path).toBe("");
    expect(noteRow.querySelector(".inspector-row-note")).not.toBeNull();
    expect(noteRow.querySelector(".inspector-toggle")).toBeNull();
    expect(noteRow.textContent).toContain(BIG);
  });
});
