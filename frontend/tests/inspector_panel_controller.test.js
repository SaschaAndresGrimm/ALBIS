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
      <nav id="path"></nav><button id="copy"></button>
      <div id="facts"></div>
      <div id="attrs"></div><div id="preview"></div>
    </div>`;
  const byId = (id) => document.getElementById(id);
  const setSectionBadgeState = vi.fn();
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
      inspectorFacts: byId("facts"),
      inspectorCopyPath: byId("copy"),
      inspectorAttrs: byId("attrs"),
      inspectorPreview: byId("preview"),
    },
    callbacks: {
      fetchJSON,
      isHdf5File: () => true,
      isHeaderCapableFile: () => false,
      setSectionBadgeState,
      renderSkeletonBlock: () => {},
      formatInspectorValue: (v) => String(v),
    },
  });
  // The value of a details row by its label, or undefined when it is absent.
  const fact = (label) =>
    [...byId("facts").querySelectorAll(".meta-row")]
      .find((row) => row.firstChild.textContent === label)
      ?.lastChild.textContent;
  return { controller, byId, fetchJSON, fact, setSectionBadgeState };
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
    expect(metas).toEqual(["", "NXmx", "12.4 keV"]);
    expect(byId("tree").textContent).not.toContain("object");
  });

  it("names a scalar's shape in the details", async () => {
    const { controller, fact } = await setup({
      "/hdf5/node": { path: "/entry/definition", type: "dataset", shape: [], dtype: "string", attrs: [] },
      "/hdf5/value": { path: "/entry/definition", type: "dataset", shape: [], dtype: "string", preview: "NXmx", truncated: false },
    });
    await controller.showInspectorNode("/entry/definition");

    expect(fact("Shape")).toBe("scalar");
    expect(fact("DType")).toBe("string");
  });
});

const CHILDREN = {
  path: "/entry/data",
  children: [{ name: "data", path: "/entry/data/data", type: "dataset", shape: [3, 4, 4], dtype: "uint32" }],
};

function routesWithChildren() {
  return {
    "path=%2Fentry%2Fdata": CHILDREN,
    "/hdf5/tree": ROOT,
    "/hdf5/node": { path: "/entry/data", type: "group", attrs: [] },
  };
}

const key = (target, k) => {
  const event = new KeyboardEvent("keydown", { key: k, bubbles: true, cancelable: true });
  target.dispatchEvent(event);
  return event;
};
const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

describe("inspector tree from the keyboard", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
  });

  async function loaded() {
    const ctx = await setup(routesWithChildren());
    await ctx.controller.loadInspectorRoot();
    ctx.byId("tree").addEventListener("keydown", (e) => ctx.controller.handleInspectorTreeKey(e));
    ctx.items = () => [...ctx.byId("tree").querySelectorAll('[role="treeitem"]')];
    return ctx;
  }

  it("is a tree with one tab stop", async () => {
    const { byId, items } = await loaded();
    expect(byId("tree").querySelector('[role="tree"]')).not.toBeNull();
    expect(items().map((el) => el.tabIndex)).toEqual([0, -1, -1]);
    expect(items().map((el) => el.getAttribute("aria-level"))).toEqual(["1", "1", "1"]);
    expect(items()[0].getAttribute("aria-expanded")).toBe("false");
    expect(items()[1].hasAttribute("aria-expanded")).toBe(false);
  });

  it("reaches a dataset with the arrows and moves the tab stop with focus", async () => {
    const { items } = await loaded();
    items()[0].focus();
    const event = key(items()[0], "ArrowDown");
    expect(event.defaultPrevented).toBe(true);
    expect(document.activeElement).toBe(items()[1]);
    expect(items().map((el) => el.tabIndex)).toEqual([-1, 0, -1]);
  });

  it("opens a group on the right and gives its children the next level", async () => {
    const { items } = await loaded();
    key(items()[0], "ArrowRight");
    await flush();
    expect(items()[0].getAttribute("aria-expanded")).toBe("true");
    const child = items()[1];
    expect(child.dataset.path).toBe("/entry/data/data");
    expect(child.getAttribute("aria-level")).toBe("2");
    key(items()[0], "ArrowRight");
    expect(document.activeElement).toBe(child);
    key(child, "ArrowLeft");
    expect(document.activeElement).toBe(items()[0]);
  });

  it("shows a dataset's details on Enter", async () => {
    const ctx = await loaded();
    key(ctx.items()[1], "Enter");
    await flush();
    expect(ctx.items()[1].getAttribute("aria-selected")).toBe("true");
    const urls = ctx.fetchJSON.mock.calls.map(([url]) => url);
    expect(urls.some((url) => url.includes("/hdf5/node") && url.includes("%2Fentry%2Fdefinition"))).toBe(true);
  });
});

describe("inspector tree clicks", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
  });

  it("opens a group on the first click and closes it only once it is selected", async () => {
    const { controller, byId } = await setup(routesWithChildren());
    await controller.loadInspectorRoot();
    const group = byId("tree").querySelector('[role="treeitem"]');

    await controller.activateInspectorNode(group);
    await flush();
    expect(group.getAttribute("aria-expanded")).toBe("true");
    expect(group.getAttribute("aria-selected")).toBe("true");

    await controller.activateInspectorNode(group);
    expect(group.getAttribute("aria-expanded")).toBe("false");
  });

  it("keeps an open group open when selecting it from elsewhere", async () => {
    const { controller, byId } = await setup(routesWithChildren());
    await controller.loadInspectorRoot();
    const [group, leaf] = byId("tree").querySelectorAll('[role="treeitem"]');

    await controller.activateInspectorNode(group, { toggleOnly: true });
    await flush();
    expect(group.getAttribute("aria-selected")).toBe("false");
    await controller.activateInspectorNode(leaf);
    await controller.activateInspectorNode(group);
    expect(group.getAttribute("aria-expanded")).toBe("true");
    expect(group.getAttribute("aria-selected")).toBe("true");
  });
});

describe("inspector links", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
  });

  const LINKS = {
    path: "/entry",
    children: [
      { name: "beam", path: "/entry/beam", type: "link", link: "soft", target: "/entry/instrument/beam" },
      { name: "start_time", path: "/entry/start_time", type: "link", link: "external", target: "data_000001.h5:/entry/start_time" },
      { name: "gone", path: "/entry/gone", type: "link", link: "external", target: "missing.h5:/x" },
    ],
  };

  async function withLinks(extra) {
    const routes = {
      "path=%2Fentry%2Fbeam&": undefined,
      ...extra,
      "/hdf5/tree": LINKS,
    };
    const ctx = await setup(routes);
    ctx.fetchJSON.mockImplementation(async (url) => {
      if (url.includes("%2Fentry%2Fgone")) throw new Error("Path not found");
      const route = Object.keys(routes).find((k) => routes[k] && url.includes(k));
      return routes[route];
    });
    await ctx.controller.loadInspectorRoot();
    ctx.item = (name) => [...ctx.byId("tree").querySelectorAll('[role="treeitem"]')].find((el) => el.dataset.path === `/entry/${name}`);
    return ctx;
  }

  it("shows what a link points to, with its target on top", async () => {
    const { controller, byId, fact, item } = await withLinks({
      "node?file=%2Fdata%2Fmaster.h5&path=%2Fentry%2Fstart_time": { path: "/entry/start_time", type: "dataset", shape: [], dtype: "string", attrs: [] },
      "/hdf5/value": { path: "/entry/start_time", type: "dataset", shape: [], dtype: "string", preview: "2026-11-02T20:05:00", truncated: false },
    });
    await controller.activateInspectorNode(item("start_time"));

    expect(fact("Type")).toBe("link to dataset");
    expect(fact("DType")).toBe("string");
    expect(fact("Target")).toBe("data_000001.h5:/entry/start_time");
    expect(byId("preview").textContent).toContain("2026-11-02T20:05:00");
  });

  it("makes a link to a group expandable and opens it", async () => {
    const { controller, item } = await withLinks({
      "tree?file=%2Fdata%2Fmaster.h5&path=%2Fentry%2Fbeam": {
        path: "/entry/beam",
        children: [{ name: "incident_wavelength", path: "/entry/beam/incident_wavelength", type: "dataset", shape: [1], dtype: "float64", value: 1.03, units: "angstrom" }],
      },
      "node?file=%2Fdata%2Fmaster.h5&path=%2Fentry%2Fbeam": { path: "/entry/beam", type: "group", attrs: [] },
    });
    const beam = item("beam");
    expect(beam.hasAttribute("aria-expanded")).toBe(false);

    await controller.activateInspectorNode(beam);
    await flush();

    expect(beam.getAttribute("aria-expanded")).toBe("true");
    const child = beam.querySelector('[role="treeitem"]');
    expect(child.dataset.path).toBe("/entry/beam/incident_wavelength");
    expect(child.getAttribute("aria-level")).toBe("2");
  });

  it("keeps the tree when a link does not resolve", async () => {
    const { controller, byId, fact, item } = await withLinks({});
    await controller.activateInspectorNode(item("gone"));

    expect(byId("tree").querySelectorAll('[role="treeitem"]')).toHaveLength(3);
    expect(fact("Type")).toBe("link");
    expect(fact("Target")).toBe("missing.h5:/x");
  });

  it("keeps the tree when any node fails to load", async () => {
    // It used to be replaced by "Failed to load node", losing the user's place.
    const { controller, byId } = await withLinks({});
    await controller.showInspectorNode("/entry/gone");

    expect(byId("tree").querySelectorAll('[role="treeitem"]')).toHaveLength(3);
  });
});

describe("inspector details", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
  });

  const DETECTOR = {
    "/hdf5/tree": {
      path: "/",
      children: [{ name: "entry", path: "/entry", type: "group", hasChildren: true }],
    },
    "/hdf5/node": {
      path: "/entry/instrument/detector",
      type: "group",
      attrs: [{ name: "NX_class", value: "NXdetector" }],
    },
  };

  it("leaves out rows a node has no value for, and heads the attributes", async () => {
    const { controller, byId, fact } = await setup(DETECTOR);
    await controller.showInspectorNode("/entry/instrument/detector");

    expect(fact("Type")).toBe("group");
    expect(fact("Shape")).toBeUndefined();
    expect(fact("DType")).toBeUndefined();
    expect(fact("Target")).toBeUndefined();
    expect(byId("attrs").querySelector(".inspector-subhead").textContent).toBe("Attributes");
    expect(byId("details").classList.contains("is-empty")).toBe(false);
  });

  it("shows the path as a breadcrumb whose ancestors select their entry", async () => {
    const { controller, byId } = await setup(DETECTOR);
    await controller.loadInspectorRoot();
    await controller.showInspectorNode("/entry/instrument/detector");

    const crumbs = [...byId("path").querySelectorAll("[data-crumb-path]")].map((b) => b.dataset.crumbPath);
    expect(crumbs).toEqual(["/entry", "/entry/instrument"]);
    expect(byId("path").querySelector('[aria-current="location"]').textContent).toBe("detector");

    await controller.revealInspectorPath("/entry");
    const entry = byId("tree").querySelector('[data-path="/entry"]');
    expect(entry.getAttribute("aria-selected")).toBe("true");
    expect(document.activeElement).toBe(entry);
  });

  it("copies the selected path", async () => {
    const writeText = vi.fn().mockResolvedValue();
    Object.defineProperty(navigator, "clipboard", { value: { writeText }, configurable: true });
    const { controller, byId } = await setup(DETECTOR);
    await controller.showInspectorNode("/entry/instrument/detector");

    expect(await controller.copyInspectorPath()).toBe(true);
    expect(writeText).toHaveBeenCalledWith("/entry/instrument/detector");
    expect(byId("copy").getAttribute("aria-label")).toBe("Path copied");
    delete navigator.clipboard;
  });

  it("names the file in the status line instead of announcing every click", async () => {
    const { controller, setSectionBadgeState } = await setup(DETECTOR);
    await controller.showInspectorNode("/entry/instrument/detector");

    const [, tone, message] = setSectionBadgeState.mock.calls.at(-1);
    expect(tone).toBe("active");
    expect(message).toBe("master.h5");
  });

  it("marks each row with its kind", async () => {
    const { controller, byId } = await setup({ "/hdf5/tree": ROOT });
    await controller.loadInspectorRoot();

    const kinds = [...byId("tree").querySelectorAll(".inspector-icon")].map((i) => i.dataset.kind);
    expect(kinds).toEqual(["group", "dataset", "dataset"]);
    expect(byId("tree").querySelector('[data-path="/entry/data"]').getAttribute("aria-label")).toBe("data, group");
  });
});
