/**
 * The Image Header section had no test of its own while it lived inside the
 * inspector controller; these pin what it shows for a header, for none, and
 * when the request fails.
 */

import fs from "node:fs";
import path from "node:path";

import { beforeEach, describe, expect, it, vi } from "vitest";

const EN = JSON.parse(
  fs.readFileSync(path.join(process.cwd(), "frontend", "locales", "en.json"), "utf8")
);

async function setup(fetchJSON) {
  vi.resetModules();
  global.fetch = vi.fn(async () => ({ ok: true, json: async () => EN }));
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  const { createImageHeaderPanel } = await import("../modules/image_header_panel.js");
  document.body.innerHTML = `
    <div id="state"></div><div id="loading" class="is-hidden"></div>
    <pre id="text" class="is-hidden"></pre><div id="empty"></div>`;
  const byId = (id) => document.getElementById(id);
  const setSectionBadgeState = vi.fn();
  const state = { imageHeaderFile: "", imageHeaderText: "" };
  const panel = createImageHeaderPanel({
    apiBase: "/api",
    state,
    elements: {
      imageHeaderStateEl: byId("state"),
      imageHeaderLoading: byId("loading"),
      imageHeaderText: byId("text"),
      imageHeaderEmpty: byId("empty"),
    },
    callbacks: {
      fetchJSON,
      isHeaderCapableFile: (file) => /\.(cbf|tiff?)$/.test(file),
      setSectionBadgeState,
      renderSkeletonBlock: () => {},
    },
  });
  return { panel, byId, state, setSectionBadgeState };
}

describe("image header panel", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
  });

  it("shows a file's header and caches it for the same file", async () => {
    const fetchJSON = vi.fn().mockResolvedValue({ header: "# Detector: PILATUS4" });
    const { panel, byId } = await setup(fetchJSON);

    await panel.loadImageHeader("a.cbf");
    await panel.loadImageHeader("a.cbf");

    expect(byId("text").textContent).toBe("# Detector: PILATUS4");
    expect(byId("text").classList.contains("is-hidden")).toBe(false);
    expect(byId("empty").classList.contains("is-hidden")).toBe(true);
    expect(fetchJSON).toHaveBeenCalledTimes(1);
  });

  it("says there is no header when the request fails", async () => {
    const { panel, byId, setSectionBadgeState } = await setup(vi.fn().mockRejectedValue(new Error("boom")));

    await panel.loadImageHeader("b.tiff");

    expect(byId("empty").textContent).toBe("No header available.");
    expect(byId("empty").classList.contains("is-hidden")).toBe(false);
    expect(setSectionBadgeState.mock.calls.at(-1)[1]).toBe("warning");
  });

  it("clears without asking the server for a file that has no header", async () => {
    const fetchJSON = vi.fn();
    const { panel, state } = await setup(fetchJSON);

    await panel.loadImageHeader("c.h5");

    expect(fetchJSON).not.toHaveBeenCalled();
    expect(state.imageHeaderFile).toBe("");
  });
});
