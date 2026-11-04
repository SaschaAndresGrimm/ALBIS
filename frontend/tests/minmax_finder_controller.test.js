import { beforeAll, describe, expect, it, vi } from "vitest";
import fs from "node:fs";

import { createMinMaxFinderController } from "../modules/minmax_finder_controller.js";
import { initializeI18n } from "../modules/i18n.js";

// The real English catalogue, so a renamed key fails here.
beforeAll(async () => {
  const catalogue = JSON.parse(fs.readFileSync("frontend/locales/en.json", "utf8"));
  globalThis.fetch = vi.fn(async () => ({ ok: true, json: async () => catalogue }));
  await initializeI18n({ backendLanguage: "en" });
});

// A 3 x 3 frame whose highest pixel (index 4) is a gap in the mask.
function setup({ maskEnabled = true } = {}) {
  vi.useFakeTimers();
  document.body.innerHTML = `
    <section id="section">
      <input id="toggle" type="checkbox" />
      <div id="state"></div><span id="summary"></span>
      <select id="mode"><option value="max"></option><option value="min"></option><option value="both"></option></select>
      <input id="count" type="number" />
      <input id="masked" type="checkbox" />
      <button id="export"></button>
      <div id="list"><div id="body"></div></div>
    </section>`;
  const byId = (id) => document.getElementById(id);
  const state = {
    hasFrame: true,
    width: 3,
    height: 3,
    dataRaw: Uint32Array.from([1, 8, 0, 3, 99, 5, 0, 7, 2]),
    maskAvailable: true,
    maskEnabled,
    maskRaw: Uint32Array.from([0, 0, 0, 0, 1, 0, 0, 0, 0]),
    maskShape: [3, 3],
    maskSaturatedEnabled: false,
  };
  const analysisState = {
    minmaxEnabled: false,
    minmaxMode: "max",
    minmaxCount: 3,
    minmaxIncludeMasked: false,
    minmaxFound: [],
    minmaxSelected: [],
    minmaxSelectionAnchor: null,
  };
  const core = { overlayRender: { schedulePeakOverlay: vi.fn() } };
  const setStatus = vi.fn();
  const controller = createMinMaxFinderController({
    core,
    state,
    analysisState,
    elements: {
      minmaxToggle: byId("toggle"),
      minmaxStateEl: byId("state"),
      minmaxSummaryEl: byId("summary"),
      minmaxModeSelect: byId("mode"),
      minmaxCountInput: byId("count"),
      minmaxIncludeMasked: byId("masked"),
      minmaxExportBtn: byId("export"),
      minmaxList: byId("list"),
      minmaxBody: byId("body"),
    },
    constants: { badMaskBits: 0x1f },
    callbacks: {
      setSectionBadgeState: (el, _tone, text) => (el.textContent = text),
      setSummaryChip: (el, text) => (el.textContent = text),
      formatStat: String,
      getActiveSaturationMax: () => null,
      isSaturatedValue: () => false,
      setStatus,
    },
  });
  const switchOn = () => {
    byId("toggle").checked = true;
    byId("toggle").dispatchEvent(new Event("change"));
  };
  const change = (id, value) => {
    const el = byId(id);
    if (el.type === "checkbox") el.checked = value;
    else el.value = value;
    el.dispatchEvent(new Event("change"));
    vi.runAllTimers();
  };
  const rows = () =>
    Array.from(document.querySelectorAll("#body .peaks-row"), (row) =>
      Array.from(row.children, (cell) => cell.textContent),
    );
  return { controller, state, analysisState, core, setStatus, byId, switchOn, change, rows };
}

describe("Min/Max Finder", () => {
  it("lists the highest pixels, leaving out masked ones, and marks them", () => {
    const { switchOn, rows, core, byId } = setup();
    switchOn();
    expect(rows()).toEqual([
      ["1", "1", "0", "8", "—"],
      ["2", "1", "2", "7", "—"],
      ["3", "2", "1", "5", "—"],
    ]);
    expect(byId("list").classList.contains("has-mask")).toBe(true);
    expect(byId("state").textContent).toBe("Highest pixel: 8 at X 1, Y 0");
    expect(core.overlayRender.schedulePeakOverlay).toHaveBeenCalled();
  });

  it("lists the lowest pixels with Min, and says when several share the lowest value", () => {
    const { switchOn, change, rows, byId } = setup();
    switchOn();
    change("mode", "min");
    expect(rows().map((row) => row.slice(1, 4))).toEqual([
      ["2", "0", "0"],
      ["0", "2", "0"],
      ["0", "0", "1"],
    ]);
    expect(byId("state").textContent).toBe("Lowest value 0, shared by 2 pixels");
  });

  it("lists both ends in one table with Both, each numbered on its own", () => {
    const { switchOn, change, rows, byId, analysisState } = setup();
    switchOn();
    change("count", "2");
    change("mode", "both");
    expect(rows().map((row) => [row[0], row[3]])).toEqual([
      ["▲1", "8"],
      ["▲2", "7"],
      ["▼1", "0"],
      ["▼2", "0"],
    ]);
    expect(analysisState.minmaxFound.map((pixel) => pixel.kind)).toEqual(["max", "max", "min", "min"]);
    expect(byId("state").textContent).toBe("Highest 8 (1 pixel), lowest 0 (2 pixels)");
  });

  it("lists masked pixels too when asked, with their mask value", () => {
    const { switchOn, change, rows } = setup();
    switchOn();
    change("masked", true);
    expect(rows()[0]).toEqual(["1", "1", "1", "99", "1"]);
  });

  it("keeps masked pixels while the mask is not applied", () => {
    const { switchOn, rows } = setup({ maskEnabled: false });
    switchOn();
    expect(rows()[0][3]).toBe("99");
  });

  it("selects like the Peak Finder: click, Shift for a range, Cmd/Ctrl to toggle", () => {
    const { switchOn, analysisState, byId } = setup();
    switchOn();
    const click = (row, init = {}) =>
      byId("body").querySelectorAll(".peaks-row")[row].dispatchEvent(new MouseEvent("click", { bubbles: true, ...init }));
    click(0);
    expect(analysisState.minmaxSelected).toEqual([0]);
    click(2, { shiftKey: true });
    expect(analysisState.minmaxSelected).toEqual([0, 1, 2]);
    click(1, { metaKey: true });
    expect(analysisState.minmaxSelected).toEqual([0, 2]);
    click(1, { ctrlKey: true });
    expect(analysisState.minmaxSelected).toEqual([0, 1, 2]);
    expect(byId("body").querySelectorAll(".peaks-row.is-selected")).toHaveLength(3);
    click(1);
    expect(analysisState.minmaxSelected).toEqual([1]);
    click(1);
    expect(analysisState.minmaxSelected).toEqual([]);
  });

  it("re-runs on schedule and keeps the selection on the same pixels", () => {
    const { switchOn, state, analysisState, controller, byId } = setup();
    switchOn();
    const rows = byId("body").querySelectorAll(".peaks-row");
    rows[1].click(); // the 7 at (1, 2)
    rows[2].dispatchEvent(new MouseEvent("click", { bubbles: true, metaKey: true })); // the 5 at (2, 1)
    state.dataRaw = Uint32Array.from([1, 2, 0, 3, 99, 5, 0, 7, 2]); // the 8 cools to 2
    controller.schedule();
    vi.runAllTimers();
    expect(analysisState.minmaxFound.map((p) => p.value)).toEqual([7, 5, 3]);
    expect(analysisState.minmaxSelected).toEqual([0, 1]);
  });

  it("switches off and clears with its switch, as hiding the tool does", () => {
    const { switchOn, analysisState, byId, rows } = setup();
    switchOn();
    byId("toggle").checked = false;
    byId("toggle").dispatchEvent(new Event("change"));
    expect(analysisState.minmaxEnabled).toBe(false);
    expect(rows()).toEqual([]);
  });

  it("refuses to export with nothing listed, saying why", () => {
    const { controller, setStatus } = setup();
    controller.exportCsv();
    expect(setStatus).toHaveBeenCalledWith(expect.any(String), { tone: "warning" });
  });

  it("clamps the count typed", () => {
    const { switchOn, change, byId, rows } = setup();
    switchOn();
    change("count", "0");
    expect(byId("count").value).toBe("1");
    expect(rows()).toHaveLength(1);
  });
});
