import { describe, expect, it, vi } from "vitest";

import { createHottestPixelsController } from "../modules/hottest_pixels_controller.js";

// A 3 x 3 frame whose hottest pixel (index 4) is a gap in the mask.
function setup({ enabled = true, maskEnabled = true } = {}) {
  vi.useFakeTimers();
  document.body.innerHTML = `
    <section id="section" hidden>
      <input id="toggle" type="checkbox" />
      <div id="state"></div><span id="summary"></span>
      <input id="count" type="number" />
      <input id="masked" type="checkbox" />
      <button id="export"></button>
      <div id="list"><div id="body"></div></div>
    </section>`;
  const byId = (id) => document.getElementById(id);
  const state = {
    hottestPixels: enabled,
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
    hottestEnabled: false,
    hottestCount: 3,
    hottestIncludeMasked: false,
    hottestFound: [],
    hottestSelected: null,
  };
  const core = { overlayRender: { schedulePeakOverlay: vi.fn() } };
  const setStatus = vi.fn();
  const controller = createHottestPixelsController({
    core,
    state,
    analysisState,
    elements: {
      hottestSection: byId("section"),
      hottestToggle: byId("toggle"),
      hottestStateEl: byId("state"),
      hottestSummaryEl: byId("summary"),
      hottestCountInput: byId("count"),
      hottestIncludeMasked: byId("masked"),
      hottestExportBtn: byId("export"),
      hottestList: byId("list"),
      hottestBody: byId("body"),
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
  const rows = () =>
    Array.from(document.querySelectorAll("#body .peaks-row"), (row) =>
      Array.from(row.children, (cell) => cell.textContent),
    );
  return { controller, state, analysisState, core, setStatus, byId, switchOn, rows };
}

describe("Hottest Pixels", () => {
  it("is hidden until the beta setting is on", () => {
    expect(setup({ enabled: false }).byId("section").hidden).toBe(true);
    expect(setup({ enabled: true }).byId("section").hidden).toBe(false);
  });

  it("lists the hottest pixels, leaving out masked ones, and marks them", () => {
    const { switchOn, rows, core, byId } = setup();
    switchOn();
    expect(rows()).toEqual([
      ["1", "1", "0", "8", "—"],
      ["2", "1", "2", "7", "—"],
      ["3", "2", "1", "5", "—"],
    ]);
    expect(byId("list").classList.contains("has-mask")).toBe(true);
    expect(core.overlayRender.schedulePeakOverlay).toHaveBeenCalled();
  });

  it("lists masked pixels too when asked, with their mask value", () => {
    const { switchOn, rows, byId } = setup();
    switchOn();
    byId("masked").checked = true;
    byId("masked").dispatchEvent(new Event("change"));
    vi.runAllTimers();
    expect(rows()[0]).toEqual(["1", "1", "1", "99", "1"]);
  });

  it("keeps masked pixels while the mask is not applied", () => {
    const { switchOn, rows } = setup({ maskEnabled: false });
    switchOn();
    expect(rows()[0][3]).toBe("99");
  });

  it("re-runs on schedule and keeps the selection on the same pixel", () => {
    const { switchOn, state, analysisState, controller, byId } = setup();
    switchOn();
    byId("body").querySelectorAll(".peaks-row")[1].click(); // the 7 at (1, 2)
    state.dataRaw = Uint32Array.from([1, 2, 0, 3, 99, 5, 0, 7, 2]); // the 8 cools to 2
    controller.schedule();
    vi.runAllTimers();
    expect(analysisState.hottestSelected).toBe(0);
    expect(analysisState.hottestFound[0].value).toBe(7);
  });

  it("switches off when the setting is turned off", () => {
    const { switchOn, controller, analysisState, byId, rows } = setup();
    switchOn();
    controller.setEnabled(false);
    expect(byId("section").hidden).toBe(true);
    expect(byId("toggle").checked).toBe(false);
    expect(analysisState.hottestEnabled).toBe(false);
    expect(rows()).toEqual([]);
  });

  it("refuses to export with nothing listed, saying why", () => {
    const { controller, setStatus } = setup();
    controller.exportCsv();
    expect(setStatus).toHaveBeenCalledWith(expect.any(String), { tone: "warning" });
  });

  it("clamps the count typed", () => {
    const { switchOn, byId, rows } = setup();
    switchOn();
    byId("count").value = "0";
    byId("count").dispatchEvent(new Event("change"));
    vi.runAllTimers();
    expect(byId("count").value).toBe("1");
    expect(rows()).toHaveLength(1);
  });
});
