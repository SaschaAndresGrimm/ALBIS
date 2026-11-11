import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

async function setup({ stored, rings, storedReference } = {}) {
  vi.resetModules();
  localStorage.clear();
  if (stored !== undefined) localStorage.setItem("albis.rings", JSON.stringify(stored));
  if (storedReference !== undefined) localStorage.setItem("albis.rings.reference", storedReference);
  globalThis.fetch = vi.fn(async () => ({ ok: true, json: async () => ({}) }));
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  const mod = await import("../modules/ring_list_controller.js");
  document.body.innerHTML = `
    <div id="list"></div><button id="add"></button><button id="clear"></button>
    <select id="reference"></select><button id="outside"></button>`;
  const analysisState = { rings: rings ?? mod.DEFAULT_RINGS.slice() };
  const onChange = vi.fn();
  const controller = mod.createRingListController({
    analysisState,
    elements: {
      ringList: document.getElementById("list"),
      ringAddBtn: document.getElementById("add"),
      ringClearBtn: document.getElementById("clear"),
      ringReferenceSelect: document.getElementById("reference"),
    },
    callbacks: { onChange },
  });
  return { mod, controller, analysisState, onChange };
}

const fields = () => Array.from(document.querySelectorAll("#list input"));
const values = () => fields().map((input) => input.value);

function type(input, value) {
  input.value = value;
  input.dispatchEvent(new Event("input", { bubbles: true }));
}

function leaveList(input) {
  input.dispatchEvent(new FocusEvent("focusout", { bubbles: true, relatedTarget: document.getElementById("outside") }));
}

describe("ring list", () => {
  beforeEach(() => localStorage.clear());
  afterEach(() => {
    vi.restoreAllMocks();
    delete globalThis.fetch;
  });

  it("starts from the three default rings and does not redraw while it is built", async () => {
    const { analysisState, onChange } = await setup();
    expect(values()).toEqual(["1", "3.67", "11.01"]);
    expect(analysisState.rings).toEqual([1, 3.67, 11.01]);
    expect(onChange).not.toHaveBeenCalled();
  });

  it("adds an empty field, which counts as a ring once it holds one", async () => {
    const { analysisState, onChange } = await setup();
    document.getElementById("add").click();
    expect(fields()).toHaveLength(4);
    expect(document.activeElement).toBe(fields()[3]);
    expect(analysisState.rings).toHaveLength(3);
    type(fields()[3], "2");
    expect(analysisState.rings).toEqual([1, 3.67, 11.01, 2]);
    expect(onChange).toHaveBeenCalled();
  });

  it("sorts and drops drafts only when focus leaves the list", async () => {
    const { analysisState } = await setup();
    document.getElementById("add").click();
    type(fields()[3], "2");
    expect(values()).toEqual(["1", "3.67", "11.01", "2"]);
    document.getElementById("add").click();
    leaveList(fields()[4]);
    expect(values()).toEqual(["1", "2", "3.67", "11.01"]);
    expect(analysisState.rings).toEqual([1, 2, 3.67, 11.01]);
  });

  it("marks a value that is not a d-spacing and leaves it out of the rings", async () => {
    const { analysisState } = await setup();
    type(fields()[1], "-3");
    expect(fields()[1].classList.contains("is-invalid")).toBe(true);
    expect(analysisState.rings).toEqual([1, 11.01]);
  });

  it("removes a ring, and allows none at all", async () => {
    const { analysisState } = await setup();
    document.querySelector('[data-ring-remove="1"]').click();
    expect(analysisState.rings).toEqual([1, 11.01]);
    document.getElementById("clear").click();
    expect(analysisState.rings).toEqual([]);
    expect(fields()).toHaveLength(0);
    expect(document.getElementById("clear").disabled).toBe(true);
  });

  it("sets rings from elsewhere without duplicates, sorted", async () => {
    const { controller, analysisState } = await setup({ rings: [3.669, 11.01] });
    controller.setRings([11.01, 3.897, 3.669, 3.6695]);
    expect(analysisState.rings).toEqual([3.669, 3.897, 11.01]);
  });

  it("stops at the maximum number of rings", async () => {
    const { mod } = await setup({ stored: Array.from({ length: 20 }, (_, i) => i + 1) });
    expect(fields()).toHaveLength(mod.MAX_RINGS);
    expect(document.getElementById("add").disabled).toBe(true);
  });

  it("remembers the rings across a reload", async () => {
    await setup();
    document.querySelector('[data-ring-remove="0"]').click();
    expect(JSON.parse(localStorage.getItem("albis.rings"))).toEqual([3.67, 11.01]);
    const stored = JSON.parse(localStorage.getItem("albis.rings"));
    const { analysisState } = await setup({ stored });
    expect(analysisState.rings).toEqual([3.67, 11.01]);
  });

  it("takes a dragged ring by its index among the valid rings", async () => {
    const { controller, analysisState } = await setup();
    type(fields()[0], "");
    controller.setRingValue(1, 9.87654);
    expect(analysisState.rings).toEqual([3.67, 9.877]);
    expect(fields()[2].value).toBe("9.877");
  });
  describe("reference material", () => {
    const select = () => document.getElementById("reference");
    const choose = (id) => {
      select().value = id;
      select().dispatchEvent(new Event("change", { bubbles: true }));
    };

    it("starts at None, and lists every material under its group", async () => {
      const { analysisState } = await setup();
      const { CALIBRANTS, CALIBRANT_GROUPS } = await import("../modules/calibrant_data.js");
      expect(analysisState.ringReference).toBe("");
      expect(select().value).toBe("");
      const groups = Array.from(select().querySelectorAll("optgroup"));
      expect(groups).toHaveLength(CALIBRANT_GROUPS.length);
      // Ice first, under None and above the groups, where it is one click away.
      const options = Array.from(select().options).map((option) => option.value);
      expect(options.slice(0, 2)).toEqual(["", "ice_Ih"]);
      expect(options).toHaveLength(CALIBRANTS.length + 1);
      expect(groups[0].querySelector("option").value).toBe("Si");
    });

    it("draws the chosen material and remembers it, leaving the ring list alone", async () => {
      const { analysisState, onChange } = await setup();
      choose("Al");
      expect(analysisState.ringReference).toBe("Al");
      expect(onChange).toHaveBeenCalledTimes(1);
      expect(localStorage.getItem("albis.rings.reference")).toBe("Al");
      expect(analysisState.rings).toEqual([1, 3.67, 11.01]);
      document.getElementById("clear").click();
      expect(analysisState.ringReference).toBe("Al");
    });

    it("comes back after a reload, and ignores a material it does not know", async () => {
      let { analysisState } = await setup({ storedReference: "LaB6" });
      expect(analysisState.ringReference).toBe("LaB6");
      expect(select().value).toBe("LaB6");
      ({ analysisState } = await setup({ storedReference: "unobtainium" }));
      expect(analysisState.ringReference).toBe("");
    });

    it("keeps the choice when the menu is rebuilt for another language", async () => {
      const { controller } = await setup();
      choose("CeO2");
      controller.refresh();
      expect(select().value).toBe("CeO2");
    });

    it("turns off with None", async () => {
      const { analysisState } = await setup({ storedReference: "Si" });
      choose("");
      expect(analysisState.ringReference).toBe("");
      expect(localStorage.getItem("albis.rings.reference")).toBe("");
    });
  });
});
