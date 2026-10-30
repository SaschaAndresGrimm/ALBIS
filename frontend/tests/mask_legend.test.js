import { afterEach, describe, expect, it, vi } from "vitest";

async function setup(stateOverrides = {}, { inpainted = null } = {}) {
  vi.resetModules();
  globalThis.fetch = vi.fn(async () => ({
    ok: true,
    json: async () => ({ "legend.mask.gap": "Gap", "legend.mask.defective": "Defective", "legend.mask.saturated": "Saturated" }),
  }));
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  const { createMaskLegend } = await import("../modules/mask_legend.js");
  document.body.innerHTML = `<div class="hist-mask-key" hidden><div id="legend"></div></div>`;
  localStorage.clear();
  const state = {
    colormap: "viridis",
    width: 2,
    height: 2,
    dataRaw: new Uint16Array([1, 2, 3, 4]),
    maskEnabled: true,
    maskAvailable: true,
    maskRaw: new Uint32Array([0, 1, 0, 0]),
    maskShape: [2, 2],
    maskSaturatedEnabled: false,
    ...stateOverrides,
  };
  const changes = { count: 0 };
  const legend = createMaskLegend({
    state,
    element: document.getElementById("legend"),
    getInpaintedFrame: () => inpainted,
    getActiveSaturationMax: () => 4,
    isSaturatedValue: (v, max) => v === max,
    onChange: () => (changes.count += 1),
  });
  return { legend, state, changes };
}

// The classes on screen: chips that are not dimmed.
const labels = () =>
  Array.from(document.querySelectorAll(".mask-chip:not(.is-absent)")).map((el) => el.textContent);
const wrapper = () => document.querySelector(".hist-mask-key");

describe("mask legend", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete globalThis.fetch;
  });

  it("lists only the mask colours on screen", async () => {
    const { legend } = await setup();
    legend.update();
    expect(labels()).toEqual(["Gap"]);
    expect(document.querySelectorAll(".mask-chip")).toHaveLength(3);
    expect(wrapper().hidden).toBe(false);
  });

  it("adds Saturated only with Mask saturated on and such a pixel present", async () => {
    const { legend, state } = await setup({ maskSaturatedEnabled: true });
    legend.update();
    expect(labels()).toEqual(["Gap", "Saturated"]);
    state.dataRaw = new Uint16Array([1, 2, 3, 3]);
    legend.update();
    expect(labels()).toEqual(["Gap"]);
  });

  it("says nothing while the mask is off, or where inpainting filled it", async () => {
    const off = await setup({ maskEnabled: false, maskRaw: new Uint32Array([1, 2, 0, 0]) });
    off.legend.update();
    expect(labels()).toEqual([]);

    const filled = await setup({ maskRaw: new Uint32Array([1, 2, 0, 0]) }, { inpainted: { data: new Float32Array(4), holes: new Uint8Array(4) } });
    filled.legend.update();
    expect(labels()).toEqual([]);
  });

  it("colours each swatch as the image draws that class", async () => {
    const { legend } = await setup({ maskRaw: new Uint32Array([0, 2, 0, 0]) });
    legend.update();
    const { currentMaskColors, rgbToHex } = await import("../modules/mask_colors.js");
    const swatch = document.querySelector('[data-mask-class="defective"] .mask-legend-swatch');
    const expected = rgbToHex(currentMaskColors({ colormap: "viridis" }).defective);
    const [r, g, b] = [1, 3, 5].map((i) => parseInt(expected.slice(i, i + 2), 16));
    expect(swatch.style.background).toBe(`rgb(${r}, ${g}, ${b})`);
  });

  it("hides the row until there is an image", async () => {
    const { legend } = await setup({ dataRaw: null });
    legend.update();
    expect(wrapper().hidden).toBe(true);
  });

  it("opens a popover of ready choices, including the colour map's own, and applies a pick", async () => {
    const { legend, state, changes } = await setup();
    legend.update();
    document.querySelector('[data-mask-class="gap"]').click();
    const popover = document.querySelector(".mask-popover");
    expect(popover).not.toBeNull();
    const groups = popover.querySelectorAll(".mask-popover-swatches");
    expect(groups).toHaveLength(2);
    expect(groups[1].querySelectorAll(".mask-swatch")).toHaveLength(3);
    // On Viridis, what is offered as standing out does stand out.
    const { blendsWithMap, hexToRgb } = await import("../modules/mask_colors.js");
    for (const swatch of groups[0].querySelectorAll(".mask-swatch")) {
      const rgb = swatch.style.background.match(/\d+/g).map(Number);
      expect(blendsWithMap(state, rgb)).toBe(false);
    }
    expect(hexToRgb).toBeTypeOf("function");
    groups[1].querySelector(".mask-swatch").click(); // the map's low end: blend the gaps in
    const { mapEndColors, rgbToHex } = await import("../modules/mask_colors.js");
    expect(state.maskColorOverrides.gap).toBe(rgbToHex(mapEndColors(state).low));
    expect(changes.count).toBe(1);
    expect(document.querySelector('[data-mask-class="gap"]').classList.contains("is-custom")).toBe(true);
  });

  it("closes the popover on Escape and when clicking elsewhere", async () => {
    const { legend } = await setup();
    legend.update();
    document.querySelector('[data-mask-class="gap"]').click();
    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }));
    expect(document.querySelector(".mask-popover")).toBeNull();
    document.querySelector('[data-mask-class="gap"]').click();
    document.body.dispatchEvent(new Event("pointerdown", { bubbles: true }));
    expect(document.querySelector(".mask-popover")).toBeNull();
  });

  it("Reset colours, shown once a colour is hand-set, drops every hand-set colour", async () => {
    const { legend, state } = await setup();
    legend.update();
    expect(document.getElementById("mask-colors-auto").hidden).toBe(true);
    const { setMaskColorOverride } = await import("../modules/mask_colors.js");
    setMaskColorOverride(state, "saturated", "#123456");
    legend.refresh();
    expect(document.getElementById("mask-colors-auto").hidden).toBe(false);
    document.getElementById("mask-colors-auto").click();
    expect(state.maskColorOverrides).toEqual({ gap: null, defective: null, saturated: null });
  });
});
