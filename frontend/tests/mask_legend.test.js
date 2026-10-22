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
  document.body.innerHTML = `<div id="legend" hidden></div>`;
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
  const legend = createMaskLegend({
    state,
    element: document.getElementById("legend"),
    getInpaintedFrame: () => inpainted,
    getActiveSaturationMax: () => 4,
    isSaturatedValue: (v, max) => v === max,
  });
  return { legend, state };
}

const labels = () => Array.from(document.querySelectorAll(".mask-legend-item")).map((el) => el.textContent);

describe("mask legend", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete globalThis.fetch;
  });

  it("lists only the mask colours on screen", async () => {
    const { legend } = await setup();
    legend.update();
    expect(labels()).toEqual(["Gap"]);
    expect(document.getElementById("legend").hidden).toBe(false);
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
    expect(document.getElementById("legend").hidden).toBe(true);

    const filled = await setup({ maskRaw: new Uint32Array([1, 2, 0, 0]) }, { inpainted: { data: new Float32Array(4), holes: new Uint8Array(4) } });
    filled.legend.update();
    expect(labels()).toEqual([]);
  });

  it("colours each swatch as the image draws that class", async () => {
    const { legend } = await setup({ maskRaw: new Uint32Array([0, 2, 0, 0]) });
    legend.update();
    const { currentMaskColors, rgbToHex } = await import("../modules/mask_colors.js");
    const swatch = document.querySelector(".mask-legend-swatch");
    const expected = rgbToHex(currentMaskColors({ colormap: "viridis" }).defective);
    const [r, g, b] = [1, 3, 5].map((i) => parseInt(expected.slice(i, i + 2), 16));
    expect(swatch.style.background).toBe(`rgb(${r}, ${g}, ${b})`);
  });
});
