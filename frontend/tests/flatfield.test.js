import fs from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { applyFlat, flatFits, normalizeFlatScale, prepareFlat } from "../modules/flatfield.js";
import { createInpaintView } from "../modules/inpainting.js";

const { flatfield } = JSON.parse(fs.readFileSync(path.join(process.cwd(), "tests", "fixtures", "inpainting_cases.json"), "utf8"));

describe("flatfield", () => {
  it.each(flatfield.map((c) => [c.name, c]))("divides like the exports do: %s", (_name, c) => {
    const flat = prepareFlat(Uint32Array.from(c.flat), c.width, c.height);
    expect([...flat.holes]).toEqual(c.holes);
    expect(flat.mean).toBeCloseTo(c.mean, 3);
    const out = applyFlat(Uint32Array.from(c.raw), flat, c.scale);
    c.expected.forEach((value, i) => (value === null ? expect(out[i]).toBeNaN() : expect(out[i]).toBeCloseTo(value, 2)));
  });

  it("leaves out what a flat cannot divide by: masked, zero, weak and overflowing pixels", () => {
    const data = new Uint16Array([1000, 0, 1000, 20, 65535, 1000]);
    const mask = new Uint32Array([0, 0, 1, 0, 0, 0]);
    const flat = prepareFlat(data, 3, 2, mask);
    expect([...flat.holes]).toEqual([0, 1, 1, 1, 1, 0]);
    expect(flat.mean).toBe(1000);
    expect(prepareFlat(new Uint16Array(4), 2, 2)).toBeNull();
    expect(flatFits(flat, 3, 2)).toBe(true);
    expect(flatFits(flat, 2, 3)).toBe(false);
    expect(normalizeFlatScale("odd")).toBe("counts");
  });

  it("keeps the image's scale in counts, gives 0 to 1 in transmission, and has no value where the frame has none", () => {
    const flat = prepareFlat(new Float32Array([500, 1000, 1500, 1000]), 2, 2);
    expect([...applyFlat(new Uint32Array([500, 500, 1500, 1000]), flat, "counts")]).toEqual([1000, 500, 1000, 1000]);
    expect([...applyFlat(new Uint32Array([500, 500, 1500, 1000]), flat, "transmission")]).toEqual([1, 0.5, 1, 1]);
    const out = applyFlat(new Uint32Array([5, 5, 5, 5]), flat, "counts", new Uint8Array([0, 0, 0, 1]));
    expect(out[3]).toBeNaN();
  });

  it("runs before inpainting in the viewer's frame: what the flat cannot divide is filled", () => {
    const flat = prepareFlat(new Float32Array([1000, 0, 1000, 1000, 1000, 1000]), 3, 2);
    const state = {
      dataRaw: new Uint32Array([100, 7, 100, 100, 100, 100]),
      width: 3,
      height: 2,
      flatEnabled: true,
      flatScale: "transmission",
      inpaintEnabled: false,
      maskAvailable: false,
    };
    const view = createInpaintView({ state, getActiveSaturationMax: () => null, isSaturatedValue: () => false, getFlat: () => flat });
    const shown = view.frame();
    expect(shown.flatfielded).toBe(true);
    expect(shown.data[0]).toBeCloseTo(0.1, 6);
    expect(shown.data[1]).toBeNaN();
    state.inpaintEnabled = true;
    const filled = view.frame();
    expect(filled.holes[1]).toBe(1);
    expect(filled.data[1]).toBeCloseTo(0.1, 6);
    state.flatEnabled = false;
    state.inpaintEnabled = false;
    expect(view.frame()).toBeNull();
  });
});
