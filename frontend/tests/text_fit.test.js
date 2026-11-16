import { describe, expect, it } from "vitest";
import { ELLIPSIS, longestFitting, middleEllipsis } from "../modules/text_fit_utils.js";
import { fitMiddle, showEnd } from "../modules/field_text_fit.js";

// jsdom lays nothing out: a box `width` characters wide, its text one unit each.
function box(element, width) {
  Object.defineProperty(element, "clientWidth", { configurable: true, get: () => width });
  Object.defineProperty(element, "scrollWidth", {
    configurable: true,
    get: () => String(element.value ?? element.textContent).length,
  });
  return element;
}

describe("text_fit_utils", () => {
  it("cuts a name in its middle, keeping a little more of the end", () => {
    expect(middleEllipsis("abcdefghij", 4)).toBe(`ab${ELLIPSIS}ij`);
    expect(middleEllipsis("abcdefghij", 5)).toBe(`ab${ELLIPSIS}hij`);
    expect(middleEllipsis("abc", 5)).toBe("abc");
    expect(middleEllipsis("abc", 0)).toBe(ELLIPSIS);
    expect(middleEllipsis(null, 3)).toBe("");
  });

  it("finds the longest length that fits", () => {
    expect(longestFitting(100, (n) => n <= 37)).toBe(37);
    expect(longestFitting(10, () => true)).toBe(10);
    expect(longestFitting(10, () => false)).toBe(0);
    expect(longestFitting(0, () => true)).toBe(0);
  });
});

describe("field_text_fit", () => {
  it("fits the planned names to the width, the frame number kept, the whole on hover", () => {
    const names = "DDR4_1ms_tomo_5keV_20261007_041925_projection_0438_corrected_?????.tiff";
    const element = box(document.createElement("span"), 30);
    fitMiddle(element, names);
    expect(element.textContent.length).toBeLessThanOrEqual(31); // 1 px of rounding allowed
    expect(element.textContent.startsWith("DDR4_1ms")).toBe(true);
    expect(element.textContent.endsWith("_?????.tiff")).toBe(true);
    expect(element.title).toBe(names);
    // Short enough: shown whole, no tooltip repeating it.
    fitMiddle(element, "img_?????.tiff");
    expect(element.textContent).toBe("img_?????.tiff");
    expect(element.hasAttribute("title")).toBe(false);
    // Not laid out (the dialog hidden): the text, uncut.
    const hidden = box(document.createElement("span"), 0);
    fitMiddle(hidden, names);
    expect(hidden.textContent).toBe(names);
  });

  it("scrolls a long path to its end and names it whole on hover", () => {
    const input = box(document.createElement("input"), 20);
    input.value = "/Volumes/exchange$/SasG/DDR4_1ms_tomo_projection";
    showEnd(input);
    expect(input.scrollLeft).toBe(input.value.length);
    expect(input.title).toBe(input.value);
    input.value = "/data/out";
    showEnd(input);
    expect(input.hasAttribute("title")).toBe(false);
    expect(() => showEnd(null)).not.toThrow();
  });
});
