import { describe, expect, it } from "vitest";

import { attachRangeFill, rangeFillPercent, syncRangeFill } from "../modules/range_fill.js";

// The track fills up to the thumb; outside Firefox that takes --range-fill.
function slider({ min = "0", max = "100", value = "0" } = {}) {
  const input = document.createElement("input");
  input.type = "range";
  input.min = min;
  input.max = max;
  input.value = value;
  return input;
}

describe("range fill", () => {
  it("is where the value sits between min and max", () => {
    expect(rangeFillPercent(slider({ min: "1", max: "2000", value: "1" }))).toBe(0);
    expect(rangeFillPercent(slider({ min: "1", max: "2000", value: "2000" }))).toBe(100);
    expect(rangeFillPercent(slider({ min: "0.02", max: "50", value: "25.01" }))).toBeCloseTo(50);
  });

  it("is empty for a slider with no range, such as one frame", () => {
    expect(rangeFillPercent(slider({ min: "1", max: "1", value: "1" }))).toBe(0);
  });

  it("is set by syncRangeFill, which code that sets a value calls", () => {
    const input = slider({ value: "40" });
    syncRangeFill(input);
    expect(input.style.getPropertyValue("--range-fill")).toBe("40%");
    input.value = "75";
    syncRangeFill(input);
    expect(input.style.getPropertyValue("--range-fill")).toBe("75%");
  });

  it("leaves other inputs alone", () => {
    const input = document.createElement("input");
    input.type = "number";
    syncRangeFill(input);
    expect(input.style.getPropertyValue("--range-fill")).toBe("");
  });

  it("follows the sliders on the page and a user's drag", () => {
    document.body.innerHTML = '<input id="r" type="range" min="0" max="10" value="2" />';
    const input = document.getElementById("r");
    attachRangeFill(document);
    expect(input.style.getPropertyValue("--range-fill")).toBe("20%");
    input.value = "5";
    input.dispatchEvent(new Event("input", { bubbles: true }));
    expect(input.style.getPropertyValue("--range-fill")).toBe("50%");
  });
});
