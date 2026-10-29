import { afterEach, describe, expect, it, vi } from "vitest";

import { attachNumberStepper, stepNumberInput } from "../modules/number_stepper.js";

// ALBIS's own up and down buttons for a number field, in place of the
// browser's spinner: they must step and notify exactly as the native one did.
function field(attrs = {}) {
  document.body.innerHTML = '<label class="range-inline"><span>BG</span><input id="n" type="number" /></label>';
  const input = document.getElementById("n");
  for (const [key, value] of Object.entries(attrs)) input.setAttribute(key, value);
  return input;
}

function press(button) {
  button.dispatchEvent(new MouseEvent("pointerdown", { bubbles: true, cancelable: true, button: 0 }));
  button.dispatchEvent(new MouseEvent("pointerup", { bubbles: true }));
}

afterEach(() => {
  vi.useRealTimers();
});

describe("stepNumberInput", () => {
  it("steps by the field's step within min and max", () => {
    const input = field({ step: "5", min: "0", max: "12", value: "5" });
    expect(stepNumberInput(input, 1)).toBe(true);
    expect(input.value).toBe("10");
    stepNumberInput(input, -1);
    expect(input.value).toBe("5");
  });

  it("steps by 1 where step is any, as the native spinner does", () => {
    const input = field({ step: "any", value: "2.5" });
    stepNumberInput(input, 1);
    expect(input.value).toBe("3.5");
    stepNumberInput(input, -1);
    stepNumberInput(input, -1);
    expect(input.value).toBe("1.5");
  });

  it("keeps a step='any' value within min and max", () => {
    const input = field({ step: "any", min: "0", max: "1", value: "0.5" });
    stepNumberInput(input, 1);
    expect(input.value).toBe("1");
  });

  it("fires input and then change, and nothing when the value stays", () => {
    const input = field({ max: "3", value: "3" });
    const seen = [];
    input.addEventListener("input", () => seen.push("input"));
    input.addEventListener("change", () => seen.push("change"));
    expect(stepNumberInput(input, 1)).toBe(false);
    expect(seen).toEqual([]);
    stepNumberInput(input, -1);
    expect(seen).toEqual(["input", "change"]);
  });

  it("does nothing to a disabled or read-only field", () => {
    const disabled = field({ value: "1" });
    disabled.disabled = true;
    expect(stepNumberInput(disabled, 1)).toBe(false);
    const readOnly = field({ value: "1" });
    readOnly.readOnly = true;
    expect(stepNumberInput(readOnly, 1)).toBe(false);
    expect(readOnly.value).toBe("1");
  });
});

describe("attachNumberStepper", () => {
  it("wraps the field where it was, keeping it and its listeners", () => {
    const input = field({ value: "4" });
    const onChange = vi.fn();
    input.addEventListener("change", onChange);
    const wrapper = attachNumberStepper(input, { variant: "num-stepper-compact" });
    expect(wrapper.className).toBe("num-stepper num-stepper-compact");
    expect(wrapper.parentElement.tagName).toBe("LABEL");
    expect(wrapper.firstElementChild).toBe(input);
    expect(input.classList.contains("num-stepper-input")).toBe(true);
    press(wrapper.querySelector(".num-stepper-btn.up"));
    expect(input.value).toBe("5");
    expect(onChange).toHaveBeenCalledTimes(1);
  });

  it("is attached only once", () => {
    const input = field();
    const first = attachNumberStepper(input);
    expect(attachNumberStepper(input)).toBe(first);
    expect(document.querySelectorAll(".num-stepper")).toHaveLength(1);
  });

  it("keeps its buttons out of the tab order and the accessibility tree", () => {
    const wrapper = attachNumberStepper(field());
    for (const button of wrapper.querySelectorAll("button")) {
      expect(button.tabIndex).toBe(-1);
      expect(button.getAttribute("aria-hidden")).toBe("true");
      expect(button.type).toBe("button");
    }
  });

  it("focuses the field, so the arrow keys carry on", () => {
    const input = field({ value: "1" });
    const wrapper = attachNumberStepper(input);
    press(wrapper.querySelector(".num-stepper-btn.down"));
    expect(document.activeElement).toBe(input);
    expect(input.value).toBe("0");
  });

  it("repeats while held and stops on release", () => {
    vi.useFakeTimers();
    const input = field({ value: "0" });
    const up = attachNumberStepper(input).querySelector(".num-stepper-btn.up");
    up.dispatchEvent(new MouseEvent("pointerdown", { bubbles: true, cancelable: true, button: 0 }));
    expect(input.value).toBe("1");
    vi.advanceTimersByTime(399);
    expect(input.value).toBe("1");
    vi.advanceTimersByTime(1 + 60 * 3);
    expect(input.value).toBe("4");
    up.dispatchEvent(new MouseEvent("pointerup", { bubbles: true }));
    vi.advanceTimersByTime(1000);
    expect(input.value).toBe("4");
  });

  it("ignores the buttons of a disabled field", () => {
    const input = field({ value: "1" });
    input.disabled = true;
    press(attachNumberStepper(input).querySelector(".num-stepper-btn.up"));
    expect(input.value).toBe("1");
  });
});
