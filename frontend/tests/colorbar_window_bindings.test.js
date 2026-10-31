import { beforeEach, describe, expect, it, vi } from "vitest";
import { bindColorbarWindowInteractions } from "../modules/colorbar_window_bindings.js";

function setup() {
  document.body.innerHTML = `<canvas id="bar"></canvas><input id="min" /><input id="max" />
    <input id="auto" type="checkbox" checked />`;
  const colorbar = document.getElementById("bar");
  Object.defineProperty(colorbar, "clientWidth", { value: 100 });
  colorbar.setPointerCapture = () => {};
  colorbar.hasPointerCapture = () => false;
  // Values 0..1000 across a 100 px bar.
  const state = { stats: { min: 0, max: 1000 }, min: 200, max: 400, autoScale: true };
  const handleContrastChanged = vi.fn();
  const autoContrast = vi.fn();
  document.getElementById("auto").addEventListener("change", autoContrast);
  bindColorbarWindowInteractions({
    state,
    elements: {
      colorbar,
      minInput: document.getElementById("min"),
      maxInput: document.getElementById("max"),
      autoScaleToggle: document.getElementById("auto"),
    },
    callbacks: {
      histogramValueToX: (v, w) => (v / 1000) * w,
      histogramXToValue: (x, w) => (x / w) * 1000,
      snapHistogramValue: (v) => Math.round(v),
      formatValue: (v) => String(v),
      redraw: vi.fn(),
      scheduleHistogram: vi.fn(),
    },
    core: {
      viewerSync: { handleContrastChanged },
    },
  });
  const fire = (type, init) => colorbar.dispatchEvent(Object.assign(new Event(type, { bubbles: true, cancelable: true }), init));
  return { state, fire, handleContrastChanged, autoContrast };
}

describe("colour bar as window/level", () => {
  beforeEach(() => {
    // Runs the frame at once; returning 0 leaves nothing pending, as after a
    // real frame has run.
    vi.spyOn(window, "requestAnimationFrame").mockImplementation((cb) => {
      cb(0);
      return 0;
    });
  });

  it("moves the window with the pointer, keeping its width, and switches Auto off", () => {
    const { state, fire, handleContrastChanged } = setup();
    fire("pointerdown", { button: 0, clientX: 30, pointerId: 1 });
    fire("pointermove", { clientX: 40, pointerId: 1 });
    expect([state.min, state.max]).toEqual([300, 500]);
    expect(state.autoScale).toBe(false);
    expect(document.getElementById("auto").checked).toBe(false);
    expect(document.getElementById("min").value).toBe("300");
    expect(handleContrastChanged).toHaveBeenCalledWith("levels");
  });

  it("stops the window at the end of the bar", () => {
    const { state, fire } = setup();
    fire("pointerdown", { button: 0, clientX: 30, pointerId: 1 });
    fire("pointermove", { clientX: 200, pointerId: 1 });
    expect([state.min, state.max]).toEqual([800, 1000]);
  });

  it("does nothing after the pointer is released", () => {
    const { state, fire } = setup();
    fire("pointerdown", { button: 0, clientX: 30, pointerId: 1 });
    fire("pointerup", { pointerId: 1 });
    fire("pointermove", { clientX: 60, pointerId: 1 });
    expect([state.min, state.max]).toEqual([200, 400]);
  });

  it("widens and narrows the window around its centre on scroll", () => {
    const { state, fire } = setup();
    fire("wheel", { deltaY: -200 });
    const narrowed = state.max - state.min;
    expect(narrowed).toBeLessThan(200);
    expect((state.min + state.max) / 2).toBeCloseTo(300, 0);
    fire("wheel", { deltaY: 400 });
    expect(state.max - state.min).toBeGreaterThan(200);
  });

  it("turns Auto Contrast back on on double-click", () => {
    const { fire, autoContrast } = setup();
    const toggle = document.getElementById("auto");
    toggle.checked = false;
    fire("dblclick", {});
    expect(toggle.checked).toBe(true);
    expect(autoContrast).toHaveBeenCalledTimes(1);
    fire("dblclick", {});
    expect(autoContrast).toHaveBeenCalledTimes(1);
  });
});
