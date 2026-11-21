import { describe, expect, it, vi } from "vitest";
import { createFrameSumController } from "../modules/frame_sum_controller.js";

function setup(stateOverrides = {}) {
  document.body.innerHTML = `
    <div id="sum-wrap" class="is-hidden"><button id="sum" type="button"></button></div>
    <select id="step"></select>
    <select id="more-step"></select>
    <button id="pill" type="button" hidden></button>
    <div id="elsewhere"></div>
  `;
  const state = {
    frameIndex: 0,
    frameCount: 100,
    frameSum: 1,
    frameSumOp: "sum",
    step: 1,
    hasFrame: true,
    seriesFiles: [],
    autoload: { mode: "file" },
    ...stateOverrides,
  };
  const setFrameStep = vi.fn((value) => {
    state.step = value;
  });
  const core = {
    framePlayback: { requestFrame: vi.fn() },
    chromeToolbar: { updateToolbar: vi.fn(), closeToolbarPlaybackPopover: vi.fn() },
  };
  const el = (id) => document.getElementById(id);
  const controller = createFrameSumController({
    state,
    elements: {
      frameSumWrap: el("sum-wrap"),
      frameSumButton: el("sum"),
      footerFrameSum: el("pill"),
      frameStep: el("step"),
      toolbarMoreStep: el("more-step"),
    },
    callbacks: { setFrameStep },
    core,
  });
  controller.sync();
  const popover = () => el("sum-wrap").querySelector(".frame-sum-popover");
  const count = () => popover().querySelector("input");
  const isOpen = () => el("sum-wrap").classList.contains("is-open");
  return { state, controller, core, setFrameStep, el, popover, count, isOpen };
}

describe("the Sum control's button", () => {
  it("says what is shown: one frame, a sum or a mean", () => {
    const { controller, el, state } = setup();
    expect(el("sum").textContent).toBe("frame_sum.option.single ▾");
    controller.setFrameSum(5);
    expect(el("sum").textContent).toBe("frame_sum.option ▾");
    expect(el("sum").classList.contains("is-on")).toBe(true);
    controller.setFrameSumOp("mean");
    expect(state.frameSumOp).toBe("mean");
    expect(el("sum").textContent).toBe("frame_sum.option.mean ▾");
  });

  it("is shown for a stack of frames and hidden for a single image", () => {
    expect(setup().el("sum-wrap").classList.contains("is-hidden")).toBe(false);
    expect(setup({ frameCount: 1 }).el("sum-wrap").classList.contains("is-hidden")).toBe(true);
  });

  it("opens its popover, and closes it on a click elsewhere or Escape", () => {
    const { el, isOpen, core } = setup();
    el("sum").click();
    expect(isOpen()).toBe(true);
    expect(el("sum").getAttribute("aria-expanded")).toBe("true");
    expect(core.chromeToolbar.closeToolbarPlaybackPopover).toHaveBeenCalled();
    el("elsewhere").click();
    expect(isOpen()).toBe(false);
    el("sum").click();
    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }));
    expect(isOpen()).toBe(false);
  });
});

describe("the Sum popover", () => {
  it("takes any count, typed: 3 as well as the presets", () => {
    const { state, count, core } = setup();
    count().value = "3";
    count().dispatchEvent(new Event("change"));
    expect(state.frameSum).toBe(3);
    expect(core.framePlayback.requestFrame).toHaveBeenCalledWith(0);
  });

  it("keeps a typed count inside 1 to 1000", () => {
    const { state, count } = setup();
    count().value = "5000";
    count().dispatchEvent(new Event("change"));
    expect(state.frameSum).toBe(1000);
    expect(count().value).toBe("1000");
  });

  it("sets a preset in one click and closes", () => {
    const { state, popover, isOpen, el } = setup();
    el("sum").click();
    const ten = [...popover().querySelectorAll(".frame-sum-preset")].find((b) => b.textContent === "10");
    ten.click();
    expect(state.frameSum).toBe(10);
    expect(ten.classList.contains("is-active")).toBe(true);
    expect(isOpen()).toBe(false);
  });

  it("switches between sum and mean, showing which is on", () => {
    const { state, popover } = setup({ frameSum: 10, step: 10 });
    const [sum, mean] = popover().querySelectorAll(".frame-sum-op");
    expect(sum.getAttribute("aria-pressed")).toBe("true");
    mean.click();
    expect(state.frameSumOp).toBe("mean");
    expect(mean.getAttribute("aria-pressed")).toBe("true");
    expect(sum.getAttribute("aria-pressed")).toBe("false");
  });

  it("goes back to a single frame", () => {
    const { state, popover, el } = setup({ frameSum: 10, step: 10 });
    popover().querySelector(".frame-sum-off").click();
    expect(state.frameSum).toBe(1);
    expect(el("pill").hidden).toBe(true);
  });
});

describe("the step with the Sum", () => {
  it("follows any sum, adding it to the step lists", () => {
    const { state, controller, el, setFrameStep } = setup();
    controller.setFrameSum(3);
    expect(setFrameStep).toHaveBeenCalledWith(3);
    expect(state.step).toBe(3);
    expect([...el("step").options].map((o) => o.value)).toContain("3");
    expect([...el("more-step").options].map((o) => o.value)).toContain("3");
    expect(el("step").value).toBe("3");
  });

  it("leaves a step the user chose alone", () => {
    const { controller, setFrameStep } = setup({ step: 100 });
    controller.setFrameSum(10);
    expect(setFrameStep).toHaveBeenCalledWith(100);
  });
});

describe("the footer pill and the keys", () => {
  it("says what is shown and opens the popover with the count ready to type", () => {
    const { controller, el, isOpen, count } = setup();
    controller.setFrameSum(10);
    expect(el("pill").hidden).toBe(false);
    expect(el("pill").textContent).toBe("frame_sum.footer.sum");
    el("pill").click();
    expect(isOpen()).toBe(true);
    expect(document.activeElement).toBe(count());
  });

  it("steps the count along the ladder with [ and ]", () => {
    const { state, controller } = setup({ frameSum: 3, step: 3 });
    controller.stepFrameSum(1);
    expect(state.frameSum).toBe(5);
    controller.stepFrameSum(-1);
    controller.stepFrameSum(-1);
    expect(state.frameSum).toBe(1);
    expect(controller.stepFrameSum(-1)).toBe(true);
    expect(state.frameSum).toBe(1);
  });

  it("does nothing for a live source", () => {
    const { state, controller } = setup({ autoload: { mode: "simplon" } });
    expect(controller.stepFrameSum(1)).toBe(false);
    expect(state.frameSum).toBe(1);
  });
});
