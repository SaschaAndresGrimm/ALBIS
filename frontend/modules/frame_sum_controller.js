/**
 * The toolbar's Sum control: show the sum (or mean) of N consecutive frames.
 *
 * Owns `state.frameSum` and `state.frameSumOp`, the button beside the frame
 * number ("1 frame", "Sum 5", "Mean 5"), its popover (the count, presets,
 * Sum or Mean, and back to a single frame), the footer pill that says a sum
 * is on screen and opens the popover, and `[` / `]`. The window, the query
 * and the step rule are pure helpers in frame_sum_utils.js.
 *
 * The popover is built here rather than in index.html: the page is at its
 * size limit, and nothing but this control uses it.
 */

import { t } from "./i18n.js";
import { attachNumberStepper } from "./number_stepper.js";
import {
  FRAME_SUM_PRESETS,
  MAX_FRAME_SUM,
  activeFrameSum,
  frameSumAvailableFor,
  frameSumWindow,
  nextFrameSum,
  normalizeFrameSum,
  normalizeFrameSumOp,
  stepForFrameSum,
  stepOptionsFor,
} from "./frame_sum_utils.js";

export function createFrameSumController({ state, elements, callbacks, core }) {
  const { frameSumWrap, frameSumButton, footerFrameSum, frameStep, toolbarMoreStep } = elements;
  const { setFrameStep } = callbacks;

  function button(text, className, onClick) {
    const el = document.createElement("button");
    el.type = "button";
    el.className = `tool-btn ${className}`;
    el.textContent = text;
    el.addEventListener("click", onClick);
    return el;
  }

  // The popover: Frames [n ⌃⌄], presets, Sum | Mean, Single frame.
  const popover = document.createElement("div");
  popover.className = "toolbar-playback-popover frame-sum-popover";
  popover.id = "frame-sum-popover";
  popover.setAttribute("aria-hidden", "true");
  const countLabel = document.createElement("label");
  countLabel.className = "frame-sum-count";
  const countText = document.createElement("span");
  const countInput = document.createElement("input");
  Object.assign(countInput, { type: "number", min: "1", max: String(MAX_FRAME_SUM), step: "1", value: "1" });
  countLabel.append(countText, countInput);
  const presets = document.createElement("div");
  presets.className = "frame-sum-presets";
  const presetButtons = FRAME_SUM_PRESETS.map((value) =>
    button(String(value), "frame-sum-preset", () => {
      setFrameSum(value);
      setOpen(false);
    })
  );
  presets.append(...presetButtons);
  const ops = document.createElement("div");
  ops.className = "frame-sum-ops";
  ops.setAttribute("role", "group");
  const opButtons = { sum: button("", "frame-sum-op", () => setFrameSumOp("sum")), mean: button("", "frame-sum-op", () => setFrameSumOp("mean")) };
  ops.append(opButtons.sum, opButtons.mean);
  const offButton = button("", "frame-sum-off", () => {
    setFrameSum(1);
    setOpen(false);
  });
  popover.append(countLabel, presets, ops, offButton);
  frameSumWrap?.append(popover);
  attachNumberStepper(countInput);

  function commitCount() {
    setFrameSum(countInput.value);
    countInput.value = String(normalizeFrameSum(state.frameSum));
  }
  countInput.addEventListener("change", commitCount);
  countInput.addEventListener("keydown", (event) => {
    if (event.key !== "Enter") return;
    event.preventDefault();
    commitCount();
    setOpen(false);
  });

  function isOpen() {
    return Boolean(frameSumWrap?.classList.contains("is-open"));
  }

  function setOpen(open, { focusCount = false } = {}) {
    if (!frameSumWrap) return;
    if (open) core.chromeToolbar?.closeToolbarPlaybackPopover?.();
    frameSumWrap.classList.toggle("is-open", open);
    popover.setAttribute("aria-hidden", open ? "false" : "true");
    sync();
    if (open && focusCount) countInput.focus({ preventScroll: true });
    if (!open && popover.contains(document.activeElement)) frameSumButton?.focus({ preventScroll: true });
  }

  frameSumButton?.addEventListener("click", () => setOpen(!isOpen()));
  // The pill is where one looks on wondering why the image looks different.
  footerFrameSum?.addEventListener("click", () => setOpen(true, { focusCount: true }));
  document.addEventListener("click", (event) => {
    if (!isOpen()) return;
    if (frameSumWrap.contains(event.target) || footerFrameSum?.contains(event.target)) return;
    setOpen(false);
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && isOpen()) setOpen(false);
  });

  function localizeOptions() {
    [frameStep, toolbarMoreStep].forEach((select) => {
      Array.from(select?.options || []).forEach((option) => {
        option.textContent = t("toolbar.step.option", { value: option.value });
      });
    });
    countText.textContent = t("frame_sum.count");
    opButtons.sum.textContent = t("frame_sum.op.sum");
    opButtons.mean.textContent = t("frame_sum.op.mean");
    ops.setAttribute("aria-label", t("frame_sum.op.aria"));
    offButton.textContent = t("frame_sum.off");
    sync();
  }

  // The step lists hold the usual steps plus the sum and the step in use, so
  // a step can follow a sum of 3.
  function syncStepOptions() {
    const values = stepOptionsFor(state.frameSum, state.step);
    [frameStep, toolbarMoreStep].forEach((select) => {
      if (!select) return;
      const current = Array.from(select.options).map((option) => Number(option.value));
      if (current.join() !== values.join()) {
        select.replaceChildren(
          ...values.map((value) => {
            const option = document.createElement("option");
            option.value = String(value);
            option.textContent = t("toolbar.step.option", { value });
            return option;
          })
        );
      }
      select.value = String(state.step || 1);
    });
  }

  /** What the footer pill and the status line say of the frames shown. */
  function describe() {
    const count = activeFrameSum(state);
    if (count <= 1) return "";
    const window = frameSumWindow(state.frameIndex, count, state.frameCount);
    const key = normalizeFrameSumOp(state.frameSumOp) === "mean" ? "frame_sum.footer.mean" : "frame_sum.footer.sum";
    return t(key, { count: window.count });
  }

  /** The button: "1 frame", "Sum 5" or "Mean 5", and which way the popover is. */
  function buttonLabel() {
    const count = normalizeFrameSum(state.frameSum);
    const mean = normalizeFrameSumOp(state.frameSumOp) === "mean";
    const text = count <= 1 ? t("frame_sum.option.single") : t(mean ? "frame_sum.option.mean" : "frame_sum.option", { value: count });
    return `${text} ${isOpen() ? "▴" : "▾"}`;
  }

  function sync() {
    const available = frameSumAvailableFor(state);
    const inFileMode = !state.autoload || state.autoload.mode === "file";
    // Shown wherever there are frames to step through in a file.
    const show = inFileMode && Number(state.frameCount) > 1;
    frameSumWrap?.classList.toggle("is-hidden", !show);
    if (!show && isOpen()) frameSumWrap.classList.remove("is-open");
    const count = normalizeFrameSum(state.frameSum);
    const op = normalizeFrameSumOp(state.frameSumOp);
    if (frameSumButton) {
      frameSumButton.textContent = buttonLabel();
      frameSumButton.disabled = !available;
      frameSumButton.classList.toggle("is-on", activeFrameSum(state) > 1);
      frameSumButton.setAttribute("aria-expanded", isOpen() ? "true" : "false");
    }
    if (document.activeElement !== countInput) countInput.value = String(count);
    presetButtons.forEach((el, i) => el.classList.toggle("is-active", FRAME_SUM_PRESETS[i] === count));
    Object.entries(opButtons).forEach(([key, el]) => {
      el.classList.toggle("is-active", key === op);
      el.setAttribute("aria-pressed", key === op ? "true" : "false");
    });
    offButton.disabled = count <= 1;
    if (footerFrameSum) {
      const text = describe();
      footerFrameSum.hidden = !text;
      footerFrameSum.textContent = text;
    }
  }

  function reloadFrame() {
    if (!state.hasFrame || !frameSumAvailableFor(state)) return;
    core.framePlayback?.requestFrame(state.frameIndex);
  }

  function setFrameSum(value) {
    const previous = normalizeFrameSum(state.frameSum);
    const next = normalizeFrameSum(value);
    if (next === previous) {
      sync();
      return;
    }
    state.frameSum = next;
    const step = stepForFrameSum(state.step, previous, next);
    state.step = step;
    syncStepOptions();
    setFrameStep(step);
    sync();
    core.chromeToolbar?.updateToolbar();
    reloadFrame();
  }

  function setFrameSumOp(value) {
    const next = normalizeFrameSumOp(value);
    if (next === normalizeFrameSumOp(state.frameSumOp)) return;
    state.frameSumOp = next;
    sync();
    core.chromeToolbar?.updateToolbar();
    if (normalizeFrameSum(state.frameSum) > 1) reloadFrame();
  }

  /** `[` (-1) and `]` (+1): the next count down or up the ladder. */
  function stepFrameSum(direction) {
    if (!frameSumAvailableFor(state)) return false;
    setFrameSum(nextFrameSum(state.frameSum, direction));
    return true;
  }

  syncStepOptions();
  localizeOptions();

  return {
    describe,
    localizeOptions,
    setFrameSum,
    setFrameSumOp,
    stepFrameSum,
    setOpen,
    sync,
  };
}
