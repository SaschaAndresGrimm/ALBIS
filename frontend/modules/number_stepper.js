/**
 * Up and down buttons for a number field, drawn by ALBIS.
 *
 * A browser's own spinner cannot be styled in Firefox, and looks different in
 * each engine; these replace it with the same pair of chevrons everywhere.
 * The field is wrapped in a `.num-stepper` with the buttons beside it, and the
 * native spinner is hidden by the stylesheet.
 *
 * The buttons step as the native ones do: by the field's step within its min
 * and max, firing `input` and then `change` per step, so the field's own
 * handlers see a click exactly as before. Holding one repeats. They stay out of
 * the tab order and the accessibility tree: the field is still a spinbutton,
 * and the arrow keys step it.
 */

const REPEAT_DELAY_MS = 400;
const REPEAT_INTERVAL_MS = 60;

function decimals(number) {
  const text = String(number);
  const dot = text.indexOf(".");
  return dot < 0 ? 0 : text.length - dot - 1;
}

/**
 * Step `input` by one step up (+1) or down (-1); true if the value changed.
 *
 * `stepUp()` and `stepDown()` throw for `step="any"`, where the native spinner
 * steps by 1: so does this.
 */
export function stepNumberInput(input, direction) {
  if (!input || input.disabled || input.readOnly) return false;
  const before = input.value;
  try {
    if (direction > 0) input.stepUp();
    else input.stepDown();
  } catch {
    const current = Number(input.value);
    let next = (Number.isFinite(current) && input.value !== "" ? current : 0) + direction;
    next = Number(next.toFixed(decimals(current)));
    if (input.min !== "" && Number.isFinite(Number(input.min))) next = Math.max(Number(input.min), next);
    if (input.max !== "" && Number.isFinite(Number(input.max))) next = Math.min(Number(input.max), next);
    input.value = String(next);
  }
  if (input.value === before) return false;
  input.dispatchEvent(new window.Event("input", { bubbles: true }));
  input.dispatchEvent(new window.Event("change", { bubbles: true }));
  return true;
}

function makeButton(direction) {
  const button = document.createElement("button");
  button.type = "button";
  button.tabIndex = -1;
  button.className = `num-stepper-btn ${direction > 0 ? "up" : "down"}`;
  button.setAttribute("aria-hidden", "true");
  return button;
}

/** Give `input` the ALBIS stepper; a field that has one already is left as is. */
export function attachNumberStepper(input, { variant = "" } = {}) {
  if (!input || input.type !== "number") return null;
  const existing = input.parentElement;
  if (existing?.classList.contains("num-stepper")) return existing;

  const wrapper = document.createElement("span");
  wrapper.className = variant ? `num-stepper ${variant}` : "num-stepper";
  const buttons = document.createElement("span");
  buttons.className = "num-stepper-buttons";
  const up = makeButton(1);
  const down = makeButton(-1);
  buttons.append(up, down);

  input.classList.add("num-stepper-input");
  input.replaceWith(wrapper);
  wrapper.append(input, buttons);

  let delayTimer = 0;
  let repeatTimer = 0;
  function stop() {
    clearTimeout(delayTimer);
    clearInterval(repeatTimer);
    delayTimer = 0;
    repeatTimer = 0;
  }

  for (const [button, direction] of [
    [up, 1],
    [down, -1],
  ]) {
    button.addEventListener("pointerdown", (event) => {
      if (event.button !== 0 || input.disabled || input.readOnly) return;
      // Keep the press from taking focus off the field, as the native
      // spinner does: the field is focused instead, so the arrow keys go on
      // from where the mouse left off.
      event.preventDefault();
      if (document.activeElement !== input) input.focus({ preventScroll: true });
      stop();
      stepNumberInput(input, direction);
      delayTimer = setTimeout(() => {
        repeatTimer = setInterval(() => {
          if (!input.isConnected || !stepNumberInput(input, direction)) stop();
        }, REPEAT_INTERVAL_MS);
      }, REPEAT_DELAY_MS);
    });
    button.addEventListener("pointerup", stop);
    button.addEventListener("pointerleave", stop);
    button.addEventListener("pointercancel", stop);
  }
  return wrapper;
}

// Fields that hide their spinner on purpose, for want of room: the pixel-mask
// rules and the geometry axes, each behind a label of its own.
const WITHOUT_STEPPER = ".pixel-mask-find-rules, .geometry-axis";

/** Every number field under `root`, but those above; the toolbar's compact. */
export function attachAllNumberSteppers(root = document) {
  return [...root.querySelectorAll('input[type="number"]')]
    .filter((input) => !input.closest(WITHOUT_STEPPER))
    .map((input) =>
      attachNumberStepper(input, { variant: input.closest(".toolbar") ? "num-stepper-compact" : "" }),
    );
}
