import { beforeEach, describe, expect, it } from "vitest";

import { createPointerFocusRelease } from "../modules/pointer_focus_release.js";

/**
 * `Tab` toggles playback only when focus is not already on a control, which is
 * what stopped it swallowing keyboard navigation across ~265 controls. The
 * cost is that a clicked button keeps focus, so pressing the toolbar's own
 * play button once disabled the shortcut until focus moved elsewhere.
 */
describe("pointer focus release", () => {
  let button;

  beforeEach(() => {
    document.body.innerHTML = "";
    button = document.createElement("button");
    button.className = "tool-btn";
    button.textContent = "Play";
    document.body.appendChild(button);
    createPointerFocusRelease().attach();
  });

  function click({ detail }) {
    button.focus();
    button.dispatchEvent(new MouseEvent("click", { bubbles: true, detail }));
  }

  it("releases focus after a pointer click, so Tab is a shortcut again", () => {
    click({ detail: 1 });

    expect(document.activeElement).not.toBe(button);
  });

  it("keeps focus after a keyboard activation", () => {
    // Enter and Space fire click with detail 0. Moving focus there would be
    // the accessibility bug this exists to avoid, pointing the other way.
    click({ detail: 0 });

    expect(document.activeElement).toBe(button);
  });

  it("releases focus from a click on a button's inner markup", () => {
    const icon = document.createElement("span");
    button.appendChild(icon);
    button.focus();
    icon.dispatchEvent(new MouseEvent("click", { bubbles: true, detail: 1 }));

    expect(document.activeElement).not.toBe(button);
  });

  it("leaves non-toolbar controls alone", () => {
    const other = document.createElement("button");
    document.body.appendChild(other);
    other.focus();
    other.dispatchEvent(new MouseEvent("click", { bubbles: true, detail: 1 }));

    expect(document.activeElement).toBe(other);
  });

  it("does not undo a handler that moved focus deliberately", () => {
    const elsewhere = document.createElement("input");
    document.body.appendChild(elsewhere);
    button.addEventListener("click", () => elsewhere.focus());

    click({ detail: 1 });

    expect(document.activeElement).toBe(elsewhere);
  });
});
