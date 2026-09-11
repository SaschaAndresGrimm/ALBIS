import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

import { createHelpTooltipController } from "../modules/help_tooltips.js";

/**
 * Two tooltips were drawn on the side-panel button at once: ours reading
 * "Toggle the side panel open or closed (M)." and the browser's own reading
 * "Open side menu (M)".
 *
 * `applyHelpMap` did strip `title` from every `[data-help]` element, but only
 * when it ran. `applyPanelState` re-sets `panelFab.title` on every call, and
 * i18n re-applies every `data-i18n-title` on each language change, so the
 * attribute came straight back. Taking it away for the lifetime of the hover
 * instead is robust against whatever re-adds it, and leaves the native tooltip
 * in place for anyone who has switched tool hints off — which the old
 * permanent strip did not, leaving those controls with no tooltip at all.
 *
 * One controller for the file: `initHelpTooltips` attaches its listeners to
 * `document`, and those outlive `document.body.innerHTML = ""`. Building one
 * per test leaves the earlier ones handling events and the assertions then
 * describe whichever controller answered first.
 */

let tooltip;
let state;

beforeAll(() => {
  state = { toolHintsEnabled: true };
  createHelpTooltipController({
    state,
    platformShortcutLabel: () => "M",
    roiCanvases: [],
  }).initHelpTooltips();
  // initHelpTooltips builds its own element; asserting against a hand-made
  // one passes whatever the controller does, because it writes to neither.
  tooltip = document.getElementById("help-tooltip");
  if (!tooltip) throw new Error("initHelpTooltips did not create #help-tooltip");
});

function hover(el) {
  el.dispatchEvent(new MouseEvent("mouseover", { bubbles: true, clientX: 10, clientY: 10 }));
  vi.advanceTimersByTime(1500);
}

function leave(el) {
  el.dispatchEvent(new MouseEvent("mouseout", { bubbles: true, relatedTarget: document.body }));
}

function addButton(attrs = {}) {
  const button = document.createElement("button");
  for (const [name, value] of Object.entries(attrs)) button.setAttribute(name, value);
  document.body.appendChild(button);
  return button;
}

describe("the browser's own tooltip does not double up with ours", () => {
  let fab;

  beforeEach(() => {
    vi.useFakeTimers();
    state.toolHintsEnabled = true;
    document.body.querySelectorAll("button").forEach((el) => el.remove());
    tooltip.textContent = "";
    tooltip.classList.remove("is-visible");
    fab = addButton({ id: "panel-fab", class: "panel-fab", title: "Open side menu (M)" });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("takes the title away while our bubble is up", () => {
    hover(fab);

    expect(fab.hasAttribute("title")).toBe(false);
  });

  it("takes the title away before the browser can paint its own tooltip", () => {
    // The regression this file was written for came back on the folder-browse
    // button: our bubble read "Browse and select a source folder." with the
    // browser's own "Browse folder" beside it. The attribute was being removed
    // at reveal time -- HELP_DELAY_MS after mouseover -- by which point the
    // browser had already painted from it, and a painted tooltip does not
    // retract. So the removal has to happen on mouseover, not on reveal.
    fab.dispatchEvent(new MouseEvent("mouseover", { bubbles: true, clientX: 10, clientY: 10 }));

    // Not one tick of the reveal delay has passed.
    expect(fab.hasAttribute("title")).toBe(false);
    expect(tooltip.classList.contains("is-visible")).toBe(false);

    // And our own bubble still arrives afterwards.
    vi.advanceTimersByTime(1500);
    expect(tooltip.classList.contains("is-visible")).toBe(true);
  });

  it("puts the title back when the pointer leaves", () => {
    hover(fab);
    leave(fab);

    expect(fab.getAttribute("title")).toBe("Open side menu (M)");
  });

  it("survives something re-setting the title after startup", () => {
    // applyPanelState does exactly this on every panel toggle, which is how
    // the attribute came back after the old startup strip had removed it.
    fab.title = "Close side menu (M)";
    hover(fab);
    expect(fab.hasAttribute("title")).toBe(false);

    leave(fab);
    expect(fab.getAttribute("title")).toBe("Close side menu (M)");
  });

  it("gives a control its title back when the pointer moves to another one", () => {
    // mouseover fires for a new target without a mouseout for the old one --
    // a nested control, or simply a fast sweep across a toolbar. Re-arming
    // without restoring left the abandoned control stripped for good, and
    // moving suppression to mouseover widened that from "after a full second
    // of dwell" to "any brush past".
    const other = addButton({ title: "Frame step" });

    hover(fab);
    expect(fab.hasAttribute("title")).toBe(false);

    other.dispatchEvent(new MouseEvent("mouseover", { bubbles: true, clientX: 40, clientY: 40 }));

    expect(fab.getAttribute("title")).toBe("Open side menu (M)");
    expect(other.hasAttribute("title")).toBe(false);
  });

  it("does not overwrite a title that came back during the hover", () => {
    // i18n re-applies every data-i18n-title on a language change. Writing the
    // stash back over that would restore the previous language's string.
    hover(fab);
    expect(fab.hasAttribute("title")).toBe(false);
    fab.setAttribute("title", "Seitenmenü öffnen (M)");

    leave(fab);

    expect(fab.getAttribute("title")).toBe("Seitenmenü öffnen (M)");
    expect(fab.dataset.helpNativeTitle).toBeUndefined();
  });

  it("leaves the native tooltip alone when tool hints are off", () => {
    state.toolHintsEnabled = false;
    hover(fab);

    expect(fab.getAttribute("title")).toBe("Open side menu (M)");
    expect(tooltip.classList.contains("is-visible")).toBe(false);
  });

  it("still finds its text from a title it has stashed away", () => {
    // The one place a control's title is also its hint: no data-help, no
    // aria-label, so getHelpText falls through to the attribute it just took.
    const plain = addButton({ title: "Browse output path" });

    hover(plain);

    expect(tooltip.textContent).toBe("Browse output path");
    expect(plain.hasAttribute("title")).toBe(false);
  });

  it("does not lose the title if the pointer leaves before the delay", () => {
    fab.dispatchEvent(new MouseEvent("mouseover", { bubbles: true, clientX: 5, clientY: 5 }));
    leave(fab);
    vi.advanceTimersByTime(1500);

    expect(fab.getAttribute("title")).toBe("Open side menu (M)");
  });
});
