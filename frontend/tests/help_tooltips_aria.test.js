import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import fs from "node:fs";
import path from "node:path";

function readLocale(language) {
  const filePath = path.join(process.cwd(), "frontend", "locales", `${language}.json`);
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

// initHelpTooltips binds to `document`, which outlives an individual test, so
// the controller is built once for the file: a second one would leave the first
// one's listeners running against the same DOM.
let shared = null;

async function buildController() {
  if (shared) {
    shared.state.toolHintsEnabled = true;
    return shared;
  }
  global.fetch = vi.fn(async () => ({ ok: true, json: async () => readLocale("en") }));
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  const { createHelpTooltipController } = await import("../modules/help_tooltips.js");
  const state = { toolHintsEnabled: true };
  const controller = createHelpTooltipController({
    state,
    platformShortcutLabel: () => "",
    roiCanvases: [],
  });
  controller.initHelpTooltips();
  shared = { controller, state };
  return shared;
}

// Focus, not hover: hovering is how a sighted user reaches a hint, and it is
// the focus path that has to carry it to everyone else.
function focus(element) {
  element.dispatchEvent(new FocusEvent("focusin", { bubbles: true }));
}

function blur(element) {
  element.dispatchEvent(new FocusEvent("focusout", { bubbles: true }));
}

describe("help tooltip announcement", () => {
  beforeEach(() => {
    // Only the fixture is reset: the tooltip element lives on `document.body`,
    // and wiping the body would take it away from the shared controller.
    let root = document.getElementById("fixture");
    if (!root) {
      root = document.createElement("div");
      root.id = "fixture";
      document.body.appendChild(root);
    }
    root.innerHTML = `
      <button id="btn-prev">◀</button>
      <div id="standing-note">Something else entirely</div>
      <button id="already-described" data-help="Own hint"
              aria-describedby="standing-note">described</button>
    `;
    localStorage.clear();
  });

  afterEach(() => {
    // The tooltip element and its listeners are deliberately left in place for
    // the next test; see buildController.
    shared?.controller.setToolHintsEnabled(true);
  });

  it("names the tooltip as the focused control's description while it is open", async () => {
    await buildController();
    const button = document.getElementById("btn-prev");
    const tooltip = document.querySelector(".help-tooltip");

    expect(tooltip.id).toBe("help-tooltip");
    expect(button.hasAttribute("aria-describedby")).toBe(false);

    focus(button);

    expect(tooltip.classList.contains("is-visible")).toBe(true);
    expect(tooltip.textContent).toBe(readLocale("en")["hint.frame.previous"]);
    expect(button.getAttribute("aria-describedby")).toBe("help-tooltip");
  });

  it("drops the association when the tooltip closes, so it never points at hidden text", async () => {
    await buildController();
    const button = document.getElementById("btn-prev");

    focus(button);
    blur(button);

    expect(document.querySelector(".help-tooltip").classList.contains("is-visible")).toBe(false);
    expect(button.hasAttribute("aria-describedby")).toBe(false);
  });

  it("keeps a description the control already had", async () => {
    await buildController();
    const button = document.getElementById("already-described");

    focus(button);
    expect(button.getAttribute("aria-describedby")).toBe("standing-note help-tooltip");

    blur(button);
    expect(button.getAttribute("aria-describedby")).toBe("standing-note");
  });

  it("drops the association when hints are switched off mid-hover", async () => {
    const { controller } = await buildController();
    const button = document.getElementById("btn-prev");

    focus(button);
    expect(button.getAttribute("aria-describedby")).toBe("help-tooltip");

    controller.setToolHintsEnabled(false);
    expect(button.hasAttribute("aria-describedby")).toBe(false);
  });
});
