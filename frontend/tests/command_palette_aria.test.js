import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { createCommandPaletteController } from "../modules/command_palette.js";

// Minimal controller wiring: render() pulls from getCommands() and filters by
// the input value, so a couple of fake commands is enough to exercise the
// listbox/option ARIA contract.
function setup() {
  document.body.innerHTML = `
    <div id="command-modal" class="modal"></div>
    <input id="command-input" role="combobox" aria-expanded="false" aria-controls="command-list" />
    <div id="command-list" role="listbox"></div>
  `;
  if (!window.Element.prototype.scrollIntoView) {
    window.Element.prototype.scrollIntoView = () => {};
  }
  const commandInput = document.getElementById("command-input");
  const commandList = document.getElementById("command-list");
  const controller = createCommandPaletteController({
    elements: {
      commandModal: document.getElementById("command-modal"),
      commandInput,
      commandList,
    },
    callbacks: {
      getCommands: () => [
        { id: "alpha", label: "Alpha", run: () => {} },
        { id: "beta", label: "Beta", shortcut: "B", run: () => {} },
        { id: "gamma", label: "Gamma", run: () => {} },
      ],
      closeMenu: () => {},
      closeToolbarPlaybackPopover: () => {},
      closeToolbarMorePopover: () => {},
      focusModal: () => {},
      openModal: () => {},
      closeModal: () => true,
    },
  });
  return { controller, commandInput, commandList };
}

afterEach(() => {
  document.body.innerHTML = "";
  vi.restoreAllMocks();
});

describe("command palette ARIA", () => {
  let ctx;
  beforeEach(() => {
    ctx = setup();
  });

  it("renders each command as a listbox option with a stable id", () => {
    ctx.controller.render();
    const options = ctx.commandList.querySelectorAll('[role="option"]');
    expect(options.length).toBe(3);
    options.forEach((opt, idx) => {
      expect(opt.id).toBe(`command-option-${idx}`);
    });
  });

  it("marks exactly one option aria-selected and points the combobox at it", () => {
    ctx.controller.render();
    const selected = ctx.commandList.querySelectorAll('[aria-selected="true"]');
    expect(selected.length).toBe(1);
    expect(selected[0].id).toBe("command-option-0");
    expect(ctx.commandInput.getAttribute("aria-activedescendant")).toBe("command-option-0");
  });

  it("clears aria-activedescendant when no command matches", () => {
    ctx.commandInput.value = "no-such-command-xyz";
    ctx.controller.render();
    expect(ctx.commandList.querySelectorAll('[role="option"]').length).toBe(0);
    expect(ctx.commandInput.hasAttribute("aria-activedescendant")).toBe(false);
  });
});
