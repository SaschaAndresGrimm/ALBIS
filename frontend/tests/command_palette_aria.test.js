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
        { id: "alpha", label: "File: Alpha", run: () => {} },
        { id: "beta", label: "View: Beta", shortcut: "B", run: () => {} },
        { id: "gamma", label: "File: Gamma", run: () => {} },
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
  localStorage.clear();
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

  it("groups an empty query by category, each group labelled by its heading", () => {
    ctx.controller.render();
    const groups = ctx.commandList.querySelectorAll('[role="group"]');
    expect(groups.length).toBe(2);
    groups.forEach((group) => {
      const heading = document.getElementById(group.getAttribute("aria-labelledby"));
      expect(heading.getAttribute("role")).toBe("presentation");
      expect(heading.closest('[role="group"]')).toBe(group);
    });
    expect(Array.from(groups).map((g) => g.querySelector(".command-group-heading").textContent)).toEqual(["File", "View"]);
    const texts = Array.from(ctx.commandList.querySelectorAll(".command-label")).map((el) => el.textContent);
    expect(texts).toEqual(["Alpha", "Gamma", "Beta"]);
  });

  it("moves through options only, skipping the headings, and wraps", () => {
    ctx.commandModal = document.getElementById("command-modal");
    ctx.commandModal.classList.add("is-open");
    ctx.controller.render();
    const press = (key) => ctx.controller.handleKeydown(new KeyboardEvent("keydown", { key }));
    press("ArrowDown");
    expect(ctx.commandInput.getAttribute("aria-activedescendant")).toBe("command-option-1");
    press("ArrowDown");
    expect(ctx.commandList.querySelector(".is-active .command-label").textContent).toBe("Beta");
    press("ArrowDown");
    expect(ctx.commandInput.getAttribute("aria-activedescendant")).toBe("command-option-0");
    press("ArrowUp");
    expect(ctx.commandInput.getAttribute("aria-activedescendant")).toBe("command-option-2");
  });

  it("remembers a command run from the palette and lists it under Recent first", () => {
    const modal = document.getElementById("command-modal");
    modal.classList.add("is-open");
    ctx.controller.render();
    ctx.controller.setIndex(2);
    ctx.controller.handleKeydown(new KeyboardEvent("keydown", { key: "Enter" }));
    expect(JSON.parse(localStorage.getItem("albis.commandPalette.recent"))).toEqual(["beta"]);

    ctx.controller.render();
    const first = ctx.commandList.querySelector('[role="group"]');
    expect(first.dataset.group).toBe("recent");
    expect(first.querySelector(".command-label").textContent).toBe("View: Beta");
    const texts = Array.from(ctx.commandList.querySelectorAll(".command-label")).map((el) => el.textContent);
    expect(texts).toEqual(["View: Beta", "Alpha", "Gamma"]);
  });

  it("lists a query flat and ranked, without headings", () => {
    ctx.commandInput.value = "gam";
    ctx.controller.render();
    expect(ctx.commandList.querySelectorAll('[role="group"]').length).toBe(0);
    const texts = Array.from(ctx.commandList.querySelectorAll(".command-label")).map((el) => el.textContent);
    expect(texts).toEqual(["File: Gamma"]);
  });
});
