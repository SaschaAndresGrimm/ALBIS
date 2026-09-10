import { afterEach, describe, expect, it, vi } from "vitest";

import { createShortcutHandlers } from "../modules/shortcut_handlers.js";

function makeHandlers(callbackOverrides = {}) {
  const handleMenuAction = vi.fn();
  const noop = vi.fn();
  return {
    handleMenuAction,
    ...createShortcutHandlers({
      state: {
        playing: false,
        thresholdCount: 1,
        autoload: { mode: "file" },
        step: 1,
        frameIndex: 0,
      },
      elements: {
        thresholdSelect: null,
        toolbarThresholdSelect: null,
        toolbarMoreThreshold: null,
      },
      callbacks: {
        handleMenuAction,
        isCommandPaletteOpen: () => false,
        closeCommandPalette: noop,
        openCommandPalette: noop,
        openFileModal: noop,
        closeCurrentFile: noop,
        stopPlayback: noop,
        startPlayback: noop,
        setThresholdIndex: noop,
        getThresholdIndexAtOffset: () => 0,
        toggleFullscreen: noop,
        togglePanel: noop,
        requestFrame: noop,
        ...callbackOverrides,
      },
    }),
  };
}

describe("shortcut_handlers", () => {
  it("maps Mod+Shift+X to dataset export without stealing normal cut", () => {
    const { handleShortcut, handleMenuAction } = makeHandlers();
    const exportEvent = new KeyboardEvent("keydown", {
      key: "x",
      ctrlKey: true,
      shiftKey: true,
      bubbles: true,
      cancelable: true,
    });

    handleShortcut(exportEvent);

    expect(exportEvent.defaultPrevented).toBe(true);
    expect(handleMenuAction).toHaveBeenCalledWith("export-data");

    const cutEvent = new KeyboardEvent("keydown", {
      key: "x",
      ctrlKey: true,
      bubbles: true,
      cancelable: true,
    });

    handleShortcut(cutEvent);

    expect(cutEvent.defaultPrevented).toBe(false);
    expect(handleMenuAction).toHaveBeenCalledTimes(1);
  });
});

describe("Tab must not swallow keyboard focus traversal", () => {
  function tabEvent(target, { shiftKey = false } = {}) {
    const event = new KeyboardEvent("keydown", { key: "Tab", shiftKey, bubbles: true });
    Object.defineProperty(event, "target", { value: target });
    vi.spyOn(event, "preventDefault");
    return event;
  }

  afterEach(() => {
    document.body.innerHTML = "";
  });

  it("toggles playback when focus is not on a control", () => {
    // The canvas carries no tabindex, so with nothing focused the event
    // target is the body -- which is where Tab-as-play/pause is meant to act.
    const startPlayback = vi.fn();
    const { handleNavShortcut } = makeHandlers({ startPlayback });
    const event = tabEvent(document.body);

    expect(handleNavShortcut(event)).toBe(true);
    expect(startPlayback).toHaveBeenCalledTimes(1);
    expect(event.preventDefault).toHaveBeenCalled();
  });

  it.each([
    ["button", () => Object.assign(document.createElement("button"), { textContent: "Open" })],
    ["input", () => Object.assign(document.createElement("input"), { type: "text" })],
    ["select", () => document.createElement("select")],
    ["link", () => Object.assign(document.createElement("a"), { href: "#x" })],
    ["tabindex=0 div", () => {
      const el = document.createElement("div");
      el.setAttribute("tabindex", "0");
      return el;
    }],
    // A native tab stop with no tabindex and no disabled state, so it matched
    // none of the other rules. index.html has two, in visible panel sections.
    ["summary", () => {
      const details = document.createElement("details");
      const summary = document.createElement("summary");
      summary.textContent = "Advanced view controls";
      details.appendChild(summary);
      document.body.appendChild(details);
      return summary;
    }],
  ])("leaves Tab alone when focus is on a %s", (_label, make) => {
    // Before this the branch sat above the isFormElement guard and called
    // preventDefault() unconditionally, so focus could not move through any
    // of the window's ~265 controls -- WCAG 2.1.1 at level A.
    const startPlayback = vi.fn();
    const { handleNavShortcut } = makeHandlers({ startPlayback });
    const target = make();
    document.body.appendChild(target);
    const event = tabEvent(target);

    expect(handleNavShortcut(event)).toBe(false);
    expect(startPlayback).not.toHaveBeenCalled();
    expect(event.preventDefault).not.toHaveBeenCalled();
  });

  it("leaves a control's inner markup alone too", () => {
    // Icon buttons wrap a span, so the event target is often the child.
    const { handleNavShortcut } = makeHandlers();
    const button = document.createElement("button");
    const icon = document.createElement("span");
    button.appendChild(icon);
    document.body.appendChild(button);

    expect(handleNavShortcut(tabEvent(icon))).toBe(false);
  });

  it("never treats Shift+Tab as a playback gesture", () => {
    const startPlayback = vi.fn();
    const { handleNavShortcut } = makeHandlers({ startPlayback });
    const event = tabEvent(document.body, { shiftKey: true });

    expect(handleNavShortcut(event)).toBe(false);
    expect(startPlayback).not.toHaveBeenCalled();
    expect(event.preventDefault).not.toHaveBeenCalled();
  });

  it("still reaches a disabled control's neighbours", () => {
    // A disabled button is not a tab stop, so Tab there is not traversal.
    const startPlayback = vi.fn();
    const { handleNavShortcut } = makeHandlers({ startPlayback });
    const button = document.createElement("button");
    button.disabled = true;
    document.body.appendChild(button);

    expect(handleNavShortcut(tabEvent(button))).toBe(true);
    expect(startPlayback).toHaveBeenCalledTimes(1);
  });
});
