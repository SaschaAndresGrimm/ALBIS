import { describe, expect, it, vi } from "vitest";

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
