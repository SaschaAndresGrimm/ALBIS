import { describe, expect, it } from "vitest";

import { isIconOnlyText, keyshortcutLabel, pickTipText, placeTip } from "../modules/hover_tip_utils.js";

describe("isIconOnlyText", () => {
  it("treats glyphs and nothing as an icon", () => {
    expect(isIconOnlyText("◀")).toBe(true);
    expect(isIconOnlyText(" ⛶ ")).toBe(true);
    expect(isIconOnlyText("")).toBe(true);
    expect(isIconOnlyText(undefined)).toBe(true);
  });

  it("treats words and numbers in any script as text", () => {
    expect(isIconOnlyText("Full screen")).toBe(false);
    expect(isIconOnlyText("HDF5")).toBe(false);
    expect(isIconOnlyText("打开")).toBe(false);
    expect(isIconOnlyText("123")).toBe(false);
  });
});

describe("pickTipText", () => {
  it("says why a control is unavailable before anything else", () => {
    expect(pickTipText({ reason: "Load a file first", title: "Export CSV", isButton: true })).toBe("Load a file first");
  });

  it("uses the title", () => {
    expect(pickTipText({ title: "Browse  folder\n" })).toBe("Browse folder");
  });

  it("names an icon-only button by its aria-label", () => {
    expect(pickTipText({ ariaLabel: "Previous frame", isButton: true, visibleText: "◀" })).toBe("Previous frame");
  });

  it("says nothing over a button that shows its own words", () => {
    expect(pickTipText({ ariaLabel: "Toggle full screen", isButton: true, visibleText: "Full screen" })).toBe("");
  });

  it("says nothing over a field that only has an aria-label", () => {
    // The Combine Frames Operation dropdown showed "Operation" over "Sum".
    expect(pickTipText({ ariaLabel: "Operation", isButton: false, visibleText: "Sum" })).toBe("");
  });
});

describe("keyshortcutLabel", () => {
  it("draws arrow keys as arrows", () => {
    expect(keyshortcutLabel("ArrowLeft")).toBe("←");
    expect(keyshortcutLabel("Shift+ArrowUp")).toBe("Shift+↑");
  });

  it("shows letters upper case and only the first of several shortcuts", () => {
    expect(keyshortcutLabel("m")).toBe("M");
    expect(keyshortcutLabel("F F11")).toBe("F");
    expect(keyshortcutLabel("Tab")).toBe("Tab");
  });

  it("is empty without a shortcut", () => {
    expect(keyshortcutLabel(null)).toBe("");
    expect(keyshortcutLabel("  ")).toBe("");
  });
});

describe("placeTip", () => {
  const viewport = { width: 800, height: 600 };
  const size = { width: 100, height: 24 };

  it("centres the tip under its control", () => {
    const anchor = { left: 300, top: 100, width: 40, bottom: 130 };
    expect(placeTip(anchor, size, viewport)).toEqual({ left: 270, top: 136, side: "below" });
  });

  it("goes above when there is no room below", () => {
    const anchor = { left: 300, top: 560, width: 40, bottom: 590 };
    expect(placeTip(anchor, size, viewport)).toEqual({ left: 270, top: 530, side: "above" });
  });

  it("stays inside the viewport at the edges", () => {
    expect(placeTip({ left: 0, top: 10, width: 20, bottom: 30 }, size, viewport).left).toBe(8);
    expect(placeTip({ left: 790, top: 10, width: 10, bottom: 30 }, size, viewport).left).toBe(692);
  });

  it("keeps a tip taller than either side on screen", () => {
    const placed = placeTip({ left: 300, top: 20, width: 40, bottom: 580 }, { width: 100, height: 300 }, viewport);
    expect(placed.top).toBe(292);
  });
});
