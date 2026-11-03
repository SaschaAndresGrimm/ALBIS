/**
 * Over plain HTTP on a LAN, which is a documented way to run ALBIS, there is
 * no `navigator.clipboard`; copying has to fall back to a selection.
 */

import { afterEach, describe, expect, it, vi } from "vitest";

import { copyText } from "../modules/clipboard_copy.js";

describe("copyText", () => {
  afterEach(() => {
    delete navigator.clipboard;
    delete document.execCommand;
  });

  it("uses the Clipboard API when there is one", async () => {
    const writeText = vi.fn().mockResolvedValue();
    Object.defineProperty(navigator, "clipboard", { value: { writeText }, configurable: true });
    expect(await copyText("/entry/data")).toBe(true);
    expect(writeText).toHaveBeenCalledWith("/entry/data");
  });

  it("falls back to a selection without one, and cleans up after itself", async () => {
    document.execCommand = vi.fn(() => true);
    expect(await copyText("/entry/data")).toBe(true);
    expect(document.execCommand).toHaveBeenCalledWith("copy");
    expect(document.querySelector("textarea")).toBeNull();
  });

  it("falls back when the Clipboard API refuses, and reports a double refusal", async () => {
    Object.defineProperty(navigator, "clipboard", {
      value: { writeText: vi.fn().mockRejectedValue(new Error("denied")) },
      configurable: true,
    });
    document.execCommand = vi.fn(() => false);
    expect(await copyText("x")).toBe(false);
    expect(document.execCommand).toHaveBeenCalled();
  });
});
