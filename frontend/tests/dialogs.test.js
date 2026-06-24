import { beforeEach, describe, expect, it, vi } from "vitest";

async function loadDialogs() {
  vi.resetModules();
  document.body.innerHTML = "";
  return import("../modules/dialogs.js");
}

function getOverlay() {
  return document.querySelector(".dialog-modal");
}

function pressKey(key, opts = {}) {
  const target = document.activeElement || getOverlay();
  target.dispatchEvent(new window.KeyboardEvent("keydown", { key, bubbles: true, cancelable: true, ...opts }));
}

describe("prompt/confirm dialogs", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
  });

  it("prompt resolves the input value when confirmed", async () => {
    const { showPromptDialog } = await loadDialogs();
    const pending = showPromptDialog({ title: "Save As", defaultValue: "frame.png" });
    const overlay = getOverlay();
    expect(overlay).not.toBeNull();
    const input = overlay.querySelector(".dialog-input");
    expect(input.value).toBe("frame.png");
    input.value = "renamed.png";
    overlay.querySelector(".dialog-confirm").click();
    await expect(pending).resolves.toBe("renamed.png");
    expect(getOverlay()).toBeNull();
  });

  it("prompt resolves null when cancelled", async () => {
    const { showPromptDialog } = await loadDialogs();
    const pending = showPromptDialog({ title: "Save As" });
    getOverlay().querySelector(".dialog-cancel").click();
    await expect(pending).resolves.toBeNull();
  });

  it("prompt resolves null when the backdrop is clicked", async () => {
    const { showPromptDialog } = await loadDialogs();
    const pending = showPromptDialog({ title: "Save As" });
    getOverlay().querySelector(".modal-backdrop").click();
    await expect(pending).resolves.toBeNull();
  });

  it("prompt resolves null on Escape and the value on Enter", async () => {
    const { showPromptDialog } = await loadDialogs();
    const escPending = showPromptDialog({ title: "Save As", defaultValue: "x" });
    getOverlay().querySelector(".dialog-input").focus();
    pressKey("Escape");
    await expect(escPending).resolves.toBeNull();

    const enterPending = showPromptDialog({ title: "Save As", defaultValue: "keep.png" });
    getOverlay().querySelector(".dialog-input").focus();
    pressKey("Enter");
    await expect(enterPending).resolves.toBe("keep.png");
  });

  it("confirm resolves true/false", async () => {
    const { showConfirmDialog } = await loadDialogs();
    const yes = showConfirmDialog({ title: "Sure?" });
    getOverlay().querySelector(".dialog-confirm").click();
    await expect(yes).resolves.toBe(true);

    const no = showConfirmDialog({ title: "Sure?" });
    getOverlay().querySelector(".dialog-cancel").click();
    await expect(no).resolves.toBe(false);
  });

  it("confirm dialog uses the alertdialog role and supports danger styling", async () => {
    const { showConfirmDialog } = await loadDialogs();
    showConfirmDialog({ title: "Danger", danger: true });
    const overlay = getOverlay();
    expect(overlay.querySelector(".dialog-card").getAttribute("role")).toBe("alertdialog");
    expect(overlay.querySelector(".dialog-confirm").classList.contains("btn-danger")).toBe(true);
  });

  it("opening a second dialog cancels the first", async () => {
    const { showConfirmDialog } = await loadDialogs();
    const first = showConfirmDialog({ title: "First" });
    showConfirmDialog({ title: "Second" });
    await expect(first).resolves.toBe(false);
    expect(document.querySelectorAll(".dialog-modal").length).toBe(1);
  });

  it("stops key events from propagating to app-level shortcuts", async () => {
    const { showPromptDialog } = await loadDialogs();
    const appHandler = vi.fn();
    document.addEventListener("keydown", appHandler);
    showPromptDialog({ title: "Save As" });
    getOverlay().querySelector(".dialog-input").focus();
    pressKey("ArrowRight");
    document.removeEventListener("keydown", appHandler);
    expect(appHandler).not.toHaveBeenCalled();
  });
});
