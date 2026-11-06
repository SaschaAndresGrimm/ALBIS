import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

async function loadToast() {
  vi.resetModules();
  document.body.innerHTML = "";
  return import("../modules/toast.js");
}

afterEach(() => {
  vi.useRealTimers();
});

describe("toast notifications", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
  });

  it("creates the region lazily and renders the message", async () => {
    const { notifyInfo } = await loadToast();
    notifyInfo("Hello world");
    const region = document.getElementById("toast-region");
    expect(region).not.toBeNull();
    const toasts = region.querySelectorAll(".toast");
    expect(toasts.length).toBe(1);
    expect(toasts[0].querySelector(".toast-message").textContent).toBe("Hello world");
  });

  it("uses role=alert for errors/warnings and role=status for info/success", async () => {
    const { notifyError, notifySuccess, notifyWarning, notifyInfo } = await loadToast();
    notifyError("boom");
    notifyWarning("careful");
    notifySuccess("nice");
    notifyInfo("fyi");
    const region = document.getElementById("toast-region");
    expect(region.querySelector(".toast-error").getAttribute("role")).toBe("alert");
    expect(region.querySelector(".toast-warning").getAttribute("role")).toBe("alert");
    expect(region.querySelector(".toast-success").getAttribute("role")).toBe("status");
    expect(region.querySelector(".toast-info").getAttribute("role")).toBe("status");
  });

  it("dedupes identical messages of the same tone", async () => {
    const { notifyError } = await loadToast();
    notifyError("Failed to load frame");
    notifyError("Failed to load frame");
    const region = document.getElementById("toast-region");
    expect(region.querySelectorAll(".toast").length).toBe(1);
  });

  it("caps the number of visible toasts", async () => {
    vi.useFakeTimers();
    const { notifyInfo } = await loadToast();
    for (let i = 0; i < 6; i += 1) {
      notifyInfo(`message ${i}`);
    }
    // Over-cap toasts start leaving immediately; advance past the removal timer.
    vi.advanceTimersByTime(500);
    const region = document.getElementById("toast-region");
    expect(region.querySelectorAll(".toast").length).toBe(4);
    // Oldest is dropped, newest is kept.
    expect(region.textContent).toContain("message 5");
    expect(region.textContent).not.toContain("message 0");
  });

  it("removes a toast when its close button is clicked", async () => {
    vi.useFakeTimers();
    const { notifyError } = await loadToast();
    notifyError("dismiss me");
    const region = document.getElementById("toast-region");
    region.querySelector(".toast-close").click();
    vi.advanceTimersByTime(500); // run the removal fallback timer
    expect(region.querySelectorAll(".toast").length).toBe(0);
  });

  it("auto-dismisses info toasts after their duration", async () => {
    vi.useFakeTimers();
    const { notifyInfo } = await loadToast();
    notifyInfo("temporary");
    const region = document.getElementById("toast-region");
    expect(region.querySelectorAll(".toast").length).toBe(1);
    vi.advanceTimersByTime(5000 + 500);
    expect(region.querySelectorAll(".toast").length).toBe(0);
  });

  it("dismissAllToasts clears everything", async () => {
    vi.useFakeTimers();
    const { notifyError, notifyInfo, dismissAllToasts } = await loadToast();
    notifyError("a");
    notifyInfo("b");
    dismissAllToasts();
    vi.advanceTimersByTime(500);
    expect(document.getElementById("toast-region").querySelectorAll(".toast").length).toBe(0);
  });

  it("shows the cause under the message", async () => {
    const { notifyError } = await loadToast();
    notifyError("Failed to load frame", { detail: "Frame 3: Cannot reach the server." });
    const detail = document.querySelector(".toast .toast-detail");
    expect(detail.textContent).toBe("Frame 3: Cannot reach the server.");
    expect(detail.title).toBe("Frame 3: Cannot reach the server.");
  });

  it("counts a repeated message on one toast and keeps its newest detail", async () => {
    const { notifyError } = await loadToast();
    notifyError("Failed to load frame", { detail: "Frame 3" });
    expect(document.querySelector(".toast-count")).toBeNull();
    notifyError("Failed to load frame", { detail: "Frame 4" });
    notifyError("Failed to load frame", { detail: "Frame 5" });
    const toasts = document.querySelectorAll(".toast");
    expect(toasts.length).toBe(1);
    expect(toasts[0].querySelector(".toast-count").textContent).toBe("×3");
    expect(toasts[0].querySelector(".toast-count").getAttribute("aria-label")).toContain("toast.repeated");
    expect(toasts[0].querySelector(".toast-detail").textContent).toBe("Frame 5");
  });

  it("runs its action with the newest handler and closes", async () => {
    vi.useFakeTimers();
    const { notifyError } = await loadToast();
    const first = vi.fn();
    const second = vi.fn();
    notifyError("Failed to load frame", { action: { label: "Retry", onClick: first } });
    notifyError("Failed to load frame", { action: { label: "Retry", onClick: second } });
    const button = document.querySelector(".toast .toast-action");
    expect(button.textContent).toBe("Retry");
    button.click();
    expect(first).not.toHaveBeenCalled();
    expect(second).toHaveBeenCalledTimes(1);
    vi.advanceTimersByTime(500);
    expect(document.querySelectorAll(".toast").length).toBe(0);
  });

  it("holds back what the gate refuses, and closes toasts of one tone", async () => {
    vi.useFakeTimers();
    const { notifyError, notifyInfo, setToastGate, dismissToasts } = await loadToast();
    notifyError("before");
    notifyInfo("info");
    setToastGate((type) => type !== "error");
    notifyError("held back");
    expect(document.body.textContent).not.toContain("held back");
    dismissToasts("error");
    vi.advanceTimersByTime(500);
    expect(document.querySelectorAll(".toast-error").length).toBe(0);
    expect(document.querySelectorAll(".toast-info").length).toBe(1);
    setToastGate(null);
    notifyError("after");
    expect(document.body.textContent).toContain("after");
  });
});

describe("toast placement", () => {
  it("stands clear of the anchor's top edge", async () => {
    const { toastBottomOffset } = await loadToast();
    // Footer from 1330 to 1380 in a 1397px window: 67px to its top, plus the gap.
    expect(toastBottomOffset(1397, { top: 1330, height: 50 })).toBe(75);
  });

  it("keeps the edge gap when there is no anchor, or it is hidden or above the bottom", async () => {
    const { toastBottomOffset } = await loadToast();
    expect(toastBottomOffset(900, null)).toBe(16);
    expect(toastBottomOffset(900, { top: 0, height: 0 })).toBe(16);
    expect(toastBottomOffset(900, { top: 950, height: 40 })).toBe(16);
  });

  it("places the region above the anchor when a toast is shown", async () => {
    const { anchorToastsAbove, notifyInfo } = await loadToast();
    const footer = document.createElement("div");
    document.body.appendChild(footer);
    footer.getBoundingClientRect = () => ({ top: window.innerHeight - 60, height: 48 });
    anchorToastsAbove(footer);
    notifyInfo("hello");
    expect(document.getElementById("toast-region").style.getPropertyValue("--toast-bottom")).toBe("68px");
  });
});
