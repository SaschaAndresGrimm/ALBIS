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
});
