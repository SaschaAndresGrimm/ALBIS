import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { createHandoffController } from "../modules/handoff_controller.js";

function job(body) {
  return { ok: true, status: 200, json: async () => body };
}

const pollWarnings = () => console.warn.mock.calls.filter(([message]) => message === "Handoff polling error").length;

function build() {
  const callbacks = {
    isHdfFile: (path) => path.endsWith(".h5"),
    selectDatasetIfPresent: vi.fn(async () => true),
    setStatus: vi.fn(),
  };
  // The file opener it reaches through the registry (decision 0006).
  const core = { fileOpen: { openPathInViewer: vi.fn(async () => {}) } };
  return { controller: createHandoffController({ apiBase: "/api", callbacks, core }), callbacks, core };
}

describe("handoff_controller", () => {
  beforeEach(() => {
    localStorage.clear();
    vi.spyOn(console, "warn").mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete globalThis.fetch;
  });

  it("opens the newest job, with its dataset, and remembers it was handled", async () => {
    globalThis.fetch = vi.fn(async () => job({ id: 4, open_path: "/data/run_master.h5", dataset: "/entry/data/data", run_id: "r7" }));
    const { controller, callbacks, core } = build();
    await controller.poll();
    expect(globalThis.fetch.mock.calls[0][0]).toBe("/api/handoff/v1/jobs/latest?after_id=0");
    expect(core.fileOpen.openPathInViewer).toHaveBeenCalledWith("/data/run_master.h5", { refreshFileList: true });
    expect(callbacks.selectDatasetIfPresent).toHaveBeenCalledWith("/entry/data/data");
    expect(localStorage.getItem("albis.handoff.lastId")).toBe("4");

    // A window started later asks only for newer jobs.
    globalThis.fetch = vi.fn(async () => ({ ok: true, status: 204 }));
    await build().controller.poll();
    expect(globalThis.fetch.mock.calls[0][0]).toBe("/api/handoff/v1/jobs/latest?after_id=4");
  });

  it("does nothing when there is no new job, or one already handled", async () => {
    localStorage.setItem("albis.handoff.lastId", "9");
    globalThis.fetch = vi.fn(async () => job({ id: 9, open_path: "/data/old.cbf" }));
    const { controller, callbacks, core } = build();
    await controller.poll();
    globalThis.fetch = vi.fn(async () => ({ ok: true, status: 204 }));
    await controller.poll();
    expect(core.fileOpen.openPathInViewer).not.toHaveBeenCalled();
  });

  it("stays quiet when the queue answers with an error status, and warns only when it cannot be reached", async () => {
    // Polled every second: a warning per failed poll filled the log file.
    globalThis.fetch = vi.fn(async () => ({ ok: false, status: 500, statusText: "Internal Server Error", text: async () => "", json: async () => ({}) }));
    const { controller, core } = build();
    await controller.poll();
    await controller.poll();
    expect(pollWarnings()).toBe(0);
    expect(core.fileOpen.openPathInViewer).not.toHaveBeenCalled();

    globalThis.fetch = vi.fn(async () => {
      throw new TypeError("Failed to fetch");
    });
    await controller.poll();
    expect(pollWarnings()).toBe(1);
  });

  it("does not pick a dataset for a file that is not HDF5, and says when a job names no file", async () => {
    globalThis.fetch = vi.fn(async () => job({ id: 1, open_path: "/data/frame.cbf", dataset: "/entry" }));
    const { controller, callbacks } = build();
    await controller.poll();
    expect(callbacks.selectDatasetIfPresent).not.toHaveBeenCalled();

    globalThis.fetch = vi.fn(async () => job({ id: 2, open_path: "" }));
    await controller.poll();
    expect(callbacks.setStatus).toHaveBeenLastCalledWith(expect.stringContaining("status.handoff.no_target"));
  });
});
