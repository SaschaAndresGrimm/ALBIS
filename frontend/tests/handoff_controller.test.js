import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { createHandoffController } from "../modules/handoff_controller.js";

function job(body) {
  return { ok: true, status: 200, json: async () => body };
}

function build() {
  const callbacks = {
    openPathInViewer: vi.fn(async () => {}),
    isHdfFile: (path) => path.endsWith(".h5"),
    selectDatasetIfPresent: vi.fn(async () => true),
    setStatus: vi.fn(),
  };
  return { controller: createHandoffController({ apiBase: "/api", callbacks }), callbacks };
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
    const { controller, callbacks } = build();
    await controller.poll();
    expect(globalThis.fetch.mock.calls[0][0]).toBe("/api/handoff/v1/jobs/latest?after_id=0");
    expect(callbacks.openPathInViewer).toHaveBeenCalledWith("/data/run_master.h5", { refreshFileList: true });
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
    const { controller, callbacks } = build();
    await controller.poll();
    globalThis.fetch = vi.fn(async () => ({ ok: true, status: 204 }));
    await controller.poll();
    expect(callbacks.openPathInViewer).not.toHaveBeenCalled();
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
