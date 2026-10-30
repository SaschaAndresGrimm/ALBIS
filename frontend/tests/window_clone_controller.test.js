import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { createWindowCloneController } from "../modules/window_clone_controller.js";
import { createAnalysisState, createRoiState } from "../modules/state.js";

function build({ file = "/data/frame.cbf", unavailable = "" } = {}) {
  const callbacks = {
    fileCommandUnavailableReason: () => unavailable,
    setStatus: vi.fn(),
    getViewport: () => ({ zoom: 2, centerX: 10, centerY: 20 }),
  };
  const controller = createWindowCloneController({
    state: { file, frameIndex: 0 },
    analysisState: createAnalysisState(),
    roiState: createRoiState(),
    callbacks,
  });
  return { controller, callbacks };
}

describe("window_clone_controller: File > Duplicate Window", () => {
  beforeEach(() => localStorage.clear());
  afterEach(() => vi.restoreAllMocks());

  it("opens this page again with a slot named in the fragment, not the path", () => {
    const open = vi.spyOn(window, "open").mockReturnValue({});
    const { controller, callbacks } = build();
    controller.duplicateWindow();
    const url = new URL(open.mock.calls[0][0]);
    expect(url.hash).toMatch(/^#albis-clone=/);
    expect(url.toString()).not.toContain("frame.cbf");
    expect(callbacks.setStatus).not.toHaveBeenCalled();
  });

  it("says why when the command cannot run, and opens nothing", () => {
    const open = vi.spyOn(window, "open");
    const { controller, callbacks } = build({ unavailable: "Open an image first." });
    controller.duplicateWindow();
    expect(open).not.toHaveBeenCalled();
    expect(callbacks.setStatus).toHaveBeenCalledWith("Open an image first.", { tone: "warning" });
  });

  it("takes the slot back and says so when the pop-up is blocked", () => {
    vi.spyOn(window, "open").mockReturnValue(null);
    const { controller, callbacks } = build();
    controller.duplicateWindow();
    expect(callbacks.setStatus).toHaveBeenCalledWith(expect.stringContaining("status.duplicate_window.blocked"), { tone: "warning" });
    const slots = Object.keys(localStorage).filter((key) => key.includes("clone"));
    expect(slots).toEqual([]);
  });
});
