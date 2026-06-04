import { describe, expect, it, vi } from "vitest";

import { createPostFilePickerBindingsCallbacks } from "../modules/app_binding_contexts.js";

describe("app_binding_contexts", () => {
  it("passes ROI plot log callbacks through post-file-picker bindings", () => {
    const getRoiPlotLog = vi.fn(() => false);
    const setRoiPlotLog = vi.fn();

    const callbacks = createPostFilePickerBindingsCallbacks({
      getRoiPlotLog,
      setRoiPlotLog,
    });

    expect(callbacks.getRoiPlotLog).toBe(getRoiPlotLog);
    expect(callbacks.setRoiPlotLog).toBe(setRoiPlotLog);
  });
});
