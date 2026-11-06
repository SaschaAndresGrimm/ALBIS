import { describe, expect, it } from "vitest";

import { statusPillContent } from "../modules/status_line.js";

describe("statusPillContent", () => {
  it("shows an ambient message as it is", () => {
    expect(statusPillContent("Loading metadata…")).toEqual({ text: "Loading metadata…" });
  });

  it("falls back to Idle for an empty message", () => {
    expect(statusPillContent("  ")).toEqual({ key: "common.idle" });
  });

  it("shows Ready for a frame position", () => {
    expect(statusPillContent("Frame 3 / 10", { frameStatus: true })).toEqual({ key: "common.ready" });
  });

  it("does not repeat a toned message, which goes to a toast", () => {
    expect(statusPillContent("Failed to load frame", { tone: "error", hasFrame: true })).toEqual({
      key: "common.ready",
    });
    expect(statusPillContent("Failed to load frame", { tone: "error", hasFrame: false })).toEqual({
      key: "common.idle",
    });
    expect(statusPillContent("Saved", { tone: "success", hasFrame: true })).toEqual({ key: "common.ready" });
  });
});
