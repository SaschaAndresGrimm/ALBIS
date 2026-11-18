import { describe, expect, it } from "vitest";

import { followFrameTarget, frameOffsetFor, normalizeFramePayload } from "../modules/viewer_sync_frame_utils.js";

describe("viewer_sync_frame_utils", () => {
  it("reads a frame message and rejects what is not one", () => {
    expect(normalizeFramePayload({ index: 4, previousIndex: 3, playing: 1 })).toEqual({
      index: 4,
      previousIndex: 3,
      playing: true,
      wrap: true,
    });
    expect(normalizeFramePayload({ index: 4 })).toEqual({ index: 4, previousIndex: 4, playing: false, wrap: false });
    // The message that stops playback is not playing, but still wraps.
    expect(normalizeFramePayload({ index: 4, wrap: true }).wrap).toBe(true);
    expect(normalizeFramePayload({ index: -1 })).toBeNull();
    expect(normalizeFramePayload({ index: 1.5 })).toBeNull();
    expect(normalizeFramePayload(null)).toBeNull();
  });

  it("learns the offset from where the sender was before it moved", () => {
    // This window at 12, the sender stepped from 10 to 11: two frames apart.
    expect(frameOffsetFor(12, { index: 11, previousIndex: 10 })).toBe(2);
    expect(frameOffsetFor(3, { index: 9, previousIndex: 8 })).toBe(-5);
  });

  it("keeps the gap when stepping and stops at the ends", () => {
    expect(followFrameTarget({ index: 11, offset: 2, frameCount: 100, wrap: false })).toBe(13);
    expect(followFrameTarget({ index: 99, offset: 2, frameCount: 100, wrap: false })).toBe(99);
    expect(followFrameTarget({ index: 1, offset: -5, frameCount: 100, wrap: false })).toBe(0);
  });

  it("wraps round during playback, as playback itself does", () => {
    expect(followFrameTarget({ index: 99, offset: 2, frameCount: 100, wrap: true })).toBe(1);
    expect(followFrameTarget({ index: 1, offset: -5, frameCount: 100, wrap: true })).toBe(96);
    // A window with fewer frames wraps sooner.
    expect(followFrameTarget({ index: 30, offset: 0, frameCount: 25, wrap: true })).toBe(5);
  });

  it("does not move past the frames a window has", () => {
    expect(followFrameTarget({ index: 5, offset: 0, frameCount: 0, wrap: false })).toBe(0);
  });
});
