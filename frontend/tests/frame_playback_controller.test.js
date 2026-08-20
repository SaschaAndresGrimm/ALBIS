import { beforeEach, describe, expect, it, vi } from "vitest";

import { createFramePlaybackController } from "../modules/frame_playback_controller.js";
import { createTransientFrameLoadState } from "../modules/transient_frame_load_state.js";

// Mirrors the real wiring in app.js: a frame load takes `loadMs`, and an abort
// settles it immediately without applying a frame, exactly as loadFrame() does
// when its AbortController fires.
function harness({ fps = 1, loadMs = 100, frameCount = 100 } = {}) {
  const state = {
    file: "frames.h5",
    dataset: "/entry/data/data",
    seriesFiles: [],
    frameCount,
    frameIndex: 0,
    step: 1,
    fps,
    playing: false,
    playTimer: null,
    isLoading: false,
    pendingFrame: null,
  };

  const transient = createTransientFrameLoadState(state);
  const rendered = [];
  const cancels = [];
  let viewportBusy = false;
  let abortInFlight = null;
  let controller;

  function loadFrame() {
    if (!transient.startFrameLoad()) return;
    const target = state.frameIndex;
    const timer = setTimeout(() => {
      abortInFlight = null;
      rendered.push(target);
      transient.finishFrameLoad();
      controller.processPendingFrameRequest(true);
    }, loadMs);
    abortInFlight = () => {
      clearTimeout(timer);
      abortInFlight = null;
      transient.finishFrameLoad();
      controller.processPendingFrameRequest(false);
    };
  }

  controller = createFramePlaybackController({
    state,
    elements: {},
    callbacks: {
      updatePlayButtons() {},
      setLoading() {},
      updateToolbar() {},
      isViewportInteractionActive: () => viewportBusy,
      cancelActiveFrameLoad() {
        cancels.push(state.frameIndex);
        if (abortInFlight) abortInFlight();
      },
      loadFrame,
      queuePendingFrameRequest: transient.queuePendingFrame,
      consumePendingFrameRequest: transient.consumePendingFrameRequest,
      isFrameLoading: transient.isFrameLoading,
    },
  });

  return {
    state,
    controller,
    rendered,
    cancels,
    setViewportBusy: (value) => {
      viewportBusy = value;
    },
  };
}

describe("frame playback pacing", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  it("renders frames when the frame load fits inside the tick interval", () => {
    const { controller, rendered } = harness({ fps: 2, loadMs: 100 });

    controller.startPlayback();
    vi.advanceTimersByTime(3000);

    expect(rendered.length).toBeGreaterThan(3);
  });

  it("still renders frames when a load takes longer than the tick interval", () => {
    // The regression: at 10 fps the interval is 100ms, so a 300ms load used to be
    // aborted by the following tick every single time. Playback rendered nothing
    // at all until it was stopped.
    const { controller, rendered } = harness({ fps: 10, loadMs: 300 });

    controller.startPlayback();
    vi.advanceTimersByTime(3000);

    expect(rendered.length).toBeGreaterThan(3);
  });

  it("never aborts a frame it still wants while playing", () => {
    const { controller, cancels } = harness({ fps: 10, loadMs: 300 });

    controller.startPlayback();
    vi.advanceTimersByTime(3000);

    expect(cancels).toEqual([]);
  });

  it("advances through frames in order rather than sticking on one", () => {
    const { controller, rendered } = harness({ fps: 10, loadMs: 300 });

    controller.startPlayback();
    vi.advanceTimersByTime(3000);

    expect(rendered).toEqual([...rendered].sort((a, b) => a - b));
    expect(new Set(rendered).size).toBe(rendered.length);
  });

  it("throttles to what the source can deliver instead of the requested rate", () => {
    const slow = harness({ fps: 10, loadMs: 500 });
    const fast = harness({ fps: 10, loadMs: 50 });

    slow.controller.startPlayback();
    fast.controller.startPlayback();
    vi.advanceTimersByTime(5000);

    expect(fast.rendered.length).toBeGreaterThan(slow.rendered.length);
    // A 500ms load cannot exceed 2 frames/sec no matter what fps is selected.
    expect(slow.rendered.length).toBeLessThanOrEqual(10);
  });

  it("honours a speed change without restarting playback", () => {
    const { state, controller, rendered } = harness({ fps: 1, loadMs: 10 });

    controller.startPlayback();
    vi.advanceTimersByTime(2000);
    const atOneFps = rendered.length;

    state.fps = 10;
    vi.advanceTimersByTime(2000);

    expect(rendered.length - atOneFps).toBeGreaterThan(atOneFps);
  });

  it("stops issuing loads once playback is stopped", () => {
    const { controller, rendered } = harness({ fps: 10, loadMs: 50 });

    controller.startPlayback();
    vi.advanceTimersByTime(1000);
    controller.stopPlayback();

    // A frame already in flight is allowed to land: the fetch is already paid
    // for and it belongs to the current index. What must not happen is a new
    // load starting after the stop.
    vi.advanceTimersByTime(1000);
    const settled = rendered.length;
    vi.advanceTimersByTime(5000);

    expect(rendered.length).toBe(settled);
  });

  it("clears its timer on stop so no tick can leak past it", () => {
    const { state, controller } = harness({ fps: 10, loadMs: 50 });

    controller.startPlayback();
    vi.advanceTimersByTime(1000);
    controller.stopPlayback();

    expect(state.playing).toBe(false);
    expect(state.playTimer).toBeNull();

    // Once the in-flight frame has settled, nothing at all remains scheduled --
    // a re-arming tick that outlived the stop would show up here.
    vi.advanceTimersByTime(1000);
    expect(vi.getTimerCount()).toBe(0);
  });
});

describe("interactive frame navigation", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  it("drops an in-flight load to chase the newest index when not playing", () => {
    // Every interactive caller (slider, arrow keys, command palette) calls
    // stopPlayback() first, so scrubbing must keep its cancel-and-chase
    // behaviour: intermediate frames are worthless while dragging.
    const { controller, cancels, rendered } = harness({ loadMs: 300 });

    controller.requestFrame(10);
    vi.advanceTimersByTime(50);
    controller.requestFrame(20);

    expect(cancels).toHaveLength(1);

    vi.advanceTimersByTime(1000);
    expect(rendered).toEqual([20]);
  });

  it("defers a request made while the viewport is being manipulated", () => {
    const { controller, rendered, setViewportBusy } = harness({ fps: 10, loadMs: 50 });
    setViewportBusy(true);

    controller.startPlayback();
    vi.advanceTimersByTime(1000);

    expect(rendered).toEqual([]);

    setViewportBusy(false);
    vi.advanceTimersByTime(1000);

    expect(rendered.length).toBeGreaterThan(0);
  });
});
