import { beforeEach, describe, expect, it, vi } from "vitest";

import { createFramePlaybackController } from "../modules/frame_playback_controller.js";
import { createTransientFrameLoadState } from "../modules/transient_frame_load_state.js";

// Mirrors the real wiring in app.js: a frame load takes `loadMs`, and an abort
// settles it immediately without applying a frame, exactly as loadFrame() does
// when its AbortController fires.
function harness({ fps = 1, loadMs = 100, frameCount = 100, viewerSync } = {}) {
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
      queuePendingFrameRequest: transient.queuePendingFrame,
      consumePendingFrameRequest: transient.consumePendingFrameRequest,
      isFrameLoading: transient.isFrameLoading,
    },
    core: {
      viewerSync,
      fileDataPipeline: {
        cancelActiveLoad() {
          cancels.push(state.frameIndex);
          if (abortInFlight) abortInFlight();
        },
        loadFrame,
      },
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

  it("plays at 30 fps when frames load fast enough", () => {
    // The ceiling used to be a 50 ms tick, 20 fps whatever was chosen.
    const { controller, rendered } = harness({ fps: 30, loadMs: 5 });

    controller.startPlayback();
    vi.advanceTimersByTime(2000);

    expect(rendered.length).toBeGreaterThanOrEqual(55);
    expect(rendered.length).toBeLessThanOrEqual(61);
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

describe("frame linking with other windows", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  function linked(options = {}) {
    const sent = [];
    const viewerSync = {
      handleFrameChanged: (previousIndex, reason = "change") => sent.push({ previousIndex, reason }),
    };
    return { ...harness({ ...options, viewerSync }), sent };
  }

  it("tells linked windows about a frame stepped to here, with the frame before it", () => {
    const { controller, sent } = linked({ loadMs: 10 });

    controller.requestFrame(5);

    expect(sent).toEqual([{ previousIndex: 0, reason: "change" }]);
  });

  it("does not send back a frame it went to because another window asked", () => {
    // Sending it on would echo, and in a group of three move the third twice.
    const { controller, sent, rendered } = linked({ loadMs: 10 });

    controller.followFrame(7);
    vi.advanceTimersByTime(100);

    expect(rendered).toEqual([7]);
    expect(sent).toEqual([]);
  });

  it("never cancels a load while following, and lands the newest frame", () => {
    // Messages arrive faster than a frame loads; cancelling each time, as
    // scrubbing does, would mean no frame ever lands.
    const { controller, cancels, rendered, sent } = linked({ loadMs: 300 });

    controller.followFrame(1, { playing: true });
    for (let index = 2; index <= 10; index += 1) {
      vi.advanceTimersByTime(40);
      controller.followFrame(index, { playing: true });
    }
    vi.advanceTimersByTime(2000);

    expect(cancels).toEqual([]);
    // Frame 1 lands at 300 ms, when 8 is the newest asked for; 10 lands last.
    // Everything in between is skipped.
    expect(rendered).toEqual([1, 8, 10]);
    expect(sent).toEqual([]);
  });

  it("counts as playing while following playback, without a timer of its own", () => {
    const { state, controller } = linked({ loadMs: 10 });

    controller.followFrame(3, { playing: true });

    expect(state.playing).toBe(true);
    vi.advanceTimersByTime(100);
    // Only the frame load ran: no playback tick was armed here.
    expect(vi.getTimerCount()).toBe(0);
    expect(state.frameIndex).toBe(3);
  });

  it("stops quietly when the leading window stops", () => {
    const { state, controller, sent } = linked({ loadMs: 10 });

    controller.followFrame(3, { playing: true });
    controller.followFrame(3, { playing: false });

    expect(state.playing).toBe(false);
    expect(sent).toEqual([]);
  });

  it("announces play and stop, so Stop in any window stops them all", () => {
    const { controller, sent } = linked({ loadMs: 10 });

    controller.followFrame(3, { playing: true });
    controller.stopPlayback();
    controller.startPlayback();

    expect(sent.map((entry) => entry.reason)).toEqual(["stop", "play"]);
  });

  it("takes the lead from a window it was following when started here", () => {
    const { state, controller, rendered } = linked({ fps: 10, loadMs: 10 });

    controller.followFrame(3, { playing: false });
    controller.startPlayback();
    vi.advanceTimersByTime(1000);

    expect(state.playing).toBe(true);
    expect(rendered.length).toBeGreaterThan(3);
  });

  it("ignores another window's frame while showing a live source's history", () => {
    const { state, controller, rendered } = linked({ loadMs: 10 });
    state.autoload = { mode: "simplon", historyEntries: [{}, {}] };

    controller.followFrame(4);
    vi.advanceTimersByTime(100);

    expect(rendered).toEqual([]);
  });
});
