import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { createLiveSeriesWatch } from "../modules/live_series_watch.js";

/**
 * The backend reports the current frame count on every request, so a series
 * being written is readable — but a viewer that asks once shows the count the
 * run happened to have when the file was opened. This is the half that follows
 * the acquisition, and the half that has to stop when the run ends.
 */

describe("live series watch", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  function build(results, callbacks = {}) {
    const queue = [...results];
    const refreshFrameCount = vi.fn(async () => queue.shift() ?? results[results.length - 1]);
    const watch = createLiveSeriesWatch({ refreshFrameCount, callbacks, intervalMs: 1000 });
    return { watch, refreshFrameCount };
  }

  it("does nothing until it is started", () => {
    const { watch, refreshFrameCount } = build([{ writerPresent: true }]);

    vi.advanceTimersByTime(5000);

    expect(refreshFrameCount).not.toHaveBeenCalled();
    expect(watch.isRunning()).toBe(false);
  });

  it("re-reads the count while a writer holds the file", async () => {
    const { watch, refreshFrameCount } = build([
      { writerPresent: true, changed: true, frameCount: 2 },
      { writerPresent: true, changed: true, frameCount: 3 },
    ]);

    watch.start();
    await vi.advanceTimersByTimeAsync(2000);

    expect(refreshFrameCount).toHaveBeenCalledTimes(2);
    expect(watch.isRunning()).toBe(true);
  });

  it("reports only the ticks where the count actually changed", async () => {
    const onGrew = vi.fn();
    const { watch } = build(
      [
        { writerPresent: true, changed: true, frameCount: 2 },
        { writerPresent: true, changed: false, frameCount: 2 },
        { writerPresent: true, changed: true, frameCount: 5 },
      ],
      { onGrew },
    );

    watch.start();
    await vi.advanceTimersByTimeAsync(3000);

    // Otherwise a run in progress rewrites the status line every second.
    expect(onGrew.mock.calls.map(([count]) => count)).toEqual([2, 5]);
  });

  it("stops itself when the writer lets go, and says the run finished", async () => {
    const onFinished = vi.fn();
    const { watch, refreshFrameCount } = build(
      [
        { writerPresent: true, changed: true, frameCount: 4 },
        { writerPresent: false, changed: false, frameCount: 4 },
      ],
      { onFinished },
    );

    watch.start();
    await vi.advanceTimersByTimeAsync(2000);

    expect(watch.isRunning()).toBe(false);
    expect(onFinished).toHaveBeenCalledWith(4);

    const callsAtEnd = refreshFrameCount.mock.calls.length;
    await vi.advanceTimersByTimeAsync(5000);
    expect(refreshFrameCount).toHaveBeenCalledTimes(callsAtEnd);
  });

  it("never has two requests in flight", async () => {
    // A slow filesystem is the case this exists for, so ticks that outpace the
    // answer must not stack up.
    let release;
    const refreshFrameCount = vi.fn(
      () => new Promise((resolve) => {
        release = () => resolve({ writerPresent: true, changed: false });
      }),
    );
    const watch = createLiveSeriesWatch({ refreshFrameCount, intervalMs: 100 });

    watch.start();
    await vi.advanceTimersByTimeAsync(1000);

    expect(refreshFrameCount).toHaveBeenCalledTimes(1);
    release();
    watch.stop();
  });

  it("keeps going when one read fails", async () => {
    // A file being appended to can momentarily refuse a read; giving up on the
    // first hiccup would end the watch mid-run.
    const results = [
      { writerPresent: true, changed: false },
      { writerPresent: true, changed: true, frameCount: 7 },
    ];
    let call = 0;
    const refreshFrameCount = vi.fn(async () => {
      call += 1;
      if (call === 1) throw new Error("momentarily unreadable");
      return results[1];
    });
    const onGrew = vi.fn();
    const watch = createLiveSeriesWatch({
      refreshFrameCount,
      callbacks: { onGrew },
      intervalMs: 1000,
    });
    vi.spyOn(console, "error").mockImplementation(() => {});

    watch.start();
    await vi.advanceTimersByTimeAsync(2000);

    expect(watch.isRunning()).toBe(true);
    expect(onGrew).toHaveBeenCalledWith(7);
    watch.stop();
  });

  it("starts and stops from what the last metadata read said", async () => {
    const { watch } = build([{ writerPresent: true, changed: false }]);

    watch.setWriterPresent(true);
    expect(watch.isRunning()).toBe(true);

    watch.setWriterPresent(false);
    expect(watch.isRunning()).toBe(false);
  });

  it("starting twice does not double the polling", async () => {
    const { watch, refreshFrameCount } = build([{ writerPresent: true, changed: false }]);

    watch.start();
    watch.start();
    await vi.advanceTimersByTimeAsync(1000);

    expect(refreshFrameCount).toHaveBeenCalledTimes(1);
    watch.stop();
  });
});
