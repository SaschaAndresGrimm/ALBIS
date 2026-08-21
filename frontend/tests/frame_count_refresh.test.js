import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { createFrameMetadataController } from "../modules/frame_metadata_controller.js";

/**
 * `loadMetadata` cannot be used to follow a growing series: it stops playback,
 * resets the frame index to zero, reloads the mask and refetches the frame. On a
 * one-second timer that would drag the viewer back to the first frame for as
 * long as an acquisition runs. `refreshFrameCount` exists to touch the count and
 * nothing else, and these tests are that promise.
 */

function build({ shape = [4, 8, 8], writerPresent = true, state: overrides = {} } = {}) {
  document.body.innerHTML = `
    <input id="autoload-dir" value="" />
    <datalist id="autoload-dir-list"></datalist>
    <select id="file-select"></select>
    <span id="meta-shape"></span>
    <span id="meta-dtype"></span>
  `;

  const state = {
    file: "/data/series_master.h5",
    dataset: "/entry/data/data",
    frameCount: 2,
    frameIndex: 1,
    thresholdCount: 1,
    thresholdIndex: 0,
    playing: true,
    hasFrame: true,
    pendingFrame: null,
    isLoading: false,
    shape: [2, 8, 8],
    dtype: "<u4",
    maskAuto: false,
    autoload: { dir: "", types: {} },
    ...overrides,
  };

  const fetchJSON = vi.fn(async () => ({
    path: state.dataset,
    shape,
    dtype: "<u4",
    ndim: shape.length,
    linked_stack: false,
    writer_present: writerPresent,
  }));

  const callbacks = {
    fetchJSON,
    option: vi.fn(),
    fileLabel: (name) => name,
    setDataControlsForHdf5: vi.fn(),
    setDataSourceSectionState: vi.fn(),
    setStatus: vi.fn(),
    stopPlayback: vi.fn(),
    onWriterPresenceChange: vi.fn(),
    updateToolbar: vi.fn(),
    showSplash: vi.fn(),
    setSplashStatus: vi.fn(),
    setLoading: vi.fn(),
    showProcessingProgress: vi.fn(),
    hideProcessingProgress: vi.fn(),
    getDefaultThresholdIndex: vi.fn(() => 0),
    syncSeriesSumOutputPath: vi.fn(),
    updateFrameControls: vi.fn(),
    updateThresholdOptions: vi.fn(),
    loadMask: vi.fn(async () => true),
    loadFrame: vi.fn(async () => true),
    isHdf5File: () => true,
    getDefaultCenter: vi.fn(),
    loadImageGeometry: vi.fn(async () => {}),
    resetTransientFrameLoadState: vi.fn(),
    scheduleResolutionOverlay: vi.fn(),
  };

  const controller = createFrameMetadataController({
    apiBase: "/api",
    state,
    analysisState: { rings: {} },
    elements: {
      autoloadDir: document.getElementById("autoload-dir"),
      autoloadDirList: document.getElementById("autoload-dir-list"),
      fileSelect: document.getElementById("file-select"),
      metaShape: document.getElementById("meta-shape"),
      metaDtype: document.getElementById("meta-dtype"),
      ringInputs: [],
    },
    callbacks,
  });

  return { controller, state, callbacks, fetchJSON };
}

function buildWithFailingFetch(state) {
  return createFrameMetadataController({
    apiBase: "/api",
    state,
    analysisState: { rings: {} },
    elements: {
      metaShape: document.getElementById("meta-shape"),
      metaDtype: document.getElementById("meta-dtype"),
      ringInputs: [],
    },
    callbacks: {
      fetchJSON: vi.fn(async () => {
        throw new Error("file momentarily unreadable");
      }),
      updateFrameControls: vi.fn(),
      updateToolbar: vi.fn(),
    },
  });
}

describe("live frame count refresh", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("picks up frames written since the file was opened", async () => {
    const { controller, state, callbacks } = build({ shape: [7, 8, 8] });

    const result = await controller.refreshFrameCount();

    expect(state.frameCount).toBe(7);
    expect(result).toMatchObject({ writerPresent: true, changed: true, frameCount: 7 });
    expect(callbacks.updateFrameControls).toHaveBeenCalled();
    expect(document.getElementById("meta-shape").textContent).toBe("7 × 8 × 8");
  });

  it("leaves the frame on screen, the playback and the mask alone", async () => {
    const { controller, state, callbacks } = build({ shape: [7, 8, 8] });

    await controller.refreshFrameCount();

    expect(state.frameIndex).toBe(1);
    expect(state.playing).toBe(true);
    expect(callbacks.stopPlayback).not.toHaveBeenCalled();
    expect(callbacks.loadFrame).not.toHaveBeenCalled();
    expect(callbacks.loadMask).not.toHaveBeenCalled();
    expect(callbacks.setLoading).not.toHaveBeenCalled();
    expect(callbacks.showProcessingProgress).not.toHaveBeenCalled();
  });

  it("reports no change when the series has not grown", async () => {
    const { controller, callbacks } = build({ shape: [2, 8, 8] });

    const result = await controller.refreshFrameCount();

    expect(result.changed).toBe(false);
    expect(callbacks.updateFrameControls).not.toHaveBeenCalled();
  });

  it("passes on that the writer has let go, so the watch can stop", async () => {
    const { controller } = build({ shape: [9, 8, 8], writerPresent: false });

    const result = await controller.refreshFrameCount();

    expect(result.writerPresent).toBe(false);
    expect(result.changed).toBe(true);
  });

  it("treats a failed read as a reason to try again, not to give up", async () => {
    const { state } = build();
    vi.spyOn(console, "error").mockImplementation(() => {});
    const failing = buildWithFailingFetch(state);

    const result = await failing.refreshFrameCount();

    expect(result.writerPresent).toBe(true);
    expect(result.changed).toBe(false);
  });

  it("does nothing without an open dataset", async () => {
    const { controller, fetchJSON } = build({ state: { file: "", dataset: "" } });

    const result = await controller.refreshFrameCount();

    expect(fetchJSON).not.toHaveBeenCalled();
    expect(result).toMatchObject({ writerPresent: false, changed: false });
  });

  it("counts a single image as one frame", async () => {
    const { controller, state } = build({ shape: [8, 8] });

    await controller.refreshFrameCount();

    expect(state.frameCount).toBe(1);
  });

  it("reads the frame count of a multi-threshold stack from the first axis", async () => {
    const { controller, state } = build({ shape: [6, 2, 8, 8] });

    await controller.refreshFrameCount();

    expect(state.frameCount).toBe(6);
  });
});
