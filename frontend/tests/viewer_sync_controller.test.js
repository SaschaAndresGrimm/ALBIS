import { afterEach, describe, expect, it, vi } from "vitest";

function createCanvasWrap({ clientWidth = 320, clientHeight = 240 } = {}) {
  return {
    clientWidth,
    clientHeight,
  };
}

function createChannelFactory() {
  const channels = [];
  return {
    createChannel: () => {
      const channel = {
        onmessage: null,
        closed: false,
        postMessage(message) {
          channels.forEach((target) => {
            if (target === channel || target.closed || typeof target.onmessage !== "function") return;
            target.onmessage({ data: message });
          });
        },
        close() {
          channel.closed = true;
        },
      };
      channels.push(channel);
      return channel;
    },
  };
}

function createState(overrides = {}) {
  return {
    hasFrame: true,
    width: 1000,
    height: 800,
    zoom: 1,
    renderOffsetX: 0,
    renderOffsetY: 0,
    viewerSync: {
      enabled: false,
      group: "default",
      viewport: true,
      contrast: true,
      roi: true,
    },
    ...overrides,
  };
}

describe("viewer_sync_controller", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("broadcasts zoom and image-space center to linked viewers", async () => {
    vi.useFakeTimers();
    const { createViewerSyncController } = await import("../modules/viewer_sync_controller.js");
    const { createChannel } = createChannelFactory();

    const stateA = createState({ zoom: 2 });
    const stateB = createState();
    const setZoomB = vi.fn((zoom) => {
      stateB.zoom = Number(zoom);
    });
    const setEffectiveScrollB = vi.fn();

    const controllerA = createViewerSyncController({
      state: stateA,
      elements: {
        syncToggle: null,
        canvasWrap: createCanvasWrap(),
      },
      callbacks: {
        getViewRect: () => ({ viewX: 100, viewY: 50, viewW: 200, viewH: 100 }),
      },
      core: {
        overviewViewport: { setZoom: vi.fn(), setEffectiveScroll: vi.fn() },
      },
      options: {
        createChannel,
        sourceId: "viewer-a",
        publishIntervalMs: 40,
      },
    });

    createViewerSyncController({
      state: stateB,
      elements: {
        syncToggle: null,
        canvasWrap: createCanvasWrap({ clientWidth: 300, clientHeight: 200 }),
      },
      callbacks: {
        getViewRect: vi.fn(),
      },
      core: {
        overviewViewport: { setZoom: setZoomB, setEffectiveScroll: setEffectiveScrollB },
      },
      options: {
        createChannel,
        sourceId: "viewer-b",
        publishIntervalMs: 40,
      },
    }).setEnabled(true);

    controllerA.setEnabled(true);
    controllerA.handleViewportChanged("pan");

    expect(setZoomB).toHaveBeenCalledWith(2);
    expect(setEffectiveScrollB).toHaveBeenCalledWith(250, 100, true);
  });

  it("publishes live viewport changes at a throttled cadence", async () => {
    vi.useFakeTimers();
    const { createViewerSyncController } = await import("../modules/viewer_sync_controller.js");
    const { createChannel } = createChannelFactory();

    const stateA = createState();
    const stateB = createState();
    const setZoomB = vi.fn((zoom) => {
      stateB.zoom = Number(zoom);
    });
    const setEffectiveScrollB = vi.fn();
    let effectiveLeft = 10;
    let effectiveTop = 20;

    const controllerA = createViewerSyncController({
      state: stateA,
      elements: {
        syncToggle: null,
        canvasWrap: createCanvasWrap({ clientWidth: 200, clientHeight: 100 }),
      },
      callbacks: {
        getViewRect: vi.fn(),
        getEffectiveScrollLeft: () => effectiveLeft,
        getEffectiveScrollTop: () => effectiveTop,
      },
      core: {
        overviewViewport: { setZoom: vi.fn(), setEffectiveScroll: vi.fn() },
      },
      options: {
        createChannel,
        sourceId: "viewer-a",
        publishIntervalMs: 40,
      },
    });

    createViewerSyncController({
      state: stateB,
      elements: {
        syncToggle: null,
        canvasWrap: createCanvasWrap({ clientWidth: 200, clientHeight: 100 }),
      },
      callbacks: {
        getViewRect: vi.fn(),
      },
      core: {
        overviewViewport: { setZoom: setZoomB, setEffectiveScroll: setEffectiveScrollB },
      },
      options: {
        createChannel,
        sourceId: "viewer-b",
        publishIntervalMs: 40,
      },
    }).setEnabled(true);

    controllerA.setEnabled(true);
    controllerA.handleViewportChanged("pan");
    expect(setEffectiveScrollB).toHaveBeenCalledWith(10, 20, true);

    effectiveLeft = 30;
    effectiveTop = 40;
    await vi.advanceTimersByTimeAsync(10);
    controllerA.handleViewportChanged("pan");
    expect(setEffectiveScrollB).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(30);
    expect(setEffectiveScrollB).toHaveBeenLastCalledWith(30, 40, true);
  });

  it("requests the current viewport when a viewer joins an active sync group", async () => {
    const { createViewerSyncController } = await import("../modules/viewer_sync_controller.js");
    const { createChannel } = createChannelFactory();

    const stateA = createState({ zoom: 3 });
    const stateB = createState();
    const setZoomB = vi.fn((zoom) => {
      stateB.zoom = Number(zoom);
    });
    const setEffectiveScrollB = vi.fn();

    createViewerSyncController({
      state: stateA,
      elements: {
        syncToggle: null,
        canvasWrap: createCanvasWrap(),
      },
      callbacks: {
        getViewRect: () => ({ viewX: 10, viewY: 20, viewW: 100, viewH: 80 }),
      },
      core: {
        overviewViewport: { setZoom: vi.fn(), setEffectiveScroll: vi.fn() },
      },
      options: {
        createChannel,
        sourceId: "viewer-a",
      },
    }).setEnabled(true);

    createViewerSyncController({
      state: stateB,
      elements: {
        syncToggle: null,
        canvasWrap: createCanvasWrap({ clientWidth: 200, clientHeight: 120 }),
      },
      callbacks: {
        getViewRect: vi.fn(),
      },
      core: {
        overviewViewport: { setZoom: setZoomB, setEffectiveScroll: setEffectiveScrollB },
      },
      options: {
        createChannel,
        sourceId: "viewer-b",
      },
    }).setEnabled(true);

    expect(setZoomB).toHaveBeenCalledWith(3);
    expect(setEffectiveScrollB).toHaveBeenCalledWith(80, 120, true);
  });

  it("keeps unclamped image position synchronized when zoomed out", async () => {
    const { createViewerSyncController } = await import("../modules/viewer_sync_controller.js");
    const { createChannel } = createChannelFactory();

    const stateA = createState({
      zoom: 0.2,
      renderOffsetX: 100,
      renderOffsetY: 80,
    });
    const stateB = createState({
      zoom: 0.2,
      renderOffsetX: 100,
      renderOffsetY: 80,
    });
    const setZoomB = vi.fn((zoom) => {
      stateB.zoom = Number(zoom);
    });
    const setEffectiveScrollB = vi.fn();

    const controllerA = createViewerSyncController({
      state: stateA,
      elements: {
        syncToggle: null,
        canvasWrap: createCanvasWrap({ clientWidth: 500, clientHeight: 400 }),
      },
      callbacks: {
        getViewRect: () => ({ viewX: 0, viewY: 0, viewW: 1000, viewH: 800 }),
        getEffectiveScrollLeft: () => -70,
        getEffectiveScrollTop: () => 180,
      },
      core: {
        overviewViewport: { setZoom: vi.fn(), setEffectiveScroll: vi.fn() },
      },
      options: {
        createChannel,
        sourceId: "viewer-a",
        publishIntervalMs: 0,
      },
    });

    createViewerSyncController({
      state: stateB,
      elements: {
        syncToggle: null,
        canvasWrap: createCanvasWrap({ clientWidth: 500, clientHeight: 400 }),
      },
      callbacks: {
        getViewRect: vi.fn(),
      },
      core: {
        overviewViewport: { setZoom: setZoomB, setEffectiveScroll: setEffectiveScrollB },
      },
      options: {
        createChannel,
        sourceId: "viewer-b",
      },
    }).setEnabled(true);

    controllerA.setEnabled(true);
    setZoomB.mockClear();
    setEffectiveScrollB.mockClear();

    controllerA.publishViewport("manual");

    expect(setZoomB).toHaveBeenCalledWith(0.2);
    expect(setEffectiveScrollB).toHaveBeenCalledWith(-70, 180, true);
  });

  it("broadcasts contrast settings when contrast sync is enabled", async () => {
    const { createViewerSyncController } = await import("../modules/viewer_sync_controller.js");
    const { createChannel } = createChannelFactory();

    const stateA = createState({
      autoScale: false,
      min: 12,
      max: 48,
      colormap: "viridis",
      invert: true,
    });
    const stateB = createState({
      autoScale: true,
      min: 0,
      max: 1,
      colormap: "gray",
      invert: false,
    });
    const applySyncedContrastB = vi.fn((contrast) => {
      stateB.autoScale = contrast.autoScale;
      stateB.min = contrast.min;
      stateB.max = contrast.max;
      stateB.colormap = contrast.colormap;
      stateB.invert = contrast.invert;
    });

    const controllerA = createViewerSyncController({
      state: stateA,
      elements: {
        syncToggle: null,
        canvasWrap: createCanvasWrap(),
      },
      callbacks: {
        getViewRect: vi.fn(),
      },
      core: {
        overviewViewport: { setZoom: vi.fn(), setEffectiveScroll: vi.fn() },
      },
      options: {
        createChannel,
        sourceId: "viewer-a",
        publishIntervalMs: 0,
      },
    });

    const controllerB = createViewerSyncController({
      state: stateB,
      elements: {
        syncToggle: null,
        canvasWrap: createCanvasWrap(),
      },
      callbacks: {
        getViewRect: vi.fn(),
        applySyncedContrast: applySyncedContrastB,
      },
      core: {
        overviewViewport: { setZoom: vi.fn(), setEffectiveScroll: vi.fn() },
      },
      options: {
        createChannel,
        sourceId: "viewer-b",
      },
    });

    controllerA.setSyncOption("contrast", true);
    controllerB.setSyncOption("contrast", true);
    controllerA.setEnabled(true);
    controllerB.setEnabled(true);
    applySyncedContrastB.mockClear();

    stateA.min = 14;
    stateA.max = 56;
    controllerA.handleContrastChanged("levels");

    expect(applySyncedContrastB).toHaveBeenCalledWith({
      autoScale: false,
      min: 14,
      max: 56,
      colormap: "viridis",
      invert: true,
    });
    expect(stateB).toMatchObject({
      autoScale: false,
      min: 14,
      max: 56,
      colormap: "viridis",
      invert: true,
    });
  });

  it("broadcasts ROI geometry when ROI sync is enabled", async () => {
    const { createViewerSyncController } = await import("../modules/viewer_sync_controller.js");
    const { createChannel } = createChannelFactory();

    const roiA = {
      enabled: true,
      mode: "box",
      active: true,
      start: { x: 20, y: 30 },
      end: { x: 220, y: 180 },
      innerRadius: 0,
      outerRadius: 0,
    };
    const roiB = {
      enabled: true,
      mode: "line",
      active: false,
      start: null,
      end: null,
      innerRadius: 0,
      outerRadius: 0,
    };
    const stateA = createState();
    const stateB = createState();
    const applySyncedRoiB = vi.fn((roi) => {
      Object.assign(roiB, roi);
    });

    const controllerA = createViewerSyncController({
      state: stateA,
      roiState: roiA,
      elements: {
        syncToggle: null,
        canvasWrap: createCanvasWrap(),
      },
      callbacks: {
        getViewRect: vi.fn(),
      },
      core: {
        overviewViewport: { setZoom: vi.fn(), setEffectiveScroll: vi.fn() },
      },
      options: {
        createChannel,
        sourceId: "viewer-a",
        publishIntervalMs: 0,
      },
    });

    const controllerB = createViewerSyncController({
      state: stateB,
      roiState: roiB,
      elements: {
        syncToggle: null,
        canvasWrap: createCanvasWrap(),
      },
      callbacks: {
        getViewRect: vi.fn(),
        applySyncedRoi: applySyncedRoiB,
      },
      core: {
        overviewViewport: { setZoom: vi.fn(), setEffectiveScroll: vi.fn() },
      },
      options: {
        createChannel,
        sourceId: "viewer-b",
      },
    });

    controllerA.setSyncOption("roi", true);
    controllerB.setSyncOption("roi", true);
    controllerA.setEnabled(true);
    controllerB.setEnabled(true);
    applySyncedRoiB.mockClear();

    roiA.end = { x: 240, y: 190 };
    controllerA.handleRoiChanged("roi");

    expect(applySyncedRoiB).toHaveBeenCalledWith({
      enabled: true,
      mode: "box",
      active: true,
      start: { x: 20, y: 30 },
      end: { x: 240, y: 190 },
      innerRadius: 0,
      outerRadius: 0,
    });
    expect(roiB).toMatchObject({
      enabled: true,
      mode: "box",
      active: true,
      start: { x: 20, y: 30 },
      end: { x: 240, y: 190 },
    });
  });

  describe("frame", () => {
    async function pair({ frameA = 10, frameB = 12, frameCount = 100 } = {}) {
      const { createViewerSyncController } = await import("../modules/viewer_sync_controller.js");
      const { createChannel } = createChannelFactory();
      const make = (sourceId, frameIndex) => {
        const state = createState({ frameIndex, frameCount, playing: false });
        const followFrame = vi.fn((index) => {
          state.frameIndex = index;
        });
        const controller = createViewerSyncController({
          state,
          elements: { syncToggle: null, canvasWrap: createCanvasWrap() },
          callbacks: { getViewRect: vi.fn() },
          core: { framePlayback: { followFrame } },
          options: { createChannel, sourceId, publishIntervalMs: 0 },
        });
        return { state, followFrame, controller };
      };
      const a = make("viewer-a", frameA);
      const b = make("viewer-b", frameB);
      return { a, b };
    }

    function link(...viewers) {
      viewers.forEach(({ controller }) => {
        controller.setEnabled(true);
        controller.setSyncOption("frame", true);
      });
    }

    function step(viewer, index) {
      const previous = viewer.state.frameIndex;
      viewer.state.frameIndex = index;
      viewer.controller.handleFrameChanged(previous);
    }

    it("is off by default, so linking does not move frames", async () => {
      const { a, b } = await pair();
      a.controller.setEnabled(true);
      b.controller.setEnabled(true);

      step(a, 11);

      expect(a.state.viewerSync.frame).toBe(false);
      expect(b.followFrame).not.toHaveBeenCalled();
    });

    it("keeps the gap between the windows as one steps", async () => {
      const { a, b } = await pair({ frameA: 10, frameB: 12 });
      link(a, b);

      step(a, 11);
      expect(b.followFrame).toHaveBeenLastCalledWith(13, { playing: false });

      step(a, 30);
      expect(b.followFrame).toHaveBeenLastCalledWith(32, { playing: false });

      // And the other way round.
      step(b, 40);
      expect(a.followFrame).toHaveBeenLastCalledWith(38, { playing: false });
    });

    it("wraps round while the sender plays", async () => {
      const { a, b } = await pair({ frameA: 10, frameB: 12 });
      link(a, b);
      a.state.playing = true;

      step(a, 99);

      expect(b.followFrame).toHaveBeenLastCalledWith(1, { playing: true });
    });

    it("wraps the frame that stops playback, which came from a playback that wrapped", async () => {
      // The regression: the windows stopped on either side of the wrap, B at
      // 1 and A due at 98; the stop message was clamped, and put A on frame 0.
      const { a, b } = await pair({ frameA: 10, frameB: 13, frameCount: 100 });
      link(a, b);
      step(b, 14);
      expect(a.followFrame).toHaveBeenLastCalledWith(11, { playing: false });

      b.state.frameIndex = 3;
      b.controller.handleFrameChanged(14, "stop");

      expect(a.followFrame).toHaveBeenLastCalledWith(0, { playing: false });
      b.state.frameIndex = 1;
      b.controller.handleFrameChanged(3, "stop");
      expect(a.followFrame).toHaveBeenLastCalledWith(98, { playing: false });
    });

    it("learns the gap again after linking anew", async () => {
      const { a, b } = await pair({ frameA: 10, frameB: 12 });
      link(a, b);
      step(a, 11);

      b.controller.setSyncOption("frame", false);
      b.state.frameIndex = 50;
      b.controller.setSyncOption("frame", true);
      step(a, 12);

      expect(b.followFrame).toHaveBeenLastCalledWith(51, { playing: false });
    });
  });
});
