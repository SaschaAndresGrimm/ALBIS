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
      contrast: false,
      roi: false,
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
        setZoom: vi.fn(),
        setEffectiveScroll: vi.fn(),
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
        setZoom: setZoomB,
        setEffectiveScroll: setEffectiveScrollB,
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
        setZoom: vi.fn(),
        setEffectiveScroll: vi.fn(),
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
        setZoom: setZoomB,
        setEffectiveScroll: setEffectiveScrollB,
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
        setZoom: vi.fn(),
        setEffectiveScroll: vi.fn(),
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
        setZoom: setZoomB,
        setEffectiveScroll: setEffectiveScrollB,
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
        setZoom: vi.fn(),
        setEffectiveScroll: vi.fn(),
      },
      options: {
        createChannel,
        sourceId: "viewer-a",
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
        setZoom: setZoomB,
        setEffectiveScroll: setEffectiveScrollB,
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
});
