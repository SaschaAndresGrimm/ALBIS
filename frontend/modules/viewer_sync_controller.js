/**
 * Cross-window viewer synchronization for viewport, contrast, ROI and frame.
 */

import { t } from "./i18n.js";
import { followFrameTarget, frameOffsetFor, normalizeFramePayload } from "./viewer_sync_frame_utils.js";

const DEFAULT_CHANNEL_NAME = "albis.viewerSync";
const DEFAULT_GROUP = "default";
const MESSAGE_VIEWPORT = "albis.viewerSync.viewport";
const MESSAGE_REQUEST_VIEWPORT = "albis.viewerSync.requestViewport";
const MESSAGE_CONTRAST = "albis.viewerSync.contrast";
const MESSAGE_REQUEST_CONTRAST = "albis.viewerSync.requestContrast";
const MESSAGE_ROI = "albis.viewerSync.roi";
const MESSAGE_REQUEST_ROI = "albis.viewerSync.requestRoi";
const MESSAGE_FRAME = "albis.viewerSync.frame";
const SYNC_OPTIONS = new Set(["viewport", "contrast", "roi", "frame"]);
const ROI_MODES = new Set(["line", "box", "circle", "annulus"]);

function createSourceId(windowObj) {
  const cryptoObj = windowObj?.crypto;
  if (typeof cryptoObj?.randomUUID === "function") {
    return cryptoObj.randomUUID();
  }
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}

function getViewerSyncState(state) {
  if (!state.viewerSync) {
    state.viewerSync = {
      enabled: false,
      group: DEFAULT_GROUP,
      viewport: true,
      contrast: true,
      roi: true,
      frame: true,
    };
  }
  if (!state.viewerSync.group) {
    state.viewerSync.group = DEFAULT_GROUP;
  }
  if (state.viewerSync.viewport !== false) {
    state.viewerSync.viewport = true;
  }
  if (state.viewerSync.contrast !== false) {
    state.viewerSync.contrast = true;
  }
  if (state.viewerSync.roi !== false) {
    state.viewerSync.roi = true;
  }
  if (state.viewerSync.frame !== false) {
    state.viewerSync.frame = true;
  }
  return state.viewerSync;
}

function normalizeViewportPayload(payload) {
  if (!payload || typeof payload !== "object") return null;
  const zoom = Number(payload.zoom);
  const centerX = Number(payload.centerX);
  const centerY = Number(payload.centerY);
  if (!Number.isFinite(zoom) || zoom <= 0) return null;
  if (!Number.isFinite(centerX) || !Number.isFinite(centerY)) return null;
  return {
    zoom,
    centerX,
    centerY,
    imageWidth: Number(payload.imageWidth) || 0,
    imageHeight: Number(payload.imageHeight) || 0,
  };
}

function normalizeContrastPayload(payload) {
  if (!payload || typeof payload !== "object") return null;
  const min = Number(payload.min);
  const max = Number(payload.max);
  if (!Number.isFinite(min) || !Number.isFinite(max) || max < min) return null;
  const colormap = typeof payload.colormap === "string" ? payload.colormap.trim() : "";
  return {
    autoScale: Boolean(payload.autoScale),
    min,
    max,
    colormap,
    invert: Boolean(payload.invert),
  };
}

function normalizePoint(point) {
  if (!point || typeof point !== "object") return null;
  const x = Number(point.x);
  const y = Number(point.y);
  if (!Number.isFinite(x) || !Number.isFinite(y)) return null;
  return { x, y };
}

function normalizeRoiPayload(payload) {
  if (!payload || typeof payload !== "object") return null;
  const mode = ROI_MODES.has(payload.mode) ? payload.mode : "line";
  const active = Boolean(payload.active);
  const start = active ? normalizePoint(payload.start) : null;
  const end = active ? normalizePoint(payload.end) : null;
  if (active && (!start || !end)) return null;
  const innerRadius = Math.max(0, Math.round(Number(payload.innerRadius) || 0));
  const outerRadius = Math.max(0, Math.round(Number(payload.outerRadius) || 0));
  return {
    enabled: payload.enabled !== false,
    mode,
    active,
    start,
    end,
    innerRadius,
    outerRadius,
  };
}

function payloadSignature(payload) {
  try {
    return JSON.stringify(payload);
  } catch {
    return "";
  }
}

export function createViewerSyncController({
  state,
  roiState = null,
  elements,
  callbacks,
  core,
  options = {},
}) {
  const {
    syncWrap,
    syncToggle,
    syncOptionsToggle,
    syncPopover,
    syncViewportToggle,
    syncContrastToggle,
    syncRoiToggle,
    syncFrameToggle,
    canvasWrap,
  } = elements;

  const {
    getViewRect,
    getEffectiveScrollLeft,
    getEffectiveScrollTop,
    applySyncedContrast,
    applySyncedRoi,
  } = callbacks;

  const windowObj = options.windowObj || window;
  const documentObj = windowObj.document || (typeof document !== "undefined" ? document : null);
  const channelName = options.channelName || DEFAULT_CHANNEL_NAME;
  const sourceId = options.sourceId || createSourceId(windowObj);
  const rawPublishIntervalMs = Number(options.publishIntervalMs ?? options.publishDelayMs);
  const publishIntervalMs = Number.isFinite(rawPublishIntervalMs)
    ? Math.max(0, rawPublishIntervalMs)
    : 40;
  const createChannel = options.createChannel || ((name) => {
    if (typeof windowObj.BroadcastChannel !== "function") return null;
    return new windowObj.BroadcastChannel(name);
  });

  let channel = null;
  const publishQueues = {
    viewport: { timer: null, lastTs: 0, reason: "" },
    contrast: { timer: null, lastTs: 0, reason: "" },
    roi: { timer: null, lastTs: 0, reason: "" },
    frame: { timer: null, lastTs: 0, reason: "" },
  };
  // Frame linking: this window's offset to each sender, learned from the
  // sender's first message, and where this window was before the first change
  // still waiting to be sent (the throttle may merge several changes).
  const frameOffsets = new Map();
  let framePreviousIndex = null;
  let lastContrastSignature = "";
  let lastRoiSignature = "";
  let applyingRemote = false;
  let available = Boolean(options.createChannel || typeof windowObj.BroadcastChannel === "function");
  let optionsOpen = false;

  function syncState() {
    return getViewerSyncState(state);
  }

  function groupName() {
    return syncState().group || DEFAULT_GROUP;
  }

  function openChannel() {
    if (channel || !available) return channel;
    try {
      channel = createChannel(channelName);
    } catch {
      channel = null;
      available = false;
    }
    if (!channel) {
      available = false;
      return null;
    }
    channel.onmessage = handleChannelMessage;
    return channel;
  }

  function closeChannel() {
    if (!channel) return;
    channel.onmessage = null;
    if (typeof channel.close === "function") {
      channel.close();
    }
    channel = null;
  }

  function postMessage(message) {
    const target = openChannel();
    if (!target) return false;
    target.postMessage({
      ...message,
      sourceId,
      group: groupName(),
      ts: Date.now(),
    });
    return true;
  }

  function readViewport() {
    if (!state.hasFrame || !state.width || !state.height) return null;
    // The view controller holds the view as exactly this: a centre and a zoom.
    const own = core?.overviewViewport?.getViewCenter?.();
    if (own && Number.isFinite(own.centerX) && Number.isFinite(own.centerY)) {
      return { ...own, imageWidth: state.width, imageHeight: state.height };
    }
    const zoom = Number.isFinite(state.zoom) && state.zoom > 0 ? state.zoom : 1;
    const viewWidth = Number(canvasWrap?.clientWidth) || 0;
    const viewHeight = Number(canvasWrap?.clientHeight) || 0;
    const effectiveLeft = Number(getEffectiveScrollLeft?.());
    const effectiveTop = Number(getEffectiveScrollTop?.());
    if (
      viewWidth > 0 &&
      viewHeight > 0 &&
      Number.isFinite(effectiveLeft) &&
      Number.isFinite(effectiveTop)
    ) {
      const zoomY = zoom * (state.pixelAspect || 1);
      const centerX = (effectiveLeft + viewWidth / 2 - (state.renderOffsetX || 0)) / zoom;
      const centerY = (effectiveTop + viewHeight / 2 - (state.renderOffsetY || 0)) / zoomY;
      if (!Number.isFinite(centerX) || !Number.isFinite(centerY)) return null;
      return {
        zoom,
        centerX,
        centerY,
        imageWidth: state.width,
        imageHeight: state.height,
      };
    }

    const view = getViewRect?.();
    if (!view) return null;
    const centerX = view.viewX + view.viewW / 2;
    const centerY = view.viewY + view.viewH / 2;
    if (!Number.isFinite(centerX) || !Number.isFinite(centerY)) return null;
    return {
      zoom,
      centerX,
      centerY,
      imageWidth: state.width,
      imageHeight: state.height,
    };
  }

  function readContrast() {
    if (!state.hasFrame) return null;
    const min = Number(state.min);
    const max = Number(state.max);
    if (!Number.isFinite(min) || !Number.isFinite(max) || max < min) return null;
    const colormap = typeof state.colormap === "string" ? state.colormap : "";
    return {
      autoScale: Boolean(state.autoScale),
      min,
      max,
      colormap,
      invert: Boolean(state.invert),
    };
  }

  function readRoi() {
    if (!roiState) return null;
    const mode = ROI_MODES.has(roiState.mode) ? roiState.mode : "line";
    const start = normalizePoint(roiState.start);
    const end = normalizePoint(roiState.end);
    const active = Boolean(roiState.active && start && end);
    return {
      enabled: roiState.enabled !== false,
      mode,
      active,
      start: active ? start : null,
      end: active ? end : null,
      innerRadius: Math.max(0, Math.round(Number(roiState.innerRadius) || 0)),
      outerRadius: Math.max(0, Math.round(Number(roiState.outerRadius) || 0)),
    };
  }

  function markPublished(kind) {
    const queue = publishQueues[kind];
    if (queue) {
      queue.lastTs = Date.now();
    }
  }

  function publishViewport(reason = "change") {
    const current = syncState();
    if (!current.enabled || current.viewport === false || applyingRemote) return false;
    const viewport = readViewport();
    if (!viewport) return false;
    const sent = postMessage({
      type: MESSAGE_VIEWPORT,
      reason,
      viewport,
    });
    if (sent) {
      markPublished("viewport");
    }
    return sent;
  }

  function publishContrast(reason = "change", { force = false } = {}) {
    const current = syncState();
    if (!current.enabled || current.contrast !== true || applyingRemote) return false;
    const contrast = readContrast();
    if (!contrast) return false;
    const signature = payloadSignature(contrast);
    if (!force && signature === lastContrastSignature) return false;
    const sent = postMessage({
      type: MESSAGE_CONTRAST,
      reason,
      contrast,
    });
    if (sent) {
      markPublished("contrast");
      lastContrastSignature = signature;
    }
    return sent;
  }

  function publishRoi(reason = "change", { force = false } = {}) {
    const current = syncState();
    if (!current.enabled || current.roi !== true || applyingRemote) return false;
    const roi = readRoi();
    if (!roi) return false;
    const signature = payloadSignature(roi);
    if (!force && signature === lastRoiSignature) return false;
    const sent = postMessage({
      type: MESSAGE_ROI,
      reason,
      roi,
    });
    if (sent) {
      markPublished("roi");
      lastRoiSignature = signature;
    }
    return sent;
  }

  function frameLinked() {
    const current = syncState();
    return Boolean(current.enabled && current.frame === true);
  }

  function publishFrame(reason = "change") {
    if (!frameLinked()) return false;
    const index = Math.max(0, Math.floor(Number(state.frameIndex) || 0));
    const previousIndex = framePreviousIndex ?? index;
    framePreviousIndex = null;
    const sent = postMessage({
      type: MESSAGE_FRAME,
      reason,
      frame: { index, previousIndex, playing: Boolean(state.playing), wrap: reason === "stop" },
    });
    if (sent) {
      markPublished("frame");
    }
    return sent;
  }

  function clearPublishTimer(kind) {
    const queue = publishQueues[kind];
    if (!queue?.timer) return;
    windowObj.clearTimeout(queue.timer);
    queue.timer = null;
  }

  function cancelQueuedPublish(kind) {
    const queue = publishQueues[kind];
    if (!queue) return;
    clearPublishTimer(kind);
    queue.reason = "";
  }

  function cancelAllQueuedPublishes() {
    Object.keys(publishQueues).forEach(cancelQueuedPublish);
  }

  function flushQueuedPublish(kind, publishFn) {
    const queue = publishQueues[kind];
    if (!queue) return;
    queue.timer = null;
    const reason = queue.reason || "change";
    queue.reason = "";
    publishFn(reason);
  }

  function scheduleThrottledPublish(kind, reason, publishFn) {
    const queue = publishQueues[kind];
    if (!queue) return;
    queue.reason = reason || "change";
    const elapsedMs = queue.lastTs > 0 ? Date.now() - queue.lastTs : Infinity;
    if (elapsedMs >= publishIntervalMs) {
      clearPublishTimer(kind);
      flushQueuedPublish(kind, publishFn);
      return;
    }
    if (queue.timer) return;
    queue.timer = windowObj.setTimeout(
      () => flushQueuedPublish(kind, publishFn),
      publishIntervalMs - elapsedMs,
    );
  }

  function handleViewportChanged(reason = "change") {
    const current = syncState();
    if (!current.enabled || current.viewport === false || applyingRemote) return;
    scheduleThrottledPublish("viewport", reason, publishViewport);
  }

  function handleContrastChanged(reason = "change") {
    const current = syncState();
    if (!current.enabled || current.contrast !== true || applyingRemote) return;
    scheduleThrottledPublish("contrast", reason, publishContrast);
  }

  function handleRoiChanged(reason = "change") {
    const current = syncState();
    if (!current.enabled || current.roi !== true || applyingRemote) return;
    scheduleThrottledPublish("roi", reason, publishRoi);
  }

  /**
   * The frame or the playback state changed here, by the user or by this
   * window's own playback. `previousIndex` is the frame before the change.
   */
  function handleFrameChanged(previousIndex, reason = "change") {
    if (!frameLinked()) return;
    if (framePreviousIndex === null && Number.isInteger(previousIndex)) {
      framePreviousIndex = previousIndex;
    }
    scheduleThrottledPublish("frame", reason, publishFrame);
  }

  /**
   * Put the view on an image-space centre at a zoom, whatever size this window
   * is. Separated from applyRemoteViewport because duplicating a window needs
   * exactly this and must not be gated on the link control being switched on:
   * a clone starts independent. One copy of the centre-to-scroll arithmetic,
   * shared by both callers.
   */
  function applyViewportCenter(rawViewport) {
    if (!state.hasFrame || !state.width || !state.height) return false;
    const viewport = normalizeViewportPayload(rawViewport);
    if (!viewport) return false;

    const centerX = viewport.centerX;
    const centerY = viewport.centerY;
    const viewWidth = Math.max(0, Number(canvasWrap?.clientWidth) || 0);
    const viewHeight = Math.max(0, Number(canvasWrap?.clientHeight) || 0);

    cancelQueuedPublish("viewport");
    applyingRemote = true;
    try {
      if (core?.overviewViewport?.setViewCenter) {
        core.overviewViewport.setViewCenter(centerX, centerY, viewport.zoom);
        return true;
      }
      core.overviewViewport?.setZoom(viewport.zoom);
      const zoom = Number.isFinite(state.zoom) ? state.zoom : viewport.zoom;
      const zoomY = zoom * (state.pixelAspect || 1);
      const targetX = centerX * zoom - viewWidth / 2 + (state.renderOffsetX || 0);
      const targetY = centerY * zoomY - viewHeight / 2 + (state.renderOffsetY || 0);
      core.overviewViewport?.setEffectiveScroll(targetX, targetY, true);
    } finally {
      applyingRemote = false;
    }
    return true;
  }

  function applyRemoteViewport(rawViewport) {
    const current = syncState();
    if (!current.enabled || current.viewport === false) return false;
    return applyViewportCenter(rawViewport);
  }

  function applyRemoteContrast(rawContrast) {
    const current = syncState();
    if (!current.enabled || current.contrast !== true) return false;
    if (typeof applySyncedContrast !== "function") return false;
    const contrast = normalizeContrastPayload(rawContrast);
    if (!contrast) return false;

    cancelQueuedPublish("contrast");
    applyingRemote = true;
    try {
      const applied = applySyncedContrast(contrast);
      if (applied === false) return false;
      lastContrastSignature = payloadSignature(contrast);
    } finally {
      applyingRemote = false;
    }
    return true;
  }

  function applyRemoteRoi(rawRoi) {
    const current = syncState();
    if (!current.enabled || current.roi !== true) return false;
    if (typeof applySyncedRoi !== "function") return false;
    const roi = normalizeRoiPayload(rawRoi);
    if (!roi) return false;

    cancelQueuedPublish("roi");
    applyingRemote = true;
    try {
      const applied = applySyncedRoi(roi);
      if (applied === false) return false;
      lastRoiSignature = payloadSignature(roi);
    } finally {
      applyingRemote = false;
    }
    return true;
  }

  function applyRemoteFrame(rawFrame, senderId) {
    if (!frameLinked()) return false;
    const frame = normalizeFramePayload(rawFrame);
    const playback = core?.framePlayback;
    if (!frame || !playback?.followFrame) return false;
    if (!frameOffsets.has(senderId)) {
      frameOffsets.set(senderId, frameOffsetFor(state.frameIndex, frame));
    }
    const target = followFrameTarget({
      index: frame.index,
      offset: frameOffsets.get(senderId),
      frameCount: state.frameCount,
      wrap: frame.wrap,
    });
    cancelQueuedPublish("frame");
    framePreviousIndex = null;
    playback.followFrame(target, { playing: frame.playing });
    return true;
  }

  function requestRemoteViewport() {
    const current = syncState();
    if (!current.enabled || current.viewport === false) return false;
    return postMessage({
      type: MESSAGE_REQUEST_VIEWPORT,
    });
  }

  function requestRemoteContrast() {
    const current = syncState();
    if (!current.enabled || current.contrast !== true) return false;
    return postMessage({
      type: MESSAGE_REQUEST_CONTRAST,
    });
  }

  function requestRemoteRoi() {
    const current = syncState();
    if (!current.enabled || current.roi !== true) return false;
    return postMessage({
      type: MESSAGE_REQUEST_ROI,
    });
  }

  function requestRemoteState() {
    requestRemoteViewport();
    requestRemoteContrast();
    requestRemoteRoi();
  }

  function handleChannelMessage(event) {
    const data = event?.data;
    if (!data || typeof data !== "object") return;
    if (data.sourceId === sourceId) return;
    if (data.group !== groupName()) return;
    if (data.type === MESSAGE_REQUEST_VIEWPORT) {
      publishViewport("request");
      return;
    }
    if (data.type === MESSAGE_REQUEST_CONTRAST) {
      publishContrast("request", { force: true });
      return;
    }
    if (data.type === MESSAGE_REQUEST_ROI) {
      publishRoi("request", { force: true });
      return;
    }
    if (data.type === MESSAGE_VIEWPORT) {
      applyRemoteViewport(data.viewport);
      return;
    }
    if (data.type === MESSAGE_CONTRAST) {
      applyRemoteContrast(data.contrast);
      return;
    }
    if (data.type === MESSAGE_ROI) {
      applyRemoteRoi(data.roi);
      return;
    }
    if (data.type === MESSAGE_FRAME) {
      applyRemoteFrame(data.frame, data.sourceId);
    }
  }

  function setOptionsOpen(open) {
    optionsOpen = Boolean(open);
    syncWrap?.classList.toggle("is-open", optionsOpen);
    syncOptionsToggle?.setAttribute("aria-expanded", optionsOpen ? "true" : "false");
    syncPopover?.setAttribute("aria-hidden", optionsOpen ? "false" : "true");
  }

  function toggleOptionsOpen(event) {
    event?.stopPropagation?.();
    setOptionsOpen(!optionsOpen);
  }

  function stopOptionsClick(event) {
    event?.stopPropagation?.();
  }

  function closeOptionsOnOutsideClick(event) {
    if (!optionsOpen) return;
    const target = event?.target;
    if (target && syncWrap?.contains?.(target)) return;
    setOptionsOpen(false);
  }

  function closeOptionsOnEscape(event) {
    if (event?.key === "Escape") {
      setOptionsOpen(false);
    }
  }

  function refreshOptionCheckbox(checkbox, enabled, disabled) {
    if (!checkbox) return;
    checkbox.checked = Boolean(enabled);
    checkbox.disabled = Boolean(disabled);
  }

  function refreshUi() {
    const current = syncState();
    const enabled = Boolean(current.enabled);
    const labelKey = enabled ? "toolbar.viewer_sync.disable" : "toolbar.viewer_sync.enable";
    syncWrap?.classList.toggle("is-active", enabled);
    if (syncToggle) {
      syncToggle.classList.toggle("is-active", enabled);
      syncToggle.disabled = !available;
      syncToggle.dataset.i18nAriaLabel = labelKey;
      syncToggle.setAttribute("aria-pressed", enabled ? "true" : "false");
      syncToggle.setAttribute("aria-label", t(labelKey));
    }
    if (syncOptionsToggle) {
      syncOptionsToggle.disabled = !available;
      syncOptionsToggle.setAttribute("aria-label", t("toolbar.viewer_sync.options"));
      syncOptionsToggle.dataset.i18nAriaLabel = "toolbar.viewer_sync.options";
      syncOptionsToggle.setAttribute("aria-expanded", optionsOpen ? "true" : "false");
    }
    refreshOptionCheckbox(syncViewportToggle, current.viewport !== false, !available);
    refreshOptionCheckbox(syncContrastToggle, current.contrast === true, !available);
    refreshOptionCheckbox(syncRoiToggle, current.roi === true, !available);
    refreshOptionCheckbox(syncFrameToggle, current.frame === true, !available);
  }

  function setEnabled(enabled) {
    const current = syncState();
    const next = Boolean(enabled);
    if (current.enabled === next) {
      refreshUi();
      return current.enabled;
    }
    current.enabled = next;
    cancelAllQueuedPublishes();
    resetFrameLink();
    if (next) {
      if (!openChannel()) {
        current.enabled = false;
        refreshUi();
        return false;
      }
      requestRemoteState();
    } else {
      closeChannel();
    }
    refreshUi();
    return current.enabled;
  }

  // Offsets are learned again after linking: the gap between the windows at
  // that moment is the one kept.
  function resetFrameLink() {
    frameOffsets.clear();
    framePreviousIndex = null;
  }

  function toggleEnabled() {
    return setEnabled(!syncState().enabled);
  }

  function setSyncOption(option, enabled) {
    if (!SYNC_OPTIONS.has(option)) return false;
    const current = syncState();
    const next = Boolean(enabled);
    current[option] = next;
    cancelQueuedPublish(option);
    if (option === "frame") resetFrameLink();
    refreshUi();
    if (!current.enabled || !next) return next;
    if (option === "viewport") {
      requestRemoteViewport();
    } else if (option === "contrast") {
      requestRemoteContrast();
    } else if (option === "roi") {
      requestRemoteRoi();
    }
    return next;
  }

  function handleOptionCheckboxChange(event) {
    const target = event?.currentTarget || event?.target;
    if (!target) return;
    if (target === syncViewportToggle) {
      setSyncOption("viewport", target.checked);
    } else if (target === syncContrastToggle) {
      setSyncOption("contrast", target.checked);
    } else if (target === syncRoiToggle) {
      setSyncOption("roi", target.checked);
    } else if (target === syncFrameToggle) {
      setSyncOption("frame", target.checked);
    }
  }

  function destroy() {
    cancelAllQueuedPublishes();
    closeChannel();
    setOptionsOpen(false);
    syncToggle?.removeEventListener("click", toggleEnabled);
    syncOptionsToggle?.removeEventListener("click", toggleOptionsOpen);
    syncPopover?.removeEventListener("click", stopOptionsClick);
    syncViewportToggle?.removeEventListener("change", handleOptionCheckboxChange);
    syncContrastToggle?.removeEventListener("change", handleOptionCheckboxChange);
    syncRoiToggle?.removeEventListener("change", handleOptionCheckboxChange);
    syncFrameToggle?.removeEventListener("change", handleOptionCheckboxChange);
    documentObj?.removeEventListener("click", closeOptionsOnOutsideClick);
    documentObj?.removeEventListener("keydown", closeOptionsOnEscape);
  }

  syncToggle?.addEventListener("click", toggleEnabled);
  syncOptionsToggle?.addEventListener("click", toggleOptionsOpen);
  syncPopover?.addEventListener("click", stopOptionsClick);
  syncViewportToggle?.addEventListener("change", handleOptionCheckboxChange);
  syncContrastToggle?.addEventListener("change", handleOptionCheckboxChange);
  syncRoiToggle?.addEventListener("change", handleOptionCheckboxChange);
  syncFrameToggle?.addEventListener("change", handleOptionCheckboxChange);
  documentObj?.addEventListener("click", closeOptionsOnOutsideClick);
  documentObj?.addEventListener("keydown", closeOptionsOnEscape);
  if (syncState().enabled) {
    openChannel();
  }
  refreshUi();

  return {
    // Exported so the window-duplication snapshot and the link control agree
    // on what "the current view" is. Two copies of this arithmetic would drift,
    // and the image-space centre it returns is precisely what survives being
    // reopened in a window of a different size.
    readViewport,
    applyViewportCenter,
    handleViewportChanged,
    handleContrastChanged,
    handleRoiChanged,
    handleFrameChanged,
    publishViewport,
    publishContrast,
    publishRoi,
    applyRemoteViewport,
    applyRemoteContrast,
    applyRemoteRoi,
    applyRemoteFrame,
    requestRemoteViewport,
    requestRemoteContrast,
    requestRemoteRoi,
    setSyncOption,
    setEnabled,
    isEnabled: () => Boolean(syncState().enabled),
    refreshUi,
    destroy,
  };
}
