/**
 * Cross-window viewer synchronization for image-space viewport state.
 */

import { t } from "./i18n.js";

const DEFAULT_CHANNEL_NAME = "albis.viewerSync";
const DEFAULT_GROUP = "default";
const MESSAGE_VIEWPORT = "albis.viewerSync.viewport";
const MESSAGE_REQUEST_VIEWPORT = "albis.viewerSync.requestViewport";

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
      contrast: false,
      roi: false,
    };
  }
  if (!state.viewerSync.group) {
    state.viewerSync.group = DEFAULT_GROUP;
  }
  if (state.viewerSync.viewport !== false) {
    state.viewerSync.viewport = true;
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

export function createViewerSyncController({
  state,
  elements,
  callbacks,
  options = {},
}) {
  const {
    syncToggle,
    canvasWrap,
  } = elements;

  const {
    getViewRect,
    getEffectiveScrollLeft,
    getEffectiveScrollTop,
    setZoom,
    setEffectiveScroll,
  } = callbacks;

  const windowObj = options.windowObj || window;
  const channelName = options.channelName || DEFAULT_CHANNEL_NAME;
  const sourceId = options.sourceId || createSourceId(windowObj);
  const rawPublishDelayMs = Number(options.publishDelayMs);
  const publishDelayMs = Number.isFinite(rawPublishDelayMs) ? Math.max(0, rawPublishDelayMs) : 60;
  const createChannel = options.createChannel || ((name) => {
    if (typeof windowObj.BroadcastChannel !== "function") return null;
    return new windowObj.BroadcastChannel(name);
  });

  let channel = null;
  let publishTimer = null;
  let applyingRemote = false;
  let available = Boolean(options.createChannel || typeof windowObj.BroadcastChannel === "function");

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
      const centerX = (effectiveLeft + viewWidth / 2 - (state.renderOffsetX || 0)) / zoom;
      const centerY = (effectiveTop + viewHeight / 2 - (state.renderOffsetY || 0)) / zoom;
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

  function publishViewport(reason = "change") {
    const current = syncState();
    if (!current.enabled || current.viewport === false || applyingRemote) return false;
    const viewport = readViewport();
    if (!viewport) return false;
    return postMessage({
      type: MESSAGE_VIEWPORT,
      reason,
      viewport,
    });
  }

  function clearPublishTimer() {
    if (!publishTimer) return;
    windowObj.clearTimeout(publishTimer);
    publishTimer = null;
  }

  function handleViewportChanged(reason = "change") {
    const current = syncState();
    if (!current.enabled || current.viewport === false || applyingRemote) return;
    clearPublishTimer();
    publishTimer = windowObj.setTimeout(() => {
      publishTimer = null;
      publishViewport(reason);
    }, publishDelayMs);
  }

  function applyRemoteViewport(rawViewport) {
    const current = syncState();
    if (!current.enabled || current.viewport === false) return false;
    if (!state.hasFrame || !state.width || !state.height) return false;
    const viewport = normalizeViewportPayload(rawViewport);
    if (!viewport) return false;

    const centerX = viewport.centerX;
    const centerY = viewport.centerY;
    const viewWidth = Math.max(0, Number(canvasWrap?.clientWidth) || 0);
    const viewHeight = Math.max(0, Number(canvasWrap?.clientHeight) || 0);

    clearPublishTimer();
    applyingRemote = true;
    try {
      setZoom?.(viewport.zoom);
      const zoom = Number.isFinite(state.zoom) ? state.zoom : viewport.zoom;
      const targetX = centerX * zoom - viewWidth / 2 + (state.renderOffsetX || 0);
      const targetY = centerY * zoom - viewHeight / 2 + (state.renderOffsetY || 0);
      setEffectiveScroll?.(targetX, targetY, true);
    } finally {
      applyingRemote = false;
    }
    return true;
  }

  function requestRemoteViewport() {
    if (!syncState().enabled) return false;
    return postMessage({
      type: MESSAGE_REQUEST_VIEWPORT,
    });
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
    if (data.type === MESSAGE_VIEWPORT) {
      applyRemoteViewport(data.viewport);
    }
  }

  function refreshUi() {
    const current = syncState();
    const enabled = Boolean(current.enabled);
    if (!syncToggle) return;
    const labelKey = enabled ? "toolbar.viewer_sync.disable" : "toolbar.viewer_sync.enable";
    syncToggle.classList.toggle("is-active", enabled);
    syncToggle.disabled = !available;
    syncToggle.dataset.i18nAriaLabel = labelKey;
    syncToggle.setAttribute("aria-pressed", enabled ? "true" : "false");
    syncToggle.setAttribute("aria-label", t(labelKey));
  }

  function setEnabled(enabled) {
    const current = syncState();
    const next = Boolean(enabled);
    if (current.enabled === next) {
      refreshUi();
      return current.enabled;
    }
    current.enabled = next;
    clearPublishTimer();
    if (next) {
      if (!openChannel()) {
        current.enabled = false;
        refreshUi();
        return false;
      }
      requestRemoteViewport();
    } else {
      closeChannel();
    }
    refreshUi();
    return current.enabled;
  }

  function toggleEnabled() {
    return setEnabled(!syncState().enabled);
  }

  function destroy() {
    clearPublishTimer();
    closeChannel();
    syncToggle?.removeEventListener("click", toggleEnabled);
  }

  syncToggle?.addEventListener("click", toggleEnabled);
  if (syncState().enabled) {
    openChannel();
  }
  refreshUi();

  return {
    handleViewportChanged,
    publishViewport,
    applyRemoteViewport,
    requestRemoteViewport,
    setEnabled,
    isEnabled: () => Boolean(syncState().enabled),
    refreshUi,
    destroy,
  };
}
