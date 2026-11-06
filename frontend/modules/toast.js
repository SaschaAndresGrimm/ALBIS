/**
 * Lightweight toast / notification system for ALBIS.
 *
 * Complements the footer status pill (`setStatus`): the footer carries ambient,
 * transient state, while toasts surface things the user must actively notice
 * (errors, warnings, and confirmations that would otherwise scroll past).
 *
 * Singleton, imported directly like `i18n.t` so any module can call
 * `notifyError(...)` without threading a callback through every controller.
 */

import { t } from "./i18n.js";

const REGION_ID = "toast-region";
const MAX_VISIBLE = 4;

// Auto-dismiss delays per tone, in milliseconds. 0 means "sticky" (manual close
// only). Errors linger longest so they are not missed.
const DEFAULT_DURATIONS = {
  info: 5000,
  success: 4000,
  warning: 7000,
  error: 10000,
};

const VALID_TYPES = new Set(["info", "success", "warning", "error"]);

// Tones that should interrupt assistive tech (role="alert") vs. announce
// politely when idle (role="status").
const ASSERTIVE_TYPES = new Set(["warning", "error"]);

// Currently visible toasts, oldest first. Each entry: { el, key, timerId, ... }.
const liveToasts = [];

// The region's gap to the window's edges, and above its anchor.
const EDGE_GAP_PX = 16;
const ANCHOR_GAP_PX = 8;

// Decides whether a toast of a tone may show now (setToastGate).
let toastGate = null;

// What toasts stand above: the viewer's footer, and what opens up from it
// (the Versions popover).
let anchorEls = [];
let listeningForResize = false;

/**
 * How far above the window's bottom edge the toasts start: clear of the
 * anchor's top edge when the anchor is shown and reaches into the bottom of
 * the window, the plain edge gap otherwise.
 */
export function toastBottomOffset(viewportHeight, anchorRect) {
  if (!anchorRect || !(anchorRect.height > 0) || !(viewportHeight > 0)) return EDGE_GAP_PX;
  const clearance = viewportHeight - anchorRect.top + ANCHOR_GAP_PX;
  return Math.max(EDGE_GAP_PX, Math.round(clearance));
}

// A closed popover keeps its box (it fades out with visibility, which lags
// the class by the transition), so it says it is closed with aria-hidden.
function isShown(el) {
  return Boolean(el?.isConnected) && !el.hidden && el.getAttribute("aria-hidden") !== "true";
}

// The highest of the shown anchors, which is the one toasts must clear.
function topmostAnchorRect() {
  let best = null;
  for (const el of anchorEls) {
    if (!isShown(el)) continue;
    const rect = el.getBoundingClientRect();
    if (!(rect.height > 0)) continue;
    if (!best || rect.top < best.top) best = rect;
  }
  return best;
}

function placeRegion(region) {
  if (!region || typeof window === "undefined") return;
  region.style.setProperty("--toast-bottom", `${toastBottomOffset(window.innerHeight, topmostAnchorRect())}px`);
}

/**
 * Keep toasts above the given elements instead of over them: the viewer's
 * footer, where they used to hide the server badge, Versions and GitHub, and
 * the Versions popover, which opens upward from it and was drawn under them.
 * Measured when a toast is shown, when the window is resized (the footer can
 * wrap), and on refreshToastPlacement().
 */
export function anchorToastsAbove(...elements) {
  anchorEls = elements.filter(Boolean);
  if (!listeningForResize && typeof window !== "undefined") {
    listeningForResize = true;
    window.addEventListener("resize", refreshToastPlacement);
  }
  refreshToastPlacement();
}

/** Measure the anchors again: call after one opens or closes. */
export function refreshToastPlacement() {
  if (typeof document === "undefined") return;
  placeRegion(document.getElementById(REGION_ID));
}

function getRegion() {
  if (typeof document === "undefined") return null;
  let region = document.getElementById(REGION_ID);
  if (!region) {
    region = document.createElement("div");
    region.id = REGION_ID;
    region.className = "toast-region";
    region.setAttribute("role", "region");
    region.setAttribute("aria-label", t("toast.region_label"));
    document.body.appendChild(region);
  }
  return region;
}

function normalizeType(type) {
  return VALID_TYPES.has(type) ? type : "info";
}

function clearTimer(entry) {
  if (entry.timerId !== null) {
    window.clearTimeout(entry.timerId);
    entry.timerId = null;
  }
}

function removeToast(entry) {
  const index = liveToasts.indexOf(entry);
  if (index === -1) return;
  liveToasts.splice(index, 1);
  clearTimer(entry);
  const { el } = entry;
  el.classList.remove("is-visible");
  el.classList.add("is-leaving");
  // Fall back to immediate removal if the transitionend never fires (e.g. the
  // element is detached, or motion is reduced to ~0ms).
  let removed = false;
  const finalize = () => {
    if (removed) return;
    removed = true;
    el.remove();
  };
  el.addEventListener("transitionend", finalize, { once: true });
  window.setTimeout(finalize, 400);
}

function scheduleDismiss(entry) {
  clearTimer(entry);
  if (!entry.duration) return; // sticky
  entry.timerId = window.setTimeout(() => removeToast(entry), entry.duration);
}

function renderDetail(entry, detail) {
  const text = String(detail || "").trim();
  if (!text) {
    entry.detailEl?.remove();
    entry.detailEl = null;
    return;
  }
  if (!entry.detailEl) {
    entry.detailEl = document.createElement("div");
    entry.detailEl.className = "toast-detail";
    entry.bodyEl.appendChild(entry.detailEl);
  }
  entry.detailEl.textContent = text;
  // Clamped to three lines; the whole of it on hover.
  entry.detailEl.title = text;
}

function renderCount(entry) {
  if (entry.count < 2) return;
  if (!entry.countEl) {
    entry.countEl = document.createElement("span");
    entry.countEl.className = "toast-count";
    entry.messageEl.appendChild(entry.countEl);
  }
  entry.countEl.textContent = `×${entry.count}`;
  const label = t("toast.repeated", { count: entry.count });
  entry.countEl.title = label;
  entry.countEl.setAttribute("aria-label", label);
}

function renderAction(entry, action) {
  entry.action = action && action.label && typeof action.onClick === "function" ? action : null;
  if (!entry.action) {
    entry.actionsEl?.remove();
    entry.actionsEl = null;
    return;
  }
  if (!entry.actionsEl) {
    entry.actionsEl = document.createElement("div");
    entry.actionsEl.className = "toast-actions";
    const button = document.createElement("button");
    button.type = "button";
    button.className = "btn btn-secondary toast-action";
    // The handler of the latest report, so Retry retries what failed last.
    button.addEventListener("click", () => {
      const current = entry.action;
      removeToast(entry);
      current?.onClick();
    });
    entry.actionsEl.appendChild(button);
    entry.bodyEl.appendChild(entry.actionsEl);
  }
  entry.actionsEl.firstChild.textContent = entry.action.label;
}

/**
 * Show a toast notification.
 *
 * The same message again, while the first is still showing, does not stack a
 * second toast: it counts up on the first (×2, ×3, …), takes the newer detail
 * and action, and restarts its timer, so a failure that keeps happening during
 * playback reads as one toast that is still current.
 *
 * @param {string} message Already-translated, user-facing message.
 * @param {{type?: string, duration?: number, dismissible?: boolean,
 *   detail?: string, action?: {label: string, onClick: () => void}}} [options]
 *   `detail` is a second, smaller line with the cause (already translated);
 *   `action` adds one button, which closes the toast and runs `onClick`.
 * @returns {{ dismiss: () => void }}
 */
export function showToast(message, options = {}) {
  const text = String(message || "").trim();
  if (!text) return { dismiss: () => {} };

  const type = normalizeType(options.type);
  if (toastGate && !toastGate(type)) return { dismiss: () => {} };

  const region = getRegion();
  if (!region) return { dismiss: () => {} };

  const duration = Number.isFinite(options.duration)
    ? Math.max(0, options.duration)
    : DEFAULT_DURATIONS[type];
  const dismissible = options.dismissible !== false;
  const key = `${type}::${text}`;

  const existing = liveToasts.find((entry) => entry.key === key);
  if (existing) {
    existing.count += 1;
    renderCount(existing);
    if ("detail" in options) renderDetail(existing, options.detail);
    if ("action" in options) renderAction(existing, options.action);
    scheduleDismiss(existing);
    return { dismiss: () => removeToast(existing) };
  }

  const el = document.createElement("div");
  el.className = `toast toast-${type}`;
  el.setAttribute("role", ASSERTIVE_TYPES.has(type) ? "alert" : "status");

  const body = document.createElement("div");
  body.className = "toast-body";
  const messageEl = document.createElement("div");
  messageEl.className = "toast-message";
  messageEl.textContent = text;
  body.appendChild(messageEl);
  el.appendChild(body);

  const entry = {
    el,
    key,
    type,
    timerId: null,
    duration,
    count: 1,
    bodyEl: body,
    messageEl,
    detailEl: null,
    countEl: null,
    actionsEl: null,
    action: null,
  };
  renderDetail(entry, options.detail);
  renderAction(entry, options.action);

  if (dismissible) {
    const closeBtn = document.createElement("button");
    closeBtn.type = "button";
    closeBtn.className = "toast-close";
    closeBtn.setAttribute("aria-label", t("common.close"));
    closeBtn.textContent = "×";
    closeBtn.addEventListener("click", () => removeToast(entry));
    el.appendChild(closeBtn);
  }

  // Pause auto-dismiss while the user is reading (hover or keyboard focus).
  el.addEventListener("mouseenter", () => clearTimer(entry));
  el.addEventListener("mouseleave", () => scheduleDismiss(entry));
  el.addEventListener("focusin", () => clearTimer(entry));
  el.addEventListener("focusout", () => scheduleDismiss(entry));

  placeRegion(region);
  region.appendChild(el);
  liveToasts.push(entry);

  // Trim the oldest when we exceed the cap.
  while (liveToasts.length > MAX_VISIBLE) {
    removeToast(liveToasts[0]);
  }

  // Trigger the enter transition on the next frame.
  window.requestAnimationFrame(() => {
    el.classList.add("is-visible");
  });

  scheduleDismiss(entry);
  return { dismiss: () => removeToast(entry) };
}

export function notifyInfo(message, options = {}) {
  return showToast(message, { ...options, type: "info" });
}

export function notifySuccess(message, options = {}) {
  return showToast(message, { ...options, type: "success" });
}

export function notifyWarning(message, options = {}) {
  return showToast(message, { ...options, type: "warning" });
}

export function notifyError(message, options = {}) {
  return showToast(message, { ...options, type: "error" });
}

export function dismissAllToasts() {
  [...liveToasts].forEach((entry) => removeToast(entry));
}

/** Close every toast of one tone, e.g. the errors a lost connection caused. */
export function dismissToasts(type) {
  liveToasts.filter((entry) => entry.type === type).forEach((entry) => removeToast(entry));
}

/**
 * Hold toasts back while `gate(type)` returns false; `null` lets all through.
 * While the server is unreachable, every request fails, and one notice that
 * says so beats a pile of "failed to load" toasts that each blame the request.
 */
export function setToastGate(gate) {
  toastGate = typeof gate === "function" ? gate : null;
}
