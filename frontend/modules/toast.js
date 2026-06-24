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

/**
 * Show a toast notification.
 *
 * @param {string} message Already-translated, user-facing message.
 * @param {{type?: string, duration?: number, dismissible?: boolean}} [options]
 * @returns {{ dismiss: () => void }}
 */
export function showToast(message, options = {}) {
  const text = String(message || "").trim();
  if (!text) return { dismiss: () => {} };

  const region = getRegion();
  if (!region) return { dismiss: () => {} };

  const type = normalizeType(options.type);
  const duration = Number.isFinite(options.duration)
    ? Math.max(0, options.duration)
    : DEFAULT_DURATIONS[type];
  const dismissible = options.dismissible !== false;
  const key = `${type}::${text}`;

  // Dedupe: if an identical toast is already live, just refresh its timer
  // instead of stacking a near-duplicate.
  const existing = liveToasts.find((entry) => entry.key === key);
  if (existing) {
    scheduleDismiss(existing);
    return { dismiss: () => removeToast(existing) };
  }

  const el = document.createElement("div");
  el.className = `toast toast-${type}`;
  el.setAttribute("role", ASSERTIVE_TYPES.has(type) ? "alert" : "status");

  const body = document.createElement("div");
  body.className = "toast-message";
  body.textContent = text;
  el.appendChild(body);

  const entry = { el, key, timerId: null, duration };

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
