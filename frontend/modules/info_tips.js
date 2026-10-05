/**
 * "?" buttons that explain an option where it is.
 *
 * The hover hints of help_tooltips.js are off by default (`ui.tool_hints`) and
 * describe what a control does in a line. Some options need more than that and
 * should not depend on a setting: what the series normalizations divide by, what
 * the masks hide. Those carry a small "?" (`.info-tip[data-info-key]`) that shows
 * the explanation on hover or keyboard focus, keeps it on a click or tap until
 * dismissed, and closes on Escape. It takes no room until asked.
 *
 * The text is looked up when shown, so it follows the interface language
 * without a refresh. The button's own name comes from `data-i18n-aria-label`.
 */

import { t } from "./i18n.js";

const BUBBLE_ID = "info-tip-bubble";
const MARGIN_PX = 8;

export function createInfoTips({ root = document } = {}) {
  let bubble = null;
  let active = null;
  // Opened by a click or tap: stays until dismissed, instead of following the
  // pointer, which a touch screen does not have.
  let pinned = false;

  function ensureBubble() {
    if (bubble) return bubble;
    bubble = document.createElement("div");
    bubble.id = BUBBLE_ID;
    bubble.className = "info-tip-bubble";
    bubble.setAttribute("role", "tooltip");
    document.body.appendChild(bubble);
    return bubble;
  }

  function position(button) {
    const rect = button.getBoundingClientRect();
    const width = bubble.offsetWidth;
    const height = bubble.offsetHeight;
    const viewW = window.innerWidth || document.documentElement.clientWidth;
    const viewH = window.innerHeight || document.documentElement.clientHeight;
    let left = rect.left + rect.width / 2 - width / 2;
    left = Math.max(MARGIN_PX, Math.min(left, viewW - width - MARGIN_PX));
    let top = rect.bottom + 6;
    if (top + height > viewH - MARGIN_PX) top = Math.max(MARGIN_PX, rect.top - height - 6);
    bubble.style.left = `${Math.round(left)}px`;
    bubble.style.top = `${Math.round(top)}px`;
  }

  function show(button, { pin = false } = {}) {
    ensureBubble();
    if (active && active !== button) active.setAttribute("aria-expanded", "false");
    active = button;
    pinned = pin;
    bubble.textContent = t(button.dataset.infoKey);
    bubble.classList.add("is-visible");
    button.setAttribute("aria-expanded", "true");
    button.setAttribute("aria-describedby", BUBBLE_ID);
    position(button);
  }

  function hide() {
    if (!bubble || !active) return;
    bubble.classList.remove("is-visible");
    active.setAttribute("aria-expanded", "false");
    active.removeAttribute("aria-describedby");
    active = null;
    pinned = false;
  }

  function bind(button) {
    if (button.dataset.infoBound) return;
    button.dataset.infoBound = "true";
    button.setAttribute("aria-expanded", "false");
    button.addEventListener("mouseenter", () => {
      if (!pinned) show(button);
    });
    button.addEventListener("mouseleave", () => {
      if (!pinned && active === button) hide();
    });
    button.addEventListener("focus", () => {
      if (!pinned) show(button);
    });
    button.addEventListener("blur", () => {
      if (!pinned && active === button) hide();
    });
    button.addEventListener("click", (event) => {
      // Inside a <label>: the click is for the explanation, not the control.
      event.preventDefault();
      event.stopPropagation();
      if (pinned && active === button) {
        hide();
      } else {
        show(button, { pin: true });
      }
    });
  }

  function refresh() {
    root.querySelectorAll(".info-tip[data-info-key]").forEach(bind);
    if (active && bubble) bubble.textContent = t(active.dataset.infoKey);
  }

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && active) hide();
  });
  document.addEventListener("click", (event) => {
    if (pinned && active && !active.contains(event.target)) hide();
  });
  // A fixed bubble would drift away from its button.
  window.addEventListener("resize", hide);
  document.addEventListener("scroll", hide, true);

  refresh();
  return { refresh, hide };
}
