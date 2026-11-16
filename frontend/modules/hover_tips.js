/**
 * The hover tooltip: one small bubble, anchored under the control it names.
 *
 * Always on; there is no setting. It shows only what a control cannot say by
 * itself (see hover_tip_utils.js): why it is unavailable, its title, or the
 * name of an icon-only button, with its keyboard shortcut as a keycap when it
 * declares one in `aria-keyshortcuts`. Half a second the first time, then at
 * once while moving along a toolbar. Keyboard focus shows it too; a mouse
 * click does not.
 *
 * It replaces the browser's own `title` tooltip: the attribute is taken away
 * while the pointer is on the control and put back when it leaves, so nothing
 * draws two bubbles, whatever re-sets the title in between (i18n on a language
 * change, applyPanelState on every panel toggle).
 */

import { keyshortcutLabel, pickTipText, placeTip } from "./hover_tip_utils.js";

const TIP_ID = "help-tooltip";
const DELAY_MS = 500;
// Moving from one control to the next within this long shows the next at once.
const WARM_MS = 400;
const CANDIDATES = [
  "[data-help-reason]",
  "[title]",
  "[data-tip-native-title]",
  "button[aria-label]",
  '[role="button"][aria-label]',
].join(",");

export function createHoverTips({ delayMs = DELAY_MS, warmMs = WARM_MS } = {}) {
  let tip = null;
  let textEl = null;
  let keyEl = null;
  let target = null;
  let timer = null;
  let lastHiddenAt = -Infinity;
  let keyboardFocus = false;
  let previousDescribedBy = null;

  function ensureTip() {
    if (tip) return tip;
    tip = document.createElement("div");
    tip.className = "help-tooltip";
    tip.id = TIP_ID;
    tip.setAttribute("role", "tooltip");
    textEl = document.createElement("span");
    textEl.className = "help-tooltip-text";
    keyEl = document.createElement("kbd");
    keyEl.className = "help-tooltip-key";
    tip.append(textEl, keyEl);
    document.body.appendChild(tip);
    return tip;
  }

  function findTarget(node) {
    if (!node || typeof node.closest !== "function") return null;
    if (node.closest(".help-tooltip, .info-tip, [data-no-hover-help]")) return null;
    return node.closest(CANDIDATES);
  }

  function contentFor(el) {
    const isButton = el.tagName === "BUTTON" || el.getAttribute("role") === "button";
    const text = pickTipText({
      reason: el.dataset?.helpReason,
      title: el.getAttribute("title") ?? el.dataset?.tipNativeTitle,
      ariaLabel: el.getAttribute("aria-label"),
      isButton,
      visibleText: el.textContent,
    });
    // A reason is not a name: the shortcut of a control that cannot be used
    // would only invite pressing it.
    const key = el.dataset?.helpReason ? "" : keyshortcutLabel(el.getAttribute("aria-keyshortcuts"));
    return { text, key };
  }

  function suppressTitle(el) {
    const title = el.getAttribute("title");
    if (title === null) return;
    el.dataset.tipNativeTitle = title;
    el.removeAttribute("title");
  }

  function restoreTitle(el) {
    if (!el?.dataset) return;
    const stashed = el.dataset.tipNativeTitle;
    if (typeof stashed !== "string") return;
    // Only if nothing has put one back meanwhile (a language change).
    if (!el.hasAttribute("title")) el.setAttribute("title", stashed);
    delete el.dataset.tipNativeTitle;
  }

  // Named as the control's description only while it is on screen: a
  // description aimed at hidden text describes nothing.
  function describe(el) {
    const existing = (el.getAttribute("aria-describedby") || "").split(/\s+/).filter((id) => id && id !== TIP_ID);
    previousDescribedBy = existing.join(" ");
    el.setAttribute("aria-describedby", [...existing, TIP_ID].join(" "));
  }

  function undescribe(el) {
    if (!el || previousDescribedBy === null) return;
    if (previousDescribedBy) el.setAttribute("aria-describedby", previousDescribedBy);
    else el.removeAttribute("aria-describedby");
    previousDescribedBy = null;
  }

  function render() {
    if (!tip || !target) return false;
    const { text, key } = contentFor(target);
    if (!text) return false;
    textEl.textContent = text;
    keyEl.textContent = key;
    keyEl.hidden = !key;
    tip.classList.add("is-visible");
    const anchor = target.getBoundingClientRect();
    const placed = placeTip(
      anchor,
      { width: tip.offsetWidth, height: tip.offsetHeight },
      { width: window.innerWidth, height: window.innerHeight },
    );
    tip.style.left = `${placed.left}px`;
    tip.style.top = `${placed.top}px`;
    tip.dataset.side = placed.side;
    return true;
  }

  function reveal() {
    timer = null;
    if (render()) describe(target);
  }

  function hide() {
    if (timer) {
      clearTimeout(timer);
      timer = null;
    }
    if (tip?.classList.contains("is-visible")) {
      tip.classList.remove("is-visible");
      lastHiddenAt = Date.now();
    }
    if (target) {
      undescribe(target);
      restoreTitle(target);
    }
    target = null;
  }

  function show(el, { immediate = false } = {}) {
    if (el === target) return;
    const sinceHidden = Date.now() - lastHiddenAt;
    const warm = (sinceHidden >= 0 && sinceHidden < warmMs) || tip?.classList.contains("is-visible");
    hide();
    if (!contentFor(el).text) return;
    ensureTip();
    target = el;
    // Now, not when our bubble appears: the browser paints its own from the
    // title at about 500 ms and does not take it back once painted.
    suppressTitle(el);
    if (immediate || warm) reveal();
    else timer = setTimeout(reveal, delayMs);
  }

  function onOver(event) {
    if (event.pointerType === "touch") return;
    const el = findTarget(event.target);
    if (!el) {
      if (target && !target.contains(event.target)) hide();
      return;
    }
    show(el);
  }

  function onOut(event) {
    if (!target) return;
    const related = event.relatedTarget;
    if (related && target.contains(related)) return;
    hide();
  }

  function init() {
    if (tip) return;
    ensureTip();
    document.addEventListener("pointerover", onOver, true);
    document.addEventListener("mouseover", onOver, true);
    document.addEventListener("pointerout", onOut, true);
    document.addEventListener("mouseout", onOut, true);
    document.addEventListener(
      "keydown",
      (event) => {
        keyboardFocus = true;
        if (event.key === "Escape") hide();
      },
      true,
    );
    document.addEventListener(
      "pointerdown",
      () => {
        keyboardFocus = false;
        hide();
      },
      true,
    );
    document.addEventListener(
      "focusin",
      (event) => {
        if (!keyboardFocus) return;
        const el = findTarget(event.target);
        if (el) show(el, { immediate: true });
      },
      true,
    );
    document.addEventListener("focusout", () => hide(), true);
    document.addEventListener("scroll", () => hide(), true);
    window.addEventListener("resize", () => hide());
  }

  /** After a language change: the open bubble reads its control again. */
  function refresh() {
    if (tip?.classList.contains("is-visible") && target) render();
  }

  return { init, refresh, hide };
}
