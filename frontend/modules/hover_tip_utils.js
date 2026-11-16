/**
 * Pure helpers for the hover tooltip (hover_tips.js): what it says and where
 * it goes. No DOM here, so the rules can be tested on plain values.
 *
 * The tooltip names a control; it does not describe it. It appears only where
 * the control cannot say it by itself:
 *
 * - why a control is unavailable right now (`data-help-reason`), first;
 * - a control's `title`;
 * - the `aria-label` of a button that shows only an icon.
 *
 * A labelled field, a button with text or a checkbox with its label gets
 * nothing: a bubble that repeats what is on screen only covers it. What an
 * option means belongs in a "?" (info_tips.js), units and limits in a hint
 * under the field.
 */

const KEY_NAMES = {
  ArrowLeft: "←",
  ArrowRight: "→",
  ArrowUp: "↑",
  ArrowDown: "↓",
  Escape: "Esc",
  " ": "Space",
};

/** True when a control's own text is only an icon glyph (◀, ⛶, ⏯) or empty. */
export function isIconOnlyText(text) {
  return !/[\p{L}\p{N}]/u.test(String(text || ""));
}

/**
 * The tooltip's text for a control, or "" for none.
 *
 * @param {object} source
 * @param {string} [source.reason]     data-help-reason: why it is unavailable
 * @param {string} [source.title]      its title (or the one stashed during hover)
 * @param {string} [source.ariaLabel]  its aria-label
 * @param {boolean} [source.isButton]  a button, or role="button"
 * @param {string} [source.visibleText] the text it shows
 */
export function pickTipText({ reason, title, ariaLabel, isButton, visibleText } = {}) {
  const clean = (value) => String(value || "").replace(/\s+/g, " ").trim();
  if (clean(reason)) return clean(reason);
  if (clean(title)) return clean(title);
  if (isButton && clean(ariaLabel) && isIconOnlyText(visibleText)) return clean(ariaLabel);
  return "";
}

/**
 * The keycap for an `aria-keyshortcuts` value: its first shortcut, with the
 * arrow keys as arrows ("ArrowLeft" → "←", "Shift+ArrowUp" → "Shift+↑").
 */
export function keyshortcutLabel(value) {
  const first = String(value || "").trim().split(/\s+/)[0] || "";
  if (!first) return "";
  return first
    .split("+")
    .map((part) => KEY_NAMES[part] || (part.length === 1 ? part.toUpperCase() : part))
    .join("+");
}

/**
 * Where the tooltip goes: centred under its control, above it when there is
 * no room below, and always inside the viewport.
 *
 * @param {{left:number, top:number, width:number, bottom:number}} anchor
 * @param {{width:number, height:number}} size  the tooltip's own size
 * @param {{width:number, height:number}} viewport
 * @returns {{left:number, top:number, side:"below"|"above"}}
 */
export function placeTip(anchor, size, viewport, { gap = 6, margin = 8 } = {}) {
  const maxLeft = Math.max(margin, viewport.width - size.width - margin);
  const left = Math.round(Math.min(Math.max(anchor.left + anchor.width / 2 - size.width / 2, margin), maxLeft));
  const below = anchor.bottom + gap;
  if (below + size.height <= viewport.height - margin) {
    return { left, top: Math.round(below), side: "below" };
  }
  const above = anchor.top - gap - size.height;
  if (above >= margin) return { left, top: Math.round(above), side: "above" };
  // Taller than the room on either side: keep it on screen below.
  return { left, top: Math.round(Math.max(margin, viewport.height - size.height - margin)), side: "below" };
}
