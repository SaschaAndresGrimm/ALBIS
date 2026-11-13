/**
 * The small chips in a panel section's title: the summary chip ("On", "File")
 * and the state badge ("Image loaded", "Scanning datasets…").
 *
 * Their text is set from code as things happen, but the markup starts them
 * with a `data-i18n` key for the text shown before anything has. Left in
 * place, that key made every re-translation of the page (applyI18nToDom)
 * put the start-up text back over the current one, while the tone stayed:
 * a blue "active" badge said "Choose an image source to begin." with a file
 * open. So a text set here replaces the key: a plain string drops it, and a
 * `{ key, vars }` message becomes the element's key, so a language switch
 * translates the current text instead of the start-up one.
 */
import { t } from "./i18n.js";

const BADGE_TONES = ["empty", "loading", "active", "warning"];
const CHIP_TONES = ["active", "warning", "off"];

function writeText(element, message) {
  if (message && typeof message === "object" && message.key) {
    const vars = message.vars && typeof message.vars === "object" ? message.vars : null;
    element.dataset.i18n = message.key;
    if (vars) element.dataset.i18nVars = JSON.stringify(vars);
    else delete element.dataset.i18nVars;
    element.textContent = t(message.key, vars || {});
    return;
  }
  delete element.dataset.i18n;
  delete element.dataset.i18nVars;
  element.textContent = message || "";
}

/** A section's state badge; `message` is a string or `{ key, vars }`. */
export function setSectionBadgeState(element, tone, message) {
  if (!element) return;
  const nextTone = BADGE_TONES.includes(tone) ? tone : "empty";
  BADGE_TONES.forEach((name) => element.classList.remove(`is-${name}`));
  element.classList.add(`is-${nextTone}`);
  writeText(element, message);
}

/** A section's summary chip; `text` is a string or `{ key, vars }`. */
export function setSummaryChip(element, text, tone = "default") {
  if (!element) return;
  writeText(element, text);
  // "off": beside a switch that already shows it; the workspace layout hides it.
  for (const name of CHIP_TONES) element.classList.toggle(`is-${name}`, tone === name);
}
