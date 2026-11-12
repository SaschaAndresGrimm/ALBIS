/**
 * Applies the interface theme: data-theme on <html>, which the light token
 * set in style.css keys on, and a "albis:themechange" event for what is drawn
 * on canvas (the histogram, the ROI plots, the overview) and has to be drawn
 * again in the new colours.
 *
 * The preference lives in localStorage, per browser like the panel width, so
 * people sharing one ALBIS server each keep their own, and so the inline
 * script in index.html can set data-theme before the first paint; otherwise
 * a light-theme window would open dark and flash over.
 */

import { DEFAULT_THEME_PREFERENCE, normalizeThemePreference, resolveTheme } from "./theme_utils.js";

export const THEME_STORAGE_KEY = "albis.theme";
export const THEME_CHANGE_EVENT = "albis:themechange";

export function createThemeController({
  root = document.documentElement,
  storage = globalThis.localStorage,
  media = globalThis.matchMedia?.("(prefers-color-scheme: light)") ?? null,
  target = globalThis,
} = {}) {
  let preference = DEFAULT_THEME_PREFERENCE;

  function readStored() {
    try {
      return normalizeThemePreference(storage?.getItem(THEME_STORAGE_KEY));
    } catch {
      return DEFAULT_THEME_PREFERENCE;
    }
  }

  function render() {
    const theme = resolveTheme(preference, Boolean(media?.matches));
    if (root.dataset.theme === theme) return theme;
    root.dataset.theme = theme;
    target.dispatchEvent?.(new globalThis.CustomEvent(THEME_CHANGE_EVENT, { detail: { theme, preference } }));
    return theme;
  }

  /** Set the preference ("system", "dark" or "light"), remember it, apply it. */
  function applyPreference(value) {
    preference = normalizeThemePreference(value);
    try {
      storage?.setItem(THEME_STORAGE_KEY, preference);
    } catch {
      // No storage: the theme still applies, only the next start may flash.
    }
    return render();
  }

  // "system" switches with the operating system, also while ALBIS is open.
  media?.addEventListener?.("change", () => {
    if (preference === "system") render();
  });

  preference = readStored();
  render();

  return {
    applyPreference,
    getPreference: () => preference,
    getTheme: () => root.dataset.theme,
  };
}

let shared = null;

/** The one controller for the page, created on first use. */
export function getThemeController() {
  if (!shared) shared = createThemeController();
  return shared;
}
