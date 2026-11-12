/**
 * The interface theme, as a value: the user's preference (Settings -> Viewer
 * -> Appearance, kept per browser) and the theme it resolves to.
 *
 * "dark" is the default, so ALBIS looks as it always has until someone picks
 * another; "light" is fixed; "system" follows the operating system's light or
 * dark setting, and so resolves differently on different machines and when
 * the system switches at sunset. Anything else -- a value from a later
 * release -- is read as the default, never as no theme, because a page
 * without one has no colours at all.
 */

export const THEME_PREFERENCES = ["dark", "light", "system"];
export const DEFAULT_THEME_PREFERENCE = "dark";

export function normalizeThemePreference(value) {
  const pref = String(value ?? "").trim().toLowerCase();
  return THEME_PREFERENCES.includes(pref) ? pref : DEFAULT_THEME_PREFERENCE;
}

/**
 * @param {string} preference "system", "dark" or "light"
 * @param {boolean} systemPrefersLight what (prefers-color-scheme: light) says
 * @returns {"dark" | "light"}
 */
export function resolveTheme(preference, systemPrefersLight) {
  const pref = normalizeThemePreference(preference);
  if (pref === "system") return systemPrefersLight ? "light" : "dark";
  return pref;
}
