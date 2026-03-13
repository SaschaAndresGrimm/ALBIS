/**
 * Lightweight runtime i18n helper for ALBIS frontend.
 */

const SUPPORTED_LANGUAGES = ["en", "zh-CN", "ja", "fr", "es", "it", "pt"];
const FALLBACK_LANGUAGE = "en";
const STORAGE_KEY = "albis.ui.language";

const dictionaries = new Map();
const warnedMissingKeys = new Set();
const listeners = new Set();

let currentLanguage = FALLBACK_LANGUAGE;
let initialized = false;
let preloadPromise = null;

function safeLocalStorageGet(key) {
  try {
    return window.localStorage.getItem(key);
  } catch {
    return null;
  }
}

function safeLocalStorageSet(key, value) {
  try {
    window.localStorage.setItem(key, value);
  } catch {
    // ignore storage errors
  }
}

export function getSupportedLanguages() {
  return [...SUPPORTED_LANGUAGES];
}

export function normalizeLanguage(language) {
  const raw = String(language || "").trim();
  if (!raw) return FALLBACK_LANGUAGE;
  if (SUPPORTED_LANGUAGES.includes(raw)) return raw;
  const lower = raw.toLowerCase();
  if (lower.startsWith("zh")) return "zh-CN";
  if (lower.startsWith("ja")) return "ja";
  if (lower.startsWith("fr")) return "fr";
  if (lower.startsWith("es")) return "es";
  if (lower.startsWith("it")) return "it";
  if (lower.startsWith("pt")) return "pt";
  if (lower.startsWith("en")) return "en";
  return FALLBACK_LANGUAGE;
}

export function getStoredLanguagePreference() {
  const raw = safeLocalStorageGet(STORAGE_KEY);
  if (!raw) return "";
  const normalized = normalizeLanguage(raw);
  return SUPPORTED_LANGUAGES.includes(normalized) ? normalized : "";
}

export function hasStoredLanguagePreference() {
  return Boolean(getStoredLanguagePreference());
}

export function resolveBrowserLanguage() {
  const nav = typeof navigator === "object" && navigator
    ? (navigator.languages?.[0] || navigator.language || "")
    : "";
  return normalizeLanguage(nav);
}

export function resolveInitialLanguage(backendLanguage = "") {
  const stored = getStoredLanguagePreference();
  if (stored) return stored;
  if (backendLanguage) {
    const normalizedBackend = normalizeLanguage(backendLanguage);
    if (SUPPORTED_LANGUAGES.includes(normalizedBackend)) {
      return normalizedBackend;
    }
  }
  return resolveBrowserLanguage();
}

async function loadDictionary(language) {
  if (dictionaries.has(language)) {
    return dictionaries.get(language);
  }
  const url = `locales/${encodeURIComponent(language)}.json`;
  try {
    const response = await fetch(url, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`Failed loading locale ${language}: ${response.status}`);
    }
    const payload = await response.json();
    const dictionary = payload && typeof payload === "object" ? payload : {};
    dictionaries.set(language, dictionary);
    return dictionary;
  } catch (err) {
    console.warn(err);
    const fallback = {};
    dictionaries.set(language, fallback);
    return fallback;
  }
}

function getValue(vars, key) {
  if (!vars || typeof vars !== "object") return undefined;
  if (Object.prototype.hasOwnProperty.call(vars, key)) {
    return vars[key];
  }
  if (!key.includes(".")) return undefined;
  return key.split(".").reduce((acc, part) => {
    if (acc && typeof acc === "object" && Object.prototype.hasOwnProperty.call(acc, part)) {
      return acc[part];
    }
    return undefined;
  }, vars);
}

function applyPluralTokens(template, vars) {
  return template.replace(
    /\{\{\s*plural\s*:\s*([a-zA-Z0-9_.-]+)\s*\|\s*([^|}]*)\|\s*([^}]*)\s*\}\}/g,
    (_match, varName, singular, plural) => {
      const count = Number(getValue(vars, String(varName)));
      return Math.abs(count) === 1 ? singular : plural;
    },
  );
}

function interpolate(template, vars) {
  const withPlural = applyPluralTokens(template, vars);
  return withPlural.replace(/\{\{\s*([a-zA-Z0-9_.-]+)\s*\}\}/g, (_match, varName) => {
    const value = getValue(vars, String(varName));
    if (value === null || typeof value === "undefined") return "";
    return String(value);
  });
}

function lookup(language, key) {
  const dictionary = dictionaries.get(language);
  if (!dictionary || typeof dictionary !== "object") return undefined;
  const value = dictionary[key];
  return typeof value === "string" ? value : undefined;
}

export function t(key, vars) {
  const normalizedKey = String(key || "").trim();
  if (!normalizedKey) return "";
  const activeValue = lookup(currentLanguage, normalizedKey);
  const fallbackValue = lookup(FALLBACK_LANGUAGE, normalizedKey);
  const value = activeValue ?? fallbackValue;
  if (typeof value !== "string") {
    if (!warnedMissingKeys.has(normalizedKey)) {
      warnedMissingKeys.add(normalizedKey);
      console.warn(`Missing i18n key: ${normalizedKey}`);
    }
    return normalizedKey;
  }
  return interpolate(value, vars);
}

function parseVars(element) {
  const encoded = element?.dataset?.i18nVars;
  if (!encoded) return {};
  try {
    const parsed = JSON.parse(encoded);
    return parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    return {};
  }
}

function applyElementI18n(element) {
  if (!element || !element.dataset) return;
  const vars = parseVars(element);
  if (element.dataset.i18n) {
    element.textContent = t(element.dataset.i18n, vars);
  }
  if (element.dataset.i18nTitle) {
    element.title = t(element.dataset.i18nTitle, vars);
  }
  if (element.dataset.i18nAriaLabel) {
    element.setAttribute("aria-label", t(element.dataset.i18nAriaLabel, vars));
  }
  if (element.dataset.i18nPlaceholder) {
    element.setAttribute("placeholder", t(element.dataset.i18nPlaceholder, vars));
  }
  if (element.dataset.i18nValue) {
    element.setAttribute("value", t(element.dataset.i18nValue, vars));
  }
}

export function applyI18nToDom(root = document) {
  if (!root) return;
  const selector = "[data-i18n], [data-i18n-title], [data-i18n-aria-label], [data-i18n-placeholder], [data-i18n-value]";
  const matches = [];
  if (typeof root.matches === "function" && root.matches(selector)) {
    matches.push(root);
  }
  if (typeof root.querySelectorAll === "function") {
    matches.push(...root.querySelectorAll(selector));
  }
  matches.forEach((element) => applyElementI18n(element));
  if (document?.documentElement) {
    document.documentElement.lang = currentLanguage;
  }
}

export function getLanguage() {
  return currentLanguage;
}

export function setLanguage(language, options = {}) {
  const { persist = true, applyDom = true, notify = true } = options;
  const normalized = normalizeLanguage(language);
  const next = SUPPORTED_LANGUAGES.includes(normalized) ? normalized : FALLBACK_LANGUAGE;
  currentLanguage = next;
  if (persist) {
    safeLocalStorageSet(STORAGE_KEY, next);
  }
  if (applyDom) {
    applyI18nToDom(document);
  }
  if (notify) {
    listeners.forEach((listener) => {
      try {
        listener(next);
      } catch (err) {
        console.error(err);
      }
    });
  }
  return next;
}

export function onLanguageChange(listener) {
  if (typeof listener !== "function") {
    return () => {};
  }
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

export async function initializeI18n(options = {}) {
  if (initialized) return;
  if (!preloadPromise) {
    preloadPromise = Promise.all(SUPPORTED_LANGUAGES.map((language) => loadDictionary(language)));
  }
  await preloadPromise;
  const initialLanguage = resolveInitialLanguage(options.backendLanguage || "");
  // Keep inferred startup language ephemeral until a user or config preference is applied.
  setLanguage(initialLanguage, { persist: false, applyDom: true, notify: false });
  initialized = true;
}
