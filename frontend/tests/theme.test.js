import { describe, expect, it, vi } from "vitest";
import { normalizeThemePreference, resolveTheme } from "../modules/theme_utils.js";
import { createThemeController, THEME_CHANGE_EVENT, THEME_STORAGE_KEY } from "../modules/theme.js";
import { createPlotTheme } from "../modules/plot_theme.js";

function fakeMedia(matches) {
  const listeners = [];
  return {
    matches,
    addEventListener: (_type, fn) => listeners.push(fn),
    flip(next) {
      this.matches = next;
      listeners.forEach((fn) => fn());
    },
  };
}

function fakeStorage(initial = {}) {
  const data = { ...initial };
  return { getItem: (k) => data[k] ?? null, setItem: (k, v) => (data[k] = String(v)), data };
}

describe("theme_utils", () => {
  it("reads anything unknown as dark, the default, never as no theme", () => {
    expect(normalizeThemePreference(" Light ")).toBe("light");
    expect(normalizeThemePreference("System")).toBe("system");
    expect(normalizeThemePreference("solarized")).toBe("dark");
    expect(normalizeThemePreference(undefined)).toBe("dark");
  });

  it("resolves system from the operating system, and keeps a fixed choice", () => {
    expect(resolveTheme("system", true)).toBe("light");
    expect(resolveTheme("system", false)).toBe("dark");
    expect(resolveTheme("dark", true)).toBe("dark");
    expect(resolveTheme("light", false)).toBe("light");
  });
});

describe("createThemeController", () => {
  it("starts from the stored preference, so the first paint and the controller agree", () => {
    const root = document.createElement("html");
    const controller = createThemeController({ root, storage: fakeStorage({ [THEME_STORAGE_KEY]: "light" }), media: fakeMedia(false), target: new EventTarget() });
    expect(root.dataset.theme).toBe("light");
    expect(controller.getPreference()).toBe("light");
  });

  it("applies and remembers a choice, and announces a change once", () => {
    const root = document.createElement("html");
    const storage = fakeStorage();
    const target = new EventTarget();
    const heard = vi.fn();
    target.addEventListener(THEME_CHANGE_EVENT, heard);
    const controller = createThemeController({ root, storage, media: fakeMedia(false), target });
    expect(root.dataset.theme).toBe("dark");
    heard.mockClear();

    controller.applyPreference("light");
    expect(root.dataset.theme).toBe("light");
    expect(storage.data[THEME_STORAGE_KEY]).toBe("light");
    controller.applyPreference("light");
    expect(heard).toHaveBeenCalledTimes(1);
  });

  it("stays dark on a light system until someone chooses otherwise", () => {
    const root = document.createElement("html");
    createThemeController({ root, storage: fakeStorage(), media: fakeMedia(true), target: new EventTarget() });
    expect(root.dataset.theme).toBe("dark");
  });

  it("follows the operating system while set to system, not while fixed", () => {
    const root = document.createElement("html");
    const media = fakeMedia(false);
    const controller = createThemeController({ root, storage: fakeStorage({ [THEME_STORAGE_KEY]: "system" }), media, target: new EventTarget() });
    media.flip(true);
    expect(root.dataset.theme).toBe("light");
    controller.applyPreference("dark");
    media.flip(false);
    media.flip(true);
    expect(root.dataset.theme).toBe("dark");
  });
});

describe("createPlotTheme", () => {
  it("reads the plot colours from the tokens, and again after a theme change", () => {
    let value = "#111111";
    const target = new EventTarget();
    const theme = createPlotTheme({ readToken: (name) => (name === "--plot-bg" ? value : ""), target });
    expect(theme.bg).toBe("#111111");
    // An unset token falls back to the dark value rather than to nothing.
    expect(theme.text).toBe("#dce8ff");
    value = "#ffffff";
    expect(theme.bg).toBe("#111111");
    target.dispatchEvent(new CustomEvent(THEME_CHANGE_EVENT));
    expect(theme.bg).toBe("#ffffff");
  });
});
