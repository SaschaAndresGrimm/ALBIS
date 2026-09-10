import { describe, expect, it } from "vitest";
import fs from "node:fs";
import path from "node:path";

/**
 * The README advertises thirteen languages, and today every locale really does
 * carry the same 948 keys. Nothing enforced that: `npm run review:i18n` prints
 * a report but returns 0 unless `--strict-untranslated` is passed, and that
 * mode fails on 313 legitimate carryovers (proper nouns, units, "OK"), so it
 * cannot be a gate. It also only looks at carryovers -- a key missing from one
 * locale would not fail it even in strict mode.
 *
 * These are the two properties that break the interface rather than merely
 * reading oddly: a key present in English and absent elsewhere renders the raw
 * key to that user, and an interpolation placeholder that does not survive
 * translation renders a sentence with a hole in it or an unreplaced token.
 */

const LOCALE_DIR = path.join(process.cwd(), "frontend", "locales");
const PLACEHOLDER = /\{\{\s*([a-zA-Z0-9_.-]+)\s*(?:\|[^}]*)?\}\}/g;

function load(name) {
  return JSON.parse(fs.readFileSync(path.join(LOCALE_DIR, name), "utf8"));
}

function placeholders(value) {
  if (typeof value !== "string") return [];
  // `plural:count|a|b` and `{{count}}` both name `count`; compare the names,
  // not the surrounding syntax, so a locale may pluralise differently.
  const names = new Set();
  for (const match of value.matchAll(PLACEHOLDER)) {
    names.add(match[1].replace(/^plural:/, ""));
  }
  return [...names].sort();
}

const LOCALES = fs
  .readdirSync(LOCALE_DIR)
  .filter((f) => f.endsWith(".json"))
  .sort();
const EN = load("en.json");
const EN_KEYS = Object.keys(EN).sort();
const OTHERS = LOCALES.filter((f) => f !== "en.json");

describe("locale parity", () => {
  it("ships the thirteen languages the README claims", () => {
    expect(LOCALES.length).toBe(13);
  });

  it.each(OTHERS)("%s has exactly the keys en.json has", (file) => {
    const dict = load(file);
    const keys = Object.keys(dict).sort();
    const missing = EN_KEYS.filter((k) => !(k in dict));
    const extra = keys.filter((k) => !(k in EN));

    // Named rather than counted: a failure should say which key to add.
    expect({ missing, extra }).toEqual({ missing: [], extra: [] });
  });

  it.each(OTHERS)("%s keeps every interpolation placeholder", (file) => {
    const dict = load(file);
    const mismatched = [];
    for (const key of EN_KEYS) {
      if (!(key in dict)) continue;
      const expected = placeholders(EN[key]);
      const actual = placeholders(dict[key]);
      if (expected.join(",") !== actual.join(",")) {
        mismatched.push(`${key}: expected [${expected}] got [${actual}]`);
      }
    }

    expect(mismatched).toEqual([]);
  });

  it.each(OTHERS)("%s has no empty translations", (file) => {
    const dict = load(file);
    const empty = Object.entries(dict)
      .filter(([, v]) => typeof v === "string" && v.trim() === "")
      .map(([k]) => k);

    expect(empty).toEqual([]);
  });

  it("keeps every value a string, so nothing renders as [object Object]", () => {
    const offenders = [];
    for (const file of LOCALES) {
      const dict = load(file);
      for (const [key, value] of Object.entries(dict)) {
        if (typeof value !== "string") offenders.push(`${file}:${key} is ${typeof value}`);
      }
    }

    expect(offenders).toEqual([]);
  });
});
