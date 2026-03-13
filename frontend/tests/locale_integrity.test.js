import { describe, expect, it } from "vitest";
import fs from "node:fs";
import path from "node:path";

const LOCALES_DIR = path.join(process.cwd(), "frontend", "locales");
const INTERPOLATION_PATTERN = /\{\{\s*([a-zA-Z0-9_.-]+)\s*\}\}/g;
const PLURAL_PATTERN = /\{\{\s*plural\s*:\s*([a-zA-Z0-9_.-]+)\s*\|\s*([^|}]*)\|\s*([^}]*)\s*\}\}/g;

function readLocale(fileName) {
  return JSON.parse(fs.readFileSync(path.join(LOCALES_DIR, fileName), "utf8"));
}

function collectMatches(pattern, value, mapper) {
  const matches = [];
  for (const match of String(value).matchAll(pattern)) {
    matches.push(mapper(match));
  }
  return matches.sort();
}

function tokenSignature(value) {
  const source = String(value || "");
  const pluralMatches = collectMatches(PLURAL_PATTERN, source, ([, variable]) => variable);
  const withoutPlurals = source.replace(PLURAL_PATTERN, "");
  const interpolationMatches = collectMatches(INTERPOLATION_PATTERN, withoutPlurals, ([, variable]) => variable);
  const withoutTokens = withoutPlurals.replace(INTERPOLATION_PATTERN, "");

  expect(withoutTokens.includes("{{")).toBe(false);
  expect(withoutTokens.includes("}}")).toBe(false);

  return {
    interpolations: interpolationMatches,
    plurals: pluralMatches,
    variables: [...new Set([...interpolationMatches, ...pluralMatches])].sort(),
  };
}

describe("locale integrity", () => {
  it("keeps locale keys and token signatures aligned with English", () => {
    const localeFiles = fs.readdirSync(LOCALES_DIR)
      .filter((fileName) => fileName.endsWith(".json"))
      .sort();
    const baseLocale = readLocale("en.json");
    const baseKeys = Object.keys(baseLocale).sort();

    expect(baseKeys).toHaveLength(644);

    localeFiles.forEach((fileName) => {
      const locale = readLocale(fileName);
      expect(Object.keys(locale).sort()).toEqual(baseKeys);

      baseKeys.forEach((key) => {
        const localeTokens = tokenSignature(locale[key]);
        const baseTokens = tokenSignature(baseLocale[key]);
        expect(localeTokens.variables).toEqual(baseTokens.variables);
        expect(localeTokens.plurals.every((variable) => baseTokens.plurals.includes(variable))).toBe(true);
      });
    });
  });
});
