import { describe, expect, it } from "vitest";
import fs from "node:fs";
import path from "node:path";

/**
 * Keeps frontend/style.css on the design tokens described in
 * docs/UI_GUIDE.md. The interface reads as one piece because colours, text
 * sizes, corners, stacking and focus rings come from a small set of shared
 * values; each literal added beside them is a near-copy that drifts, and by
 * 2026-10 there were 42 shades of the same border blue.
 *
 * Two kinds of check:
 *   - Rules with no exceptions (focus rings, the Safari blur prefix, the
 *     stacking scale): any violation fails.
 *   - Ratchets for literals still in use (colours, text sizes, corners): the
 *     count may not grow. When you remove some, lower the baseline to the new
 *     count in the same change, so it cannot creep back up.
 *
 * If one of these fails, use a token from :root (see docs/UI_GUIDE.md); if
 * none fits, add one there, with a comment saying what it is for.
 */

const CSS_PATH = path.join(process.cwd(), "frontend", "style.css");

// Literals outside :root and @font-face. Lower these as they are removed.
const BASELINE = {
  colours: 617,
  fontSizes: 14,
  radii: 8,
};

// The stacking scale (docs/UI_GUIDE.md, "Stacking"). A new layer takes one of
// these, or is added here and to the guide with what it sits between.
const Z_SCALE = new Set([1, 2, 3, 4, 5, 6, 8, 20, 24, 25, 28, 30, 32, 34, 36, 42, 48, 60, 62, 70, 82, 90, 1000, 1500, 2000, 3000, 3001]);

// outline: none without a replacement ring in the same rule, and why it is fine.
const OUTLINE_EXCEPTIONS = {
  // The slider draws its ring on the thumb: input[type="range"]:focus-visible::-webkit-slider-thumb / ::-moz-range-thumb.
  'input[type="range"]:focus': "ring on the thumb",
};

function parseRules() {
  const css = fs.readFileSync(CSS_PATH, "utf8").replace(/\/\*[\s\S]*?\*\//g, "");
  // Innermost blocks only: "selector { declarations }". @media and other
  // wrappers are not matched themselves; the rules inside them are.
  return [...css.matchAll(/([^{}]+)\{([^{}]*)\}/g)].map((match) => {
    const selector = match[1].trim();
    const declarations = [...match[2].matchAll(/(?:^|;)\s*([-\w]+)\s*:\s*([^;]+)/g)].map((d) => ({
      prop: d[1],
      value: d[2].trim(),
    }));
    return { selector, declarations };
  });
}

const rules = parseRules();
const styled = rules.filter((rule) => !/^:root\b|^@font-face/.test(rule.selector));
const declarations = styled.flatMap((rule) =>
  rule.declarations.filter((d) => !d.prop.startsWith("--")).map((d) => ({ ...d, rule })),
);

describe("style guard: tokens over literals", () => {
  it("adds no colour literals outside :root", () => {
    const count = declarations.reduce((sum, d) => sum + (d.value.match(/#[0-9a-fA-F]{3,8}\b|rgba?\(/g) || []).length, 0);
    expect(count, `colour literals: ${count}, baseline ${BASELINE.colours}. Use a token (--text, --muted, --border-*, --surface-*, --accent); if you removed some, lower BASELINE.colours to ${count}.`).toBe(BASELINE.colours);
  });

  it("adds no literal font sizes", () => {
    const count = declarations.filter((d) => d.prop === "font-size" && /\d(px|rem|em)\b/.test(d.value)).length;
    expect(count, `literal font sizes: ${count}, baseline ${BASELINE.fontSizes}. Use --font-size-xs..xl; if you removed some, lower BASELINE.fontSizes to ${count}.`).toBe(BASELINE.fontSizes);
  });

  it("adds no literal corner radii (pills at 999px and circles at 50% are fine)", () => {
    const count = declarations.filter((d) => d.prop.endsWith("radius") && /\b(?!0\b)(?!999px)\d+(\.\d+)?px\b/.test(d.value)).length;
    expect(count, `literal radii: ${count}, baseline ${BASELINE.radii}. Use --radius-sm/md/lg; if you removed some, lower BASELINE.radii to ${count}.`).toBe(BASELINE.radii);
  });

  it("sets font-family only through the font tokens", () => {
    const literal = declarations.filter((d) => d.prop === "font-family" && !/^(var\(|inherit)/.test(d.value));
    expect(literal.map((d) => d.rule.selector), "use var(--font-ui), var(--font-mono, ...) or inherit").toEqual([]);
  });
});

describe("style guard: rules without exceptions", () => {
  it("keeps every z-index on the stacking scale", () => {
    const off = declarations
      .filter((d) => d.prop === "z-index" && /^\d+$/.test(d.value) && !Z_SCALE.has(Number(d.value)))
      .map((d) => `${d.rule.selector}: ${d.value}`);
    expect(off, "pick a layer from Z_SCALE / docs/UI_GUIDE.md, or add the new one to both").toEqual([]);
  });

  it("never removes a focus outline without drawing a replacement ring", () => {
    const bare = styled
      .filter((rule) => rule.declarations.some((d) => d.prop === "outline" && /^none\b/.test(d.value)))
      .filter((rule) => !(rule.selector in OUTLINE_EXCEPTIONS))
      .filter(
        (rule) =>
          !/:focus/.test(rule.selector) ||
          !rule.declarations.some(
            (d) => /box-shadow|border-color|outline-color/.test(d.prop) || (d.prop === "outline" && !/^none\b/.test(d.value)),
          ),
      )
      .map((rule) => rule.selector);
    expect(bare, "outline: none belongs in a :focus-visible rule that sets box-shadow: var(--focus-ring) or similar").toEqual([]);
  });

  it("pairs every backdrop-filter with -webkit-backdrop-filter for Safari before 18", () => {
    const unpaired = styled
      .filter((rule) => {
        const blur = rule.declarations.find((d) => d.prop === "backdrop-filter");
        return blur && !rule.declarations.some((d) => d.prop === "-webkit-backdrop-filter" && d.value === blur.value);
      })
      .map((rule) => rule.selector);
    expect(unpaired).toEqual([]);
  });

  it("keeps the token definitions free of self-references", () => {
    // `--border: var(--border)` is a cycle: the browser drops it and every use
    // falls back to the text colour (it turned every border white once).
    const root = rules.filter((rule) => /^:root\b/.test(rule.selector)).flatMap((rule) => rule.declarations);
    const cycles = root.filter((d) => d.prop.startsWith("--") && new RegExp(`var\\(\\s*${d.prop}\\s*[,)]`).test(d.value));
    expect(cycles.map((d) => d.prop)).toEqual([]);
  });
});
