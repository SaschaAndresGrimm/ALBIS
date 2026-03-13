import fs from "node:fs";
import path from "node:path";
import { JSDOM } from "jsdom";

import { isApprovedEnglishCarryover } from "./i18n_review_allowlist.mjs";

const TOKEN_PATTERN = /\{\{[^}]+\}\}/g;
const WHITESPACE_PATTERN = /\s+/g;
const LETTER_PATTERN = /\p{L}/u;

const HTML_ATTRIBUTE_BINDINGS = [
  { attribute: "aria-label", binding: "data-i18n-aria-label", kind: "html-aria-label" },
  { attribute: "placeholder", binding: "data-i18n-placeholder", kind: "html-placeholder" },
  { attribute: "title", binding: "data-i18n-title", kind: "html-title" },
  { attribute: "value", binding: "data-i18n-value", kind: "html-value" },
];

const JAVASCRIPT_LITERAL_PATTERNS = [
  { kind: "js-text", regex: /\.(?:textContent|innerText)\s*=\s*(['"`])((?:\\.|(?!\1).)*)\1/g },
  { kind: "js-property", regex: /\.(?:title|placeholder|ariaLabel|value)\s*=\s*(['"`])((?:\\.|(?!\1).)*)\1/g },
  { kind: "js-attribute", regex: /\.setAttribute\(\s*(['"])(aria-label|title|placeholder|value)\1\s*,\s*(['"`])((?:\\.|(?!\3).)*)\3/g },
  { kind: "js-dialog", regex: /\b(?:window\.)?(?:alert|confirm|prompt)\(\s*(['"`])((?:\\.|(?!\1).)*)\1/g },
  {
    kind: "js-status",
    regex: /\b(?:setStatus|setAutoloadStatus|setSettingsMessage|setManagedHelp|setFieldHint|setSectionBadgeState)\(\s*(['"`])((?:\\.|(?!\1).)*)\1/g,
  },
];

const STATUS_BUCKETS = new Set([
  "analysis",
  "autoload",
  "data",
  "frame",
  "render",
  "roi",
  "series",
  "upload",
]);

const HINT_BUCKETS = new Set([
  "autoload",
  "frame",
  "jfjoch",
  "overlay",
  "remote",
  "rings",
  "roi",
  "series",
  "simplon",
  "toolbar",
  "view",
]);

function normalizeText(value) {
  return String(value || "").replace(WHITESPACE_PATTERN, " ").trim();
}

function sanitizeForComparison(value) {
  return normalizeText(String(value || "").replace(TOKEN_PATTERN, " "));
}

function isLikelyUserFacingText(value) {
  const normalized = normalizeText(value);
  if (!normalized) {
    return false;
  }
  if (normalized.includes("${")) {
    return false;
  }
  if (/^[a-z0-9_]+(?:\.[a-z0-9_]+)+$/i.test(normalized)) {
    return false;
  }
  if (/^(?:https?:)?\/\//i.test(normalized)) {
    return false;
  }
  if (/^[./#][^\s]+$/.test(normalized)) {
    return false;
  }
  if (/^[a-z0-9.+-]+\/[a-z0-9.+-]+$/i.test(normalized)) {
    return false;
  }
  if (/^[A-Za-z0-9_.-]+\.[A-Za-z0-9_.-]+$/.test(normalized) && !/\s/.test(normalized)) {
    return false;
  }
  return LETTER_PATTERN.test(normalized);
}

function readLocale(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function collectLocaleFileNames(localeDir) {
  return fs.readdirSync(localeDir)
    .filter((entry) => entry.endsWith(".json"))
    .sort();
}

function listJavaScriptFiles(rootDir) {
  const results = [];
  const pending = [rootDir];
  while (pending.length > 0) {
    const currentDir = pending.pop();
    for (const entry of fs.readdirSync(currentDir, { withFileTypes: true })) {
      const fullPath = path.join(currentDir, entry.name);
      if (entry.isDirectory()) {
        pending.push(fullPath);
        continue;
      }
      if (entry.isFile() && entry.name.endsWith(".js")) {
        results.push(fullPath);
      }
    }
  }
  return results.sort();
}

function getElementLocationLine(dom, element, attribute) {
  const location = dom.nodeLocation(element);
  if (!location) {
    return 1;
  }
  if (attribute && location.attrs?.[attribute]?.startLine) {
    return location.attrs[attribute].startLine;
  }
  return location.startLine || 1;
}

function escapeReportCell(value) {
  return normalizeText(String(value || "")).replace(/\t/g, " ");
}

function createTechnicalRow(key, baseLocale, localeEntries) {
  const values = { en: baseLocale[key] || "" };
  for (const [locale, localeData] of localeEntries) {
    values[locale] = localeData[key] || "";
  }
  return { key, values };
}

export function getTechnicalBucket(key) {
  const normalizedKey = String(key || "");
  if (normalizedKey.startsWith("analysis.")) {
    return normalizedKey.split(".").slice(0, 2).join(".");
  }
  if (normalizedKey.startsWith("data_source.meta.")) {
    return "data_source.meta";
  }
  if (normalizedKey.startsWith("peaks.")) {
    return "peaks";
  }
  if (normalizedKey.startsWith("rings.")) {
    return "rings";
  }
  if (normalizedKey.startsWith("roi.")) {
    return "roi";
  }
  if (normalizedKey.startsWith("series.")) {
    return "series";
  }
  if (normalizedKey.startsWith("validation.")) {
    return "validation";
  }
  if (normalizedKey.startsWith("status.")) {
    const second = normalizedKey.split(".")[1];
    return STATUS_BUCKETS.has(second) ? `status.${second}` : null;
  }
  if (normalizedKey.startsWith("hint.")) {
    const second = normalizedKey.split(".")[1];
    return HINT_BUCKETS.has(second) ? `hint.${second}` : null;
  }
  return null;
}

export function collectUntranslatedFindings(baseLocale, localeEntries) {
  const findings = [];
  for (const [locale, localeData] of localeEntries) {
    for (const [key, baseValue] of Object.entries(baseLocale)) {
      const localeValue = localeData[key];
      if (localeValue !== baseValue) {
        continue;
      }
      if (isApprovedEnglishCarryover(key, localeValue)) {
        continue;
      }
      const sanitized = sanitizeForComparison(localeValue);
      if (!isLikelyUserFacingText(sanitized)) {
        continue;
      }
      findings.push({
        locale,
        key,
        value: localeValue,
      });
    }
  }
  return findings.sort((left, right) => {
    if (left.locale !== right.locale) {
      return left.locale.localeCompare(right.locale);
    }
    return left.key.localeCompare(right.key);
  });
}

export function collectTechnicalReviewRows(baseLocale, localeEntries) {
  const grouped = new Map();
  for (const key of Object.keys(baseLocale).sort()) {
    const bucket = getTechnicalBucket(key);
    if (!bucket) {
      continue;
    }
    const rows = grouped.get(bucket) || [];
    rows.push(createTechnicalRow(key, baseLocale, localeEntries));
    grouped.set(bucket, rows);
  }
  return grouped;
}

export function scanHtmlForHardcodedText({ filePath, source }) {
  const dom = new JSDOM(source, { includeNodeLocations: true });
  const { document, NodeFilter } = dom.window;
  const findings = [];

  const walker = document.createTreeWalker(document.documentElement, NodeFilter.SHOW_TEXT);
  let current = walker.nextNode();
  while (current) {
    const parent = current.parentElement;
    const text = normalizeText(current.textContent);
    if (
      parent &&
      !parent.closest("[data-i18n]") &&
      !["SCRIPT", "STYLE", "NOSCRIPT"].includes(parent.tagName) &&
      isLikelyUserFacingText(text)
    ) {
      const location = dom.nodeLocation(current) || dom.nodeLocation(parent);
      findings.push({
        filePath,
        line: location?.startLine || 1,
        kind: "html-text",
        text,
      });
    }
    current = walker.nextNode();
  }

  const allElements = document.querySelectorAll("*");
  allElements.forEach((element) => {
    HTML_ATTRIBUTE_BINDINGS.forEach(({ attribute, binding, kind }) => {
      if (!element.hasAttribute(attribute) || element.hasAttribute(binding)) {
        return;
      }
      const value = normalizeText(element.getAttribute(attribute));
      if (!isLikelyUserFacingText(value)) {
        return;
      }
      findings.push({
        filePath,
        line: getElementLocationLine(dom, element, attribute),
        kind,
        text: value,
      });
    });
  });

  return findings.sort((left, right) => left.line - right.line || left.text.localeCompare(right.text));
}

export function scanJavaScriptForHardcodedText({ filePath, source }) {
  const findings = [];
  const seen = new Set();
  const lines = String(source || "").split("\n");

  lines.forEach((lineSource, index) => {
    JAVASCRIPT_LITERAL_PATTERNS.forEach(({ kind, regex }) => {
      regex.lastIndex = 0;
      let match = regex.exec(lineSource);
      while (match) {
        const candidate = normalizeText(match.at(-1));
        if (isLikelyUserFacingText(candidate)) {
          const locationKey = `${kind}:${index + 1}:${candidate}`;
          if (!seen.has(locationKey)) {
            findings.push({
              filePath,
              line: index + 1,
              kind,
              text: candidate,
            });
            seen.add(locationKey);
          }
        }
        match = regex.exec(lineSource);
      }
    });
  });

  return findings.sort((left, right) => left.line - right.line || left.text.localeCompare(right.text));
}

export function buildI18nReviewReport(rootDir) {
  const localeDir = path.join(rootDir, "frontend", "locales");
  const baseLocale = readLocale(path.join(localeDir, "en.json"));
  const localeEntries = collectLocaleFileNames(localeDir)
    .filter((fileName) => fileName !== "en.json")
    .map((fileName) => [path.basename(fileName, ".json"), readLocale(path.join(localeDir, fileName))]);

  const untranslatedFindings = collectUntranslatedFindings(baseLocale, localeEntries);
  const technicalGroups = collectTechnicalReviewRows(baseLocale, localeEntries);

  const htmlFilePath = path.join(rootDir, "frontend", "index.html");
  const jsFilePaths = [
    path.join(rootDir, "frontend", "app.js"),
    ...listJavaScriptFiles(path.join(rootDir, "frontend", "modules")),
  ];

  const hardcodedFindings = [
    ...scanHtmlForHardcodedText({
      filePath: path.relative(rootDir, htmlFilePath),
      source: fs.readFileSync(htmlFilePath, "utf8"),
    }),
    ...jsFilePaths.flatMap((absolutePath) => scanJavaScriptForHardcodedText({
      filePath: path.relative(rootDir, absolutePath),
      source: fs.readFileSync(absolutePath, "utf8"),
    })),
  ].sort((left, right) => {
    if (left.filePath !== right.filePath) {
      return left.filePath.localeCompare(right.filePath);
    }
    if (left.line !== right.line) {
      return left.line - right.line;
    }
    return left.text.localeCompare(right.text);
  });

  return {
    reviewedLocales: localeEntries.map(([locale]) => locale),
    untranslatedFindings,
    hardcodedFindings,
    technicalGroups,
  };
}

export function formatI18nReviewReport(report, { strictUntranslated = false } = {}) {
  const lines = [];
  const untranslatedByLocale = report.untranslatedFindings.reduce((acc, finding) => {
    const list = acc.get(finding.locale) || [];
    list.push(finding);
    acc.set(finding.locale, list);
    return acc;
  }, new Map());
  const technicalRowCount = [...report.technicalGroups.values()].reduce((sum, rows) => sum + rows.length, 0);

  lines.push("ALBIS translation review report");
  lines.push(`Source locale: en`);
  lines.push(`Reviewed locales: ${report.reviewedLocales.join(", ")}`);
  lines.push(`Potential untranslated/suspect strings: ${report.untranslatedFindings.length}`);
  lines.push(`Hardcoded user-facing literals: ${report.hardcodedFindings.length}`);
  lines.push(`Technical review rows: ${technicalRowCount}`);
  lines.push(`Strict untranslated mode: ${strictUntranslated ? "on" : "off"}`);
  lines.push("");

  lines.push("=== Untranslated Or Suspect Strings ===");
  if (report.untranslatedFindings.length === 0) {
    lines.push("none");
  } else {
    report.reviewedLocales.forEach((locale) => {
      const localeFindings = untranslatedByLocale.get(locale) || [];
      lines.push(`${locale}: ${localeFindings.length}`);
      localeFindings.forEach((finding) => {
        lines.push(`  ${finding.key}\t${escapeReportCell(finding.value)}`);
      });
    });
  }
  lines.push("");

  lines.push("=== Hardcoded User-Facing Text ===");
  if (report.hardcodedFindings.length === 0) {
    lines.push("none");
  } else {
    report.hardcodedFindings.forEach((finding) => {
      lines.push(`${finding.filePath}:${finding.line}\t${finding.kind}\t${escapeReportCell(finding.text)}`);
    });
  }
  lines.push("");

  for (const [bucket, rows] of [...report.technicalGroups.entries()].sort(([left], [right]) => left.localeCompare(right))) {
    const headers = ["key", "en", ...report.reviewedLocales];
    lines.push(`=== Technical Review: ${bucket} (${rows.length}) ===`);
    lines.push(headers.join("\t"));
    rows.forEach((row) => {
      const cells = [row.key, row.values.en, ...report.reviewedLocales.map((locale) => row.values[locale] || "")];
      lines.push(cells.map(escapeReportCell).join("\t"));
    });
    lines.push("");
  }

  return `${lines.join("\n").trimEnd()}\n`;
}

export function getExitCodeForReview(report, { strictUntranslated = false } = {}) {
  if (strictUntranslated && report.untranslatedFindings.length > 0) {
    return 1;
  }
  return 0;
}
