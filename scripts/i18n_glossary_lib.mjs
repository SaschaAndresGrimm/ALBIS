/**
 * Glossary check for the translation review (`npm run review:i18n`).
 *
 * docs/I18N_GLOSSARY.csv fixes how a technical term is translated. A string
 * whose English uses a glossary term should use that term's translation; the
 * check reports the ones that do not. It is a warning, not a gate: grammar
 * changes words (a plural, a case ending, a compound), and the matching below
 * only allows for word endings, so a finding is a place to look, not proof.
 */

import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

/** RFC 4180 CSV: quoted fields may hold commas, newlines and "" for a quote. */
export function parseCsv(source) {
  const rows = [];
  let row = [];
  let field = "";
  let quoted = false;
  for (let i = 0; i < source.length; i += 1) {
    const char = source[i];
    if (quoted) {
      if (char === '"' && source[i + 1] === '"') {
        field += '"';
        i += 1;
      } else if (char === '"') {
        quoted = false;
      } else {
        field += char;
      }
    } else if (char === '"') {
      quoted = true;
    } else if (char === ",") {
      row.push(field);
      field = "";
    } else if (char === "\n" || char === "\r") {
      if (char === "\r" && source[i + 1] === "\n") i += 1;
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
    } else {
      field += char;
    }
  }
  if (field !== "" || row.length) {
    row.push(field);
    rows.push(row);
  }
  return rows.filter((cells) => cells.some((cell) => cell.trim() !== ""));
}

/** Glossary rows as `{ id, english, doNotTranslate, translations: { locale: term } }`. */
export function parseGlossary(source) {
  const [header, ...rows] = parseCsv(source);
  if (!header) return [];
  const column = (name) => header.indexOf(name);
  const fixed = new Set(["term_id", "domain", "english_term", "definition", "do_not_translate", "notes"]);
  const locales = header.filter((name) => !fixed.has(name));
  return rows.map((cells) => ({
    id: cells[column("term_id")] || "",
    english: cells[column("english_term")] || "",
    doNotTranslate: String(cells[column("do_not_translate")] || "").trim().toLowerCase() === "true",
    translations: Object.fromEntries(
      locales.map((locale) => [locale, String(cells[column(locale)] || "").trim()]).filter(([, term]) => term),
    ),
  }));
}

export function readGlossary(rootDir) {
  const filePath = path.join(rootDir, "docs", "I18N_GLOSSARY.csv");
  if (!fs.existsSync(filePath)) return [];
  return parseGlossary(fs.readFileSync(filePath, "utf8"));
}

function escapeRegExp(text) {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** "Frame", "frames", "Frame's" in English; not "framework". */
function englishPattern(term) {
  const words = term.trim().split(/\s+/).map(escapeRegExp);
  return new RegExp(`\\b${words.join("\\s+")}(?:s|es|'s)?\\b`, "i");
}

/**
 * A word's stem, so that an ending may change: bildruta finds bildrutan and
 * bildrutor, pic finds pics. Short words must match whole.
 */
function stem(word) {
  if (word.length > 4) return word.slice(0, word.length - 2);
  // anel → anéis, ring → ringar: a four-letter word may lose its last letter.
  return word.length === 4 ? word.slice(0, 3) : word;
}

/**
 * Placeholders out, so {{frames}} does not count as the word frame: in
 * "{{frames}} frames" the German "{{frames}} Bilder" looked as if it used it.
 */
function withoutPlaceholders(text) {
  return text.replace(/\{\{[^}]*\}\}/g, " ");
}

/** Lower case without accents, so píxeis matches pixel and fotões fotão. */
function fold(text) {
  return withoutPlaceholders(text).normalize("NFD").replace(/\p{M}/gu, "").toLocaleLowerCase();
}

/**
 * Whether `text` uses the glossary translation `term`, allowing for endings.
 * Short linking words (de, del, do, dal, des) are left out of the comparison
 * when the term has longer ones: "energía de fotón" uses "energía del fotón".
 */
export function usesTerm(text, term) {
  const haystack = fold(text);
  const words = fold(term).split(/\s+/).filter(Boolean);
  const significant = words.filter((word) => [...word].length > 3);
  return (significant.length ? significant : words).every((word) => haystack.includes(stem(word)));
}

/**
 * Strings whose English uses a glossary term but whose translation does not
 * use that term's glossary translation. A missing or untranslated string is
 * left to the other checks.
 */
export function collectGlossaryFindings(glossary, baseLocale, localeEntries, { isException = () => false } = {}) {
  const findings = [];
  const terms = glossary
    .filter((entry) => entry.english && !entry.doNotTranslate)
    .map((entry) => ({ ...entry, pattern: englishPattern(entry.english) }));
  for (const [key, english] of Object.entries(baseLocale)) {
    if (typeof english !== "string") continue;
    const matching = terms.filter((entry) => entry.pattern.test(withoutPlaceholders(english)));
    if (!matching.length) continue;
    for (const [locale, entries] of localeEntries) {
      const value = entries[key];
      if (typeof value !== "string" || value === english) continue;
      for (const entry of matching) {
        const expected = entry.translations[locale];
        if (expected && !usesTerm(value, expected) && !isException(locale, key, entry.id)) {
          findings.push({ key, locale, termId: entry.id, expected, value });
        }
      }
    }
  }
  return findings.sort(
    (left, right) =>
      left.locale.localeCompare(right.locale) || left.key.localeCompare(right.key) || left.termId.localeCompare(right.termId),
  );
}

/**
 * The (locale, key) pairs whose text differs from `ref` (a git revision), new
 * keys included, as a Set of "locale\tkey"; null when git cannot tell (not a
 * checkout, unknown ref). The review then limits the glossary findings to
 * what a change wrote, since older strings have their own backlog.
 */
export function changedLocaleStrings(rootDir, ref, localeEntries) {
  const changed = new Set();
  for (const [locale, entries] of localeEntries) {
    let before;
    try {
      const source = execFileSync("git", ["show", `${ref}:frontend/locales/${locale}.json`], {
        cwd: rootDir,
        encoding: "utf8",
        stdio: ["ignore", "pipe", "ignore"],
        maxBuffer: 16 * 1024 * 1024,
      });
      before = JSON.parse(source);
    } catch {
      return null;
    }
    for (const [key, value] of Object.entries(entries)) {
      if (before[key] !== value) changed.add(`${locale}\t${key}`);
    }
  }
  return changed;
}
