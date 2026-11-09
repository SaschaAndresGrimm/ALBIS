import { describe, expect, it } from "vitest";

import fs from "node:fs";
import path from "node:path";

import {
  collectGlossaryFindings,
  parseCsv,
  parseGlossary,
  readGlossary,
  usesTerm,
} from "../../scripts/i18n_glossary_lib.mjs";
import { getGlossaryExceptions, isGlossaryException } from "../../scripts/i18n_review_allowlist.mjs";

const GLOSSARY = parseGlossary(
  [
    "term_id,domain,english_term,definition,do_not_translate,de,da,sv,notes",
    'frame,data,Frame,"Single image, in a stack",false,Frame,ramme,bildruta,"Keep ""frame"" distinct"',
    "hdf5,format,HDF5,File format,true,HDF5,HDF5,HDF5,",
  ].join("\n"),
);

describe("glossary review", () => {
  it("reads quoted CSV fields with commas and doubled quotes", () => {
    expect(parseCsv('a,"b, c","say ""hi"""\n1,2,3\n')).toEqual([
      ["a", "b, c", 'say "hi"'],
      ["1", "2", "3"],
    ]);
    expect(GLOSSARY[0]).toMatchObject({ id: "frame", english: "Frame", translations: { de: "Frame", sv: "bildruta" } });
    expect(GLOSSARY[1].doNotTranslate).toBe(true);
  });

  it("allows an ending to change but not the word", () => {
    expect(usesTerm("Ingen pixel i den här bildrutan.", "bildruta")).toBe(true);
    expect(usesTerm("Stega fem bildrutor", "bildruta")).toBe(true);
    expect(usesTerm("Ingen pixel i den här bilden.", "bildruta")).toBe(false);
    expect(usesTerm("pics de diffraction", "pic de diffraction")).toBe(true);
    // Accents and a four-letter word's last letter may change: European
    // Portuguese plurals.
    expect(usesTerm("píxeis defeituosos", "pixel defeituoso")).toBe(true);
    expect(usesTerm("Anéis de resolução", "anel de resolução")).toBe(true);
    // A placeholder named after the term is not the term.
    expect(usesTerm("{{frames}} Bilder", "Frame")).toBe(false);
  });

  it("leaves short linking words out, so de and del count the same", () => {
    expect(usesTerm("Fija la energía de fotón.", "energía del fotón")).toBe(true);
    expect(usesTerm("grondezza da pixel", "grondezza dal pixel")).toBe(true);
    // A term made only of short words still has to be there.
    expect(usesTerm("Summen", "sum")).toBe(true);
    expect(usesTerm("Total", "sum")).toBe(false);
  });

  it("leaves out a finding the allowlist explains", () => {
    const en = { "upload.hint": "0 means no limit" };
    const locales = [["de", { "upload.hint": "0 bedeutet kein Limit" }]];
    const glossary = parseGlossary("term_id,domain,english_term,definition,do_not_translate,de,notes\nmean,stats,Mean,,false,Mittelwert,");
    expect(collectGlossaryFindings(glossary, en, locales)).toHaveLength(1);
    const isException = (locale, key, term) => term === "mean" && key === "upload.hint";
    expect(collectGlossaryFindings(glossary, en, locales, { isException })).toEqual([]);
  });

  it("reports a translation that leaves the glossary term out, as in #202", () => {
    // The Min/Max cutoffs' message said picture where the glossary says frame.
    const en = { "minmax.none": "No pixel within the cutoffs on this frame.", "other.framework": "A framework." };
    const locales = [
      ["de", { "minmax.none": "Kein Pixel innerhalb der Grenzen in diesem Bild.", "other.framework": "Ein Rahmenwerk." }],
      ["sv", { "minmax.none": "Ingen pixel inom gränserna i den här bildrutan." }],
      ["da", { "minmax.none": "No pixel within the cutoffs on this frame." }],
    ];
    expect(collectGlossaryFindings(GLOSSARY, en, locales)).toEqual([
      {
        key: "minmax.none",
        locale: "de",
        termId: "frame",
        expected: "Frame",
        value: "Kein Pixel innerhalb der Grenzen in diesem Bild.",
      },
    ]);
  });
});

// The locales themselves, against docs/I18N_GLOSSARY.csv. In #202 a new
// message said picture where the glossary says frame in three languages, and
// a later sweep found 352 such strings; after it, only the Māori photon
// energy strings are left, which need a Māori speaker. The baseline may only
// go down: fix a new finding, or add an exception with its reason to
// scripts/i18n_review_allowlist.mjs.
const GLOSSARY_FINDINGS_BASELINE = 5;

function realLocales() {
  const dir = path.join(process.cwd(), "frontend", "locales");
  const read = (name) => JSON.parse(fs.readFileSync(path.join(dir, name), "utf8"));
  const entries = fs
    .readdirSync(dir)
    .filter((name) => name.endsWith(".json") && name !== "en.json")
    .map((name) => [name.slice(0, -5), read(name)]);
  return { en: read("en.json"), entries };
}

describe("the locales against the glossary", () => {
  const glossary = readGlossary(process.cwd());
  const { en, entries } = realLocales();

  it("do not leave out more glossary terms than the baseline", () => {
    const findings = collectGlossaryFindings(glossary, en, entries, { isException: isGlossaryException });
    const listed = findings.map((f) => `${f.locale} ${f.key}: ${f.termId} = ${f.expected}`);
    expect(listed.length, listed.join("\n")).toBeLessThanOrEqual(GLOSSARY_FINDINGS_BASELINE);
  });

  it("keep each exception pointing at a real term and key, with a reason, and still needed", () => {
    const terms = new Set(glossary.map((entry) => entry.id));
    const without = collectGlossaryFindings(glossary, en, entries);
    for (const exception of getGlossaryExceptions()) {
      expect(terms.has(exception.term), exception.term).toBe(true);
      expect(typeof en[exception.key], exception.key).toBe("string");
      expect(exception.reason.length).toBeGreaterThan(10);
      const needed = without.some(
        (f) =>
          f.termId === exception.term &&
          f.key === exception.key &&
          (exception.locales === "*" || exception.locales.includes(f.locale)),
      );
      expect(needed, `${exception.term} ${exception.key} is no longer needed`).toBe(true);
    }
  });
});
