import { describe, expect, it } from "vitest";

import { collectGlossaryFindings, parseCsv, parseGlossary, usesTerm } from "../../scripts/i18n_glossary_lib.mjs";

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
