# Translation Review Guide

This guide covers the ALBIS translation review workflow for completeness and technical correctness.

## Run the checks

```bash
npm run test:js
npm run review:i18n
```

`npm run test:js` is the strict gate for locale structure. It must stay green.

`npm run review:i18n` is a report. It does not fail CI by default. It prints:

- untranslated or suspect strings where a locale still matches English and is not allowlisted
- hardcoded user-facing text in `frontend/index.html`, `frontend/app.js`, and `frontend/modules/**/*.js`
- a technical review matrix grouped by ALBIS domain buckets
- glossary terms not used: a string whose English uses a term from
  `I18N_GLOSSARY.csv` but whose translation leaves out that term's listed
  translation (in #202, "Bild" where the glossary says "Frame"). By default only
  the strings changed since `origin/main` are listed, so a pull request sees its
  own; `--since <ref>` compares with another revision, `--glossary-all` lists
  every string. A plural or case ending can be a false alarm, so read each one.

`frontend/tests/i18n_glossary_lib.test.js` holds the whole set to a baseline
that may only go down (5: the Māori photon energy strings, which need a Māori
speaker). A new finding fails `npm run test:js`. Fix the translation, or, when
it is correct as written (a verb where the glossary has the noun, an adjective,
a short label), add an entry with its reason to `GLOSSARY_EXCEPTIONS` in
`scripts/i18n_review_allowlist.mjs`. The same test fails on an exception that is
no longer needed.

When most of a locale uses another word than the glossary, and that word is
right, change the glossary rather than the locale: count first. In November
2026 the German, Swedish and Danish "reflection" became the crystallographic
Reflex / reflex / refleks, Māori "series" became raupapa (47 of 57 strings),
and Italian "detector" became rivelatore (82 of 104).

Future tightening is available through:

```bash
npm run review:i18n -- --strict-untranslated
```

## Review order

1. Fix structural failures first.
2. Review untranslated or suspect strings and decide whether each item should be translated or explicitly allowlisted.
3. Review the technical matrix by domain bucket, starting with:
   - `analysis.*`
   - `data_source.meta`
   - `peaks`
   - `rings`
   - `roi`
   - `series`
   - `validation`
   - relevant `status.*` and `hint.*`
4. Update `docs/I18N_GLOSSARY.csv` before editing locale JSON files.
5. Re-run the checks and confirm the report only contains approved carryovers and expected hardcoded bootstrap fallbacks.

## UI spot-check workflow

Review every locale in the running UI. Use the same workflow for each language:

1. Open a diffraction image and verify Data panel metadata.
2. Open Analysis / Overlay and review peaks, rings, ROI, and validation hints.
3. Exercise Combine Frames and confirm frames, range, every Nth, result, mean, median, sum, and status messages use correct technical terms.
4. Exercise live/autoload modes and confirm SIMPLON, JFJ/JUNGFRAUJOCH, remote stream, mask, threshold, and frame wording.
5. Open Settings and verify language labels and technical viewer settings.

Record any terminology changes in the glossary first, then update locale JSON.

## Allowlist policy

Only allowlist terms that are intentionally shared across languages, such as:

- product or protocol names
- accepted acronyms like `ROI`
- units and symbolic placeholders
- path examples and reference filenames
- colormap names that are intentionally kept as proper names

Do not allowlist a string just because it is difficult to translate. If a user-facing technical term remains English, note the rationale in the glossary.
