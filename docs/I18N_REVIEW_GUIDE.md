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
3. Exercise Series Operations and confirm normalization, mean, median, sum, range, and status messages use correct technical terms.
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
