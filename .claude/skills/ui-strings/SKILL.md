---
name: ui-strings
description: Add, rename or reword a visible ALBIS string - the data-i18n attribute or t() call, the key in all 13 locale files, translations that follow the glossary, and the checks. Use whenever a change adds or alters text the user sees.
argument-hint: "[key.name]"
---

# Adding or changing a visible string

Every string the user sees goes through `data-i18n*` in markup or `t()` in
JavaScript, with its key in all 13 files in `frontend/locales/`. The locale
tests fail on a key missing from any language or on a placeholder that does
not match English, so a half-done string does not get through CI, but a
careless translation does. Translation rules are in
`docs/I18N_REVIEW_GUIDE.md`; fixed terms are in `docs/I18N_GLOSSARY.csv`.

## 1. Pick the key and wire it

- Reuse an existing key when the text and meaning are the same. Search
  `frontend/locales/en.json` first.
- Otherwise, name the new key after its place, as its neighbours are named:
  `settings.viewer.…`, `analysis.roi.…`, `status.…`, `hint.…`.
- Markup: `data-i18n` for the text, `data-i18n-title`,
  `data-i18n-aria-label`, `data-i18n-placeholder` for attributes. Keep the
  English text in the element as the fallback, as the elements around it do.
- JavaScript: `t("key", { vars })` from `modules/i18n.js`. Never build a
  sentence by concatenating translated pieces: word order differs between
  languages. Put the whole sentence in one key with placeholders.
- Placeholders are `{{name}}`. A plural is
  `{{plural:count|singular|plural}}`, picked by whether `count` is 1; English
  uses it for the ending, as in `{{count}} image{{plural:count||s}}`. Every
  language must use the same variable names as English, and may write whole
  words into the two branches where its grammar needs to.

## 2. Write the English, then translate

Write the English as the interface speaks: short, the control's own names,
sentence case. Then translate it into all 12 other languages, `da de es fr
gsw it ja mi pt rm sv zh-CN`.

- Look the technical terms up in `docs/I18N_GLOSSARY.csv` and use its
  translation. A new technical term goes into the glossary first, with a
  translation for each language, then into the locales.
- `gsw` is Swiss German, `rm` Romansh, `mi` Māori. Do not fill them with
  standard German or English.
- Keep product, protocol and format names (SIMPLON, JUNGFRAUJOCH, HDF5, ROI)
  and units as they are. A string left identical to English on purpose goes
  into `scripts/i18n_review_allowlist.mjs`, and only for the reasons the
  review guide lists.
- When you reword English, the other languages need retranslating too. A key
  whose English changed but whose translations did not is wrong in 12
  languages.

## 3. Write the files

The locale files are sorted by key, indented by two spaces, non-ASCII written
as is, with a trailing newline. Keep it that way so diffs stay small. One way,
with the new keys in a scratch file `/tmp/new_keys.json` shaped
`{"key": {"en": "…", "de": "…", …}}`:

```bash
.venv/bin/python - /tmp/new_keys.json <<'EOF'
import json, sys
from pathlib import Path
new = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
for path in sorted(Path("frontend/locales").glob("*.json")):
    lang = path.stem
    data = json.loads(path.read_text(encoding="utf-8"))
    for key, texts in new.items():
        data[key] = texts[lang]  # KeyError: a language is missing
    path.write_text(json.dumps(dict(sorted(data.items())), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
EOF
```

Removing a key: drop it from all 13 files, and check that no
`data-i18n*` or `t()` still uses it.

## 4. Check

```bash
npm run test:js        # locale parity and placeholder checks
npm run review:i18n    # report: carryovers, hardcoded text in markup and JS
```

`review:i18n` does not fail, so read its output: your new key must not show
up as a suspect carryover, and your markup must not show up as hardcoded
text. Then look at the string in place with the `ui-check` skill, in English
and in one long-worded language (`de` or `rm`), to see that it fits.
