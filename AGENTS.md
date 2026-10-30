# Notes for coding agents

ALBIS is a viewer for DECTRIS detector data: a Python backend (`backend/`) and a
browser frontend (`frontend/`, plain ES modules, no build step). Read
[CONTRIBUTING.md](CONTRIBUTING.md) first; this file is the short version of the
rules that are easy to miss.

## Before you change the interface

Read [docs/UI_GUIDE.md](docs/UI_GUIDE.md). In short:

- Colours, text sizes, corners, borders and stacking come from the tokens in
  `:root` of `frontend/style.css`. `frontend/tests/style_guard.test.js` fails
  on new literals; use a token, or add one with a comment saying what it is for.
- Reuse the drawn controls (checkbox, dropdown, chevron, number stepper, Auto
  toggle). One `.btn-primary` per dialog, rightmost; Close and Cancel are
  secondary.
- Anything floating over the image is nearly opaque and frosted; the
  `--surface-well` inset is only for inside panels and dialogs.
- Every visible string goes through `data-i18n*` or `t()`, with the key in all
  13 files in `frontend/locales/` ([docs/I18N_REVIEW_GUIDE.md](docs/I18N_REVIEW_GUIDE.md)).
- Never `outline: none` without a `:focus-visible` replacement ring.

## Checks

```bash
npm run test:js                 # frontend tests, style guard, locale parity
npm run lint:js
ruff check backend albis_launcher.py tests scripts test_scripts
black --check backend albis_launcher.py tests scripts test_scripts   # CI formats with black
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest -p pytest_cov --cov=backend --cov-fail-under=77
```

For a visible change, screenshot every window before and after:

```bash
.venv/bin/python -m uvicorn backend.app:app --port 8731 &
npm run ui:snapshots -- --out /tmp/albis-ui
```

Look at the screenshots — tests do not see layout. The script never writes
into the data folder; uploading a file through the page would, so do not test
that way.

## Changes and history

- User-visible changes get an entry under `## [Unreleased]` in
  [CHANGELOG.md](CHANGELOG.md): a bold first sentence saying what changed for
  the user, then why, in plain language.
- Commit messages: `type(scope): summary` (`feat`, `fix`, `style`, `docs`, …),
  with the reasoning in the body.
- Update [docs/USER_GUIDE.md](docs/USER_GUIDE.md) when a control's behaviour
  or name changes.

## Where things are

[docs/CODE_MAP.md](docs/CODE_MAP.md) maps the code;
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) and
[docs/DEVELOPER_GUIDE.md](docs/DEVELOPER_GUIDE.md) explain how it fits
together.
