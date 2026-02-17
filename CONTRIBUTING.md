# Contributing to ALBIS

Thanks for considering a contribution! This project is a practical, ALBULA‑style viewer for HDF5 diffraction data. Contributions that improve usability, stability, and workflows are welcome.

## Quick start (dev)

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r backend/requirements.txt
python backend/app.py
```

Open `http://localhost:8000`.

Install contributor tooling:

```bash
pip install -r requirements-dev.txt
npm ci
```

## Contributor docs

- Architecture and data flow: `docs/ARCHITECTURE.md`
- Function/file navigation map: `docs/CODE_MAP.md`

## How to help

- Bug reports with clear steps and sample data (if shareable).
- UX improvements for beamline use.
- Performance improvements for large datasets.
- Packaging and distribution tooling.

## Reporting issues

Please include:
- OS + version
- How you installed/ran ALBIS
- Steps to reproduce
- Expected vs. actual behavior
- Logs or console errors

## Pull requests

- Keep changes focused and small.
- Describe the motivation and approach.
- Include screenshots or recordings for UI changes.
- Update docs when behavior changes.
- If you add/reshape major logic paths, update `docs/ARCHITECTURE.md` or `docs/CODE_MAP.md`.

## Local checks before PR

```bash
ruff check backend tests scripts test_scripts
black --check tests scripts test_scripts
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest
npm run lint:js
```

Optional:

```bash
pre-commit install
pre-commit run --all-files
```

## Code style

- Use clear, readable names.
- Prefer small helper functions over long blocks.
- Keep CSS and JS changes aligned with existing patterns.

## License

By contributing, you agree that your contributions will be licensed under the repository’s license.
