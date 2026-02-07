# Contributing to ALBIS

Thanks for considering a contribution! This project is a practical, ALBULA‑style viewer
for large HDF5 diffraction data. Contributions that improve usability, stability, and
scientific workflows are welcome.

## Quick start (dev)

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r backend/requirements.txt
python backend/app.py
```

Open `http://localhost:8000`.

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

## Code style

- Use clear, readable names.
- Prefer small helper functions over long blocks.
- Keep CSS and JS changes aligned with existing patterns.

## License

By contributing, you agree that your contributions will be licensed under the repository’s
license.
