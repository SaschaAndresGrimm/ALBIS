## Summary

Describe the change and why it is needed.

## Scope

- [ ] Backend
- [ ] Frontend
- [ ] Packaging/build scripts
- [ ] Docs
- [ ] Tests

## Validation

- [ ] `ruff check backend albis_launcher.py tests scripts test_scripts`
- [ ] `black --check tests scripts test_scripts`
- [ ] `PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest -p pytest_cov --cov=backend --cov-fail-under=77`
- [ ] `npm run lint:js`

## Screenshots / Recordings (UI changes)

Attach before/after screenshots or short videos when relevant.

## Data and compatibility notes

List any format compatibility impacts (HDF5/CBF/TIFF/EDF/monitor/remote stream).

## Checklist

- [ ] Updated docs if behavior changed
- [ ] Added/updated tests for changed behavior
- [ ] No secrets, credentials, or local paths included
