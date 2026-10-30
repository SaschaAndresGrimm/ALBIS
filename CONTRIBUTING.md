# Contributing to ALBIS

Thanks for considering a contribution! ALBIS is an ALBULA-style viewer for DECTRIS detector data. Contributions that improve usability, stability, and workflows are welcome.

## Quick start (dev)

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r backend/requirements.txt
python albis_launcher.py
```

Default config uses `server.port: 0` (random free port), and the launcher opens the browser for you.
For the backend on its own, without the browser launch, run `python -m uvicorn backend.app:app` and open the
URL Uvicorn prints.

Install contributor tooling (or run `scripts/dev_setup.sh`, which sets up the
virtualenv, the tooling and the pre-commit hooks in one go):

```bash
pip install -r requirements-dev.txt
npm ci
```

Before opening a pull request, `npm run check` runs every gate CI runs.

## Contributor docs

- Developer Guide: `docs/DEVELOPER_GUIDE.md`
- Architecture: `docs/ARCHITECTURE.md`
- Code Map: `docs/CODE_MAP.md`
- API Contracts: `docs/API_CONTRACTS.md`
- Release process checklist: `docs/RELEASE_CHECKLIST.md`
- Translation review guide: `docs/I18N_REVIEW_GUIDE.md`
- UI guide (tokens, controls, dialogs, checking a change): `docs/UI_GUIDE.md`
- Notes for coding agents: `AGENTS.md`

## How to help

- Bug reports with clear steps and sample data (if shareable).
- UX improvements for use at the instrument.
- Performance improvements for large datasets.
- Packaging and distribution tooling.

## Reporting issues

Please include:

- OS + version
- How you installed/ran ALBIS
- Steps to reproduce
- Expected vs. actual behavior
- Logs or console errors

Use the GitHub issue forms:

- Bug report: `.github/ISSUE_TEMPLATE/bug_report.yml`
- Feature request: `.github/ISSUE_TEMPLATE/feature_request.yml`

## Pull requests

- Keep changes focused and small.
- Describe the motivation and approach.
- Include screenshots or recordings for UI changes.
- Update docs when behavior changes.
- Update `CHANGELOG.md` for user-visible changes.
- If you add/reshape major logic paths, update `docs/DEVELOPER_GUIDE.md`.
- If you move frontend module boundaries or callback/context contracts, update the frontend module map in `docs/CODE_MAP.md`.

## Using coding agents

Contributions written with coding agents (Claude Code, Codex, Copilot, Cursor,
…) are welcome on the same terms as any other:

- Agents read [`AGENTS.md`](AGENTS.md) (Claude Code through `CLAUDE.md`,
  which imports it). It holds the rules, the checks and the pitfalls; keep it
  current when you change how the project works.
- You are the author. Read and understand every line before you open the pull
  request, and be ready to explain it in review.
- Agent work goes through a branch and a pull request with green CI, like
  everyone's.
- The shared `.claude/settings.json` allows the routine checks and blocks
  force-pushes, `rm -rf` and reading key files. Put personal settings in
  `.claude/settings.local.json` and personal instructions in
  `CLAUDE.local.md`; both stay out of the repository.

## Local checks before PR

`npm run check` runs the first five of these, as CI does:

```bash
ruff check backend albis_launcher.py tests scripts test_scripts
black --check backend albis_launcher.py tests scripts test_scripts
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest -p pytest_cov --cov=backend --cov-report=term-missing --cov-report=xml --cov-fail-under=77
npm run lint:js
npm run test:js
npm run review:i18n
```

Optional:

```bash
pre-commit install
pre-commit run --all-files
```

## Repository governance

- How the project is run, and what you can and cannot rely on: [GOVERNANCE.md](GOVERNANCE.md).
- Where to ask for help and what response to expect: [SUPPORT.md](SUPPORT.md).
- Code owners are defined in `CODEOWNERS`.
- PR template is in `.github/PULL_REQUEST_TEMPLATE.md`.
- Branch protection can be configured with:

```bash
./scripts/setup_branch_protection.sh main
```

Requirements:

- GitHub CLI (`gh`) installed
- Authenticated with repository admin permissions

## Code style

- Use clear, readable names.
- Prefer small helper functions over long blocks.
- Keep CSS and JS changes aligned with existing patterns; for the interface, follow `docs/UI_GUIDE.md` (enforced in part by `frontend/tests/style_guard.test.js`).

## License

By contributing, you agree that your contributions will be licensed under the repository’s license.
