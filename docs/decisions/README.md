# Decision records

Why ALBIS is built the way it is, where the choice is deliberate and might
look, to someone new, like something to fix. One short record per decision;
the format is in [docs/AGENTS.md](../AGENTS.md#decision-records).

Records marked *recorded after the fact* describe a choice that predates this
folder, from what the code, docs and history show, with the reasons confirmed
by the maintainer.

| # | Decision |
| --- | --- |
| [0001](0001-frontend-without-framework-or-build.md) | The frontend has no framework and no build step |
| [0002](0002-black-formats-python.md) | Black formats the Python, not `ruff format` |
| [0003](0003-backend-paths-and-injected-route-deps.md) | Request paths go through `PathPolicy`; routes get their dependencies injected |
| [0004](0004-drawn-controls-and-design-tokens.md) | The interface draws its own controls and takes its values from tokens |
| [0005](0005-app-elements-table-and-stable-wrappers.md) | `app.js` takes its elements from one table; late-created controllers keep stable wrappers |
