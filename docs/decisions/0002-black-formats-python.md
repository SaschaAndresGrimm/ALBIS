# 0002 — Black formats the Python, not `ruff format`

**Status:** Accepted — *recorded after the fact*, 2026-10-30.

## Context

Ruff lints the Python and can also format it. Black and `ruff format` mostly
agree, but not always: `ruff format` rewrites some files black considers
correct.

## Decision

Black is the formatter (`black --check` in CI, the black hook in
`.pre-commit-config.yaml`); ruff only lints. `ruff-format` is deliberately
absent from the pre-commit configuration, and its versions are pinned in step
with `requirements-dev.txt`.

## Consequences

- One formatter, one answer: with a `ruff-format` hook installed, a
  contributor's commits would have been reformatted into a state CI then
  rejects — which is why the hook is not there.
- Do not run `ruff format`, and do not "simplify" the setup by switching to
  it without changing CI, the hook and the codebase in one go.
- **Revisit if** the two formatters converge, or if moving to `ruff format` is
  done deliberately as one change across CI, hooks and code.
