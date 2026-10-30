# 0001 — The frontend has no framework and no build step

**Status:** Accepted — *recorded after the fact*, 2026-10-30; reasons confirmed by the maintainer.

## Context

The interface is a single page: an image canvas with overlays, a side panel,
dialogs. ALBIS ships as a desktop app (PyInstaller) and a Docker image, runs
offline on beamline and lab machines, and is maintained by a small team with
outside contributors. It is meant to keep working for years: a detector
viewer outlives several generations of frontend frameworks.

Two reasons decided it:

- **Longevity, no churn.** A framework brings major versions, deprecations
  and toolchain upgrades that have to be followed whether or not ALBIS needs
  anything new. Plain ES modules on the web platform keep working without
  that upkeep.
- **Offline use and simple packaging.** The frontend has to ship inside the
  desktop app and the Docker image exactly as it is, and run on machines with
  no network — nothing to fetch, build or update at install time.

## Decision

Plain ES modules in `frontend/`, loaded by `index.html` and served as they are
by the backend. No framework, no bundler, no transpiler, no runtime npm
dependencies; npm is used only for development tools (eslint, vitest). The
little third-party code there is lives in `frontend/vendor/` with its licence.

## Consequences

- Edit a file, reload the browser: what is in the folder is what runs, in
  development, in the packaged app and in Docker alike.
- Nothing to fetch or build at install time, and nothing loaded from a CDN, so
  ALBIS works offline (see [NETWORK_AND_PRIVACY.md](../NETWORK_AND_PRIVACY.md)).
- The structure a framework would impose is kept by convention instead: the
  layers and wiring in the developer guide and `frontend/AGENTS.md`, partly
  enforced by eslint.
- Wiring is manual and `app.js`, the composition root, is large; new logic
  goes into modules, not there.
- **Revisit if** the interface needs something the platform cannot do without
  a build step — not merely because a framework would be more familiar.
