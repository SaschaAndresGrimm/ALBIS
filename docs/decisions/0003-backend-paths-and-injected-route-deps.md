# 0003 — Request paths go through `PathPolicy`; routes get their dependencies injected

**Status:** Accepted — *recorded after the fact*, 2026-10-30.

## Context

The backend serves files from a data root to a browser, which may be on
another machine. File and folder names arrive in requests, and the data files
themselves come from elsewhere: an HDF5 file can link to external or virtual
datasets anywhere on disk, and can carry names with newlines in them.

## Decision

- Every path from a request is resolved by `PathPolicy`
  (`backend/services/path_policy.py`), which confines it to the data root.
  HDF5 external and virtual storage pointing outside the root is refused.
- Route modules declare what they need as a frozen `XRouteDeps` dataclass and
  are wired by `register_x_routes(app, deps)` in `app.py`. They do not import
  the app or reach for module globals.
- Values from outside are passed through `sanitize_log_value` before they are
  logged.

## Consequences

- Confinement is decided in one place and tested there
  (`tests/test_path_policy.py`, `tests/test_hdf5_storage_confinement.py`);
  a new route that takes a path adds such a test.
- Routes are thin and testable with fakes; the wiring in `app.py` is explicit
  and long.
- A route that joins or opens a request path itself bypasses all of this,
  which is why `backend/AGENTS.md` forbids it. Routes still do legitimate path
  work after resolution, so this is a rule for review, not a lint.
- **Revisit if** the backend moves to a framework-level dependency system that
  keeps the same single point of confinement.
