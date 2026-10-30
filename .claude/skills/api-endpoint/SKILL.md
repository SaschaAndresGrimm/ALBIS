---
name: api-endpoint
description: Add or change an ALBIS HTTP endpoint end to end - thin route, logic in a service, strict Pydantic models, PathPolicy for paths, the contract in docs/API_CONTRACTS.md, and the tests (contract, confinement, behaviour). Use for any change under backend/routes/ or to a request or response model.
argument-hint: "[METHOD /api/path]"
---

# Adding or changing an endpoint

The rules are in `backend/AGENTS.md`; this is the order to apply them in and
the places that are easy to forget. An endpoint is also a promise: documented
ones are covered by `docs/COMPATIBILITY.md`, so changing an existing one is
not the same job as adding one.

## 0. Is this a breaking change?

For an endpoint already in `docs/API_CONTRACTS.md`, read
`docs/COMPATIBILITY.md › The HTTP API` first. Adding an optional parameter or
a response field is fine in a minor release. Renaming or removing one,
changing its type or meaning, or changing what a status code means is not.
Keep the old shape working, mark it **Deprecated** in the contract, and log a
warning when it is used, as the `normalize_*` fields under "Combining Frames"
do. If that cannot work, stop and ask the maintainer before going on.

## 1. The service

Logic goes into `backend/services/<area>.py`: plain Python, no FastAPI beyond
an exception it genuinely raises, testable without a server.
`services/known_detectors.py` is the model. Reading HDF5 goes through
`open_hdf5_for_read` and `read_hdf5_array` (`services/hdf5_stack.py`).

## 2. The models

In `backend/api_models.py`:

- Request models derive from `_StrictModel` (unknown fields are a 422) and
  bound every field: `Field(..., max_length=512)`, `ge=`/`le=` on numbers.
- JSON responses get a response model, so the OpenAPI schema is typed.

## 3. The route

In the area's `backend/routes/<area>.py`, with `routes/known_detectors.py` as
the model:

- Parse, check, call the service, answer. Nothing more.
- Whatever it needs comes in through the module's frozen `XRouteDeps`
  dataclass, filled in `backend/app.py`. A new area is a new module with
  `XRouteDeps` and `register_x_routes(app, deps)`, called from `app.py`.
- A path from the request goes through `PathPolicy` (`deps.resolve_image_file`,
  `deps.resolve_dir`, …). Never `Path(user_value)` directly.
- Anything that changes state is POST, PATCH or DELETE, never GET: the request
  guard's cross-site check only covers those.
- Plain `def`, not `async def`, unless the handler never blocks.
- Errors are `HTTPException` with a `detail` a user can read. An expected
  failure (`OSError` writing a file) becomes a logged warning and a 4xx/500
  saying what failed.
- Values from outside (file names, HDF5 paths, detector answers) go through
  `sanitize_log_value` before they are logged.
- A feature that Settings can switch off checks `deps.enabled()` on each
  request, and answers 404 with a message saying where to turn it on.
- The handler's docstring is its OpenAPI description: write it for the caller.

## 4. The tests

In `tests/`, named for the behaviour they protect, each with a docstring
saying what breaks without it:

- Behaviour through `TestClient(app)`, with `tmp_path` for files and
  `tests/fake_simplon.py` for a detector.
- `tests/test_api_contracts.py`: an unknown field in the request is a 422,
  and the OpenAPI response references the response model.
- A route that takes a path: `../` and an absolute path outside the root are
  refused (copy `tests/test_hdf5_storage_confinement.py`).
- The service on its own, without HTTP, for its edge cases.

## 5. The documents

- `docs/API_CONTRACTS.md`: the endpoint under its area's section, with its
  method and path, parameters, what it answers, binary headers if any, and
  its error cases. Mark it **(beta)** if it may still change, as the Detector
  endpoints are.
- An endpoint that reaches out to the network (a detector, an update server):
  `docs/NETWORK_AND_PRIVACY.md`.
- A new config key behind it: `config.py`, `albis.config.schema.json` and
  `docs/POWER_USER_GUIDE.md` (see `backend/AGENTS.md`).
- A frontend caller uses `fetchJSON` / `fetchJSONWithInit` from
  `frontend/modules/http.js`.
- Visible to users: an entry under `## [Unreleased]` in `CHANGELOG.md`.

## 6. Check

```bash
npm run check
```

Then open `http://127.0.0.1:8731/docs` with ALBIS running and read your
endpoint's entry as a caller would.
