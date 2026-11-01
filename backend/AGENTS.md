# Backend notes for coding agents

Read the root [AGENTS.md](../AGENTS.md) first; this adds what applies inside
`backend/`. How the pieces run together is in
[docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md).

## Layout

| Where | What |
| --- | --- |
| `app.py` | Composition root: builds the app, middleware, `PathPolicy`, the stores and services, and calls every `register_*_routes(app, deps)`. Wiring only — no request logic. |
| `routes/*.py` | HTTP endpoints, one module per area. Thin: parse, check, call a service, answer. |
| `services/*.py` | The logic: file formats, HDF5, scanning, exports, detector control, updates. No FastAPI imports beyond what a service genuinely raises. |
| `api_models.py` | Pydantic request and response models. |
| `config.py` | Defaults (`DEFAULT_CONFIG`), validation (`normalize_config`), loading, environment overrides. |
| `image_formats.py` | Readers shared by several endpoints (TIFF, CBF, EDF, MYTHEN, …). |
| `request_guard.py`, `response_compression.py` | Middleware. |

## Where does it go

| You are adding | Touch |
| --- | --- |
| **An endpoint** | A handler in the area's `routes/` module, logic in a `services/` module, request/response models in `api_models.py`, an entry in [docs/API_CONTRACTS.md](../docs/API_CONTRACTS.md), a `TestClient` test. A new area: a new `routes/x.py` with an `XRouteDeps` dataclass and `register_x_routes`, wired in `app.py`. |
| **A config key** | `DEFAULT_CONFIG` and `normalize_config` in `config.py`, `albis.config.schema.json`, [docs/POWER_USER_GUIDE.md](../docs/POWER_USER_GUIDE.md). Two tests in `tests/test_documentation_consistency.py` fail until the schema and the guide agree with the code. An `ALBIS_<SECTION>_<KEY>` environment override comes for free. If users set it in Settings: the Settings dialog, its i18n keys, the user guide. |
| **A file format** | The reader in `image_formats.py` (or a service), the extension in `AUTOLOAD_EXTS` in `app.py`, the README format list and the in-app help (a test fails otherwise), the frontend's `file_type_utils.js`. |
| **A background job** | A service holding its state, routes to start, poll and cancel it — `services/series_summing.py` and `routes/analysis.py` are the model. |

## Rules

- **Dependencies are injected.** A route module declares a frozen
  `XRouteDeps` dataclass of what it needs (callables, stores, the logger);
  `app.py` fills it. Routes do not import `app.py` or reach for globals.
- **Paths from a request go through `PathPolicy`** (`deps.resolve_image_file`,
  `resolve_dir`, …), which confines them to the data root. A new route that
  takes a path gets a test that `../` and an absolute path outside the root
  are refused — see `tests/test_hdf5_storage_confinement.py`.
- **HDF5** is opened with `open_hdf5_for_read` and read through
  `read_hdf5_array` (`services/hdf5_open.py`): they handle file locking and
  tell a lock failure apart from a broken file.
- **Handlers are plain `def`.** FastAPI runs them in a thread pool, which is
  what file and HDF5 work needs. `async def` only for a handler that never
  blocks; `routes/files.py` explains why its upload is deliberately not async.
- **Errors**: `HTTPException` with a `detail` a user can read ("Not a detector
  address."). Turn an expected failure (`OSError` writing a file) into a
  logged warning and a 4xx/500 that says what failed, as
  `routes/known_detectors.py` does; never let a traceback be the answer.
- **Request models** derive from `_StrictModel` (unknown fields are a 422)
  and bound their fields (`Field(..., max_length=512)`).
- **Changes are POST, PATCH or DELETE**, never GET: the request guard's
  cross-site check only covers those.
- **Feature switches are read per request** (`deps.enabled()`), since Settings
  can change while ALBIS runs; a switched-off feature answers 404 with a
  message saying where to turn it on.
- **Logging**: the `albis` logger from the deps. A value that came from
  outside — a file name, an HDF5 path, a detector's answer — goes through
  `sanitize_log_value` (`services/log_safety.py`) first.
- **Docstrings on handlers** are the OpenAPI description; write them for the
  person calling the endpoint.

## Tests

`tests/`, pytest. `TestClient(app)` for endpoints, `tmp_path` for files, the
fake detector in `tests/fake_simplon.py` for SIMPLON. Tests are named for the
behaviour they protect (`test_external_raw_storage_pointing_outside_the_root_is_refused`)
and their docstring says what broke, or would, without them.

## Copy these

- **An area end to end**: `routes/known_detectors.py` (77 lines: deps, models,
  error mapping) with `services/known_detectors.py` and
  `tests/test_known_detectors.py`.
- **A small service with a security guarantee**: `services/path_policy.py`
  and `services/log_safety.py`, each with its test.

Do not take `app.py`'s size as licence: it is the composition root, and new
logic belongs in a service. `tests/test_code_size_guard.py` fails when a
module over 1,000 lines gets longer, or a new one passes 1,000.
