# Compatibility Policy

This document says what a version number promises, so that "ALBIS follows
semantic versioning" means something specific rather than something reassuring.

Related docs: [API Contracts](API_CONTRACTS.md) · [Power User Guide](POWER_USER_GUIDE.md) · [Security Policy](../SECURITY.md)

## Status

ALBIS is `0.x`. Until `1.0.0`, anything below may change in a minor release, and
the changelog is the record of what did. This policy describes what takes effect
at `1.0.0` and is published now so that it can be argued with before it binds.

## What a version number means

Given `MAJOR.MINOR.PATCH`:

- **PATCH** — fixes. Nothing in the covered surfaces changes shape.
- **MINOR** — additions. New endpoints, new response fields, new configuration
  keys, new formats. Existing behaviour keeps working without changes on the
  client's side.
- **MAJOR** — a covered surface changed in a way that can break a working
  setup, after the deprecation period below.

## Covered surfaces

These are the promises. A change that breaks one of them waits for a major
release.

### The HTTP API

- **Paths and methods** of documented endpoints keep working.
- **Query parameters** keep their names, types and meaning. A new parameter is
  optional, and omitting it keeps the previous behaviour.
- **Response fields** keep their names, types and meaning. Fields are added, not
  renamed or repurposed; a client that ignores unknown fields keeps working.
  Request bodies reject unknown fields (`extra = "forbid"`), so a client must
  not send fields it invented.
- **Status codes** for a given outcome keep their meaning, including the
  `204`-means-nothing-to-report convention on stream-style endpoints.
- **Binary response headers** — `X-Dtype`, `X-Shape`, `X-Frame`, and the
  documented `X-Mask-*`, `X-Simplon-*`, `X-Remote-*` and `X-Scan-*` headers —
  keep their names and encoding. Payload bytes stay decodable from `X-Dtype` and
  `X-Shape` alone.
- **Classification vocabularies** (the `code` values on the SIMPLON and
  JUNGFRAUJOCH probes) keep their existing members. Values are added, so a
  client must treat an unrecognised code as "something else" rather than an
  error.

`/openapi.json` is the machine-readable form of all of this, and
`tests/test_openapi_key_contract_baseline.py` pins a snapshot of the key
endpoints: a change to a covered surface fails CI and has to be made
deliberately, by updating the snapshot in the same commit.

The `v1` in `/api/remote/v1/*` and `/api/handoff/v1/*` marks the two surfaces
built for external producers, where a second shape may one day have to exist
alongside the first. It is not a statement that the rest is less stable — the
promise above covers every documented endpoint.

### Configuration

- **Keys keep their names, types, defaults and meaning.** A default only changes
  in a major release; a *new* key may arrive in a minor one.
- **A removed key is ignored rather than fatal**, and says so in the log.
- `albis.config.schema.json` describes the file, and is checked against the code
  by `tests/test_documentation_consistency.py`, so the schema cannot drift from
  what ALBIS actually reads.
- Config files are forward-compatible within a major version: a file written by
  `1.0` is read by `1.9`.

### Exported files

- **TIFF and CBF layouts** stay readable by the same tools. The pixel
  conventions do not change: masked gaps are `-1`, bad or saturated pixels are
  `-2`.
- **Provenance stays present.** Every written file names the ALBIS build that
  produced it, the source it came from, and the substitutions applied. The
  wording may improve; the facts stay.

### Command surface

- Documented keyboard shortcuts keep their actions.
- The launcher keeps starting from the same paths, and packaged artifact naming
  keeps the `v<version>-<commit>` convention that release automation depends on.

## Not covered

These change whenever there is a reason, including in a patch release:

- **Log format and log levels.** Read them, don't parse them.
- **The frontend's own calls to the backend.** The two ship in one bundle and
  are versioned together; a request the interface makes to itself is an internal
  detail even when it goes over HTTP.
- **Internal module layout.** `backend/services/*`, `frontend/modules/*`,
  function names, and anything not reachable through the API.
- **Undocumented endpoints, parameters and headers.** If it is not in
  [API Contracts](API_CONTRACTS.md) or `/openapi.json`, it is not a promise.
- **Diagnostic and debug output**, including `/api/log-tail` content and the
  contents of `Copy build info`.
- **Performance characteristics**, including scan budgets, cache TTLs and how
  many entries a listing returns before it reports itself truncated.
- **Translations.** Wording changes in any release; the keys behind them are
  internal.

## Deprecation

Something covered above is removed in three steps, never fewer:

1. **Announced** in a minor release: the changelog says what is deprecated, what
   replaces it, and the earliest version that may remove it. The replacement
   ships in the same release, so nobody has to choose between an old and a
   missing feature.
2. **Warned** for at least one full minor release: using it logs a warning
   naming the replacement. Deprecated endpoints keep answering, deprecated
   config keys keep working.
3. **Removed** in the next major release, listed in the changelog under a
   heading a reader can find.

A security fix may move faster. When it does, the release notes say so plainly
and name what changed — see the [Security Policy](../SECURITY.md).

## Reporting a break

If an upgrade within a major version breaks a working setup, that is a bug in
this policy's application, not a change you have to absorb: open an issue with
the two versions, the request or config involved, and what changed. Compatibility
promises are only worth what their enforcement is worth, and a report is how the
missing test gets written.
