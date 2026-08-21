# ALBIS API Contracts

This document describes the HTTP contract for ALBIS clients and integrations: the
endpoints, their payloads and their headers.

How stable that contract is — which parts a version number promises not to break,
and how anything covered is deprecated before it is removed — is
[Compatibility Policy](COMPATIBILITY.md). Read that one before building against this one.

Related docs: [Compatibility Policy](COMPATIBILITY.md) · [Developer Guide](DEVELOPER_GUIDE.md) · [Architecture](ARCHITECTURE.md) · [Code Map](CODE_MAP.md)

OpenAPI source of truth:

- `GET /openapi.json`

Code source of truth:

- `backend/api_models.py`
- `backend/routes/*.py`

## Contract Principles

- Request bodies use strict Pydantic models (`extra = "forbid"`), so unknown JSON fields are rejected with `422`.
- JSON responses use explicit response models for typed schema generation.
- Binary endpoints explicitly document:
  - media type (`application/octet-stream`),
  - payload format (`string` with `binary` format in OpenAPI),
  - response headers required for decoding.
- `204 No Content` is used when "no current payload" is a valid non-error outcome.

## Binary Endpoints

### Frame and image payloads

- `GET /api/frame`
- `GET /api/mask`
- `GET /api/image`
- `GET /api/simplon/monitor`
- `GET /api/simplon/mask`
- `GET /api/remote/v1/latest`

Common headers:

- `X-Dtype`: NumPy dtype string used to decode raw bytes.
- `X-Shape`: comma-separated dimensions.
- `X-Frame`: frame index (where applicable).

Endpoint-specific headers are documented in OpenAPI for each route:

- `X-Mask-Path` for HDF5 pixel-mask source.
- `X-Simplon-*` for SIMPLON metadata.
- `X-Remote-*` for remote stream metadata and sequence control.
- `X-Scan-Truncated` for a directory scan that stopped at its budget (see Directory Scans).

### Transfer encoding

Frames are raw pixel bytes, so a single large frame runs to tens of megabytes.
These responses honor `Accept-Encoding` for remote clients, serving **zstd** when
the client offers it and **gzip** otherwise (see `server.compression` in
`albis.config.schema.json`; loopback clients are not compressed, since the transfer
was already local).

This is transport-level only and changes nothing about the payload contract:

- The decoded bytes are identical to the uncompressed response.
- `X-Dtype`, `X-Shape`, `X-Frame` and the endpoint-specific headers are unaffected.
- `Vary: Accept-Encoding` is set, so intermediate caches key on the encoding.
- Clients sending `Accept-Encoding: identity` receive uncompressed bytes.
- A client that accepts neither codec receives uncompressed bytes.

Standard content negotiation applies, so a client is never sent an encoding it did
not advertise. Browsers decode transparently, including for
`response.arrayBuffer()`, and HTTP libraries with automatic content decoding
(`requests`, `httpx`, `curl --compressed`) need no changes either.

`GET /api/health` reports `compression_encodings`: the encodings the running build
can produce, best first. zstd needs an optional native extension, so a build
without it reports `["gzip"]`.

### CSV export

- `GET /api/hdf5/csv`

Response:

- `text/csv`
- `Content-Disposition` attachment header

## Directory Scans

`GET /api/files`, `GET /api/folders` and `GET /api/autoload/latest` walk a
directory, and a walk is bounded by `data.max_scan_entries` and
`data.max_scan_seconds` (see the Power User Guide). A scan that reaches either
budget stops early, and every one of these endpoints reports that rather than
presenting a partial answer as a complete one:

- `truncated: true` in the JSON body of all three.
- `X-Scan-Truncated: 1` on `GET /api/autoload/latest`, on the `200` and on the
  `204` alike — a bodyless response still has to be able to say "there is no
  newest file *because the walk ran out*", which is not the same answer as "this
  folder is empty". The header is absent when the scan completed.

Clients that poll should treat truncation as a standing condition to surface, not
an error: the newest file may be one the walk never reached.

## Remote Stream Semantics

- `POST /api/remote/v1/frame`: ingest a frame and metadata.
- `GET /api/remote/v1/latest`:
  - returns `200` with raw bytes when a frame is available,
  - returns `204` when no frame is available or no newer frame exists for `after_seq`.
- `GET /api/remote/v1/meta`:
  - returns `200` with `RemoteMetaResponse`,
  - returns `204` when no frame exists for source,
  - returns `409` with `RemoteMetaConflictResponse` when requested `seq` is no longer current.

## Handoff Semantics

- `POST /api/handoff/v1/jobs`: ingest one handoff manifest path and enqueue a typed job payload.
- `GET /api/handoff/v1/jobs/latest`:
  - returns `200` with `HandoffJobResponse` when a newer job than `after_id` exists,
  - returns `204` when no newer handoff job exists.
- Queue behavior:
  - bounded in-memory queue (max 1024 jobs),
  - oldest entries are evicted when full,
  - job IDs remain monotonic.

## JUNGFRAUJOCH Preview Bridge

- `POST /api/jfjoch/preview/start`: start or reconfigure backend ZeroMQ preview subscription.
- `POST /api/jfjoch/preview/stop`: stop active subscription worker.
- `GET /api/jfjoch/preview/status`: return current worker state and latest ingest counters.
- Preview frames are exposed through existing Remote Stream endpoints under configured `source_id`.

## SIMPLON Diagnostics

- `GET /api/simplon/probe`: test whether a SIMPLON monitor API answers at an address.
- A dead or misconfigured detector is a **successful** probe: the route returns `200` with
  `status: "error"` plus a classified `code`. Only an unusable address returns `400`.
- `code` vocabulary: `ok`, `dns`, `refused`, `timeout`, `api_missing`, `http_error`, `unreachable`.
  Accompanying fields carry the specifics: `port` (refused), `http_status`, `api_version`, `timeout_s`.
- On success the payload also reports `detector` and `serial`, so a client can confirm the address
  points at the intended instrument.
- When the configured API version is absent but a known one answers, `code` is `ok_other_version`,
  `api_version` holds the version that worked and `requested_version` the one asked for.
- `502` responses from `/api/simplon/monitor`, `/api/simplon/mask`, and `/api/simplon/mode` carry the
  same classification as an object detail — `{"detail": {"summary", "code", ...}}` — so clients can
  localize the reason instead of showing a generic transport error.
- `url` accepts a bare hostname or IP; `http://` and port 80 are assumed when omitted, and a pasted
  `/monitor/api/<version>` path is normalized away.

## JUNGFRAUJOCH Endpoint Diagnostics

- `GET /api/jfjoch/probe`: reachability check for a preview endpoint. `endpoint` accepts a bare
  `host:port` — `tcp://` is added — but a port is required, since JUNGFRAUJOCH has no default.
- **TCP reachability only.** A successful probe means the host resolves and the port accepts
  connections; it cannot confirm the peer is a JUNGFRAUJOCH publisher. Use
  `GET /api/jfjoch/preview/status` for whether frames actually arrive.
- `code` vocabulary: `ok`, `not_probed` (path transports such as `ipc://`), `dns`, `refused`,
  `timeout`, `unreachable`. An unusable endpoint returns `400` with wording that says what to enter.
- `POST /api/jfjoch/preview/start` validates the endpoint the same way, so a missing port now fails
  with an actionable `400` instead of an opaque ZeroMQ error later.

## Client Guidance

- Prefer schema-driven clients from `/openapi.json` for JSON endpoints.
- For binary endpoints, always decode payloads using `X-Dtype` + `X-Shape` rather than assumptions.
- Handle `204` as a normal polling outcome for stream-style endpoints, and read
  `X-Scan-Truncated` on it rather than assuming an empty directory.
- Treat `409` on `/api/remote/v1/meta` as a retry signal with current sequence from response body.
