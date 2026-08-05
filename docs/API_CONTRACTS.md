# ALBIS API Contracts

This document describes the stable HTTP contract for ALBIS clients and integrations.

Related docs: [Developer Guide](DEVELOPER_GUIDE.md) · [Architecture](ARCHITECTURE.md) · [Code Map](CODE_MAP.md)

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

### CSV export

- `GET /api/hdf5/csv`

Response:

- `text/csv`
- `Content-Disposition` attachment header

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
- `502` responses from `/api/simplon/monitor`, `/api/simplon/mask`, and `/api/simplon/mode` carry the
  same classification as an object detail — `{"detail": {"summary", "code", ...}}` — so clients can
  localize the reason instead of showing a generic transport error.
- `url` accepts a bare hostname or IP; `http://` and port 80 are assumed when omitted, and a pasted
  `/monitor/api/<version>` path is normalized away.

## Client Guidance

- Prefer schema-driven clients from `/openapi.json` for JSON endpoints.
- For binary endpoints, always decode payloads using `X-Dtype` + `X-Shape` rather than assumptions.
- Handle `204` as a normal polling outcome for stream-style endpoints.
- Treat `409` on `/api/remote/v1/meta` as a retry signal with current sequence from response body.
