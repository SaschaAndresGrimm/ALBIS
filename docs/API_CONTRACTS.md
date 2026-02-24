# ALBIS API Contracts

This document describes the stable HTTP contract for ALBIS clients and integrations.

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
- `GET /api/preview`
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

- `X-Preview` for preview payloads.
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

## Client Guidance

- Prefer schema-driven clients from `/openapi.json` for JSON endpoints.
- For binary endpoints, always decode payloads using `X-Dtype` + `X-Shape` rather than assumptions.
- Handle `204` as a normal polling outcome for stream-style endpoints.
- Treat `409` on `/api/remote/v1/meta` as a retry signal with current sequence from response body.
