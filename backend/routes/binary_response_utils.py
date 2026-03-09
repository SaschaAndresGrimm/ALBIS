"""Reusable binary endpoint docs and header helpers."""

from __future__ import annotations

from typing import Any


def octet_stream_responses(
    description: str,
    headers: dict[str, str],
    include_no_content: bool = False,
) -> dict[int, dict[str, Any]]:
    responses: dict[int, dict[str, Any]] = {
        200: {
            "description": description,
            "content": {
                "application/octet-stream": {
                    "schema": {"type": "string", "format": "binary"}
                }
            },
            "headers": {
                name: {"description": header_desc, "schema": {"type": "string"}}
                for name, header_desc in headers.items()
            },
        }
    }
    if include_no_content:
        responses[204] = {"description": "No matching frame payload available."}
    return responses


def build_binary_headers(
    *,
    dtype: str,
    shape: tuple[int, ...] | list[int],
    frame: int | str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, str]:
    headers = {
        "X-Dtype": str(dtype),
        "X-Shape": ",".join(str(v) for v in shape),
    }
    if frame is not None:
        headers["X-Frame"] = str(frame)
    if extra:
        for key, value in extra.items():
            if value is None:
                continue
            headers[str(key)] = str(value)
    return headers


def add_optional_header(headers: dict[str, str], name: str, value: Any) -> None:
    if value is None:
        return
    if isinstance(value, str) and not value:
        return
    headers[name] = str(value)
