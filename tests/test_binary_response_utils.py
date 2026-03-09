from __future__ import annotations

from backend.routes.binary_response_utils import (
    add_optional_header,
    build_binary_headers,
    octet_stream_responses,
)


def test_octet_stream_responses_includes_binary_schema_and_optional_204() -> None:
    responses = octet_stream_responses(
        "Binary payload",
        {"X-Test": "Test header"},
        include_no_content=True,
    )
    assert 200 in responses
    assert 204 in responses
    schema = responses[200]["content"]["application/octet-stream"]["schema"]
    assert schema == {"type": "string", "format": "binary"}
    assert "X-Test" in responses[200]["headers"]


def test_build_binary_headers_and_optional_header() -> None:
    headers = build_binary_headers(dtype="<u2", shape=(4, 6), frame=3, extra={"X-Mode": "sum"})
    assert headers["X-Dtype"] == "<u2"
    assert headers["X-Shape"] == "4,6"
    assert headers["X-Frame"] == "3"
    assert headers["X-Mode"] == "sum"

    add_optional_header(headers, "X-Optional", 42)
    add_optional_header(headers, "X-Empty", "")
    add_optional_header(headers, "X-None", None)
    assert headers["X-Optional"] == "42"
    assert "X-Empty" not in headers
    assert "X-None" not in headers
