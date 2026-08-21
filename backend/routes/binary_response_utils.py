"""Reusable binary endpoint docs and header helpers."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

# Everything printable in ASCII except `%`, which has to be escaped itself so
# the encoding round-trips through one `decodeURIComponent` on the client.
_HEADER_SAFE = "".join(chr(code) for code in range(0x20, 0x7F) if code != 0x25)

# Generous for a label, small enough that the fifteen-odd `X-*` headers on a
# frame response stay far below the ~8 KiB header block servers and proxies
# accept. h11 aside, an oversized header is unreadable to the client anyway.
_HEADER_VALUE_MAX = 512


def sanitize_header_value(value: Any) -> str:
    """Make an arbitrary string safe to carry in an HTTP response header.

    Frame metadata reaches these headers straight from an external producer --
    a remote-stream `display_name`, a JUNGFRAUJOCH `series_unique_id`, an HDF5
    dataset path -- and none of it is guaranteed to be header-shaped. Left raw,
    three separate things break the response for that source until a clean
    frame replaces it: a non-Latin-1 character raises inside Starlette (500), a
    CRLF makes h11 abort the connection with no response at all, and a very
    long value pushes the header block past what the client will read.

    Percent-encoding on a printable-ASCII safe set solves all three at once: it
    escapes control characters into inert text rather than dropping them, keeps
    the common all-ASCII case byte-identical, and preserves the real characters
    of a non-ASCII sample name instead of mangling them, since the frontend
    decodes it back for display.
    """
    text = str(value)
    encoded = quote(text, safe=_HEADER_SAFE, encoding="utf-8", errors="replace")
    if len(encoded) <= _HEADER_VALUE_MAX:
        return encoded

    # Truncate the source by characters rather than the encoded form by bytes.
    # One character can span several escapes -- `é` is `%C3%A9` -- so cutting
    # the encoded string can leave a sequence that is well-formed percent
    # encoding but invalid UTF-8, which makes the client's decodeURIComponent
    # throw and costs the whole value rather than just its tail.
    budget = _HEADER_VALUE_MAX - 3  # room for the ellipsis that marks the cut
    low, high = 0, min(len(text), budget)  # every character costs at least one
    while low < high:
        mid = (low + high + 1) // 2
        if len(quote(text[:mid], safe=_HEADER_SAFE, errors="replace")) <= budget:
            low = mid
        else:
            high = mid - 1
    return quote(text[:low], safe=_HEADER_SAFE, errors="replace") + "..."


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
        "X-Dtype": sanitize_header_value(dtype),
        "X-Shape": ",".join(str(v) for v in shape),
    }
    if frame is not None:
        headers["X-Frame"] = str(frame)
    if extra:
        for key, value in extra.items():
            if value is None:
                continue
            headers[str(key)] = sanitize_header_value(value)
    return headers


def add_optional_header(headers: dict[str, str], name: str, value: Any) -> None:
    if value is None:
        return
    if isinstance(value, str) and not value:
        return
    headers[name] = sanitize_header_value(value)
