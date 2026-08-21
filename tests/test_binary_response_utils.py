from __future__ import annotations

from urllib.parse import unquote

from backend.routes.binary_response_utils import (
    add_optional_header,
    build_binary_headers,
    octet_stream_responses,
    sanitize_header_value,
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


def test_sanitize_header_value_leaves_plain_ascii_untouched() -> None:
    """The common case must stay byte-identical; encoding is only for the rest."""
    assert sanitize_header_value("Remote stream (default) S1 Img2") == (
        "Remote stream (default) S1 Img2"
    )
    assert sanitize_header_value("<u4") == "<u4"
    assert sanitize_header_value("/entry/instrument/detector/pixel_mask") == (
        "/entry/instrument/detector/pixel_mask"
    )


def test_sanitize_header_value_encodes_values_that_break_the_response() -> None:
    """Each of these makes an unsanitized response unreadable; see the docstring.

    Non-Latin-1 raises inside Starlette, a CRLF makes h11 drop the connection,
    and an oversized value pushes the header block past what a client reads.
    """
    for raw in ("結晶 α-helix", "a\r\nX-Evil: pwned", "a\x00b\tc", "A" * 100_000):
        encoded = sanitize_header_value(raw)
        encoded.encode("latin-1")  # must not raise: Starlette encodes headers as latin-1
        assert "\r" not in encoded and "\n" not in encoded
        assert len(encoded) <= 512


def test_sanitize_header_value_round_trips_through_url_decoding() -> None:
    """The frontend's readHeaderText() undoes this, so the text must survive."""
    for raw in ("結晶 α-helix ~2.1 Å", "a\r\nX-Evil: 1", "100% sure", "a\x00b\tc"):
        assert unquote(sanitize_header_value(raw)) == raw


def test_sanitize_header_value_truncation_stays_decodable() -> None:
    """A cut inside a `%XX` escape would make decodeURIComponent throw."""
    # `é` is `%C3%A9`, so the 512-char budget lands between the two escapes of
    # one character at some length in this range -- the case that produces
    # well-formed percent encoding of an invalid UTF-8 byte sequence.
    for source in ("é", "結", "😀", "aé"):
        for length in range(1, 400):
            encoded = sanitize_header_value(source * length)
            assert len(encoded) <= 512
            decoded = unquote(encoded, errors="strict")  # must not raise
            assert (source * length).startswith(decoded.removesuffix("..."))


def test_header_helpers_sanitize_dynamic_values() -> None:
    """The helpers are the choke point, so callers cannot forget to sanitize."""
    headers = build_binary_headers(
        dtype="<u2", shape=(4, 6), extra={"X-Remote-Display": "結晶\r\nX-Evil: 1"}
    )
    add_optional_header(headers, "X-Mask-Path", "/entry/données")
    for value in headers.values():
        value.encode("latin-1")
        assert "\r" not in value and "\n" not in value
    assert unquote(headers["X-Remote-Display"]) == "結晶\r\nX-Evil: 1"
    assert unquote(headers["X-Mask-Path"]) == "/entry/données"
