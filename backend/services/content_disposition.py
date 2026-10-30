"""A `Content-Disposition` header for a download, whatever its file name.

Download names come from outside: an HDF5 dataset path, a file on the
detector. Put into the header as they are, a character outside Latin-1 (`Ω`,
`温度`) made Starlette raise and the download a 500, and a `"` ended the
quoted name early. RFC 6266 gives the answer browsers follow: a plain-ASCII
`filename` for old clients, and the real name, percent-encoded as UTF-8, in
`filename*`, which every current browser prefers.
"""

from __future__ import annotations

from urllib.parse import quote


def _ascii_fallback(name: str) -> str:
    out = "".join(ch if 0x20 <= ord(ch) < 0x7F and ch not in '"\\%' else "_" for ch in name).strip()
    return out or "download"


def attachment_disposition(filename: str) -> str:
    """Return the header value that offers `filename` as a download."""
    fallback = _ascii_fallback(filename)
    if fallback == filename:
        return f'attachment; filename="{filename}"'
    encoded = quote(filename, safe="", encoding="utf-8", errors="replace")
    return f"attachment; filename=\"{fallback}\"; filename*=UTF-8''{encoded}"
