"""A TIFF's baseline tags as header text, for a TIFF with no header of its own.

A TIFF whose first page has no ImageDescription has no text header, and ALBIS
then asked fabio for one. fabio's own TIFF reader fails on some files (an
EIGER2 monitor TIFF among them), and it then read the tags through Pillow,
listing each as ``name value``: that is where the header's detector model and
DAQ software came from. Pillow is not bundled any more (ALBIS.spec; it carried
six image libraries ALBIS never decodes with), so the same text is made here
from the tags tifffile has already read.

The names, their order and the formatting are fabio's (``fabio.TiffIO.TAG_ID``
with the first letter lowered, a one-element value unwrapped), so the header a
user sees is the one they saw before.
"""

from __future__ import annotations

import enum
from typing import Any

TAG_NAMES = {
    256: "numberOfColumns",
    257: "numberOfRows",
    258: "bitsPerSample",
    259: "compression",
    262: "photometricInterpretation",
    270: "imageDescription",
    272: "model",
    273: "stripOffsets",
    277: "samplesPerPixel",
    278: "rowsPerStrip",
    279: "stripByteCounts",
    305: "software",
    306: "date",
    320: "colormap",
    339: "sampleFormat",
}


def _plain(value: Any) -> Any:
    # tifffile hands back enums (COMPRESSION.NONE) where Pillow gave the number.
    if isinstance(value, enum.Enum):
        return int(value.value)
    if isinstance(value, bytes):
        return value.decode("latin-1").rstrip("\x00")
    if isinstance(value, (tuple, list)):
        value = tuple(_plain(v) for v in value)
        return value[0] if len(value) == 1 else value
    if hasattr(value, "tolist"):  # a numpy array, such as a colormap
        return _plain(value.tolist())
    return value


def tiff_tag_header_text(page: Any) -> str:
    """``name value`` lines for the baseline tags a tifffile page carries."""
    tags = getattr(page, "tags", None)
    if tags is None:
        return ""
    lines = []
    for code, name in TAG_NAMES.items():
        tag = tags.get(code)
        if tag is None:
            continue
        lines.append(f"{name} {_plain(tag.value)}")
    return "\n".join(lines)
