"""The type a virtual dataset's frames are served as.

A virtual dataset (VDS) declares a type of its own, which need not be its
sources' type. Diamond's nexgen declares its NXmx master's `/entry/data/data`
as int64 over Odin's uint32 frames. Read as declared, every frame went to the
browser at twice its size -- 36 MB rather than 18 MB for an Eiger2 4M -- and,
worse, as a type the interface cannot judge saturation in: it reads the gap
and defective-pixel value off the type's width, and gives up above 52 bits, so
the detector's 0xFFFFFFFF gaps showed as four billion counts.

The values cannot be larger than the sources' type allows, since that is where
they come from, so serving them in that type loses nothing. The one value that
comes from elsewhere is the fill value, which the VDS returns for any region no
source maps; it has to fit too.
"""

from __future__ import annotations

import contextlib
import threading
from collections import OrderedDict
from collections.abc import Callable, Iterable
from typing import Any

import numpy as np


def narrowed_dtype(
    declared: Any, source_dtypes: Iterable[Any], fillvalue: Any = 0
) -> np.dtype | None:
    """The sources' integer type when it can stand in for `declared`, else None.

    None means "serve as declared": no sources, sources that disagree, a
    non-integer anywhere, a source type `declared` does not hold exactly, one
    no narrower than `declared`, or a fill value outside the source type.
    """
    declared = np.dtype(declared)
    sources = [np.dtype(item) for item in source_dtypes]
    if declared.kind not in "iu" or not sources:
        return None
    first = sources[0]
    if any(item != first for item in sources) or first.kind not in "iu":
        return None
    if first.itemsize >= declared.itemsize or not np.can_cast(first, declared, "safe"):
        return None
    try:
        fill = int(np.asarray(fillvalue).reshape(-1)[0]) if fillvalue is not None else 0
    except (TypeError, ValueError, IndexError):
        return None
    limits = np.iinfo(first)
    if not limits.min <= fill <= limits.max:
        return None
    return first.newbyteorder("=")


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value or "")


# Opens one VDS source: (file name as the VDS records it, dataset path) ->
# (the source dataset, handles to close). Raises when it may not be read.
SourceOpener = Callable[[str, str], "tuple[Any, list[Any]]"]


class VirtualDtypeCache:
    """`narrowed_dtype` for a dataset, remembered per file version.

    Finding the sources' type opens every source file, and every frame request
    resolves its dataset afresh, so the answer is kept: keyed by the file, its
    size and modification time, and the dataset path, so a rewritten file is
    asked again. Bounded, oldest out first.
    """

    def __init__(self, size: int = 64) -> None:
        self._size = size
        self._lock = threading.Lock()
        self._known: OrderedDict[tuple[Any, ...], np.dtype | None] = OrderedDict()

    def read_dtype(
        self, dset: Any, key: tuple[Any, ...], open_source: SourceOpener
    ) -> np.dtype | None:
        if not getattr(dset, "is_virtual", False):
            return None
        with self._lock:
            if key in self._known:
                self._known.move_to_end(key)
                return self._known[key]
        result = self._compute(dset, open_source)
        with self._lock:
            self._known[key] = result
            while len(self._known) > self._size:
                self._known.popitem(last=False)
        return result

    @staticmethod
    def _compute(dset: Any, open_source: SourceOpener) -> np.dtype | None:
        try:
            sources = dset.virtual_sources()
            fillvalue = dset.fillvalue
        except Exception:  # noqa: BLE001 - anything unreadable is served as declared
            return None
        dtypes: list[np.dtype] = []
        for source in sources:
            try:
                node, handles = open_source(_text(source.file_name), _text(source.dset_name))
            except Exception:  # noqa: BLE001
                return None
            try:
                dtypes.append(np.dtype(node.dtype))
            except Exception:  # noqa: BLE001
                return None
            finally:
                for handle in handles:
                    with contextlib.suppress(Exception):
                        handle.close()
        return narrowed_dtype(dset.dtype, dtypes, fillvalue)
