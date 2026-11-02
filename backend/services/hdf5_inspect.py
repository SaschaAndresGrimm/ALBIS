"""How the file inspector describes one HDF5 node in a listing.

Shared by `GET /api/hdf5/tree` and `GET /api/hdf5/search`, which list the same
kinds of row and used to build them twice.
"""

from __future__ import annotations

import contextlib
from collections.abc import Callable
from typing import Any

# A scalar's value goes into the listing so the reader sees `NXmx` or `12.4 keV`
# without opening each node. Long strings (a NeXus `notes` field, a JSON blob
# a writer stored as text) are cut; the details pane has the whole value.
SCALAR_TEXT_LIMIT = 200


def dtype_label(h5py: Any, dtype: Any) -> str:
    """The dtype as a reader would name it: `string` rather than `object`.

    h5py reports a variable-length string as numpy's `object`, which is what a
    NeXus master stores most of its metadata as, and which tells the reader
    nothing about what the field holds.
    """
    with contextlib.suppress(Exception):
        if h5py.check_string_dtype(dtype) is not None:
            return "string"
    return str(dtype)


def _storage_is_inline(dset: Any) -> bool:
    """True when reading the dataset touches no file but its own.

    External raw storage and virtual sources are confined only on the paths
    that go through `assert_dataset_storage_confined`; a listing reads children
    by plain indexing, so it leaves such datasets alone rather than read bytes
    from wherever a crafted file points.
    """
    try:
        if dset.is_virtual:
            return False
        return dset.id.get_create_plist().get_external_count() == 0
    except Exception:
        return False


def scalar_summary(
    h5py: Any, dset: Any, serialize: Callable[[Any], Any]
) -> tuple[Any | None, str | None]:
    """The value and `units` of a one-element dataset, or `(None, None)`.

    One element means shape `()` or `(1,)`: NeXus writers use both for a single
    number. Only numbers and text are summarised; a compound or reference has
    no short form.
    """
    try:
        if dset.size != 1 or dset.ndim > 1:
            return None, None
        is_text = h5py.check_string_dtype(dset.dtype) is not None
        if not is_text and dset.dtype.kind not in "biuf":
            return None, None
        if not _storage_is_inline(dset):
            return None, None
        value = serialize(dset[()])
    except Exception:
        return None, None
    if isinstance(value, list):
        value = value[0] if len(value) == 1 else None
    if isinstance(value, float) and value != value:  # NaN is not valid JSON
        value = None
    if isinstance(value, str) and len(value) > SCALAR_TEXT_LIMIT:
        value = value[:SCALAR_TEXT_LIMIT] + "…"
    if not isinstance(value, str | int | float | bool):
        return None, None
    units: str | None = None
    with contextlib.suppress(Exception):
        raw = serialize(dset.attrs["units"]) if "units" in dset.attrs else None
        if isinstance(raw, str) and raw.strip():
            units = raw.strip()
    return value, units


def describe_link(h5py: Any, link: Any, name: str, path: str) -> dict[str, Any] | None:
    """A row for a soft or external link, or None for a hard link."""
    if isinstance(link, h5py.ExternalLink):
        return {
            "name": name,
            "path": path,
            "type": "link",
            "link": "external",
            "target": f"{link.filename}:{link.path}",
        }
    if isinstance(link, h5py.SoftLink):
        return {
            "name": name,
            "path": path,
            "type": "link",
            "link": "soft",
            "target": str(link.path),
        }
    return None


def describe_object(
    h5py: Any, obj: Any, name: str, path: str, serialize: Callable[[Any], Any]
) -> dict[str, Any] | None:
    """A row for a group or dataset, or None for anything else."""
    if isinstance(obj, h5py.Group):
        return {
            "name": name,
            "path": path,
            "type": "group",
            "hasChildren": len(obj.keys()) > 0,
        }
    if isinstance(obj, h5py.Dataset):
        row: dict[str, Any] = {
            "name": name,
            "path": path,
            "type": "dataset",
            "shape": tuple(int(x) for x in obj.shape),
            "dtype": dtype_label(h5py, obj.dtype),
        }
        value, units = scalar_summary(h5py, obj, serialize)
        if value is not None:
            row["value"] = value
            if units:
                row["units"] = units
        return row
    return None
