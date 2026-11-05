"""The files written next to an HDF5 data file that hold its metadata.

A detector's frames and its metadata often live in different files. Diamond's
Eiger beamlines write, for one collection:

    CuHF2PYZ2PF6_00001_000001.h5   frames 1-1000, a bare `/data` dataset
    CuHF2PYZ2PF6_00001_000002.h5   the next frames
    CuHF2PYZ2PF6_00001_meta.h5     the detector's configuration (Odin, `/_dectris`)
    CuHF2PYZ2PF6_00001_w00001.nxs  the NXmx master file (nexgen), with the
                                   geometry and links to the frames

and the DECTRIS file writer `PREFIX_data_000001.h5` next to `PREFIX_master.h5`.
Someone used to adxv opens a data file -- Diamond's own notes tell adxv users
to -- and ALBIS then had frames but no energy, distance, pixel size or beam
centre, so no resolution rings, although all of it was one file away.

This finds those neighbours by name. It only lists candidates; reading them,
and deciding whether a found file may be read at all, is the caller's.
"""

from __future__ import annotations

import bisect
import os
import re
from collections.abc import Callable
from pathlib import Path

# `PREFIX_data_000001.h5`, the DECTRIS file writer. Matched before the Odin
# pattern, which would otherwise take `PREFIX_data` for the prefix.
_DECTRIS_DATA_RE = re.compile(r"^(?P<prefix>.+)_data_\d{6}\.h5$", re.IGNORECASE)
# `PREFIX_000001.h5` and `PREFIX_meta.h5`, Odin.
_ODIN_RE = re.compile(r"^(?P<prefix>.+)_(?:\d{6}|meta)\.h5$", re.IGNORECASE)

# A beamline folder can hold every collection of a visit. Enough to find the
# master file of any real one, and a bound on a folder that is something else.
_MAX_SCAN_ENTRIES = 20000


def collection_prefix(name: str) -> str | None:
    """The collection a data or meta file belongs to, or None for any other name."""
    match = _DECTRIS_DATA_RE.match(name) or _ODIN_RE.match(name)
    return match.group("prefix") if match else None


def companion_names(prefix: str, folder_names: list[str]) -> list[str]:
    """Which of `folder_names` hold metadata for `prefix`, best first.

    NXmx masters first, since they carry units and the geometry: the `.nxs`
    named after the collection, then any `PREFIX_<suffix>.nxs` (Diamond writes
    `_w00001` for some collections), then a DECTRIS `_master.h5`. Odin's
    `_meta.h5` last: it has the detector's settings but no units and nothing
    the detector itself does not know, such as an arm it is mounted on.
    """
    by_folded = {name.casefold(): name for name in folder_names}
    folded_prefix = prefix.casefold()
    ordered: list[str] = []

    def take(folded: str) -> None:
        name = by_folded.get(folded)
        if name is not None and name not in ordered:
            ordered.append(name)

    take(f"{folded_prefix}.nxs")
    for folded in sorted(by_folded):
        if folded.startswith(f"{folded_prefix}_") and folded.endswith(".nxs"):
            take(folded)
    take(f"{folded_prefix}_master.h5")
    take(f"{folded_prefix}_meta.h5")
    return ordered


def companion_metadata_files(
    path: Path, accept: Callable[[Path], Path | None] = lambda found: found
) -> list[Path]:
    """The metadata files for the data file at `path`, best first.

    `accept` vets each one -- the data-root rules apply to a file ALBIS found as
    much as to one it was asked for -- and returns the path to read, or None.
    The file itself is never in the list.
    """
    prefix = collection_prefix(path.name)
    if prefix is None:
        return []
    names: list[str] = []
    try:
        with os.scandir(path.parent) as entries:
            for count, entry in enumerate(entries):
                if count >= _MAX_SCAN_ENTRIES:
                    break
                names.append(entry.name)
    except OSError:
        return []
    found: list[Path] = []
    for name in companion_names(prefix, names):
        if name == path.name:
            continue
        accepted = accept(path.parent / name)
        if accepted is not None:
            found.append(accepted)
    return found


def nexus_collection_leads(names: list[str]) -> dict[str, str]:
    """For each data or meta file whose collection has a NeXus master: that master.

    The file browser's series view shows such a collection as its `.nxs`
    alone, the way it shows a DECTRIS master over its data files: opening a
    data file gives one file's share of the frames, the master gives them all.
    A collection with no `.nxs` is not touched. The `.nxs` chosen is the one
    `companion_names` would pick first. Sorted lookups rather than a scan per
    file, since a visit's folder can hold thousands of collections.
    """
    nexus = sorted((name.casefold(), name) for name in names if name.casefold().endswith(".nxs"))
    folded = [item[0] for item in nexus]
    by_folded = dict(nexus)
    lead_of_prefix: dict[str, str | None] = {}
    leads: dict[str, str] = {}
    for name in names:
        prefix = collection_prefix(name)
        if prefix is None:
            continue
        key = prefix.casefold()
        if key not in lead_of_prefix:
            lead = by_folded.get(f"{key}.nxs")
            if lead is None:
                start = bisect.bisect_left(folded, f"{key}_")
                if start < len(folded) and folded[start].startswith(f"{key}_"):
                    lead = nexus[start][1]
            lead_of_prefix[key] = lead
        lead = lead_of_prefix[key]
        if lead is not None:
            leads[name] = lead
    return leads
