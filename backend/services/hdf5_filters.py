"""The hdf5plugin compression filters the packaged ALBIS leaves out.

hdf5plugin ships each filter as a library of its own, `plugins/libh5<name>.*`,
and every one carries compression libraries of its own into the bundle and
its SBOM. The ones named here are kept out of the build (ALBIS.spec), and so
out of the SBOM (scripts/sbom_bundle.py), because no detector file ALBIS is
for uses them:

- zfp, sz, sz3, sperr, htj2k: lossy compressors for floating-point simulation
  and climate data. Photon counts are not stored lossily.
- fcidecomp: JPEG-LS for EUMETSAT's satellite imagery.
- bzip2: PyTables' bzip2, rare and slow.

What stays: bitshuffle with LZ4 or Zstd (what EIGER, EIGER2, PILATUS4, ARINA
and Jungfraujoch write), plain LZ4 (DECTRIS's other option), and Zstd, Blosc
and Blosc2, which turn up in files repacked with h5py or PyTables. Deflate
(gzip) is built into HDF5 itself.

A file that needs a left-out filter opens and lists its datasets; reading a
frame then fails with the "compression filter may be unavailable" message of
services/hdf5_open.py.

Imported by ALBIS.spec at build time, so this module imports nothing beyond
the standard library.
"""

from __future__ import annotations

import logging
import os

OMITTED_FILTERS = frozenset({"bzip2", "fcidecomp", "htj2k", "sperr", "sz", "sz3", "zfp"})

_PLUGIN_PREFIX = "libh5"


def plugin_filter_name(path: str) -> str | None:
    """`.../plugins/libh5sz3.dylib` -> `sz3`; None for any other file."""
    base = os.path.basename(path.replace("\\", "/"))
    if not base.startswith(_PLUGIN_PREFIX):
        return None
    stem = base[len(_PLUGIN_PREFIX) :].split(".", 1)[0]
    return stem or None


def is_omitted_plugin(path: str) -> bool:
    """Whether `path` is the plugin library of a left-out filter."""
    parts = path.replace("\\", "/").split("/")
    return (
        len(parts) >= 3
        and parts[-3] == "hdf5plugin"
        and parts[-2] == "plugins"
        and plugin_filter_name(path) in OMITTED_FILTERS
    )


class _OmittedFilterNotice(logging.Filter):
    """Drop hdf5plugin's "Cannot initialize filter X: File not found" for the
    filters the bundle leaves out on purpose; anything else still shows."""

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        if "File not found" not in message:
            return True
        return not any(f"filter {name}:" in message for name in OMITTED_FILTERS)


_notice = _OmittedFilterNotice()


def quiet_omitted_filters() -> None:
    """Keep the left-out filters from logging an error each time ALBIS starts.

    hdf5plugin tries to register every filter it was built with when it is
    imported, and logs an error for each library it cannot find. Call this
    before the import; calling it again does nothing.
    """
    logger = logging.getLogger("hdf5plugin._utils")
    if _notice not in logger.filters:
        logger.addFilter(_notice)


def load_hdf5plugin() -> None:
    """Import hdf5plugin, which registers its filters with h5py's HDF5."""
    quiet_omitted_filters()
    import hdf5plugin  # noqa: F401
