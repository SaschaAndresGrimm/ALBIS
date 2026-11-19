"""What a PyInstaller build actually put in the bundle, for `generate_sbom.py`.

The dependency closure of the pins says what *could* be in a bundle. PyInstaller
decides what *is*: it follows imports, so of the ~170 pyobjc framework packages
the pins reach on macOS it takes the few ALBIS imports, and its hooks can add a
package no pin names. The TOC files it leaves in its work directory
(`build/ALBIS/*.toc`) list every source file it collected; a distribution is in
the bundle when one of its files is among them.

The TOC files are Python literals of `(name, source, typecode)` tuples. They
are PyInstaller's internal format, read here because the version is pinned in
the build scripts; `tests/test_generate_sbom.py` holds a sample to it.

Native libraries a wheel carries inside itself (the HDF5 in h5py, the OpenBLAS
in numpy) have no metadata of their own, so they are asked for their version
directly, or, for hdf5plugin's filters, looked up in its documented list. Each probe runs only when its module is in the bundle, and a probe
that stops working is reported and skipped rather than failing the build: the
test suite runs every probe against the dev environment, so an upstream change
shows up there, on the pull request that bumps the pin.
"""

from __future__ import annotations

import ast
import os
import re
import sys
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from importlib.metadata import Distribution
from pathlib import Path

_MODULE_TYPES = {"PYMODULE", "EXTENSION", "PYSOURCE"}


@dataclass
class Bundle:
    """The source paths PyInstaller collected and the modules among them."""

    sources: set[str] = field(default_factory=set)
    modules: set[str] = field(default_factory=set)

    def has_module(self, name: str) -> bool:
        return name in self.modules or name in sys.builtin_module_names


def _norm(path: str) -> str:
    return os.path.normcase(os.path.normpath(path))


def _module_names(name: str, typecode: str) -> set[str]:
    """`lxml.etree` stays itself; `lxml/etree.cpython-313-darwin.so` becomes it."""
    if typecode != "EXTENSION" or (
        "/" not in name and "\\" not in name and not name.endswith((".so", ".pyd"))
    ):
        return {name}
    stem = name.replace("\\", "/")
    head, _, tail = stem.rpartition("/")
    tail = tail.split(".", 1)[0]
    dotted = f"{head.replace('/', '.')}.{tail}" if head else tail
    # Standard-library extensions sit under lib-dynload/ but import by stem.
    return {dotted, tail}


def _entries(node) -> Iterable[tuple[str, str, str]]:
    if isinstance(node, tuple) and len(node) == 3 and all(isinstance(x, str) for x in node):
        yield node
    elif isinstance(node, (list, tuple)):
        for child in node:
            yield from _entries(child)


def read_bundle(workdir: Path) -> Bundle:
    """Read every TOC file PyInstaller wrote to `workdir`."""
    tocs = sorted(workdir.glob("*.toc"))
    if not tocs:
        raise FileNotFoundError(
            f"no PyInstaller TOC files in {workdir}; run this after PyInstaller"
        )
    bundle = Bundle()
    for toc in tocs:
        for name, source, typecode in _entries(ast.literal_eval(toc.read_text(encoding="utf-8"))):
            if os.path.isabs(source):
                bundle.sources.add(_norm(source))
            if typecode in _MODULE_TYPES:
                bundle.modules |= _module_names(name, typecode)
    return bundle


def is_bundled(dist: Distribution, bundle: Bundle) -> bool:
    return any(_norm(str(dist.locate_file(file))) in bundle.sources for file in dist.files or ())


# --- Native libraries -------------------------------------------------------


@dataclass(frozen=True)
class Native:
    name: str
    version: str
    # The distribution that carries it, or None for one inside the interpreter.
    carrier: str | None


# CPE vendor:product for the libraries NVD lists, so a scanner matching on CPE
# (grype does, for anything without a package ecosystem) finds their CVEs.
# Each was looked up in NVD's CPE dictionary; where it has two spellings, this
# is the one the library's CVEs are recorded under. A wrong entry would match
# nothing, silently, so a library NVD does not list is left without one
# rather than guessed (c-blosc 1.x, ZFP, CharLS, OpenJPH, SPERR, bitshuffle).
CPE_PRODUCTS = {
    "CPython": "python:python",
    "OpenSSL": "openssl:openssl",
    "zlib": "zlib:zlib",
    "expat": "libexpat_project:libexpat",
    "SQLite": "sqlite:sqlite",
    "HDF5": "hdfgroup:hdf5",
    "libzmq": "zeromq:libzmq",
    "libzstd": "facebook:zstandard",
    "Zstd": "facebook:zstandard",
    "LZ4": "lz4_project:lz4",
    "bzip2": "bzip:bzip2",
    "Snappy": "google:snappy",
    "c-blosc2": "blosc:c-blosc2",
    "libjpeg-turbo": "libjpeg-turbo:libjpeg-turbo",
    "openjpeg": "uclouvain:openjpeg",
    "libtiff": "libtiff:libtiff",
    "libwebp": "webmproject:libwebp",
    "freetype": "freetype:freetype",
    "lcms2": "littlecms:little_cms",
    "libxml2": "xmlsoft:libxml2",
    "libxslt": "xmlsoft:libxslt",
}

_RELEASE = re.compile(r"^\d+(\.\d+)+$")


def cpe_for(name: str, version: str) -> str | None:
    """A CPE 2.3 name, or None when NVD lists no product or the version is a commit."""
    product = CPE_PRODUCTS.get(name)
    if product is None or not _RELEASE.match(version):
        return None
    return f"cpe:2.3:a:{product}:{version}:*:*:*:*:*:*:*"


def _hdf5() -> list[tuple[str, str]]:
    import h5py

    return [("HDF5", h5py.version.hdf5_version)]


def _blas() -> list[tuple[str, str]]:
    import numpy

    blas = numpy.show_config(mode="dicts")["Build Dependencies"]["blas"]
    # macOS wheels link Apple's Accelerate, part of the system and not bundled;
    # it reports no version and there is nothing to list.
    if blas.get("detection method") == "system" or blas.get("version") in (None, "unknown"):
        return []
    return [(blas["name"], blas["version"])]


def _libzmq() -> list[tuple[str, str]]:
    import zmq

    return [("libzmq", zmq.zmq_version())]


def _libzstd() -> list[tuple[str, str]]:
    import zstandard

    return [("libzstd", ".".join(str(part) for part in zstandard.ZSTD_VERSION))]


# Pillow's "jpg" feature reports the libjpeg API level (6.2, 8.0), not the
# release, so libjpeg-turbo is asked for under its own name.
_PILLOW_CODECS = {
    "libjpeg_turbo": "libjpeg-turbo",
    "jpg_2000": "openjpeg",
    "libtiff": "libtiff",
    "webp": "libwebp",
    "freetype2": "freetype",
    "littlecms2": "lcms2",
}


def _pillow() -> list[tuple[str, str]]:
    from PIL import features

    found = []
    for feature, library in _PILLOW_CODECS.items():
        version = features.version(feature)
        if version:
            found.append((library, version))
    return found


def _libxml() -> list[tuple[str, str]]:
    from lxml import etree

    return [
        ("libxml2", ".".join(str(part) for part in etree.LIBXML_VERSION)),
        ("libxslt", ".".join(str(part) for part in etree.LIBXSLT_VERSION)),
    ]


def _openssl() -> list[tuple[str, str]]:
    import ssl

    # "OpenSSL 3.5.9 29 Sep 2026"; the first two words are name and version.
    name, version, *_ = ssl.OPENSSL_VERSION.split()
    return [(name, version)]


def _zlib() -> list[tuple[str, str]]:
    import zlib

    return [("zlib", zlib.ZLIB_RUNTIME_VERSION)]


def _expat() -> list[tuple[str, str]]:
    import pyexpat

    return [("expat", pyexpat.EXPAT_VERSION.removeprefix("expat_"))]


def _sqlite() -> list[tuple[str, str]]:
    import sqlite3

    return [("SQLite", sqlite3.sqlite_version)]


# hdf5plugin's filter plugins and the compression libraries compiled into them
# report no version at run time. hdf5plugin documents them per release, in
# doc/information.rst; this is that list for each pinned version, keyed by the
# filter that embeds them. A commit stands in where upstream names no release.
# tests/test_generate_sbom.py fails when the pin moves to a version not listed
# here, so the pull request that bumps it is the one that updates this.
_SNAPPY = ("Snappy", "1.2.2")
_LZ4 = ("LZ4", "1.10.0")
_ZSTD = ("Zstd", "1.5.7")
_ZLIB = ("zlib", "1.3.1")
_ZFP = ("ZFP", "1.0.1")
HDF5PLUGIN_EMBEDDED: dict[str, dict[str, tuple[tuple[str, str], ...]]] = {
    "7.1.0": {
        "blosc": (("hdf5-blosc", "1.0.1"), ("c-blosc", "1.21.6"), _LZ4, _SNAPPY, _ZLIB, _ZSTD),
        "blosc2": (("HDF5-Blosc2", "3.0.1"), ("c-blosc2", "3.3.2"), _LZ4, _ZFP, _ZLIB, _ZSTD),
        "bshuf": (("bitshuffle", "0.5.2"), _LZ4, _ZSTD),
        "bzip2": (("PyTables bzip2 filter", "3.10.2"), ("bzip2", "1.0.8")),
        "fcidecomp": (("FCIDECOMP", "2.1.1"), ("CharLS", "2.1.0")),
        "htj2k": (("h5z-htj2k", "0.1.1"), ("OpenJPH", "0.31.0")),
        "lz4": (("HDF5 LZ4 filter", "5573db8"), _LZ4),
        "sperr": (("H5Z-SPERR", "0.2.3"), ("SPERR", "0.8.5")),
        "sz": (("SZ", "308bd06"),),
        "sz3": (("SZ3", "4bbe9df7e4bcb"),),
        "zfp": (("H5Z-ZFP", "1.1.1"), _ZFP),
        "zstd": (("HDF5 Zstd filter", "5573db8"), _ZSTD),
    },
}


def _hdf5plugin() -> list[tuple[str, str]]:
    import hdf5plugin

    table = HDF5PLUGIN_EMBEDDED.get(hdf5plugin.version)
    if table is None:
        raise LookupError(f"no list of embedded libraries for hdf5plugin {hdf5plugin.version}")
    config = hdf5plugin.get_config().build_config
    # Built with Intel IPP, LZ4 is replaced by a patched 1.9.3, which this list
    # does not describe. The wheels on PyPI are built without it.
    if config.ipp:
        raise LookupError("hdf5plugin was built with Intel IPP")
    found: dict[str, str] = {}
    for name in config.embedded_filters:
        for library, version in table.get(name, ()):
            found.setdefault(library, version)
    return sorted(found.items())


# (carrier distribution or None for the interpreter, module that must be in
# the bundle, probe). The module is the one that links the library, so a bundle
# that leaves it out does not get the library listed.
PROBES: tuple[tuple[str | None, str, Callable[[], list[tuple[str, str]]]], ...] = (
    ("h5py", "h5py.defs", _hdf5),
    ("hdf5plugin", "hdf5plugin", _hdf5plugin),
    ("numpy", "numpy._core._multiarray_umath", _blas),
    ("pyzmq", "zmq.backend.cython._zmq", _libzmq),
    ("zstandard", "zstandard.backend_c", _libzstd),
    ("pillow", "PIL._imaging", _pillow),
    ("lxml", "lxml.etree", _libxml),
    (None, "_ssl", _openssl),
    (None, "zlib", _zlib),
    (None, "pyexpat", _expat),
    (None, "_sqlite3", _sqlite),
)


def probe_natives(bundle: Bundle, carriers: set[str], warn=print) -> list[Native]:
    """The native libraries in the bundle whose version can be asked for."""
    found = []
    for carrier, module, probe in PROBES:
        if carrier is not None and carrier not in carriers:
            continue
        if not bundle.has_module(module):
            continue
        try:
            results = probe()
        except Exception as exc:  # noqa: BLE001 -- any upstream change, reported below
            warn(f"SBOM: could not read the native library version via {probe.__name__}: {exc!r}")
            continue
        found += [Native(name, version, carrier) for name, version in results]
    return found
