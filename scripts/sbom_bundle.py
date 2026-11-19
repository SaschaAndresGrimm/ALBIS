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
directly. Each probe runs only when its module is in the bundle, and a probe
that stops working is reported and skipped rather than failing the build: the
test suite runs every probe against the dev environment, so an upstream change
shows up there, on the pull request that bumps the pin.
"""

from __future__ import annotations

import ast
import os
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


_PILLOW_CODECS = {
    "jpg": "libjpeg-turbo",
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


# (carrier distribution or None for the interpreter, module that must be in
# the bundle, probe). The module is the one that links the library, so a bundle
# that leaves it out does not get the library listed.
PROBES: tuple[tuple[str | None, str, Callable[[], list[tuple[str, str]]]], ...] = (
    ("h5py", "h5py.defs", _hdf5),
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
