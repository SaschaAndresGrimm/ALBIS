#!/usr/bin/env python3
"""Regenerate the compression-filtered HDF5 fixture in `testdata/`.

Real detector files are written with an hdf5plugin filter, so reading one needs
a native plugin library that HDF5 loads at runtime from hdf5plugin's plugin
directory. That is a separate thing from importing h5py, and it is exactly the
piece a packaged build can lose: PyInstaller has to collect those `.so`/`.dylib`
files, and nothing about a successful startup proves it did. Every other HDF5
fixture in the test suite is written by plain h5py with no filter, so none of
them exercise that path.

The fixture is committed rather than generated during the test run because the
packaged smoke test has no Python environment of its own -- it drives a frozen
binary over HTTP.

LZ4 (filter 32004) rather than the bitshuffle+LZ4 (32008) an EIGER writes: the
filters all ship from the same plugin directory, so any one of them proves the
directory came along, and bitshuffle cannot currently be *written* by
hdf5plugin 7 against HDF5 2.0 even though reading it works fine.

Usage:
    python scripts/make_compressed_hdf5_fixture.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import h5py
import hdf5plugin
import numpy as np

FIXTURE = Path(__file__).resolve().parents[1] / "testdata" / "compressed_stack.h5"
FRAMES, HEIGHT, WIDTH = 3, 64, 64


def main() -> int:
    # Deterministic, and structured rather than pure noise so it compresses to a
    # sensible size and a wrong-frame read is visible: frame N is filled with N.
    data = np.zeros((FRAMES, HEIGHT, WIDTH), dtype="<u4")
    for index in range(FRAMES):
        data[index] = index
        data[index, index, :] = 1000 + index

    FIXTURE.parent.mkdir(parents=True, exist_ok=True)
    with h5py.File(FIXTURE, "w") as handle:
        handle.create_dataset(
            "entry/data/data",
            data=data,
            chunks=(1, HEIGHT, WIDTH),
            **hdf5plugin.LZ4(),
        )

    with h5py.File(FIXTURE, "r") as handle:
        dataset = handle["entry/data/data"]
        filters = dict(dataset._filters)
        if not filters:
            raise SystemExit("Fixture was written without a compression filter.")
        if not np.array_equal(dataset[FRAMES - 1], data[FRAMES - 1]):
            raise SystemExit("Fixture did not round-trip.")

    print(f"{FIXTURE} ({FIXTURE.stat().st_size} bytes) filters={filters}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
