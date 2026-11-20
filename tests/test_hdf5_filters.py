"""The hdf5plugin filters the bundle leaves out, and the ones it must keep."""

from __future__ import annotations

import logging
import subprocess
import sys
from pathlib import Path

import pytest

from backend.services import hdf5_filters
from backend.services.hdf5_filters import OMITTED_FILTERS, is_omitted_plugin

ROOT = Path(__file__).resolve().parents[1]

# What DECTRIS detectors and Jungfraujoch write (bitshuffle with LZ4 or Zstd,
# plain LZ4), and what repacked files use. Dropping one breaks real files.
KEPT = {"bshuf", "lz4", "zstd", "blosc", "blosc2"}


def test_the_filters_detector_files_need_are_kept() -> None:
    assert not KEPT & OMITTED_FILTERS


def test_every_filter_hdf5plugin_builds_is_either_kept_or_left_out() -> None:
    """A new hdf5plugin filter needs a decision, not a silent default."""
    import hdf5plugin

    built = set(hdf5plugin.get_config().build_config.embedded_filters)
    assert built == KEPT | OMITTED_FILTERS


@pytest.mark.parametrize(
    "path",
    [
        "hdf5plugin/plugins/libh5sz3.dylib",
        "hdf5plugin/plugins/libh5zfp.so",
        "hdf5plugin\\plugins\\libh5fcidecomp.dll",
        "/venv/site-packages/hdf5plugin/plugins/libh5sz.so",
    ],
)
def test_a_left_out_filter_library_is_recognised(path: str) -> None:
    assert is_omitted_plugin(path)


@pytest.mark.parametrize(
    "path",
    [
        "hdf5plugin/plugins/libh5bshuf.dylib",
        "hdf5plugin/plugins/libh5lz4.so",
        "hdf5plugin\\plugins\\libh5zstd.dll",
        "hdf5plugin/plugins/libh5blosc2.so",
        # Same names outside hdf5plugin's plugin folder are not its filters.
        "h5py/.dylibs/libh5sz3.dylib",
        "libh5zfp.so",
    ],
)
def test_a_kept_filter_or_other_library_is_not_dropped(path: str) -> None:
    assert not is_omitted_plugin(path)


def test_the_spec_drops_the_left_out_filters_from_the_bundle() -> None:
    spec = (ROOT / "ALBIS.spec").read_text(encoding="utf-8")
    assert "from backend.services.hdf5_filters import is_omitted_plugin" in spec
    assert "a.binaries = [entry for entry in a.binaries if not is_omitted_plugin" in spec
    assert "a.datas = [entry for entry in a.datas if not is_omitted_plugin" in spec


def _record(message: str, level: int = logging.ERROR) -> logging.LogRecord:
    return logging.LogRecord("hdf5plugin._utils", level, __file__, 1, message, None, None)


def test_only_the_notices_for_left_out_filters_are_quieted() -> None:
    notice = hdf5_filters._notice
    assert not notice.filter(_record("Cannot initialize filter sz3: File not found"))
    assert not notice.filter(_record("Cannot initialize filter sz: File not found"))
    assert notice.filter(_record("Cannot initialize filter lz4: File not found"))
    assert notice.filter(_record("Failed to load filter sz3: /x/libh5sz3.so"))


_BUNDLE_LIKE = """
import glob, sys
sys.path.insert(0, {root!r})
from backend.services.hdf5_filters import is_omitted_plugin

# What the bundle looks like: the left-out filters' libraries are not there.
_glob = glob.glob
glob.glob = lambda pattern, *a, **k: [p for p in _glob(pattern, *a, **k) if not is_omitted_plugin(p)]

from backend.services.hdf5_filters import load_hdf5plugin
load_hdf5plugin()

import h5py, hdf5plugin
for name in {kept!r}:
    assert h5py.h5z.filter_avail(hdf5plugin.FILTERS[name]), name
with h5py.File({fixture!r}) as f:
    frame = f["entry/data/data"][2]
assert int(frame[0, 0]) == 2, frame[0, 0]
print("ok")
"""


def test_without_the_left_out_filters_lz4_data_still_reads_and_nothing_is_logged() -> None:
    """Before the notices were quieted, every start logged seven errors."""
    script = _BUNDLE_LIKE.format(
        root=str(ROOT),
        kept=sorted(KEPT),
        fixture=str(ROOT / "testdata" / "compressed_stack.h5"),
    )
    result = subprocess.run(
        [sys.executable, "-c", script], cwd=ROOT, capture_output=True, text=True, timeout=120
    )
    assert result.returncode == 0, result.stderr[-2000:]
    assert result.stdout.strip() == "ok"
    assert "Cannot initialize filter" not in result.stderr, result.stderr
