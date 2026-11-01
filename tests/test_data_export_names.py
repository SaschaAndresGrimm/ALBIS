"""The names a data export writes its files under (services/data_export_names.py).

XDS (`name_?????.cbf`), DIALS, ALBULA and ALBIS itself read an export as a
series only when the frame number comes last and every file shares one base.
These were tested only through whole exports; this pins the naming rules on
their own, so a change to them shows here first.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import HTTPException

from backend.services.data_export_names import DataExportNamesMixin as Names


@pytest.mark.parametrize(
    ("name", "dataset", "base"),
    [
        ("series_190_master.h5", "entry/data/data", "series_190"),
        ("series_190_master.h5", "/data", "series_190"),
        ("series_190_master.h5", "entry/data/other", "series_190_entry_data_other"),
        ("image_00007.cbf", "", "image"),
        ("scan.0012.cbf.gz", "", "scan"),
        ("00042.tif", "", "00042"),
    ],
)
def test_default_export_base_drops_master_and_number(name: str, dataset: str, base: str) -> None:
    """Without this a re-export of `image_00007.cbf` would be named after its
    first frame, and a master file's frames would carry `_master` along."""
    assert Names.default_export_base(Path(name), dataset) == base


def test_frame_name_puts_the_number_last_and_the_threshold_before_it() -> None:
    assert Names.frame_name("scan", 7) == "scan_00007"
    assert Names.frame_name("scan", 7, threshold=2) == "scan_thr2_00007"
    assert Names.frame_name("scan", -3) == "scan_00000"


def test_source_frame_number_reads_the_number_in_the_name() -> None:
    assert Names.source_frame_number(Path("image_00007.cbf"), 1) == 7
    assert Names.source_frame_number(Path("image_00012.cbf.gz"), 1) == 12
    assert Names.source_frame_number(Path("image.tif"), 5) == 5


def test_safe_name_keeps_only_file_name_characters() -> None:
    assert Names._safe_name(" /entry\\data//a b?c/ ") == "entry_data_a_b_c"
    assert Names._safe_name("///", "fallback") == "fallback"


def test_free_base_moves_the_whole_series_to_one_new_name(tmp_path: Path) -> None:
    """One file of the series taken: the whole export moves to `base_2`, so
    its files do not end up under two names."""
    planned = [(1, None), (2, None)]
    (tmp_path / "scan_00002.tiff").write_bytes(b"")
    names = Names()
    assert names._free_base(tmp_path, "scan", planned, ".tiff", overwrite=True) == "scan"
    assert names._free_base(tmp_path, "scan", planned, ".tiff", overwrite=False) == "scan_2"


def test_next_available_path_counts_up_from_a_taken_name(tmp_path: Path) -> None:
    taken = tmp_path / "flat.tiff"
    assert Names._next_available_path(taken) == taken
    taken.write_bytes(b"")
    (tmp_path / "flat_001.tiff").write_bytes(b"")
    assert Names._next_available_path(taken) == tmp_path / "flat_002.tiff"


def test_free_base_gives_up_with_a_readable_error(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(Path, "exists", lambda _self: True)
    with pytest.raises(HTTPException) as caught:
        Names()._free_base(tmp_path, "scan", [(1, None)], ".tiff", overwrite=False)
    assert caught.value.status_code == 500
