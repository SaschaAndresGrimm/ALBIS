"""A collection's mask and its place in the file browser.

Diamond's Eiger collections (`PREFIX_000001.h5`, `PREFIX_meta.h5`,
`PREFIX[_w00001].nxs`) keep the pixel mask in the meta file, linked from the
`.nxs`. A data file opened on its own answered `/api/mask` with 404, so the gaps
and dead pixels showed as data. And the browser's series view listed all four
files, where opening anything but the `.nxs` gives one file's share of the
frames.
"""

from __future__ import annotations

from pathlib import Path

import h5py
import numpy as np
import pytest
from fastapi.testclient import TestClient

from backend.services.companion_files import nexus_collection_leads

PREFIX = "CuHF2PYZ2PF6_00001"
SHAPE = (6, 8)


def _client() -> TestClient:
    from backend.app import app

    return TestClient(app, base_url="http://localhost")


def _data(path: Path) -> None:
    with h5py.File(path, "w") as handle:
        handle.create_dataset("data", data=np.zeros((2, *SHAPE), dtype="u4"))


def _meta(path: Path, masked: int = 3) -> None:
    mask = np.zeros(SHAPE, dtype="u4")
    mask.flat[:masked] = 1
    with h5py.File(path, "w") as handle:
        handle.create_dataset("mask", data=mask)


def _nxs(path: Path, meta_name: str) -> None:
    with h5py.File(path, "w") as handle:
        handle["entry/instrument/detector/pixel_mask"] = h5py.ExternalLink(meta_name, "/mask")


# --------------------------------------------------------------------------
# The mask
# --------------------------------------------------------------------------


def _mask(path: Path):
    response = _client().get("/api/mask", params={"file": str(path)})
    if response.status_code != 200:
        return response.status_code, None, None
    shape = tuple(int(x) for x in response.headers["x-shape"].split(","))
    arr = np.frombuffer(response.content, dtype=response.headers["x-dtype"]).reshape(shape)
    return 200, arr, response.headers["x-mask-path"]


def test_a_data_file_takes_the_mask_from_its_meta_file(tmp_path: Path) -> None:
    """The reported case: without this, 404 and the gaps shown as counts."""
    _data(tmp_path / f"{PREFIX}_000001.h5")
    _meta(tmp_path / f"{PREFIX}_meta.h5", masked=5)

    status, mask, source = _mask(tmp_path / f"{PREFIX}_000001.h5")

    assert status == 200
    assert mask.shape == SHAPE and int(mask.sum()) == 5
    assert source == f"{PREFIX}_meta.h5:/mask", "the header says which file it came from"


def test_the_nexus_master_is_asked_first(tmp_path: Path) -> None:
    _data(tmp_path / f"{PREFIX}_000001.h5")
    _meta(tmp_path / f"{PREFIX}_meta.h5")
    _nxs(tmp_path / f"{PREFIX}_w00001.nxs", f"{PREFIX}_meta.h5")

    status, _mask_arr, source = _mask(tmp_path / f"{PREFIX}_000001.h5")

    assert status == 200
    assert source.startswith(f"{PREFIX}_w00001.nxs:")


def test_a_file_with_its_own_mask_keeps_it(tmp_path: Path) -> None:
    own = tmp_path / f"{PREFIX}_000001.h5"
    with h5py.File(own, "w") as handle:
        handle.create_dataset("data", data=np.zeros((2, *SHAPE), dtype="u4"))
        handle.create_dataset("entry/instrument/detector/pixel_mask", data=np.ones(SHAPE, "u4"))
    _meta(tmp_path / f"{PREFIX}_meta.h5", masked=1)

    status, mask, source = _mask(own)

    assert status == 200 and int(mask.sum()) == SHAPE[0] * SHAPE[1]
    assert source == "/entry/instrument/detector/pixel_mask"


def test_a_meta_file_whose_mask_is_not_2d_is_passed_over(tmp_path: Path) -> None:
    _data(tmp_path / f"{PREFIX}_000001.h5")
    with h5py.File(tmp_path / f"{PREFIX}_meta.h5", "w") as handle:
        handle.create_dataset("mask", data=np.zeros(5, dtype="u4"))

    assert _mask(tmp_path / f"{PREFIX}_000001.h5")[0] == 404


def test_a_bare_mask_dataset_is_read_only_from_a_collection_file(tmp_path: Path) -> None:
    """`/mask` is Odin's spelling; an unrelated file's `/mask` is not a pixel mask."""
    loose = tmp_path / "summed.h5"
    with h5py.File(loose, "w") as handle:
        handle.create_dataset("data", data=np.zeros((2, *SHAPE), dtype="u4"))
        handle.create_dataset("mask", data=np.ones(SHAPE, dtype="u4"))

    assert _mask(loose)[0] == 404


# --------------------------------------------------------------------------
# The browser's series view
# --------------------------------------------------------------------------


def test_data_and_meta_files_are_led_by_their_nexus_master() -> None:
    names = [
        f"{PREFIX}_000001.h5",
        f"{PREFIX}_000002.h5",
        f"{PREFIX}_meta.h5",
        f"{PREFIX}_w00001.nxs",
        "CuHF2PYZ2PF6_00002_000001.h5",  # a collection without a master
        "CuHF2PYZ2PF6_00002_meta.h5",
        "notes.txt",
    ]
    assert nexus_collection_leads(names) == {
        f"{PREFIX}_000001.h5": f"{PREFIX}_w00001.nxs",
        f"{PREFIX}_000002.h5": f"{PREFIX}_w00001.nxs",
        f"{PREFIX}_meta.h5": f"{PREFIX}_w00001.nxs",
    }


def test_the_master_named_after_the_collection_wins() -> None:
    names = [f"{PREFIX}_000001.h5", f"{PREFIX}_w00001.nxs", f"{PREFIX}.nxs"]
    assert nexus_collection_leads(names)[f"{PREFIX}_000001.h5"] == f"{PREFIX}.nxs"


def _browse(folder: Path, mode: str) -> list[tuple[str, int, bool]]:
    response = _client().get("/api/browse", params={"path": str(folder), "series_mode": mode})
    assert response.status_code == 200, response.text
    return [(i["name"], i["seriesCount"], i["isSeriesLead"]) for i in response.json()["fileItems"]]


@pytest.fixture
def visit(tmp_path: Path) -> Path:
    for index in (1, 2):
        _data(tmp_path / f"{PREFIX}_{index:06d}.h5")
    _meta(tmp_path / f"{PREFIX}_meta.h5")
    _nxs(tmp_path / f"{PREFIX}_w00001.nxs", f"{PREFIX}_meta.h5")
    _data(tmp_path / "CuHF2PYZ2PF6_00002_000001.h5")  # no master yet
    return tmp_path


def test_the_series_view_shows_a_collection_as_its_master(visit: Path) -> None:
    assert sorted(_browse(visit, "first_only")) == [
        (f"{PREFIX}_w00001.nxs", 2, True),
        ("CuHF2PYZ2PF6_00002_000001.h5", 1, False),  # left alone: no master to open
    ]


def test_the_full_view_still_lists_every_file(visit: Path) -> None:
    assert len(_browse(visit, "all")) == 5
