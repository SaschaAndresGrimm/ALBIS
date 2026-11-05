"""A data file opened on its own takes its metadata from its collection's files.

Reported from Diamond I19-2: an Eiger collection is written as
`PREFIX_000001.h5` (frames, a bare `/data`), `PREFIX_meta.h5` (Odin, the
detector's settings under `/_dectris`) and `PREFIX[_w00001].nxs` (nexgen's NXmx
master). Opening the data file, as adxv users do, gave frames with no energy,
distance, pixel size or beam centre, so no resolution rings -- and ALBIS would
not open the `.nxs` at all. The layout below is a small copy of the real one
(dials_data `four_circle_eiger`, Zenodo 6093231), with values that differ per
file so each test can tell which file answered.
"""

from __future__ import annotations

from pathlib import Path

import h5py
import numpy as np
import pytest
from fastapi.testclient import TestClient

from backend.services.companion_files import (
    collection_prefix,
    companion_metadata_files,
    companion_names,
)
from backend.services.path_policy import PathPolicy

PREFIX = "CuHF2PYZ2PF6_00001"


def _data_file(path: Path, frames: int = 2) -> None:
    """Odin's data file: frames and nothing else."""
    with h5py.File(path, "w") as handle:
        handle.create_dataset("data", data=np.zeros((frames, 8, 6), dtype="u4"))


def _meta_file(path: Path, energy_ev: float = 25516.4) -> None:
    """Odin's meta file: the detector's configuration, unitless (m, eV, px)."""
    with h5py.File(path, "w") as handle:
        group = handle.create_group("_dectris")
        group["photon_energy"] = np.array([energy_ev])
        group["wavelength"] = np.array([0.4859])
        group["detector_distance"] = np.array([0.085])
        group["x_pixel_size"] = np.array([7.5e-05])
        group["y_pixel_size"] = np.array([7.5e-05])
        group["beam_center_x"] = np.array([985.0666])
        group["beam_center_y"] = np.array([1419.4666])


def _nxs_file(path: Path, data_name: str, wavelength_a: float = 0.4859) -> None:
    """nexgen's NXmx master: geometry with units, frames by external link."""
    with h5py.File(path, "w") as handle:
        entry = handle.create_group("entry")
        entry.attrs["NX_class"] = "NXentry"
        entry["data/data_000001"] = h5py.ExternalLink(data_name, "/data")
        beam = entry.create_group("instrument/beam")
        beam["incident_wavelength"] = wavelength_a
        beam["incident_wavelength"].attrs["units"] = "angstrom"
        det = entry.create_group("instrument/detector")
        for name, value, units in (
            ("detector_distance", 0.120, "m"),
            ("x_pixel_size", 7.5e-05, "m"),
            ("y_pixel_size", 7.5e-05, "m"),
            ("beam_center_x", 1000.0, "pixels"),
            ("beam_center_y", 1500.0, "pixels"),
        ):
            det[name] = value
            det[name].attrs["units"] = units


@pytest.fixture
def collection(tmp_path: Path) -> Path:
    folder = tmp_path / "visit"
    folder.mkdir()
    _data_file(folder / f"{PREFIX}_000001.h5")
    return folder


def _params(path: Path, dataset: str = "/data") -> dict:
    from backend.app import app

    client = TestClient(app, base_url="http://localhost")
    response = client.get("/api/analysis/params", params={"file": str(path), "dataset": dataset})
    assert response.status_code == 200, response.text
    return response.json()


# --------------------------------------------------------------------------
# Which names belong to a collection
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("name", "prefix"),
    [
        (f"{PREFIX}_000001.h5", PREFIX),
        (f"{PREFIX}_000002.H5", PREFIX),
        (f"{PREFIX}_meta.h5", PREFIX),
        ("scan_data_000001.h5", "scan"),  # DECTRIS file writer, not `scan_data`
        ("scan_master.h5", None),
        (f"{PREFIX}.nxs", None),
        ("summed.h5", None),
        ("frame_0001.h5", None),  # four digits: a numbered single, not Odin
    ],
)
def test_the_collection_is_read_from_the_name(name: str, prefix: str | None) -> None:
    assert collection_prefix(name) == prefix


def test_masters_come_before_the_meta_file_and_strangers_are_left_out() -> None:
    folder = [
        f"{PREFIX}_meta.h5",
        f"{PREFIX}_000001.h5",
        f"{PREFIX}_w00001.nxs",  # Diamond's name for some collections
        f"{PREFIX}.nxs",
        f"{PREFIX}_master.h5",
        "CuHF2PYZ2PF6_00002.nxs",  # the next collection
        "CuHF2PYZ2PF6_00001x.nxs",  # a different prefix that merely starts the same
        "notes.txt",
    ]
    assert companion_names(PREFIX, folder) == [
        f"{PREFIX}.nxs",
        f"{PREFIX}_w00001.nxs",
        f"{PREFIX}_master.h5",
        f"{PREFIX}_meta.h5",
    ]


def test_names_match_whatever_their_case() -> None:
    assert companion_names("scan", ["SCAN_META.H5"]) == ["SCAN_META.H5"]


def test_a_file_is_never_its_own_companion(collection: Path) -> None:
    meta = collection / f"{PREFIX}_meta.h5"
    _meta_file(meta)
    assert companion_metadata_files(meta) == []


def test_a_name_from_no_collection_has_no_companions(tmp_path: Path) -> None:
    _data_file(tmp_path / "summed.h5")
    (tmp_path / "summed_meta.h5").write_bytes(b"")
    assert companion_metadata_files(tmp_path / "summed.h5") == []


# --------------------------------------------------------------------------
# The data root applies to a file ALBIS found as much as to one it was asked for
# --------------------------------------------------------------------------


def _policy(root: Path, allow_abs_paths: bool) -> PathPolicy:
    return PathPolicy(
        data_dir=root,
        autoload_exts={".h5", ".hdf5", ".nxs"},
        image_ext_name=lambda name: Path(name).suffix.lower(),
        allow_abs_paths=lambda: allow_abs_paths,
    )


def test_a_companion_symlinked_out_of_the_root_is_not_read(tmp_path: Path) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    _data_file(root / f"{PREFIX}_000001.h5")
    _meta_file(outside / "elsewhere.h5")
    (root / f"{PREFIX}_meta.h5").symlink_to(outside / "elsewhere.h5")

    confined = _policy(root, allow_abs_paths=False).accept_found_hdf5_file
    assert companion_metadata_files(root / f"{PREFIX}_000001.h5", confined) == []

    # With absolute paths allowed, a requested file outside the root is
    # readable too, so a found one is held to nothing stricter.
    unconfined = _policy(root, allow_abs_paths=True).accept_found_hdf5_file
    assert companion_metadata_files(root / f"{PREFIX}_000001.h5", unconfined) == [
        (outside / "elsewhere.h5").resolve()
    ]


def test_a_companion_inside_the_root_is_read(collection: Path) -> None:
    _meta_file(collection / f"{PREFIX}_meta.h5")
    accept = _policy(collection.parent, allow_abs_paths=False).accept_found_hdf5_file
    assert companion_metadata_files(collection / f"{PREFIX}_000001.h5", accept) == [
        (collection / f"{PREFIX}_meta.h5").resolve()
    ]


# --------------------------------------------------------------------------
# The route: the reported case
# --------------------------------------------------------------------------


def test_a_data_file_alone_has_no_geometry(collection: Path) -> None:
    params = _params(collection / f"{PREFIX}_000001.h5")
    assert params["energy_ev"] is None and params["distance_mm"] is None


def test_a_data_file_takes_its_geometry_from_the_meta_file(collection: Path) -> None:
    """The reported bug: without this every field was empty, so no rings."""
    _meta_file(collection / f"{PREFIX}_meta.h5")

    params = _params(collection / f"{PREFIX}_000001.h5")

    assert params["energy_ev"] == pytest.approx(25516.4)
    assert params["distance_mm"] == pytest.approx(85.0)
    assert params["pixel_size_um"] == pytest.approx(75.0)
    assert params["center_x_px"] == pytest.approx(985.0666)
    assert params["center_y_px"] == pytest.approx(1419.4666)
    assert params["shape"] == [2, 8, 6], "the shape is still the data file's own"


def test_the_nexus_master_wins_over_the_meta_file(collection: Path) -> None:
    _meta_file(collection / f"{PREFIX}_meta.h5")
    _nxs_file(collection / f"{PREFIX}_w00001.nxs", f"{PREFIX}_000001.h5", wavelength_a=1.0)

    params = _params(collection / f"{PREFIX}_000001.h5")

    # 1 A from the master, not the meta file's 25516 eV.
    assert params["energy_ev"] == pytest.approx(12398.4193)
    assert params["distance_mm"] == pytest.approx(120.0)
    assert params["center_x_px"] == pytest.approx(1000.0)


def test_the_meta_file_opened_directly_gives_its_own_settings(collection: Path) -> None:
    meta = collection / f"{PREFIX}_meta.h5"
    _meta_file(meta, energy_ev=17000.0)
    with h5py.File(meta, "a") as handle:
        handle.create_dataset("mask", data=np.zeros((8, 6), dtype="u4"))

    params = _params(meta, dataset="/mask")

    assert params["energy_ev"] == pytest.approx(17000.0)
    assert params["distance_mm"] == pytest.approx(85.0)


def test_the_nexus_master_opens_and_reads_its_own_geometry(collection: Path) -> None:
    """`.nxs` used to be refused outright as "File not found"."""
    from backend.app import app

    nxs = collection / f"{PREFIX}_w00001.nxs"
    _nxs_file(nxs, f"{PREFIX}_000001.h5")
    client = TestClient(app, base_url="http://localhost")

    datasets = client.get("/api/datasets", params={"file": str(nxs)})
    assert datasets.status_code == 200, datasets.text
    paths = [item["path"] for item in datasets.json()["datasets"] if item["image"]]
    assert "/entry/data/data_000001" in paths

    params = _params(nxs, dataset="/entry/data/data_000001")
    assert params["energy_ev"] == pytest.approx(25516.4, rel=1e-4)
    assert params["distance_mm"] == pytest.approx(120.0)

    frame = client.get(
        "/api/frame",
        params={"file": str(nxs), "dataset": "/entry/data/data_000001", "index": 1},
    )
    assert frame.status_code == 200


def test_the_file_browser_lists_nexus_files(collection: Path) -> None:
    from backend.app import app

    _nxs_file(collection / f"{PREFIX}.nxs", f"{PREFIX}_000001.h5")
    client = TestClient(app, base_url="http://localhost")

    listing = client.get("/api/browse", params={"path": str(collection)})

    assert f"{PREFIX}.nxs" in listing.json()["files"]
