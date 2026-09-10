"""Confinement for the two storage mechanisms libhdf5 resolves below h5py.

`data.allow_abs_paths: false` is documented -- in the Dockerfile's own comment
and in the Power User Guide -- as the reason a deployment cannot read the
filesystem around it. `resolve_external_path` enforces that for an h5py-level
`ExternalLink`, but two mechanisms name files underneath h5py and so never
reached it:

* external raw-data storage (`H5Pset_external`), which points a dataset at
  arbitrary bytes in another file, and
* virtual dataset sources, which libhdf5 opens transparently on read.

Either one, in a crafted `.h5` dropped inside the data root, returned the
contents of any file the server process could open -- as pixels from
`/api/frame`, as a JSON preview from `/api/hdf5/value`, or as a CSV download.

The fix confines rather than forbids: a virtual dataset whose sources sit
beside it inside the root is ordinary detector output, and the filewriter2
master that stitches a series together is precisely what this viewer exists to
open. The regression tests below assert both halves -- escape refused, and the
legitimate in-root layouts still readable.
"""

from __future__ import annotations

from pathlib import Path

import h5py
import numpy as np
import pytest
from fastapi import HTTPException

from backend.services.hdf5_stack import HDF5StackService
from backend.services.path_policy import PathPolicy

SECRET = b"TOPSECRET-BEAMLINE-KEY-0123456789ABCDEF"


def _service(data_dir: Path, allow_abs_paths: bool = False) -> HDF5StackService:
    return HDF5StackService(
        data_dir=data_dir,
        get_allow_abs_paths=lambda: allow_abs_paths,
        is_within=PathPolicy.is_within,
        get_h5py=lambda: h5py,
    )


@pytest.fixture()
def roots(tmp_path: Path) -> tuple[Path, Path]:
    """A served data root, and a sibling directory that must stay unreachable."""
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    return root, outside


def _external_storage_file(path: Path, target: Path, size: int) -> None:
    with h5py.File(path, "w") as handle:
        handle.create_dataset("data", shape=(size,), dtype="u1", external=[(str(target), 0, size)])


def _vds_file(path: Path, source: Path, dset: str = "data") -> None:
    layout = h5py.VirtualLayout(shape=(1, 4, 4), dtype="u2")
    layout[...] = h5py.VirtualSource(str(source), dset, shape=(1, 4, 4))
    with h5py.File(path, "w") as handle:
        handle.create_virtual_dataset("data", layout)


def _plain_stack(path: Path, base: int = 0) -> None:
    with h5py.File(path, "w") as handle:
        handle.create_dataset("data", data=np.arange(16, dtype="u2").reshape(1, 4, 4) + base)


# ---------------------------------------------------------------------------
# Escape is refused
# ---------------------------------------------------------------------------


def test_external_raw_storage_pointing_outside_the_root_is_refused(roots) -> None:
    """The bytes of an arbitrary file, served as a dataset's contents."""
    root, outside = roots
    secret = outside / "secret.bin"
    secret.write_bytes(SECRET)
    evil = root / "evil.h5"
    _external_storage_file(evil, secret, len(SECRET))
    service = _service(root)

    with h5py.File(evil, "r") as handle, pytest.raises(HTTPException) as exc:
        service.assert_dataset_storage_confined(handle["data"], evil)

    assert exc.value.status_code == 422
    assert "outside the permitted data directory" in exc.value.detail


def test_a_virtual_source_outside_the_root_is_refused(roots) -> None:
    root, outside = roots
    source = outside / "source.h5"
    _plain_stack(source, base=100)
    evil = root / "evil_vds.h5"
    _vds_file(evil, source)
    service = _service(root)

    with h5py.File(evil, "r") as handle, pytest.raises(HTTPException) as exc:
        service.assert_dataset_storage_confined(handle["data"], evil)

    assert exc.value.status_code == 422


def test_a_relative_escape_from_the_root_is_refused(roots) -> None:
    """`../` in the stored name resolves before the containment check."""
    root, outside = roots
    secret = outside / "secret.bin"
    secret.write_bytes(SECRET)
    evil = root / "evil_rel.h5"
    with h5py.File(evil, "w") as handle:
        handle.create_dataset(
            "data",
            shape=(len(SECRET),),
            dtype="u1",
            external=[(f"../outside/{secret.name}", 0, len(SECRET))],
        )
    service = _service(root)

    with h5py.File(evil, "r") as handle, pytest.raises(HTTPException):
        service.assert_dataset_storage_confined(handle["data"], evil)


def test_resolve_node_applies_the_guard(roots) -> None:
    """The chokepoint, not just the helper: every read path resolves here."""
    root, outside = roots
    source = outside / "source.h5"
    _plain_stack(source, base=100)
    evil = root / "evil_vds.h5"
    _vds_file(evil, source)
    service = _service(root)

    with h5py.File(evil, "r") as handle, pytest.raises(HTTPException) as exc:
        service.resolve_node(handle, evil, "/data")

    assert exc.value.status_code == 422


# ---------------------------------------------------------------------------
# Legitimate layouts keep working
# ---------------------------------------------------------------------------


def test_an_ordinary_dataset_is_untouched(roots) -> None:
    root, _outside = roots
    plain = root / "plain.h5"
    _plain_stack(plain)
    service = _service(root)

    with h5py.File(plain, "r") as handle:
        service.assert_dataset_storage_confined(handle["data"], plain)
        node, _file, opened = service.resolve_node(handle, plain, "/data")
        assert node.shape == (1, 4, 4)
        assert not opened


def test_a_virtual_source_inside_the_root_is_allowed(roots) -> None:
    """The filewriter2 shape: a master stitching data files beside it.

    Refusing virtual datasets outright would close this, which is the layout
    the viewer exists to open.
    """
    root, _outside = roots
    source = root / "series_data_000001.h5"
    _plain_stack(source, base=100)
    master = root / "series_master.h5"
    _vds_file(master, source)
    service = _service(root)

    with h5py.File(master, "r") as handle:
        service.assert_dataset_storage_confined(handle["data"], master)
        assert handle["data"][0, 0, 0] == 100


def test_a_self_referential_virtual_dataset_is_allowed(roots) -> None:
    """h5py stores `.` for a source in the dataset's own file."""
    root, _outside = roots
    path = root / "self.h5"
    layout = h5py.VirtualLayout(shape=(1, 4, 4), dtype="u2")
    layout[...] = h5py.VirtualSource(".", "real", shape=(1, 4, 4))
    with h5py.File(path, "w") as handle:
        handle.create_dataset("real", data=np.arange(16, dtype="u2").reshape(1, 4, 4))
        handle.create_virtual_dataset("data", layout)
    service = _service(root)

    with h5py.File(path, "r") as handle:
        service.assert_dataset_storage_confined(handle["data"], path)


def test_allow_abs_paths_restores_the_unconfined_behaviour(roots) -> None:
    """The setting is what the guard keys on, so turning it on lifts the guard."""
    root, outside = roots
    source = outside / "source.h5"
    _plain_stack(source, base=100)
    evil = root / "evil_vds.h5"
    _vds_file(evil, source)
    service = _service(root, allow_abs_paths=True)

    with h5py.File(evil, "r") as handle:
        service.assert_dataset_storage_confined(handle["data"], evil)


# ---------------------------------------------------------------------------
# The rejections in PathPolicy that were live but unasserted
# ---------------------------------------------------------------------------


def _policy(data_dir: Path, allow_abs_paths: bool = False) -> PathPolicy:
    return PathPolicy(
        data_dir=data_dir,
        autoload_exts={".h5", ".hdf5"},
        image_ext_name=lambda name: Path(name).suffix.lower(),
        allow_abs_paths=lambda: allow_abs_paths,
    )


def test_a_symlink_inside_the_root_pointing_out_of_it_is_refused(roots) -> None:
    """`resolve()` follows the link, so containment is judged on the target."""
    root, outside = roots
    real = outside / "secret.h5"
    real.write_bytes(b"")
    link = root / "innocent.h5"
    try:
        link.symlink_to(real)
    except (OSError, NotImplementedError):  # pragma: no cover - platform dependent
        pytest.skip("symlinks unavailable on this platform")
    policy = _policy(root)

    with pytest.raises(HTTPException) as exc:
        policy.resolve_hdf5_file("innocent.h5")

    assert exc.value.status_code == 400


def test_a_symlinked_directory_escaping_the_root_is_refused(roots) -> None:
    root, outside = roots
    link = root / "elsewhere"
    try:
        link.symlink_to(outside, target_is_directory=True)
    except (OSError, NotImplementedError):  # pragma: no cover - platform dependent
        pytest.skip("symlinks unavailable on this platform")
    policy = _policy(root)

    with pytest.raises(HTTPException) as exc:
        policy.resolve_dir("elsewhere")

    assert exc.value.status_code == 400


# ---------------------------------------------------------------------------
# End to end, through the routes that actually served the bytes
# ---------------------------------------------------------------------------


@pytest.fixture()
def confined_client(roots, monkeypatch):
    """A client whose data root is the fixture root, with escapes disabled."""
    import dataclasses

    from fastapi.testclient import TestClient

    from backend import app as backend_app

    root, outside = roots
    # Three places hold the root: the runtime state, the path policy that turns
    # a query parameter into a path, and the stack service that reads it. They
    # are each snapshotted at import, so all three have to move together.
    monkeypatch.setattr(backend_app.runtime_state, "allow_abs_paths", False)
    monkeypatch.setattr(backend_app.runtime_state, "data_dir", root)
    # PathPolicy is frozen, so the instance is replaced rather than mutated;
    # `_resolve_file` reads the module global on each call, so this takes.
    monkeypatch.setattr(
        backend_app, "path_policy", dataclasses.replace(backend_app.path_policy, data_dir=root)
    )
    monkeypatch.setattr(backend_app.hdf5_stack, "data_dir", root)
    client = TestClient(backend_app.app)
    client.headers.update({"Host": "localhost"})
    return client, root, outside


def test_the_inspector_routes_no_longer_serve_out_of_root_bytes(confined_client) -> None:
    """`/api/hdf5/value` and `/api/hdf5/csv` returned the file's contents.

    The preview came back as a JSON array of the secret's bytes, and the CSV
    route streamed the same thing as a download.
    """
    client, root, outside = confined_client
    secret = outside / "secret.bin"
    secret.write_bytes(SECRET)
    _external_storage_file(root / "evil.h5", secret, len(SECRET))

    for route in ("/api/hdf5/value", "/api/hdf5/csv", "/api/hdf5/node"):
        response = client.get(route, params={"file": "evil.h5", "path": "/data"})
        assert response.status_code == 422, route
        assert SECRET[:9].decode() not in response.text, route
        assert str(SECRET[0]) not in response.text.split("detail")[-1], route


def test_the_frame_route_no_longer_serves_an_out_of_root_virtual_source(
    confined_client,
) -> None:
    """The one that mattered most: `/api/frame` is the primary read path.

    A plain `ExternalLink` was already refused here, which is why the gap
    looked like it only reached the inspector. A virtual source went straight
    through and returned the out-of-root pixels with a 200.
    """
    client, root, outside = confined_client
    source = outside / "source.h5"
    _plain_stack(source, base=100)
    _vds_file(root / "evil_vds.h5", source)

    response = client.get(
        "/api/frame", params={"file": "evil_vds.h5", "dataset": "/data", "index": 0}
    )

    assert response.status_code == 422
    assert response.headers.get("X-Shape") is None


def test_the_controls_that_already_worked_still_do(confined_client) -> None:
    """Guards against the fix quietly replacing one refusal with another."""
    client, _root, outside = confined_client
    (outside / "source.h5").write_bytes(b"")

    absolute = client.get(
        "/api/hdf5/value", params={"file": str(outside / "source.h5"), "path": "/data"}
    )
    traversal = client.get(
        "/api/hdf5/value", params={"file": "../outside/source.h5", "path": "/data"}
    )

    assert absolute.status_code == 400
    assert "Absolute paths are disabled" in absolute.text
    assert traversal.status_code == 400


def test_a_legitimate_in_root_series_master_still_reads(confined_client) -> None:
    """A filewriter2-shaped master over data files beside it, end to end."""
    client, root, _outside = confined_client
    for index in (1, 2):
        with h5py.File(root / f"series_data_{index:06d}.h5", "w") as handle:
            handle.create_dataset(
                "data", data=np.arange(32, dtype="u2").reshape(2, 4, 4) + index * 100
            )
    layout = h5py.VirtualLayout(shape=(4, 4, 4), dtype="u2")
    for index in (1, 2):
        layout[(index - 1) * 2 : index * 2] = h5py.VirtualSource(
            f"series_data_{index:06d}.h5", "data", shape=(2, 4, 4)
        )
    with h5py.File(root / "series_master.h5", "w") as handle:
        handle.create_virtual_dataset("data", layout)

    response = client.get(
        "/api/frame", params={"file": "series_master.h5", "dataset": "/data", "index": 3}
    )

    assert response.status_code == 200
    frame = np.frombuffer(response.content, dtype=response.headers["X-Dtype"])
    # Frame 3 is the second frame of the second source file.
    assert frame[0] == 216
