from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import HTTPException

from backend.image_formats import _image_ext_name
from backend.services.path_policy import PathPolicy

AUTOLOAD_EXTS = {".h5", ".hdf5", ".tif", ".tiff", ".cbf", ".cbf.gz", ".edf"}


def _policy(data_dir: Path, allow_abs_paths: bool = True) -> PathPolicy:
    return PathPolicy(
        data_dir=data_dir,
        autoload_exts=AUTOLOAD_EXTS,
        image_ext_name=_image_ext_name,
        allow_abs_paths=lambda: allow_abs_paths,
    )


@pytest.mark.parametrize("value", ["", "/tmp/test.h5", "../test.h5", ".hidden/file.h5", "a/.."])
def test_safe_rel_path_rejects_invalid_inputs(tmp_path: Path, value: str) -> None:
    policy = _policy(tmp_path)
    with pytest.raises(HTTPException):
        policy.safe_rel_path(value)


def test_resolve_hdf5_file_allows_relative_and_validates_suffix(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    (tmp_path / "scan.h5").write_bytes(b"")
    (tmp_path / "scan.txt").write_bytes(b"")

    assert policy.resolve_hdf5_file("scan.h5") == (tmp_path / "scan.h5").resolve()
    with pytest.raises(HTTPException) as exc:
        policy.resolve_hdf5_file("scan.txt")
    assert exc.value.status_code == 404


def test_resolve_hdf5_file_blocks_absolute_when_disabled(tmp_path: Path) -> None:
    target = (tmp_path / "scan.h5").resolve()
    target.write_bytes(b"")
    policy = _policy(tmp_path, allow_abs_paths=False)
    with pytest.raises(HTTPException) as exc:
        policy.resolve_hdf5_file(str(target))
    assert exc.value.status_code == 400


def test_resolve_dir_resolves_default_and_relative(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    sub = tmp_path / "nested"
    sub.mkdir()

    assert policy.resolve_dir(None) == tmp_path.resolve()
    assert policy.resolve_dir(".") == tmp_path.resolve()
    assert policy.resolve_dir("nested") == sub.resolve()


def test_resolve_image_file_supports_compound_extensions(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    image = tmp_path / "frame_0001.cbf.gz"
    image.write_bytes(b"")
    resolved = policy.resolve_image_file("frame_0001.cbf.gz")
    assert resolved == image.resolve()


def test_parse_ext_filter_normalizes_and_intersects(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    assert policy.parse_ext_filter("cbf") == {".cbf", ".cbf.gz"}
    assert policy.parse_ext_filter("tif,unknown") == {".tif"}
    assert policy.parse_ext_filter(None) == AUTOLOAD_EXTS
