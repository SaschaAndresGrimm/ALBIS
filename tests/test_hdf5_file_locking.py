"""A locked HDF5 file is not a corrupt one, and must still open.

A colleague's macOS install refused every HDF5 file in a directory with
"not a readable HDF5 file (it may be incomplete or corrupt)". The files were
fine. The backend log carried the real cause:

    BlockingIOError: [Errno 35] Unable to synchronously open file
    (unable to lock file, errno = 35, ...)

errno 35 is EAGAIN: HDF5 could not take the file lock, so it never read a
byte. Something else held the file, or the filesystem could not lock it.
Telling a scientist their data may be corrupt when it is not sends them
looking for a problem that does not exist -- and ALBIS would not open a file
it was perfectly able to read.

The lock here is taken with flock on a separate descriptor rather than by
spawning a writer: HDF5 reuses an open handle within a process, so a second
h5py.File in the same process would succeed and prove nothing.
"""

from __future__ import annotations

import errno
import os
import sys
from pathlib import Path

import numpy as np
import pytest
from fastapi import HTTPException

h5py = pytest.importorskip("h5py")

from backend.services.hdf5_stack import (  # noqa: E402
    is_hdf5_lock_failure,
    open_hdf5_for_read_reporting_writer,
    open_hdf5_read_only_reporting_writer,
)

pytestmark = pytest.mark.skipif(
    sys.platform.startswith("win"), reason="flock is POSIX; the lock path differs on Windows"
)

DATASET = "/entry/data/data"


@pytest.fixture
def h5_file(tmp_path: Path) -> Path:
    path = tmp_path / "series.h5"
    with h5py.File(path, "w") as handle:
        handle.create_dataset(DATASET, data=np.arange(24, dtype="uint32").reshape(2, 3, 4))
    return path


class Locked:
    """Hold an exclusive flock on a path, as another process would."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def __enter__(self):
        import fcntl

        self.fd = os.open(self.path, os.O_RDWR)
        fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return self

    def __exit__(self, *exc):
        import fcntl

        fcntl.flock(self.fd, fcntl.LOCK_UN)
        os.close(self.fd)


def test_the_fixture_really_blocks_a_plain_open(h5_file: Path) -> None:
    """Guards the guard: without a real lock everything below is vacuous."""
    with Locked(h5_file), pytest.raises(OSError) as caught:
        h5py.File(h5_file, "r")
    assert caught.value.errno in {errno.EAGAIN, errno.EACCES, errno.EWOULDBLOCK}


def test_a_locked_file_still_opens_and_reads_correctly(h5_file: Path) -> None:
    with Locked(h5_file):
        handle, writer_present = open_hdf5_read_only_reporting_writer(h5py, h5_file)
        try:
            # Not merely opened -- the data has to come back intact.
            assert handle[DATASET].shape == (2, 3, 4)
            assert int(handle[DATASET][1, 2, 3]) == 23
        finally:
            handle.close()
    # A refused lock says something holds the file, not that frames are still
    # arriving; claiming a writer would leave the client polling a static file.
    assert writer_present is False


def test_an_unlocked_file_opens_by_the_ordinary_path(h5_file: Path) -> None:
    handle, writer_present = open_hdf5_read_only_reporting_writer(h5py, h5_file)
    handle.close()
    assert writer_present is False


def test_a_genuinely_corrupt_file_is_still_reported_as_corrupt(tmp_path: Path) -> None:
    """The lock fallback must not paper over a real problem."""
    junk = tmp_path / "truncated.h5"
    junk.write_bytes(b"\x89HDF\r\n\x1a\n" + b"\x00" * 64)
    with pytest.raises(HTTPException) as caught:
        open_hdf5_for_read_reporting_writer(h5py, junk)
    assert caught.value.status_code == 422
    assert "incomplete or corrupt" in caught.value.detail
    assert "locked" not in caught.value.detail


def test_the_plain_error_survives_a_retry_that_fails_differently(tmp_path: Path) -> None:
    """The non-HTTP entry point must not leak the retry's exception type.

    A truncated file fails the first open with OSError and the SWMR retry with
    RuntimeError. Callers outside a route (there are several) get whatever
    escapes, so the first, accurate error has to be the one that propagates --
    not the second attempt's incidental failure mode.
    """
    junk = tmp_path / "truncated.h5"
    junk.write_bytes(b"\x89HDF\r\n\x1a\n" + b"\x00" * 64)
    with pytest.raises(OSError) as caught:
        open_hdf5_read_only_reporting_writer(h5py, junk)
    assert not isinstance(caught.value, BlockingIOError)


def test_a_missing_file_is_still_a_404(tmp_path: Path) -> None:
    with pytest.raises(HTTPException) as caught:
        open_hdf5_for_read_reporting_writer(h5py, tmp_path / "absent.h5")
    assert caught.value.status_code == 404


def test_the_lock_message_names_the_lock_when_even_that_fails(h5_file: Path) -> None:
    """If the unlocked read cannot happen either, say lock, not corruption."""

    class NoUnlockedRead:
        """h5py that refuses every open the way a locked file does."""

        @staticmethod
        def File(*_args, **_kwargs):
            raise BlockingIOError(
                errno.EAGAIN,
                "Unable to synchronously open file (unable to lock file, errno = 35)",
            )

    with pytest.raises(HTTPException) as caught:
        open_hdf5_for_read_reporting_writer(NoUnlockedRead, h5_file)
    assert caught.value.status_code == 422
    assert "locked by another program" in caught.value.detail
    assert "corrupt" not in caught.value.detail


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (BlockingIOError(errno.EAGAIN, "unable to lock file"), True),
        (OSError(errno.EACCES, "permission"), True),
        (OSError(0, "Unable to synchronously open file (unable to lock file, errno = 35)"), True),
        (OSError(errno.ENOENT, "no such file"), False),
        (OSError(0, "truncated file: eof"), False),
    ],
)
def test_lock_failures_are_told_apart_from_other_errors(exc: OSError, expected: bool) -> None:
    assert is_hdf5_lock_failure(exc) is expected
