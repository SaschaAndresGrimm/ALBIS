"""Opening HDF5 files for reading, and reporting what goes wrong as HTTP errors.

Split out of `hdf5_stack.py`. A file a writer still holds (SWMR), a file
something else has locked, and a file that will not decode each need telling
apart, and every route that touches HDF5 opens through here.
"""

from __future__ import annotations

import errno
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any

from fastapi import HTTPException

_log = logging.getLogger("albis.hdf5_stack")


def _text(value: Any) -> str:
    """Decode a name libhdf5 handed back, which may be bytes or str."""
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value or "")


def open_hdf5_read_only(h5py: Any, path: Path) -> Any:
    """Open an HDF5 file for reading, including one a writer still holds open.

    A detector filewriter keeps its output open in SWMR mode for the length of a
    series, and HDF5 refuses a plain read-only open of such a file: it reports
    `file is already open for write`, the same bare `OSError` it uses for a
    corrupt file. Read as "unreadable" that is simply wrong -- the file is
    readable, it just has to be asked for in the mode the writer promised. A
    reader that opens with `swmr=True` sees the frames flushed so far, and
    because every request opens its own handle, the next one sees the frames
    written since.

    Plain first, SWMR second. The plain open is the overwhelmingly common case
    (a finished file) and stays exactly as fast as it was; the retry costs one
    failed open, and only for a file that would otherwise have been refused
    outright. When both fail the *first* error is raised, because for a genuinely
    truncated file it is the one that names the real problem.
    """
    handle, _writer_present = open_hdf5_read_only_reporting_writer(h5py, path)
    return handle


# What HDF5 reports when it cannot take the file lock: EAGAIN (errno 35 on
# macOS, 11 on Linux) or EACCES. The message is matched too, because the errno
# HDF5 surfaces has not been consistent across versions.
_LOCK_ERRNOS = frozenset({errno.EACCES, errno.EAGAIN, errno.EWOULDBLOCK})


def is_hdf5_lock_failure(exc: OSError) -> bool:
    """True when HDF5 refused a file because it could not lock it.

    This is emphatically not corruption -- not one byte of the file was read.
    Something else holds it open (a filewriter mid-acquisition, another viewer,
    a crashed process whose handle has not been reaped), or the filesystem
    cannot take POSIX locks at all, which is routine on the network mounts a
    beamline serves data from. Reporting it as a damaged file sends the user
    looking for a problem with their data that does not exist.
    """
    if exc.errno in _LOCK_ERRNOS:
        return True
    return "unable to lock file" in str(exc).lower()


def open_hdf5_read_only_reporting_writer(h5py: Any, path: Path) -> tuple[Any, bool]:
    """Open for reading, and say whether a writer is still holding the file.

    The second half matters to the interface. A reader that only ever asks once
    sees the frame count the series happened to have at open time, and a series
    being written grows -- so "is someone still writing this" is the difference
    between a slider that follows an acquisition and one that is quietly stale.
    Needing SWMR to open the file at all is exactly that signal, and it costs
    nothing extra to report it.

    Three attempts, narrowing: a plain read; SWMR, for a writer that opened the
    file for concurrent reading; and finally, only for a lock failure, a read
    with locking disabled.
    """
    try:
        return h5py.File(path, "r"), False
    except FileNotFoundError:
        raise
    except OSError as plain_exc:
        try:
            handle = h5py.File(path, "r", swmr=True)
        except (OSError, ValueError, RuntimeError):
            # ValueError is what older HDF5 raises when it cannot do SWMR at
            # all; RuntimeError is what it raises for a malformed superblock,
            # which a truncated file reaches on this second attempt even
            # though the first failed with a plain OSError. Fall through --
            # the plain error is the better diagnosis either way.
            pass
        else:
            _log.info("Opened %s in SWMR mode: a writer still holds it open", path)
            return handle, True

        if not is_hdf5_lock_failure(plain_exc):
            raise plain_exc from None

        # SWMR only helps when the writer opted into it. A file held by an
        # ordinary writer, or sitting on a mount that cannot lock, refuses
        # both attempts -- and a viewer that will not open a perfectly
        # readable file is worse than one that reads it unsynchronised. For a
        # finished file there is nothing to synchronise with anyway.
        #
        # Warning rather than info because it is not free: if a writer really
        # is appending, an unlocked read can catch a chunk mid-write. That is
        # the same exposure SWMR exists to remove, and the reason this is the
        # last attempt rather than the first.
        try:
            handle = h5py.File(path, "r", locking=False)
        except TypeError:
            # h5py predating the `locking` argument.
            raise plain_exc from None
        except (OSError, ValueError, RuntimeError):
            raise plain_exc from None
        _log.warning(
            "Opened %s with file locking disabled (%s). Another process holds "
            "the file, or the filesystem cannot lock it.",
            path,
            plain_exc,
        )
        # Deliberately not reported as a writer: a refused lock says something
        # holds the file, not that frames are still arriving, and claiming a
        # writer would leave the client polling a static file forever.
        return handle, False


def open_hdf5_for_read(h5py: Any, path: Path) -> Any:
    """Open an HDF5 file for reading, reporting an undecodable file as 422.

    h5py signals a truncated or corrupt file with a bare `OSError`, or a
    `RuntimeError` when the superblock itself will not decode -- either would
    otherwise escape a route as a 500. That is the wrong story to tell: the
    request was fine, the bytes on disk are not -- and at a beamline the usual
    reason is a file the filewriter has not finished writing yet, which the
    user can simply retry. 422 with the cause named matches how the MYTHEN
    readers already report an unreadable acquisition.
    """
    handle, _writer_present = open_hdf5_for_read_reporting_writer(h5py, path)
    return handle


def open_hdf5_for_read_reporting_writer(h5py: Any, path: Path) -> tuple[Any, bool]:
    """`open_hdf5_for_read`, plus whether a writer still holds the file."""
    try:
        return open_hdf5_read_only_reporting_writer(h5py, path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="File not found") from exc
    except RuntimeError as exc:
        # "Can't decode file superblock prefix" and friends: the file is not
        # HDF5, or not intact. Same story for the caller as an OSError.
        _log.warning("Cannot open HDF5 file %s: %s", path, exc)
        raise HTTPException(
            status_code=422,
            detail=f"Cannot read {path.name}: not a readable HDF5 file "
            "(it may be incomplete or corrupt)",
        ) from exc
    except OSError as exc:
        _log.warning("Cannot open HDF5 file %s: %s", path, exc)
        if is_hdf5_lock_failure(exc):
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Cannot read {path.name}: the file is locked by another "
                    "program. Close whatever is holding it -- a filewriter "
                    "still running, another viewer, an open analysis session "
                    "-- and try again. The file itself is not damaged."
                ),
            ) from exc
        raise HTTPException(
            status_code=422,
            detail=f"Cannot read {path.name}: not a readable HDF5 file "
            "(it may be incomplete or corrupt)",
        ) from exc


def read_hdf5_array(path: Path, read: Callable[[], Any]) -> Any:
    """Run an HDF5 read, reporting a failure to decode the data as 422.

    Opening a file successfully does not mean its contents can be read. Detector
    data is stored with a compression filter, and HDF5 only needs the matching
    plugin library when the bytes are actually pulled in -- so a missing filter,
    or a chunk the filewriter has not finished flushing, surfaces here rather
    than at open time and would otherwise escape the route as a 500.
    """
    try:
        return read()
    except HTTPException:
        raise
    except OSError as exc:
        _log.warning("Cannot read HDF5 data from %s: %s", path, exc)
        raise HTTPException(
            status_code=422,
            detail=(
                f"Cannot read data from {path.name}: {exc}. The file may be "
                "incomplete, or its compression filter may be unavailable."
            ),
        ) from exc
