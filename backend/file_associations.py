"""The file types ALBIS registers with the desktop environment.

One list, because the same set has to be spelled out in four unrelated places
that no compiler checks against each other: the Windows installer's registry
keys, the Linux `.desktop` file and its shared-MIME-info XML, the macOS bundle's
`CFBundleDocumentTypes`, and the launcher's own check on a path handed to it by
the operating system. `tests/test_file_associations.py` asserts the four agree
with this module, so adding a format here fails until it is registered
everywhere.

Two supported formats are deliberately absent:

`.cfg` is in `AUTOLOAD_EXTS` because a MYTHEN acquisition writes one, but it is
also the extension half the software on a workstation uses for its own
configuration. Claiming it would take over files that have nothing to do with
ALBIS, which is worse for the user than having to open theirs by hand.

`.cbf.gz` cannot be registered at all. Every one of the three desktop
environments keys associations off the last extension, so there is nothing to
claim but `.gz` -- and an image viewer that opens every gzip archive on the
system would be a bug, not a feature.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FileAssociation:
    """One extension ALBIS offers to open."""

    extension: str
    """Lower-case, with the leading dot."""

    mime_type: str
    """For the Linux desktop entry. Shared with other extensions where the
    format already has a registered type."""

    description: str
    """Shown by the file manager, and as the Windows ProgID's friendly name."""

    uti: str
    """The macOS Uniform Type Identifier. `public.*` where Apple already
    declares one, otherwise an ALBIS-scoped identifier the bundle imports."""


FILE_ASSOCIATIONS: tuple[FileAssociation, ...] = (
    FileAssociation(".h5", "application/x-hdf5", "HDF5 data file", "org.hdfgroup.hdf5"),
    FileAssociation(".hdf5", "application/x-hdf5", "HDF5 data file", "org.hdfgroup.hdf5"),
    FileAssociation(
        ".cbf",
        "image/x-cbf",
        "Crystallographic Binary Format image",
        "com.saschaandresgrimm.albis.cbf",
    ),
    FileAssociation(
        ".edf",
        "image/x-edf",
        "ESRF Data Format image",
        "com.saschaandresgrimm.albis.edf",
    ),
    FileAssociation(".tif", "image/tiff", "TIFF image", "public.tiff"),
    FileAssociation(".tiff", "image/tiff", "TIFF image", "public.tiff"),
)

ASSOCIATED_EXTENSIONS: frozenset[str] = frozenset(item.extension for item in FILE_ASSOCIATIONS)

# Ordered, de-duplicated: `.h5`/`.hdf5` share one MIME type, as do `.tif`/`.tiff`.
ASSOCIATED_MIME_TYPES: tuple[str, ...] = tuple(
    dict.fromkeys(item.mime_type for item in FILE_ASSOCIATIONS)
)

# Formats ALBIS opens but does not register. See the module docstring.
UNASSOCIATED_EXTENSIONS: frozenset[str] = frozenset({".cfg", ".cbf.gz"})


def is_associated_path(name: str) -> bool:
    """Whether a path names one of the registered types.

    `.cbf.gz` needs no special case: it ends in `.gz`, which is not registered,
    so it falls out on its own rather than by being listed twice.
    """
    lowered = str(name or "").lower()
    return any(lowered.endswith(ext) for ext in ASSOCIATED_EXTENSIONS)
