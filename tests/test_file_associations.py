"""The four places that register ALBIS as a file handler must agree.

A colleague on Windows asked for `.h5` to open in ALBIS by double-clicking it.
Granting that means spelling the same set of formats out in four unrelated
files, none of which any compiler checks against the others: the Inno Setup
registry keys, the Linux desktop entry and its shared-MIME-info XML, the macOS
bundle's `CFBundleDocumentTypes`, and the launcher's own check on the path the
operating system hands it.

`backend/file_associations.py` is the list; these tests are what stop the four
from drifting apart. Adding a format there fails here until it is registered
everywhere, which is the only way a change like this stays honest -- three of
the four cannot be exercised on the machine that edits them.
"""

from __future__ import annotations

import configparser
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from backend.file_associations import (
    ASSOCIATED_EXTENSIONS,
    ASSOCIATED_MIME_TYPES,
    FILE_ASSOCIATIONS,
    UNASSOCIATED_EXTENSIONS,
    is_associated_path,
)

ROOT = Path(__file__).resolve().parents[1]
ISS = ROOT / "scripts" / "installer_windows.iss"
DESKTOP = ROOT / "packaging" / "linux" / "ALBIS.desktop"
MIME_XML = ROOT / "packaging" / "linux" / "ALBIS-mime.xml"
INSTALL_SH = ROOT / "scripts" / "install_linux_appimage.sh"
UNINSTALL_SH = ROOT / "scripts" / "uninstall_linux.sh"
SPEC = ROOT / "ALBIS.spec"


# --------------------------------------------------------------------------
# The list itself
# --------------------------------------------------------------------------


def test_the_supported_formats_are_covered_or_excluded_on_purpose() -> None:
    """Every format ALBIS opens is either registered or listed as not.

    `AUTOLOAD_EXTS` is the set the viewer can read. A format that is in neither
    the associated nor the deliberately-unassociated set is an oversight, not a
    decision -- which is what this catches.
    """
    from backend.app import AUTOLOAD_EXTS

    accounted = ASSOCIATED_EXTENSIONS | UNASSOCIATED_EXTENSIONS
    assert accounted == AUTOLOAD_EXTS, (
        "a supported format is neither registered nor deliberately excluded: "
        f"{AUTOLOAD_EXTS ^ accounted}"
    )


def test_the_two_exclusions_are_the_intended_ones() -> None:
    # Stated explicitly so removing either one has to be deliberate: `.cfg` is
    # every other program's config extension, and `.cbf.gz` ends in `.gz`, so
    # claiming it would claim every gzip archive on the machine.
    assert {".cfg", ".cbf.gz"} == UNASSOCIATED_EXTENSIONS


@pytest.mark.parametrize(
    "name",
    ["frame.h5", "master.HDF5", "image.cbf", "scan.edf", "pic.tif", "pic.TIFF"],
)
def test_associated_paths_are_recognized(name: str) -> None:
    assert is_associated_path(name)


@pytest.mark.parametrize(
    "name",
    [
        "series.cbf.gz",  # ends in .gz, which ALBIS must never claim
        "mythen.cfg",
        "notes.txt",
        "archive.gz",
        "",
    ],
)
def test_unassociated_paths_are_not(name: str) -> None:
    assert not is_associated_path(name)


# --------------------------------------------------------------------------
# Windows: Inno Setup
# --------------------------------------------------------------------------


BACKSLASH = "\\"


def _classes_key(ext: str = "") -> str:
    """The registry path `Software\\Classes\\<ext>\\`, concatenated not written.

    A literal backslash immediately before a closing quote is valid Python and
    ruff accepts it, but black's parser does not -- and black is what CI gates
    on. Building the string sidesteps the argument entirely.
    """
    key = "Software" + BACKSLASH + "Classes" + BACKSLASH
    return key + ext + BACKSLASH if ext else key


def _iss_text() -> str:
    return ISS.read_text(encoding="utf-8")


def test_windows_installer_registers_every_extension() -> None:
    text = _iss_text()
    for ext in sorted(ASSOCIATED_EXTENSIONS):
        expected = 'Subkey: "' + _classes_key(ext) + 'OpenWithProgids"'
        assert expected in text, ext + " is not registered in the Windows installer"


def test_windows_installer_claims_no_excluded_extension() -> None:
    text = _iss_text()
    for ext in sorted(UNASSOCIATED_EXTENSIONS):
        assert '"' + _classes_key(ext) not in text, ext + " must not be registered"
    # `.gz` is what `.cbf.gz` would really register as, so name it directly.
    assert '"' + _classes_key(".gz") not in text


def test_windows_installer_adds_itself_rather_than_taking_the_default() -> None:
    """OpenWithProgids, never the extension's default value.

    Since Windows 10 an installer cannot silently become the default handler
    anyway, so overwriting the default only breaks whatever held it -- usually
    HDFView or a Python install on a detector workstation, which the user still
    wants.
    """
    for ext in sorted(ASSOCIATED_EXTENSIONS):
        # The extension's own key with an empty ValueName is its default handler.
        bare = 'Subkey: "' + "Software" + BACKSLASH + "Classes" + BACKSLASH + ext + '";'
        for line in _iss_text().splitlines():
            if line.startswith("Root:") and bare in line:
                assert 'ValueName: ""' not in line, ext + " default handler is overwritten"


def test_windows_open_command_quotes_the_path() -> None:
    # Without the inner quotes a path containing a space arrives as two
    # arguments and the file never opens.
    command = '"""{app}' + BACKSLASH + 'ALBIS.exe"" ""%1"""'
    assert command in _iss_text()


def test_windows_associations_are_opt_in_and_refresh_the_shell() -> None:
    text = _iss_text()
    assert 'Name: "associate";' in text, "the user must be able to decline"
    assert text.count("Tasks: associate") >= len(ASSOCIATED_EXTENSIONS)
    # HKCU entries are invisible to Explorer until it is told to re-read them.
    assert "ChangesAssociations=yes" in text


def test_windows_uninstall_leaves_other_handlers_alone() -> None:
    """The OpenWithProgids key belongs to the extension, not to ALBIS.

    Deleting the key would take every other application listed in it with us,
    so those rows remove only ALBIS's own value.
    """
    for line in _iss_text().splitlines():
        if "OpenWithProgids" in line and line.startswith("Root:"):
            assert "uninsdeletevalue" in line, line
            assert "uninsdeletekey" not in line, line


# --------------------------------------------------------------------------
# Linux: desktop entry and MIME definitions
# --------------------------------------------------------------------------


def _desktop_entry(path: Path) -> configparser.SectionProxy:
    parser = configparser.ConfigParser(interpolation=None)
    parser.read_string(path.read_text(encoding="utf-8"))
    return parser["Desktop Entry"]


def test_linux_desktop_entry_declares_the_mime_types() -> None:
    entry = _desktop_entry(DESKTOP)
    declared = [item for item in entry["MimeType"].split(";") if item]
    assert declared == list(ASSOCIATED_MIME_TYPES)


def test_linux_desktop_entry_is_passed_the_file() -> None:
    # Without a field code the desktop environment launches ALBIS with no
    # argument at all, and the association silently does nothing.
    assert _desktop_entry(DESKTOP)["Exec"] == "ALBIS %f"


def test_the_installed_desktop_entry_matches_the_bundled_one() -> None:
    """Two desktop files, one type list.

    The AppImage carries `packaging/linux/ALBIS.desktop`; the installer script
    writes its own, because the Exec path differs. The MIME list must not.
    """
    script = INSTALL_SH.read_text(encoding="utf-8")
    expected = "MimeType=" + "".join(f"{item};" for item in ASSOCIATED_MIME_TYPES)
    assert expected in script
    assert "Exec=$LAUNCHER_PATH %f" in script


def test_mime_xml_declares_the_types_nothing_else_owns() -> None:
    """CBF and EDF only -- and deliberately not HDF5 or TIFF.

    `application/x-hdf5` and `image/tiff` are already in shared-mime-info.
    Redeclaring a type the system already defines risks contradicting it, and
    gains nothing: the desktop entry can claim them as they stand.
    """
    ns = {"m": "http://www.freedesktop.org/standards/shared-mime-info"}
    root = ET.parse(MIME_XML).getroot()
    declared = {node.attrib["type"] for node in root.findall("m:mime-type", ns)}
    assert declared == {"image/x-cbf", "image/x-edf"}

    globs = {
        node.attrib["type"]: {g.attrib["pattern"] for g in node.findall("m:glob", ns)}
        for node in root.findall("m:mime-type", ns)
    }
    assert globs["image/x-cbf"] == {"*.cbf"}
    assert globs["image/x-edf"] == {"*.edf"}


def test_every_declared_mime_type_is_either_standard_or_declared_here() -> None:
    ns = {"m": "http://www.freedesktop.org/standards/shared-mime-info"}
    root = ET.parse(MIME_XML).getroot()
    ours = {node.attrib["type"] for node in root.findall("m:mime-type", ns)}
    standard = {"application/x-hdf5", "image/tiff"}
    for mime in ASSOCIATED_MIME_TYPES:
        assert mime in ours or mime in standard, f"{mime} is claimed but never defined"


def test_the_installer_registers_and_the_uninstaller_removes_the_mime_file() -> None:
    install = INSTALL_SH.read_text(encoding="utf-8")
    assert "share/mime/packages" in install
    assert (
        "update-mime-database" in install
    ), "a new type is invisible until the database is rebuilt"

    uninstall = UNINSTALL_SH.read_text(encoding="utf-8")
    assert 'rm -f "$PREFIX/share/mime/packages/ALBIS.xml"' in uninstall
    assert "update-mime-database" in uninstall


# --------------------------------------------------------------------------
# macOS: the bundle's document types
# --------------------------------------------------------------------------


def _spec_info_plist() -> dict:
    """The `info_plist` dict from ALBIS.spec, without running PyInstaller.

    Read, not executed: at module scope the spec calls into PyInstaller's build
    machinery, which is not importable here.

    Only the entries that are literals come back. Two of them are not -- the
    version strings are computed from the environment -- and evaluating the
    dict as a whole fails on those rather than on anything this file asserts.
    """
    import ast

    tree = ast.parse(SPEC.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not (isinstance(node, ast.keyword) and node.arg == "info_plist"):
            continue
        assert isinstance(node.value, ast.Dict), "info_plist is no longer a literal dict"
        plist: dict = {}
        for key_node, value_node in zip(node.value.keys, node.value.values, strict=True):
            try:
                key = ast.literal_eval(key_node)
                plist[key] = ast.literal_eval(value_node)
            except ValueError:
                continue  # A computed value; none of the ones asserted here are.
        return plist
    raise AssertionError("ALBIS.spec no longer passes info_plist to BUNDLE()")


def test_macos_bundle_declares_a_document_type_per_uti() -> None:
    plist = _spec_info_plist()
    declared: set[str] = set()
    for entry in plist["CFBundleDocumentTypes"]:
        declared.update(entry["LSItemContentTypes"])
    assert declared == {item.uti for item in FILE_ASSOCIATIONS}


def test_macos_document_types_are_viewer_only() -> None:
    # ALBIS never writes back to the file it opened, and claiming Editor would
    # offer it for "Save" and as a drop target for edits it cannot perform.
    for entry in _spec_info_plist()["CFBundleDocumentTypes"]:
        assert entry["CFBundleTypeRole"] == "Viewer", entry["CFBundleTypeName"]


def test_macos_declares_only_the_types_nobody_else_owns() -> None:
    """Export what ALBIS invented; import what Apple and the HDF Group own.

    Exporting a type another application declares is how Launch Services ends
    up with two definitions of one identifier and picks the wrong handler.
    """
    plist = _spec_info_plist()
    exported = {item["UTTypeIdentifier"] for item in plist["UTExportedTypeDeclarations"]}
    imported = {item["UTTypeIdentifier"] for item in plist["UTImportedTypeDeclarations"]}
    assert exported == {
        "com.saschaandresgrimm.albis.cbf",
        "com.saschaandresgrimm.albis.edf",
    }
    assert "public.tiff" not in exported, "Apple owns public.tiff"
    assert "org.hdfgroup.hdf5" in imported


def test_macos_extension_tags_match_the_association_list() -> None:
    plist = _spec_info_plist()
    tagged: dict[str, set[str]] = {}
    for item in plist["UTExportedTypeDeclarations"] + plist["UTImportedTypeDeclarations"]:
        tags = item["UTTypeTagSpecification"]["public.filename-extension"]
        tagged[item["UTTypeIdentifier"]] = set(tags)

    for association in FILE_ASSOCIATIONS:
        bare = association.extension.lstrip(".")
        if association.uti == "public.tiff":
            continue  # Apple's declaration already covers tif/tiff.
        assert (
            bare in tagged[association.uti]
        ), f"{association.extension} is not tagged on {association.uti}"


# --------------------------------------------------------------------------
# The launcher end of it
# --------------------------------------------------------------------------


@pytest.mark.parametrize("flag", ["--help"])
def test_the_launcher_documents_the_positional_argument(flag: str) -> None:
    """A real subprocess, because argparse's own help is the contract here.

    Run as a module so this works from the source tree without a build.
    """
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import albis_launcher; albis_launcher._build_arg_parser().print_help()",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert "FILE" in result.stdout
    assert "open on start" in result.stdout
