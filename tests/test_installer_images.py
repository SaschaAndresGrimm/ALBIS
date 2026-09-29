"""The Windows installer's wizard artwork, and the Inno Setup version pin.

None of this can be checked by running it: Inno Setup only runs on Windows,
and CI runs the installer silently, so nothing ever looks at the wizard.
What can be checked is what has already gone wrong twice:

- The macOS DMG background had the wrong size for the area it is drawn in.
  The sizes below come from the Inno Setup documentation
  (https://jrsoftware.org/ishelp/, WizardImageFile, WizardSmallImageFile,
  WizardBackImageFile); 6.6.0 changed the first two from 164x314 / 55x55.
- The first corner icon here was drawn edge to edge. Inno Setup's area for
  it is flush with the window frame, so on Windows it sat against the frame.

No Pillow: PNG headers are read with struct and pixels decoded with zlib, as
tests/test_dmg_styling.py does for the DMG background.
"""

from __future__ import annotations

import re
import struct
import zlib
from pathlib import Path, PureWindowsPath

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
INSTALLER_DIR = REPO_ROOT / "albis_assets" / "installer"
ISS = REPO_ROOT / "scripts" / "installer_windows.iss"
PACKAGE_SCRIPT = REPO_ROOT / "scripts" / "package_windows_innosetup.ps1"
WORKFLOWS = [
    REPO_ROOT / ".github" / "workflows" / "release.yml",
    REPO_ROOT / ".github" / "workflows" / "artifacts.yml",
]

# DPI percent -> the image area Inno Setup documents at that step.
WIZARD_IMAGE_SIZES = {
    100: (202, 386),
    125: (269, 515),
    150: (336, 643),
    175: (403, 772),
    200: (430, 824),
    225: (498, 953),
    250: (534, 1022),
}
WIZARD_SMALL_IMAGE_SIZES = {
    100: 58,
    125: 77,
    150: 97,
    175: 116,
    200: 124,
    225: 143,
    250: 159,
}
# The whole wizard client area, aspect 497:360 (Inno Setup 6.7.0+).
WIZARD_BACK_IMAGE_SIZES = {
    100: (596, 432),
    125: (796, 576),
    150: (994, 720),
    175: (1193, 864),
    200: (1272, 922),
    225: (1471, 1066),
    250: (1630, 1148),
}

# PNG IHDR color types.
PNG_RGB = 2
PNG_RGBA = 6


def _png_header(path: Path) -> tuple[int, int, int]:
    """(width, height, color type), read from the IHDR chunk."""
    data = path.read_bytes()[:26]
    assert data[:8] == b"\x89PNG\r\n\x1a\n", f"{path.name} is not a PNG"
    assert data[12:16] == b"IHDR", f"{path.name} has no IHDR as its first chunk"
    width, height = struct.unpack(">II", data[16:24])
    return width, height, data[25]


def _png_alpha(path: Path) -> list[list[int]]:
    """The alpha channel of an 8-bit, non-interlaced RGBA PNG, row by row.

    Enough of a decoder for what the tests below ask: inflate the IDAT data
    and undo the five per-row filters from the PNG specification.
    """
    data = path.read_bytes()
    pos, idat = 8, b""
    width = height = 0
    while pos < len(data):
        (length,) = struct.unpack(">I", data[pos : pos + 4])
        kind = data[pos + 4 : pos + 8]
        body = data[pos + 8 : pos + 8 + length]
        if kind == b"IHDR":
            width, height = struct.unpack(">II", body[:8])
            bit_depth, color_type, _, _, interlace = body[8:13]
            assert (bit_depth, color_type, interlace) == (8, PNG_RGBA, 0), path.name
        elif kind == b"IDAT":
            idat += body
        pos += 12 + length
    raw = zlib.decompress(idat)
    bpp, stride = 4, width * 4
    rows: list[bytearray] = []
    prev = bytearray(stride)
    for y in range(height):
        start = y * (stride + 1)
        kind, line = raw[start], bytearray(raw[start + 1 : start + 1 + stride])
        for i in range(stride):
            left = line[i - bpp] if i >= bpp else 0
            up = prev[i]
            upper_left = prev[i - bpp] if i >= bpp else 0
            if kind == 1:
                line[i] = (line[i] + left) & 0xFF
            elif kind == 2:
                line[i] = (line[i] + up) & 0xFF
            elif kind == 3:
                line[i] = (line[i] + (left + up) // 2) & 0xFF
            elif kind == 4:
                p = left + up - upper_left
                pa, pb, pc = abs(p - left), abs(p - up), abs(p - upper_left)
                pred = left if pa <= pb and pa <= pc else (up if pb <= pc else upper_left)
                line[i] = (line[i] + pred) & 0xFF
        rows.append(line)
        prev = line
    return [list(row[3::4]) for row in rows]


def _iss_value(directive: str) -> str:
    match = re.search(rf"^{directive}=(.+)$", ISS.read_text(encoding="utf-8"), re.MULTILINE)
    assert match, f"installer_windows.iss does not set {directive}"
    return match.group(1).strip()


def _iss_glob(directive: str) -> list[Path]:
    """Resolve an .iss wildcard the way ISCC does: relative to the script."""
    pattern = PureWindowsPath(_iss_value(directive))
    base = ISS.parent.joinpath(*pattern.parent.parts).resolve()
    return sorted(base.glob(pattern.name))


# -- the committed images match the documented areas ------------------------


@pytest.mark.parametrize(("percent", "size"), sorted(WIZARD_IMAGE_SIZES.items()))
def test_banner_matches_the_documented_area_at_each_dpi(
    percent: int, size: tuple[int, int]
) -> None:
    path = INSTALLER_DIR / f"wizard_image_{percent}.png"
    assert path.is_file(), f"{path.name} missing; run scripts/generate_installer_images.py"
    assert _png_header(path)[:2] == size


@pytest.mark.parametrize(("percent", "side"), sorted(WIZARD_SMALL_IMAGE_SIZES.items()))
def test_corner_image_matches_the_documented_area_at_each_dpi(percent: int, side: int) -> None:
    path = INSTALLER_DIR / f"wizard_small_image_{percent}.png"
    assert path.is_file(), f"{path.name} missing; run scripts/generate_installer_images.py"
    assert _png_header(path)[:2] == (side, side)


@pytest.mark.parametrize(("percent", "size"), sorted(WIZARD_BACK_IMAGE_SIZES.items()))
def test_background_matches_the_documented_area_at_each_dpi(
    percent: int, size: tuple[int, int]
) -> None:
    path = INSTALLER_DIR / f"wizard_back_image_{percent}.png"
    assert path.is_file(), f"{path.name} missing; run scripts/generate_installer_images.py"
    assert _png_header(path)[:2] == size


# -- how each image has to be built to sit on the background ----------------


@pytest.mark.parametrize("percent", sorted(WIZARD_SMALL_IMAGE_SIZES))
def test_corner_icon_keeps_clear_of_the_window_frame(percent: int) -> None:
    """The regression seen on Windows: an icon drawn edge to edge.

    Inno Setup's area for this image is flush with the window's top and
    right edges, so the margin has to be inside the image. Its outermost
    rows and columns must be fully transparent.
    """
    alpha = _png_alpha(INSTALLER_DIR / f"wizard_small_image_{percent}.png")
    side = len(alpha)
    margin = max(1, side // 12)
    for y in range(side):
        for x in range(side):
            if y < margin or y >= side - margin or x < margin or x >= side - margin:
                assert alpha[y][x] == 0, f"wizard_small_image_{percent}.png: opaque at {x},{y}"
    assert max(max(row) for row in alpha) == 255, "the icon itself should be opaque"


@pytest.mark.parametrize("percent", sorted(WIZARD_IMAGE_SIZES))
def test_banner_is_transparent_so_the_background_shows_through(percent: int) -> None:
    """An opaque banner ends in a hard edge above the button row.

    The background runs under the whole window, so a banner with its own
    gradient and ridge stops at the top of the button row and cuts its ridge
    off in a line. That is how the first attempt looked in a preview built
    from a Windows screenshot.
    """
    alpha = _png_alpha(INSTALLER_DIR / f"wizard_image_{percent}.png")
    height, width = len(alpha), len(alpha[0])
    for x, y in ((0, 0), (width - 1, 0), (0, height - 1), (width - 1, height - 1)):
        assert alpha[y][x] == 0, f"wizard_image_{percent}.png is opaque at {x},{y}"
    # The bottom row especially: that is where the old banner's seam was.
    assert max(alpha[-1]) == 0


@pytest.mark.parametrize("percent", sorted(WIZARD_BACK_IMAGE_SIZES))
def test_background_is_opaque(percent: int) -> None:
    # A transparent background would let the plain window color through
    # wherever it is clear, which is exactly what it exists to replace.
    color_type = _png_header(INSTALLER_DIR / f"wizard_back_image_{percent}.png")[2]
    assert color_type == PNG_RGB


# -- installer_windows.iss uses exactly those images ------------------------


def test_iss_spells_out_the_light_background_image_style() -> None:
    # The same set Inno Setup picks by itself for a background image, written
    # down so it is visible: no appearance mode, so it stays light, which is
    # what the art is drawn for. `dark` or `dynamic` would put this art in a
    # dark wizard.
    style = _iss_value("WizardStyle").split()
    assert style == ["modern", "windows11", "excludelightcontrols", "hidebevels"]


@pytest.mark.parametrize(
    ("directive", "prefix", "sizes"),
    [
        ("WizardImageFile", "wizard_image_", WIZARD_IMAGE_SIZES),
        ("WizardSmallImageFile", "wizard_small_image_", WIZARD_SMALL_IMAGE_SIZES),
        ("WizardBackImageFile", "wizard_back_image_", WIZARD_BACK_IMAGE_SIZES),
    ],
)
def test_each_wildcard_matches_exactly_its_own_files(
    directive: str, prefix: str, sizes: dict[int, object]
) -> None:
    # All three share a directory; this also pins that no pattern picks up
    # another's files.
    matched = _iss_glob(directive)
    assert [p.name for p in matched] == sorted(f"{prefix}{percent}.png" for percent in sizes)


# -- the Inno Setup version pin ---------------------------------------------


def _script_version(variable: str) -> str:
    match = re.search(
        rf'\${variable} = \[version\]"([0-9.]+)"',
        PACKAGE_SCRIPT.read_text(encoding="utf-8"),
    )
    assert match, f"package_windows_innosetup.ps1 no longer declares ${variable}"
    return match.group(1)


def _as_tuple(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


@pytest.mark.parametrize("workflow", WORKFLOWS, ids=lambda p: p.name)
def test_workflows_install_the_version_the_script_requires(workflow: Path) -> None:
    """A pin in one place and a different version in another fails CI outright.

    The script refuses any other version on a GitHub runner, so a bump that
    misses one of these three files would stop the Windows build -- better
    to catch it here.
    """
    text = workflow.read_text(encoding="utf-8")
    installs = re.findall(r"choco install innosetup[^\n]*", text)
    assert installs, f"{workflow.name} no longer installs Inno Setup"
    for line in installs:
        assert f"--version={_script_version('pinnedInnoVersion')}" in line, line
        # The runner image's own copy may be newer than the pin.
        assert "--allow-downgrade" in line, line


def test_version_floor_covers_every_directive_the_iss_uses() -> None:
    # WizardBackImageFile arrived in 6.7.0; the styles and image sizes in 6.6.0.
    # A floor below that lets a local build reach ISCC and fail on an unknown
    # directive rather than on a clear version message.
    assert _as_tuple(_script_version("minimumInnoVersion")) >= (6, 7, 0)
    assert _as_tuple(_script_version("pinnedInnoVersion")) >= _as_tuple(
        _script_version("minimumInnoVersion")
    )
