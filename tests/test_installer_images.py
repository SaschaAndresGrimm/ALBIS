"""The Windows installer's wizard artwork, and the Inno Setup version pin.

None of this can be checked by running it: Inno Setup only runs on Windows,
and CI runs the installer silently, so nothing ever looks at the wizard.
What can be checked is what went wrong with the macOS DMG background before
it: images whose sizes do not match what the tool actually draws. The sizes
below come from the Inno Setup 6.6+ documentation
(https://jrsoftware.org/ishelp/, WizardImageFile and WizardSmallImageFile);
6.6.0 changed them from the older 164x314 / 55x55.

No Pillow: PNG headers are read with struct, as in tests/test_dmg_styling.py.
"""

from __future__ import annotations

import re
import struct
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

# DPI percent -> the image area Inno Setup 6.6+ documents at that step.
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


def _iss_value(directive: str) -> str:
    match = re.search(rf"^{directive}=(.+)$", ISS.read_text(encoding="utf-8"), re.MULTILINE)
    assert match, f"installer_windows.iss does not set {directive}"
    return match.group(1).strip()


def _iss_glob(directive: str) -> list[Path]:
    """Resolve an .iss wildcard the way ISCC does: relative to the script."""
    pattern = PureWindowsPath(_iss_value(directive))
    base = ISS.parent.joinpath(*pattern.parent.parts).resolve()
    return sorted(base.glob(pattern.name))


# -- the committed images ---------------------------------------------------


@pytest.mark.parametrize(("percent", "size"), sorted(WIZARD_IMAGE_SIZES.items()))
def test_banner_matches_the_documented_area_at_each_dpi(
    percent: int, size: tuple[int, int]
) -> None:
    path = INSTALLER_DIR / f"wizard_image_{percent}.png"
    assert path.is_file(), f"{path.name} missing; run scripts/generate_installer_images.py"
    width, height, _ = _png_header(path)
    assert (width, height) == size


@pytest.mark.parametrize(("percent", "side"), sorted(WIZARD_SMALL_IMAGE_SIZES.items()))
def test_corner_image_matches_the_documented_area_at_each_dpi(percent: int, side: int) -> None:
    path = INSTALLER_DIR / f"wizard_small_image_{percent}.png"
    assert path.is_file(), f"{path.name} missing; run scripts/generate_installer_images.py"
    width, height, _ = _png_header(path)
    assert (width, height) == (side, side)


def test_corner_images_keep_their_transparency() -> None:
    # The icon's rounded corners are transparent, so it sits cleanly on the
    # wizard's top panel. An RGB export would fill them with a solid color.
    for side_percent in WIZARD_SMALL_IMAGE_SIZES:
        _, _, color_type = _png_header(INSTALLER_DIR / f"wizard_small_image_{side_percent}.png")
        assert color_type == PNG_RGBA, f"wizard_small_image_{side_percent}.png lost its alpha"


# -- installer_windows.iss uses exactly those images ------------------------


def test_iss_uses_the_modern_light_style() -> None:
    # `modern` with no appearance mode is light, which is what the art is
    # drawn for. `dark` or `dynamic` would put a light banner in a dark wizard.
    assert _iss_value("WizardStyle") == "modern"


def test_banner_wildcard_matches_exactly_one_file_per_dpi_step() -> None:
    matched = _iss_glob("WizardImageFile")
    assert [p.name for p in matched] == sorted(
        f"wizard_image_{percent}.png" for percent in WIZARD_IMAGE_SIZES
    )


def test_corner_wildcard_matches_exactly_one_file_per_dpi_step() -> None:
    # The two wildcards share a directory; this also pins that the banner's
    # pattern does not pick up the corner images, or the other way round.
    matched = _iss_glob("WizardSmallImageFile")
    assert [p.name for p in matched] == sorted(
        f"wizard_small_image_{percent}.png" for percent in WIZARD_SMALL_IMAGE_SIZES
    )


# -- the Inno Setup version pin ---------------------------------------------


def _pinned_version_in_script() -> str:
    match = re.search(
        r'\$pinnedInnoVersion = \[version\]"([0-9.]+)"',
        PACKAGE_SCRIPT.read_text(encoding="utf-8"),
    )
    assert match, "package_windows_innosetup.ps1 no longer declares $pinnedInnoVersion"
    return match.group(1)


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
        assert f"--version={_pinned_version_in_script()}" in line, line
        # The runner image's own copy may be newer than the pin.
        assert "--allow-downgrade" in line, line


def test_pinned_version_supports_the_wizard_styling() -> None:
    # WizardStyle's light mode and the image sizes above arrived in 6.6.0.
    pinned = tuple(int(part) for part in _pinned_version_in_script().split("."))
    assert pinned >= (6, 6, 0)
