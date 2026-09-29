"""The macOS DMG's background art, its shared geometry, and the vendored tool.

Three things have to agree, and nothing forces them to: the window/icon
geometry `scripts/build_styled_dmg.sh` hands to `create-dmg`, the pixel
dimensions of `albis_assets/dmg_background.png`, and the vendored copy of
`create-dmg` itself actually being the pinned, complete tool rather than a
partial or silently-edited one. `scripts/dmg_layout.py` is what keeps the
first two from drifting apart -- see its module docstring -- so this pins
that they still agree, without needing Pillow (not a project dependency; see
`scripts/generate_dmg_background.py`'s own docstring) or a macOS runner.
"""

from __future__ import annotations

import importlib.util
import struct
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
BACKGROUND_1X = REPO_ROOT / "albis_assets" / "dmg_background.png"
BACKGROUND_2X = REPO_ROOT / "albis_assets" / "dmg_background@2x.png"
BUILD_SCRIPT = REPO_ROOT / "scripts" / "build_styled_dmg.sh"
SIGN_SCRIPT = REPO_ROOT / "scripts" / "sign_macos.sh"
BUILD_MAC_SCRIPT = REPO_ROOT / "scripts" / "build_mac.sh"
VENDOR_DIR = REPO_ROOT / "scripts" / "vendor" / "create-dmg"


def _load_dmg_layout():
    spec = importlib.util.spec_from_file_location(
        "albis_dmg_layout", REPO_ROOT / "scripts" / "dmg_layout.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    # dmg_layout.py uses @dataclass, whose machinery resolves annotations via
    # sys.modules[cls.__module__] -- a module loaded by file path but never
    # registered in sys.modules fails that lookup with a bare AttributeError.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _png_dimensions(path: Path) -> tuple[int, int]:
    """Read width/height straight out of the IHDR chunk. No Pillow needed."""
    data = path.read_bytes()
    assert data[:8] == b"\x89PNG\r\n\x1a\n", f"{path} is not a PNG"
    # IHDR is always the first chunk: 8-byte signature, 4-byte length,
    # 4-byte "IHDR", then 4-byte width, 4-byte height, both big-endian.
    assert data[12:16] == b"IHDR", f"{path} has no IHDR as its first chunk"
    width, height = struct.unpack(">II", data[16:24])
    return width, height


# -- scripts/dmg_layout.py --------------------------------------------------


def test_shell_mode_needs_nothing_beyond_the_standard_library() -> None:
    """Every real macOS build calls `--shell`, on a runner with no Pillow.

    A stray top-level `import PIL` in dmg_layout.py would only be noticed the
    next time someone actually packages for macOS -- exactly the kind of
    build-time-only breakage this suite exists to catch before that.
    """
    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "dmg_layout.py"), "--shell"],
        capture_output=True,
        text=True,
        check=True,
        env={"PATH": "/usr/bin:/bin"},  # no site-packages on PATH resolution
    )
    assert "DMG_WINDOW_W=" in result.stdout
    assert "DMG_APPLICATIONS_ICON_X=" in result.stdout


def test_shell_output_matches_the_layout_object() -> None:
    layout = _load_dmg_layout()
    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "dmg_layout.py"), "--shell"],
        capture_output=True,
        text=True,
        check=True,
    )
    values = dict(line.split("=", 1) for line in result.stdout.splitlines())

    assert int(values["DMG_WINDOW_POS_X"]) == layout.LAYOUT.window_pos[0]
    assert int(values["DMG_WINDOW_POS_Y"]) == layout.LAYOUT.window_pos[1]
    assert int(values["DMG_WINDOW_W"]) == layout.LAYOUT.window_size[0]
    assert int(values["DMG_WINDOW_H"]) == layout.LAYOUT.window_size[1]
    assert int(values["DMG_ICON_SIZE"]) == layout.LAYOUT.icon_size
    assert int(values["DMG_APP_ICON_X"]) == layout.LAYOUT.app_icon_pos[0]
    assert int(values["DMG_APP_ICON_Y"]) == layout.LAYOUT.app_icon_pos[1]
    assert int(values["DMG_APPLICATIONS_ICON_X"]) == layout.LAYOUT.applications_icon_pos[0]
    assert int(values["DMG_APPLICATIONS_ICON_Y"]) == layout.LAYOUT.applications_icon_pos[1]


def test_icon_size_fits_within_creat_dmgs_own_ceiling() -> None:
    # create-dmg's --icon-size help text states "up to 128"; a layout change
    # that pushed past it would fail only on a macOS runner, minutes into a
    # release build.
    layout = _load_dmg_layout()
    assert 0 < layout.LAYOUT.icon_size <= 128


def test_the_two_icon_positions_stay_inside_the_window() -> None:
    # Not a defensive nicety: create-dmg does not validate this itself, and a
    # position outside the window bounds would silently place an icon where
    # the user never sees it.
    layout = _load_dmg_layout()
    w, h = layout.LAYOUT.window_size
    half_icon = layout.LAYOUT.icon_size / 2
    for x, y in (layout.LAYOUT.app_icon_pos, layout.LAYOUT.applications_icon_pos):
        assert half_icon <= x <= w - half_icon
        assert half_icon <= y <= h - half_icon


# -- albis_assets/dmg_background.png + @2x ----------------------------------


def test_background_pair_is_committed() -> None:
    for path in (BACKGROUND_1X, BACKGROUND_2X):
        assert path.is_file(), (
            f"{path.relative_to(REPO_ROOT)} is missing; regenerate the pair with "
            "scripts/generate_dmg_background.py"
        )


def test_standard_background_is_exactly_the_window_size() -> None:
    """Finder draws a background at one image pixel per window point.

    It does not scale a larger image down to fit. An earlier version of this
    test asserted the *single* committed PNG was twice the window size, on
    the belief that Finder would scale it -- which enshrined the bug rather
    than catching it: that DMG showed only the art's top-left quarter, the
    arrow pushed out of frame into the bottom-right corner.
    """
    layout = _load_dmg_layout()
    actual = _png_dimensions(BACKGROUND_1X)
    assert actual == layout.LAYOUT.window_size, (
        f"dmg_background.png is {actual[0]}x{actual[1]}, but Finder draws it at "
        f"one pixel per point in a {layout.LAYOUT.window_size[0]}x"
        f"{layout.LAYOUT.window_size[1]} window. Re-run "
        "scripts/generate_dmg_background.py after changing dmg_layout.py."
    )


def test_hidpi_background_is_exactly_twice_the_standard_one() -> None:
    # The Retina representation of the same art. tiffutil -cathidpicheck
    # refuses a pair that is not exactly 1:2, so a drift here would fail the
    # macOS build rather than look wrong -- but only on a macOS runner,
    # minutes in.
    layout = _load_dmg_layout()
    assert _png_dimensions(BACKGROUND_2X) == layout.LAYOUT.background_pixel_size
    w1, h1 = _png_dimensions(BACKGROUND_1X)
    assert _png_dimensions(BACKGROUND_2X) == (w1 * 2, h1 * 2)


# -- scripts/vendor/create-dmg -----------------------------------------------


def test_vendored_create_dmg_is_present_and_executable() -> None:
    script = VENDOR_DIR / "create-dmg"
    assert script.is_file()
    assert script.stat().st_mode & 0o111, "vendored create-dmg lost its executable bit"


def test_vendored_create_dmg_reports_the_pinned_version() -> None:
    # Guards against the file being silently replaced by a different version
    # without the pin in VENDORED.md being updated to match.
    result = subprocess.run(
        [str(VENDOR_DIR / "create-dmg"), "--version"],
        capture_output=True,
        text=True,
        check=True,
    )
    # "create-dmg 1.3.0" -> "1.3.0", to compare against VENDORED.md's own
    # "Pinned tag: `v1.3.0`" phrasing rather than requiring identical text.
    version = result.stdout.strip().rsplit(" ", 1)[-1]
    vendored_doc = (VENDOR_DIR / "VENDORED.md").read_text(encoding="utf-8")
    assert version in vendored_doc


def test_vendored_support_files_are_present() -> None:
    # The script resolves these relative to itself (via
    # .this-is-the-create-dmg-repo); a partial vendoring would only fail the
    # next time --background or --eula was actually used.
    assert (VENDOR_DIR / ".this-is-the-create-dmg-repo").is_file()
    assert (VENDOR_DIR / "support" / "template.applescript").is_file()
    assert (VENDOR_DIR / "LICENSE").is_file()


def test_vendored_license_is_mit() -> None:
    text = (VENDOR_DIR / "LICENSE").read_text(encoding="utf-8")
    assert "MIT License" in text
    assert "Andrey Tarantsov" in text or "Andrew Janke" in text


def test_create_dmg_is_not_listed_as_a_shipped_dependency() -> None:
    # It never ships inside ALBIS.app or the DMG's contents -- it only runs on
    # the packaging machine, the same category as PyInstaller and the
    # AppImage tool. THIRD_PARTY_LICENSES.md's own scope note says as much;
    # this pins that nobody "fixes" that by adding an entry for it.
    text = (REPO_ROOT / "THIRD_PARTY_LICENSES.md").read_text(encoding="utf-8")
    assert "create-dmg" not in text.lower()


# -- wiring: the packaging scripts actually use all of the above ------------


def test_build_styled_dmg_uses_the_vendored_tool_and_the_shared_layout() -> None:
    text = BUILD_SCRIPT.read_text(encoding="utf-8")
    assert 'scripts/vendor/create-dmg/create-dmg"' in text
    assert "scripts/dmg_layout.py" in text
    assert "${4:-$ROOT/albis_assets}" in text, "the committed pair must stay the default"
    assert "--app-drop-link" in text
    assert "--background" in text


def test_build_styled_dmg_hands_finder_a_multi_resolution_tiff() -> None:
    # Not a lone PNG: Finder would draw a 2x one at double size. Both
    # representations have to reach create-dmg together, combined.
    text = BUILD_SCRIPT.read_text(encoding="utf-8")
    assert "dmg_background.png" in text
    assert "dmg_background@2x.png" in text
    assert "tiffutil -cathidpicheck" in text
    assert '--background "$BACKGROUND"' in text
    assert 'BACKGROUND="$TEMP_DIR/dmg_background.tiff"' in text


def test_build_styled_dmg_retries_a_transient_applescript_timeout() -> None:
    # Observed directly while building this: the very first Finder-scripting
    # invocation in a session can time out ("AppleEvent timed out", -1712)
    # and then succeed cleanly on retry. create-dmg treats that as non-fatal
    # on its own (exit 0, an unstyled-but-valid DMG), so the retry has to key
    # off the log text rather than the exit code.
    text = BUILD_SCRIPT.read_text(encoding="utf-8")
    assert "Failed running AppleScript" in text
    assert "MAX_ATTEMPTS" in text


def test_a_still_unstyled_dmg_is_reported_loudly_not_silently() -> None:
    # The DMG this produces is still a valid installer either way -- nothing
    # here should fail a release over cosmetics -- but a quietly-degraded
    # release is exactly the failure mode this project has already shipped
    # once (v0.20.0's unstapled DMG) and does not want to repeat with a
    # missing background image.
    text = BUILD_SCRIPT.read_text(encoding="utf-8")
    assert "::warning::" in text


@pytest.mark.parametrize("script_path", [SIGN_SCRIPT, BUILD_MAC_SCRIPT])
def test_both_packaging_paths_call_the_shared_dmg_builder(script_path: Path) -> None:
    # scripts/sign_macos.sh (the release path) and scripts/build_mac.sh (the
    # unsigned dev path) must both produce the same window rather than one
    # drifting from the other, which is exactly what happened before this:
    # each had its own independent `hdiutil create -srcfolder` staging block.
    text = script_path.read_text(encoding="utf-8")
    assert "build_styled_dmg.sh" in text


def test_sign_macos_no_longer_hand_stages_the_applications_symlink() -> None:
    # That symlink now comes from create-dmg's --app-drop-link, added inside
    # build_styled_dmg.sh; a second one added here would collide with it.
    text = SIGN_SCRIPT.read_text(encoding="utf-8")
    assert 'ln -s "/Applications"' not in text
