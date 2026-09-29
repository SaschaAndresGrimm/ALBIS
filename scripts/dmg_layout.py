"""The one place the macOS DMG's window/icon geometry is written down.

`scripts/sign_macos.sh` and `scripts/build_mac.sh` both hand this geometry to
`scripts/vendor/create-dmg/create-dmg` as `--window-size`, `--icon-size`,
`--icon`, and `--app-drop-link` flags. `albis_assets/dmg_background.png` and
its `@2x` twin were drawn to match it -- the arrow's empty lane, and the clear space left for
Finder's own icon and label under each drop target, are only correct for
*this* window size and *these* icon positions.

Those are two independent things (a shell script's flags, a PNG's pixels) that
would silently drift apart the moment either changed on its own: the arrow
would stop pointing between the icons, or the background would be cropped
against a differently-sized window. So both read the numbers from here rather
than each hardcoding its own copy -- the packaging scripts via `--shell`
(mirroring how they already read `scripts/version_info.py --shell`), and
`scripts/generate_dmg_background.py` by importing this module directly.

`--shell` needs nothing beyond the standard library, because it runs on every
macOS packaging build, on a CI runner that does not have Pillow installed.
Pillow is only ever imported by `generate_dmg_background.py`, and only when
that dev-only art tool actually runs.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass

# Point-space (Finder window coordinates), not pixels. `create-dmg` and every
# AppleScript Finder property below are all in points.
WINDOW_POS = (200, 120)
WINDOW_SIZE = (660, 400)
ICON_SIZE = 128
APP_ICON_POS = (180, 170)
APPLICATIONS_ICON_POS = (480, 170)

# The high-resolution background is rendered at this multiple of the window's
# point size. Finder draws a background at one image pixel per point and does
# not scale it to fit, so a 2x image on its own shows only its top-left
# quarter -- which is what the first version of this did. The 2x image is
# therefore paired with a 1x one and the two combined into a
# multi-resolution TIFF (scripts/build_styled_dmg.sh), from which Finder picks
# the representation that matches the display.
BACKGROUND_SCALE = 2


@dataclass(frozen=True)
class DmgLayout:
    window_pos: tuple[int, int]
    window_size: tuple[int, int]
    icon_size: int
    app_icon_pos: tuple[int, int]
    applications_icon_pos: tuple[int, int]
    background_scale: int

    @property
    def background_pixel_size(self) -> tuple[int, int]:
        w, h = self.window_size
        return (w * self.background_scale, h * self.background_scale)

    def as_shell(self) -> str:
        lines = [
            f"DMG_WINDOW_POS_X={self.window_pos[0]}",
            f"DMG_WINDOW_POS_Y={self.window_pos[1]}",
            f"DMG_WINDOW_W={self.window_size[0]}",
            f"DMG_WINDOW_H={self.window_size[1]}",
            f"DMG_ICON_SIZE={self.icon_size}",
            f"DMG_APP_ICON_X={self.app_icon_pos[0]}",
            f"DMG_APP_ICON_Y={self.app_icon_pos[1]}",
            f"DMG_APPLICATIONS_ICON_X={self.applications_icon_pos[0]}",
            f"DMG_APPLICATIONS_ICON_Y={self.applications_icon_pos[1]}",
        ]
        return "\n".join(lines)


LAYOUT = DmgLayout(
    window_pos=WINDOW_POS,
    window_size=WINDOW_SIZE,
    icon_size=ICON_SIZE,
    app_icon_pos=APP_ICON_POS,
    applications_icon_pos=APPLICATIONS_ICON_POS,
    background_scale=BACKGROUND_SCALE,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--shell",
        action="store_true",
        help="Print DMG_* variables in a form a shell script can eval.",
    )
    args = parser.parse_args()
    if args.shell:
        print(LAYOUT.as_shell())
        return
    parser.print_help()


if __name__ == "__main__":
    main()
