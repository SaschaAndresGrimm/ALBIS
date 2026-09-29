"""Regenerate the Windows installer's wizard images in albis_assets/installer/.

A dev-only art tool, like scripts/generate_dmg_background.py: it writes
committed PNGs that scripts/installer_windows.iss points at, and nothing
renders them at build time. It needs Pillow, which is not a project
dependency.

    .venv/bin/pip install pillow   # once, into a throwaway or dev venv
    .venv/bin/python scripts/generate_installer_images.py

Three images, each at every size Inno Setup lists for its DPI steps:

- WizardImageFile, the tall banner on the Welcome and Finished pages. It is
  only the icon, on a transparent background.
- WizardSmallImageFile, the square in the top-right corner of every other
  page, including the progress window the in-app "Install Update" shows
  when it runs the installer with /SILENT.
- WizardBackImageFile (Inno Setup 6.7.0+), behind every page, the button row
  included: the gradient and the faceted ridge. The ridge stays low enough
  to sit under the inner pages' last line of text.

The banner is transparent because the background runs under the whole
window. A banner with its own gradient and ridge ends at the top of the
button row, which cuts its ridge off in a hard line above the background's.
That showed up in a preview built from a real Windows screenshot. With the
banner reduced to the icon, the Welcome and Finished pages become one
continuous canvas, the same composition as the macOS DMG window.

The corner image carries its own margin. Inno Setup's area for it is
full-bleed, flush with the window's top and right edges, so an icon drawn
edge to edge sits against the frame -- which is how the first version
looked when run on Windows.

The sizes are the ones Inno Setup 6.6.0 and later document for the default
font and wizard size (https://jrsoftware.org/ishelp/, WizardImageFile and
WizardSmallImageFile). 6.6.0 changed them: at 100% the banner area grew from
164x314 to 202x386 and the corner square to 58x58. Setup picks whichever
file best matches the area on the user's display, so shipping every step
keeps the art sharp instead of stretched. tests/test_installer_images.py
holds the same table and checks the committed files against it.

The palette is the DMG's light theme, imported from
generate_dmg_background.py, so the two installers read as one product.
"""

from __future__ import annotations

import pathlib
import sys

from PIL import Image, ImageDraw, ImageFilter

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from generate_dmg_background import THEMES  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "albis_assets" / "installer"
ICON_PATH = ROOT / "albis_assets" / "icon_1024x1024.png"

# DPI percent -> pixel size, from the Inno Setup 6.6+ documentation.
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
# The whole wizard client area, aspect 497:360, from the Inno Setup 6.7+
# documentation for WizardBackImageFile.
WIZARD_BACK_IMAGE_SIZES = {
    100: (596, 432),
    125: (796, 576),
    150: (994, 720),
    175: (1193, 864),
    200: (1272, 922),
    225: (1471, 1066),
    250: (1630, 1148),
}

# The corner icon's share of its square; the rest is a transparent margin.
SMALL_IMAGE_ICON_FRACTION = 0.70

# (x as a fraction of width, peak height as a fraction of height)
# Tall on the left, under the icon on the Welcome and Finished pages, where
# the first version's banner had its ridge; then tapering off to the right,
# under the page text and the buttons. On the inner pages the left peaks run
# under the destination page's disk-space line, which stays readable: dark
# text on pale blue.
BACK_PEAKS = [
    (-0.05, 0.09),
    (0.06, 0.17),
    (0.17, 0.11),
    (0.27, 0.16),
    (0.39, 0.085),
    (0.52, 0.06),
    (0.66, 0.045),
    (0.82, 0.08),
    (1.03, 0.055),
]
# A little fainter than the macOS DMG window's ridge (120), because on the
# inner pages it runs under text.
BACK_RIDGE_ALPHA = 110

# Drawn at this multiple of the target and then downsampled, because
# Pillow's polygons are not anti-aliased and these images are small.
SUPERSAMPLE = 4

THEME = THEMES["light"]


def gradient(size: tuple[int, int]) -> Image.Image:
    w, h = size
    column = Image.new("RGB", (1, h))
    for y in range(h):
        t = y / max(1, h - 1)
        column.putpixel(
            (0, y),
            tuple(
                int(a + (b - a) * t)
                for a, b in zip(THEME.background_top, THEME.background_bottom, strict=True)
            ),
        )
    return column.resize(size).convert("RGBA")


def ridge(size: tuple[int, int], peaks: list[tuple[float, float]], alpha: int) -> Image.Image:
    """The DMG's faceted ridge, with each peak a lit and a shadowed face."""
    w, h = size
    layer = Image.new("RGBA", size, (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    base = h + 2
    for i, (px, ph) in enumerate(peaks):
        left = peaks[i - 1][0] if i > 0 else px - 0.35
        right = peaks[i + 1][0] if i + 1 < len(peaks) else px + 0.35
        apex = (px * w, base - ph * h)
        d.polygon(
            [(left * w, base), apex, (px * w, base)],
            fill=(*THEME.ridge_light, alpha),
        )
        d.polygon(
            [(px * w, base), apex, (right * w, base)],
            fill=(*THEME.ridge_shadow, alpha),
        )
    return layer


def icon(side: int) -> Image.Image:
    return Image.open(ICON_PATH).convert("RGBA").resize((side, side), Image.LANCZOS)


def with_shadow(canvas: Image.Image, art: Image.Image, pos: tuple[int, int], blur: float) -> None:
    """Paste `art` onto `canvas` with a soft drop shadow beneath it."""
    shadow = Image.new("RGBA", canvas.size, (10, 20, 45, 0))
    mask = Image.new("L", canvas.size, 0)
    mask.paste(art.getchannel("A").point(lambda a: a * 70 // 255), (pos[0], pos[1] + int(blur)))
    shadow.putalpha(mask.filter(ImageFilter.GaussianBlur(blur)))
    canvas.alpha_composite(shadow)
    canvas.alpha_composite(art, pos)


def banner(size: tuple[int, int]) -> Image.Image:
    """The icon and its shadow, on transparency: the background supplies the rest."""
    w, h = size[0] * SUPERSAMPLE, size[1] * SUPERSAMPLE
    canvas = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    side = int(w * 0.62)
    with_shadow(canvas, icon(side), ((w - side) // 2, int(h * 0.36) - side // 2), w * 0.035)
    return canvas.resize(size, Image.LANCZOS)


def small_image(side: int) -> Image.Image:
    """The icon centred in a transparent margin.

    Transparent around the icon, so it sits on the background image behind
    it; the margin keeps it off the window frame.
    """
    canvas_side = side * SUPERSAMPLE
    canvas = Image.new("RGBA", (canvas_side, canvas_side), (0, 0, 0, 0))
    icon_side = round(canvas_side * SMALL_IMAGE_ICON_FRACTION)
    offset = (canvas_side - icon_side) // 2
    canvas.alpha_composite(icon(icon_side), (offset, offset))
    return canvas.resize((side, side), Image.LANCZOS)


def back_image(size: tuple[int, int]) -> Image.Image:
    w, h = size[0] * SUPERSAMPLE, size[1] * SUPERSAMPLE
    canvas = gradient((w, h))
    canvas.alpha_composite(ridge((w, h), BACK_PEAKS, BACK_RIDGE_ALPHA))
    return canvas.resize(size, Image.LANCZOS).convert("RGB")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for stale in OUTPUT_DIR.glob("wizard_*.png"):
        stale.unlink()
    for percent, size in WIZARD_IMAGE_SIZES.items():
        path = OUTPUT_DIR / f"wizard_image_{percent}.png"
        banner(size).save(path, optimize=True)
        print(f"Wrote {path.relative_to(ROOT)} ({size[0]}x{size[1]})")
    for percent, side in WIZARD_SMALL_IMAGE_SIZES.items():
        path = OUTPUT_DIR / f"wizard_small_image_{percent}.png"
        small_image(side).save(path, optimize=True)
        print(f"Wrote {path.relative_to(ROOT)} ({side}x{side})")
    for percent, size in WIZARD_BACK_IMAGE_SIZES.items():
        path = OUTPUT_DIR / f"wizard_back_image_{percent}.png"
        back_image(size).save(path, optimize=True)
        print(f"Wrote {path.relative_to(ROOT)} ({size[0]}x{size[1]})")


if __name__ == "__main__":
    main()
