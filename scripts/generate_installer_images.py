"""Regenerate the Windows installer's wizard images in albis_assets/installer/.

A dev-only art tool, like scripts/generate_dmg_background.py: it writes
committed PNGs that scripts/installer_windows.iss points at, and nothing
renders them at build time. It needs Pillow, which is not a project
dependency.

    .venv/bin/pip install pillow   # once, into a throwaway or dev venv
    .venv/bin/python scripts/generate_installer_images.py

Two images, each at every size Inno Setup lists for its DPI steps:

- WizardImageFile, the tall banner on the Welcome and Finished pages.
- WizardSmallImageFile, the square in the top-right corner of every other
  page, including the progress window the in-app "Install Update" shows
  when it runs the installer with /SILENT.

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


def ridge(size: tuple[int, int]) -> Image.Image:
    """The DMG's faceted ridge, proportioned for a tall, narrow banner."""
    w, h = size
    layer = Image.new("RGBA", size, (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    # (x as a fraction of width, peak height as a fraction of height)
    peaks = [(-0.15, 0.13), (0.18, 0.24), (0.50, 0.15), (0.80, 0.22), (1.15, 0.12)]
    base = h + 2
    for i, (px, ph) in enumerate(peaks):
        left = peaks[i - 1][0] if i > 0 else px - 0.35
        right = peaks[i + 1][0] if i + 1 < len(peaks) else px + 0.35
        apex = (px * w, base - ph * h)
        d.polygon(
            [(left * w, base), apex, (px * w, base)],
            fill=(*THEME.ridge_light, THEME.ridge_alpha),
        )
        d.polygon(
            [(px * w, base), apex, (right * w, base)],
            fill=(*THEME.ridge_shadow, THEME.ridge_alpha),
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
    w, h = size[0] * SUPERSAMPLE, size[1] * SUPERSAMPLE
    canvas = gradient((w, h))
    canvas.alpha_composite(ridge((w, h)))
    side = int(w * 0.62)
    with_shadow(canvas, icon(side), ((w - side) // 2, int(h * 0.36) - side // 2), w * 0.035)
    return canvas.resize(size, Image.LANCZOS).convert("RGB")


def small_image(side: int) -> Image.Image:
    # Transparent around the icon's rounded corners, so it sits cleanly on
    # the wizard's top panel whatever its color.
    return icon(side * SUPERSAMPLE).resize((side, side), Image.LANCZOS)


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


if __name__ == "__main__":
    main()
