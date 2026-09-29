"""Regenerate albis_assets/dmg_background.png and dmg_background@2x.png.

A dev-only art tool, not a build step: the packaging scripts consume the
committed PNGs this writes, the same way they consume `albis_assets/icon.icns`
-- nothing re-renders them at package time, so this is the only place that
needs Pillow, which is deliberately not a project dependency (see
`requirements-dev.txt`; `scripts/dmg_layout.py`'s `--shell` mode, which every
real build calls, imports nothing beyond the standard library for exactly
this reason).

Run it after changing the palette, the arrow, or the geometry in
`scripts/dmg_layout.py`:

    .venv/bin/pip install pillow   # once, into a throwaway or dev venv
    .venv/bin/python scripts/generate_dmg_background.py

Two files, because Finder draws a DMG background at one image pixel per
window point -- it does not scale a large bitmap down to fit. A single 2x PNG
therefore shows only its top-left quarter, which is exactly what the first
version of this did. `scripts/build_styled_dmg.sh` combines the pair into one
multi-resolution TIFF with `tiffutil -cathidpicheck`, and Finder picks the
representation that matches the display. Both are drawn from one 2x render,
so they cannot differ in anything but resolution.

The design carries no baked-in text. Finder draws the real "ALBIS.app" and
"Applications" labels under the real icons at the positions in
`scripts/dmg_layout.py`, so those columns are kept clear. That also means no
font: no licensing question, and no missing system font producing a
different result on a different machine.

Finder draws those labels in dark text on any custom background, in Dark Mode
too, so a background has to be light enough for dark text to read. That is
why "light" is the default theme; "dark" is kept for comparison.
"""

from __future__ import annotations

import argparse
import math
import pathlib
import sys
from dataclasses import dataclass

from PIL import Image, ImageDraw, ImageFilter

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from dmg_layout import LAYOUT  # noqa: E402

ASSETS_DIR = pathlib.Path(__file__).resolve().parents[1] / "albis_assets"
BASENAME = "dmg_background"

SCALE = LAYOUT.background_scale
W, H = LAYOUT.background_pixel_size
APP_X, APP_Y = (LAYOUT.app_icon_pos[0] * SCALE, LAYOUT.app_icon_pos[1] * SCALE)
LINK_X = LAYOUT.applications_icon_pos[0] * SCALE

RGB = tuple[int, int, int]


@dataclass(frozen=True)
class Theme:
    """Colors lifted from albis_assets/icon_1024x1024.png and frontend/style.css."""

    background_top: RGB
    background_bottom: RGB
    ridge_light: RGB
    ridge_shadow: RGB
    ridge_alpha: int
    arrow_under: RGB
    arrow_top: RGB
    arrow_head_upper: RGB
    arrow_head_middle: RGB
    arrow_head_lower: RGB
    arrow_edge: RGB
    arrow_glow: RGB | None
    arrow_shadow_alpha: int


THEMES = {
    # Cool off-white, so Finder's dark label text reads; the navy/blue of the
    # icon carries the brand in the arrow and the ridge instead.
    "light": Theme(
        background_top=(247, 250, 254),
        background_bottom=(226, 234, 245),
        ridge_light=(176, 199, 230),
        ridge_shadow=(142, 171, 214),
        ridge_alpha=120,
        arrow_under=(23, 49, 99),
        arrow_top=(27, 95, 155),  # --toolbar-blue
        arrow_head_upper=(78, 161, 255),  # --accent
        arrow_head_middle=(27, 95, 155),
        arrow_head_lower=(23, 49, 99),
        arrow_edge=(255, 255, 255),
        arrow_glow=None,
        arrow_shadow_alpha=60,
    ),
    # The original direction: the app icon's own navy. Handsome, but Finder's
    # labels are close to unreadable on it.
    "dark": Theme(
        background_top=(19, 28, 54),
        background_bottom=(7, 10, 20),
        ridge_light=(40, 78, 148),
        ridge_shadow=(22, 40, 82),
        ridge_alpha=90,
        arrow_under=(32, 66, 128),
        arrow_top=(78, 161, 255),
        arrow_head_upper=(140, 197, 255),
        arrow_head_middle=(78, 161, 255),
        arrow_head_lower=(32, 66, 128),
        arrow_edge=(224, 240, 255),
        arrow_glow=(78, 161, 255),
        arrow_shadow_alpha=0,
    ),
}


def vertical_gradient(top: RGB, bottom: RGB) -> Image.Image:
    column = Image.new("RGB", (1, H))
    for y in range(H):
        t = y / (H - 1)
        column.putpixel(
            (0, y), tuple(int(a + (b - a) * t) for a, b in zip(top, bottom, strict=True))
        )
    return column.resize((W, H)).convert("RGBA")


def radial_glow(color: RGB, center: tuple[float, float], radius: float, peak: int) -> Image.Image:
    layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    box = int(radius * 2)
    mask = Image.new("L", (box, box), 0)
    for yy in range(box):
        for xx in range(box):
            d = math.hypot(xx - radius, yy - radius) / radius
            if d <= 1:
                mask.putpixel((xx, yy), int(peak * (1 - d) ** 1.8))
    solid = Image.new("RGBA", (box, box), (*color, 255))
    solid.putalpha(mask)
    layer.paste(solid, (int(center[0] - radius), int(center[1] - radius)), solid)
    return layer


def faceted_ridge(theme: Theme) -> Image.Image:
    """A low-poly mountain ridge along the bottom, faceted like the icon's peak.

    Each peak is two triangles, a lit left face and a shadowed right face.
    Peaks stay below the label line and are lowest in the middle, well clear
    of the arrow.
    """
    layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    # (x of peak, height above the bottom edge), in points.
    peaks = [
        (-20, 70),
        (70, 110),
        (160, 62),
        (250, 44),
        (330, 34),
        (410, 46),
        (500, 66),
        (590, 104),
        (680, 72),
    ]
    base = H + 2
    for i, (px, ph) in enumerate(peaks):
        left = peaks[i - 1][0] if i > 0 else px - 110
        right = peaks[i + 1][0] if i + 1 < len(peaks) else px + 110
        apex = (px * SCALE, base - ph * SCALE)
        d.polygon(
            [(left * SCALE, base), apex, (px * SCALE, base)],
            fill=(*theme.ridge_light, theme.ridge_alpha),
        )
        d.polygon(
            [(px * SCALE, base), apex, (right * SCALE, base)],
            fill=(*theme.ridge_shadow, theme.ridge_alpha),
        )
    return layer.filter(ImageFilter.GaussianBlur(1))


def faceted_arrow(theme: Theme, cy: float, x0: float, x1: float) -> Image.Image:
    """Three facets and a bright top edge, matching the icon's cut-crystal peak."""
    shaft_h = 22 * SCALE
    head_w = 62 * SCALE
    head_h = 58 * SCALE

    layer = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    d.polygon(
        [(x0, cy), (x1, cy), (x1, cy + shaft_h / 2), (x0, cy + shaft_h / 2)],
        fill=(*theme.arrow_under, 255),
    )
    d.polygon(
        [(x0, cy - shaft_h / 2), (x1, cy - shaft_h / 2), (x1, cy), (x0, cy)],
        fill=(*theme.arrow_top, 255),
    )
    d.polygon(
        [(x1, cy - head_h / 2), (x1 + head_w, cy), (x1, cy)], fill=(*theme.arrow_head_upper, 255)
    )
    d.polygon(
        [(x1, cy), (x1 + head_w, cy), (x1, cy + head_h / 2)], fill=(*theme.arrow_head_lower, 255)
    )
    d.polygon(
        [(x1, cy - head_h / 2), (x1 + head_w * 0.55, cy - head_h * 0.06), (x1, cy)],
        fill=(*theme.arrow_head_middle, 255),
    )
    d.line(
        [(x0, cy - shaft_h / 2 + 1), (x1, cy - shaft_h / 2 + 1)],
        fill=(*theme.arrow_edge, 170),
        width=3,
    )
    d.line([(x1, cy - head_h / 2), (x1 + head_w, cy)], fill=(*theme.arrow_edge, 190), width=4)
    return layer


def drop_shadow(layer: Image.Image, alpha: int) -> Image.Image:
    shadow = Image.new("RGBA", (W, H), (10, 20, 45, 0))
    shadow.putalpha(layer.getchannel("A").point(lambda a: a * alpha // 255))
    shadow = shadow.filter(ImageFilter.GaussianBlur(6 * SCALE))
    offset = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    offset.paste(shadow, (0, 3 * SCALE))
    return offset


def build(theme: Theme) -> Image.Image:
    base = vertical_gradient(theme.background_top, theme.background_bottom)
    base = Image.alpha_composite(base, faceted_ridge(theme))

    cy = APP_Y
    x0 = APP_X + 100 * SCALE
    x1 = LINK_X - 128 * SCALE
    if theme.arrow_glow is not None:
        base = Image.alpha_composite(
            base, radial_glow(theme.arrow_glow, ((x0 + x1) / 2 + 25 * SCALE, cy), 150 * SCALE, 110)
        )
    arrow = faceted_arrow(theme, cy, x0, x1)
    if theme.arrow_shadow_alpha:
        base = Image.alpha_composite(base, drop_shadow(arrow, theme.arrow_shadow_alpha))
    base = Image.alpha_composite(base, arrow)
    return base.convert("RGB")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--theme", choices=sorted(THEMES), default="light")
    parser.add_argument(
        "--output-dir",
        type=pathlib.Path,
        default=ASSETS_DIR,
        help="Where to write the pair (default: albis_assets/, the committed assets).",
    )
    args = parser.parse_args()

    hidpi = build(THEMES[args.theme])
    standard = hidpi.resize(LAYOUT.window_size, Image.LANCZOS)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    one_x = args.output_dir / f"{BASENAME}.png"
    two_x = args.output_dir / f"{BASENAME}@2x.png"
    # 72 and 144 dpi so tiffutil and Finder read them as the same point size.
    standard.save(one_x, dpi=(72, 72))
    hidpi.save(two_x, dpi=(144, 144))
    print(f"Wrote {one_x} ({standard.width}x{standard.height})")
    print(f"Wrote {two_x} ({hidpi.width}x{hidpi.height})")


if __name__ == "__main__":
    main()
