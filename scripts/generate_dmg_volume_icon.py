"""Regenerate albis_assets/dmg_volume_icon.icns, the icon of the mounted DMG.

A dev-only art tool, like `generate_dmg_background.py`: the packaging scripts
use the committed .icns and nothing re-renders it at package time. Needs
Pillow (not a project dependency) and macOS's `iconutil`:

    .venv/bin/python scripts/generate_dmg_volume_icon.py [--preview sheet.png]

The icon is the ALBIS app icon on a silver drive slab, the way Apple draws
disk images: the drive says "this is a volume you eject", the app icon says
which one. The ALBIS icon alone would look the same as the app inside it.

Two drawings, as Apple does for its own icons:

- From 64 px up, the app icon (from `albis_assets/icon_1024x1024.png`) stands
  on the slab. Everything stays inside the 824 px body of Apple's 1024 px
  icon grid, so the icon is as large as its neighbours on the desktop.
- At 16 and 32 px (the Finder sidebar, list views), the app icon's rounded
  square would shrink to a dark blob and the slab to a grey line, and the
  whole would read as the app. There the drive is drawn larger with a bare
  mountain on it, outlined so it holds on light and dark backgrounds.

`--preview` writes a sheet of every size on light and dark backgrounds, with
the small ones magnified, so a change can be checked by eye before
committing.
"""

from __future__ import annotations

import argparse
import pathlib
import subprocess
import tempfile

from PIL import Image, ImageDraw, ImageFilter

ASSETS = pathlib.Path(__file__).resolve().parents[1] / "albis_assets"
OUTPUT = ASSETS / "dmg_volume_icon.icns"
SIZE = 1024  # the large drawing, scaled down for 64 px and up
SMALL = 256  # the small drawing, scaled down to 16 and 32 px

RGB = tuple[int, int, int]
SILVER_TOP: RGB = (241, 244, 248)
SILVER_BOTTOM: RGB = (170, 179, 194)
EDGE = (52, 62, 82, 150)  # keeps the slab's outline on a light background
SLOT: RGB = (98, 108, 126)
LIGHT: RGB = (64, 156, 255)
NAVY: RGB = (10, 22, 58)


def gradient_fill(size: int, box: tuple[int, int, int, int], radius: int) -> Image.Image:
    """The slab: silver, darker towards the bottom, inside a rounded box."""
    x0, y0, x1, y1 = box
    column = Image.new("RGB", (1, y1 - y0))
    for y in range(y1 - y0):
        t = y / max(1, y1 - y0 - 1)
        column.putpixel(
            (0, y),
            tuple(round(a + (b - a) * t) for a, b in zip(SILVER_TOP, SILVER_BOTTOM, strict=True)),
        )
    layer = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    layer.paste(column.resize((x1 - x0, y1 - y0)), (x0, y0))
    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).rounded_rectangle(box, radius, fill=255)
    layer.putalpha(mask)
    return layer


def shadow_of(layer: Image.Image, blur: float, alpha: float, dy: int) -> Image.Image:
    shadow = Image.new("RGBA", layer.size, (0, 0, 0, 0))
    shadow.putalpha(layer.getchannel("A").point(lambda v: int(v * alpha)))
    moved = Image.new("RGBA", layer.size, (0, 0, 0, 0))
    moved.alpha_composite(shadow.filter(ImageFilter.GaussianBlur(blur)), (0, dy))
    return moved


def slab(size: int, box: tuple[int, int, int, int], radius: int, edge: int) -> Image.Image:
    """The drive body with its outline, top highlight and shadow."""
    x0, y0, x1, y1 = box
    body = gradient_fill(size, box, radius)
    layer = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    layer.alpha_composite(shadow_of(body, size * 0.022, 0.40, round(size * 0.018)))
    outline = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    ImageDraw.Draw(outline).rounded_rectangle(
        (x0 - edge, y0 - edge, x1 + edge, y1 + edge), radius + edge, fill=EDGE
    )
    layer.alpha_composite(outline)
    layer.alpha_composite(body)
    # A bright line just inside the top edge: light falling on the lid.
    sheen = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    ImageDraw.Draw(sheen).rounded_rectangle(
        (x0 + edge, y0 + edge, x1 - edge, y0 + radius),
        radius - edge,
        outline=(255, 255, 255, 200),
        width=max(1, edge),
    )
    clip = Image.new("L", (size, size), 0)
    ImageDraw.Draw(clip).rectangle((0, 0, size, y0 + radius // 2), fill=255)
    sheen.putalpha(Image.composite(sheen.getchannel("A"), Image.new("L", (size, size), 0), clip))
    layer.alpha_composite(sheen)
    return layer


def drive_details(
    draw: ImageDraw.ImageDraw, slot: tuple[int, int, int, int], light: tuple[int, int, int, int]
) -> None:
    """An engraved slot and, apart from it, the activity light."""
    x0, y0, x1, y1 = slot
    height = y1 - y0
    draw.rounded_rectangle(slot, height // 2, fill=SLOT + (255,))
    draw.rounded_rectangle(
        (x0, y1, x1, y1 + max(1, height // 3)), height // 2, fill=(255, 255, 255, 150)
    )
    draw.ellipse(light, fill=LIGHT + (255,))


def build_large() -> Image.Image:
    canvas = Image.new("RGBA", (SIZE, SIZE), (0, 0, 0, 0))
    canvas.alpha_composite(slab(SIZE, (100, 440, 924, 850), 76, 4))
    drive_details(ImageDraw.Draw(canvas), (180, 772, 700, 790), (782, 765, 814, 797))

    # The ALBIS icon, standing on the slab and overlapping its top edge.
    app = (
        Image.open(ASSETS / "icon_1024x1024.png")
        .convert("RGBA")
        .resize((500, 500), Image.Resampling.LANCZOS)
    )
    placed = Image.new("RGBA", (SIZE, SIZE), (0, 0, 0, 0))
    placed.alpha_composite(app, (262, 170))
    canvas.alpha_composite(shadow_of(placed, 16, 0.45, 12))
    canvas.alpha_composite(placed)
    return canvas


def build_small() -> Image.Image:
    canvas = Image.new("RGBA", (SMALL, SMALL), (0, 0, 0, 0))
    canvas.alpha_composite(slab(SMALL, (14, 126, 242, 234), 30, 6))
    draw = ImageDraw.Draw(canvas)
    drive_details(draw, (44, 204, 172, 214), (194, 199, 212, 217))

    # The ALBIS mountain without its rounded square: a high peak right of
    # centre and a lower one on its left, as in the app icon. A lone
    # symmetric triangle on a bar would read as the eject symbol. It stands
    # in the drive rather than on it, for the same reason. A navy outline,
    # then a lit left face, a shaded right face and snow on both peaks.
    outer = [(18, 178), (78, 66), (100, 94), (150, 14), (238, 178)]
    cx, cy = 132, 120

    def inset(point: tuple[int, int], k: float = 0.86) -> tuple[float, float]:
        return (cx + (point[0] - cx) * k, cy + (point[1] - cy) * k)

    foot_l, low, notch, high, foot_r = (inset(p) for p in outer)
    ridge_foot = inset((156, 178))
    draw.polygon(outer, fill=NAVY + (255,))
    draw.polygon((foot_l, low, notch, high, ridge_foot), fill=(56, 112, 204, 255))
    draw.polygon((high, foot_r, ridge_foot), fill=(30, 66, 146, 255))
    snow = (255, 255, 255, 255)
    hx, hy = high
    draw.polygon(
        ((hx, hy), (hx - 22, hy + 40), (hx - 8, hy + 32), (hx + 2, hy + 44), (hx + 12, hy + 26)),
        fill=snow,
    )
    lx, ly = low
    draw.polygon(((lx, ly), (lx - 12, ly + 24), (lx + 4, ly + 18), (lx + 12, ly + 18)), fill=snow)
    return canvas


def renders() -> dict[str, Image.Image]:
    large, small = build_large(), build_small()
    images = {}
    for base in (16, 32, 128, 256, 512):
        for scale in (1, 2):
            px = base * scale
            source = small if px <= 32 else large
            name = f"icon_{base}x{base}{'@2x' if scale == 2 else ''}.png"
            images[name] = source.resize((px, px), Image.Resampling.LANCZOS)
    return images


def preview(images: dict[str, Image.Image], path: pathlib.Path) -> None:
    shown = [
        "icon_16x16.png",
        "icon_32x32.png",
        "icon_32x32@2x.png",
        "icon_128x128.png",
        "icon_256x256.png",
    ]
    rows = []
    for background in ((236, 236, 236), (38, 38, 40)):
        row = Image.new("RGB", (1100, 300), background)
        x = 20
        for name in shown:
            image = images[name]
            row.paste(image, (x, 20), image)
            if image.width <= 32:  # magnified, to judge the pixels
                big = image.resize((image.width * 6, image.height * 6), Image.Resampling.NEAREST)
                row.paste(big, (x, 60), big)
                x += big.width + 30
            else:
                x += image.width + 30
        rows.append(row)
    sheet = Image.new("RGB", (1100, 600))
    sheet.paste(rows[0], (0, 0))
    sheet.paste(rows[1], (0, 300))
    sheet.save(path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Regenerate the mounted DMG's icon.")
    parser.add_argument("--preview", type=pathlib.Path, help="also write a sheet of every size")
    args = parser.parse_args()
    images = renders()
    with tempfile.TemporaryDirectory() as tmp:
        iconset = pathlib.Path(tmp) / "volume.iconset"
        iconset.mkdir()
        for name, image in images.items():
            image.save(iconset / name)
        subprocess.run(["iconutil", "-c", "icns", str(iconset), "-o", str(OUTPUT)], check=True)
    print(f"Wrote {OUTPUT}")
    if args.preview:
        preview(images, args.preview)
        print(f"Wrote {args.preview}")


if __name__ == "__main__":
    main()
