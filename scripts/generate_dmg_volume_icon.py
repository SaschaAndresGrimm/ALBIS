"""Regenerate albis_assets/dmg_volume_icon.icns, the icon of the mounted DMG.

A dev-only art tool, like `generate_dmg_background.py`: the packaging scripts
use the committed .icns and nothing re-renders it at package time. Needs
Pillow (not a project dependency) and macOS's `iconutil`:

    .venv/bin/python scripts/generate_dmg_volume_icon.py

The icon is the ALBIS app icon on a silver drive slab, the way Apple draws
disk images: the drive says "this is a volume you eject", the app icon says
which one. The ALBIS icon alone would look the same as the app inside it.
Both are drawn from `albis_assets/icon_1024x1024.png`, so a new app icon only
needs this re-run.
"""

from __future__ import annotations

import pathlib
import subprocess
import tempfile

from PIL import Image, ImageChops, ImageDraw, ImageFilter

ASSETS = pathlib.Path(__file__).resolve().parents[1] / "albis_assets"
OUTPUT = ASSETS / "dmg_volume_icon.icns"
SIZE = 1024  # drawn once, scaled down for the smaller sizes


def rounded_mask(box: tuple[int, int, int, int], radius: int) -> Image.Image:
    mask = Image.new("L", (SIZE, SIZE), 0)
    ImageDraw.Draw(mask).rounded_rectangle(box, radius, fill=255)
    return mask


def vertical_gradient(top: tuple[int, int, int], bottom: tuple[int, int, int]) -> Image.Image:
    column = Image.new("RGB", (1, SIZE))
    for y in range(SIZE):
        t = y / (SIZE - 1)
        column.putpixel(
            (0, y), tuple(round(a + (b - a) * t) for a, b in zip(top, bottom, strict=True))
        )
    return column.resize((SIZE, SIZE))


def build() -> Image.Image:
    canvas = Image.new("RGBA", (SIZE, SIZE), (0, 0, 0, 0))

    # The slab: a wide, shallow rounded box, silver with a darker underside.
    box = (56, 300, 968, 900)
    body = rounded_mask(box, 96)
    shadow = Image.new("RGBA", (SIZE, SIZE), (0, 0, 0, 0))
    shadow.putalpha(body.filter(ImageFilter.GaussianBlur(28)).point(lambda v: v * 0.45))
    canvas.alpha_composite(shadow, (0, 22))
    slab = vertical_gradient((236, 240, 246), (150, 160, 176)).convert("RGBA")
    slab.putalpha(body)
    canvas.alpha_composite(slab)

    # A thin highlight along the top edge, and a recessed strip along the bottom.
    inner = rounded_mask((box[0] + 6, box[1] + 6, box[2] - 6, box[3] - 6), 90)
    rim = ImageChops.subtract(body, inner)
    highlight = Image.new("RGBA", (SIZE, SIZE), (255, 255, 255, 0))
    highlight.putalpha(rim.point(lambda v: v * 0.8))
    canvas.alpha_composite(highlight)
    draw = ImageDraw.Draw(canvas)
    draw.rounded_rectangle((150, 790, 874, 822), 16, fill=(110, 120, 138, 255))
    draw.ellipse((824, 773, 858, 807), fill=(86, 170, 255, 255))  # the activity light

    # The ALBIS icon, standing on the slab and overlapping its top edge.
    app = (
        Image.open(ASSETS / "icon_1024x1024.png").convert("RGBA").resize((600, 600), Image.LANCZOS)
    )
    app_shadow = Image.new("RGBA", (SIZE, SIZE), (0, 0, 0, 0))
    app_shadow.paste((0, 0, 0, 120), (212, 150), app.getchannel("A"))
    canvas.alpha_composite(app_shadow.filter(ImageFilter.GaussianBlur(18)), (0, 14))
    canvas.alpha_composite(app, (212, 150))
    return canvas


def main() -> None:
    master = build()
    with tempfile.TemporaryDirectory() as tmp:
        iconset = pathlib.Path(tmp) / "volume.iconset"
        iconset.mkdir()
        for base in (16, 32, 128, 256, 512):
            for scale in (1, 2):
                px = base * scale
                name = f"icon_{base}x{base}{'@2x' if scale == 2 else ''}.png"
                master.resize((px, px), Image.LANCZOS).save(iconset / name)
        subprocess.run(["iconutil", "-c", "icns", str(iconset), "-o", str(OUTPUT)], check=True)
        master.save(pathlib.Path(tmp) / "preview.png")
        master.save("/tmp/dmg_volume_icon_preview.png")
    print(f"Wrote {OUTPUT}")


if __name__ == "__main__":
    main()
