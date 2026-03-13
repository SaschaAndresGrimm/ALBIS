#!/usr/bin/env python3
from __future__ import annotations

import argparse
import struct
import sys
from pathlib import Path

PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
DEFAULT_ICON_NAMES = (
    "icon_16x16.png",
    "icon_32x32.png",
    "icon_64x64.png",
    "icon_128x128.png",
    "icon_256x256.png",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a Windows .ico from ALBIS PNG icon assets."
    )
    parser.add_argument("--output", required=True, help="Output .ico path")
    parser.add_argument(
        "--source-dir",
        default="albis_assets",
        help="Directory containing icon_<size>x<size>.png assets",
    )
    parser.add_argument("inputs", nargs="*", help="Optional explicit PNG inputs")
    return parser.parse_args()


def read_png(path: Path) -> tuple[bytes, int, int]:
    data = path.read_bytes()
    if not data.startswith(PNG_SIGNATURE):
        raise ValueError(f"{path} is not a PNG file")
    width = int.from_bytes(data[16:20], "big")
    height = int.from_bytes(data[20:24], "big")
    return data, width, height


def collect_inputs(source_dir: Path, explicit_inputs: list[str]) -> list[Path]:
    if explicit_inputs:
        return [Path(item) for item in explicit_inputs]
    return [source_dir / name for name in DEFAULT_ICON_NAMES]


def build_ico(entries: list[tuple[bytes, int, int]]) -> bytes:
    header = struct.pack("<HHH", 0, 1, len(entries))
    directory = bytearray()
    payload = bytearray()
    offset = 6 + (16 * len(entries))

    for png_bytes, width, height in entries:
        directory.extend(
            struct.pack(
                "<BBBBHHII",
                0 if width >= 256 else width,
                0 if height >= 256 else height,
                0,
                0,
                1,
                32,
                len(png_bytes),
                offset,
            )
        )
        payload.extend(png_bytes)
        offset += len(png_bytes)

    return header + directory + payload


def main() -> int:
    args = parse_args()
    source_dir = Path(args.source_dir)
    output_path = Path(args.output)
    raw_inputs = collect_inputs(source_dir, args.inputs)

    seen_sizes: set[tuple[int, int]] = set()
    icon_entries: list[tuple[bytes, int, int]] = []
    for path in raw_inputs:
        if not path.is_file():
            continue
        png_bytes, width, height = read_png(path)
        if width != height or width > 256 or height > 256:
            continue
        size = (width, height)
        if size in seen_sizes:
            continue
        seen_sizes.add(size)
        icon_entries.append((png_bytes, width, height))

    if not icon_entries:
        print("No suitable PNG icon assets found for Windows ICO generation.", file=sys.stderr)
        return 1

    icon_entries.sort(key=lambda item: item[1])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(build_ico(icon_entries))
    print(output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
