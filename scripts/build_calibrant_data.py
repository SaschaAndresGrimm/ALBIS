#!/usr/bin/env python3
"""Write frontend/modules/calibrant_data.js from pyFAI's calibrant files.

The Resolution Rings section can draw the rings of a reference material -- a
calibrant such as Si, LaB6 or CeO2, a metal such as Al, or ice -- over the
image. The d-spacings come from pyFAI's calibrant library (MIT, (c) European
Synchrotron Radiation Facility), one ``<name>.D`` file per material: a header
of ``#`` comments naming the material and its reference, then one line per
reflection with the d-spacing in Å first and, after a ``#``, its hkl.

ALBIS works offline and has no pyFAI dependency, so the values are copied into
a static module once, by this script, and committed. Run it again only to pick
up a newer pyFAI:

    python scripts/build_calibrant_data.py /path/to/pyFAI/resources/calibration
    python scripts/build_calibrant_data.py            # finds an installed pyFAI

Only the lowest-angle MAX_LINES distinct d-spacings of each material are kept:
the viewer draws at most that many, always the ones nearest the beam, so a
line past them could never be shown.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "frontend" / "modules" / "calibrant_data.js"

# The viewer's cap on reference rings; keep in step with calibrant_utils.js.
MAX_LINES = 60
# Two reflections closer than this in d are one ring (Si 333 and 511).
SAME_D = 1e-5

# The groups of the Reference list, in order: what each material is used for.
# A material in no group (ice) is listed before them.
GROUPS = ("standards", "saxs", "metals", "oxides", "other")

# Every pyFAI calibrant ALBIS lists, in the order the list shows them, with the
# label it shows. Labels are formulas and standard numbers, which read the same
# in every language; mineral names are proper names and stay as they are.
# pyFAI's "mock" is left out: it is a test fixture, not a material.
# One entry per calibrant, not one per NIST issue: pyFAI's Si, LaB6 and CeO2
# are SRM 640c, 660c and 674b. The issues differ by at most 0.0075 % in d (Si
# 640 to 640e), which moves a ring by a third of a pixel at the corner of an
# EIGER2 16M at 100 mm: they matter to a fit, not to a ring drawn as a guide,
# and the nine of them in the list looked like a choice that changed nothing.
CATALOGUE = (
    ("Si", "standards", "Si"),
    ("LaB6", "standards", "LaB₆"),
    ("CeO2", "standards", "CeO₂"),
    ("AgBh", "saxs", "AgBh (silver behenate)"),
    ("C14H30O", "saxs", "C₁₄H₃₀O (tetradecanol)"),
    ("PBBA", "saxs", "PBBA (p-bromobenzoic acid)"),
    ("Al", "metals", "Al"),
    ("Au", "metals", "Au"),
    ("Ni", "metals", "Ni"),
    ("Pt", "metals", "Pt"),
    ("alpha_Al2O3", "oxides", "α-Al₂O₃ (corundum)"),
    ("Cr2O3", "oxides", "Cr₂O₃ (eskolaite)"),
    ("CrOx", "oxides", "CrOx (ESRF MX)"),
    ("CuO", "oxides", "Cu₂O (cuprite)"),
    ("TiO2", "oxides", "TiO₂ (rutile)"),
    ("ZnO", "oxides", "ZnO (zincite)"),
    ("NaCl", "other", "NaCl (halite)"),
    ("quartz", "other", "SiO₂ (quartz)"),
    ("cristobaltite", "other", "SiO₂ (cristobalite)"),
    ("hydrocerussite", "other", "Pb₃(CO₃)₂(OH)₂ (hydrocerussite)"),
    ("vanadinite", "other", "Pb₅(VO₄)₃Cl (vanadinite)"),
)

# Hexagonal ice Ih, which pyFAI does not carry: the rings ice puts on a
# diffraction image. In no group, so the menu lists it first, under None: it is
# the reference wanted most often, and replaced a one-click Ice rings button.
ICE = {
    "id": "ice_Ih",
    "group": None,
    "label": "H₂O (ice Ih)",
    "ref": "Hexagonal ice Ih",
    "lines": [
        [3.897, "100"],
        [3.669, "002"],
        [3.441, "101"],
        [2.671, "102"],
        [2.249, "110"],
        [2.072, "103"],
        [1.948, "200"],
        [1.918, "112"],
        [1.883, "201"],
        [1.721, "202"],
    ],
}

_PARENS_RE = re.compile(r"\(([^)]*)\)")
_INT_RE = re.compile(r"^-?\d+$")


def format_hkl(indices: list[int]) -> str:
    """'111', '10-2'; spaced when an index has two digits ('10 0 0')."""
    if any(abs(index) > 9 for index in indices):
        return " ".join(str(index) for index in indices)
    return "".join(str(index) for index in indices)


def parse_hkl(comment: str) -> str:
    """The hkl in a line's comment, or '' when it gives none.

    Most files write ``(1, 1, 1) 8``; PBBA and vanadinite write the indices as
    bare integer columns, among other numbers that all carry a decimal point.
    """
    match = _PARENS_RE.search(comment)
    if match:
        parts = [part.strip() for part in match.group(1).split(",")]
    else:
        parts = [token for token in comment.split() if _INT_RE.match(token)]
    if len(parts) < 3 or not all(_INT_RE.match(part) for part in parts[:3]):
        return ""
    return format_hkl([int(part) for part in parts[:3]])


def parse_calibrant(text: str) -> tuple[str, list[list]]:
    """(reference, [[d, hkl], ...]) from a .D file, d descending and distinct."""
    reference = ""
    lines: list[list] = []
    for raw in text.splitlines():
        stripped = raw.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            body = stripped.lstrip("#").strip()
            if body.lower().startswith("ref:") and not reference:
                reference = body[4:].strip()
            continue
        value, _, comment = stripped.partition("#")
        try:
            d = float(value.split()[0])
        except (ValueError, IndexError):
            continue
        if d > 0:
            lines.append([d, parse_hkl(comment)])
    lines.sort(key=lambda line: -line[0])
    merged: list[list] = []
    for d, hkl in lines:
        if merged and abs(merged[-1][0] - d) < SAME_D:
            # One ring: name every reflection on it, once each.
            if hkl and hkl not in merged[-1][1].split("/"):
                merged[-1][1] = f"{merged[-1][1]}/{hkl}" if merged[-1][1] else hkl
            continue
        merged.append([round(d, 6), hkl])
    return reference, merged[:MAX_LINES]


def find_calibration_dir() -> Path:
    try:
        import pyFAI  # noqa: PLC0415 -- optional, only for this script
    except ImportError:
        sys.exit("pyFAI is not installed; pass its resources/calibration folder.")
    return Path(pyFAI.__file__).resolve().parent / "resources" / "calibration"


def build(calibration_dir: Path) -> list[dict]:
    entries = [ICE]
    for name, group, label in CATALOGUE:
        path = calibration_dir / f"{name}.D"
        reference, lines = parse_calibrant(path.read_text(encoding="utf-8"))
        entries.append(
            {"id": name, "group": group, "label": label, "ref": reference, "lines": lines}
        )
    return entries


def render(entries: list[dict]) -> str:
    out = [
        "/**",
        " * Reference materials for the Resolution Rings section: d-spacings in Å,",
        " * highest first, with the hkl of each ring ('' where the source gives none).",
        " * A material with group null is listed above the groups.",
        " *",
        " * GENERATED by scripts/build_calibrant_data.py from pyFAI's calibrant",
        " * library (MIT, (c) European Synchrotron Radiation Facility); edit the",
        " * script, not this file. Ice Ih is ALBIS's own.",
        " */",
        "",
        f"export const CALIBRANT_GROUPS = Object.freeze({json.dumps(list(GROUPS))});",
        "",
        "export const CALIBRANTS = Object.freeze([",
    ]
    for entry in entries:
        lines = json.dumps(entry["lines"], ensure_ascii=False, separators=(",", ":"))
        out += [
            "  {",
            f"    id: {json.dumps(entry['id'])},",
            f"    group: {json.dumps(entry['group'])},",
            f"    label: {json.dumps(entry['label'], ensure_ascii=False)},",
            f"    ref: {json.dumps(entry['ref'], ensure_ascii=False)},",
            f"    lines: {lines},",
            "  },",
        ]
    out += ["]);", ""]
    return "\n".join(out)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "calibration_dir",
        nargs="?",
        type=Path,
        help="pyFAI's resources/calibration folder (default: an installed pyFAI's)",
    )
    parser.add_argument("--output", type=Path, default=OUTPUT)
    args = parser.parse_args()
    calibration_dir = args.calibration_dir or find_calibration_dir()
    args.output.write_text(render(build(calibration_dir)), encoding="utf-8")
    print(f"wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
