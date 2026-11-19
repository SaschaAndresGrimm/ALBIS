#!/usr/bin/env python3
"""Write a CycloneDX bill of materials for the build this interpreter is making.

The release used to scan the repository checkout. That answered the wrong
question: it listed the sixteen direct pins and every development tool in
`requirements-dev.txt` and `package-lock.json`, none of which ships, and missed
what does -- the transitive Python packages, the interpreter itself and the
PyInstaller bootloader. Each platform's bundle also differs (pyobjc is macOS
only, uvloop is not on Windows), so one file could not describe all of them.

Run this with the interpreter PyInstaller used, after PyInstaller has run. It
walks the runtime closure of `backend/requirements.txt` through the installed
distributions, evaluating every environment marker for this platform, and adds
the interpreter, PyInstaller and the third-party files vendored into the
frontend. Build tools in the same environment (pip, PyInstaller's own
dependencies) are left out because nothing reaches them from the pins.

What it does not see: native libraries a wheel carries inside itself (the HDF5
in h5py, the OpenBLAS in numpy, libzmq in pyzmq) are covered only through the
wheel that bundles them. On macOS the closure lists every pyobjc framework
package although PyInstaller bundles only the few ALBIS imports, so the file
over-reports there rather than under-reports.

The build fails if a pin is not installed, or installed at another version: a
bill of materials for a build that did not use the pins would be wrong.
"""

from __future__ import annotations

import argparse
import json
import platform
import re
import sys
import uuid
from datetime import UTC, datetime
from importlib.metadata import Distribution, PackageNotFoundError, distribution
from pathlib import Path

from packaging.markers import default_environment
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

ROOT = Path(__file__).resolve().parents[1]
REQUIREMENTS = ROOT / "backend" / "requirements.txt"
VERSION_FILE = ROOT / "VERSION"

# Third-party files copied into the bundle as files rather than installed as
# packages, so no metadata names them. Versions follow THIRD_PARTY_LICENSES.md;
# tests/test_generate_sbom.py holds the two together and checks every file in
# frontend/vendor/ is claimed here.
VENDORED = (
    {
        "name": "html2canvas",
        "version": "1.4.1",
        "license": "MIT",
        "purl": "pkg:npm/html2canvas@1.4.1",
        "files": ("frontend/vendor/html2canvas.min.js",),
    },
    {
        "name": "Inter",
        "version": "4.1",
        "license": "OFL-1.1",
        "purl": "pkg:github/rsms/inter@v4.1",
        "files": ("frontend/vendor/InterVariable.woff2", "frontend/vendor/InterVariable-OFL.txt"),
    },
    {
        "name": "Michroma",
        "version": "1.100",
        "license": "OFL-1.1",
        "purl": None,
        "files": ("frontend/vendor/Michroma-Regular.ttf", "frontend/vendor/Michroma-OFL.txt"),
    },
    {
        "name": "pyFAI calibrant d-spacings",
        "version": "2023.5",
        "license": "MIT",
        "purl": "pkg:pypi/pyfai@2023.5",
        "files": ("frontend/modules/calibrant_data.js",),
    },
)


class SbomError(RuntimeError):
    """The environment does not match the pins, so no honest SBOM can be written."""


def read_pins(path: Path = REQUIREMENTS) -> list[Requirement]:
    """The requirement lines of `path`, comments and blanks skipped."""
    pins = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.split("#", 1)[0].strip()
        if stripped:
            pins.append(Requirement(stripped))
    return pins


def _applies(req: Requirement, extras: frozenset[str], env: dict[str, str]) -> bool:
    if req.marker is None:
        return True
    # A marker without `extra` ignores the value; one with it must match one of
    # the extras the parent was asked for.
    return any(req.marker.evaluate({**env, "extra": extra}) for extra in extras or {""})


def resolve_closure(
    pins: list[Requirement],
    env: dict[str, str] | None = None,
    lookup=distribution,
) -> tuple[dict[str, Distribution], dict[str, set[str]]]:
    """Every installed distribution the pins reach, and who depends on whom.

    Returns the distributions by canonical name and the dependency edges between
    them. `lookup` is the metadata source, replaceable in tests.
    """
    env = dict(env or default_environment())
    found: dict[str, Distribution] = {}
    edges: dict[str, set[str]] = {}
    seen: set[tuple[str, frozenset[str]]] = set()
    queue: list[tuple[Requirement, frozenset[str], bool]] = []

    for pin in pins:
        if _applies(pin, frozenset(), env):
            queue.append((pin, frozenset(pin.extras), True))

    while queue:
        req, extras, is_pin = queue.pop()
        name = canonicalize_name(req.name)
        if (name, extras) in seen:
            continue
        seen.add((name, extras))
        try:
            dist = lookup(req.name)
        except PackageNotFoundError as exc:
            raise SbomError(f"{req.name} is required but not installed") from exc
        installed = dist.version
        if is_pin and req.specifier and not req.specifier.contains(installed, prereleases=True):
            raise SbomError(f"{req.name} is pinned {req.specifier} but {installed} is installed")
        found[name] = dist
        edges.setdefault(name, set())
        for raw in dist.requires or []:
            child = Requirement(raw)
            if _applies(child, extras, env):
                edges[name].add(canonicalize_name(child.name))
                queue.append((child, frozenset(child.extras), False))
    return found, edges


_SPDX_ID = re.compile(r"^[A-Za-z0-9.+-]+$")


def _licenses(dist: Distribution) -> list[dict]:
    meta = dist.metadata
    expression = (meta.get("License-Expression") or "").strip()
    if expression:
        return [{"expression": expression}]
    text = (meta.get("License") or "").strip()
    # The free-text field holds anything from an SPDX id to the whole licence.
    if text and "\n" not in text and len(text) <= 64:
        key = "id" if _SPDX_ID.match(text) else "name"
        return [{"license": {key: text}}]
    return []


def _ref(kind: str, name: str) -> str:
    return f"{kind}:{name}"


def python_component(dist: Distribution) -> dict:
    name = canonicalize_name(dist.metadata["Name"])
    component = {
        "type": "library",
        "bom-ref": _ref("pypi", name),
        "name": dist.metadata["Name"],
        "version": dist.version,
        "purl": f"pkg:pypi/{name}@{dist.version}",
    }
    licenses = _licenses(dist)
    if licenses:
        component["licenses"] = licenses
    return component


def interpreter_component() -> dict:
    version = platform.python_version()
    return {
        "type": "platform",
        "bom-ref": _ref("runtime", "cpython"),
        "name": "CPython",
        "version": version,
        "description": f"Python interpreter bundled by PyInstaller ({sys.implementation.name}, {platform.machine()})",
        "licenses": [{"license": {"id": "PSF-2.0"}}],
        "purl": f"pkg:generic/cpython@{version}",
    }


def pyinstaller_component(lookup=distribution) -> dict:
    """PyInstaller's bootloader is the executable each bundle starts from."""
    try:
        dist = lookup("pyinstaller")
    except PackageNotFoundError as exc:
        raise SbomError("PyInstaller is not installed; run this after the build") from exc
    component = python_component(dist)
    component["bom-ref"] = _ref("bootloader", "pyinstaller")
    component["type"] = "application"
    component["description"] = "Bootloader that starts the bundle"
    return component


def vendored_components() -> list[dict]:
    components = []
    for item in VENDORED:
        component = {
            "type": "library",
            "bom-ref": _ref("vendored", canonicalize_name(item["name"])),
            "name": item["name"],
            "version": item["version"],
            "licenses": [{"license": {"id": item["license"]}}],
            "properties": [{"name": "albis:path", "value": path} for path in item["files"]],
        }
        if item["purl"]:
            component["purl"] = item["purl"]
        components.append(component)
    return components


def build_sbom(
    target: str,
    version: str,
    pins: list[Requirement],
    timestamp: datetime,
    env: dict[str, str] | None = None,
    lookup=distribution,
) -> dict:
    found, edges = resolve_closure(pins, env, lookup)
    root_ref = "albis"
    python_components = [python_component(found[name]) for name in sorted(found)]
    pinned = {canonicalize_name(p.name) for p in pins} & set(found)
    dependencies = [
        {
            "ref": root_ref,
            "dependsOn": sorted(
                [_ref("pypi", name) for name in pinned]
                + [_ref("runtime", "cpython"), _ref("bootloader", "pyinstaller")]
                + [c["bom-ref"] for c in vendored_components()]
            ),
        }
    ]
    dependencies += [
        {
            "ref": _ref("pypi", name),
            "dependsOn": sorted(_ref("pypi", child) for child in edges[name]),
        }
        for name in sorted(edges)
    ]
    return {
        "bomFormat": "CycloneDX",
        "specVersion": "1.6",
        "serialNumber": f"urn:uuid:{uuid.uuid4()}",
        "version": 1,
        "metadata": {
            "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "tools": {
                "components": [{"type": "application", "name": "albis scripts/generate_sbom.py"}]
            },
            "component": {
                "type": "application",
                "bom-ref": root_ref,
                "name": "ALBIS",
                "version": version,
                "licenses": [{"license": {"id": "MIT"}}],
                "properties": [{"name": "albis:target", "value": target}],
            },
        },
        "components": [interpreter_component(), pyinstaller_component(lookup)]
        + python_components
        + vendored_components(),
        "dependencies": dependencies,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--target", required=True, help="Build target, e.g. linux-x64.")
    parser.add_argument("--output", required=True, type=Path, help="Where to write the JSON.")
    args = parser.parse_args(argv)

    version = VERSION_FILE.read_text(encoding="utf-8").strip()
    try:
        sbom = build_sbom(args.target, version, read_pins(), datetime.now(UTC))
    except SbomError as exc:
        print(f"SBOM: {exc}", file=sys.stderr)
        return 1
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(sbom, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"SBOM: {len(sbom['components'])} components -> {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
