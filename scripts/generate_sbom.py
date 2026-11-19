#!/usr/bin/env python3
"""Write a CycloneDX bill of materials for the build this interpreter is making.

The release used to scan the repository checkout. That answered the wrong
question: it listed the sixteen direct pins and every development tool in
`requirements-dev.txt` and `package-lock.json`, none of which ships, and missed
what does -- the transitive Python packages, the interpreter itself and the
PyInstaller bootloader. Each platform's bundle also differs (pyobjc is macOS
only, uvloop is not on Windows), so one file could not describe all of them.

Run this with the interpreter PyInstaller used, after PyInstaller has run. It
lists what the bundle holds, read from PyInstaller's work directory
(`sbom_bundle.py`): every installed distribution with a file in the bundle, the
native libraries those carry that can be asked for their version (HDF5, BLAS,
libzmq, libzstd, Pillow's codecs, and OpenSSL, zlib, expat and SQLite in the
interpreter), the interpreter, the PyInstaller bootloader and the third-party
files vendored into the frontend. The dependency edges come from walking the
runtime closure of `backend/requirements.txt` for this platform.

Build tools in the same environment are not in the bundle and so not listed.
The pyobjc meta-package pins some 170 framework packages on macOS; only the few
ALBIS imports are bundled, and only those are listed.

The build fails if a pin is not installed, or installed at another version: a
bill of materials for a build that did not use the pins would be wrong.
"""

from __future__ import annotations

import argparse
import json
import platform
import re
import sys
import tomllib
import uuid
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from importlib.metadata import Distribution, PackageNotFoundError, distribution, distributions
from pathlib import Path

from packaging.markers import default_environment
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

sys.path.insert(0, str(Path(__file__).resolve().parent))
from sbom_bundle import (
    Bundle,
    Native,
    cpe_for,
    is_bundled,
    probe_natives,
    read_bundle,
)  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
REQUIREMENTS = ROOT / "backend" / "requirements.txt"
VERSION_FILE = ROOT / "VERSION"
PYPROJECT = ROOT / "pyproject.toml"

# Who makes the components that have no package metadata to say so. Names
# only: a bill of materials says whose code it is, not how to reach them.
SUPPLIERS = {
    "CPython": "Python Software Foundation",
    "PyInstaller": "PyInstaller Development Team",
    "OpenSSL": "OpenSSL Foundation",
    "SQLite": "SQLite Consortium",
    "expat": "The libexpat project",
    "zlib": "Jean-loup Gailly and Mark Adler",
    "HDF5": "The HDF Group",
    "libzmq": "The ZeroMQ authors",
    "libzstd": "Meta Platforms",
    "Zstd": "Meta Platforms",
    "html2canvas": "Niklas von Hertzen",
    "Inter": "The Inter Project Authors",
    "Michroma": "The Michroma Project Authors",
    "pyFAI calibrant d-spacings": "European Synchrotron Radiation Facility",
}
# PyInstaller's work directory for ALBIS.spec, where its TOC files land.
WORKDIR = ROOT / "build" / "ALBIS"

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


def _slug(name: str) -> str:
    """`HDF5 LZ4 filter` -> `hdf5-lz4-filter`: a package URL has no spaces."""
    return re.sub(r"[^a-z0-9.+-]+", "-", name.lower()).strip("-")


def _ref(kind: str, name: str) -> str:
    return f"{kind}:{name}"


_EMAIL = re.compile(r"\s*<[^>]*>")


def package_authors(dist: Distribution) -> list[str]:
    """Author names from the metadata, with any e-mail addresses dropped.

    `Author-email` reads `Name <address>, "Other, Name" <address>`; `Author`
    is free text. Either may be missing, and a bare address names nobody.
    """
    meta = dist.metadata
    text = meta.get("Author") or ""
    if not text or "@" in text:
        text = meta.get("Author-email") or meta.get("Maintainer-email") or ""
    names = []
    for part in re.split(r",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", _EMAIL.sub("", text)):
        name = part.strip().strip('"').strip()
        if name and "@" not in name and name not in names:
            names.append(name)
    return names


def python_component(dist: Distribution) -> dict:
    name = canonicalize_name(dist.metadata["Name"])
    component = {
        "type": "library",
        "bom-ref": _ref("pypi", name),
        "name": dist.metadata["Name"],
        "version": dist.version,
        "purl": f"pkg:pypi/{name}@{dist.version}",
    }
    authors = package_authors(dist)
    if authors:
        component["authors"] = [{"name": author} for author in authors]
    licenses = _licenses(dist)
    if licenses:
        component["licenses"] = licenses
    return component


_SUPPLIERS_BY_NAME = {name.casefold(): supplier for name, supplier in SUPPLIERS.items()}


def _with_supplier(component: dict) -> dict:
    supplier = _SUPPLIERS_BY_NAME.get(component["name"].casefold())
    if supplier:
        component["supplier"] = {"name": supplier}
    return component


def albis_project() -> tuple[list[str], str]:
    """ALBIS's authors and home page, as pyproject.toml already publishes them."""
    project = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["project"]
    return [a["name"] for a in project.get("authors", []) if a.get("name")], project["urls"][
        "Homepage"
    ]


def interpreter_component(from_system: list[Native] = ()) -> dict:
    version = platform.python_version()
    component = {
        "type": "platform",
        "bom-ref": _ref("runtime", "cpython"),
        "name": "CPython",
        "version": version,
        "description": f"Python interpreter bundled by PyInstaller ({sys.implementation.name}, {platform.machine()})",
        "licenses": [{"license": {"id": "PSF-2.0"}}],
        "purl": f"pkg:generic/cpython@{version}",
        "cpe": cpe_for("CPython", version),
    }
    # Libraries the interpreter uses from the operating system: named, so the
    # file is complete, but not as components a scanner would hold against
    # the download.
    if from_system:
        component["properties"] = [
            {"name": "albis:system-library", "value": f"{n.name} {n.version}"} for n in from_system
        ]
    return component


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
            "bom-ref": _ref("vendored", _slug(item["name"])),
            "name": item["name"],
            "version": item["version"],
            "licenses": [{"license": {"id": item["license"]}}],
            "properties": [{"name": "albis:path", "value": path} for path in item["files"]],
        }
        if item["purl"]:
            component["purl"] = item["purl"]
        components.append(component)
    return components


def native_component(native: Native) -> dict:
    owner = canonicalize_name(native.carrier) if native.carrier else "cpython"
    slug = _slug(native.name)
    return {
        "type": "library",
        "bom-ref": _ref("native", f"{owner}/{slug}"),
        "name": native.name,
        "version": native.version,
        "purl": f"pkg:generic/{slug}@{native.version}",
        **({"cpe": cpe} if (cpe := cpe_for(native.name, native.version)) else {}),
        "properties": [{"name": "albis:bundled-in", "value": native.carrier or "CPython"}],
    }


def bundled_distributions(
    bundle: Bundle, installed: Iterable[Distribution]
) -> dict[str, Distribution]:
    """Installed distributions with at least one file in the bundle, by name."""
    found: dict[str, Distribution] = {}
    for dist in installed:
        name = canonicalize_name(dist.metadata["Name"] or "")
        # PyInstaller's own runtime hooks land in the bundle; it is listed once,
        # as the bootloader.
        if not name or name == "pyinstaller" or name in found:
            continue
        if is_bundled(dist, bundle):
            found[name] = dist
    return found


def contract(children: Iterable[str], edges: dict[str, set[str]], keep: set[str]) -> set[str]:
    """The nearest kept nodes below `children`, passing through dropped ones.

    The pyobjc meta-package ships no file of its own, so it is not in the
    bundle; the frameworks it pulls in are, and stay reachable from ALBIS.
    """
    reached: set[str] = set()
    stack, seen = list(children), set()
    while stack:
        node = stack.pop()
        if node in seen:
            continue
        seen.add(node)
        if node in keep:
            reached.add(node)
        else:
            stack.extend(edges.get(node, ()))
    return reached


def build_sbom(
    target: str,
    version: str,
    pins: list[Requirement],
    timestamp: datetime,
    bundle: Bundle,
    env: dict[str, str] | None = None,
    lookup=distribution,
    installed: Callable[[], Iterable[Distribution]] = distributions,
    probe: Callable[[Bundle, set[str]], list[Native]] = probe_natives,
    log=print,
) -> dict:
    env = dict(env or default_environment())
    found, edges = resolve_closure(pins, env, lookup)
    included = bundled_distributions(bundle, installed())
    left_out = sorted(set(found) - set(included))
    added = sorted(set(included) - set(found))
    if left_out:
        log(f"SBOM: {len(left_out)} distributions reached from the pins are not in the bundle")
    if added:
        log(f"SBOM: in the bundle without a pin reaching them: {', '.join(added)}")

    probed = probe(bundle, set(included))
    natives = [n for n in probed if not n.system]
    from_system = [n for n in probed if n.system]
    if from_system:
        log(
            "SBOM: linked from the operating system, not shipped: "
            + ", ".join(f"{n.name} {n.version}" for n in from_system)
        )
    native_refs: dict[str, list[str]] = {}
    for native in natives:
        owner = (
            _ref("pypi", canonicalize_name(native.carrier))
            if native.carrier
            else _ref("runtime", "cpython")
        )
        native_refs.setdefault(owner, []).append(native_component(native)["bom-ref"])

    keep = set(included)
    pinned = {canonicalize_name(p.name) for p in pins if p.marker is None or p.marker.evaluate(env)}
    root_ref = "albis"
    vendored = vendored_components()
    dependencies = [
        {
            "ref": root_ref,
            "dependsOn": sorted(
                [_ref("pypi", name) for name in contract(pinned, edges, keep)]
                # What PyInstaller's hooks add hangs off ALBIS directly; no
                # pin leads to it.
                + [_ref("pypi", name) for name in added]
                + [_ref("runtime", "cpython"), _ref("bootloader", "pyinstaller")]
                + [c["bom-ref"] for c in vendored]
            ),
        },
        {
            "ref": _ref("runtime", "cpython"),
            "dependsOn": sorted(native_refs.get(_ref("runtime", "cpython"), [])),
        },
    ]
    for name in sorted(included):
        ref = _ref("pypi", name)
        children = contract(edges.get(name, ()), edges, keep - {name})
        dependencies.append(
            {
                "ref": ref,
                "dependsOn": sorted(
                    [_ref("pypi", child) for child in children] + native_refs.get(ref, [])
                ),
            }
        )
    albis_authors, homepage = albis_project()
    project = {"name": "ALBIS project", "url": [homepage]}
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
            # Who made this bill of materials and the product it describes:
            # the ALBIS project, by the names pyproject.toml already gives.
            "manufacturer": project,
            "authors": [{"name": author} for author in albis_authors],
            "component": {
                "type": "application",
                "bom-ref": root_ref,
                "name": "ALBIS",
                "version": version,
                "supplier": project,
                "authors": [{"name": author} for author in albis_authors],
                "licenses": [{"license": {"id": "MIT"}}],
                "externalReferences": [{"type": "website", "url": homepage}],
                "properties": [{"name": "albis:target", "value": target}],
            },
        },
        "components": [
            _with_supplier(interpreter_component(from_system)),
            _with_supplier(pyinstaller_component(lookup)),
        ]
        + [python_component(included[name]) for name in sorted(included)]
        + [_with_supplier(native_component(native)) for native in natives]
        + [_with_supplier(c) for c in vendored],
        "dependencies": dependencies,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--target", required=True, help="Build target, e.g. linux-x64.")
    parser.add_argument("--output", required=True, type=Path, help="Where to write the JSON.")
    parser.add_argument(
        "--workdir",
        type=Path,
        default=WORKDIR,
        help="PyInstaller's work directory (default build/ALBIS).",
    )
    args = parser.parse_args(argv)

    version = VERSION_FILE.read_text(encoding="utf-8").strip()
    try:
        bundle = read_bundle(args.workdir)
        sbom = build_sbom(args.target, version, read_pins(), datetime.now(UTC), bundle)
    except (SbomError, FileNotFoundError) as exc:
        print(f"SBOM: {exc}", file=sys.stderr)
        return 1
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(sbom, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"SBOM: {len(sbom['components'])} components -> {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
