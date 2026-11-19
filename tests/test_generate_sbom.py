"""The per-platform bill of materials lists what the bundle carries, and only that."""

from __future__ import annotations

import importlib.util
import pprint
import re
import sys
from datetime import UTC, datetime
from email.message import Message
from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path, PurePosixPath

import pytest
from packaging.requirements import Requirement

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "generate_sbom", ROOT / "scripts" / "generate_sbom.py"
)
sbom = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(sbom)
bundle_mod = sys.modules["sbom_bundle"]

LINUX = {
    "implementation_name": "cpython",
    "implementation_version": "3.13.9",
    "os_name": "posix",
    "platform_machine": "x86_64",
    "platform_release": "",
    "platform_system": "Linux",
    "platform_version": "",
    "python_full_version": "3.13.9",
    "platform_python_implementation": "CPython",
    "python_version": "3.13",
    "sys_platform": "linux",
}
WINDOWS = {**LINUX, "os_name": "nt", "platform_system": "Windows", "sys_platform": "win32"}
MACOS = {**LINUX, "platform_system": "Darwin", "sys_platform": "darwin"}
SITE = Path("/venv/site-packages")


class FakeDist:
    def __init__(
        self,
        name: str,
        version: str,
        requires: list[str] | None = None,
        files: list[str] | None = None,
        **meta: str,
    ):
        self.version = version
        self.requires = requires
        self.files = [PurePosixPath(f) for f in (files or [])]
        self.metadata = Message()
        self.metadata["Name"] = name
        for key, value in meta.items():
            self.metadata[key.replace("_", "-")] = value

    def locate_file(self, path):
        return SITE / path


def _lookup(dists: dict[str, FakeDist]):
    def lookup(name: str) -> FakeDist:
        try:
            return dists[name.lower()]
        except KeyError:
            raise PackageNotFoundError(name) from None

    return lookup


INSTALLED = {
    "uvicorn": FakeDist(
        "uvicorn",
        "0.54.0",
        [
            "h11>=0.8",
            "typing-extensions>=4.0; python_version < '3.11'",
            "websockets>=13.0; extra == 'standard'",
            "uvloop>=0.15.1; sys_platform != 'win32' and extra == 'standard'",
        ],
        ["uvicorn/__init__.py"],
    ),
    "h11": FakeDist("h11", "0.16.0", files=["h11/__init__.py"]),
    "websockets": FakeDist("websockets", "15.0", files=["websockets/__init__.py"]),
    "uvloop": FakeDist("uvloop", "0.21.0", files=["uvloop/__init__.py"]),
    # A meta-package: metadata only, nothing that could be bundled.
    "pyobjc": FakeDist(
        "pyobjc",
        "13.0",
        ["pyobjc-framework-cocoa==13.0", "pyobjc-framework-webkit==13.0"],
        ["pyobjc-13.0.dist-info/METADATA"],
    ),
    "pyobjc-framework-cocoa": FakeDist(
        "pyobjc-framework-Cocoa", "13.0", files=["AppKit/__init__.py"]
    ),
    "pyobjc-framework-webkit": FakeDist(
        "pyobjc-framework-WebKit", "13.0", files=["WebKit/__init__.py"]
    ),
    "pyinstaller": FakeDist(
        "pyinstaller", "6.19.0", ["altgraph"], ["PyInstaller/hooks/rthooks/pyi_rth_inspect.py"]
    ),
    "altgraph": FakeDist("altgraph", "0.17", files=["altgraph/__init__.py"]),
    "setuptools": FakeDist("setuptools", "80.0", files=["_distutils_hack/__init__.py"]),
}
PINS = [
    Requirement("uvicorn[standard]==0.54.0"),
    Requirement('pyobjc==13.0; sys_platform == "darwin"'),
]


def _bundle(*files: str, modules: tuple[str, ...] = ()) -> object:
    return bundle_mod.Bundle(
        sources={bundle_mod._norm(str(SITE / f)) for f in files}, modules=set(modules)
    )


def _build(bundle, env=LINUX, probe=lambda b, c: [], log=lambda msg: None) -> dict:
    return sbom.build_sbom(
        "linux-x64",
        "9.9.9",
        PINS,
        datetime(2026, 1, 2, tzinfo=UTC),
        bundle,
        env,
        _lookup(INSTALLED),
        lambda: INSTALLED.values(),
        probe,
        log,
    )


def _names(doc: dict) -> set[str]:
    return {c["bom-ref"] for c in doc["components"]}


def _deps(doc: dict, ref: str) -> set[str]:
    return set(next(d["dependsOn"] for d in doc["dependencies"] if d["ref"] == ref))


# --- The closure: pins, versions, edges --------------------------------------


def test_the_closure_follows_extras_and_this_platforms_markers() -> None:
    found, edges = sbom.resolve_closure(PINS, LINUX, _lookup(INSTALLED))
    assert set(found) == {"uvicorn", "h11", "websockets", "uvloop"}
    assert edges["uvicorn"] == {"h11", "websockets", "uvloop"}


def test_a_windows_closure_has_no_uvloop_and_no_pyobjc() -> None:
    found, _ = sbom.resolve_closure(PINS, WINDOWS, _lookup(INSTALLED))
    assert set(found) == {"uvicorn", "h11", "websockets"}


def test_a_pin_installed_at_another_version_fails_the_build() -> None:
    stale = {**INSTALLED, "uvicorn": FakeDist("uvicorn", "0.53.0")}
    with pytest.raises(sbom.SbomError, match="pinned ==0.54.0 but 0.53.0"):
        sbom.resolve_closure(PINS, LINUX, _lookup(stale))


def test_a_missing_dependency_fails_the_build() -> None:
    missing = {k: v for k, v in INSTALLED.items() if k != "h11"}
    with pytest.raises(sbom.SbomError, match="h11 is required but not installed"):
        sbom.resolve_closure(PINS, LINUX, _lookup(missing))


# --- What the bundle holds ---------------------------------------------------


EVERYTHING = (
    "uvicorn/__init__.py",
    "h11/__init__.py",
    "websockets/__init__.py",
    "uvloop/__init__.py",
    "PyInstaller/hooks/rthooks/pyi_rth_inspect.py",
)


def test_only_distributions_with_a_file_in_the_bundle_are_listed() -> None:
    doc = _build(_bundle("uvicorn/__init__.py", "h11/__init__.py"))
    pypi = {r for r in _names(doc) if r.startswith("pypi:")}
    assert pypi == {"pypi:uvicorn", "pypi:h11"}


def test_build_tools_in_the_same_environment_are_left_out() -> None:
    doc = _build(_bundle(*EVERYTHING))
    assert "pypi:altgraph" not in _names(doc)
    # PyInstaller's runtime hooks are bundled, but it is listed once, as the bootloader.
    assert "pypi:pyinstaller" not in _names(doc)
    assert "bootloader:pyinstaller" in _names(doc)


def test_a_bundled_package_no_pin_reaches_is_listed_and_reported() -> None:
    messages = []
    doc = _build(_bundle(*EVERYTHING, "_distutils_hack/__init__.py"), log=messages.append)
    assert "pypi:setuptools" in _names(doc)
    assert any("setuptools" in m for m in messages)
    assert "pypi:setuptools" in _deps(doc, "albis")


def test_on_macos_only_the_bundled_pyobjc_frameworks_are_listed() -> None:
    doc = _build(_bundle(*EVERYTHING, "AppKit/__init__.py"), env=MACOS)
    assert "pypi:pyobjc-framework-cocoa" in _names(doc)
    assert "pypi:pyobjc-framework-webkit" not in _names(doc)
    # The meta-package ships no file; the framework it pulls in still hangs off ALBIS.
    assert "pypi:pyobjc" not in _names(doc)
    assert "pypi:pyobjc-framework-cocoa" in _deps(doc, "albis")


def test_native_libraries_hang_off_the_package_that_carries_them() -> None:
    def probe(bundle, carriers):
        assert carriers == {"uvicorn", "h11", "websockets", "uvloop"}
        return [
            bundle_mod.Native("libuv", "1.50.0", "uvloop"),
            bundle_mod.Native("OpenSSL", "3.5.9", None),
        ]

    doc = _build(_bundle(*EVERYTHING), probe=probe)
    assert "native:uvloop/libuv" in _deps(doc, "pypi:uvloop")
    assert "native:cpython/openssl" in _deps(doc, "runtime:cpython")
    component = next(c for c in doc["components"] if c["bom-ref"] == "native:uvloop/libuv")
    assert component["purl"] == "pkg:generic/libuv@1.50.0"


def test_package_urls_and_refs_have_no_spaces() -> None:
    def probe(bundle, carriers):
        return [bundle_mod.Native("HDF5 LZ4 filter", "5573db8", "uvicorn")]

    doc = _build(_bundle(*EVERYTHING), probe=probe)
    native = next(c for c in doc["components"] if c["name"] == "HDF5 LZ4 filter")
    assert native["purl"] == "pkg:generic/hdf5-lz4-filter@5573db8"
    for component in doc["components"]:
        assert " " not in component["bom-ref"], component["bom-ref"]
        assert " " not in component.get("purl", ""), component["purl"]


def test_every_component_is_reachable_from_albis() -> None:
    doc = _build(
        _bundle(*EVERYTHING, "_distutils_hack/__init__.py", "AppKit/__init__.py"), env=MACOS
    )
    graph = {d["ref"]: d["dependsOn"] for d in doc["dependencies"]}
    reached, stack = set(), ["albis"]
    while stack:
        node = stack.pop()
        if node not in reached:
            reached.add(node)
            stack.extend(graph.get(node, ()))
    assert _names(doc) <= reached, sorted(_names(doc) - reached)


def test_the_document_is_cyclonedx_and_names_its_target() -> None:
    doc = _build(_bundle(*EVERYTHING))
    assert doc["bomFormat"] == "CycloneDX"
    assert doc["specVersion"] == "1.6"
    assert doc["metadata"]["timestamp"] == "2026-01-02T00:00:00Z"
    assert doc["metadata"]["component"]["version"] == "9.9.9"
    assert {"name": "albis:target", "value": "linux-x64"} in doc["metadata"]["component"][
        "properties"
    ]
    refs = [c["bom-ref"] for c in doc["components"]]
    assert len(refs) == len(set(refs))
    assert {"runtime:cpython", "vendored:html2canvas"} <= set(refs)
    # Every edge points at a component that exists.
    known = set(refs) | {"albis"}
    for dep in doc["dependencies"]:
        assert dep["ref"] in known
        assert set(dep["dependsOn"]) <= known


@pytest.mark.parametrize(
    ("meta", "expected"),
    [
        (
            {"License_Expression": "BSD-3-Clause AND MIT"},
            [{"expression": "BSD-3-Clause AND MIT"}],
        ),
        ({"License": "MPL-2.0"}, [{"license": {"id": "MPL-2.0"}}]),
        ({"License": "BSD License"}, [{"license": {"name": "BSD License"}}]),
        ({"License": "Copyright (c) someone\n\nPermission is hereby granted"}, []),
        ({}, []),
    ],
)
def test_licences_come_from_metadata_without_guessing(meta: dict, expected: list) -> None:
    assert sbom.python_component(FakeDist("x", "1.0", **meta)).get("licenses", []) == expected


# --- PyInstaller's TOC files --------------------------------------------------


def _write_toc(workdir: Path, name: str, entries: list[tuple[str, str, str]]) -> None:
    # The shape PyInstaller 6 writes: a tuple of lists of (name, source, typecode).
    workdir.mkdir(parents=True, exist_ok=True)
    (workdir / name).write_text(pprint.pformat(("/src/ALBIS", entries, False)), encoding="utf-8")


def test_the_toc_files_give_sources_and_module_names(tmp_path: Path) -> None:
    _write_toc(
        tmp_path,
        "Analysis-00.toc",
        [
            ("h5py", "/venv/site-packages/h5py/__init__.py", "PYMODULE"),
            ("lxml/etree.cpython-313-darwin.so", "/venv/site-packages/lxml/etree.so", "EXTENSION"),
            (
                "python3.13/lib-dynload/_ssl.cpython-313-darwin.so",
                "/py/lib-dynload/_ssl.so",
                "EXTENSION",
            ),
            ("_sqlite3.pyd", "C:\\py\\DLLs\\_sqlite3.pyd", "EXTENSION"),
            ("frontend/index.html", "/src/frontend/index.html", "DATA"),
        ],
    )
    bundle = bundle_mod.read_bundle(tmp_path)
    assert {"h5py", "lxml.etree", "_ssl", "_sqlite3"} <= bundle.modules
    assert "frontend/index.html" not in bundle.modules
    assert bundle_mod._norm("/venv/site-packages/h5py/__init__.py") in bundle.sources


def test_a_missing_work_directory_fails_the_build(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="run this after PyInstaller"):
        bundle_mod.read_bundle(tmp_path / "build" / "ALBIS")


def test_a_real_installed_distribution_is_found_by_its_files(tmp_path: Path) -> None:
    dist = distribution("packaging")
    file = next(f for f in dist.files if f.suffix == ".py")
    _write_toc(tmp_path, "PYZ-00.toc", [("packaging", str(dist.locate_file(file)), "PYMODULE")])
    bundle = bundle_mod.read_bundle(tmp_path)
    assert bundle_mod.is_bundled(dist, bundle)
    assert not bundle_mod.is_bundled(distribution("pytest"), bundle)


# --- Native probes, against the dev environment --------------------------------


def test_every_native_probe_still_reads_a_version() -> None:
    """Run each probe whose package is installed here.

    The release build skips a probe that throws rather than failing; this is
    where an upstream change to a version API is meant to surface, on the pull
    request that bumps the pin.
    """
    ran = 0
    for carrier, _module, probe in bundle_mod.PROBES:
        if carrier is not None:
            try:
                distribution(carrier)
            except PackageNotFoundError:
                continue
        results = probe()
        ran += 1
        for name, version in results:
            assert name and re.match(r"\d", version), (probe.__name__, name, version)
    assert ran >= 6


def test_the_hdf5plugin_list_covers_the_pinned_version() -> None:
    pin = next(p for p in sbom.read_pins() if p.name == "hdf5plugin")
    pinned = next(iter(pin.specifier)).version
    assert pinned in bundle_mod.HDF5PLUGIN_EMBEDDED, (
        f"hdf5plugin {pinned} is pinned; add its embedded libraries to HDF5PLUGIN_EMBEDDED "
        "from doc/information.rst at that release's tag"
    )


def test_hdf5plugin_lists_its_filters_and_the_libraries_in_them() -> None:
    found = dict(bundle_mod._hdf5plugin())
    assert found["c-blosc2"] == "3.3.2"
    assert found["LZ4"] == "1.10.0"
    assert found["bitshuffle"] == "0.5.2"
    # Shared libraries are listed once, though several filters embed them.
    assert list(dict(bundle_mod._hdf5plugin())) == sorted(found)


def test_an_unlisted_hdf5plugin_version_is_reported_not_guessed(monkeypatch) -> None:
    import hdf5plugin

    monkeypatch.setattr(hdf5plugin, "version", "0.0.1")
    with pytest.raises(LookupError, match="0.0.1"):
        bundle_mod._hdf5plugin()


def test_a_probe_only_runs_for_a_bundled_module() -> None:
    bundle = bundle_mod.Bundle(modules={"h5py.defs"})
    names = {n.name for n in bundle_mod.probe_natives(bundle, {"h5py"})}
    assert "HDF5" in names
    assert "libzmq" not in names


def test_a_broken_probe_is_reported_and_skipped(monkeypatch) -> None:
    def broken():
        raise AttributeError("moved")

    monkeypatch.setattr(bundle_mod, "PROBES", (("h5py", "h5py.defs", broken),))
    warnings = []
    result = bundle_mod.probe_natives(
        bundle_mod.Bundle(modules={"h5py.defs"}), {"h5py"}, warnings.append
    )
    assert result == []
    assert "moved" in warnings[0]


# --- Vendored files ------------------------------------------------------------


def test_every_vendored_file_is_claimed_and_exists() -> None:
    claimed = {path for item in sbom.VENDORED for path in item["files"]}
    for path in claimed:
        assert (ROOT / path).is_file(), path
    vendor = ROOT / "frontend" / "vendor"
    on_disk = {f"frontend/vendor/{p.name}" for p in vendor.iterdir() if p.is_file()}
    assert on_disk <= claimed, f"unlisted in VENDORED: {sorted(on_disk - claimed)}"


def test_vendored_versions_match_the_licence_notice() -> None:
    table = (ROOT / "THIRD_PARTY_LICENSES.md").read_text(encoding="utf-8")
    rows = dict(re.findall(r"^\|\s*([^|]+?)\s*\|\s*([0-9][^|]*?)\s*\|", table, re.MULTILINE))
    for item in sbom.VENDORED:
        row = next((v for k, v in rows.items() if k.startswith(item["name"])), None)
        assert row == item["version"], f"{item['name']}: SBOM {item['version']}, notice {row}"


def test_html2canvas_version_matches_the_file_header() -> None:
    header = (ROOT / "frontend" / "vendor" / "html2canvas.min.js").read_text(encoding="utf-8")[:200]
    item = next(i for i in sbom.VENDORED if i["name"] == "html2canvas")
    assert f"html2canvas {item['version']}" in header


def test_the_real_pins_parse() -> None:
    names = {p.name for p in sbom.read_pins()}
    assert {"fastapi", "h5py", "numpy"} <= names
