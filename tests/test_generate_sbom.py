"""The per-platform bill of materials lists what the bundle carries, and only that."""

from __future__ import annotations

import importlib.util
import re
from datetime import UTC, datetime
from email.message import Message
from importlib.metadata import PackageNotFoundError
from pathlib import Path

import pytest
from packaging.requirements import Requirement

ROOT = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "generate_sbom", ROOT / "scripts" / "generate_sbom.py"
)
sbom = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(sbom)

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


class FakeDist:
    def __init__(self, name: str, version: str, requires: list[str] | None = None, **meta: str):
        self.version = version
        self.requires = requires
        self.metadata = Message()
        self.metadata["Name"] = name
        for key, value in meta.items():
            self.metadata[key.replace("_", "-")] = value


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
    ),
    "h11": FakeDist("h11", "0.16.0"),
    "websockets": FakeDist("websockets", "15.0"),
    "uvloop": FakeDist("uvloop", "0.21.0"),
    "pyobjc": FakeDist("pyobjc", "13.0"),
    "pyinstaller": FakeDist("pyinstaller", "6.19.0", ["altgraph"]),
}
PINS = [
    Requirement("uvicorn[standard]==0.54.0"),
    Requirement('pyobjc==13.0; sys_platform == "darwin"'),
]


def test_the_closure_follows_extras_and_this_platforms_markers() -> None:
    found, edges = sbom.resolve_closure(PINS, LINUX, _lookup(INSTALLED))
    assert set(found) == {"uvicorn", "h11", "websockets", "uvloop"}
    assert edges["uvicorn"] == {"h11", "websockets", "uvloop"}


def test_a_windows_bundle_lists_no_uvloop_and_no_pyobjc() -> None:
    found, _ = sbom.resolve_closure(PINS, WINDOWS, _lookup(INSTALLED))
    assert set(found) == {"uvicorn", "h11", "websockets"}


def test_build_tools_in_the_same_environment_are_left_out() -> None:
    found, _ = sbom.resolve_closure(PINS, LINUX, _lookup(INSTALLED))
    assert "pyinstaller" not in found
    assert "altgraph" not in found


def test_a_pin_installed_at_another_version_fails_the_build() -> None:
    stale = {**INSTALLED, "uvicorn": FakeDist("uvicorn", "0.53.0")}
    with pytest.raises(sbom.SbomError, match="pinned ==0.54.0 but 0.53.0"):
        sbom.resolve_closure(PINS, LINUX, _lookup(stale))


def test_a_missing_dependency_fails_the_build() -> None:
    missing = {k: v for k, v in INSTALLED.items() if k != "h11"}
    with pytest.raises(sbom.SbomError, match="h11 is required but not installed"):
        sbom.resolve_closure(PINS, LINUX, _lookup(missing))


@pytest.mark.parametrize(
    ("meta", "expected"),
    [
        ({"License_Expression": "BSD-3-Clause AND MIT"}, [{"expression": "BSD-3-Clause AND MIT"}]),
        ({"License": "MPL-2.0"}, [{"license": {"id": "MPL-2.0"}}]),
        ({"License": "BSD License"}, [{"license": {"name": "BSD License"}}]),
        ({"License": "Copyright (c) someone\n\nPermission is hereby granted"}, []),
        ({}, []),
    ],
)
def test_licences_come_from_metadata_without_guessing(meta: dict, expected: list) -> None:
    assert sbom.python_component(FakeDist("x", "1.0", **meta)).get("licenses", []) == expected


def test_the_document_is_cyclonedx_and_names_its_target() -> None:
    when = datetime(2026, 1, 2, tzinfo=UTC)
    doc = sbom.build_sbom("linux-x64", "9.9.9", PINS, when, LINUX, _lookup(INSTALLED))

    assert doc["bomFormat"] == "CycloneDX"
    assert doc["specVersion"] == "1.6"
    assert doc["metadata"]["timestamp"] == "2026-01-02T00:00:00Z"
    assert doc["metadata"]["component"]["version"] == "9.9.9"
    assert {"name": "albis:target", "value": "linux-x64"} in doc["metadata"]["component"][
        "properties"
    ]

    refs = [c["bom-ref"] for c in doc["components"]]
    assert len(refs) == len(set(refs))
    assert {"runtime:cpython", "bootloader:pyinstaller", "pypi:h11", "vendored:html2canvas"} <= set(
        refs
    )
    # Every edge points at a component that exists.
    known = set(refs) | {"albis"}
    for dep in doc["dependencies"]:
        assert dep["ref"] in known
        assert set(dep["dependsOn"]) <= known


def test_every_vendored_file_is_claimed_and_exists() -> None:
    claimed = {path for item in sbom.VENDORED for path in item["files"]}
    for path in claimed:
        assert (ROOT / path).is_file(), path
    on_disk = {
        f"frontend/vendor/{p.name}" for p in (ROOT / "frontend" / "vendor").iterdir() if p.is_file()
    }
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
