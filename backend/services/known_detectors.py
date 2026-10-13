"""The detectors this ALBIS server has talked to, shared by everyone using it.

A lab has a handful of detectors at fixed addresses, and an IP says nothing
about which detector it is. A working address is remembered with what the
detector says it is -- model and serial -- and a detector can be kept and given
a name of its own. Kept ones stay; the others are the last few used.

The list is one JSON file, by default ``~/.config/albis/known_detectors.json``
(beside ``server.json``), so it survives restarts and upgrades and moves with a
copied folder. ``ui.known_detectors_file`` points it elsewhere, for instance at
one list on a network drive for several installations.

Every change is applied here, not sent as a whole list: the file is re-read,
changed and written back under a lock, so two people starring at once cannot
undo each other, and an edit made to the file by hand or by another server is
picked up on the next read. The write is atomic (a sibling temp file and
``os.replace``), so the file is only ever the old list or the new one.

Entries are unique by address. The serial follows a detector: found at a new
address, its entry moves with it rather than appearing twice; a different
detector answering at a known address takes that entry over, without the old
one's name. Mirrors the rules ``frontend/modules/known_detectors.js`` shows.
"""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

from .simplon import normalize_simplon_base_url

MAX_RECENT = 8
MAX_KEPT = 50
NAME_LIMIT = 80
TEXT_LIMIT = 200
FILE_NAME = "known_detectors.json"


def default_path() -> Path:
    return Path.home() / ".config" / "albis" / FILE_NAME


def resolve_path(setting: str, config_path: Path | None = None) -> Path:
    """The list's file: the setting if given, else the default.

    A relative setting is taken from the config file's folder, where whoever
    wrote it was looking.
    """
    raw = str(setting or "").strip()
    if not raw:
        return default_path()
    path = Path(raw).expanduser()
    if not path.is_absolute() and config_path is not None:
        path = config_path.parent / path
    return path


def _text(value: Any, limit: int = TEXT_LIMIT) -> str:
    return value.strip()[:limit] if isinstance(value, str) else ""


def normalize_address(value: Any) -> str:
    """A canonical http(s) base URL, or "" for anything that is not one."""
    address = normalize_simplon_base_url(_text(value))
    return address if address.startswith(("http://", "https://")) else ""


def _trim(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_recent = sorted(entries, key=lambda entry: entry["lastUsed"], reverse=True)
    kept = [entry for entry in by_recent if entry["kept"]][:MAX_KEPT]
    recent = [entry for entry in by_recent if not entry["kept"]][:MAX_RECENT]
    return kept + recent


def sanitize(value: Any) -> list[dict[str, Any]]:
    """Well-formed entries only, one per address, within the limits."""
    if not isinstance(value, list):
        return []
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for item in value:
        # Objects only: anything else in the file is damage, not a detector.
        if not isinstance(item, dict):
            continue
        address = normalize_address(item.get("address"))
        if not address or address in seen:
            continue
        seen.add(address)
        last_used = item.get("lastUsed")
        out.append(
            {
                "address": address,
                "name": _text(item.get("name"), NAME_LIMIT),
                "model": _text(item.get("model")),
                "serial": _text(item.get("serial")),
                "kept": item.get("kept") is True,
                "lastUsed": (
                    int(last_used)
                    if isinstance(last_used, (int, float)) and not isinstance(last_used, bool)
                    else 0
                ),
            }
        )
    return _trim(out)


def record(
    entries: list[dict[str, Any]],
    address: Any,
    model: Any = "",
    serial: Any = "",
    now: int | None = None,
) -> list[dict[str, Any]]:
    """Remember that a detector answered at ``address``."""
    current = sanitize(entries)
    at = normalize_address(address)
    if not at:
        return current
    stamp = int(time.time() * 1000) if now is None else now
    ident = _text(serial)
    here = next((entry for entry in current if entry["address"] == at), None)
    # The same detector known at another address: it moved, its entry follows.
    moved = (
        next((e for e in current if e["serial"] == ident and e["address"] != at), None)
        if ident
        else None
    )
    if moved is not None:
        base = {**moved, "kept": moved["kept"] or bool(here and here["kept"])}
    elif here is not None and ident and here["serial"] and here["serial"] != ident:
        # Another detector now answers here: the old name was for the old one.
        base = {"name": "", "model": "", "serial": "", "kept": here["kept"]}
    else:
        base = here or {"name": "", "model": "", "serial": "", "kept": False}
    entry = {
        **base,
        "address": at,
        "model": _text(model) or base["model"],
        "serial": ident or base["serial"],
        "lastUsed": stamp,
    }
    others = [e for e in current if e["address"] != at and e is not moved]
    return _trim([entry, *others])


def update(
    entries: list[dict[str, Any]], address: Any, changes: dict[str, Any]
) -> list[dict[str, Any]]:
    """Rename an entry or change whether it is kept; an unknown address changes nothing."""
    at = normalize_address(address)
    out = []
    for entry in sanitize(entries):
        if entry["address"] == at:
            entry = dict(entry)
            if "name" in changes:
                entry["name"] = _text(changes["name"], NAME_LIMIT)
            if "kept" in changes:
                entry["kept"] = bool(changes["kept"])
        out.append(entry)
    return sanitize(out)


def forget(entries: list[dict[str, Any]], address: Any) -> list[dict[str, Any]]:
    at = normalize_address(address)
    return [entry for entry in sanitize(entries) if entry["address"] != at]


def merge(entries: list[dict[str, Any]], incoming: Any) -> list[dict[str, Any]]:
    """Take over a list kept elsewhere (a browser's, from before): each entry
    is recorded as seen when it was, and keeps its name and star."""
    current = sanitize(entries)
    for item in sorted(sanitize(incoming), key=lambda entry: entry["lastUsed"]):
        current = record(current, item["address"], item["model"], item["serial"], item["lastUsed"])
        changes: dict[str, Any] = {}
        if item["name"]:
            changes["name"] = item["name"]
        if item["kept"]:
            changes["kept"] = True
        if changes:
            current = update(current, item["address"], changes)
    return current


class KnownDetectorStore:
    """The list on disk, read and changed one request at a time."""

    def __init__(self, path: Path | Any) -> None:
        # A path, or a callable giving it: the setting can change while ALBIS runs.
        self._path = path
        self._lock = threading.Lock()

    @property
    def path(self) -> Path:
        return Path(self._path() if callable(self._path) else self._path)

    def _read(self, path: Path) -> list[dict[str, Any]]:
        try:
            return sanitize(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, ValueError):
            # Missing, unreadable or damaged: an empty list, never a broken field.
            return []

    def _write(self, path: Path, entries: list[dict[str, Any]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(entries, indent=2, ensure_ascii=False) + "\n"
        # Same folder as the target: os.replace is only atomic within a filesystem.
        handle, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(handle, "w", encoding="utf-8") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, path)
        except BaseException:
            with contextlib.suppress(OSError):
                tmp_path.unlink()
            raise

    def list(self) -> list[dict[str, Any]]:
        with self._lock:
            return self._read(self.path)

    def change(self, apply) -> list[dict[str, Any]]:
        """Re-read, apply ``apply(entries)``, write back if anything changed."""
        with self._lock:
            path = self.path
            before = self._read(path)
            after = sanitize(apply(before))
            if after != before:
                self._write(path, after)
            return after
