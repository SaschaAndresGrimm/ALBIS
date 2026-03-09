"""Path policy helpers for backend routes and services."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from fastapi import HTTPException


@dataclass(frozen=True)
class PathPolicy:
    data_dir: Path
    autoload_exts: set[str]
    image_ext_name: Callable[[str], str]
    allow_abs_paths: Callable[[], bool]

    @staticmethod
    def is_within(path: Path, root: Path) -> bool:
        """Return True when `path` is inside `root` after normalization."""
        try:
            path.relative_to(root)
            return True
        except ValueError:
            return False

    def safe_rel_path(self, name: str) -> Path:
        if not name:
            raise HTTPException(status_code=400, detail="Invalid file name")
        if name.startswith(("/", "\\")):
            raise HTTPException(status_code=400, detail="Invalid file name")
        raw = Path(name)
        if raw.is_absolute():
            raise HTTPException(status_code=400, detail="Invalid file name")
        if any(part == ".." or part.startswith(".") for part in raw.parts):
            raise HTTPException(status_code=400, detail="Invalid file name")
        return raw

    def resolve_hdf5_file(self, name: str) -> Path:
        raw = Path(name)
        if raw.is_absolute():
            if not self.allow_abs_paths():
                raise HTTPException(status_code=400, detail="Absolute paths are disabled")
            path = raw.expanduser().resolve()
            if not path.exists() or path.suffix.lower() not in {".h5", ".hdf5"}:
                raise HTTPException(status_code=404, detail="File not found")
            return path
        safe = self.safe_rel_path(name)
        root = self.data_dir.resolve()
        path = (self.data_dir / safe).resolve()
        if not self.is_within(path, root):
            raise HTTPException(status_code=400, detail="Invalid file name")
        if not path.exists() or path.suffix.lower() not in {".h5", ".hdf5"}:
            raise HTTPException(status_code=404, detail="File not found")
        return path

    def resolve_dir(self, name: str | None) -> Path:
        if name is None:
            return self.data_dir.resolve()
        trimmed = name.strip()
        if trimmed in ("", ".", "./"):
            return self.data_dir.resolve()
        raw = Path(trimmed)
        if raw.is_absolute():
            if not self.allow_abs_paths():
                raise HTTPException(status_code=400, detail="Absolute paths are disabled")
            path = raw.expanduser().resolve()
            if not path.exists() or not path.is_dir():
                raise HTTPException(status_code=404, detail="Directory not found")
            return path
        safe = self.safe_rel_path(trimmed)
        root = self.data_dir.resolve()
        path = (self.data_dir / safe).resolve()
        if not self.is_within(path, root):
            raise HTTPException(status_code=400, detail="Invalid directory")
        if not path.exists() or not path.is_dir():
            raise HTTPException(status_code=404, detail="Directory not found")
        return path

    def resolve_image_file(self, name: str) -> Path:
        raw = Path(name)
        if raw.is_absolute():
            if not self.allow_abs_paths():
                raise HTTPException(status_code=400, detail="Absolute paths are disabled")
            path = raw.expanduser().resolve()
            if not path.exists() or self.image_ext_name(path.name) not in self.autoload_exts:
                raise HTTPException(status_code=404, detail="File not found")
            return path
        safe = self.safe_rel_path(name)
        root = self.data_dir.resolve()
        path = (self.data_dir / safe).resolve()
        if not self.is_within(path, root):
            raise HTTPException(status_code=400, detail="Invalid file name")
        if not path.exists() or self.image_ext_name(path.name) not in self.autoload_exts:
            raise HTTPException(status_code=404, detail="File not found")
        return path

    def parse_ext_filter(self, exts: str | None) -> set[str]:
        if not exts:
            return set(self.autoload_exts)
        cleaned: set[str] = set()
        for raw in exts.split(","):
            token = raw.strip().lower()
            if not token:
                continue
            if not token.startswith("."):
                token = f".{token}"
            cleaned.add(token)
            if token == ".cbf":
                cleaned.add(".cbf.gz")
        allowed = cleaned.intersection(self.autoload_exts)
        return allowed or set(self.autoload_exts)
