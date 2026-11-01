"""Data export: the names of the files an export writes."""

from __future__ import annotations

import re
from pathlib import Path

from fastapi import HTTPException


class DataExportNamesMixin:
    """File names for DataExportService: safe names, a series name and number,
    and a name not yet taken."""

    @staticmethod
    def _safe_name(value: str, fallback: str = "dataset") -> str:
        text = str(value or "").strip().replace("\\", "/")
        text = text.strip("/")
        text = re.sub(r"[^A-Za-z0-9_.-]+", "_", text)
        text = re.sub(r"_+", "_", text).strip("._-")
        return text or fallback

    @staticmethod
    def _next_available_path(path: Path) -> Path:
        if not path.exists():
            return path
        stem = path.stem
        suffix = path.suffix
        for idx in range(1, 10000):
            candidate = path.with_name(f"{stem}_{idx:03d}{suffix}")
            if not candidate.exists():
                return candidate
        raise HTTPException(status_code=500, detail="Unable to allocate output file name")

    # ---- Names -------------------------------------------------------------
    # `<name>[_thr<n>]_<number>`: the number last, as on a detector's own
    # images, so ALBIS, XDS (`name_?????.cbf`), DIALS and ALBULA read the
    # export as a series; each threshold its own series.
    STANDARD_DATASETS = frozenset({"entry/data/data", "data"})

    @classmethod
    def default_export_base(cls, source_path: Path, dataset: str = "") -> str:
        """`series_190` for `series_190_master.h5`, `image` for `image_00007.cbf`."""
        name = source_path.name
        lower = name.lower()
        stem = name[:-7] if lower.endswith(".cbf.gz") else source_path.stem
        stem = re.sub(r"_master$", "", stem, flags=re.IGNORECASE)
        if not dataset:
            # A numbered image: its series' name without the number.
            stem = re.sub(r"[_.-]*\d+$", "", stem) or stem
        tag = str(dataset or "").strip().strip("/")
        if tag and tag.lower() not in cls.STANDARD_DATASETS:
            stem = f"{stem}_{tag}"
        return cls._safe_name(stem, "dataset")

    @staticmethod
    def frame_name(base: str, number: int, threshold: int | None = None) -> str:
        parts = [base]
        if threshold is not None:
            parts.append(f"thr{threshold}")
        parts.append(f"{max(0, int(number)):05d}")
        return "_".join(parts)

    @staticmethod
    def source_frame_number(path: Path, fallback: int) -> int:
        """The number in a source image's own name: `image_00007.cbf` is 7."""
        name = path.name
        stem = name[:-7] if name.lower().endswith(".cbf.gz") else path.stem
        match = re.search(r"(\d+)\D*$", stem)
        return int(match.group(1)) if match else fallback

    def _free_base(
        self,
        out_dir: Path,
        base: str,
        planned: list[tuple[int, int | None]],
        suffix: str,
        overwrite: bool,
    ) -> str:
        """One name for the whole export: `base`, or `base_2`, `base_3`... if
        any of its files exists and overwrite is off, so the files stay one
        series instead of a counter landing after some of their numbers."""
        if overwrite:
            return base
        candidate = base
        for run in range(2, 10000):
            if not any(
                (out_dir / f"{self.frame_name(candidate, number, thr)}{suffix}").exists()
                for number, thr in planned
            ):
                return candidate
            candidate = f"{base}_{run}"
        raise HTTPException(status_code=500, detail="Unable to allocate output file name")
