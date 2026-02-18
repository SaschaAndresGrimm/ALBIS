from __future__ import annotations

"""Series operation helpers (grouping and mask semantics)."""

from typing import Any

import numpy as np
from fastapi import HTTPException


def mask_flag_value(dtype: np.dtype) -> float:
    if np.issubdtype(dtype, np.floating):
        return float("nan")
    if np.issubdtype(dtype, np.unsignedinteger):
        return float(np.iinfo(dtype).max)
    if np.issubdtype(dtype, np.integer):
        return float(np.iinfo(dtype).min)
    return float("nan")


def mask_slices(mask_bits: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    gap = (mask_bits & 1) != 0
    bad = (mask_bits & 0b11110) != 0
    any_mask = gap | bad
    return gap, bad, any_mask


def iter_sum_groups(
    frame_count: int,
    mode: str,
    step: int,
    range_start_1: int | None,
    range_end_1: int | None,
) -> list[dict[str, Any]]:
    if frame_count <= 0:
        raise HTTPException(status_code=400, detail="No frames available")

    mode = (mode or "chunks").lower()
    size = max(1, int(step))

    groups: list[dict[str, Any]] = []
    if mode == "all":
        indices = list(range(frame_count))
        groups.append(
            {"indices": indices, "start": 0, "end": frame_count - 1, "count": frame_count}
        )
        return groups

    if mode == "chunks":
        start = 0
        chunk_idx = 0
        while start < frame_count:
            end = min(frame_count - 1, start + size - 1)
            indices = list(range(start, end + 1))
            groups.append(
                {
                    "indices": indices,
                    "start": start,
                    "end": end,
                    "count": len(indices),
                    "chunk": chunk_idx,
                }
            )
            chunk_idx += 1
            start = end + 1
        return groups

    if mode == "nth":
        indices = list(range(0, frame_count, size))
        if not indices:
            return []
        return [
            {"indices": indices, "start": indices[0], "end": indices[-1], "count": len(indices)}
        ]

    if mode == "range":
        start_1 = int(range_start_1 or 1)
        end_1 = int(range_end_1 or frame_count)
        start = max(0, start_1 - 1)
        end = min(frame_count - 1, end_1 - 1)
        if start > end:
            raise HTTPException(status_code=400, detail="Range start must be <= range end")
        cursor = start
        while cursor <= end:
            chunk_end = min(end, cursor + size - 1)
            indices = list(range(cursor, chunk_end + 1))
            groups.append(
                {"indices": indices, "start": cursor, "end": chunk_end, "count": len(indices)}
            )
            cursor = chunk_end + 1
        return groups

    raise HTTPException(status_code=400, detail="Invalid series summing mode")
