from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

MIN_LOG_TAIL_LINES = 50
MAX_LOG_TAIL_LINES = 2000
DEFAULT_LOG_TAIL_LINES = 500
LOG_TAIL_CHUNK_BYTES = 16 * 1024


@dataclass(frozen=True)
class LogTailResult:
    path: str
    text: str
    requested_lines: int
    returned_lines: int
    truncated: bool
    size_bytes: int
    modified_at: float | None


def clamp_log_tail_lines(raw: int | None) -> int:
    try:
        value = int(raw if raw is not None else DEFAULT_LOG_TAIL_LINES)
    except (TypeError, ValueError):
        value = DEFAULT_LOG_TAIL_LINES
    return max(MIN_LOG_TAIL_LINES, min(MAX_LOG_TAIL_LINES, value))


def read_log_tail(path: Path, lines: int | None = None) -> LogTailResult:
    requested_lines = clamp_log_tail_lines(lines)
    stat_result = path.stat()
    size_bytes = int(stat_result.st_size)
    modified_at = float(stat_result.st_mtime) if stat_result.st_mtime else None

    if size_bytes <= 0:
        return LogTailResult(
            path=str(path),
            text="",
            requested_lines=requested_lines,
            returned_lines=0,
            truncated=False,
            size_bytes=0,
            modified_at=modified_at,
        )

    chunks: list[bytes] = []
    newline_count = 0
    position = size_bytes

    with path.open("rb") as handle:
        while position > 0 and newline_count < requested_lines:
            read_size = min(LOG_TAIL_CHUNK_BYTES, position)
            position -= read_size
            handle.seek(position)
            chunk = handle.read(read_size)
            if not chunk:
                break
            chunks.append(chunk)
            newline_count += chunk.count(b"\n")

    tail_bytes = b"".join(reversed(chunks))
    lines_with_endings = tail_bytes.decode("utf-8", errors="replace").splitlines(keepends=True)
    tail_lines = lines_with_endings[-requested_lines:]
    tail_text = "".join(tail_lines)

    return LogTailResult(
        path=str(path),
        text=tail_text,
        requested_lines=requested_lines,
        returned_lines=len(tail_lines),
        truncated=position > 0,
        size_bytes=size_bytes,
        modified_at=modified_at,
    )
