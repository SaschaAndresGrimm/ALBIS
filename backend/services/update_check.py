"""GitHub release-check helpers for manual ALBIS update notifications."""

from __future__ import annotations

import json
import threading
import time
import urllib.request
from dataclasses import dataclass
from logging import Logger
from urllib.error import URLError

try:
    from ..api_models import UpdateCheckResponse
except ImportError:  # pragma: no cover - supports `python backend/app.py`
    from api_models import UpdateCheckResponse  # type: ignore[no-redef]

LATEST_RELEASE_API_URL = "https://api.github.com/repos/SaschaAndresGrimm/ALBIS/releases/latest"
RELEASES_PAGE_URL = "https://github.com/SaschaAndresGrimm/ALBIS/releases"
REQUEST_TIMEOUT_SECONDS = 3.0
CACHE_TTL_SECONDS = 300.0


@dataclass(frozen=True)
class ReleaseMetadata:
    version: str
    release_url: str


@dataclass(frozen=True)
class ParsedVersion:
    numbers: tuple[int, ...]
    prerelease: tuple[int | str, ...] = ()


def _normalize_version_token(raw: str) -> str:
    token = str(raw or "").strip()
    if token.startswith(("v", "V")):
        token = token[1:]
    token = token.split("+", 1)[0].strip()
    if not token:
        raise ValueError("Missing version token")
    return token


def _parse_version(raw: str) -> ParsedVersion:
    token = _normalize_version_token(raw)
    base, sep, prerelease = token.partition("-")
    if not base:
        raise ValueError("Missing version base")

    number_parts: list[int] = []
    for part in base.split("."):
        if not part or not part.isdigit():
            raise ValueError(f"Invalid version segment: {part!r}")
        number_parts.append(int(part))

    prerelease_parts: list[int | str] = []
    if sep:
        for part in prerelease.split("."):
            ident = part.strip()
            if not ident:
                raise ValueError("Invalid prerelease segment")
            prerelease_parts.append(int(ident) if ident.isdigit() else ident.lower())

    return ParsedVersion(numbers=tuple(number_parts), prerelease=tuple(prerelease_parts))


def _compare_prerelease_identifiers(left: tuple[int | str, ...], right: tuple[int | str, ...]) -> int:
    length = max(len(left), len(right))
    for idx in range(length):
        if idx >= len(left):
            return -1
        if idx >= len(right):
            return 1
        left_part = left[idx]
        right_part = right[idx]
        if left_part == right_part:
            continue
        if isinstance(left_part, int) and isinstance(right_part, int):
            return -1 if left_part < right_part else 1
        if isinstance(left_part, int):
            return -1
        if isinstance(right_part, int):
            return 1
        return -1 if left_part < right_part else 1
    return 0


def compare_versions(left: str, right: str) -> int:
    """Compare two ALBIS/GitHub version strings."""

    left_version = _parse_version(left)
    right_version = _parse_version(right)

    length = max(len(left_version.numbers), len(right_version.numbers))
    for idx in range(length):
        left_part = left_version.numbers[idx] if idx < len(left_version.numbers) else 0
        right_part = right_version.numbers[idx] if idx < len(right_version.numbers) else 0
        if left_part != right_part:
            return -1 if left_part < right_part else 1

    if not left_version.prerelease and not right_version.prerelease:
        return 0
    if left_version.prerelease and not right_version.prerelease:
        return -1
    if not left_version.prerelease and right_version.prerelease:
        return 1
    return _compare_prerelease_identifiers(left_version.prerelease, right_version.prerelease)


class ReleaseCheckService:
    def __init__(
        self,
        current_version: str,
        logger: Logger,
        cache_ttl_seconds: float = CACHE_TTL_SECONDS,
    ) -> None:
        self.current_version = str(current_version or "0.0.0")
        self._logger = logger
        self._cache_ttl_seconds = max(0.0, float(cache_ttl_seconds))
        self._cache_lock = threading.Lock()
        self._cached_response: UpdateCheckResponse | None = None
        self._cached_at = 0.0

    def clear_cache(self) -> None:
        with self._cache_lock:
            self._cached_response = None
            self._cached_at = 0.0

    def check_for_update(self) -> UpdateCheckResponse:
        cached = self._get_cached_response()
        if cached is not None:
            return cached

        try:
            release = self._fetch_latest_release()
            status = (
                "update_available"
                if compare_versions(self.current_version, release.version) < 0
                else "up_to_date"
            )
            response = UpdateCheckResponse(
                status=status,
                current_version=self.current_version,
                latest_version=release.version,
                release_url=release.release_url or RELEASES_PAGE_URL,
                message="",
            )
        except Exception as exc:
            self._logger.warning("Update check failed: %s", exc)
            response = UpdateCheckResponse(
                status="unavailable",
                current_version=self.current_version,
                latest_version="",
                release_url=RELEASES_PAGE_URL,
                message=self._user_message_for_exception(exc),
            )

        self._store_cached_response(response)
        return response

    def _get_cached_response(self) -> UpdateCheckResponse | None:
        with self._cache_lock:
            if self._cached_response is None:
                return None
            age = time.monotonic() - self._cached_at
            if age > self._cache_ttl_seconds:
                self._cached_response = None
                self._cached_at = 0.0
                return None
            return self._cached_response

    def _store_cached_response(self, response: UpdateCheckResponse) -> None:
        with self._cache_lock:
            self._cached_response = response
            self._cached_at = time.monotonic()

    def _fetch_latest_release(self) -> ReleaseMetadata:
        request = urllib.request.Request(
            LATEST_RELEASE_API_URL,
            headers={
                "Accept": "application/vnd.github+json",
                "User-Agent": f"ALBIS/{self.current_version}",
            },
        )
        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            payload = json.load(response)

        if not isinstance(payload, dict):
            raise ValueError("Invalid GitHub release payload")

        tag_name = str(payload.get("tag_name") or "").strip()
        if not tag_name:
            raise ValueError("Missing GitHub release tag")

        release_url = str(payload.get("html_url") or RELEASES_PAGE_URL).strip() or RELEASES_PAGE_URL
        return ReleaseMetadata(version=_normalize_version_token(tag_name), release_url=release_url)

    def _user_message_for_exception(self, exc: Exception) -> str:
        if isinstance(exc, ValueError):
            return "GitHub release metadata was invalid."
        if isinstance(exc, TimeoutError):
            return "GitHub release metadata timed out."
        if isinstance(exc, URLError) and isinstance(getattr(exc, "reason", None), TimeoutError):
            return "GitHub release metadata timed out."
        return "GitHub release metadata was unavailable."
