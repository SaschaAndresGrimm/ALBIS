"""GitHub release-check helpers for ALBIS update notifications.

Finding out that a newer release exists is the easy half. The half that used to
be left to the user was working out what to *do* about it: the releases page
lists an AppImage, an AppImage bundle, a Linux tarball, a Windows installer, a
Windows portable zip, two macOS disk images and a signed checksum list, and
none of them is labelled "yours". So the check also resolves, for the way this
copy of ALBIS was installed, either the one asset to download or the one
command to run -- see `backend/install_kind.py` for how that is detected.

Nothing here downloads or installs anything. The user still clicks the link and
runs the installer themselves, which keeps the promise in
`docs/NETWORK_AND_PRIVACY.md` intact: one unsolicited request, and every byte
after it is something the user asked for.
"""

from __future__ import annotations

import json
import os
import ssl
import threading
import time
import urllib.request
from dataclasses import dataclass
from logging import Logger
from urllib.error import URLError

try:
    import certifi
except ImportError:  # pragma: no cover - certifi is a declared dependency
    certifi = None

from ..api_models import UpdateCheckResponse
from ..install_kind import (
    INSTALL_KIND_APPIMAGE,
    INSTALL_KIND_DOCKER,
    INSTALL_KIND_MACOS_APP,
    INSTALL_KIND_SOURCE,
    INSTALL_KIND_WINDOWS_INSTALLER,
    INSTALL_KIND_WINDOWS_PORTABLE,
)

# Overrides the version the check compares against GitHub, and nothing else.
#
# The whole update flow is unreachable until a newer release exists, which makes
# it the one feature that cannot be exercised before it ships. Setting this to
# an older version makes the real check, the real asset matching, the real
# download and the real verification all run against the real latest release.
#
# Deliberately narrow: it does not touch `ALBIS_VERSION`, so the About dialog,
# the footer, export provenance and bug reports keep reporting the build that
# is actually running. Its use is logged at WARNING, so it can never be
# quietly in effect.
UPDATE_CHECK_VERSION_ENV = "ALBIS_UPDATE_CHECK_VERSION"

LATEST_RELEASE_API_URL = "https://api.github.com/repos/SaschaAndresGrimm/ALBIS/releases/latest"
RELEASES_PAGE_URL = "https://github.com/SaschaAndresGrimm/ALBIS/releases"
REQUEST_TIMEOUT_SECONDS = 3.0
CACHE_TTL_SECONDS = 300.0

# The published container image, from `.github/workflows/docker.yml`.
DOCKER_IMAGE = "ghcr.io/saschaandresgrimm/albis"

# A release asset download only ever comes from GitHub. The URL arrives inside
# a JSON body, so it is treated as data to be checked rather than a location to
# be trusted: a link handed to the interface gets opened in the user's browser,
# and that is not something a malformed or tampered payload should get to aim.
ALLOWED_ASSET_URL_PREFIX = "https://github.com/"

# The AppImage is the one asset named with the kernel's architecture spelling
# rather than the release vocabulary, because `appimagetool` requires it.
# `scripts/version_info.py` does the same mapping when it builds the filename.
_APPIMAGE_ARCH_TOKENS = {"x64": "x86_64", "arm64": "aarch64"}


def _ssl_context() -> ssl.SSLContext | None:
    """Return an SSL context backed by the certifi CA bundle.

    Packaged (PyInstaller) builds ship their own Python without access to the
    system trust store, so HTTPS to api.github.com fails with
    CERTIFICATE_VERIFY_FAILED unless we point verification at certifi's bundle.
    Falls back to the default context when certifi is unavailable.
    """
    if certifi is not None:
        try:
            return ssl.create_default_context(cafile=certifi.where())
        except Exception:  # pragma: no cover - defensive fallback
            return None
    return None


@dataclass(frozen=True)
class ReleaseAsset:
    name: str
    download_url: str


@dataclass(frozen=True)
class ReleaseMetadata:
    version: str
    release_url: str
    # Empty for a release with no usable assets, and for any older caller that
    # builds a `ReleaseMetadata` without them. Either way the interface falls
    # back to the releases page, which is what it did before assets existed.
    assets: tuple[ReleaseAsset, ...] = ()


def _parse_assets(raw_assets: object) -> tuple[ReleaseAsset, ...]:
    """Pull `(name, url)` out of a GitHub `assets[]` array, skipping junk.

    A single malformed entry must not cost the user the whole update
    notification, so entries are dropped individually rather than raising.
    """
    if not isinstance(raw_assets, list):
        return ()
    assets: list[ReleaseAsset] = []
    for entry in raw_assets:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name") or "").strip()
        url = str(entry.get("browser_download_url") or "").strip()
        if not name or not url.startswith(ALLOWED_ASSET_URL_PREFIX):
            continue
        assets.append(ReleaseAsset(name=name, download_url=url))
    return tuple(assets)


def _asset_match_specs(
    install_kind: str, target_arch: str
) -> tuple[tuple[str, tuple[str, ...]], ...]:
    """`(suffix, required name tokens)` the running install can actually use.

    The architecture token is always required. A user offered a binary for the
    wrong architecture is worse off than a user offered no binary at all, so an
    unrecognised architecture -- which `read_target_arch` returns verbatim --
    deliberately matches nothing and leaves only the releases-page link.

    Suffix matching is also what keeps the detached `.sig` signatures and
    `SHA256SUMS.txt` out of the result: `ALBIS-...AppImage.sig` does not end in
    `.appimage`.
    """
    if not target_arch:
        return ()
    if install_kind == INSTALL_KIND_APPIMAGE:
        arch = _APPIMAGE_ARCH_TOKENS.get(target_arch, target_arch)
        return ((".appimage", (arch,)),)
    if install_kind == INSTALL_KIND_WINDOWS_INSTALLER:
        return ((".exe", ("albis-setup-", f"windows-{target_arch}")),)
    if install_kind == INSTALL_KIND_WINDOWS_PORTABLE:
        return ((".zip", (f"windows-{target_arch}",)),)
    if install_kind == INSTALL_KIND_MACOS_APP:
        return ((".dmg", (f"macos-{target_arch}",)),)
    # Docker and source checkouts are updated by a command, not a file.
    return ()


def select_download_asset(
    assets: tuple[ReleaseAsset, ...], install_kind: str, target_arch: str
) -> ReleaseAsset | None:
    """The one asset to offer this install, or `None` to fall back to the page."""
    for suffix, tokens in _asset_match_specs(install_kind, target_arch):
        for asset in assets:
            lowered = asset.name.lower()
            if lowered.endswith(suffix) and all(token in lowered for token in tokens):
                return asset
    return None


def update_command_for(install_kind: str, latest_version: str) -> str:
    """The shell line that updates an install a download cannot.

    A container cannot replace its own image and a checkout is the user's own
    working tree, so for those two the useful thing to put in front of someone
    is the exact command, ready to copy, rather than a file.
    """
    if install_kind == INSTALL_KIND_DOCKER:
        version = str(latest_version or "").strip()
        tag = f"v{version}" if version else "latest"
        return f"docker pull {DOCKER_IMAGE}:{tag}"
    if install_kind == INSTALL_KIND_SOURCE:
        return "git pull && pip install -r backend/requirements.txt"
    return ""


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


def _compare_prerelease_identifiers(
    left: tuple[int | str, ...], right: tuple[int | str, ...]
) -> int:
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


def _resolve_current_version(current_version: str, logger: Logger) -> str:
    """The version the check compares, honouring the testing override.

    A value that is not a version ALBIS can parse is ignored rather than
    obeyed: a typo in the variable must not turn the update check off.
    """
    configured = str(current_version or "0.0.0")
    override = str(os.environ.get(UPDATE_CHECK_VERSION_ENV, "")).strip()
    if not override:
        return configured
    try:
        _parse_version(override)
    except ValueError:
        logger.warning(
            "Ignoring %s=%r: not a version ALBIS can parse", UPDATE_CHECK_VERSION_ENV, override
        )
        return configured
    logger.warning(
        "Update check is comparing against %s instead of %s (%s is set)",
        override,
        configured,
        UPDATE_CHECK_VERSION_ENV,
    )
    return override


class ReleaseCheckService:
    def __init__(
        self,
        current_version: str,
        logger: Logger,
        cache_ttl_seconds: float = CACHE_TTL_SECONDS,
        install_kind: str = INSTALL_KIND_SOURCE,
        target_arch: str = "",
    ) -> None:
        self.current_version = _resolve_current_version(current_version, logger)
        # Detected once at start rather than per request: neither how ALBIS was
        # installed nor which CPU it runs on changes while it is running.
        self.install_kind = str(install_kind or INSTALL_KIND_SOURCE)
        self.target_arch = str(target_arch or "")
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
            # Only an actual update gets a download or a command. Handing
            # someone the installer for the version they are already running
            # invites them to reinstall it for no reason.
            asset = (
                select_download_asset(release.assets, self.install_kind, self.target_arch)
                if status == "update_available"
                else None
            )
            command = (
                update_command_for(self.install_kind, release.version)
                if status == "update_available"
                else ""
            )
            response = UpdateCheckResponse(
                status=status,
                current_version=self.current_version,
                latest_version=release.version,
                release_url=release.release_url or RELEASES_PAGE_URL,
                message="",
                install_kind=self.install_kind,
                download_url=asset.download_url if asset else "",
                download_name=asset.name if asset else "",
                update_command=command,
            )
        except Exception as exc:
            self._logger.warning("Update check failed: %s", exc)
            response = UpdateCheckResponse(
                status="unavailable",
                current_version=self.current_version,
                latest_version="",
                release_url=RELEASES_PAGE_URL,
                message=self._user_message_for_exception(exc),
                install_kind=self.install_kind,
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
        with urllib.request.urlopen(
            request, timeout=REQUEST_TIMEOUT_SECONDS, context=_ssl_context()
        ) as response:
            payload = json.load(response)

        if not isinstance(payload, dict):
            raise ValueError("Invalid GitHub release payload")

        tag_name = str(payload.get("tag_name") or "").strip()
        if not tag_name:
            raise ValueError("Missing GitHub release tag")

        release_url = str(payload.get("html_url") or RELEASES_PAGE_URL).strip() or RELEASES_PAGE_URL
        return ReleaseMetadata(
            version=_normalize_version_token(tag_name),
            release_url=release_url,
            assets=_parse_assets(payload.get("assets")),
        )

    def _user_message_for_exception(self, exc: Exception) -> str:
        if isinstance(exc, ValueError):
            return "GitHub release metadata was invalid."
        if isinstance(exc, TimeoutError):
            return "GitHub release metadata timed out."
        if isinstance(exc, URLError) and isinstance(getattr(exc, "reason", None), TimeoutError):
            return "GitHub release metadata timed out."
        return "GitHub release metadata was unavailable."
