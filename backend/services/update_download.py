"""Download a release asset and check it before handing it to the user.

The update notification can already name the one file that fits this install
(`update_check.py`). This fetches it, and the reason for fetching it here
rather than leaving it to the browser is not convenience -- it is that nobody
verifies a browser download. A truncated 180 MB disk image looks exactly like a
complete one until it fails to mount, and the `SHA256SUMS.txt` published beside
every release is a file almost nobody opens.

So: stream the asset, hash it while streaming, compare against the release's
own checksum list, and only then present it. A mismatch deletes the file and
says so, because a half-downloaded installer that the user runs anyway is worse
than no download at all.

Three deliberate limits:

* **Nothing starts without a click.** There is no background downloader and no
  polling; `start()` runs only from a request the user made.
* **A missing checksum list is reported, not fatal.** Releases before
  `SHA256SUMS.txt` existed, and a file that ALBIS cannot vouch for is exactly
  as good as the browser download it replaces -- so it is handed over with the
  verification state saying `unavailable` rather than withheld.
* **A wrong checksum is fatal.** That is the case the feature exists for.
* **A checksum list whose signature does not verify is also fatal**, because
  then the checksum proves nothing: whoever could substitute the list could
  substitute the digest in it too. Being *unable* to check the signature is a
  different thing and is only reported. See `_verify_signature`.
"""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import urllib.request
from dataclasses import dataclass, field
from logging import Logger
from pathlib import Path

from .update_check import ALLOWED_ASSET_URL_PREFIX, _ssl_context

# Release assets run to a few hundred megabytes. The cap is not a guess at the
# largest legitimate asset but a ceiling on what a redirected or substituted
# response can write to the user's disk before being cut off.
MAX_DOWNLOAD_BYTES = 1_500_000_000

# Generous, because this is a large file on a beamline network, but finite: a
# stalled connection must not leave the dialog reading "downloading" forever.
DOWNLOAD_TIMEOUT_SECONDS = 60.0
CHECKSUMS_TIMEOUT_SECONDS = 15.0

CHUNK_BYTES = 1024 * 256

CHECKSUMS_NAME = "SHA256SUMS.txt"
SIGNATURE_SUFFIX = ".sig"

# The public key that signs `SHA256SUMS.txt`, bundled beside `VERSION` the way
# `BUILD_COMMIT` is. Absent from a source checkout and from any build that did
# not ship one, in which case signature verification reports `unavailable`.
SIGNING_KEY_NAME = "SIGNING_KEY.asc"

# An asset filename is used to build a path on disk, so it is validated rather
# than sanitised: letters, digits, dot, underscore, plus and hyphen only. That
# excludes every path separator, `..`, NUL, and the leading dot that would hide
# the file -- anything else is refused instead of being repaired into something
# that looks close enough.
_ASSET_NAME_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]{0,127}")

_CHECKSUM_LINE_RE = re.compile(r"^([0-9a-fA-F]{64})\s+\*?(\S.*)$")

STATUS_IDLE = "idle"
STATUS_DOWNLOADING = "downloading"
STATUS_VERIFYING = "verifying"
STATUS_READY = "ready"
STATUS_FAILED = "failed"
STATUS_CANCELLED = "cancelled"

CHECKSUM_PENDING = "pending"
CHECKSUM_VERIFIED = "verified"
CHECKSUM_MISMATCH = "mismatch"
CHECKSUM_UNAVAILABLE = "unavailable"

SIGNATURE_PENDING = "pending"
SIGNATURE_VERIFIED = "verified"
SIGNATURE_INVALID = "invalid"
SIGNATURE_UNAVAILABLE = "unavailable"


class DownloadRefusedError(Exception):
    """The request itself is not something this service will act on."""


class DownloadFailedError(Exception):
    """The download or its verification did not succeed."""


@dataclass
class DownloadState:
    status: str = STATUS_IDLE
    name: str = ""
    path: str = ""
    bytes_downloaded: int = 0
    bytes_total: int = 0
    sha256: str = ""
    checksum: str = CHECKSUM_PENDING
    signature: str = SIGNATURE_PENDING
    message: str = ""

    def as_payload(self) -> dict[str, object]:
        return {
            "status": self.status,
            "name": self.name,
            "path": self.path,
            "bytes_downloaded": int(self.bytes_downloaded),
            "bytes_total": int(self.bytes_total),
            "sha256": self.sha256,
            "checksum": self.checksum,
            "signature": self.signature,
            "message": self.message,
        }


@dataclass
class _Job:
    url: str
    name: str
    cancel: threading.Event = field(default_factory=threading.Event)


class _HttpsOnlyRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Follow GitHub's redirect to its asset host, but only ever over HTTPS.

    A release download redirects to `objects.githubusercontent.com`, so
    redirects cannot simply be refused. What can be refused is a redirect that
    downgrades the transport: without this, a 302 to `http://` would be
    followed silently and the bytes ALBIS is about to write to the user's disk
    would arrive unauthenticated.
    """

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[override]
        if not str(newurl).lower().startswith("https://"):
            raise DownloadFailedError("Refused a redirect that was not HTTPS")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _build_opener() -> urllib.request.OpenerDirector:
    """An opener that verifies TLS and refuses to be downgraded.

    The SSL context has to be installed as a handler rather than passed to
    `open()`: `urlopen` takes a `context` keyword, `OpenerDirector.open` does
    not, and a custom redirect handler means an opener is required. Getting
    this wrong fails only against a real server, which is why
    `tests/test_update_download_service.py` models `open()` without the keyword.
    """
    return urllib.request.build_opener(
        urllib.request.HTTPSHandler(context=_ssl_context()),
        _HttpsOnlyRedirectHandler(),
    )


def validate_asset_name(raw: str) -> str:
    """Return `raw` if it is usable as a bare filename, else raise.

    The name arrives from a JSON payload and becomes a path under the download
    directory, so this is the boundary that keeps it one.
    """
    name = str(raw or "").strip()
    if not _ASSET_NAME_RE.fullmatch(name):
        raise DownloadRefusedError("Refused an asset name that is not a plain filename")
    return name


def validate_asset_url(raw: str) -> str:
    """Return `raw` if it is a GitHub release download, else raise."""
    url = str(raw or "").strip()
    if not url.startswith(ALLOWED_ASSET_URL_PREFIX):
        raise DownloadRefusedError("Refused a download URL that is not a GitHub release asset")
    return url


def checksums_url_for(asset_url: str) -> str:
    """The release's checksum list, which always sits beside its assets.

    Derived from the asset URL rather than carried through the API: both are
    under `/releases/download/<tag>/`, so the list cannot end up pointing at a
    different release than the file it is checking.
    """
    base, separator, _name = asset_url.rpartition("/")
    if not separator or not base:
        raise DownloadFailedError("Could not locate the release checksum list")
    return validate_asset_url(f"{base}/{CHECKSUMS_NAME}")


def parse_checksums(text: str, name: str) -> str:
    """The lowercase SHA-256 recorded for `name`, or `""` if it is not listed."""
    for line in str(text or "").splitlines():
        match = _CHECKSUM_LINE_RE.match(line.strip())
        if not match:
            continue
        digest, listed_name = match.group(1), match.group(2).strip()
        # The list is generated with `sha256sum *` in a flat directory, so the
        # recorded name is bare; compare on the basename regardless.
        if Path(listed_name).name == name:
            return digest.lower()
    return ""


def default_download_dir() -> Path:
    """Where a downloaded release lands.

    The user's `Downloads` folder when there is one, because that is where they
    will look for it and where their browser would have put it. Otherwise the
    temporary directory, which is writable on every platform ALBIS ships to --
    an installer is not data worth keeping, so this never falls back to
    somewhere that would accumulate them silently.
    """
    try:
        candidate = Path.home() / "Downloads"
        if candidate.is_dir():
            return candidate
    except (OSError, RuntimeError):
        pass
    return Path(tempfile.gettempdir())


class UpdateDownloadService:
    """One download at a time, because there is only one ALBIS to update."""

    def __init__(
        self,
        logger: Logger,
        download_dir: Path | None = None,
        max_bytes: int = MAX_DOWNLOAD_BYTES,
    ) -> None:
        self._logger = logger
        self._download_dir = download_dir
        self._max_bytes = int(max_bytes)
        self._lock = threading.Lock()
        self._state = DownloadState()
        self._job: _Job | None = None
        self._thread: threading.Thread | None = None

    # -- state ------------------------------------------------------------
    def status(self) -> dict[str, object]:
        with self._lock:
            return self._state.as_payload()

    def reset(self) -> dict[str, object]:
        """Forget a finished download so the dialog can offer a fresh one."""
        with self._lock:
            if self._state.status in (STATUS_DOWNLOADING, STATUS_VERIFYING):
                return self._state.as_payload()
            self._state = DownloadState()
            self._job = None
            return self._state.as_payload()

    def cancel(self) -> dict[str, object]:
        with self._lock:
            job = self._job
            if job is None or self._state.status not in (STATUS_DOWNLOADING, STATUS_VERIFYING):
                return self._state.as_payload()
            job.cancel.set()
            return self._state.as_payload()

    def download_dir(self) -> Path:
        return self._download_dir or default_download_dir()

    def ready_path(self) -> Path | None:
        """The verified file, if one is waiting. Never a path from the client."""
        with self._lock:
            if self._state.status != STATUS_READY or not self._state.path:
                return None
            candidate = Path(self._state.path)
        return candidate if candidate.is_file() else None

    # -- start ------------------------------------------------------------
    def start(self, url: str, name: str) -> dict[str, object]:
        safe_url = validate_asset_url(url)
        safe_name = validate_asset_name(name)

        with self._lock:
            if self._state.status in (STATUS_DOWNLOADING, STATUS_VERIFYING):
                raise DownloadRefusedError("A download is already in progress")
            job = _Job(url=safe_url, name=safe_name)
            self._job = job
            self._state = DownloadState(status=STATUS_DOWNLOADING, name=safe_name)
            payload = self._state.as_payload()

        thread = threading.Thread(
            target=self._run, args=(job,), name="albis-update-download", daemon=True
        )
        self._thread = thread
        thread.start()
        return payload

    # -- worker -----------------------------------------------------------
    def _run(self, job: _Job) -> None:
        partial: Path | None = None
        try:
            directory = self.download_dir()
            directory.mkdir(parents=True, exist_ok=True)
            target = directory / job.name
            partial = directory / f"{job.name}.part"

            digest = self._stream_to_file(job, partial)
            if job.cancel.is_set():
                self._finish_cancelled(partial)
                return

            self._set(status=STATUS_VERIFYING, sha256=digest)
            checksum_state, signature_state, message = self._verify(job, partial, digest)

            # Both states are reported either way, so the reason stays precise
            # -- but an invalid signature is as fatal as a wrong checksum. A
            # digest that matches a list ALBIS cannot trust is not evidence.
            if checksum_state == CHECKSUM_MISMATCH or signature_state == SIGNATURE_INVALID:
                self._discard(partial)
                self._set(
                    status=STATUS_FAILED,
                    checksum=checksum_state,
                    signature=signature_state,
                    path="",
                    message=message,
                )
                return

            os.replace(partial, target)
            partial = None
            self._set(
                status=STATUS_READY,
                path=str(target),
                checksum=checksum_state,
                signature=signature_state,
                message=message,
            )
        except DownloadRefusedError as exc:
            self._discard(partial)
            self._set(status=STATUS_FAILED, message=str(exc))
        except Exception as exc:
            self._logger.warning("Update download failed: %s", exc)
            self._discard(partial)
            self._set(
                status=STATUS_FAILED,
                message="The download did not complete. See the log for details.",
            )

    def _stream_to_file(self, job: _Job, partial: Path) -> str:
        request = urllib.request.Request(
            job.url, headers={"Accept": "application/octet-stream", "User-Agent": "ALBIS"}
        )
        opener = _build_opener()
        hasher = hashlib.sha256()
        written = 0

        with opener.open(request, timeout=DOWNLOAD_TIMEOUT_SECONDS) as response:
            total = self._content_length(response)
            self._set(bytes_total=total)
            with partial.open("wb") as handle:
                while True:
                    if job.cancel.is_set():
                        return hasher.hexdigest()
                    chunk = response.read(CHUNK_BYTES)
                    if not chunk:
                        break
                    written += len(chunk)
                    if written > self._max_bytes:
                        raise DownloadFailedError("The download exceeded the maximum allowed size")
                    handle.write(chunk)
                    hasher.update(chunk)
                    self._set(bytes_downloaded=written)

        # A truncated response would fail the checksum anyway, but "the
        # connection ended early" is a more useful thing to read than "checksum
        # mismatch" -- and it distinguishes a bad network from a bad file.
        if total and written < total:
            raise DownloadFailedError("The connection ended before the file was complete")
        return hasher.hexdigest()

    def _content_length(self, response: urllib.request.addinfourl) -> int:
        """The advertised size, or 0 when it is missing or not believable.

        Only ever used to draw a progress bar, so an absent or absurd value
        costs the bar its percentage and nothing else.
        """
        try:
            value = int(str(response.headers.get("Content-Length")).strip())
        except (AttributeError, TypeError, ValueError):
            return 0
        if value < 0 or value > self._max_bytes:
            return 0
        return value

    # -- verification -----------------------------------------------------
    def _verify(self, job: _Job, partial: Path, digest: str) -> tuple[str, str, str]:
        try:
            checksums = self._fetch_text(checksums_url_for(job.url))
        except Exception as exc:
            self._logger.warning("Update checksum list unavailable: %s", exc)
            return (
                CHECKSUM_UNAVAILABLE,
                SIGNATURE_UNAVAILABLE,
                "This release publishes no checksum list, so the download could not be verified.",
            )

        expected = parse_checksums(checksums, job.name)
        if not expected:
            return (
                CHECKSUM_UNAVAILABLE,
                SIGNATURE_UNAVAILABLE,
                "The release checksum list does not cover this file.",
            )
        if expected != digest:
            return (
                CHECKSUM_MISMATCH,
                SIGNATURE_UNAVAILABLE,
                "The download does not match the checksum published for it and was deleted.",
            )

        signature_state, signature_message = self._verify_signature(job, checksums)
        return CHECKSUM_VERIFIED, signature_state, signature_message

    def _verify_signature(self, job: _Job, checksums: str) -> tuple[str, str]:
        """Check the GPG signature over the checksum list, where that is possible.

        This is the weaker of the two checks in reach, and only because of what
        is available at runtime. It needs `gpg` on PATH and the bundled public
        key: on Linux, where nothing else vouches for an AppImage, both are the
        normal case. On macOS and Windows neither is, and neither is needed --
        the OS checks notarization and Authenticode itself when the user opens
        the installer, which is a stronger guarantee than this one.

        Being unable to check -- no key, no `gpg`, no published signature -- is
        reported and nothing more. A signature that is present and does *not*
        verify is the opposite: it is the one outcome that looks like
        tampering, and it fails the download outright, because a checksum taken
        from a list ALBIS cannot trust is not evidence of anything.

        The operational cost of that choice is a key rotation done without
        re-exporting the bundled key, which would report `invalid` for everyone
        -- see `docs/RELEASE_CHECKLIST.md`. The alternative is worse: handing
        over a possibly substituted installer labelled "checksum verified".
        """
        key_path = bundled_signing_key()
        if key_path is None:
            return SIGNATURE_UNAVAILABLE, ""
        gpg = shutil.which("gpg")
        if not gpg:
            return SIGNATURE_UNAVAILABLE, ""

        try:
            signature = self._fetch_bytes(checksums_url_for(job.url) + SIGNATURE_SUFFIX)
        except Exception as exc:
            self._logger.warning("Update checksum signature unavailable: %s", exc)
            return SIGNATURE_UNAVAILABLE, ""

        try:
            verified = verify_detached_signature(
                gpg_path=gpg,
                public_key=key_path.read_bytes(),
                signed_data=checksums.encode("utf-8"),
                signature=signature,
            )
        except Exception as exc:
            self._logger.warning("Update signature check could not run: %s", exc)
            return SIGNATURE_UNAVAILABLE, ""

        if verified:
            return SIGNATURE_VERIFIED, ""
        return (
            SIGNATURE_INVALID,
            "The release checksum list is not signed by the ALBIS release key, "
            "so the download could not be trusted and was deleted.",
        )

    def _fetch_bytes(self, url: str) -> bytes:
        request = urllib.request.Request(validate_asset_url(url), headers={"User-Agent": "ALBIS"})
        with _build_opener().open(request, timeout=CHECKSUMS_TIMEOUT_SECONDS) as response:
            # Small metadata files; the cap stops a substituted response from
            # being read into memory without limit.
            return response.read(4 * 1024 * 1024)

    def _fetch_text(self, url: str) -> str:
        return self._fetch_bytes(url).decode("utf-8", errors="replace")

    # -- helpers ----------------------------------------------------------
    def _finish_cancelled(self, partial: Path | None) -> None:
        self._discard(partial)
        self._set(status=STATUS_CANCELLED, path="", message="")

    def _discard(self, partial: Path | None) -> None:
        if partial is None:
            return
        try:
            partial.unlink(missing_ok=True)
        except OSError:
            self._logger.warning("Could not remove the partial download")

    def _set(self, **changes: object) -> None:
        with self._lock:
            for key, value in changes.items():
                setattr(self._state, key, value)


def bundled_signing_key() -> Path | None:
    """The release signing key shipped with this build, if it ships one.

    Mirrors `version.py` and `build_info.py`: the PyInstaller bundle first,
    then the checkout. A build with no key simply cannot check signatures and
    says so, rather than pretending to.
    """
    candidates: list[Path] = []
    pyinstaller_root = getattr(sys, "_MEIPASS", "")
    if pyinstaller_root:
        candidates.append(Path(pyinstaller_root) / SIGNING_KEY_NAME)
    candidates.append(Path(__file__).resolve().parents[2] / SIGNING_KEY_NAME)
    for candidate in dict.fromkeys(candidates):
        try:
            if candidate.is_file() and candidate.stat().st_size > 0:
                return candidate
        except OSError:
            continue
    return None


def verify_detached_signature(
    *, gpg_path: str, public_key: bytes, signed_data: bytes, signature: bytes
) -> bool:
    """Verify `signature` over `signed_data` using only `public_key`.

    Runs in a throwaway `GNUPGHOME` holding nothing but the bundled key, so the
    answer cannot come from a key in the user's own keyring: a signature ALBIS
    calls valid has to be valid under the key ALBIS shipped.
    """
    with tempfile.TemporaryDirectory(prefix="albis-verify-") as work_dir:
        work = Path(work_dir)
        home = work / "gnupg"
        home.mkdir(mode=0o700)
        key_file = work / "key.asc"
        data_file = work / "data"
        sig_file = work / "data.sig"
        key_file.write_bytes(public_key)
        data_file.write_bytes(signed_data)
        sig_file.write_bytes(signature)

        env = {
            "GNUPGHOME": str(home),
            "PATH": os.environ.get("PATH", ""),
            "LC_ALL": "C",
        }
        imported = subprocess.run(
            [gpg_path, "--batch", "--no-tty", "--import", str(key_file)],
            capture_output=True,
            env=env,
            timeout=30,
            check=False,
        )
        if imported.returncode != 0:
            raise DownloadFailedError("Could not import the bundled signing key")

        verified = subprocess.run(
            [gpg_path, "--batch", "--no-tty", "--verify", str(sig_file), str(data_file)],
            capture_output=True,
            env=env,
            timeout=30,
            check=False,
        )
        return verified.returncode == 0
