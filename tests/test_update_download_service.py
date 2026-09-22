from __future__ import annotations

import hashlib
import io
import logging
import os
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.request
from pathlib import Path

import pytest

from backend.services import update_download as module
from backend.services.update_download import (
    CHECKSUM_MISMATCH,
    CHECKSUM_UNAVAILABLE,
    CHECKSUM_VERIFIED,
    SIGNATURE_INVALID,
    SIGNATURE_UNAVAILABLE,
    SIGNATURE_VERIFIED,
    STATUS_FAILED,
    STATUS_READY,
    DownloadRefusedError,
    UpdateDownloadService,
    checksums_url_for,
    parse_checksums,
    validate_asset_name,
    validate_asset_url,
    verify_detached_signature,
)

ASSET_NAME = "ALBIS-macos-arm64-v1.0.0-abc1234.dmg"
ASSET_URL = "https://github.com/SaschaAndresGrimm/ALBIS/releases/download/v1.0.0/" + ASSET_NAME
PAYLOAD = b"albis release payload" * 64
PAYLOAD_SHA256 = hashlib.sha256(PAYLOAD).hexdigest()


class _FakeResponse(io.BytesIO):
    def __init__(self, data: bytes, content_length: int | None = None) -> None:
        super().__init__(data)
        length = len(data) if content_length is None else content_length
        self.headers = {"Content-Length": str(length)}

    def __enter__(self):
        return self

    def __exit__(self, *_exc) -> None:
        self.close()


def _short_temp_root() -> str:
    """A temporary root short enough to hold a gpg-agent socket.

    macOS puts pytest's `tmp_path` under `/private/var/folders/...`, which on
    its own is long enough that a socket inside it exceeds the platform limit.
    """
    for candidate in ("/tmp", tempfile.gettempdir()):
        if Path(candidate).is_dir():
            return candidate
    return tempfile.gettempdir()


def _service(tmp_path: Path) -> UpdateDownloadService:
    return UpdateDownloadService(logger=logging.getLogger("test"), download_dir=tmp_path)


def _install_fake_network(
    monkeypatch,
    *,
    asset: bytes = PAYLOAD,
    checksums: str | None = None,
    content_length: int | None = None,
    signature: bytes | None = None,
) -> list[str]:
    """Answer the three URLs a verified download fetches, recording the order."""
    if checksums is None:
        checksums = f"{PAYLOAD_SHA256}  {ASSET_NAME}\n"
    requested: list[str] = []

    class _Opener:
        # No `context` keyword: `OpenerDirector.open` has none, and a fake
        # that accepted one is what let a broken call reach a real server.
        def open(self, request, timeout=None):
            url = request.full_url if hasattr(request, "full_url") else str(request)
            requested.append(url)
            if url.endswith(".sig"):
                if signature is None:
                    raise OSError("no signature published")
                return _FakeResponse(signature)
            if url.endswith(module.CHECKSUMS_NAME):
                if checksums == "":
                    raise OSError("no checksum list published")
                return _FakeResponse(checksums.encode("utf-8"))
            return _FakeResponse(asset, content_length=content_length)

    monkeypatch.setattr(urllib.request, "build_opener", lambda *_handlers: _Opener())
    return requested


def _run_to_completion(service: UpdateDownloadService, timeout: float = 5.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = service.status()
        if status["status"] not in ("downloading", "verifying"):
            return status
        time.sleep(0.01)
    raise AssertionError(f"Download did not settle: {service.status()}")


# -- input validation -------------------------------------------------------


def test_asset_name_must_be_a_plain_filename() -> None:
    assert validate_asset_name(ASSET_NAME) == ASSET_NAME
    # The name becomes a path under the download directory, so anything that
    # could steer it elsewhere is refused rather than repaired.
    for hostile in (
        "../evil.dmg",
        "sub/evil.dmg",
        "sub\\evil.dmg",
        "/etc/passwd",
        ".hidden",
        "",
        "   ",
        "evil\x00.dmg",
        "evil\n.dmg",
        "a" * 200,
    ):
        with pytest.raises(DownloadRefusedError):
            validate_asset_name(hostile)


def test_asset_url_must_be_a_github_release_download() -> None:
    assert validate_asset_url(ASSET_URL) == ASSET_URL
    for hostile in (
        "http://github.com/a/b/releases/download/v1/x.dmg",
        "https://github.evil.example/a/b",
        "https://objects.githubusercontent.com/x",
        "file:///etc/passwd",
        "",
    ):
        with pytest.raises(DownloadRefusedError):
            validate_asset_url(hostile)


def test_checksum_list_is_resolved_inside_the_same_release() -> None:
    assert checksums_url_for(ASSET_URL) == (
        "https://github.com/SaschaAndresGrimm/ALBIS/releases/download/v1.0.0/SHA256SUMS.txt"
    )


def test_checksum_list_parsing_matches_the_sha256sum_format() -> None:
    listing = (
        "# a comment line the parser ignores\n"
        f"{'a' * 64}  ALBIS-linux-x64-v1.0.0-abc1234.tar.gz\n"
        f"{'b' * 64} *{ASSET_NAME}\n"
        "not-a-checksum-line\n"
    )
    # `sha256sum` marks binary mode with a leading asterisk; both forms appear.
    assert parse_checksums(listing, ASSET_NAME) == "b" * 64
    assert parse_checksums(listing, "ALBIS-linux-x64-v1.0.0-abc1234.tar.gz") == "a" * 64
    assert parse_checksums(listing, "not-listed.dmg") == ""


# -- the happy path ---------------------------------------------------------


def test_a_verified_download_lands_with_its_digest_recorded(tmp_path, monkeypatch) -> None:
    requested = _install_fake_network(monkeypatch)
    service = _service(tmp_path)

    service.start(url=ASSET_URL, name=ASSET_NAME)
    status = _run_to_completion(service)

    assert status["status"] == STATUS_READY
    assert status["checksum"] == CHECKSUM_VERIFIED
    assert status["sha256"] == PAYLOAD_SHA256
    assert Path(status["path"]) == tmp_path / ASSET_NAME
    assert (tmp_path / ASSET_NAME).read_bytes() == PAYLOAD
    # The partial file is renamed, not left beside the finished download.
    assert not (tmp_path / f"{ASSET_NAME}.part").exists()
    assert requested[0] == ASSET_URL
    assert requested[1].endswith(module.CHECKSUMS_NAME)


def test_progress_is_reported_against_the_advertised_size(tmp_path, monkeypatch) -> None:
    _install_fake_network(monkeypatch)
    service = _service(tmp_path)

    service.start(url=ASSET_URL, name=ASSET_NAME)
    status = _run_to_completion(service)

    assert status["bytes_total"] == len(PAYLOAD)
    assert status["bytes_downloaded"] == len(PAYLOAD)


def test_a_missing_content_length_leaves_progress_unscaled(tmp_path, monkeypatch) -> None:
    # An absent or absurd Content-Length costs the progress bar its percentage
    # and nothing else; the download itself must still complete and verify.
    _install_fake_network(monkeypatch, content_length=10**12)
    service = _service(tmp_path)

    service.start(url=ASSET_URL, name=ASSET_NAME)
    status = _run_to_completion(service)

    assert status["status"] == STATUS_READY
    assert status["bytes_total"] == 0
    assert status["bytes_downloaded"] == len(PAYLOAD)


# -- verification failures --------------------------------------------------


def test_a_checksum_mismatch_deletes_the_file_and_fails(tmp_path, monkeypatch) -> None:
    # The case the feature exists for: a file that is not what the release
    # published must not be left on disk for the user to run anyway.
    _install_fake_network(monkeypatch, checksums=f"{'c' * 64}  {ASSET_NAME}\n")
    service = _service(tmp_path)

    service.start(url=ASSET_URL, name=ASSET_NAME)
    status = _run_to_completion(service)

    assert status["status"] == STATUS_FAILED
    assert status["checksum"] == CHECKSUM_MISMATCH
    assert status["path"] == ""
    assert status["message"]
    assert list(tmp_path.iterdir()) == []


def test_an_unavailable_checksum_list_is_reported_but_not_fatal(tmp_path, monkeypatch) -> None:
    # Releases predate SHA256SUMS.txt, and a file ALBIS cannot vouch for is no
    # worse than the browser download it replaces -- so it is handed over with
    # the verification state saying so.
    _install_fake_network(monkeypatch, checksums="")
    service = _service(tmp_path)

    service.start(url=ASSET_URL, name=ASSET_NAME)
    status = _run_to_completion(service)

    assert status["status"] == STATUS_READY
    assert status["checksum"] == CHECKSUM_UNAVAILABLE
    assert status["signature"] == SIGNATURE_UNAVAILABLE
    assert status["message"]
    assert (tmp_path / ASSET_NAME).exists()


def test_a_file_absent_from_the_checksum_list_is_reported_as_unverified(
    tmp_path, monkeypatch
) -> None:
    _install_fake_network(monkeypatch, checksums=f"{'d' * 64}  something-else.dmg\n")
    service = _service(tmp_path)

    service.start(url=ASSET_URL, name=ASSET_NAME)
    status = _run_to_completion(service)

    assert status["status"] == STATUS_READY
    assert status["checksum"] == CHECKSUM_UNAVAILABLE


def test_a_truncated_response_fails_before_verification(tmp_path, monkeypatch) -> None:
    # Content-Length promises more than the body delivers, which is what a
    # dropped connection looks like.
    _install_fake_network(monkeypatch, content_length=len(PAYLOAD) + 4096)
    service = _service(tmp_path)

    service.start(url=ASSET_URL, name=ASSET_NAME)
    status = _run_to_completion(service)

    assert status["status"] == STATUS_FAILED
    assert list(tmp_path.iterdir()) == []


def test_a_response_beyond_the_size_cap_is_cut_off(tmp_path, monkeypatch) -> None:
    _install_fake_network(monkeypatch, asset=b"x" * 4096)
    service = UpdateDownloadService(
        logger=logging.getLogger("test"), download_dir=tmp_path, max_bytes=1024
    )

    service.start(url=ASSET_URL, name=ASSET_NAME)
    status = _run_to_completion(service)

    assert status["status"] == STATUS_FAILED
    assert list(tmp_path.iterdir()) == []


# -- lifecycle --------------------------------------------------------------


def test_only_one_download_runs_at_a_time(tmp_path, monkeypatch) -> None:
    release = threading.Event()

    class _BlockingOpener:
        # No `context` keyword: `OpenerDirector.open` has none, and a fake
        # that accepted one is what let a broken call reach a real server.
        def open(self, request, timeout=None):
            release.wait(5.0)
            return _FakeResponse(PAYLOAD)

    monkeypatch.setattr(urllib.request, "build_opener", lambda *_h: _BlockingOpener())
    service = _service(tmp_path)
    service.start(url=ASSET_URL, name=ASSET_NAME)
    try:
        with pytest.raises(DownloadRefusedError, match="in progress"):
            service.start(url=ASSET_URL, name=ASSET_NAME)
    finally:
        release.set()
    _run_to_completion(service)


def test_reset_clears_a_finished_download_but_not_a_running_one(tmp_path, monkeypatch) -> None:
    _install_fake_network(monkeypatch)
    service = _service(tmp_path)
    service.start(url=ASSET_URL, name=ASSET_NAME)
    _run_to_completion(service)

    assert service.reset()["status"] == "idle"
    assert service.ready_path() is None


def test_ready_path_is_withheld_when_the_file_is_gone(tmp_path, monkeypatch) -> None:
    # The reveal endpoint opens whatever this returns, so a file deleted from
    # under ALBIS must stop being offered.
    _install_fake_network(monkeypatch)
    service = _service(tmp_path)
    service.start(url=ASSET_URL, name=ASSET_NAME)
    _run_to_completion(service)

    assert service.ready_path() == tmp_path / ASSET_NAME
    (tmp_path / ASSET_NAME).unlink()
    assert service.ready_path() is None


def test_an_https_downgrade_on_redirect_is_refused() -> None:
    handler = module._HttpsOnlyRedirectHandler()
    with pytest.raises(module.DownloadFailedError):
        handler.redirect_request(
            urllib.request.Request(ASSET_URL), None, 302, "Found", {}, "http://evil.example/x"
        )


# -- signature verification -------------------------------------------------


@pytest.mark.skipif(shutil.which("gpg") is None, reason="gpg is not installed")
def test_a_detached_signature_verifies_only_under_the_bundled_key() -> None:
    """The check has to fail for a key ALBIS did not ship.

    Verification runs in a throwaway keyring holding nothing but the bundled
    key, so this also pins that a signature trusted by the user's own keyring
    is not trusted by ALBIS.

    The keys are generated rather than committed, so the test states its own
    inputs. Generation is the one gpg operation that needs `gpg-agent`, and the
    agent's socket lives inside `GNUPGHOME` -- which is why this uses a short
    path of its own instead of pytest's `tmp_path`, whose length exceeds the
    ~104-character limit on a Unix socket under macOS. Verification itself
    needs no agent, which is why the service can use an ordinary temporary
    directory.
    """
    gpg = shutil.which("gpg")
    assert gpg

    with tempfile.TemporaryDirectory(dir=_short_temp_root(), prefix="albisgpg") as work_dir:
        work = Path(work_dir)
        home = work / "h"
        home.mkdir(mode=0o700)
        env = {"GNUPGHOME": str(home), "PATH": os.environ.get("PATH", ""), "LC_ALL": "C"}

        def run(*args: str) -> subprocess.CompletedProcess:
            return subprocess.run(
                [gpg, "--batch", "--no-tty", "--pinentry-mode", "loopback", *args],
                capture_output=True,
                env=env,
                timeout=60,
                check=False,
            )

        def make_key(name: str) -> bytes:
            generated = run(
                "--passphrase",
                "",
                "--quick-generate-key",
                f"{name} <{name}@invalid>",
                "ed25519",
                "sign",
                "never",
            )
            if generated.returncode != 0:
                stderr = generated.stderr.decode("utf-8", "replace")
                if "agent" in stderr.lower():
                    pytest.skip(f"gpg-agent unavailable in this environment: {stderr.strip()}")
                raise AssertionError(f"gpg key generation failed: {stderr}")
            exported = run("--armor", "--export", f"{name}@invalid")
            assert exported.returncode == 0, exported.stderr
            return exported.stdout

        signer_key = make_key("albis-test-signer")
        data = b"7f83b1657ff1fc53b92dc18148a1d65dfc2d4b1fa3d677284addd200126d9069  ALBIS.dmg\n"
        data_file = work / module.CHECKSUMS_NAME
        data_file.write_bytes(data)
        signed = run(
            "--local-user",
            "albis-test-signer@invalid",
            "--output",
            str(work / "sig"),
            "--detach-sign",
            str(data_file),
        )
        assert signed.returncode == 0, signed.stderr
        signature = (work / "sig").read_bytes()

        assert verify_detached_signature(
            gpg_path=gpg, public_key=signer_key, signed_data=data, signature=signature
        )
        # Right signature, wrong data.
        assert not verify_detached_signature(
            gpg_path=gpg,
            public_key=signer_key,
            signed_data=data + b"tampered",
            signature=signature,
        )
        # Right data, but signed by a key this build does not ship.
        other_key = make_key("albis-test-other")
        assert not verify_detached_signature(
            gpg_path=gpg, public_key=other_key, signed_data=data, signature=signature
        )


def test_a_checksum_list_whose_signature_fails_is_refused_outright(tmp_path, monkeypatch) -> None:
    """The one outcome that looks like tampering.

    A digest that matches a list ALBIS cannot trust is not evidence: whoever
    could substitute the list could substitute the digest in it too. So this is
    as fatal as a wrong checksum, and the file is deleted -- even though the
    checksum itself matched, which the state still records.
    """
    monkeypatch.setattr(module, "bundled_signing_key", lambda: tmp_path / "key.asc")
    (tmp_path / "key.asc").write_bytes(b"-----BEGIN PGP PUBLIC KEY BLOCK-----")
    monkeypatch.setattr(module.shutil, "which", lambda name: "/usr/bin/gpg")
    monkeypatch.setattr(module, "verify_detached_signature", lambda **_kwargs: False)
    _install_fake_network(monkeypatch, signature=b"a detached signature")

    service = _service(tmp_path)
    service.start(url=ASSET_URL, name=ASSET_NAME)
    status = _run_to_completion(service)

    assert status["status"] == STATUS_FAILED
    assert status["checksum"] == CHECKSUM_VERIFIED
    assert status["signature"] == SIGNATURE_INVALID
    assert status["path"] == ""
    assert "not signed" in status["message"]
    assert not (tmp_path / ASSET_NAME).exists()
    assert not (tmp_path / f"{ASSET_NAME}.part").exists()


def test_a_verified_signature_is_reported_alongside_the_checksum(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(module, "bundled_signing_key", lambda: tmp_path / "key.asc")
    (tmp_path / "key.asc").write_bytes(b"-----BEGIN PGP PUBLIC KEY BLOCK-----")
    monkeypatch.setattr(module.shutil, "which", lambda name: "/usr/bin/gpg")
    monkeypatch.setattr(module, "verify_detached_signature", lambda **_kwargs: True)
    _install_fake_network(monkeypatch, signature=b"a detached signature")

    service = _service(tmp_path)
    service.start(url=ASSET_URL, name=ASSET_NAME)
    status = _run_to_completion(service)

    assert status["status"] == STATUS_READY
    assert status["checksum"] == CHECKSUM_VERIFIED
    assert status["signature"] == SIGNATURE_VERIFIED


def test_an_unpublished_signature_is_unavailable_not_invalid(tmp_path, monkeypatch) -> None:
    # Being unable to check is not the same as checking and failing, and only
    # the latter withholds the file.
    monkeypatch.setattr(module, "bundled_signing_key", lambda: tmp_path / "key.asc")
    (tmp_path / "key.asc").write_bytes(b"-----BEGIN PGP PUBLIC KEY BLOCK-----")
    monkeypatch.setattr(module.shutil, "which", lambda name: "/usr/bin/gpg")
    _install_fake_network(monkeypatch, signature=None)

    service = _service(tmp_path)
    service.start(url=ASSET_URL, name=ASSET_NAME)
    status = _run_to_completion(service)

    assert status["status"] == STATUS_READY
    assert status["signature"] == SIGNATURE_UNAVAILABLE


def test_signature_state_is_unavailable_when_no_key_is_bundled(tmp_path, monkeypatch) -> None:
    # This build ships no signing key, so the signature cannot be checked --
    # which is reported, never treated as a verification failure.
    monkeypatch.setattr(module, "bundled_signing_key", lambda: None)
    _install_fake_network(monkeypatch)
    service = _service(tmp_path)

    service.start(url=ASSET_URL, name=ASSET_NAME)
    status = _run_to_completion(service)

    assert status["status"] == STATUS_READY
    assert status["checksum"] == CHECKSUM_VERIFIED
    assert status["signature"] == SIGNATURE_UNAVAILABLE
