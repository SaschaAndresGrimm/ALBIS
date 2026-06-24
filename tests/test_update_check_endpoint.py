from __future__ import annotations

import ssl

from fastapi.testclient import TestClient

from backend.app import app, update_check_service
from backend.services.update_check import RELEASES_PAGE_URL, ReleaseMetadata, _ssl_context


def test_ssl_context_uses_verifying_certifi_bundle() -> None:
    # Packaged builds have no system trust store, so the GitHub update check must
    # verify against certifi's CA bundle rather than failing the TLS handshake.
    context = _ssl_context()
    assert isinstance(context, ssl.SSLContext)
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True


def _request_update_check() -> dict[str, str]:
    response = TestClient(app).get("/api/update-check")
    assert response.status_code == 200
    return response.json()


def test_update_check_endpoint_reports_newer_release(monkeypatch) -> None:
    update_check_service.clear_cache()
    monkeypatch.setattr(update_check_service, "current_version", "0.9.3")
    monkeypatch.setattr(
        update_check_service,
        "_fetch_latest_release",
        lambda: ReleaseMetadata("0.9.4", "https://example.invalid/releases/v0.9.4"),
    )

    payload = _request_update_check()

    assert payload == {
        "status": "update_available",
        "current_version": "0.9.3",
        "latest_version": "0.9.4",
        "release_url": "https://example.invalid/releases/v0.9.4",
        "message": "",
    }


def test_update_check_endpoint_reports_up_to_date_for_equal_release(monkeypatch) -> None:
    update_check_service.clear_cache()
    monkeypatch.setattr(update_check_service, "current_version", "0.9.3")
    monkeypatch.setattr(
        update_check_service,
        "_fetch_latest_release",
        lambda: ReleaseMetadata("0.9.3", "https://example.invalid/releases/v0.9.3"),
    )

    payload = _request_update_check()

    assert payload["status"] == "up_to_date"
    assert payload["current_version"] == "0.9.3"
    assert payload["latest_version"] == "0.9.3"


def test_update_check_endpoint_reports_up_to_date_when_current_version_is_ahead(
    monkeypatch,
) -> None:
    update_check_service.clear_cache()
    monkeypatch.setattr(update_check_service, "current_version", "1.0.1")
    monkeypatch.setattr(
        update_check_service,
        "_fetch_latest_release",
        lambda: ReleaseMetadata("1.0.0", "https://example.invalid/releases/v1.0.0"),
    )

    payload = _request_update_check()

    assert payload["status"] == "up_to_date"
    assert payload["current_version"] == "1.0.1"
    assert payload["latest_version"] == "1.0.0"


def test_update_check_endpoint_treats_stable_release_as_newer_than_matching_prerelease(
    monkeypatch,
) -> None:
    update_check_service.clear_cache()
    monkeypatch.setattr(update_check_service, "current_version", "1.0.0-rc.1")
    monkeypatch.setattr(
        update_check_service,
        "_fetch_latest_release",
        lambda: ReleaseMetadata("1.0.0", "https://example.invalid/releases/v1.0.0"),
    )

    payload = _request_update_check()

    assert payload["status"] == "update_available"
    assert payload["current_version"] == "1.0.0-rc.1"
    assert payload["latest_version"] == "1.0.0"


def test_update_check_endpoint_returns_unavailable_for_timeout_or_malformed_response(
    monkeypatch,
) -> None:
    for failure in (TimeoutError("timed out"), ValueError("bad payload")):
        update_check_service.clear_cache()
        monkeypatch.setattr(update_check_service, "current_version", "0.9.3")
        monkeypatch.setattr(
            update_check_service,
            "_fetch_latest_release",
            lambda failure=failure: (_ for _ in ()).throw(failure),
        )

        payload = _request_update_check()

        assert payload["status"] == "unavailable"
        assert payload["current_version"] == "0.9.3"
        assert payload["latest_version"] == ""
        assert payload["release_url"] == RELEASES_PAGE_URL
        assert payload["message"]
