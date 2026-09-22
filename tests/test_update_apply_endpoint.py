from __future__ import annotations

from fastapi.testclient import TestClient

from backend.app import (
    app,
    runtime_state,
    series_summing,
    update_apply_service,
    update_check_service,
    update_download_service,
)
from backend.install_kind import INSTALL_KIND_MACOS_APP
from backend.services.update_check import ReleaseAsset, ReleaseMetadata

ASSET_NAME = "ALBIS-macos-arm64-v1.0.0-abc1234.dmg"
ASSET_URL = "https://github.com/SaschaAndresGrimm/ALBIS/releases/download/v1.0.0/" + ASSET_NAME


def _client() -> TestClient:
    return TestClient(app)


def _reset(monkeypatch) -> None:
    update_download_service.reset()
    update_check_service.clear_cache()
    monkeypatch.setattr(runtime_state, "allow_update_download", True)
    monkeypatch.setattr(runtime_state, "allow_update_apply", False)


def test_apply_is_disabled_by_default(monkeypatch) -> None:
    _reset(monkeypatch)
    payload = _client().get("/api/update-apply/status").json()

    assert payload["status"] == "idle"
    assert payload["refusal"] == "disabled"


def test_starting_an_apply_while_disabled_is_a_conflict_naming_the_reason(monkeypatch) -> None:
    _reset(monkeypatch)
    response = _client().post("/api/update-apply/start", json={"busy": []})

    assert response.status_code == 409
    assert response.json()["detail"] == "disabled"


def test_apply_support_is_reported_on_the_update_check(monkeypatch) -> None:
    _reset(monkeypatch)
    monkeypatch.setattr(update_check_service, "current_version", "0.9.3")
    monkeypatch.setattr(update_check_service, "install_kind", INSTALL_KIND_MACOS_APP)
    monkeypatch.setattr(update_check_service, "target_arch", "arm64")
    monkeypatch.setattr(
        update_check_service,
        "_fetch_latest_release",
        lambda: ReleaseMetadata(
            "1.0.0",
            "https://example.invalid/releases/v1.0.0",
            (ReleaseAsset(name=ASSET_NAME, download_url=ASSET_URL),),
        ),
    )

    # The test suite runs from a source checkout, so applying is never on
    # offer whatever the setting says.
    assert _client().get("/api/update-check").json()["apply_supported"] is False

    monkeypatch.setattr(runtime_state, "allow_update_apply", True)
    assert _client().get("/api/update-check").json()["apply_supported"] is False


def test_apply_support_is_true_when_nothing_structural_refuses_it(monkeypatch) -> None:
    """The route reports capability, not readiness.

    "No verified download yet" and "a watch is running" are states that change
    minute to minute, so they must not make the interface hide the step
    entirely; only the setting, the platform and the launcher do that.
    """
    _reset(monkeypatch)
    monkeypatch.setattr(update_check_service, "current_version", "0.9.3")
    monkeypatch.setattr(
        update_check_service,
        "_fetch_latest_release",
        lambda: ReleaseMetadata("1.0.0", "https://example.invalid/releases/v1.0.0"),
    )

    structural = ("disabled", "unsupported_install", "shutdown_unavailable", "target_unknown")
    transient = (None, "no_verified_download", "unverified_download", "busy")

    def _always(code):
        return lambda **_kwargs: code

    for code in structural:
        monkeypatch.setattr(update_apply_service, "refusal_code", _always(code))
        assert _client().get("/api/update-check").json()["apply_supported"] is False, code

    for code in transient:
        monkeypatch.setattr(update_apply_service, "refusal_code", _always(code))
        assert _client().get("/api/update-check").json()["apply_supported"] is True, code


def test_the_backend_reports_its_own_running_work_as_busy(monkeypatch) -> None:
    # A series sum writes its output over minutes, and closing ALBIS midway
    # truncates it. The interface cannot see that job, so the backend adds it.
    from backend.app import _get_backend_busy

    monkeypatch.setattr(series_summing, "has_running_job", lambda: False)
    assert _get_backend_busy() == []

    monkeypatch.setattr(series_summing, "has_running_job", lambda: True)
    assert _get_backend_busy() == ["series_sum"]


def test_the_backends_own_work_reaches_the_refusal_check(monkeypatch) -> None:
    seen: list[list[str]] = []

    def refusal_code(*, install_kind: str, allowed: bool, busy=()) -> str | None:
        seen.append(list(busy))
        return "busy" if busy else None

    _reset(monkeypatch)
    monkeypatch.setattr(runtime_state, "allow_update_apply", True)
    monkeypatch.setattr(series_summing, "has_running_job", lambda: True)
    monkeypatch.setattr(update_apply_service, "refusal_code", refusal_code)

    # The request reports nothing; the refusal still comes from the job.
    response = _client().post("/api/update-apply/start", json={"busy": []})

    assert response.status_code == 409
    assert response.json()["detail"] == "busy"
    assert seen and "series_sum" in seen[0]


def test_the_interface_can_report_work_the_backend_cannot_see(monkeypatch) -> None:
    # A live watch runs in the browser, so the backend only learns about it
    # because the request says so.
    _reset(monkeypatch)
    monkeypatch.setattr(runtime_state, "allow_update_apply", True)
    monkeypatch.setattr(
        update_apply_service,
        "refusal_code",
        lambda *, install_kind, allowed, busy=(): "busy" if busy else None,
    )

    response = _client().post("/api/update-apply/start", json={"busy": ["live_watch"]})

    assert response.status_code == 409
    assert response.json()["detail"] == "busy"


def test_reported_busy_entries_are_filtered_to_real_values(monkeypatch) -> None:
    seen: list[list[str]] = []

    def refusal_code(*, install_kind: str, allowed: bool, busy=()) -> str | None:
        seen.append(list(busy))
        return "disabled"

    _reset(monkeypatch)
    monkeypatch.setattr(runtime_state, "allow_update_apply", True)
    monkeypatch.setattr(update_apply_service, "refusal_code", refusal_code)

    _client().post("/api/update-apply/start", json={"busy": ["live_watch", "", "   "]})

    assert seen and seen[0] == ["live_watch"]


def test_apply_status_reports_the_service_state_and_a_current_refusal(monkeypatch) -> None:
    _reset(monkeypatch)
    monkeypatch.setattr(runtime_state, "allow_update_apply", True)
    monkeypatch.setattr(
        update_apply_service, "status", lambda: {"status": "applied", "message": "done"}
    )
    monkeypatch.setattr(update_apply_service, "refusal_code", lambda **_kwargs: None)

    payload = _client().get("/api/update-apply/status").json()

    assert payload == {"status": "applied", "message": "done", "refusal": ""}


def test_the_request_cannot_name_a_file_to_install() -> None:
    # The apply acts only on the service's own record of what it downloaded
    # and verified. There is deliberately no field for a client to point it
    # somewhere else, so the contract is asserted rather than assumed.
    schema = _client().get("/openapi.json").json()
    body = schema["paths"]["/api/update-apply/start"]["post"]["requestBody"]
    ref = body["content"]["application/json"]["schema"]["$ref"].rsplit("/", 1)[-1]

    assert set(schema["components"]["schemas"][ref]["properties"]) == {"busy"}
