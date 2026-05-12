from __future__ import annotations

import subprocess
from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient

import api.server as server
from core.plugin_manager import PluginManager
from storage.sqlite_store import SQLiteStore


def _make_registered_store() -> SQLiteStore:
    artifacts_dir = Path(__file__).resolve().parent / ".artifacts"
    artifacts_dir.mkdir(exist_ok=True)
    db_path = artifacts_dir / f"external-collection-{uuid4().hex}.db"
    store = SQLiteStore(db_path)
    store.init_schema()

    manager = PluginManager()
    manager.discover_plugins()
    manager.save_discovered_plugins(store)
    return store


def _client(store: SQLiteStore) -> TestClient:
    server.store = store
    manager = PluginManager()
    manager.discover_plugins()
    server.plugin_manager = manager
    return TestClient(server.app)


def _command() -> list[str]:
    return [
        "uv",
        "run",
        "python",
        "-m",
        "app.commands.dcp_datahub",
        "sync",
        "daily_meeting",
        "--datahub-url",
        "http://127.0.0.1:8000",
        "--dataset-mode",
        "enabled",
        "--processing-mode",
        "none",
    ]


def test_create_external_collection_job_can_persist() -> None:
    store = _make_registered_store()

    job = store.create_external_collection_job(
        job_id="collect-test",
        plugin_id="dcp",
        profile="monitor_daily",
        dataset_keys=["daily_meeting"],
        mode="incremental",
        command=_command(),
        cwd="D:/vibe-coding/vibe-workspace/vibe-downloader/src",
        datahub_url="http://127.0.0.1:8000",
        processing_mode="none",
        recent_days=3,
    )

    assert job["job_id"] == "collect-test"
    assert job["status"] == "queued"
    assert job["dataset_keys"] == ["daily_meeting"]
    assert job["command"][:5] == ["uv", "run", "python", "-m", "app.commands.dcp_datahub"]
    assert job["recent_days"] == 3


def test_mark_external_collection_job_lifecycle_updates_status() -> None:
    store = _make_registered_store()
    store.create_external_collection_job(
        job_id="collect-lifecycle",
        plugin_id="dcp",
        profile=None,
        dataset_keys=["tower"],
        mode="incremental",
        command=_command(),
        cwd="D:/vibe-coding/vibe-workspace/vibe-downloader/src",
        datahub_url="http://127.0.0.1:8000",
        processing_mode="none",
    )

    store.mark_external_collection_job_running("collect-lifecycle")
    assert store.get_external_collection_job("collect-lifecycle")["status"] == "running"

    store.mark_external_collection_job_succeeded(
        "collect-lifecycle",
        0,
        "logs\n{\"ok\": true}",
        "",
        {"ok": True},
    )
    succeeded = store.get_external_collection_job("collect-lifecycle")
    assert succeeded["status"] == "succeeded"
    assert succeeded["exit_code"] == 0
    assert succeeded["result"] == {"ok": True}

    store.create_external_collection_job(
        job_id="collect-failed",
        plugin_id="dcp",
        profile=None,
        dataset_keys=["station"],
        mode="incremental",
        command=_command(),
        cwd="D:/vibe-coding/vibe-workspace/vibe-downloader/src",
        datahub_url="http://127.0.0.1:8000",
        processing_mode="none",
    )
    store.mark_external_collection_job_failed(
        "collect-failed",
        2,
        "out",
        "err",
        "failed",
    )
    failed = store.get_external_collection_job("collect-failed")
    assert failed["status"] == "failed"
    assert failed["exit_code"] == 2
    assert failed["error"] == "failed"


def test_get_external_collection_job_api_returns_job(monkeypatch) -> None:
    store = _make_registered_store()
    client = _client(store)
    monkeypatch.setattr(server, "_run_external_collection_job", lambda **_kwargs: None)

    created = client.post(
        "/collection/v1/jobs",
        json={"plugin_id": "dcp", "dataset_keys": ["daily_meeting"]},
    )
    assert created.status_code == 202

    response = client.get(f"/collection/v1/jobs/{created.json()['job_id']}")

    assert response.status_code == 200
    assert response.json()["job_id"] == created.json()["job_id"]
    assert response.json()["dataset_keys"] == ["daily_meeting"]


def test_post_external_collection_job_unsupported_plugin_returns_400() -> None:
    store = _make_registered_store()
    client = _client(store)

    response = client.post(
        "/collection/v1/jobs",
        json={"plugin_id": "other", "dataset_keys": ["daily_meeting"]},
    )

    assert response.status_code == 400
    assert response.json()["detail"]["error"] == "unsupported plugin_id: other"


def test_post_external_collection_job_dataset_must_be_enabled() -> None:
    store = _make_registered_store()
    client = _client(store)

    response = client.post(
        "/collection/v1/jobs",
        json={"plugin_id": "dcp", "dataset_keys": ["unknown_dataset"]},
    )

    assert response.status_code == 400
    assert response.json()["detail"]["unsupported"] == ["unknown_dataset"]


def test_post_external_collection_job_conflicts_on_overlapping_dataset() -> None:
    store = _make_registered_store()
    client = _client(store)
    store.create_external_collection_job(
        job_id="collect-active",
        plugin_id="dcp",
        profile=None,
        dataset_keys=["line_section"],
        mode="incremental",
        command=_command(),
        cwd="D:/vibe-coding/vibe-workspace/vibe-downloader/src",
        datahub_url="http://127.0.0.1:8000",
        processing_mode="none",
    )

    response = client.post(
        "/collection/v1/jobs",
        json={"plugin_id": "dcp", "dataset_keys": ["tower", "line_section"]},
    )

    assert response.status_code == 409
    assert response.json()["detail"]["job"]["job_id"] == "collect-active"


def test_profile_monitor_daily_expands_defaults(monkeypatch) -> None:
    store = _make_registered_store()
    client = _client(store)
    monkeypatch.setattr(server, "_run_external_collection_job", lambda **_kwargs: None)

    response = client.post(
        "/collection/v1/jobs",
        json={"plugin_id": "dcp", "profile": "monitor_daily"},
    )

    assert response.status_code == 202
    body = response.json()
    assert body["dataset_keys"] == ["daily_meeting"]
    assert body["recent_days"] == 3
    assert body["processing_mode"] == "async"
    assert body["command"][5] == "sync"
    assert "--recent-days" not in body["command"]


def test_explicit_dataset_keys_override_profile(monkeypatch) -> None:
    store = _make_registered_store()
    client = _client(store)
    monkeypatch.setattr(server, "_run_external_collection_job", lambda **_kwargs: None)

    response = client.post(
        "/collection/v1/jobs",
        json={
            "plugin_id": "dcp",
            "profile": "monitor_daily",
            "dataset_keys": ["tower"],
            "processing_mode": "none",
        },
    )

    assert response.status_code == 202
    body = response.json()
    assert body["dataset_keys"] == ["tower"]
    assert body["processing_mode"] == "none"


def test_downloader_command_uses_new_dcp_datahub_module() -> None:
    command = server.build_downloader_command(
        downloader_config={
            "uv_command": "uv",
            "python_module": "app.commands.dcp_datahub",
        },
        dataset_keys=["line_section"],
        datahub_url="http://127.0.0.1:8000",
        processing_mode="none",
        since_date="2026-05-01",
        include_existing=True,
        force=True,
        due_only=True,
    )

    assert command[:6] == [
        "uv",
        "run",
        "python",
        "-m",
        "app.commands.dcp_datahub",
        "sync",
    ]
    assert "app.collector.datahub_sync" not in command
    assert "--since-date" in command
    assert "--include-existing" not in command
    assert "--force" not in command
    assert "--due-only" not in command


def test_background_job_uses_mocked_subprocess_and_saves_result(monkeypatch) -> None:
    store = _make_registered_store()
    job = store.create_external_collection_job(
        job_id="collect-bg",
        plugin_id="dcp",
        profile=None,
        dataset_keys=["daily_meeting"],
        mode="incremental",
        command=_command(),
        cwd="D:/vibe-coding/vibe-workspace/vibe-downloader/src",
        datahub_url="http://127.0.0.1:8000",
        processing_mode="none",
    )

    calls: list[dict[str, object]] = []

    def fake_run(command: list[str], **kwargs):
        calls.append({"command": command, **kwargs})
        return subprocess.CompletedProcess(
            command,
            0,
            stdout='log line\n{"summary": {"events": 2, "accepted": 2}}\n',
            stderr="",
        )

    monkeypatch.setattr(server, "DEFAULT_DB_PATH", store.db_path)
    monkeypatch.setattr(server.subprocess, "run", fake_run)

    server._run_external_collection_job(
        job_id=job["job_id"],
        command=job["command"],
        cwd=job["cwd"],
    )

    updated = store.get_external_collection_job(job["job_id"])
    assert updated["status"] == "succeeded"
    assert updated["result"]["summary"]["events"] == 2
    assert calls[0]["command"][4] == "app.commands.dcp_datahub"
    assert calls[0]["cwd"] == "D:/vibe-coding/vibe-workspace/vibe-downloader/src"
    assert calls[0]["capture_output"] is True
    assert calls[0]["text"] is True
