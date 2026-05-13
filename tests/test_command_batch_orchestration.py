from __future__ import annotations

from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import socket
import sys
import time
from pathlib import Path
import threading
from uuid import uuid4

from collection.downloader_client import FakeDownloaderClient, HttpDownloaderClient
from collection.scope_selector import CanonicalScopeSelector
from collection.sync_orchestrator import SyncOrchestrator
from storage.sqlite_store import SQLiteStore


def _make_store() -> SQLiteStore:
    artifacts_dir = Path(__file__).resolve().parent / ".artifacts"
    artifacts_dir.mkdir(exist_ok=True)
    db_path = artifacts_dir / f"command-batch-{uuid4().hex}.db"
    store = SQLiteStore(db_path)
    store.init_schema()
    return store


def _seed_single_project(store: SQLiteStore, *, status: str = "active") -> None:
    store.upsert_canonical_entity(
        entity_type="single_project",
        entity_key="dcp:single_project:P001:S001",
        dataset_key="year_progress",
        source_system="dcp",
        source_record_key="dcp:year_progress:P001:S001",
        latest_raw_event_id=1,
        latest_collected_at="2026-05-13T02:00:00+08:00",
        latest_collected_at_epoch=1778608800.0,
        latest_source_record_hash="hash-single-project",
        source_refs=[],
        attributes={
            "project_code": "P001",
            "single_project_code": "S001",
            "bidding_section_code": "B001",
            "project_status": status,
        },
    )


def _tower_ingestion_batch(*, batch_id: str, command_run_id: str) -> dict:
    collected_at = datetime.now(timezone.utc).isoformat()
    request_id = f"req_{command_run_id}_tower"
    return {
        "schema_version": "ingestion.batch.v1",
        "batch": {
            "schema_version": "collection_batch.v1",
            "batch_id": batch_id,
            "batch_key": "daily_project_spatial_refresh",
            "source_system": "dcp",
            "plugin_id": "dcp",
            "downloader_name": "vibe-downloader-dcp",
            "trigger_type": "manual",
            "status": "running",
            "command_count": 1,
            "request_count": 1,
            "raw_record_count": 1,
            "error_count": 0,
            "metadata_snapshot": {},
            "config_snapshot": {},
            "result_summary": {},
            "started_at": collected_at,
        },
        "commands": [
            {
                "schema_version": "collection_command.v1",
                "command_run_id": command_run_id,
                "batch_id": batch_id,
                "command_key": "tower_refresh",
                "command_type": "refresh",
                "source_system": "dcp",
                "plugin_id": "dcp",
                "downloader_name": "vibe-downloader-dcp",
                "dataset_keys": ["tower"],
                "params": {},
                "processing_policy": {"auto_process": True},
                "status": "running",
                "request_count": 1,
                "raw_record_count": 1,
                "success_request_count": 1,
                "failed_request_count": 0,
            }
        ],
        "requests": [
            {
                "schema_version": "collection_request.v1",
                "request_id": request_id,
                "batch_id": batch_id,
                "command_run_id": command_run_id,
                "dataset_key": "tower",
                "request_key": "tower:P001:S001:B001",
                "request_kind": "project_single",
                "source_system": "dcp",
                "plugin_id": "dcp",
                "downloader_name": "vibe-downloader-dcp",
                "api_name": "tower_details",
                "request_params": {"single_project_code": "S001"},
                "request_context": {
                    "project_code": "P001",
                    "single_project_code": "S001",
                    "bidding_section_code": "B001",
                },
                "response_meta": {"record_count": 1},
                "status": "succeeded",
                "raw_record_count": 1,
                "error_count": 0,
                "requested_at": collected_at,
                "completed_at": collected_at,
            }
        ],
        "raw_events": [
            {
                "schema_version": "raw_event.v1",
                "raw_event_id": f"raw_{command_run_id}_tower_001",
                "batch_id": batch_id,
                "command_run_id": command_run_id,
                "request_id": request_id,
                "dataset_key": "tower",
                "source_system": "dcp",
                "plugin_id": "dcp",
                "downloader_name": "vibe-downloader-dcp",
                "source_record_id": "tower-001",
                "source_record_hash": "hash-tower-001",
                "source_record_key": "dcp:tower:tower-001",
                "raw_event_key": f"dcp:tower:{command_run_id}:tower-001",
                "source_path": "records[0]",
                "raw_payload": {
                    "id": "tower-001",
                    "towerNo": "G1",
                    "longitudeEdit": "112.90",
                    "latitudeEdit": "28.20",
                    "singleProjectCode": "S001",
                    "biddingSectionCode": "B001",
                    "prjCode": "P001",
                },
                "collected_at": collected_at,
                "processing_status": "pending",
            }
        ],
        "errors": [],
        "checkpoints": [
            {
                "schema_version": "collection_checkpoint.v1",
                "checkpoint_key": "tower:single_project:dcp:single_project:P001:S001",
                "source_system": "dcp",
                "plugin_id": "dcp",
                "dataset_key": "tower",
                "checkpoint_type": "single_project",
                "checkpoint_value": {"entity_key": "dcp:single_project:P001:S001"},
                "batch_id": batch_id,
                "command_run_id": command_run_id,
                "request_id": request_id,
            }
        ],
    }


class _MockDownloaderServer:
    def __init__(self, *, result: dict, status_sequence: list[str] | None = None):
        self.result = result
        self.status_sequence = status_sequence or ["succeeded"]
        self.sync_payloads: list[dict] = []
        self.status_calls = 0
        self.job_id = f"job_{uuid4().hex}"
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                if self.path != "/sync":
                    self.send_error(404)
                    return
                length = int(self.headers.get("Content-Length", "0"))
                payload = json.loads(self.rfile.read(length).decode("utf-8"))
                outer.sync_payloads.append(payload)
                self._send_json(
                    {
                        "schema_version": "downloader.sync.status.v1",
                        "job_id": outer.job_id,
                        "downloader_job_id": outer.job_id,
                        "status": "queued",
                    }
                )

            def do_GET(self) -> None:
                if self.path == f"/sync/jobs/{outer.job_id}":
                    index = min(outer.status_calls, len(outer.status_sequence) - 1)
                    status = outer.status_sequence[index]
                    outer.status_calls += 1
                    self._send_json(
                        {
                            "schema_version": "downloader.sync.status.v1",
                            "job_id": outer.job_id,
                            "downloader_job_id": outer.job_id,
                            "status": status,
                            "request_count": outer.result.get("request_count", 0),
                            "raw_record_count": outer.result.get("raw_record_count", 0),
                            "error_count": outer.result.get("error_count", 0),
                        }
                    )
                    return
                if self.path == f"/sync/jobs/{outer.job_id}/result":
                    self._send_json(
                        {
                            "schema_version": "downloader.sync.result.v1",
                            "job_id": outer.job_id,
                            "downloader_job_id": outer.job_id,
                            **outer.result,
                        }
                    )
                    return
                self.send_error(404)

            def _send_json(self, payload: dict) -> None:
                body = json.dumps(payload).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args: object) -> None:
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.base_url = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self) -> "_MockDownloaderServer":
        self.thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


class _DataHubIngestionServer:
    def __init__(self, store: SQLiteStore):
        outer = self
        self.store = store
        self.payloads: list[dict] = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                if self.path != "/ingestion/v1/batch":
                    self.send_error(404)
                    return
                length = int(self.headers.get("Content-Length", "0"))
                payload = json.loads(self.rfile.read(length).decode("utf-8"))
                outer.payloads.append(payload)
                stats = outer.store.save_ingestion_batch(payload)
                self._send_json({"accepted": True, **stats})

            def _send_json(self, payload: dict) -> None:
                body = json.dumps(payload, default=str).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args: object) -> None:
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.base_url = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.ingestion_url = f"{self.base_url}/ingestion/v1/batch"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self) -> "_DataHubIngestionServer":
        self.thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


class _UvicornServer:
    def __init__(self, app: object):
        import uvicorn

        self.port = _free_port()
        self.config = uvicorn.Config(
            app,
            host="127.0.0.1",
            port=self.port,
            log_level="warning",
        )
        self.server = uvicorn.Server(self.config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.base_url = f"http://127.0.0.1:{self.port}"

    def __enter__(self) -> "_UvicornServer":
        self.thread.start()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if self.server.started:
                return self
            time.sleep(0.01)
        raise RuntimeError("uvicorn test server did not start")

    def __exit__(self, *_exc: object) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=5)


def _free_port() -> int:
    sock = socket.socket()
    try:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])
    finally:
        sock.close()


def test_http_downloader_client_follows_sync_contract() -> None:
    with _MockDownloaderServer(
        result={
            "status": "succeeded",
            "request_count": 1,
            "raw_record_count": 40,
            "error_count": 0,
            "errors": [],
        },
        status_sequence=["running", "succeeded"],
    ) as server:
        client = HttpDownloaderClient(
            server.base_url,
            datahub_ingestion_url="http://datahub.test/ingestion/v1/batch",
            poll_interval_seconds=0.01,
            poll_timeout_seconds=2,
        )
        job_id = client.sync(
            {
                "batch_id": "batch_http_contract",
                "command_run_id": "cmd_http_contract",
                "command_key": "daily_meeting_today",
                "dataset_keys": ["daily_meeting"],
                "params": {"test_fixture": True},
            },
            scope_items=[],
        )

        status = client.wait_for_terminal_status(job_id)
        result = client.get_result(job_id)

    assert job_id == server.job_id
    assert status["status"] == "succeeded"
    assert result["raw_record_count"] == 40
    assert server.sync_payloads[0]["schema_version"] == "downloader.sync.request.v1"
    assert server.sync_payloads[0]["datahub"]["ingestion_url"].endswith("/ingestion/v1/batch")


def test_scope_selector_returns_active_single_project_scope_items() -> None:
    store = _make_store()
    _seed_single_project(store, status="active")
    store.upsert_canonical_entity(
        entity_type="single_project",
        entity_key="dcp:single_project:P002:S002",
        dataset_key="year_progress",
        source_system="dcp",
        source_record_key="dcp:year_progress:P002:S002",
        latest_raw_event_id=2,
        latest_collected_at="2026-05-13T02:00:00+08:00",
        latest_collected_at_epoch=1778608800.0,
        latest_source_record_hash="hash-inactive",
        source_refs=[],
        attributes={"project_code": "P002", "single_project_code": "S002", "project_status": "inactive"},
    )

    items = CanonicalScopeSelector(store).select_scope_items(
        {
            "entity_type": "single_project",
            "filter": {"project_status": "active"},
            "limit": 10,
        }
    )

    assert len(items) == 1
    assert items[0]["entity_key"] == "dcp:single_project:P001:S001"
    assert items[0]["attributes"]["project_code"] == "P001"


def test_command_batch_orchestrator_runs_fake_downloader_ingests_and_processes() -> None:
    store = _make_store()
    _seed_single_project(store)
    batch_id = "batch_orchestrator_project_spatial"
    command_run_id = "cmd_orchestrator_tower"
    fake = FakeDownloaderClient(
        {
            "tower_refresh": {
                "schema_version": "downloader.sync.result.v1",
                "job_id": f"fake_job_{command_run_id}",
                "status": "succeeded",
                "request_count": 1,
                "raw_record_count": 1,
                "error_count": 0,
                "ingestion_batch": _tower_ingestion_batch(
                    batch_id=batch_id,
                    command_run_id=command_run_id,
                ),
                "errors": [],
            }
        }
    )
    orchestrator = SyncOrchestrator(store=store, downloader_client=fake)
    orchestrator.create_batch_with_commands(
        batch_id=batch_id,
        batch_key="daily_project_spatial_refresh",
        commands=[
            {
                "command_run_id": command_run_id,
                "command_key": "tower_refresh",
                "command_type": "refresh",
                "dataset_keys": ["tower"],
                "scope_selector": {
                    "entity_type": "single_project",
                    "filter": {"project_status": "active"},
                },
                "processing_policy": {"auto_process": True},
            }
        ],
    )

    result = orchestrator.run_pending_commands(batch_id=batch_id)

    assert result["commands_run"] == 1
    assert fake.sync_calls[0]["scope_items"][0]["entity_key"] == "dcp:single_project:P001:S001"
    command = store.get_collection_command(command_run_id)
    assert command["status"] == "succeeded"
    assert command["downloader_job_id"] == f"fake_job_{command_run_id}"
    assert store.count_table_rows("collection_batches") == 1
    assert store.count_table_rows("collection_commands") == 1
    assert store.count_table_rows("collection_requests") == 1
    assert store.count_table_rows("raw_events") == 1
    towers = store.list_canonical_entities(entity_type="tower")
    assert len(towers) == 1
    assert towers[0]["entity_key"] == "dcp:tower:S001:B001:G1"
    checkpoint = store.get_collection_checkpoint(
        "tower:single_project:dcp:single_project:P001:S001"
    )
    assert checkpoint["checkpoint_value"]["entity_key"] == "dcp:single_project:P001:S001"


def test_command_batch_orchestrator_runs_http_downloader_client() -> None:
    store = _make_store()
    _seed_single_project(store)
    batch_id = "batch_orchestrator_http"
    command_run_id = "cmd_orchestrator_http_daily_meeting"
    with _MockDownloaderServer(
        result={
            "status": "succeeded",
            "request_count": 1,
            "raw_record_count": 40,
            "error_count": 0,
            "errors": [],
        },
        status_sequence=["running", "succeeded"],
    ) as server:
        orchestrator = SyncOrchestrator.from_config(
            store=store,
            config={
                "type": "http",
                "base_url": server.base_url,
                "datahub_ingestion_url": "http://datahub.test/ingestion/v1/batch",
                "poll_interval_seconds": 0.01,
                "poll_timeout_seconds": 2,
            },
        )
        orchestrator.create_batch_with_commands(
            batch_id=batch_id,
            batch_key="daily_meeting_today_refresh",
            commands=[
                {
                    "command_run_id": command_run_id,
                    "command_key": "daily_meeting_today",
                    "command_type": "refresh_today",
                    "dataset_keys": ["daily_meeting"],
                    "scope_selector": {
                        "entity_type": "single_project",
                        "filter": {"project_status": "active"},
                    },
                    "params": {"test_fixture": True},
                    "processing_policy": {"auto_process": False},
                }
            ],
        )

        result = orchestrator.run_pending_commands(batch_id=batch_id)

    command = store.get_collection_command(command_run_id)
    assert result["results"][0]["status"] == "succeeded"
    assert command["status"] == "succeeded"
    assert command["downloader_job_id"] == server.job_id
    assert command["request_count"] == 1
    assert command["raw_record_count"] == 40
    assert server.sync_payloads[0]["scope_items"][0]["entity_key"] == "dcp:single_project:P001:S001"


def test_orchestrator_with_local_downloader_service_posts_ingestion_to_datahub() -> None:
    downloader_src = Path(__file__).resolve().parents[2] / "vibe-downloader" / "src"
    if str(downloader_src) not in sys.path:
        sys.path.insert(0, str(downloader_src))
    from app.service.server import SyncJobStore, create_app

    store = _make_store()
    batch_id = "batch_local_downloader_daily_meeting"
    command_run_id = "cmd_local_downloader_daily_meeting"
    with _DataHubIngestionServer(store) as datahub_server:
        downloader_app = create_app(job_store=SyncJobStore())
        with _UvicornServer(downloader_app) as downloader_server:
            orchestrator = SyncOrchestrator.from_config(
                store=store,
                config={
                    "type": "http",
                    "base_url": downloader_server.base_url,
                    "datahub_ingestion_url": datahub_server.ingestion_url,
                    "poll_interval_seconds": 0.01,
                    "poll_timeout_seconds": 5,
                },
            )
            orchestrator.create_batch_with_commands(
                batch_id=batch_id,
                batch_key="daily_meeting_today_refresh",
                commands=[
                    {
                        "command_run_id": command_run_id,
                        "command_key": "daily_meeting_today",
                        "command_type": "refresh_today",
                        "dataset_keys": ["daily_meeting"],
                        "params": {"test_fixture": True, "record_count": 40},
                        "processing_policy": {"auto_process": False},
                    }
                ],
            )

            result = orchestrator.run_pending_commands(batch_id=batch_id)

    command = store.get_collection_command(command_run_id)
    assert result["results"][0]["status"] == "succeeded"
    assert command["status"] == "succeeded"
    assert command["downloader_job_id"].startswith("sync_")
    assert store.count_table_rows("raw_events") == 40
    assert len(datahub_server.payloads) == 1
    assert datahub_server.payloads[0]["schema_version"] == "ingestion.batch.v1"


def test_collection_errors_and_checkpoints_can_be_recorded() -> None:
    store = _make_store()

    error = store.record_collection_error(
        batch_id="batch_errors",
        command_run_id="cmd_errors",
        request_id="req_failed",
        source_system="dcp",
        plugin_id="dcp",
        downloader_name="vibe-downloader-dcp",
        dataset_key="daily_meeting",
        error_stage="request",
        error_type="Timeout",
        message="request timed out",
        retryable=True,
    )
    daily_checkpoint = store.upsert_collection_checkpoint(
        checkpoint_key="daily_meeting:today:2026-05-13",
        source_system="dcp",
        plugin_id="dcp",
        dataset_key="daily_meeting",
        checkpoint_type="date",
        checkpoint_value={"date": "2026-05-13", "last_page": 1},
        batch_id="batch_errors",
        command_run_id="cmd_errors",
        request_id="req_failed",
    )
    tower_checkpoint = store.upsert_collection_checkpoint(
        checkpoint_key="tower:single_project:dcp:single_project:P001:S001",
        source_system="dcp",
        plugin_id="dcp",
        dataset_key="tower",
        checkpoint_type="single_project",
        checkpoint_value={"entity_key": "dcp:single_project:P001:S001"},
    )

    assert error["error_type"] == "Timeout"
    assert daily_checkpoint["checkpoint_value"]["last_page"] == 1
    assert tower_checkpoint["checkpoint_value"]["entity_key"] == "dcp:single_project:P001:S001"
    assert store.count_table_rows("collection_errors") == 1
    assert store.count_table_rows("collection_checkpoints") == 2


def test_raw_events_table_uses_release_columns_without_sourceevent_compat_columns() -> None:
    store = _make_store()
    conn = store._get_connection()
    try:
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(raw_events)").fetchall()
        }
    finally:
        conn.close()

    assert {
        "raw_event_key",
        "batch_id",
        "command_run_id",
        "request_id",
        "raw_record",
        "content_hash",
    }.issubset(columns)
    assert "event_id" not in columns
    assert "idempotency_key" not in columns
    assert "payload" not in columns
    assert "source_ref" not in columns
