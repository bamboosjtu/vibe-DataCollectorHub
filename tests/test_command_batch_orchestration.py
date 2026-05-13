from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from collection.downloader_client import FakeDownloaderClient
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
