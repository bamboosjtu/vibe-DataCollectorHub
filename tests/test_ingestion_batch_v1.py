from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient

import api.server as server
from storage.sqlite_store import SQLiteStore


def _make_store() -> SQLiteStore:
    artifacts_dir = Path(__file__).resolve().parent / ".artifacts"
    artifacts_dir.mkdir(exist_ok=True)
    db_path = artifacts_dir / f"ingestion-batch-v1-{uuid4().hex}.db"
    store = SQLiteStore(db_path)
    store.init_schema()
    return store


def _client(store: SQLiteStore) -> TestClient:
    server.store = store
    return TestClient(server.app)


def _fixture() -> dict:
    path = (
        Path(__file__).resolve().parents[2]
        / "vibe-contracts"
        / "examples"
        / "dcp.daily_meeting.ingestion_batch.example.json"
    )
    return json.loads(path.read_text(encoding="utf-8-sig"))


def test_daily_meeting_ingestion_batch_writes_mvp_raw_layer() -> None:
    store = _make_store()
    response = _client(store).post("/ingestion/v1/batch", json=_fixture())

    assert response.status_code == 200
    body = response.json()
    assert body["accepted"] is True
    assert body["collection_batches_upserted"] == 1
    assert body["collection_commands_upserted"] == 2
    assert body["collection_requests_upserted"] == 3
    assert body["raw_events_inserted"] == 40
    assert body["raw_events_duplicated"] == 0

    assert store.count_table_rows("collection_batches") == 1
    assert store.count_table_rows("collection_commands") == 2
    assert store.count_table_rows("collection_requests") == 3
    assert store.count_table_rows("raw_events") == 40
    assert store.count_table_rows("collection_checkpoints") == 1


def test_daily_meeting_ingestion_batch_skips_duplicate_raw_events() -> None:
    store = _make_store()
    client = _client(store)

    first = client.post("/ingestion/v1/batch", json=_fixture())
    second = client.post("/ingestion/v1/batch", json=_fixture())

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.json()["raw_events_inserted"] == 0
    assert second.json()["raw_events_duplicated"] == 40
    assert store.count_table_rows("raw_events") == 40


def test_batch_ingested_daily_meeting_can_be_processed() -> None:
    store = _make_store()
    payload = _fixture()
    payload["raw_events"] = [payload["raw_events"][0]]
    raw_event = payload["raw_events"][0]
    raw_event["raw_event_id"] = "raw_daily_meeting_processable_001"
    raw_event["source_file"] = "daily_meeting/2026-05-12.json"
    raw_event["raw_payload"] = {
        "id": "meeting-processable-001",
        "workDate": "2026-05-12",
        "toolBoxTalkLongitude": "113.01",
        "toolBoxTalkLatitude": "28.20",
        "currentConstrHeadcount": "12",
        "reAssessmentRiskLevel": "2",
        "currentConstructionStatus": "working",
        "prjCode": "P001",
        "singleProjectCode": "SP001",
        "biddingSectionCode": "BS001",
    }
    payload["batch"]["raw_record_count"] = 1
    payload["requests"][0]["raw_record_count"] = 1
    payload["commands"][0]["raw_record_count"] = 1

    client = _client(store)
    ingestion = client.post("/ingestion/v1/batch", json=payload)
    processing = client.post(
        "/processing/v1/run",
        json={"dataset_key": "daily_meeting", "mode": "full"},
    )

    assert ingestion.status_code == 200
    assert processing.status_code == 200
    assert processing.json()["inserted"] == 1
    entities = store.list_canonical_entities(entity_type="work_point")
    assert len(entities) == 1
    assert entities[0]["attributes"]["work_date"] == "2026-05-12"
