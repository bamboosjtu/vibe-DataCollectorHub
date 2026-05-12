from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

import api.server as server
from core.plugin_manager import PluginManager
from storage.sqlite_store import SQLiteStore

pytestmark = pytest.mark.skip(
    reason="Legacy SourceEvent /ingestion/v1/events tests; release ingestion is /ingestion/v1/batch."
)


def _make_store() -> SQLiteStore:
    artifacts_dir = Path(__file__).resolve().parent / ".artifacts"
    artifacts_dir.mkdir(exist_ok=True)
    db_path = artifacts_dir / f"ingestion-{uuid4().hex}.db"
    store = SQLiteStore(db_path)
    store.init_schema()
    manager = PluginManager()
    manager.discover_plugins()
    manager.save_discovered_plugins(store)
    return store


def _client(store: SQLiteStore) -> TestClient:
    server.store = store
    return TestClient(server.app)


def _event(suffix: str = "001") -> dict:
    return {
        "schema_version": "source_event.v1",
        "event_id": f"evt-{suffix}",
        "idempotency_key": f"dcp:safePages:meetingListAdmin:queryToolBoxTalkListPagePc:meeting-{suffix}",
        "source_system": "dcp",
        "source_event_type": "dcp.record",
        "event_granularity": "record",
        "source_record_id": f"meeting-{suffix}",
        "source_record_hash": f"hash-{suffix}",
        "occurred_at": "2026-05-03T21:30:12+08:00",
        "collected_at": "2026-05-03T21:30:12+08:00",
        "payload_schema": "dcp.raw_record.v1",
        "payload": {
            "raw": {
                "id": f"meeting-{suffix}",
                "prjCode": "1716D0230007",
                "nested": {"keep": ["all", "fields"]},
            }
        },
        "source_ref": {
            "collector": "vibe-downloader",
            "run_id": "20260503_213000",
            "collection": "safePages",
            "page_name": "meetingListAdmin",
            "api_name": "queryToolBoxTalkListPagePc",
            "raw_data_index": 0,
            "record_index": 0,
            "record_path": "raw_data[0].records[0]",
            "source_file": "safePages/meetingListAdmin/20260503_213000.json",
        },
    }


def test_single_event_ingests_successfully() -> None:
    store = _make_store()
    response = _client(store).post("/ingestion/v1/events", json={"events": [_event()]})

    assert response.status_code == 200
    assert response.json()["accepted"] == 1
    assert response.json()["duplicated"] == 0
    assert response.json()["failed"] == 0
    assert store.count_raw_events() == 1


def test_batch_events_ingest_successfully() -> None:
    store = _make_store()
    response = _client(store).post(
        "/ingestion/v1/events",
        json={"events": [_event("001"), _event("002")]},
    )

    assert response.status_code == 200
    assert response.json()["accepted"] == 2
    assert response.json()["failed"] == 0
    assert store.count_raw_events() == 2


def test_duplicate_idempotency_key_is_not_inserted_twice() -> None:
    store = _make_store()
    client = _client(store)
    event = _event()

    first = client.post("/ingestion/v1/events", json={"events": [event]})
    second = client.post("/ingestion/v1/events", json={"events": [event]})

    assert first.json()["accepted"] == 1
    assert second.json()["accepted"] == 0
    assert second.json()["duplicated"] == 1
    assert second.json()["failed"] == 0
    assert store.count_raw_events() == 1


def test_same_idempotency_key_and_hash_is_duplicated() -> None:
    store = _make_store()
    client = _client(store)
    event = _event("same")

    first = client.post("/ingestion/v1/events", json={"events": [event]})
    second = client.post("/ingestion/v1/events", json={"events": [event]})

    assert first.json()["accepted"] == 1
    assert second.json()["duplicated"] == 1
    saved = store.get_raw_event_by_idempotency_key(event["idempotency_key"])
    assert saved["source_record_key"] == event["idempotency_key"]
    assert saved["raw_event_key"] == f"{event['idempotency_key']}:{event['source_record_hash']}"
    assert saved["event"]["idempotency_key"] == event["idempotency_key"]


def test_same_idempotency_key_with_different_hash_is_accepted() -> None:
    store = _make_store()
    client = _client(store)
    first_event = _event("same-record")
    second_event = deepcopy(first_event)
    second_event["event_id"] = "evt-same-record-new-hash"
    second_event["source_record_hash"] = "hash-same-record-new"

    first = client.post("/ingestion/v1/events", json={"events": [first_event]})
    second = client.post("/ingestion/v1/events", json={"events": [second_event]})

    assert first.json()["accepted"] == 1
    assert second.json()["accepted"] == 1
    assert second.json()["duplicated"] == 0
    assert store.count_raw_events() == 2


def test_missing_source_record_id_and_hash_fails() -> None:
    store = _make_store()
    event = deepcopy(_event())
    event["source_record_id"] = None
    event["source_record_hash"] = None

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    assert response.status_code == 200
    body = response.json()
    assert body["accepted"] == 0
    assert body["failed"] == 1
    assert "source_record_id or source_record_hash is required" in body["errors"][0]["error"]
    assert store.count_raw_events() == 0


def test_dcp_ingestion_missing_payload_raw_fails() -> None:
    store = _make_store()
    event = _event()
    event["payload"] = {"not_raw": {}}

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    body = response.json()
    assert body["accepted"] == 0
    assert body["failed"] == 1
    assert "payload.raw" in body["errors"][0]["error"]
    assert store.count_raw_events() == 0


def test_dcp_ingestion_missing_source_ref_record_path_fails() -> None:
    store = _make_store()
    event = _event()
    del event["source_ref"]["record_path"]

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    body = response.json()
    assert body["accepted"] == 0
    assert body["failed"] == 1
    assert "record_path" in body["errors"][0]["error"]
    assert store.count_raw_events() == 0


def test_payload_raw_is_preserved_completely() -> None:
    store = _make_store()
    event = _event()

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    assert response.status_code == 200
    saved = store.get_raw_event_by_idempotency_key(event["idempotency_key"])
    assert saved is not None
    assert saved["payload"]["raw"] == event["payload"]["raw"]
    assert saved["event"]["payload"]["raw"] == event["payload"]["raw"]


def _event_for_source_ref(
    suffix: str,
    collection: str,
    page_name: str,
    api_name: str,
) -> dict:
    event = _event(suffix)
    event["idempotency_key"] = f"dcp:{collection}:{page_name}:{api_name}:{suffix}"
    event["source_ref"]["collection"] = collection
    event["source_ref"]["page_name"] = page_name
    event["source_ref"]["api_name"] = api_name
    event["source_ref"]["source_file"] = f"{collection}/{page_name}/{suffix}.json"
    return event


def test_ingest_tower_source_event_sets_dataset_key() -> None:
    store = _make_store()
    event = _event_for_source_ref("tower", "projectPages", "杆塔信息", "tower_details")

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    assert response.status_code == 200
    saved = store.get_raw_event_by_idempotency_key(event["idempotency_key"])
    assert saved["dataset_key"] == "tower"
    assert saved["collection"] == "projectPages"
    assert saved["page_name"] == "杆塔信息"
    assert saved["api_name"] == "tower_details"


def test_ingest_station_source_event_sets_dataset_key() -> None:
    store = _make_store()
    event = _event_for_source_ref(
        "station",
        "projectPages",
        "变电站坐标",
        "substation_coordinates",
    )

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    assert response.status_code == 200
    saved = store.get_raw_event_by_idempotency_key(event["idempotency_key"])
    assert saved["dataset_key"] == "station"


def test_line_section_default_ingests_successfully() -> None:
    store = _make_store()
    event = _event_for_source_ref(
        "line-section-default",
        "projectPages",
        "区段划分",
        "section_details",
    )

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    assert response.status_code == 200
    assert response.json()["accepted"] == 1
    saved = store.get_raw_event_by_idempotency_key(event["idempotency_key"])
    assert saved["dataset_key"] == "line_section"


def test_project_preconstruction_default_ingests_successfully() -> None:
    store = _make_store()
    event = _event_for_source_ref(
        "project-preconstruction-default",
        "projectPages",
        "项目前期成果",
        "preconstruction_results_detail",
    )

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    assert response.status_code == 200
    assert response.json()["accepted"] == 1
    saved = store.get_raw_event_by_idempotency_key(event["idempotency_key"])
    assert saved["dataset_key"] == "project_preconstruction"


def test_line_section_ingests_with_old_three_dataset_runtime_config() -> None:
    store = _make_store()
    store.save_plugin_runtime_config(
        "dcp",
        {
            "enabled_datasets": ["daily_meeting", "tower", "station"],
            "monitor_datasets": ["daily_meeting", "tower", "station"],
            "datasets": {
                "daily_meeting": {
                    "enabled": True,
                    "collection": "safePages",
                    "scope": "date_partitioned",
                    "page_name": "meetingListAdmin",
                    "api_names": ["queryToolBoxTalkListPagePc"],
                },
                "tower": {
                    "enabled": True,
                    "collection": "projectPages",
                    "scope": "project_single",
                    "page_name": "杆塔信息",
                    "api_names": ["tower_details"],
                },
                "station": {
                    "enabled": True,
                    "collection": "projectPages",
                    "scope": "project_single",
                    "page_name": "变电站坐标",
                    "api_names": ["substation_coordinates"],
                },
            },
        },
    )
    event = _event_for_source_ref(
        "line-section-old-runtime",
        "projectPages",
        "区段划分",
        "section_details",
    )

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    assert response.status_code == 200
    assert response.json()["accepted"] == 1
    saved = store.get_raw_event_by_idempotency_key(event["idempotency_key"])
    assert saved["dataset_key"] == "line_section"


def test_ingest_daily_meeting_source_event_sets_dataset_key() -> None:
    store = _make_store()
    event = _event_for_source_ref(
        "daily-meeting",
        "safePages",
        "meetingListAdmin",
        "queryToolBoxTalkListPagePc",
    )

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    assert response.status_code == 200
    saved = store.get_raw_event_by_idempotency_key(event["idempotency_key"])
    assert saved["dataset_key"] == "daily_meeting"
    assert saved["source_file"] == "safePages/meetingListAdmin/daily-meeting.json"


def test_ingest_daily_meeting_page_alias_sets_dataset_key() -> None:
    store = _make_store()
    event = _event_for_source_ref(
        "daily-meeting-alias",
        "safePages",
        "站班会",
        "queryToolBoxTalkListPagePc",
    )

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    assert response.status_code == 200
    body = response.json()
    assert body["accepted"] == 1
    saved = store.get_raw_event_by_idempotency_key(event["idempotency_key"])
    assert saved["dataset_key"] == "daily_meeting"


def test_ingestion_rejects_epoch_millis_occurred_at() -> None:
    store = _make_store()
    event = _event("epoch-millis")
    event["occurred_at"] = "1714726487000"

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    assert response.status_code == 200
    body = response.json()
    assert body["accepted"] == 0
    assert body["failed"] == 1
    assert "occurred_at must be ISO datetime" in body["errors"][0]["error"]


def _downloader_daily_meeting_source_event_fixture() -> dict:
    return {
        "schema_version": "source_event.v1",
        "event_id": "evt-daily-meeting-fixture-001",
        "idempotency_key": "dcp:safePages:meetingListAdmin:queryToolBoxTalkListPagePc:tb-001",
        "source_system": "dcp",
        "source_event_type": "dcp.record",
        "event_granularity": "record",
        "source_record_id": "tb-001",
        "source_record_hash": "hash-daily-meeting-fixture-001",
        "occurred_at": "2026-05-03T08:30:00+08:00",
        "collected_at": "2026-05-03T21:30:12+08:00",
        "payload_schema": "dcp.raw_record.v1",
        "payload": {
            "raw": {
                "id": "tb-001",
                "prjCode": "1716D0230007",
                "workDate": "2026-05-03",
                "topic": "站班会",
            }
        },
        "source_ref": {
            "collector": "vibe-downloader",
            "run_id": "20260503_213000",
            "collection": "safePages",
            "page_name": "meetingListAdmin",
            "api_name": "queryToolBoxTalkListPagePc",
            "raw_data_index": 0,
            "record_index": 0,
            "record_path": "raw_data[0].records[0]",
            "source_file": "safePages/meetingListAdmin/2026-05-03.json",
        },
    }


def test_downloader_daily_meeting_source_event_fixture_ingests_as_daily_meeting() -> None:
    store = _make_store()
    event = _downloader_daily_meeting_source_event_fixture()

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    assert response.status_code == 200
    body = response.json()
    assert body["accepted"] == 1
    saved = store.get_raw_event_by_idempotency_key(event["idempotency_key"])
    assert saved["dataset_key"] == "daily_meeting"


def test_source_ref_dataset_key_overrides_fallback_resolver() -> None:
    store = _make_store()
    event = _event_for_source_ref("explicit", "projectPages", "杆塔信息", "tower_details")
    event["source_ref"]["dataset_key"] = "tower"

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    assert response.status_code == 200
    saved = store.get_raw_event_by_idempotency_key(event["idempotency_key"])
    assert saved["dataset_key"] == "tower"


def test_line_section_enabled_true_ingests_successfully() -> None:
    store = _make_store()
    runtime = store.get_plugin_runtime_config("dcp")["config"]
    runtime["datasets"]["line_section"]["enabled"] = True
    runtime["enabled_datasets"] = [
        "daily_meeting",
        "tower",
        "station",
        "line_section",
    ]
    store.save_plugin_runtime_config("dcp", runtime)
    event = _event_for_source_ref(
        "line-section-enabled",
        "projectPages",
        "区段划分",
        "section_details",
    )

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    assert response.status_code == 200
    assert response.json()["accepted"] == 1
    saved = store.get_raw_event_by_idempotency_key(event["idempotency_key"])
    assert saved["dataset_key"] == "line_section"


def test_line_section_disabled_fails_ingestion() -> None:
    store = _make_store()
    runtime = store.get_plugin_runtime_config("dcp")["config"]
    runtime["datasets"]["line_section"]["enabled"] = False
    runtime["enabled_datasets"] = [
        "daily_meeting",
        "tower",
        "station",
        "line_section",
    ]
    store.save_plugin_runtime_config("dcp", runtime)
    event = _event_for_source_ref(
        "line-section-disabled",
        "projectPages",
        "区段划分",
        "section_details",
    )

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    body = response.json()
    assert body["accepted"] == 0
    assert body["failed"] == 1
    assert "dataset_key=line_section" in body["errors"][0]["error"]
    assert "disabled" in body["errors"][0]["error"]
    assert store.count_raw_events() == 0


def test_line_section_not_in_enabled_datasets_fails_ingestion() -> None:
    store = _make_store()
    runtime = store.get_plugin_runtime_config("dcp")["config"]
    runtime["enabled_datasets"] = [
        dataset
        for dataset in runtime["enabled_datasets"]
        if dataset != "line_section"
    ]
    store.save_plugin_runtime_config("dcp", runtime)
    event = _event_for_source_ref(
        "line-section-not-enabled",
        "projectPages",
        "区段划分",
        "section_details",
    )

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    body = response.json()
    assert body["accepted"] == 0
    assert body["failed"] == 1
    assert "dataset_key=line_section" in body["errors"][0]["error"]
    assert "enabled_datasets" in body["errors"][0]["error"]
    assert store.count_raw_events() == 0


def test_year_progress_default_ingests_successfully() -> None:
    store = _make_store()
    event = _event_for_source_ref(
        "year-progress-default",
        "planPages",
        "年度进度计划分析",
        "yearly_progress_analysis",
    )

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    assert response.status_code == 200
    assert response.json()["accepted"] == 1
    saved = store.get_raw_event_by_idempotency_key(event["idempotency_key"])
    assert saved["dataset_key"] == "year_progress"


def test_year_progress_ingests_with_old_three_dataset_runtime_config() -> None:
    store = _make_store()
    store.save_plugin_runtime_config(
        "dcp",
        {
            "enabled_datasets": ["daily_meeting", "tower", "station"],
            "monitor_datasets": ["daily_meeting", "tower", "station"],
            "datasets": {
                "daily_meeting": {
                    "enabled": True,
                    "collection": "safePages",
                    "scope": "date_partitioned",
                    "page_name": "meetingListAdmin",
                    "api_names": ["queryToolBoxTalkListPagePc"],
                },
                "tower": {
                    "enabled": True,
                    "collection": "projectPages",
                    "scope": "project_single",
                    "page_name": "杆塔信息",
                    "api_names": ["tower_details"],
                },
                "station": {
                    "enabled": True,
                    "collection": "projectPages",
                    "scope": "project_single",
                    "page_name": "变电站坐标",
                    "api_names": ["substation_coordinates"],
                },
            },
        },
    )
    event = _event_for_source_ref(
        "year-progress-old-runtime",
        "planPages",
        "年度进度计划分析",
        "yearly_progress_analysis",
    )

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    assert response.status_code == 200
    assert response.json()["accepted"] == 1
    saved = store.get_raw_event_by_idempotency_key(event["idempotency_key"])
    assert saved["dataset_key"] == "year_progress"


def test_unknown_source_ref_dataset_key_fails_dcp_ingestion() -> None:
    store = _make_store()
    event = _event_for_source_ref("unknown-explicit", "projectPages", "杆塔信息", "tower_details")
    event["source_ref"]["dataset_key"] = "unknown_dataset"

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    body = response.json()
    assert body["accepted"] == 0
    assert body["failed"] == 1
    assert "dataset_key=unknown_dataset" in body["errors"][0]["error"]
    assert "not defined" in body["errors"][0]["error"]
    assert store.count_raw_events() == 0


def test_source_ref_dataset_key_must_match_dataset_config() -> None:
    store = _make_store()
    event = _event_for_source_ref("explicit-tower", "projectPages", "区段划分", "section_details")
    event["source_ref"]["dataset_key"] = "tower"

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    body = response.json()
    assert body["accepted"] == 0
    assert body["failed"] == 1
    assert "dataset_key=tower" in body["errors"][0]["error"]
    assert "source_ref mismatch" in body["errors"][0]["error"]
    assert store.count_raw_events() == 0


def test_dcp_ingestion_does_not_use_fallback_when_runtime_config_mismatch() -> None:
    store = _make_store()
    runtime = store.get_plugin_runtime_config("dcp")["config"]
    runtime["datasets"]["tower"]["page_name"] = "不匹配杆塔页面"
    store.save_plugin_runtime_config("dcp", runtime)
    event = _event_for_source_ref("runtime-mismatch", "projectPages", "杆塔信息", "tower_details")

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    body = response.json()
    assert body["accepted"] == 0
    assert body["failed"] == 1
    assert "dataset_key=None" in body["errors"][0]["error"]
    assert store.count_raw_events() == 0


def test_dcp_event_without_source_record_hash_fails() -> None:
    store = _make_store()
    event = _event_for_source_ref("missing-hash", "projectPages", "杆塔信息", "tower_details")
    event["source_record_hash"] = None

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    body = response.json()
    assert body["accepted"] == 0
    assert body["failed"] == 1
    assert "DCP event requires source_record_hash" in body["errors"][0]["error"]
    assert store.count_raw_events() == 0


def test_dcp_ingestion_fails_when_plugin_disabled() -> None:
    store = _make_store()
    plugin = store.get_plugin("dcp")
    store.save_plugin(
        plugin_id=plugin["id"],
        name=plugin["name"],
        version=plugin["version"],
        description=plugin["description"],
        author=plugin["author"],
        tags=plugin["tags"],
        config_schema=plugin["config"],
        collection_mode=plugin["collection_mode"],
        plugin_kind=plugin["plugin_kind"],
        execution_mode=plugin["execution_mode"],
        enabled=False,
    )
    event = _event_for_source_ref("plugin-disabled", "projectPages", "杆塔信息", "tower_details")

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    body = response.json()
    assert body["accepted"] == 0
    assert body["failed"] == 1
    assert "dcp plugin is disabled" in body["errors"][0]["error"]
    assert store.count_raw_events() == 0


def test_dcp_ingestion_rejects_non_record_granularity() -> None:
    store = _make_store()
    event = _event_for_source_ref("non-record", "projectPages", "杆塔信息", "tower_details")
    event["event_granularity"] = "api_result"

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    body = response.json()
    assert body["accepted"] == 0
    assert body["failed"] == 1
    assert "DCP event_granularity must be record" in body["errors"][0]["error"]
    assert store.count_raw_events() == 0


def test_list_raw_events_by_source_record_key_returns_all_versions() -> None:
    store = _make_store()
    client = _client(store)
    first_event = _event_for_source_ref("versions", "projectPages", "杆塔信息", "tower_details")
    second_event = deepcopy(first_event)
    second_event["event_id"] = "evt-versions-second"
    second_event["source_record_hash"] = "hash-versions-second"
    second_event["collected_at"] = "2026-05-03T22:30:12+08:00"

    client.post("/ingestion/v1/events", json={"events": [first_event]})
    client.post("/ingestion/v1/events", json={"events": [second_event]})

    versions = store.list_raw_events_by_source_record_key(first_event["idempotency_key"])
    assert len(versions) == 2
    assert {version["source_record_hash"] for version in versions} == {
        first_event["source_record_hash"],
        second_event["source_record_hash"],
    }
    assert store.get_raw_event_by_raw_event_key(
        f"{first_event['idempotency_key']}:{second_event['source_record_hash']}"
    )["event_id"] == "evt-versions-second"


def test_get_latest_raw_event_by_source_record_key_returns_newest_version() -> None:
    store = _make_store()
    client = _client(store)
    first_event = _event_for_source_ref("latest", "projectPages", "杆塔信息", "tower_details")
    first_event["collected_at"] = "2026-05-03T22:30:12+08:00"
    second_event = deepcopy(first_event)
    second_event["event_id"] = "evt-latest-older"
    second_event["source_record_hash"] = "hash-latest-older"
    second_event["collected_at"] = "2026-05-03T21:30:12+08:00"

    client.post("/ingestion/v1/events", json={"events": [second_event]})
    client.post("/ingestion/v1/events", json={"events": [first_event]})

    latest = store.get_latest_raw_event_by_source_record_key(first_event["idempotency_key"])
    assert latest["event_id"] == first_event["event_id"]
    assert latest["source_record_hash"] == first_event["source_record_hash"]


def test_plan_pages_non_year_progress_does_not_resolve_to_year_progress() -> None:
    store = _make_store()
    event = _event_for_source_ref(
        "other-plan",
        "planPages",
        "其他计划页面",
        "other_plan_api",
    )

    response = _client(store).post("/ingestion/v1/events", json={"events": [event]})

    body = response.json()
    assert body["accepted"] == 0
    assert body["failed"] == 1
    assert "dataset_key=None" in body["errors"][0]["error"]
    assert store.count_raw_events() == 0
