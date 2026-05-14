from datetime import datetime
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

import api.server as server
from core.plugin_manager import PluginManager
import processing.normalizer_runner as normalizer_runner
from processing.normalizer_runner import NormalizerRunner
from storage.sqlite_store import SQLiteStore
from conftest import seed_test_records


def _make_store() -> SQLiteStore:
    artifacts_dir = Path(__file__).resolve().parent / ".artifacts"
    artifacts_dir.mkdir(exist_ok=True)
    db_path = artifacts_dir / f"processing-{uuid4().hex}.db"
    store = SQLiteStore(db_path)
    store.init_schema()
    manager = PluginManager()
    manager.discover_plugins()
    manager.save_discovered_plugins(store)
    return store


def _client(store: SQLiteStore) -> TestClient:
    server.store = store
    return TestClient(server.app)


def _epoch(timestamp: str) -> float:
    return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).timestamp()


def _station_event(
    suffix: str = "001",
    station_id: str | None = None,
    longitude: str = "112.9279",
    latitude: str = "28.2147",
    collected_at: str = "2026-05-03T21:30:12+08:00",
) -> dict:
    station_id = station_id or f"station-{suffix}"
    return {
        "raw_event_id": f"raw-station-{suffix}",
        "idempotency_key": f"dcp:projectPages:变电站坐标:substation_coordinates:station-{suffix}",
        "source_system": "dcp",
        "source_record_id": f"station-{suffix}",
        "source_record_hash": f"hash-station-{suffix}",
        "occurred_at": "2026-05-03T21:30:12+08:00",
        "collected_at": collected_at,
        "payload": {
            "raw": {
                "id": station_id,
                "prjCode": "PRJ-001",
                "singleProjectCode": "SP-001",
                "longitude": longitude,
                "latitude": latitude,
                "extra": "kept in canonical raw attributes",
            }
        },
        "source_ref": {
            "collector": "vibe-downloader",
            "run_id": "20260503_213000",
            "collection": "projectPages",
            "page_name": "变电站坐标",
            "api_name": "substation_coordinates",
            "raw_data_index": 0,
            "record_index": 0,
            "record_path": "raw_data[0].records[0]",
            "source_file": f"projectPages/变电站坐标/station-{suffix}.json",
        },
    }


def _daily_meeting_event(
    suffix: str = "001", work_date: str | int = "2026-05-03"
) -> dict:
    source_file_date = "2026-05-03"
    if isinstance(work_date, str) and len(work_date) >= 10:
        source_file_date = work_date[:10]
    elif isinstance(work_date, int):
        source_file_date = datetime.fromtimestamp(work_date / 1000).date().isoformat()
    return {
        "raw_event_id": f"raw-daily-meeting-{suffix}",
        "idempotency_key": f"dcp:safePages:meetingListAdmin:queryToolBoxTalkListPagePc:meeting-{suffix}",
        "source_system": "dcp",
        "source_record_id": f"meeting-{suffix}",
        "source_record_hash": f"hash-daily-meeting-{suffix}",
        "occurred_at": "2026-05-03T08:30:00+08:00",
        "collected_at": "2026-05-03T21:30:12+08:00",
        "payload": {
            "raw": {
                "id": f"meeting-{suffix}",
                "projectName": "示例工程",
                "toolBoxTalkLongitude": "112.9388",
                "toolBoxTalkLatitude": "28.2282",
                "personCount": 12,
                "riskLevel": "medium",
                "workStatus": "working",
                "voltageLevel": "500kV",
                "city": "长沙",
                "workDate": work_date,
                "rawOnly": "not exposed",
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
            "source_file": f"safePages/daily_meeting/{source_file_date}.json",
        },
    }


def _tower_event(
    suffix: str = "001",
    api_name: str = "tower_details",
    include_id: bool = True,
) -> dict:
    raw = {
        "singleProjectCode": "SP-TOWER-001",
        "biddingSectionCode": "BS-001",
        "towerNo": f"T-{suffix}",
        "upstreamTowerNo": "T-000",
        "longitudeEdit": "112.9451",
        "latitudeEdit": "28.2311",
        "longitude": "0",
        "latitude": "0",
        "towerType": "linear",
        "towerFullHeight": "66.5",
        "nominalHeight": "54",
        "rawOnly": "not exposed",
    }
    if include_id:
        raw["id"] = f"tower-{suffix}"
    return {
        "raw_event_id": f"raw-tower-{suffix}",
        "idempotency_key": f"dcp:projectPages:杆塔信息:{api_name}:tower-{suffix}",
        "source_system": "dcp",
        "source_record_id": f"tower-{suffix}",
        "source_record_hash": f"hash-tower-{suffix}",
        "occurred_at": "2026-05-03T08:30:00+08:00",
        "collected_at": "2026-05-03T21:30:12+08:00",
        "payload": {"raw": raw},
        "source_ref": {
            "collector": "vibe-downloader",
            "run_id": "20260503_213000",
            "collection": "projectPages",
            "page_name": "杆塔信息",
            "api_name": api_name,
            "raw_data_index": 0,
            "record_index": 0,
            "record_path": "raw_data[0].records[0]",
            "source_file": f"projectPages/杆塔信息/{suffix}.json",
        },
    }


def test_station_batch_record_to_sandbox_skeleton_flow():
    store = _make_store()
    client = _client(store)
    event = _station_event()

    seed_test_records(store, "station", [event])

    processing = client.post("/processing/v1/run", json={"dataset_key": "station"})
    assert processing.status_code == 200
    assert processing.json()["processed"] == 1
    assert processing.json()["failed"] == 0

    entities = store.list_canonical_entities(entity_type="station", dataset_key="station")
    assert len(entities) == 1
    entity = entities[0]
    assert entity["entity_key"] == "dcp:station:SP-001"
    assert entity["attributes"]["project_code"] == "PRJ-001"
    assert entity["attributes"]["single_project_code"] == "SP-001"
    assert entity["attributes"]["dcp_coordinate_id"] == "station-001"
    assert entity["attributes"]["longitude"] == 112.9279
    assert entity["attributes"]["latitude"] == 28.2147
    assert entity["attributes"]["raw"]["extra"] == "kept in canonical raw attributes"
    assert entity["latest_source_record_hash"] == "hash-station-001"
    assert entity["source_refs"] == [
        {
            "source_system": "dcp",
            "dataset_key": "station",
            "source_record_key": event["idempotency_key"],
            "source_record_id": "station-001",
            "source_record_hash": "hash-station-001",
            "raw_event_id": entity["latest_raw_event_id"],
        }
    ]

    skeleton = client.get("/api/v1/sandbox/map/skeleton?limit=100")
    assert skeleton.status_code == 200
    body = skeleton.json()
    assert body["meta"] == {
        "limit": 100,
        "stations_count": 1,
        "towers_count": 0,
        "truncated": False,
    }
    assert body["towers"] == []
    assert body["lines"] == []
    assert body["stations"] == [
        {
            "id": "dcp:station:SP-001",
            "project_code": "PRJ-001",
            "single_project_code": "SP-001",
            "longitude": 112.9279,
            "latitude": 28.2147,
        }
    ]


def test_daily_meeting_batch_record_to_work_point_and_summary():
    store = _make_store()
    client = _client(store)
    event = _daily_meeting_event()

    seed_test_records(store, "daily_meeting", [event])

    processing = client.post(
        "/processing/v1/run", json={"dataset_key": "daily_meeting"}
    )
    assert processing.status_code == 200
    assert processing.json()["processed"] == 1
    assert processing.json()["inserted"] == 1

    entities = store.list_canonical_entities(
        entity_type="work_point", dataset_key="daily_meeting"
    )
    assert len(entities) == 1
    entity = entities[0]
    assert entity["entity_key"] == "dcp:work_point:2026-05-03:meeting-001"
    assert entity["entity_date"] == "2026-05-03"
    assert entity["attributes"]["project_name"] == "示例工程"
    assert entity["attributes"]["longitude"] == 112.9388
    assert entity["attributes"]["latitude"] == 28.2282
    assert entity["attributes"]["person_count"] == 12
    assert entity["attributes"]["raw"]["rawOnly"] == "not exposed"

    summary = client.get("/api/v1/sandbox/map/summary")
    assert summary.status_code == 200
    body = summary.json()
    assert body["meta"] == {
        "date": "2026-05-03",
        "limit": 10000,
        "work_points_count": 1,
        "truncated": False,
    }
    assert body["work_points"] == [
        {
            "id": "dcp:work_point:2026-05-03:meeting-001",
            "project_name": "示例工程",
            "longitude": 112.9388,
            "latitude": 28.2282,
            "person_count": 12,
            "risk_level": "3",
            "work_status": "working",
            "voltage_level": "500kV",
            "city": "长沙",
            "work_date": "2026-05-03",
        }
    ]
    assert "raw" not in body["work_points"][0]


def test_daily_meeting_same_id_on_different_dates_creates_two_work_points():
    store = _make_store()
    first = _daily_meeting_event("same-id-1", work_date="2026-05-03")
    second = _daily_meeting_event("same-id-2", work_date="2026-05-04")
    first["payload"]["raw"]["id"] = "meeting-same"
    second["payload"]["raw"]["id"] = "meeting-same"
    seed_test_records(store, "daily_meeting", [first, second])

    result = NormalizerRunner(store).run("daily_meeting")

    entities = store.list_canonical_entities(entity_type="work_point", limit=10)
    assert result["processed"] == 2
    assert {entity["entity_key"] for entity in entities} == {
        "dcp:work_point:2026-05-03:meeting-same",
        "dcp:work_point:2026-05-04:meeting-same",
    }


def test_sandbox_summary_filters_by_date_and_defaults_to_latest_date():
    store = _make_store()
    client = _client(store)
    first = _daily_meeting_event("date-1", work_date="2026-05-03")
    second = _daily_meeting_event("date-2", work_date="2026-05-04")
    first["payload"]["raw"]["projectName"] = "三号作业"
    second["payload"]["raw"]["projectName"] = "四号作业"
    seed_test_records(store, "daily_meeting", [first, second])
    NormalizerRunner(store).run("daily_meeting")

    filtered = client.get("/api/v1/sandbox/map/summary?date=2026-05-03")
    latest = client.get("/api/v1/sandbox/map/summary")

    assert filtered.status_code == 200
    assert filtered.json()["meta"]["date"] == "2026-05-03"
    assert filtered.json()["work_points"] == [
        {
            "id": "dcp:work_point:2026-05-03:meeting-date-1",
            "project_name": "三号作业",
            "longitude": 112.9388,
            "latitude": 28.2282,
            "person_count": 12,
            "risk_level": "3",
            "work_status": "working",
            "voltage_level": "500kV",
            "city": "长沙",
            "work_date": "2026-05-03",
        }
    ]
    assert latest.status_code == 200
    assert latest.json()["meta"]["date"] == "2026-05-04"
    assert latest.json()["work_points"][0]["project_name"] == "四号作业"


def test_sandbox_dates_returns_daily_meeting_dates_in_ascending_order():
    store = _make_store()
    client = _client(store)
    first = _daily_meeting_event("date-list-1", work_date="2026-05-04")
    second = _daily_meeting_event("date-list-2", work_date="2026-05-03")
    third = _daily_meeting_event("date-list-3", work_date="2026-05-04")
    first["payload"]["raw"]["id"] = "meeting-date-list-1"
    second["payload"]["raw"]["id"] = "meeting-date-list-2"
    third["payload"]["raw"]["id"] = "meeting-date-list-3"
    seed_test_records(store, "daily_meeting", [first, second, third])
    NormalizerRunner(store).run("daily_meeting")

    response = client.get("/api/v1/sandbox/dates")

    assert response.status_code == 200
    assert response.json() == {
        "dates": ["2026-05-03", "2026-05-04"],
        "latest_date": "2026-05-04",
        "count": 2,
    }


def test_sandbox_dates_returns_empty_when_no_work_points_exist():
    store = _make_store()
    client = _client(store)
    seed_test_records(store, "station", [_station_event("dates-station-only")])
    NormalizerRunner(store).run("station")

    response = client.get("/api/v1/sandbox/dates")

    assert response.status_code == 200
    assert response.json() == {
        "dates": [],
        "latest_date": None,
        "count": 0,
    }


def test_daily_meeting_maps_current_monitor_fields():
    store = _make_store()
    event = _daily_meeting_event("field-map")
    raw = event["payload"]["raw"]
    raw["currentConstrHeadcount"] = "18"
    raw["reAssessmentRiskLevel"] = "high"
    raw["currentConstructionStatus"] = "paused"
    raw["buildUnitName"] = "长沙"
    seed_test_records(store, "daily_meeting", [event])

    result = NormalizerRunner(store).run("daily_meeting")

    entity = store.list_canonical_entities(entity_type="work_point")[0]
    assert result["processed"] == 1
    assert entity["attributes"]["person_count"] == 18
    assert entity["attributes"]["risk_level"] == "2"
    assert entity["attributes"]["work_status"] == "paused"
    assert entity["attributes"]["city"] == "长沙"


def test_daily_meeting_normalizes_invalid_count_and_unknown_values():
    store = _make_store()
    event = _daily_meeting_event("stable-values")
    raw = event["payload"]["raw"]
    raw["currentConstrHeadcount"] = "not-a-number"
    raw["reAssessmentRiskLevel"] = "very risky"
    raw["currentConstructionStatus"] = "strange status"
    seed_test_records(store, "daily_meeting", [event])

    result = NormalizerRunner(store).run("daily_meeting")

    entity = store.list_canonical_entities(entity_type="work_point")[0]
    assert result["processed"] == 1
    assert entity["attributes"]["person_count"] == 0
    assert entity["attributes"]["risk_level"] == "unknown"
    assert entity["attributes"]["work_status"] == "unknown"


def test_daily_meeting_normalizes_datetime_work_date():
    store = _make_store()
    event = _daily_meeting_event("datetime-date", work_date="2026-05-03 18:20:30")
    seed_test_records(store, "daily_meeting", [event])

    result = NormalizerRunner(store).run("daily_meeting")

    entity = store.list_canonical_entities(entity_type="work_point")[0]
    assert result["processed"] == 1
    assert entity["entity_key"] == "dcp:work_point:2026-05-03:meeting-datetime-date"
    assert entity["entity_date"] == "2026-05-03"
    assert entity["attributes"]["work_date"] == "2026-05-03"


def test_daily_meeting_normalizes_epoch_millis_work_date():
    store = _make_store()
    epoch_ms = int(
        datetime.fromisoformat("2026-05-03T12:00:00+00:00").timestamp() * 1000
    )
    event = _daily_meeting_event("epoch-date", work_date=epoch_ms)
    seed_test_records(store, "daily_meeting", [event])

    result = NormalizerRunner(store).run("daily_meeting")

    entity = store.list_canonical_entities(entity_type="work_point")[0]
    assert result["processed"] == 1
    assert entity["entity_date"] == "2026-05-03"
    assert entity["attributes"]["work_date"] == "2026-05-03"


def test_daily_meeting_numeric_risk_levels_are_stable_values():
    store = _make_store()
    events = []
    for risk_level in [1, "2", 3, "4"]:
        event = _daily_meeting_event(f"risk-{risk_level}")
        event["payload"]["raw"]["riskLevel"] = risk_level
        events.append(event)
    seed_test_records(store, "daily_meeting", events)

    result = NormalizerRunner(store).run("daily_meeting")

    entities = store.list_canonical_entities(entity_type="work_point", limit=10)
    assert result["processed"] == 4
    assert {entity["attributes"]["risk_level"] for entity in entities} == {
        "1",
        "2",
        "3",
        "4",
    }


def test_daily_meeting_named_risk_levels_follow_monitor_scale():
    store = _make_store()
    cases = {
        "critical": "1",
        "重大风险": "1",
        "特高风险": "1",
        "high": "2",
        "高风险": "2",
        "medium": "3",
        "中风险": "3",
        "low": "4",
        "低风险": "4",
        "一般风险": "4",
    }
    events = []
    for source_value in cases:
        event = _daily_meeting_event(f"risk-name-{source_value}")
        event["payload"]["raw"]["riskLevel"] = source_value
        events.append(event)
    seed_test_records(store, "daily_meeting", events)

    result = NormalizerRunner(store).run("daily_meeting")

    entities = store.list_canonical_entities(entity_type="work_point", limit=20)
    risk_by_id = {
        entity["entity_key"].split(":")[-1]: entity["attributes"]["risk_level"]
        for entity in entities
    }
    assert result["processed"] == len(cases)
    for source_value, expected in cases.items():
        assert risk_by_id[f"meeting-risk-name-{source_value}"] == expected


def test_invalid_coordinates_are_skipped_by_dcp_normalizers():
    store = _make_store()
    daily = _daily_meeting_event("bad-daily-coordinate")
    tower = _tower_event("bad-tower-coordinate")
    station = _station_event("bad-station-coordinate")
    daily["payload"]["raw"]["toolBoxTalkLongitude"] = "nan"
    tower["payload"]["raw"]["longitudeEdit"] = "nan"
    station["payload"]["raw"]["longitude"] = "nan"
    seed_test_records(store, "daily_meeting", [daily])
    seed_test_records(store, "tower", [tower])
    seed_test_records(store, "station", [station])

    daily_result = NormalizerRunner(store).run("daily_meeting")
    tower_result = NormalizerRunner(store).run("tower")
    station_result = NormalizerRunner(store).run("station")

    assert daily_result["processed"] == 0
    assert daily_result["skipped"] == 1
    assert "invalid toolBoxTalkLongitude/toolBoxTalkLatitude" in daily_result["errors"][0]
    assert tower_result["processed"] == 0
    assert tower_result["skipped"] == 1
    assert "invalid longitudeEdit/latitudeEdit" in tower_result["errors"][0]
    assert station_result["processed"] == 0
    assert station_result["skipped"] == 1
    assert "invalid longitude/latitude" in station_result["errors"][0]


def test_non_hunan_coordinates_do_not_reach_sandbox_apis():
    store = _make_store()
    client = _client(store)
    daily = _daily_meeting_event("outside-hunan", work_date="2026-05-03")
    tower = _tower_event("outside-hunan")
    station = _station_event("outside-hunan")
    daily["payload"]["raw"]["toolBoxTalkLongitude"] = "116.391"
    daily["payload"]["raw"]["toolBoxTalkLatitude"] = "39.907"
    tower["payload"]["raw"]["longitudeEdit"] = "116.391"
    tower["payload"]["raw"]["latitudeEdit"] = "39.907"
    station["payload"]["raw"]["longitude"] = "116.391"
    station["payload"]["raw"]["latitude"] = "39.907"

    seed_test_records(store, "daily_meeting", [daily])
    seed_test_records(store, "tower", [tower])
    seed_test_records(store, "station", [station])

    daily_result = NormalizerRunner(store).run("daily_meeting")
    tower_result = NormalizerRunner(store).run("tower")
    station_result = NormalizerRunner(store).run("station")

    assert daily_result["processed"] == 0
    assert tower_result["processed"] == 0
    assert station_result["processed"] == 0
    assert "coordinate outside hunan range" in daily_result["errors"][0]
    assert "coordinate outside hunan range" in tower_result["errors"][0]
    assert "coordinate outside hunan range" in station_result["errors"][0]
    assert client.get("/api/v1/sandbox/map/summary?date=2026-05-03").json()[
        "work_points"
    ] == []
    skeleton = client.get("/api/v1/sandbox/map/skeleton").json()
    assert skeleton["stations"] == []
    assert skeleton["towers"] == []


def test_strict_hunan_boundary_filters_formerly_loose_coordinates():
    store = _make_store()
    client = _client(store)
    daily = _daily_meeting_event("loose-longitude", work_date="2026-05-03")
    tower = _tower_event("loose-latitude")
    station = _station_event("loose-station")
    daily["payload"]["raw"]["toolBoxTalkLongitude"] = "108.2"
    daily["payload"]["raw"]["toolBoxTalkLatitude"] = "28.0"
    tower["payload"]["raw"]["longitudeEdit"] = "112.0"
    tower["payload"]["raw"]["latitudeEdit"] = "30.8"
    station["payload"]["raw"]["longitude"] = "108.2"
    station["payload"]["raw"]["latitude"] = "28.0"

    seed_test_records(store, "daily_meeting", [daily])
    seed_test_records(store, "tower", [tower])
    seed_test_records(store, "station", [station])

    daily_result = NormalizerRunner(store).run("daily_meeting")
    tower_result = NormalizerRunner(store).run("tower")
    station_result = NormalizerRunner(store).run("station")

    assert daily_result["processed"] == 0
    assert tower_result["processed"] == 0
    assert station_result["processed"] == 0
    assert "coordinate outside hunan range" in daily_result["errors"][0]
    assert "coordinate outside hunan range" in tower_result["errors"][0]
    assert "coordinate outside hunan range" in station_result["errors"][0]
    assert client.get("/api/v1/sandbox/map/summary?date=2026-05-03").json()[
        "work_points"
    ] == []
    skeleton = client.get("/api/v1/sandbox/map/skeleton").json()
    assert skeleton["stations"] == []
    assert skeleton["towers"] == []


def test_downloader_realistic_batch_records_feed_sandbox_apis():
    store = _make_store()
    client = _client(store)
    daily = _daily_meeting_event("real-daily", work_date="2026-05-05")
    daily["payload"]["raw"].update(
        {
            "projectName": "湖南特高压作业点",
            "toolBoxTalkLongitude": "112.9388",
            "toolBoxTalkLatitude": "28.2282",
            "currentConstrHeadcount": "26",
            "reAssessmentRiskLevel": "高",
            "currentConstructionStatus": "施工中",
            "buildUnitName": "长沙",
        }
    )
    tower = _tower_event("real-tower")
    tower["payload"]["raw"].update(
        {
            "id": "TW-HN-001",
            "singleProjectCode": "SP-HN-001",
            "biddingSectionCode": "BD-HN-001",
            "towerNo": "N101",
            "longitudeEdit": "112.9451",
            "latitudeEdit": "28.2311",
        }
    )
    station = _station_event(
        suffix="real-station",
        station_id="ST-HN-001",
        longitude="112.9279",
        latitude="28.2147",
    )

    seed_test_records(store, "daily_meeting", [daily])
    seed_test_records(store, "tower", [tower])
    seed_test_records(store, "station", [station])

    assert NormalizerRunner(store).run("daily_meeting")["processed"] == 1
    assert NormalizerRunner(store).run("tower")["processed"] == 1
    assert NormalizerRunner(store).run("station")["processed"] == 1

    summary = client.get("/api/v1/sandbox/map/summary?date=2026-05-05")
    skeleton = client.get("/api/v1/sandbox/map/skeleton")

    assert summary.status_code == 200
    work_point = summary.json()["work_points"][0]
    assert work_point["project_name"] == "湖南特高压作业点"
    assert work_point["person_count"] == 26
    assert work_point["risk_level"] == "2"
    assert work_point["work_status"] == "working"
    assert work_point["city"] == "长沙"
    assert "raw" not in work_point

    assert skeleton.status_code == 200
    body = skeleton.json()
    assert body["meta"]["towers_count"] == 1
    assert body["meta"]["stations_count"] == 1
    assert body["towers"][0]["id"] == "dcp:tower:SP-HN-001:BD-HN-001:N101"
    assert body["towers"][0]["longitude"] == 112.9451
    assert body["stations"][0]["longitude"] == 112.9279
    assert "raw" not in body["towers"][0]
    assert "raw" not in body["stations"][0]


def test_tower_details_batch_record_to_tower_canonical():
    store = _make_store()
    client = _client(store)
    event = _tower_event()

    seed_test_records(store, "tower", [event])

    result = NormalizerRunner(store).run("tower")

    assert result["processed"] == 1
    assert result["inserted"] == 1
    entities = store.list_canonical_entities(entity_type="tower", dataset_key="tower")
    assert len(entities) == 1
    entity = entities[0]
    assert entity["entity_key"] == "dcp:tower:SP-TOWER-001:BS-001:T-001"
    assert entity["attributes"]["tower_id"] == "tower-001"
    assert entity["attributes"]["dcp_entity_key_fallback"] == "dcp:tower:tower-001"
    assert entity["attributes"]["longitude"] == 112.9451
    assert entity["attributes"]["latitude"] == 28.2311
    assert entity["attributes"]["raw"]["rawOnly"] == "not exposed"


def test_tower_entity_key_falls_back_when_raw_id_missing():
    store = _make_store()
    event = _tower_event("fallback", include_id=False)
    seed_test_records(store, "tower", [event])

    result = NormalizerRunner(store).run("tower")

    entities = store.list_canonical_entities(entity_type="tower", dataset_key="tower")
    assert result["processed"] == 1
    assert entities[0]["entity_key"] == "dcp:tower:SP-TOWER-001:BS-001:T-fallback"
    assert entities[0]["attributes"]["single_project_code"] == "SP-TOWER-001"
    assert entities[0]["attributes"]["bidding_section_code"] == "BS-001"
    assert entities[0]["attributes"]["tower_no"] == "T-fallback"


def test_tower_single_projects_is_skipped_by_tower_normalizer():
    store = _make_store()
    client = _client(store)
    event = _tower_event("single-project", api_name="tower_single_projects")

    seed_test_records(store, "tower", [event])

    result = NormalizerRunner(store).run("tower")

    assert result["processed"] == 0
    assert result["skipped"] == 1
    assert "not tower_details api" in result["errors"][0]
    assert store.list_canonical_entities(entity_type="tower", dataset_key="tower") == []


def test_sandbox_skeleton_returns_towers_and_stations_without_raw():
    store = _make_store()
    client = _client(store)
    station_event = _station_event()
    tower_event = _tower_event()

    seed_test_records(store, "station", [station_event])
    seed_test_records(store, "tower", [tower_event])
    assert NormalizerRunner(store).run("station")["processed"] == 1
    assert NormalizerRunner(store).run("tower")["processed"] == 1

    response = client.get("/api/v1/sandbox/map/skeleton")

    assert response.status_code == 200
    body = response.json()
    assert body["meta"]["stations_count"] == 1
    assert body["meta"]["towers_count"] == 1
    assert body["lines"] == []
    assert body["stations"][0]["id"] == "dcp:station:SP-001"
    assert body["towers"][0]["id"] == "dcp:tower:SP-TOWER-001:BS-001:T-001"
    assert body["towers"][0]["longitude"] == 112.9451
    assert body["towers"][0]["latitude"] == 28.2311
    assert "raw" not in body["stations"][0]
    assert "raw" not in body["towers"][0]


def test_tower_entity_key_uses_scoped_format_even_when_raw_id_exists():
    store = _make_store()
    event = _tower_event("scoped-with-id", include_id=True)
    event["payload"]["raw"]["id"] = "tower-scoped-with-id"
    event["payload"]["raw"]["singleProjectCode"] = "S01"
    event["payload"]["raw"]["biddingSectionCode"] = "B01"
    event["payload"]["raw"]["towerNo"] = "G1"
    seed_test_records(store, "tower", [event])

    result = NormalizerRunner(store).run("tower")

    assert result["processed"] == 1
    entity = store.list_canonical_entities(entity_type="tower", dataset_key="tower")[0]
    assert entity["entity_key"] == "dcp:tower:S01:B01:G1"
    assert entity["attributes"]["dcp_entity_key_fallback"] == "dcp:tower:tower-scoped-with-id"


def test_processing_run_supports_station_from_registry():
    store = _make_store()
    client = _client(store)

    response = client.post("/processing/v1/run", json={"dataset_key": "station"})

    assert response.status_code == 200
    assert response.json()["processed"] == 0
    assert response.json()["failed"] == 0


def test_processing_run_unknown_dataset_returns_supported_dataset_keys():
    store = _make_store()
    client = _client(store)

    response = client.post("/processing/v1/run", json={"dataset_key": "unknown_dataset"})

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["error"] == "unsupported dataset_key: unknown_dataset"
    assert set(detail["supported_datasets"]) == {"daily_meeting", "tower", "station"}


def test_processing_job_create_returns_queued_job(monkeypatch):
    store = _make_store()
    client = _client(store)

    monkeypatch.setattr(server, "_run_processing_job", lambda **_kwargs: None)

    response = client.post(
        "/processing/v1/jobs",
        json={"dataset_key": "station", "mode": "incremental", "batch_size": 25},
    )

    assert response.status_code == 202
    body = response.json()
    assert body["job_id"].startswith("proc_")
    assert body["dataset_key"] == "station"
    assert body["mode"] == "incremental"
    assert body["batch_size"] == 25
    assert body["status"] == "queued"


@pytest.mark.parametrize(
    "dataset_key",
    ["project_preconstruction", "line_section", "year_progress"],
)
def test_processing_job_create_supports_domain_normalizers(dataset_key, monkeypatch):
    store = _make_store()
    client = _client(store)

    monkeypatch.setattr(server, "_run_processing_job", lambda **_kwargs: None)

    response = client.post(
        "/processing/v1/jobs",
        json={"dataset_key": dataset_key, "mode": "incremental"},
    )

    assert response.status_code == 202
    body = response.json()
    assert body["job_id"].startswith("proc_")
    assert body["dataset_key"] == dataset_key
    assert body["status"] == "queued"


def test_processing_job_get_returns_job(monkeypatch):
    store = _make_store()
    client = _client(store)
    monkeypatch.setattr(server, "_run_processing_job", lambda **_kwargs: None)

    created = client.post(
        "/processing/v1/jobs",
        json={"dataset_key": "daily_meeting"},
    ).json()
    response = client.get(f"/processing/v1/jobs/{created['job_id']}")

    assert response.status_code == 200
    assert response.json()["job_id"] == created["job_id"]
    assert response.json()["dataset_key"] == "daily_meeting"


def test_processing_job_unsupported_dataset_returns_400():
    store = _make_store()
    client = _client(store)

    response = client.post(
        "/processing/v1/jobs",
        json={"dataset_key": "unknown_dataset"},
    )

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["error"] == "unsupported dataset_key: unknown_dataset"
    assert set(detail["supported_datasets"]) == {
        "daily_meeting",
        "tower",
        "station",
        "project_hierarchy",
        "project_preconstruction",
        "line_section",
        "year_progress",
    }


def test_processing_job_conflicts_when_dataset_already_active():
    store = _make_store()
    client = _client(store)
    store.create_processing_job(
        job_id="proc-existing",
        dataset_key="tower",
        mode="incremental",
        batch_size=1000,
    )

    response = client.post(
        "/processing/v1/jobs",
        json={"dataset_key": "tower"},
    )

    assert response.status_code == 409
    detail = response.json()["detail"]
    assert "already active" in detail["error"]
    assert detail["job"]["job_id"] == "proc-existing"


def test_processing_run_monitor_runs_dcp_monitor_datasets_in_order(monkeypatch):
    store = _make_store()
    client = _client(store)
    calls: list[str] = []

    class FakeNormalizerRunner:
        def __init__(self, _store):
            pass

        def run(self, dataset_key: str, mode: str = "incremental"):
            calls.append(dataset_key)
            return {
                "processed": 0,
                "inserted": 0,
                "updated": 0,
                "ignored_older": 0,
                "skipped": 0,
                "failed": 0,
                "last_raw_event_id": 0,
                "errors": [],
            }

    monkeypatch.setattr(server, "NormalizerRunner", FakeNormalizerRunner)

    response = client.post("/processing/v1/run-monitor")

    assert response.status_code == 200
    body = response.json()
    assert calls == ["daily_meeting", "tower", "station"]
    assert body["summary"] == {"processed": 3, "skipped": 0}
    assert list(body["results"].keys()) == ["daily_meeting", "tower", "station"]


def test_processing_run_monitor_skips_unsupported_monitor_dataset(monkeypatch):
    store = _make_store()
    store.save_plugin_runtime_config(
        "dcp",
        {"monitor_datasets": ["daily_meeting", "line_section", "year_progress"]},
    )
    client = _client(store)
    calls: list[str] = []

    class FakeNormalizerRunner:
        def __init__(self, _store):
            pass

        def run(self, dataset_key: str, mode: str = "incremental"):
            calls.append(dataset_key)
            return {"processed": 0, "skipped": 0, "failed": 0, "errors": []}

    monkeypatch.setattr(server, "NormalizerRunner", FakeNormalizerRunner)

    response = client.post("/processing/v1/run-monitor")

    assert response.status_code == 200
    body = response.json()
    assert calls == ["daily_meeting"]
    assert body["results"]["line_section"] == {
        "status": "skipped",
        "reason": "unsupported",
    }
    assert body["results"]["year_progress"] == {
        "status": "skipped",
        "reason": "unsupported",
    }
    assert body["summary"] == {"processed": 1, "skipped": 2}


def test_processing_run_monitor_default_does_not_run_line_or_year(monkeypatch):
    store = _make_store()
    client = _client(store)
    calls: list[str] = []

    class FakeNormalizerRunner:
        def __init__(self, _store):
            pass

        def run(self, dataset_key: str, mode: str = "incremental"):
            calls.append(dataset_key)
            return {"processed": 0, "skipped": 0, "failed": 0, "errors": []}

    monkeypatch.setattr(server, "NormalizerRunner", FakeNormalizerRunner)

    response = client.post("/processing/v1/run-monitor")

    assert response.status_code == 200
    assert calls == ["daily_meeting", "tower", "station"]
    assert "line_section" not in response.json()["results"]
    assert "year_progress" not in response.json()["results"]


def test_station_entity_key_prefers_single_project_code():
    store = _make_store()
    event = _station_event(suffix="key-preferred", station_id="coord-001")
    seed_test_records(store, "station", [event])

    result = NormalizerRunner(store).run("station")

    assert result["processed"] == 1
    entities = store.list_canonical_entities(entity_type="station", dataset_key="station")
    assert entities[0]["entity_key"] == "dcp:station:SP-001"
    assert entities[0]["attributes"]["dcp_coordinate_id"] == "coord-001"


def test_station_entity_key_falls_back_to_coordinate_id_without_single_project_code():
    store = _make_store()
    event = _station_event(suffix="key-fallback", station_id="coord-fallback")
    del event["payload"]["raw"]["singleProjectCode"]
    seed_test_records(store, "station", [event])

    result = NormalizerRunner(store).run("station")

    assert result["processed"] == 1
    entities = store.list_canonical_entities(entity_type="station", dataset_key="station")
    assert entities[0]["entity_key"] == "dcp:station:coord-fallback"
    assert entities[0]["attributes"]["single_project_code"] is None
    assert entities[0]["attributes"]["dcp_coordinate_id"] == "coord-fallback"


def test_station_context_can_supply_project_scoping_when_raw_lacks_codes():
    store = _make_store()
    event = _station_event(suffix="context-station", station_id="coord-context")
    del event["payload"]["raw"]["singleProjectCode"]
    del event["payload"]["raw"]["prjCode"]
    event["source_ref"]["context"] = {
        "project_code": "PRJ-CTX",
        "single_project_code": "SP-CTX",
        "bidding_section_code": "BS-CTX",
    }
    seed_test_records(store, "station", [event])

    result = NormalizerRunner(store).run("station")

    assert result["processed"] == 1
    entity = store.list_canonical_entities(entity_type="station", dataset_key="station")[0]
    assert entity["entity_key"] == "dcp:station:SP-CTX"
    assert entity["attributes"]["project_code"] == "PRJ-CTX"
    assert entity["attributes"]["single_project_code"] == "SP-CTX"
    assert entity["attributes"]["bidding_section_code"] == "BS-CTX"


def test_tower_context_can_supply_scoped_identity_when_raw_lacks_codes():
    store = _make_store()
    event = _tower_event(suffix="context-tower", include_id=False)
    del event["payload"]["raw"]["singleProjectCode"]
    del event["payload"]["raw"]["biddingSectionCode"]
    event["source_ref"]["context"] = {
        "project_code": "PRJ-TOWER-CTX",
        "single_project_code": "SP-TOWER-CTX",
        "bidding_section_code": "BS-TOWER-CTX",
    }
    seed_test_records(store, "tower", [event])

    result = NormalizerRunner(store).run("tower")

    assert result["processed"] == 1
    entity = store.list_canonical_entities(entity_type="tower", dataset_key="tower")[0]
    assert entity["entity_key"] == "dcp:tower:SP-TOWER-CTX:BS-TOWER-CTX:T-context-tower"
    assert entity["attributes"]["project_code"] == "PRJ-TOWER-CTX"
    assert entity["attributes"]["single_project_code"] == "SP-TOWER-CTX"
    assert entity["attributes"]["bidding_section_code"] == "BS-TOWER-CTX"


def test_daily_meeting_context_can_supply_missing_hierarchy_codes():
    store = _make_store()
    event = _daily_meeting_event("context-daily")
    event["payload"]["raw"].pop("prjCode", None)
    event["payload"]["raw"].pop("singleProjectCode", None)
    event["payload"]["raw"].pop("biddingSectionCode", None)
    event["source_ref"]["context"] = {
        "project_code": "PRJ-DAILY-CTX",
        "single_project_code": "SP-DAILY-CTX",
        "bidding_section_code": "BS-DAILY-CTX",
    }
    seed_test_records(store, "daily_meeting", [event])

    result = NormalizerRunner(store).run("daily_meeting")

    assert result["processed"] == 1
    entity = store.list_canonical_entities(entity_type="work_point", dataset_key="daily_meeting")[0]
    assert entity["attributes"]["project_code"] == "PRJ-DAILY-CTX"
    assert entity["attributes"]["single_project_code"] == "SP-DAILY-CTX"
    assert entity["attributes"]["bidding_section_code"] == "BS-DAILY-CTX"


def test_station_normalizer_processes_more_than_default_page_size():
    store = _make_store()
    events = []
    for index in range(1001):
        event = _station_event(suffix=f"{index:04d}")
        event["payload"]["raw"]["singleProjectCode"] = f"SP-{index:04d}"
        events.append(event)
    seed_test_records(store, "station", events)

    result = NormalizerRunner(store).run("station", batch_size=100)

    assert result["processed"] == 1001
    assert result["failed"] == 0
    assert len(store.list_canonical_entities(entity_type="station", limit=2000)) == 1001


def test_older_station_raw_event_does_not_overwrite_newer_current_entity():
    store = _make_store()
    newer = _station_event(
        suffix="newer",
        station_id="station-versioned",
        longitude="113.1",
        latitude="28.5",
        collected_at="2026-05-03T22:30:12+08:00",
    )
    older = _station_event(
        suffix="older",
        station_id="station-versioned",
        longitude="112.8",
        latitude="27.9",
        collected_at="2026-05-03T21:30:12+08:00",
    )
    seed_test_records(store, "station", [newer, older])

    result = NormalizerRunner(store).run("station")

    assert result["processed"] == 2
    entities = store.list_canonical_entities(entity_type="station", dataset_key="station")
    assert len(entities) == 1
    entity = entities[0]
    assert entity["latest_collected_at"] == newer["collected_at"]
    assert entity["latest_source_record_hash"] == newer["source_record_hash"]
    assert entity["attributes"]["longitude"] == 113.1
    assert entity["attributes"]["latitude"] == 28.5
    assert result["inserted"] == 1
    assert result["ignored_older"] == 1


def test_canonical_upsert_compares_latest_collected_at_epoch_not_strings():
    store = _make_store()
    store.upsert_canonical_entity(
        entity_type="station",
        entity_key="dcp:station:timezone",
        dataset_key="station",
        source_system="dcp",
        source_record_key="dcp:station:timezone-old",
        latest_raw_event_id=1,
        latest_collected_at="2026-05-03T16:30:00+08:00",
        latest_collected_at_epoch=_epoch("2026-05-03T16:30:00+08:00"),
        latest_source_record_hash="hash-timezone-old",
        source_refs=[
            {
                "source_system": "dcp",
                "dataset_key": "station",
                "source_record_key": "dcp:station:timezone-old",
            }
        ],
        attributes={"longitude": 112.6, "latitude": 28.0},
    )

    status = store.upsert_canonical_entity(
        entity_type="station",
        entity_key="dcp:station:timezone",
        dataset_key="station",
        source_system="dcp",
        source_record_key="dcp:station:timezone-new",
        latest_raw_event_id=2,
        latest_collected_at="2026-05-03T09:00:00Z",
        latest_collected_at_epoch=_epoch("2026-05-03T09:00:00Z"),
        latest_source_record_hash="hash-timezone-new",
        source_refs=[
            {
                "source_system": "dcp",
                "dataset_key": "station",
                "source_record_key": "dcp:station:timezone-new",
            }
        ],
        attributes={"longitude": 113.0, "latitude": 28.6},
    )

    entity = store.list_canonical_entities(entity_type="station", dataset_key="station")[0]
    assert status == "updated"
    assert entity["latest_collected_at"] == "2026-05-03T09:00:00Z"
    assert entity["latest_source_record_hash"] == "hash-timezone-new"
    assert entity["attributes"]["longitude"] == 113.0


def test_incremental_mode_second_run_does_not_reprocess_raw_events():
    store = _make_store()
    event = _station_event(suffix="incremental")
    seed_test_records(store, "station", [event])

    first = NormalizerRunner(store).run("station")
    second = NormalizerRunner(store).run("station")

    assert first["processed"] == 1
    assert first["inserted"] == 1
    assert second["processed"] == 0
    assert second["inserted"] == 0
    assert second["updated"] == 0
    assert second["ignored_older"] == 0
    assert second["last_raw_event_id"] == first["last_raw_event_id"]


def test_full_mode_rescans_all_raw_events():
    store = _make_store()
    event = _station_event(suffix="full")
    seed_test_records(store, "station", [event])

    first = NormalizerRunner(store).run("station")
    second = NormalizerRunner(store).run("station", mode="full")

    assert first["processed"] == 1
    assert second["processed"] == 1
    assert second["updated"] == 1
    assert second["last_raw_event_id"] == first["last_raw_event_id"]


def test_normalizer_state_records_last_raw_event_id():
    store = _make_store()
    first_event = _station_event(suffix="state-1")
    second_event = _station_event(suffix="state-2")
    second_event["payload"]["raw"]["singleProjectCode"] = "SP-STATE-2"
    inserted = seed_test_records(store, "station", [first_event, second_event])
    second_raw_event_id = inserted["raw_event_ids"][second_event["raw_event_id"]]

    result = NormalizerRunner(store).run("station", batch_size=1)

    state = store.get_normalizer_state("station")
    assert result["processed"] == 2
    assert result["last_raw_event_id"] == second_raw_event_id
    assert state["last_raw_event_id"] == second_raw_event_id
    assert state["normalizer_version"] == "station.v1"


def test_normalizer_run_saves_current_version():
    store = _make_store()
    event = _station_event(suffix="state-version")
    seed_test_records(store, "station", [event])

    result = NormalizerRunner(store).run("station")

    state = store.get_normalizer_state("station")
    assert result["processed"] == 1
    assert state["normalizer_version"] == "station.v1"


def test_normalizer_version_change_reprocesses_from_zero(monkeypatch):
    store = _make_store()
    event = _station_event(suffix="version-change")
    seed_test_records(store, "station", [event])

    first = NormalizerRunner(store).run("station")
    handler = normalizer_runner.NORMALIZERS["station"]["handler"]
    monkeypatch.setitem(
        normalizer_runner.NORMALIZERS,
        "station",
        {
            "version": "station.v2",
            "handler": handler,
        },
    )
    second = NormalizerRunner(store).run("station")

    state = store.get_normalizer_state("station")
    assert first["processed"] == 1
    assert second["processed"] == 1
    assert second["updated"] == 1
    assert state["normalizer_version"] == "station.v2"


def test_incremental_checkpoint_advances_past_skipped_non_target_api():
    store = _make_store()
    skipped_event = _station_event(suffix="skip-api")
    skipped_event["source_ref"]["api_name"] = "substation_single_projects"
    skipped_event["idempotency_key"] = (
        "dcp:projectPages:变电站坐标:substation_single_projects:skip-api"
    )
    processed_event = _station_event(suffix="after-skip")
    processed_event["payload"]["raw"]["singleProjectCode"] = "SP-AFTER-SKIP"
    inserted = seed_test_records(store, "station", [skipped_event, processed_event])
    processed_raw_event_id = inserted["raw_event_ids"][processed_event["raw_event_id"]]

    result = NormalizerRunner(store).run("station")

    assert result["processed"] == 1
    assert result["skipped"] == 1
    assert result["failed"] == 0
    assert result["last_raw_event_id"] == processed_raw_event_id
    assert store.get_normalizer_state("station")["last_raw_event_id"] == processed_raw_event_id


def test_incremental_checkpoint_does_not_advance_past_failed_raw_event():
    first_event = _station_event(suffix="checkpoint-success")
    second_event = _station_event(suffix="checkpoint-failure")

    raw_events = []
    for raw_event_id, event in enumerate([first_event, second_event], start=1):
        raw_events.append(
            {
                "id": raw_event_id,
                "dataset_key": "station",
                "api_name": "substation_coordinates",
                "collected_at": event["collected_at"],
                "source_system": event["source_system"],
                "source_record_key": event["idempotency_key"],
                "source_record_id": event["source_record_id"],
                "source_record_hash": event["source_record_hash"],
                "payload": event["payload"],
            }
        )

    class StoreWithSecondUpsertFailure:
        def __init__(self):
            self.state = {"last_raw_event_id": 0}
            self.upsert_count = 0

        def get_normalizer_state(self, _dataset_key):
            return self.state

        def save_normalizer_state(
            self, _dataset_key, last_raw_event_id, normalizer_version
        ):
            self.state = {
                "last_raw_event_id": last_raw_event_id,
                "normalizer_version": normalizer_version,
            }

        def list_raw_events(self, dataset_key, limit=1000, offset=0, after_id=None):
            after_id = after_id or 0
            return [event for event in raw_events if event["id"] > after_id]

        def upsert_canonical_entity(self, **_entity):
            self.upsert_count += 1
            if self.upsert_count == 2:
                raise RuntimeError("boom")
            return "inserted"

    store = StoreWithSecondUpsertFailure()
    result = NormalizerRunner(store).run("station")

    assert result["processed"] == 1
    assert result["failed"] == 1
    assert result["last_raw_event_id"] <= 1
    assert store.state["last_raw_event_id"] <= 1
    assert "boom" in result["errors"][0]


def test_station_normalizer_skips_invalid_collected_at():
    event = _station_event(suffix="invalid-collected-at")
    raw_event = {
        "id": 1,
        "dataset_key": "station",
        "api_name": "substation_coordinates",
        "collected_at": "not-a-datetime",
        "source_system": event["source_system"],
        "source_record_key": event["idempotency_key"],
        "source_record_id": event["source_record_id"],
        "source_record_hash": event["source_record_hash"],
        "payload": event["payload"],
    }

    class StoreWithInvalidCollectedAt:
        def __init__(self):
            self.calls = 0

        def get_normalizer_state(self, _dataset_key):
            return {"last_raw_event_id": 0}

        def save_normalizer_state(
            self, _dataset_key, _last_raw_event_id, _normalizer_version
        ):
            pass

        def list_raw_events(self, dataset_key, limit=1000, offset=0, after_id=None):
            self.calls += 1
            return [raw_event] if self.calls == 1 else []

        def upsert_canonical_entity(self, **_entity):
            raise AssertionError("invalid collected_at event must not be upserted")

    result = NormalizerRunner(StoreWithInvalidCollectedAt()).run("station")

    assert result["processed"] == 0
    assert result["skipped"] == 1
    assert "invalid collected_at" in result["errors"][0]


def test_incoming_missing_latest_collected_at_does_not_overwrite_current_entity():
    store = _make_store()
    source_ref = {
        "source_system": "dcp",
        "dataset_key": "station",
        "source_record_key": "dcp:station:current",
    }
    store.upsert_canonical_entity(
        entity_type="station",
        entity_key="dcp:station:current",
        dataset_key="station",
        source_system="dcp",
        source_record_key="dcp:station:current",
        latest_raw_event_id=1,
        latest_collected_at="2026-05-03T22:30:12+08:00",
        latest_collected_at_epoch=_epoch("2026-05-03T22:30:12+08:00"),
        latest_source_record_hash="hash-current",
        source_refs=[source_ref],
        attributes={"longitude": 113.2, "latitude": 28.5},
    )

    store.upsert_canonical_entity(
        entity_type="station",
        entity_key="dcp:station:current",
        dataset_key="station",
        source_system="dcp",
        source_record_key="dcp:station:missing-time",
        latest_raw_event_id=2,
        latest_collected_at=None,
        latest_collected_at_epoch=None,
        latest_source_record_hash="hash-missing-time",
        source_refs=[
            {
                "source_system": "dcp",
                "dataset_key": "station",
                "source_record_key": "dcp:station:missing-time",
            }
        ],
        attributes={"longitude": 112.2, "latitude": 27.5},
    )

    entity = store.list_canonical_entities(entity_type="station", dataset_key="station")[0]
    assert entity["latest_collected_at"] == "2026-05-03T22:30:12+08:00"
    assert entity["latest_source_record_hash"] == "hash-current"
    assert entity["source_record_key"] == "dcp:station:current"
    assert entity["attributes"]["longitude"] == 113.2
    assert entity["source_refs"] == [source_ref]


def test_source_refs_are_merged_across_current_entity_upserts():
    store = _make_store()
    first_ref = {
        "source_system": "dcp",
        "dataset_key": "station",
        "source_record_key": "dcp:station:first",
    }
    second_ref = {
        "source_system": "dcp",
        "dataset_key": "station",
        "source_record_key": "dcp:station:second",
    }
    store.upsert_canonical_entity(
        entity_type="station",
        entity_key="dcp:station:merged",
        dataset_key="station",
        source_system="dcp",
        source_record_key="dcp:station:first",
        latest_raw_event_id=1,
        latest_collected_at="2026-05-03T21:30:12+08:00",
        latest_collected_at_epoch=_epoch("2026-05-03T21:30:12+08:00"),
        latest_source_record_hash="hash-first",
        source_refs=[first_ref],
        attributes={"longitude": 112.6, "latitude": 28.0},
    )

    store.upsert_canonical_entity(
        entity_type="station",
        entity_key="dcp:station:merged",
        dataset_key="station",
        source_system="dcp",
        source_record_key="dcp:station:second",
        latest_raw_event_id=2,
        latest_collected_at="2026-05-03T22:30:12+08:00",
        latest_collected_at_epoch=_epoch("2026-05-03T22:30:12+08:00"),
        latest_source_record_hash="hash-second",
        source_refs=[second_ref],
        attributes={"longitude": 113.0, "latitude": 28.6},
    )

    entity = store.list_canonical_entities(entity_type="station", dataset_key="station")[0]
    source_record_keys = {
        ref["source_record_key"] for ref in entity["source_refs"]
    }
    assert source_record_keys == {"dcp:station:first", "dcp:station:second"}
    assert entity["latest_source_record_hash"] == "hash-second"
    assert entity["attributes"]["longitude"] == 113.0


def test_sandbox_skeleton_reports_truncated_when_over_limit():
    store = _make_store()
    client = _client(store)
    for index in range(2):
        store.upsert_canonical_entity(
            entity_type="station",
            entity_key=f"dcp:station:limit-{index}",
            dataset_key="station",
            source_system="dcp",
            source_record_key=f"dcp:station:limit-{index}",
            latest_raw_event_id=index + 1,
            latest_collected_at=f"2026-05-03T22:30:1{index}+08:00",
            latest_collected_at_epoch=_epoch(f"2026-05-03T22:30:1{index}+08:00"),
            latest_source_record_hash=f"hash-limit-{index}",
            source_refs=[
                {
                    "source_system": "dcp",
                    "dataset_key": "station",
                    "source_record_key": f"dcp:station:limit-{index}",
                }
            ],
            attributes={
                "project_code": "PRJ-001",
                "single_project_code": f"SP-LIMIT-{index}",
                "longitude": 112.0 + index,
                "latitude": 28.0 + index,
            },
        )

    response = client.get("/api/v1/sandbox/map/skeleton?limit=1")

    assert response.status_code == 200
    body = response.json()
    assert body["meta"] == {
        "limit": 1,
        "stations_count": 1,
        "towers_count": 0,
        "truncated": True,
    }
    assert len(body["stations"]) == 1


def test_station_normalizer_skips_raw_event_missing_collected_at():
    event = _station_event(suffix="missing-collected-at")
    raw_event = {
        "id": 1,
        "dataset_key": "station",
        "api_name": "substation_coordinates",
        "source_system": event["source_system"],
        "source_record_key": event["idempotency_key"],
        "source_record_id": event["source_record_id"],
        "source_record_hash": event["source_record_hash"],
        "payload": event["payload"],
    }

    class StoreWithMissingCollectedAt:
        def __init__(self):
            self.calls = 0

        def get_normalizer_state(self, _dataset_key):
            return {"last_raw_event_id": 0}

        def save_normalizer_state(
            self, _dataset_key, _last_raw_event_id, _normalizer_version
        ):
            pass

        def list_raw_events(self, dataset_key, limit=1000, offset=0, after_id=None):
            self.calls += 1
            return [raw_event] if self.calls == 1 else []

        def upsert_canonical_entity(self, **_entity):
            raise AssertionError("missing collected_at event must not be upserted")

    result = NormalizerRunner(StoreWithMissingCollectedAt()).run("station")

    assert result["processed"] == 0
    assert result["skipped"] == 1
    assert result["failed"] == 0
    assert "missing collected_at" in result["errors"][0]


def test_unsupported_dataset_key_fails_via_registry():
    store = _make_store()

    result = NormalizerRunner(store).run("unknown_dataset")

    assert result["processed"] == 0
    assert result["skipped"] == 0
    assert result["failed"] == 1
    assert result["errors"] == ["unsupported dataset_key: unknown_dataset"]
