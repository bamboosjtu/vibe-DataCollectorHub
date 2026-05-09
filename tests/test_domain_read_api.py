from __future__ import annotations

from datetime import datetime
from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient

import api.server as server
from core.plugin_manager import PluginManager
from processing.normalizer_runner import NormalizerRunner
from storage.sqlite_store import SQLiteStore


def _make_store() -> SQLiteStore:
    artifacts_dir = Path(__file__).resolve().parent / ".artifacts"
    artifacts_dir.mkdir(exist_ok=True)
    db_path = artifacts_dir / f"domain-read-{uuid4().hex}.db"
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


def _event(
    *,
    suffix: str,
    dataset_key: str,
    page_name: str,
    api_name: str,
    raw: dict,
    context: dict | None = None,
) -> dict:
    return {
        "schema_version": "source_event.v1",
        "event_id": f"evt-{dataset_key}-{suffix}",
        "idempotency_key": f"dcp:projectPages:{page_name}:{api_name}:{suffix}",
        "source_system": "dcp",
        "source_event_type": "dcp.record",
        "event_granularity": "record",
        "source_record_id": f"record-{suffix}",
        "source_record_hash": f"hash-{suffix}",
        "occurred_at": "2026-05-08T08:30:00+08:00",
        "collected_at": "2026-05-08T21:30:12+08:00",
        "payload": {"raw": raw},
        "source_ref": {
            "collector": "vibe-downloader",
            "run_id": "20260508_213000",
            "collection": "projectPages",
            "page_name": page_name,
            "api_name": api_name,
            "raw_data_index": 0,
            "record_index": 0,
            "record_path": "raw_data[0].records[0]",
            "source_file": f"projectPages/{page_name}/{suffix}.json",
            **({"context": context} if context else {}),
        },
    }


def _seed_domain_graph(store: SQLiteStore) -> None:
    store.save_raw_event(
        _event(
            suffix="preconstruction",
            dataset_key="project_preconstruction",
            page_name="项目前期成果",
            api_name="preconstruction_results_detail",
            raw={
                "prjCode": "PRJ-001",
                "prjName": "示例工程",
                "sinList": [
                    {
                        "singleProjectCode": "SP-001",
                        "singleProjectName": "单项一",
                        "bidSectList": [
                            {
                                "biddingSectionCode": "BS-001",
                                "biddingSectionName": "标段一",
                            }
                        ],
                    }
                ],
            },
        ),
        dataset_key="project_preconstruction",
    )
    store.save_raw_event(
        _event(
            suffix="line-section",
            dataset_key="line_section",
            page_name="区段划分",
            api_name="section_details",
            raw={
                "id": "LS-001",
                "sectionName": "一区段",
                "sectionVo": {"towerNoList": ["G1", "韶鹤Ⅰ线#001"]},
            },
            context={
                "project_code": "PRJ-001",
                "project_name": "示例工程",
                "single_project_code": "SP-001",
                "single_project_name": "单项一",
                "bidding_section_code": "BS-001",
                "bidding_section_name": "标段一",
                "line_section_id": "LS-001",
                "line_section_name": "一区段",
            },
        ),
        dataset_key="line_section",
    )
    store.save_raw_event(
        _event(
            suffix="year-progress",
            dataset_key="year_progress",
            page_name="年度进度计划分析",
            api_name="yearly_progress_analysis",
            raw={
                "id": "PROG-001",
                "prjCode": "PRJ-001",
                "prjName": "示例工程",
                "status": "在建",
                "singleList": [
                    {
                        "singleProjectCode": "SP-001",
                        "singleProjectName": "单项一",
                    }
                ],
            },
        ),
        dataset_key="year_progress",
    )

    assert NormalizerRunner(store).run("project_hierarchy", mode="full")["failed"] == 0
    assert NormalizerRunner(store).run("line_section", mode="full")["failed"] == 0
    assert NormalizerRunner(store).run("year_progress", mode="full")["failed"] == 0

    store.upsert_canonical_entity(
        entity_type="tower",
        entity_key="dcp:tower:SP-001:BS-001:G1",
        dataset_key="tower",
        source_system="dcp",
        source_record_key="tower-seeded",
        latest_raw_event_id=100,
        latest_collected_at="2026-05-08T21:30:12+08:00",
        latest_collected_at_epoch=_epoch("2026-05-08T21:30:12+08:00"),
        latest_source_record_hash="hash-tower-seeded",
        source_refs=[],
        attributes={
            "project_code": "PRJ-001",
            "single_project_code": "SP-001",
            "bidding_section_code": "BS-001",
            "tower_no": "G1",
            "longitude": 112.9,
            "latitude": 28.2,
        },
    )
    store.upsert_canonical_entity(
        entity_type="station",
        entity_key="dcp:station:SP-001",
        dataset_key="station",
        source_system="dcp",
        source_record_key="station-seeded",
        latest_raw_event_id=101,
        latest_collected_at="2026-05-08T21:30:12+08:00",
        latest_collected_at_epoch=_epoch("2026-05-08T21:30:12+08:00"),
        latest_source_record_hash="hash-station-seeded",
        source_refs=[],
        attributes={
            "project_code": "PRJ-001",
            "single_project_code": "SP-001",
            "longitude": 112.8,
            "latitude": 28.1,
        },
    )
    store.upsert_canonical_entity(
        entity_type="work_point",
        entity_key="dcp:work_point:2026-05-08:meeting-001",
        entity_date="2026-05-08",
        dataset_key="daily_meeting",
        source_system="dcp",
        source_record_key="meeting-seeded",
        latest_raw_event_id=102,
        latest_collected_at="2026-05-08T21:30:12+08:00",
        latest_collected_at_epoch=_epoch("2026-05-08T21:30:12+08:00"),
        latest_source_record_hash="hash-meeting-seeded",
        source_refs=[],
        attributes={
            "project_code": "PRJ-001",
            "project_name": "示例工程",
            "single_project_code": "SP-001",
            "bidding_section_code": "BS-001",
            "longitude": 112.7,
            "latitude": 28.0,
            "work_date": "2026-05-08",
        },
    )


def test_domain_projects_api_returns_project_list() -> None:
    store = _make_store()
    _seed_domain_graph(store)
    client = _client(store)

    response = client.get("/api/v1/domain/projects")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 1
    item = body["items"][0]
    assert item["project_code"] == "PRJ-001"
    assert item["single_project_count"] == 1
    assert item["bidding_section_count"] == 1
    assert item["tower_count"] == 1
    assert item["station_count"] == 1
    assert item["line_section_count"] == 1
    assert item["work_point_count"] == 1
    assert item["progress_count"] == 1


def test_domain_project_detail_api_returns_fact_view() -> None:
    store = _make_store()
    _seed_domain_graph(store)
    client = _client(store)

    response = client.get("/api/v1/domain/projects/PRJ-001?date=2026-05-08")

    assert response.status_code == 200
    body = response.json()
    assert body["project"]["attributes"]["project_code"] == "PRJ-001"
    assert len(body["single_projects"]) == 1
    assert len(body["bidding_sections"]) == 1
    assert len(body["towers"]) == 1
    assert len(body["stations"]) == 1
    assert len(body["line_sections"]) == 1
    assert len(body["work_points"]) == 1
    assert len(body["project_progress"]) == 1
    assert body["summary"]["tower_count"] == 1


def test_domain_line_sections_api_returns_sequence_stats() -> None:
    store = _make_store()
    _seed_domain_graph(store)
    client = _client(store)

    response = client.get("/api/v1/domain/line-sections?project_code=PRJ-001")

    assert response.status_code == 200
    item = response.json()["items"][0]
    assert item["line_section_id"] == "LS-001"
    assert item["tower_sequence_count"] == 2
    assert item["matched_tower_count"] == 1
    assert item["reference_node_count"] == 1
    assert item["missing_physical_count"] == 0
    assert item["scope_without_tower_count"] == 0


def test_domain_year_progress_api_returns_empty_list_without_data() -> None:
    store = _make_store()
    client = _client(store)

    response = client.get("/api/v1/domain/year-progress")

    assert response.status_code == 200
    assert response.json()["items"] == []


def test_domain_relationships_api_supports_relationship_type_filter() -> None:
    store = _make_store()
    _seed_domain_graph(store)
    client = _client(store)

    response = client.get(
        "/api/v1/domain/relationships?relationship_type=HAS_TOWER_SEQUENCE"
    )

    assert response.status_code == 200
    items = response.json()["items"]
    assert items
    assert {item["relationship_type"] for item in items} == {"HAS_TOWER_SEQUENCE"}


def test_domain_project_view_api_returns_base_aggregate() -> None:
    store = _make_store()
    _seed_domain_graph(store)
    client = _client(store)

    response = client.get("/api/v1/domain/project-view/PRJ-001")

    assert response.status_code == 200
    body = response.json()
    assert body["project"]["attributes"]["project_code"] == "PRJ-001"
    assert len(body["hierarchy"]["single_projects"]) == 1
    assert len(body["line_sections"]) == 1
    assert body["summary"]["project_progress_count"] == 1
