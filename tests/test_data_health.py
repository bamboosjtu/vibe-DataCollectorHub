from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient

import api.server as server
from core.plugin_manager import PluginManager
from health.dataset_health import (
    get_context_coverage,
    get_daily_meeting_date_health,
    get_dataset_health,
)
from health.domain_health import get_domain_health
from health.job_health import get_job_health
from health.summary import get_health_summary
from processing.normalizer_runner import NormalizerRunner
from storage.sqlite_store import SQLiteStore
from conftest import seed_test_records


def _make_store() -> SQLiteStore:
    artifacts_dir = Path(__file__).resolve().parent / ".artifacts"
    artifacts_dir.mkdir(exist_ok=True)
    db_path = artifacts_dir / f"data-health-{uuid4().hex}.db"
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


def _event(
    *,
    suffix: str,
    dataset_key: str,
    collection: str,
    page_name: str,
    api_name: str,
    raw: dict,
    collected_at: str = "2026-05-03T21:30:12+08:00",
    context: dict | None = None,
    source_file: str | None = None,
) -> dict:
    return {
        "raw_event_id": f"raw-{dataset_key}-{suffix}",
        "idempotency_key": f"dcp:{collection}:{page_name}:{api_name}:{suffix}",
        "source_system": "dcp",
        "source_record_id": f"record-{suffix}",
        "source_record_hash": f"hash-{suffix}",
        "occurred_at": collected_at,
        "collected_at": collected_at,
        "payload": {"raw": raw},
        "source_ref": {
            "collector": "vibe-downloader",
            "run_id": "run-001",
            "collection": collection,
            "page_name": page_name,
            "api_name": api_name,
            "raw_data_index": 0,
            "record_index": 0,
            "record_path": "raw_data[0].records[0]",
            "source_file": source_file
            or f"{collection}/{page_name}/{suffix}.json",
            **({"context": context} if context else {}),
        },
    }


def _seed_health_data(store: SQLiteStore) -> None:
    seed_test_records(
        store,
        "project_preconstruction",
        [
            _event(
                suffix="preconstruction",
                dataset_key="project_preconstruction",
                collection="projectPages",
                page_name="项目前期成果",
                api_name="preconstruction_results_detail",
                raw={
                    "prjCode": "PRJ-001",
                    "prjName": "项目一",
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
            )
        ],
    )
    seed_test_records(
        store,
        "line_section",
        [
            _event(
                suffix="section-scoped",
                dataset_key="line_section",
                collection="projectPages",
                page_name="区段划分",
                api_name="section_details",
                raw={
                    "id": "LS-001",
                    "sectionName": "一区段",
                    "sectionVo": {"towerNoList": [{"towerNo": "G1"}]},
                },
                context={
                    "project_code": "PRJ-001",
                    "single_project_code": "SP-001",
                    "bidding_section_code": "BS-001",
                    "line_section_id": "LS-001",
                    "line_section_name": "一区段",
                },
            ),
            _event(
                suffix="section-unscoped",
                dataset_key="line_section",
                collection="projectPages",
                page_name="区段划分",
                api_name="section_details",
                raw={
                    "id": "LS-002",
                    "sectionName": "二区段",
                    "sectionVo": {"towerNoList": [{"towerNo": "G2"}]},
                },
            ),
        ],
    )
    seed_test_records(
        store,
        "tower",
        [
            _event(
                suffix="tower",
                dataset_key="tower",
                collection="projectPages",
                page_name="杆塔信息",
                api_name="tower_details",
                raw={
                    "singleProjectCode": "SP-001",
                    "biddingSectionCode": "BS-001",
                    "towerNo": "G1",
                    "longitudeEdit": "112.9451",
                    "latitudeEdit": "28.2311",
                },
                context={"project_code": "PRJ-001"},
            )
        ],
    )
    seed_test_records(
        store,
        "station",
        [
            _event(
                suffix="station",
                dataset_key="station",
                collection="projectPages",
                page_name="变电站坐标",
                api_name="substation_coordinates",
                raw={
                    "id": "station-001",
                    "longitude": "112.9279",
                    "latitude": "28.2147",
                },
                context={
                    "project_code": "PRJ-001",
                    "single_project_code": "SP-001",
                },
            )
        ],
    )
    seed_test_records(
        store,
        "daily_meeting",
        [
            _event(
                suffix="daily-2026-05-06",
                dataset_key="daily_meeting",
                collection="safePages",
                page_name="meetingListAdmin",
                api_name="queryToolBoxTalkListPagePc",
                raw={
                    "id": "meeting-001",
                    "toolBoxTalkLongitude": "112.9388",
                    "toolBoxTalkLatitude": "28.2282",
                },
                source_file="../data/safe/daily_meeting/2026-05-06.json",
            ),
            _event(
                suffix="daily-2026-05-07",
                dataset_key="daily_meeting",
                collection="safePages",
                page_name="meetingListAdmin",
                api_name="queryToolBoxTalkListPagePc",
                raw={
                    "id": "meeting-002",
                    "toolBoxTalkLongitude": "112.9388",
                    "toolBoxTalkLatitude": "28.2282",
                },
                source_file="../data/safe/daily_meeting/2026-05-07.json",
            ),
        ],
    )

    NormalizerRunner(store).run("project_hierarchy", mode="full")
    NormalizerRunner(store).run("line_section", mode="full")
    NormalizerRunner(store).run("tower", mode="full")
    NormalizerRunner(store).run("station", mode="full")
    NormalizerRunner(store).run("daily_meeting", mode="full")


def test_dataset_health_counts_and_api_breakdown() -> None:
    store = _make_store()
    _seed_health_data(store)

    health = get_dataset_health(store)

    assert health["datasets"]["line_section"]["raw_event_count"] == 2
    assert health["datasets"]["line_section"]["canonical_entity_count"] >= 2
    assert any(
        item["api_name"] == "section_details"
        for item in health["datasets"]["line_section"]["api_breakdown"]
    )
    assert health["datasets"]["daily_meeting"]["latest_raw_event_id"] is not None


def test_job_health_counts_and_timeout_detection() -> None:
    store = _make_store()
    store.create_external_collection_job(
        job_id="collect-running",
        plugin_id="dcp",
        profile="monitor_daily",
        dataset_keys=["daily_meeting"],
        mode="incremental",
        command=["uv"],
        cwd="D:/tmp",
        datahub_url="http://127.0.0.1:8000",
        processing_mode="async",
    )
    store.mark_external_collection_job_running("collect-running")
    store.create_processing_job(job_id="proc-failed", dataset_key="tower")
    store.mark_processing_job_failed("proc-failed", "boom")

    conn = store._get_connection()
    try:
        conn.execute(
            "UPDATE external_collection_jobs SET started_at = ? WHERE job_id = ?",
            ((datetime.now().astimezone() - timedelta(hours=7)).isoformat(), "collect-running"),
        )
        conn.commit()
    finally:
        conn.close()

    health = get_job_health(store)

    assert health["external_collection_jobs"]["counts"]["running"] == 1
    assert health["processing_jobs"]["counts"]["failed"] == 1
    assert health["external_collection_jobs"]["timed_out_running_jobs"][0]["job_id"] == "collect-running"


def test_domain_health_detects_unscoped_and_known_issues() -> None:
    store = _make_store()
    _seed_health_data(store)

    health = get_domain_health(store)

    assert health["relationship_counts"]["HAS_TOWER_SEQUENCE"] >= 2
    assert health["unscoped_tower_sequence_count"] > 0
    assert health["tower_sequence_orphan_count"] > 0
    assert health["tower_sequence_missing_physical_entity_count"] > 0
    assert health["line_section_known_issue_count"] > 0


def test_domain_health_reports_zero_tower_sequence_orphans_when_scoped_tower_exists() -> None:
    store = _make_store()
    store.upsert_canonical_entity(
        entity_type="tower",
        entity_key="dcp:tower:S01:B01:G1",
        dataset_key="tower",
        source_system="dcp",
        source_record_key="tower-record",
        latest_raw_event_id=1,
        latest_collected_at="2026-05-08T08:00:00+08:00",
        latest_collected_at_epoch=1.0,
        latest_source_record_hash="hash-tower",
        source_refs=[],
        attributes={"tower_no": "G1"},
    )
    store.upsert_canonical_entity(
        entity_type="line_section",
        entity_key="dcp:line_section:LS-001",
        dataset_key="line_section",
        source_system="dcp",
        source_record_key="line-record",
        latest_raw_event_id=2,
        latest_collected_at="2026-05-08T08:01:00+08:00",
        latest_collected_at_epoch=2.0,
        latest_source_record_hash="hash-line",
        source_refs=[],
        attributes={},
    )
    store.upsert_canonical_relationship(
        relationship_key="rel-tower-sequence",
        relationship_type="HAS_TOWER_SEQUENCE",
        from_entity_type="line_section",
        from_entity_key="dcp:line_section:LS-001",
        to_entity_type="tower",
        to_entity_key="dcp:tower:S01:B01:G1",
        dataset_key="line_section",
        source_system="dcp",
        latest_raw_event_id=3,
        latest_collected_at="2026-05-08T08:02:00+08:00",
        attributes={"tower_no": "G1"},
    )

    health = get_domain_health(store)

    assert health["tower_sequence_orphan_count"] == 0


def test_domain_health_counts_unscoped_tower_entities() -> None:
    store = _make_store()
    store.upsert_canonical_entity(
        entity_type="tower",
        entity_key="dcp:tower:G1",
        dataset_key="tower",
        source_system="dcp",
        source_record_key="tower-unscoped",
        latest_raw_event_id=1,
        latest_collected_at="2026-05-08T08:00:00+08:00",
        latest_collected_at_epoch=1.0,
        latest_source_record_hash="hash-tower-unscoped",
        source_refs=[],
        attributes={"tower_no": "G1"},
    )

    health = get_domain_health(store)

    assert health["unscoped_tower_entity_count"] == 1


def test_domain_health_orphan_relationship_uses_entity_type_and_key() -> None:
    store = _make_store()
    store.upsert_canonical_entity(
        entity_type="project",
        entity_key="shared-key",
        dataset_key="project_preconstruction",
        source_system="dcp",
        source_record_key="project-record",
        latest_raw_event_id=1,
        latest_collected_at="2026-05-08T08:00:00+08:00",
        latest_collected_at_epoch=1.0,
        latest_source_record_hash="hash-project",
        source_refs=[],
        attributes={},
    )
    store.upsert_canonical_entity(
        entity_type="single_project",
        entity_key="single-key",
        dataset_key="project_preconstruction",
        source_system="dcp",
        source_record_key="single-record",
        latest_raw_event_id=2,
        latest_collected_at="2026-05-08T08:01:00+08:00",
        latest_collected_at_epoch=2.0,
        latest_source_record_hash="hash-single",
        source_refs=[],
        attributes={},
    )
    store.upsert_canonical_relationship(
        relationship_key="rel-orphan-type",
        relationship_type="HAS_SINGLE_PROJECT",
        from_entity_type="bidding_section",
        from_entity_key="shared-key",
        to_entity_type="single_project",
        to_entity_key="single-key",
        dataset_key="project_preconstruction",
        source_system="dcp",
        latest_raw_event_id=3,
        latest_collected_at="2026-05-08T08:02:00+08:00",
        attributes={},
    )

    health = get_domain_health(store)

    assert health["orphan_relationship_count"] == 1


def test_daily_meeting_health_reports_missing_dates() -> None:
    store = _make_store()
    _seed_health_data(store)

    health = get_daily_meeting_date_health(store, recent_days=4)

    assert health["latest_work_date"] >= "2026-05-06"
    assert health["missing_dates"]
    assert "2026-05-06" not in health["missing_dates"]
    assert health["work_point_count_by_date"]["2026-05-06"] == 1


def test_context_coverage_reports_line_section_gap() -> None:
    store = _make_store()
    _seed_health_data(store)

    coverage = get_context_coverage(store)

    assert coverage["line_section.section_details"]["total"] == 2
    assert coverage["line_section.section_details"]["missing_context"] == 1
    assert coverage["line_section.section_details"]["status"] == "warning"
    assert coverage["tower.tower_details"]["with_single_project_code"] == 1


def test_health_summary_reports_failed_reasons() -> None:
    store = _make_store()
    _seed_health_data(store)

    summary = get_health_summary(store, recent_days=4)

    assert summary["overall_status"] == "failed"
    assert any("unscoped tower sequence" in reason for reason in summary["reasons"])


def test_health_summary_warns_when_tower_sequence_points_to_missing_tower() -> None:
    store = _make_store()
    store.upsert_canonical_entity(
        entity_type="tower",
        entity_key="dcp:tower:S01:B01:G0",
        dataset_key="tower",
        source_system="dcp",
        source_record_key="tower-warn-existing",
        latest_raw_event_id=0,
        latest_collected_at="2026-05-08T07:59:00+08:00",
        latest_collected_at_epoch=0.5,
        latest_source_record_hash="hash-tower-warn-existing",
        source_refs=[],
        attributes={"tower_no": "G0"},
    )
    store.upsert_canonical_entity(
        entity_type="line_section",
        entity_key="dcp:line_section:LS-WARN",
        dataset_key="line_section",
        source_system="dcp",
        source_record_key="line-warn",
        latest_raw_event_id=1,
        latest_collected_at="2026-05-08T08:00:00+08:00",
        latest_collected_at_epoch=1.0,
        latest_source_record_hash="hash-line-warn",
        source_refs=[],
        attributes={},
    )
    store.upsert_canonical_relationship(
        relationship_key="rel-tower-warn",
        relationship_type="HAS_TOWER_SEQUENCE",
        from_entity_type="line_section",
        from_entity_key="dcp:line_section:LS-WARN",
        to_entity_type="tower",
        to_entity_key="dcp:tower:S01:B01:G1",
        dataset_key="line_section",
        source_system="dcp",
        latest_raw_event_id=2,
        latest_collected_at="2026-05-08T08:01:00+08:00",
        attributes={"tower_no": "G1", "node_kind": "physical_candidate"},
    )

    summary = get_health_summary(store, recent_days=1)

    assert summary["overall_status"] == "warning"
    assert "tower sequence physical candidates point to missing tower entities" in summary["reasons"]


def test_health_summary_does_not_warn_for_reference_tower_sequence_nodes() -> None:
    store = _make_store()
    store.upsert_canonical_entity(
        entity_type="line_section",
        entity_key="dcp:line_section:LS-REF",
        dataset_key="line_section",
        source_system="dcp",
        source_record_key="line-ref",
        latest_raw_event_id=1,
        latest_collected_at="2026-05-08T08:00:00+08:00",
        latest_collected_at_epoch=1.0,
        latest_source_record_hash="hash-line-ref",
        source_refs=[],
        attributes={},
    )
    store.upsert_canonical_relationship(
        relationship_key="rel-tower-ref",
        relationship_type="HAS_TOWER_SEQUENCE",
        from_entity_type="line_section",
        from_entity_key="dcp:line_section:LS-REF",
        to_entity_type="tower",
        to_entity_key="dcp:tower:S01:B01:韶鹤Ⅰ线#001",
        dataset_key="line_section",
        source_system="dcp",
        latest_raw_event_id=2,
        latest_collected_at="2026-05-08T08:01:00+08:00",
        attributes={"tower_no": "韶鹤Ⅰ线#001", "node_kind": "reference_node"},
    )

    health = get_domain_health(store)
    summary = get_health_summary(store, recent_days=1)

    assert health["tower_sequence_reference_count"] == 1
    assert health["tower_sequence_missing_physical_entity_count"] == 0
    assert "tower sequence physical candidates point to missing tower entities" not in summary["reasons"]


def test_domain_health_counts_scope_without_tower_for_missing_physical_candidates() -> None:
    store = _make_store()
    store.upsert_canonical_entity(
        entity_type="line_section",
        entity_key="dcp:line_section:LS-SCOPE-MISSING",
        dataset_key="line_section",
        source_system="dcp",
        source_record_key="line-scope-missing",
        latest_raw_event_id=1,
        latest_collected_at="2026-05-09T08:00:00+08:00",
        latest_collected_at_epoch=1.0,
        latest_source_record_hash="hash-line-scope-missing",
        source_refs=[],
        attributes={},
    )
    store.upsert_canonical_relationship(
        relationship_key="rel-tower-scope-missing",
        relationship_type="HAS_TOWER_SEQUENCE",
        from_entity_type="line_section",
        from_entity_key="dcp:line_section:LS-SCOPE-MISSING",
        to_entity_type="tower",
        to_entity_key="dcp:tower:S01:B01:G11",
        dataset_key="line_section",
        source_system="dcp",
        latest_raw_event_id=2,
        latest_collected_at="2026-05-09T08:01:00+08:00",
        attributes={"tower_no": "G11", "node_kind": "physical_candidate"},
    )

    health = get_domain_health(store)
    summary = get_health_summary(store, recent_days=1)

    assert health["tower_sequence_scope_without_tower_count"] == 1
    assert health["tower_sequence_missing_physical_entity_count"] == 0
    assert "some line-section scopes have no tower entities" in summary["reasons"]
    assert "tower sequence physical candidates point to missing tower entities" not in summary["reasons"]


def test_domain_health_counts_missing_physical_when_scope_has_other_towers() -> None:
    store = _make_store()
    store.upsert_canonical_entity(
        entity_type="tower",
        entity_key="dcp:tower:S01:B01:G10",
        dataset_key="tower",
        source_system="dcp",
        source_record_key="tower-same-scope",
        latest_raw_event_id=1,
        latest_collected_at="2026-05-09T08:00:00+08:00",
        latest_collected_at_epoch=1.0,
        latest_source_record_hash="hash-tower-same-scope",
        source_refs=[],
        attributes={"tower_no": "G10"},
    )
    store.upsert_canonical_entity(
        entity_type="line_section",
        entity_key="dcp:line_section:LS-PHYSICAL-MISSING",
        dataset_key="line_section",
        source_system="dcp",
        source_record_key="line-physical-missing",
        latest_raw_event_id=2,
        latest_collected_at="2026-05-09T08:01:00+08:00",
        latest_collected_at_epoch=2.0,
        latest_source_record_hash="hash-line-physical-missing",
        source_refs=[],
        attributes={},
    )
    store.upsert_canonical_relationship(
        relationship_key="rel-tower-physical-missing",
        relationship_type="HAS_TOWER_SEQUENCE",
        from_entity_type="line_section",
        from_entity_key="dcp:line_section:LS-PHYSICAL-MISSING",
        to_entity_type="tower",
        to_entity_key="dcp:tower:S01:B01:G11",
        dataset_key="line_section",
        source_system="dcp",
        latest_raw_event_id=3,
        latest_collected_at="2026-05-09T08:02:00+08:00",
        attributes={"tower_no": "G11", "node_kind": "physical_candidate"},
    )

    health = get_domain_health(store)
    summary = get_health_summary(store, recent_days=1)

    assert health["tower_sequence_scope_without_tower_count"] == 0
    assert health["tower_sequence_missing_physical_entity_count"] == 1
    assert "tower sequence physical candidates point to missing tower entities" in summary["reasons"]


def test_health_api_endpoints() -> None:
    store = _make_store()
    _seed_health_data(store)
    client = _client(store)

    summary = client.get("/health/v1/summary?recent_days=4")
    datasets = client.get("/health/v1/datasets")
    jobs = client.get("/health/v1/jobs")
    domain = client.get("/health/v1/domain")
    daily = client.get("/health/v1/daily-meeting?recent_days=4")
    context = client.get("/health/v1/context")

    assert summary.status_code == 200
    assert datasets.status_code == 200
    assert jobs.status_code == 200
    assert domain.status_code == 200
    assert daily.status_code == 200
    assert context.status_code == 200
    assert "overall_status" in summary.json()
    assert "datasets" in datasets.json()
    assert "external_collection_jobs" in jobs.json()
    assert "entity_counts" in domain.json()
    assert "missing_dates" in daily.json()
    assert "line_section.section_details" in context.json()
