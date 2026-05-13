from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from storage.sqlite_store import SQLiteStore


TARGET_DATASETS = [
    "daily_meeting",
    "tower",
    "station",
    "line_section",
    "project_preconstruction",
    "year_progress",
]


def _parse_iso(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _raw_and_context(raw_event: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = raw_event.get("payload") or {}
    raw = payload.get("raw") if isinstance(payload, dict) else {}
    if not isinstance(raw, dict):
        raw = {}
    source_ref = raw_event.get("source_ref") or {}
    context = source_ref.get("context") if isinstance(source_ref, dict) else {}
    if not isinstance(context, dict):
        context = {}
    return raw, context


def _has_code(raw: dict[str, Any], context: dict[str, Any], raw_key: str, context_key: str) -> bool:
    return raw.get(raw_key) not in (None, "") or context.get(context_key) not in (None, "")


def _context_status(total: int, missing_critical: int) -> str:
    if total == 0:
        return "ok"
    if missing_critical == 0:
        return "ok"
    if missing_critical == total:
        return "failed"
    return "warning"


def get_dataset_health(store: SQLiteStore) -> dict[str, Any]:
    conn = store._get_connection()
    try:
        datasets = {
            dataset_key: {
                "dataset_key": dataset_key,
                "raw_event_count": 0,
                "latest_raw_event_id": None,
                "latest_collected_at": None,
                "latest_created_at": None,
                "api_breakdown": [],
                "canonical_entity_count": 0,
                "relationship_count": 0,
                "processing_state": store.get_normalizer_state(dataset_key),
                "normalizer_state": store.get_normalizer_state(dataset_key),
                "latest_processing_job": None,
            }
            for dataset_key in TARGET_DATASETS
        }

        cursor = conn.execute(
            """
            SELECT dataset_key,
                   COUNT(*) AS raw_event_count,
                   MAX(id) AS latest_raw_event_id,
                   MAX(collected_at) AS latest_collected_at,
                   MAX(created_at) AS latest_created_at
            FROM raw_events
            WHERE dataset_key IS NOT NULL
            GROUP BY dataset_key
            """
        )
        for row in cursor.fetchall():
            dataset_key = row["dataset_key"]
            if dataset_key not in datasets:
                continue
            datasets[dataset_key].update(
                {
                    "raw_event_count": row["raw_event_count"],
                    "latest_raw_event_id": row["latest_raw_event_id"],
                    "latest_collected_at": row["latest_collected_at"],
                    "latest_created_at": row["latest_created_at"],
                }
            )

        api_counts: dict[tuple[str, Any, Any], int] = defaultdict(int)
        for raw_event in store.list_raw_events(limit=100000):
            dataset_key = raw_event.get("dataset_key")
            if dataset_key not in datasets:
                continue
            key = (
                dataset_key,
                raw_event.get("page_name"),
                raw_event.get("api_name"),
            )
            api_counts[key] += 1
        for (dataset_key, page_name, api_name), count in sorted(api_counts.items()):
            datasets[dataset_key]["api_breakdown"].append(
                {
                    "page_name": page_name,
                    "api_name": api_name,
                    "count": count,
                }
            )

        cursor = conn.execute(
            """
            SELECT dataset_key, COUNT(*) AS canonical_entity_count
            FROM canonical_entities
            GROUP BY dataset_key
            """
        )
        for row in cursor.fetchall():
            dataset_key = row["dataset_key"]
            if dataset_key in datasets:
                datasets[dataset_key]["canonical_entity_count"] = row["canonical_entity_count"]

        cursor = conn.execute(
            """
            SELECT dataset_key, COUNT(*) AS relationship_count
            FROM canonical_relationships
            GROUP BY dataset_key
            """
        )
        for row in cursor.fetchall():
            dataset_key = row["dataset_key"]
            if dataset_key in datasets:
                datasets[dataset_key]["relationship_count"] = row["relationship_count"]

        cursor = conn.execute(
            """
            SELECT *
            FROM processing_jobs
            ORDER BY created_at DESC
            """
        )
        latest_processing_by_dataset: dict[str, dict[str, Any]] = {}
        for row in cursor.fetchall():
            job = dict(row)
            job["result"] = None
            if job.get("dataset_key") not in latest_processing_by_dataset:
                latest_processing_by_dataset[job["dataset_key"]] = job
        for dataset_key, job in latest_processing_by_dataset.items():
            if dataset_key in datasets:
                datasets[dataset_key]["latest_processing_job"] = job

        return {
            "datasets": datasets,
            "dataset_order": TARGET_DATASETS,
        }
    finally:
        conn.close()


def get_daily_meeting_date_health(
    store: SQLiteStore, recent_days: int = 14
) -> dict[str, Any]:
    recent_days = max(1, int(recent_days))
    tz = ZoneInfo("Asia/Shanghai")
    today = datetime.now(tz).date()
    expected_dates = [
        (today - timedelta(days=offset)).isoformat()
        for offset in range(recent_days - 1, -1, -1)
    ]

    work_point_count_by_date: dict[str, int] = defaultdict(int)
    for entity in store.list_canonical_entities(
        entity_type="work_point",
        dataset_key="daily_meeting",
        limit=100000,
    ):
        work_date = entity.get("entity_date") or entity.get("attributes", {}).get("work_date")
        if work_date:
            work_point_count_by_date[str(work_date)] += 1

    if not work_point_count_by_date:
        for raw_event in store.list_raw_events(dataset_key="daily_meeting", limit=100000):
            source_file = raw_event.get("source_file") or raw_event.get("source_ref", {}).get("source_file")
            work_date = None
            if source_file and str(source_file).endswith(".json"):
                work_date = str(source_file).replace("\\", "/").split("/")[-1][:-5]
            raw, _context = _raw_and_context(raw_event)
            if not work_date:
                raw_date = raw.get("workDate")
                parsed = _parse_iso(raw_date)
                work_date = parsed.date().isoformat() if parsed else (str(raw_date) if raw_date else None)
            if work_date:
                work_point_count_by_date[str(work_date)] += 1

    available_dates = sorted(work_point_count_by_date.keys())
    latest_work_date = available_dates[-1] if available_dates else None
    missing_dates = [date for date in expected_dates if date not in work_point_count_by_date]
    status = "warning" if missing_dates else "ok"

    return {
        "recent_days": recent_days,
        "expected_dates": expected_dates,
        "available_dates": available_dates,
        "missing_dates": missing_dates,
        "latest_work_date": latest_work_date,
        "work_point_count_by_date": dict(sorted(work_point_count_by_date.items())),
        "status": status,
    }


def get_context_coverage(store: SQLiteStore) -> dict[str, Any]:
    raw_events = store.list_raw_events(limit=100000)

    line_section_total = 0
    line_with_context = 0
    line_missing_context = 0
    line_missing_single = 0
    line_missing_bidding = 0

    tower_total = 0
    tower_with_project = 0
    tower_with_single = 0
    tower_with_bidding = 0

    station_total = 0
    station_with_project = 0
    station_with_single = 0

    daily_total = 0
    daily_with_work_date = 0
    daily_with_project = 0
    daily_with_single = 0
    daily_with_bidding = 0

    for raw_event in raw_events:
        dataset_key = raw_event.get("dataset_key")
        api_name = raw_event.get("api_name")
        raw, context = _raw_and_context(raw_event)

        if dataset_key == "line_section" and api_name == "section_details":
            line_section_total += 1
            if context:
                line_with_context += 1
            else:
                line_missing_context += 1
            if not _has_code(raw, context, "singleProjectCode", "single_project_code"):
                line_missing_single += 1
            if not _has_code(raw, context, "biddingSectionCode", "bidding_section_code"):
                line_missing_bidding += 1

        if dataset_key == "tower" and api_name == "tower_details":
            tower_total += 1
            if _has_code(raw, context, "prjCode", "project_code"):
                tower_with_project += 1
            if _has_code(raw, context, "singleProjectCode", "single_project_code"):
                tower_with_single += 1
            if _has_code(raw, context, "biddingSectionCode", "bidding_section_code"):
                tower_with_bidding += 1

        if dataset_key == "station" and api_name == "substation_coordinates":
            station_total += 1
            if _has_code(raw, context, "prjCode", "project_code"):
                station_with_project += 1
            if _has_code(raw, context, "singleProjectCode", "single_project_code"):
                station_with_single += 1

        if dataset_key == "daily_meeting":
            daily_total += 1
            source_file = raw_event.get("source_file") or raw_event.get("source_ref", {}).get("source_file")
            if source_file or raw.get("workDate") not in (None, ""):
                daily_with_work_date += 1
            if _has_code(raw, context, "prjCode", "project_code") or raw.get("projectCode") not in (None, ""):
                daily_with_project += 1
            if _has_code(raw, context, "singleProjectCode", "single_project_code"):
                daily_with_single += 1
            if _has_code(raw, context, "biddingSectionCode", "bidding_section_code"):
                daily_with_bidding += 1

    return {
        "line_section.section_details": {
            "total": line_section_total,
            "with_context": line_with_context,
            "missing_context": line_missing_context,
            "missing_single_project_code": line_missing_single,
            "missing_bidding_section_code": line_missing_bidding,
            "status": _context_status(
                line_section_total,
                max(line_missing_single, line_missing_bidding),
            ),
        },
        "tower.tower_details": {
            "total": tower_total,
            "with_project_code": tower_with_project,
            "with_single_project_code": tower_with_single,
            "with_bidding_section_code": tower_with_bidding,
            "missing_single_project_code": max(0, tower_total - tower_with_single),
            "missing_bidding_section_code": max(0, tower_total - tower_with_bidding),
            "status": _context_status(
                tower_total,
                max(tower_total - tower_with_single, tower_total - tower_with_bidding),
            ),
        },
        "station.substation_coordinates": {
            "total": station_total,
            "with_project_code": station_with_project,
            "with_single_project_code": station_with_single,
            "missing_single_project_code": max(0, station_total - station_with_single),
            "status": _context_status(
                station_total,
                station_total - station_with_single,
            ),
        },
        "daily_meeting": {
            "total": daily_total,
            "with_work_date": daily_with_work_date,
            "with_project_code": daily_with_project,
            "with_single_project_code": daily_with_single,
            "with_bidding_section_code": daily_with_bidding,
            "missing_work_date": max(0, daily_total - daily_with_work_date),
            "status": _context_status(
                daily_total,
                daily_total - daily_with_work_date,
            ),
        },
    }
