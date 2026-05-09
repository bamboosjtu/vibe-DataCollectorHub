from __future__ import annotations

from typing import Any

from health.dataset_health import (
    TARGET_DATASETS,
    get_context_coverage,
    get_daily_meeting_date_health,
    get_dataset_health,
)
from health.domain_health import get_domain_health
from health.job_health import get_job_health
from storage.sqlite_store import SQLiteStore


def get_health_summary(
    store: SQLiteStore,
    *,
    recent_days: int = 14,
    running_timeout_hours: int = 6,
) -> dict[str, Any]:
    dataset_health = get_dataset_health(store)
    job_health = get_job_health(store, running_timeout_hours=running_timeout_hours)
    domain_health = get_domain_health(store)
    daily_meeting_health = get_daily_meeting_date_health(
        store,
        recent_days=recent_days,
    )
    context_health = get_context_coverage(store)

    failed_reasons: list[str] = []
    warning_reasons: list[str] = []

    if job_health["external_collection_jobs"]["timed_out_running_jobs"]:
        failed_reasons.append("external_collection_jobs has running job older than threshold")
    if job_health["processing_jobs"]["timed_out_running_jobs"]:
        failed_reasons.append("processing_jobs has running job older than threshold")

    latest_external = job_health["external_collection_jobs"]["latest_job"]
    if latest_external and latest_external.get("status") == "failed":
        failed_reasons.append("latest external collection job failed")

    latest_processing = job_health["processing_jobs"]["latest_job"]
    if latest_processing and latest_processing.get("status") == "failed":
        failed_reasons.append("latest processing job failed")

    if domain_health["critical_relationship_count"] == 0:
        failed_reasons.append("domain critical relationship count is 0")
    if domain_health["unscoped_tower_sequence_count"] > 0:
        failed_reasons.append("unscoped tower sequence relationships detected")

    if daily_meeting_health["missing_dates"]:
        warning_reasons.append("daily_meeting has missing recent dates")
    if domain_health["tower_sequence_missing_physical_entity_count"] > 0:
        warning_reasons.append(
            "tower sequence physical candidates point to missing tower entities"
        )
    if domain_health["tower_sequence_scope_without_tower_count"] > 0:
        warning_reasons.append("some line-section scopes have no tower entities")

    for key, coverage in context_health.items():
        if coverage["status"] == "failed":
            failed_reasons.append(f"context coverage failed: {key}")
        elif coverage["status"] == "warning":
            warning_reasons.append(f"context coverage warning: {key}")

    for dataset_key in TARGET_DATASETS:
        item = dataset_health["datasets"][dataset_key]
        if item["raw_event_count"] > 0 and item["canonical_entity_count"] == 0:
            warning_reasons.append(
                f"dataset {dataset_key} has raw_events but no canonical entities"
            )

    if domain_health["line_section_known_issue_count"] > 0:
        warning_reasons.append("line_section known_issues present")

    if failed_reasons:
        overall_status = "failed"
        reasons = failed_reasons + warning_reasons
    elif warning_reasons:
        overall_status = "warning"
        reasons = warning_reasons
    else:
        overall_status = "ok"
        reasons = []

    return {
        "overall_status": overall_status,
        "reasons": reasons,
        "dataset_health": dataset_health,
        "job_health": job_health,
        "domain_health": domain_health,
        "daily_meeting_health": daily_meeting_health,
        "context_health": context_health,
    }
