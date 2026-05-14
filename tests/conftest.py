"""Shared pytest configuration and MVP raw-event test helpers."""

from __future__ import annotations

from pathlib import Path
import sys
from typing import Any
from uuid import uuid4


PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def seed_test_records(
    store,
    dataset_key: str,
    records: list[dict[str, Any]],
    *,
    source_system: str = "dcp",
    plugin_id: str = "dcp",
    downloader_name: str = "vibe-downloader-dcp",
    profile: str = "fixture",
    command_type: str = "collect",
) -> dict[str, Any]:
    """Persist test records through the MVP ingestion.batch.v1 path."""
    batch_id = f"batch_{dataset_key}_{uuid4().hex}"
    command_run_id = f"cmd_{dataset_key}_{uuid4().hex}"
    now = records[0]["collected_at"] if records else None

    request_index_by_key: dict[str, int] = {}
    requests: list[dict[str, Any]] = []
    raw_events: list[dict[str, Any]] = []

    for record in records:
        source_ref = dict(record.get("source_ref") or {})
        request_key = str(
            source_ref.get("request_key")
            or source_ref.get("request_id")
            or record.get("idempotency_key")
            or record["raw_event_id"]
        )
        if request_key not in request_index_by_key:
            request_id = str(
                source_ref.get("request_id")
                or f"{command_run_id}:request:{len(requests) + 1}"
            )
            request_index_by_key[request_key] = len(requests)
            requests.append(
                {
                    "request_id": request_id,
                    "batch_id": batch_id,
                    "command_run_id": command_run_id,
                    "dataset_key": dataset_key,
                    "request_key": request_key,
                    "request_kind": str(source_ref.get("request_kind") or "collect"),
                    "source_system": source_system,
                    "plugin_id": plugin_id,
                    "downloader_name": downloader_name,
                    "api_name": source_ref.get("api_name"),
                    "source_path": source_ref.get("source_file"),
                    "request_params": source_ref.get("request_params") or {},
                    "request_context": source_ref.get("context") or {},
                    "response_meta": source_ref.get("response_meta") or {},
                    "status": str(source_ref.get("status") or "succeeded"),
                    "raw_record_count": 0,
                    "error_count": 0,
                    "requested_at": record.get("occurred_at") or record["collected_at"],
                    "completed_at": record["collected_at"],
                }
            )
        request = requests[request_index_by_key[request_key]]
        request["raw_record_count"] += 1

        raw_events.append(
            {
                "raw_event_id": record["raw_event_id"],
                "raw_event_key": record.get("raw_event_key") or record["raw_event_id"],
                "batch_id": batch_id,
                "command_run_id": command_run_id,
                "request_id": request["request_id"],
                "dataset_key": dataset_key,
                "source_system": source_system,
                "raw_record_type": record.get("raw_record_type") or source_ref.get("api_name"),
                "raw_payload": (record.get("payload") or {}).get("raw", {}),
                "source_path": source_ref.get("record_path"),
                "source_record_id": record.get("source_record_id"),
                "source_record_hash": record.get("source_record_hash"),
                "source_record_key": record.get("idempotency_key"),
                "occurred_at": record.get("occurred_at") or record["collected_at"],
                "collected_at": record["collected_at"],
                "processing_status": "pending",
                "request_context": source_ref.get("context") or {},
                "collection": source_ref.get("collection"),
                "page_name": source_ref.get("page_name"),
                "api_name": source_ref.get("api_name"),
                "source_file": source_ref.get("source_file"),
            }
        )

    payload = {
        "batch": {
            "batch_id": batch_id,
            "batch_key": batch_id,
            "source_system": source_system,
            "plugin_id": plugin_id,
            "downloader_name": downloader_name,
            "trigger_type": "manual",
            "status": "succeeded",
            "command_count": 1,
            "request_count": len(requests),
            "raw_record_count": len(raw_events),
            "error_count": 0,
            "metadata_snapshot": {},
            "config_snapshot": {},
            "result_summary": {},
            "started_at": now,
            "finished_at": now,
        },
        "commands": [
            {
                "command_run_id": command_run_id,
                "batch_id": batch_id,
                "command_key": f"{dataset_key}:{profile}",
                "command_type": command_type,
                "source_system": source_system,
                "plugin_id": plugin_id,
                "downloader_name": downloader_name,
                "dataset_keys": [dataset_key],
                "profile": profile,
                "params": {},
                "options": {},
                "status": "succeeded",
                "request_count": len(requests),
                "raw_record_count": len(raw_events),
                "success_request_count": len(requests),
                "failed_request_count": 0,
                "error_count": 0,
                "processing_policy": None,
                "result_summary": {},
                "started_at": now,
                "finished_at": now,
            }
        ],
        "requests": requests,
        "raw_events": raw_events,
        "errors": [],
        "checkpoints": [],
    }
    stats = store.save_ingestion_batch(payload)
    stored = {
        item["raw_event_id"]: item["id"]
        for item in store.list_raw_events(dataset_key=dataset_key, limit=max(len(raw_events), 1) + 1000)
        if item.get("raw_event_id") in {record["raw_event_id"] for record in records}
    }
    return {"stats": stats, "raw_event_ids": stored, "batch_id": batch_id, "command_run_id": command_run_id}
