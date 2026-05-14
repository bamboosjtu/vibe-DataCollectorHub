"""Manual normalizer runner for MVP raw_event data."""

from typing import Any

from processing.dcp.daily_meeting import normalize_daily_meeting
from processing.dcp.domain import (
    normalize_line_section,
    normalize_project_hierarchy,
    normalize_year_progress,
)
from processing.dcp.station import normalize_station
from processing.dcp.tower import normalize_tower
from storage.sqlite_store import SQLiteStore

NORMALIZERS = {
    "daily_meeting": {
        "version": "daily_meeting.v1",
        "handler": normalize_daily_meeting,
    },
    "tower": {
        "version": "tower.v1",
        "handler": normalize_tower,
    },
    "station": {
        "version": "station.v1",
        "handler": normalize_station,
    },
    "project_hierarchy": {
        "version": "project_hierarchy.v1",
        "handler": normalize_project_hierarchy,
        "source_dataset_key": None,
    },
    "project_preconstruction": {
        "version": "project_hierarchy.v1",
        "handler": normalize_project_hierarchy,
    },
    "line_section": {
        "version": "line_section.v1",
        "handler": normalize_line_section,
    },
    "year_progress": {
        "version": "year_progress.v1",
        "handler": normalize_year_progress,
    },
}

MONITOR_PROCESSING_DATASETS = ["daily_meeting", "tower", "station"]


def supported_datasets(include_domain: bool = False) -> list[str]:
    """Return dataset keys with registered normalizers."""
    if include_domain:
        return list(NORMALIZERS.keys())
    # Monitor processing remains scoped to the MVP datasets even though the
    # background processing job API can opt into domain normalizers.
    return list(MONITOR_PROCESSING_DATASETS)


class NormalizerRunner:
    """Run registered normalizers over raw_events."""

    def __init__(self, store: SQLiteStore):
        self.store = store

    def _normalize_result(
        self, result: dict[str, Any] | None
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any] | None]:
        if not result:
            return [], [], None
        if "entities" in result or "relationships" in result or "skip_summary" in result:
            entities = result.get("entities") or []
            relationships = result.get("relationships") or []
            return (
                [item for item in entities if isinstance(item, dict)],
                [item for item in relationships if isinstance(item, dict)],
                result.get("skip_summary") if isinstance(result.get("skip_summary"), dict) else None,
            )
        return [result], [], None

    def run(
        self,
        dataset_key: str,
        batch_size: int = 1000,
        mode: str = "incremental",
    ) -> dict[str, Any]:
        normalizer_entry = NORMALIZERS.get(dataset_key)
        if not normalizer_entry:
            return {
                "processed": 0,
                "inserted": 0,
                "updated": 0,
                "ignored_older": 0,
                "skipped": 0,
                "failed": 1,
                "last_raw_event_id": None,
                "errors": [f"unsupported dataset_key: {dataset_key}"],
            }
        normalizer_version = normalizer_entry["version"]
        normalizer = normalizer_entry["handler"]
        source_dataset_key = normalizer_entry.get("source_dataset_key", dataset_key)
        if mode not in {"incremental", "full"}:
            return {
                "processed": 0,
                "inserted": 0,
                "updated": 0,
                "ignored_older": 0,
                "skipped": 0,
                "failed": 1,
                "last_raw_event_id": None,
                "errors": [f"unsupported processing mode: {mode}"],
            }

        processed = 0
        inserted = 0
        updated = 0
        ignored_older = 0
        relationships_inserted = 0
        relationships_updated = 0
        skipped = 0
        failed = 0
        errors: list[str] = []
        state = self.store.get_normalizer_state(dataset_key)
        state_version = state.get("normalizer_version")
        if mode == "incremental" and state_version == normalizer_version:
            last_raw_event_id = int(state.get("last_raw_event_id") or 0)
        else:
            last_raw_event_id = 0
        offset = 0

        while True:
            if mode == "incremental":
                raw_events = self.store.list_raw_events(
                    dataset_key=source_dataset_key,
                    limit=batch_size,
                    after_id=last_raw_event_id,
                )
            else:
                raw_events = self.store.list_raw_events(
                    dataset_key=source_dataset_key,
                    limit=batch_size,
                    offset=offset,
                )
            if not raw_events:
                break

            batch_last_raw_event_id = last_raw_event_id
            failed_in_batch = False
            for raw_event in raw_events:
                raw_event_id = int(raw_event.get("id") or 0)
                normalized, error = normalizer(raw_event)
                if error:
                    skipped += 1
                    errors.append(f"raw_event_id={raw_event.get('id')}: {error}")
                    batch_last_raw_event_id = max(batch_last_raw_event_id, raw_event_id)
                    continue
                entities, relationships, skip_summary = self._normalize_result(normalized)
                if skip_summary is not None and not entities and not relationships:
                    skipped += 1
                    errors.append(
                        f"raw_event_id={raw_event.get('id')}: {skip_summary}"
                    )
                    batch_last_raw_event_id = max(batch_last_raw_event_id, raw_event_id)
                    continue

                try:
                    for entity in entities:
                        status = self.store.upsert_canonical_entity(**entity)
                        processed += 1
                        if status == "inserted":
                            inserted += 1
                        elif status == "updated":
                            updated += 1
                        elif status == "ignored_older":
                            ignored_older += 1
                        else:
                            failed += 1
                            failed_in_batch = True
                            errors.append(
                                f"raw_event_id={raw_event.get('id')}: unknown entity upsert status: {status}"
                            )
                            break
                    if failed_in_batch:
                        break
                    for relationship in relationships:
                        relationship_status = self.store.upsert_canonical_relationship(
                            **relationship
                        )
                        if relationship_status == "inserted":
                            relationships_inserted += 1
                        elif relationship_status == "updated":
                            relationships_updated += 1
                        else:
                            failed += 1
                            failed_in_batch = True
                            errors.append(
                                f"raw_event_id={raw_event.get('id')}: unknown relationship upsert status: {relationship_status}"
                            )
                            break
                    if failed_in_batch:
                        break
                    batch_last_raw_event_id = max(batch_last_raw_event_id, raw_event_id)
                except Exception as exc:
                    failed += 1
                    failed_in_batch = True
                    errors.append(f"raw_event_id={raw_event.get('id')}: {exc}")
                    break

            if not failed_in_batch and batch_last_raw_event_id > last_raw_event_id:
                self.store.save_normalizer_state(
                    dataset_key, batch_last_raw_event_id, normalizer_version
                )
                last_raw_event_id = batch_last_raw_event_id
            elif failed_in_batch:
                break

            if len(raw_events) < batch_size:
                break
            if mode == "full":
                offset += batch_size

        if failed == 0:
            self.store.save_normalizer_state(
                dataset_key, last_raw_event_id, normalizer_version
            )

        return {
            "processed": processed,
            "inserted": inserted,
            "updated": updated,
            "ignored_older": ignored_older,
            "relationships_inserted": relationships_inserted,
            "relationships_updated": relationships_updated,
            "skipped": skipped,
            "failed": failed,
            "last_raw_event_id": last_raw_event_id,
            "errors": errors,
        }
