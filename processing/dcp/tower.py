"""DCP tower normalizer."""

from datetime import datetime
import math
from typing import Any

from processing.dcp.geo import _is_hunan_coordinate
from processing.dcp.keys import dcp_tower_key, dcp_unscoped_tower_key, normalize_tower_no


def _parse_epoch(timestamp: Any) -> float | None:
    try:
        return datetime.fromisoformat(str(timestamp).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def _float_value(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _source_context(raw_event: dict[str, Any]) -> dict[str, Any]:
    source_ref = raw_event.get("source_ref") or {}
    context = source_ref.get("context") if isinstance(source_ref, dict) else None
    return context if isinstance(context, dict) else {}


def _first_present(raw: dict[str, Any], context: dict[str, Any], raw_key: str, context_key: str) -> Any:
    value = raw.get(raw_key)
    if value not in (None, ""):
        return value
    value = context.get(context_key)
    if value not in (None, ""):
        return value
    return None


def normalize_tower(
    raw_event: dict[str, Any],
) -> tuple[dict[str, Any] | None, str | None]:
    """Normalize one DCP tower_details raw event into a tower entity."""
    if raw_event.get("dataset_key") != "tower":
        return None, "not tower dataset"
    if raw_event.get("api_name") != "tower_details":
        return None, "not tower_details api"
    if not raw_event.get("collected_at"):
        return None, "missing collected_at"
    collected_at_epoch = _parse_epoch(raw_event.get("collected_at"))
    if collected_at_epoch is None:
        return None, "invalid collected_at"

    payload = raw_event.get("payload") or {}
    raw = payload.get("raw")
    if not isinstance(raw, dict):
        return None, "payload.raw must be an object"

    context = _source_context(raw_event)
    tower_id = raw.get("id")
    project_code = _first_present(raw, context, "prjCode", "project_code")
    single_project_code = _first_present(
        raw, context, "singleProjectCode", "single_project_code"
    )
    bidding_section_code = _first_present(
        raw, context, "biddingSectionCode", "bidding_section_code"
    )
    tower_no = normalize_tower_no(
        raw.get("towerNo") or raw.get("towerNoName") or raw.get("towerName")
    )
    known_issues: list[str] = []
    dcp_entity_key_fallback = (
        f"dcp:tower:{tower_id}" if tower_id not in (None, "") else None
    )
    if tower_no is None:
        if dcp_entity_key_fallback is None:
            return None, "missing tower identity"
        entity_key = dcp_entity_key_fallback
        known_issues.append(
            "tower_details missing stable tower number; canonical key fell back to DCP id"
        )
    elif single_project_code not in (None, "") and bidding_section_code not in (None, ""):
        entity_key = dcp_tower_key(
            str(single_project_code), str(bidding_section_code), tower_no
        )
    else:
        entity_key = dcp_unscoped_tower_key(tower_no)
        known_issues.append(
            "tower_details missing singleProjectCode/biddingSectionCode; canonical key is unscoped"
        )

    longitude = _float_value(raw.get("longitudeEdit"))
    latitude = _float_value(raw.get("latitudeEdit"))
    if longitude is None or latitude is None:
        return None, "invalid longitudeEdit/latitudeEdit"
    if not _is_hunan_coordinate(longitude, latitude):
        return None, "coordinate outside hunan range"

    attributes = {
        "tower_id": tower_id,
        "dcp_tower_id": tower_id,
        "dcp_entity_key_fallback": dcp_entity_key_fallback,
        "project_code": project_code,
        "single_project_code": single_project_code,
        "bidding_section_code": bidding_section_code,
        "tower_no": tower_no,
        "upstream_tower_no": raw.get("upstreamTowerNo"),
        "longitude": longitude,
        "latitude": latitude,
        "tower_type": raw.get("towerType"),
        "tower_full_height": raw.get("towerFullHeight"),
        "nominal_height": raw.get("nominalHeight"),
        # Debug-only lineage snapshot. Consumer DTOs must not expose DCP raw fields directly.
        "raw": raw,
    }
    if known_issues:
        attributes["known_issues"] = known_issues

    return {
        "entity_type": "tower",
        "entity_key": entity_key,
        "entity_date": None,
        "dataset_key": "tower",
        "source_system": raw_event.get("source_system"),
        "source_record_key": raw_event.get("source_record_key"),
        "latest_raw_event_id": raw_event.get("id"),
        "latest_collected_at": raw_event.get("collected_at"),
        "latest_collected_at_epoch": collected_at_epoch,
        "latest_source_record_hash": raw_event.get("source_record_hash"),
        "source_refs": [
            {
                "source_system": raw_event.get("source_system"),
                "dataset_key": raw_event.get("dataset_key"),
                "source_record_key": raw_event.get("source_record_key"),
                "source_record_id": raw_event.get("source_record_id"),
                "source_record_hash": raw_event.get("source_record_hash"),
                "raw_event_id": raw_event.get("id"),
            }
        ],
        "attributes": attributes,
    }, None
