"""DCP tower normalizer."""

from datetime import datetime
import math
from typing import Any

from processing.dcp.geo import _is_hunan_coordinate


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

    tower_id = raw.get("id")
    single_project_code = raw.get("singleProjectCode")
    bidding_section_code = raw.get("biddingSectionCode")
    tower_no = raw.get("towerNo")
    if tower_id not in (None, ""):
        entity_key = f"dcp:tower:{tower_id}"
    else:
        if (
            single_project_code in (None, "")
            or bidding_section_code in (None, "")
            or tower_no in (None, "")
        ):
            return None, "missing tower identity"
        entity_key = f"dcp:tower:{single_project_code}:{bidding_section_code}:{tower_no}"

    longitude = _float_value(raw.get("longitudeEdit"))
    latitude = _float_value(raw.get("latitudeEdit"))
    if longitude is None or latitude is None:
        return None, "invalid longitudeEdit/latitudeEdit"
    if not _is_hunan_coordinate(longitude, latitude):
        return None, "coordinate outside hunan range"

    attributes = {
        "tower_id": tower_id,
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
