"""DCP station normalizer."""

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


def normalize_station(raw_event: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    """Normalize one station raw event into a canonical entity payload."""
    if raw_event.get("dataset_key") != "station":
        return None, "not station dataset"
    if raw_event.get("api_name") != "substation_coordinates":
        return None, "not substation_coordinates api"
    if not raw_event.get("collected_at"):
        return None, "missing collected_at"
    collected_at_epoch = _parse_epoch(raw_event.get("collected_at"))
    if collected_at_epoch is None:
        return None, "invalid collected_at"

    payload = raw_event.get("payload") or {}
    raw = payload.get("raw")
    if not isinstance(raw, dict):
        return None, "payload.raw must be an object"

    dcp_coordinate_id = raw.get("id")
    station_identity = raw.get("singleProjectCode") or dcp_coordinate_id
    if station_identity in (None, ""):
        return None, "missing station identity"

    longitude = _float_value(raw.get("longitude"))
    latitude = _float_value(raw.get("latitude"))
    if longitude is None or latitude is None:
        return None, "invalid longitude/latitude"
    if not _is_hunan_coordinate(longitude, latitude):
        return None, "coordinate outside hunan range"

    attributes = {
        "project_code": raw.get("prjCode"),
        "single_project_code": raw.get("singleProjectCode"),
        "dcp_coordinate_id": dcp_coordinate_id,
        "longitude": longitude,
        "latitude": latitude,
        # Debug-only lineage snapshot. Consumer DTOs must not expose DCP raw fields directly.
        # substation_single_projects name enrichment is not implemented yet and
        # must be added before the Monitor detail panel depends on station names.
        "raw": raw,
    }
    return {
        "entity_type": "station",
        "entity_key": f"dcp:station:{station_identity}",
        "entity_date": None,
        "dataset_key": "station",
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
