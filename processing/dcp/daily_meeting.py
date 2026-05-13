"""DCP daily meeting normalizer."""

from datetime import datetime
import math
from pathlib import PurePath
import re
from typing import Any
from processing.dcp.geo import _is_hunan_coordinate, _is_valid_coordinate


def _normalize_work_date(value: Any) -> str | None:
    """Normalize DCP work date values to YYYY-MM-DD for Monitor timeline queries."""
    if value in (None, ""):
        return None

    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        try:
            return datetime.fromtimestamp(float(value) / 1000).date().isoformat()
        except (OSError, OverflowError, ValueError):
            return None

    text = str(value).strip()
    if not text:
        return None

    try:
        if text.isdigit() and len(text) >= 12:
            return datetime.fromtimestamp(float(text) / 1000).date().isoformat()
        if len(text) == 10 and text[4] == "-" and text[7] == "-":
            return datetime.strptime(text, "%Y-%m-%d").date().isoformat()
        normalized = text.replace("Z", "+00:00")
        if " " in normalized and "T" not in normalized:
            normalized = normalized.replace(" ", "T", 1)
        return datetime.fromisoformat(normalized).date().isoformat()
    except (OSError, OverflowError, ValueError):
        return None


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


def _int_value(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _first_present(raw: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = raw.get(key)
        if value not in (None, ""):
            return value
    return None


def _source_context(raw_event: dict[str, Any]) -> dict[str, Any]:
    source_ref = raw_event.get("source_ref") or {}
    context = source_ref.get("context") if isinstance(source_ref, dict) else None
    return context if isinstance(context, dict) else {}


def _first_present_raw_or_context(
    raw: dict[str, Any],
    context: dict[str, Any],
    raw_keys: tuple[str, ...],
    context_key: str,
) -> Any:
    value = _first_present(raw, *raw_keys)
    if value not in (None, ""):
        return value
    value = context.get(context_key)
    if value not in (None, ""):
        return value
    return None


_SOURCE_FILE_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _source_file_work_date(raw_event: dict[str, Any]) -> str | None:
    """Extract daily_meeting work_date from source_file filename.

    For daily_meeting, the file partition is the authoritative business date.
    Expected examples:
    - daily_meeting\\2024-01-22.json
    - ..\\data\\safe\\daily_meeting\\2024-01-22.json
    - ../data/safe/daily_meeting/2024-01-22.json
    """
    source_file = raw_event.get("source_file")

    if not source_file:
        source_ref = raw_event.get("source_ref")
        if isinstance(source_ref, dict):
            source_file = source_ref.get("source_file")

    if not source_file:
        return None

    normalized_path = str(source_file).replace("\\", "/")
    filename = PurePath(normalized_path).name

    if not filename.endswith(".json"):
        return None

    stem = filename[:-5]
    if not _SOURCE_FILE_DATE_RE.match(stem):
        return None

    try:
        return datetime.strptime(stem, "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


def _context_work_date(raw_event: dict[str, Any], raw: dict[str, Any]) -> str | None:
    context = _source_context(raw_event)
    for value in (
        context.get("date"),
        context.get("work_date"),
        raw.get("workDate"),
        raw.get("meetingDate"),
        raw.get("currentConstrDate"),
    ):
        normalized = _normalize_work_date(value)
        if normalized is not None:
            return normalized
    return None


def _normalize_risk_level(value: Any) -> str:
    if value in (None, ""):
        return "unknown"
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        numeric = float(value)
        if numeric.is_integer() and int(numeric) in {1, 2, 3, 4}:
            return str(int(numeric))
    if str(value).strip() in {"1", "2", "3", "4"}:
        return str(value).strip()
    normalized = str(value).strip().lower()
    mapping = {
        "critical": "1",
        "very high": "1",
        "very_high": "1",
        "重大": "1",
        "重大风险": "1",
        "特高": "1",
        "特高风险": "1",
        "极高": "1",
        "high": "2",
        "h": "2",
        "高": "2",
        "高风险": "2",
        "较高": "2",
        "较高风险": "2",
        "medium": "3",
        "mid": "3",
        "m": "3",
        "中": "3",
        "中等": "3",
        "中风险": "3",
        "low": "4",
        "l": "4",
        "低": "4",
        "低风险": "4",
        "一般": "4",
        "一般风险": "4",
        "unknown": "unknown",
        "未知": "unknown",
    }
    return mapping.get(normalized, "unknown")


def _normalize_work_status(value: Any) -> str:
    if value in (None, ""):
        return "unknown"
    normalized = str(value).strip().lower()
    mapping = {
        "working": "working",
        "work": "working",
        "in_progress": "working",
        "进行中": "working",
        "作业中": "working",
        "施工中": "working",
        "paused": "paused",
        "pause": "paused",
        "suspended": "paused",
        "作业暂停": "paused",
        "暂停": "paused",
        "停工": "paused",
        "finished": "finished",
        "finish": "finished",
        "completed": "finished",
        "done": "finished",
        "当日作业完工": "finished",
        "已完成": "finished",
        "完成": "finished",
        "unknown": "unknown",
        "未知": "unknown",
    }
    return mapping.get(normalized, "unknown")


def normalize_daily_meeting(
    raw_event: dict[str, Any],
) -> tuple[dict[str, Any] | None, str | None]:
    """Normalize one DCP daily meeting raw event into a work point entity."""
    if raw_event.get("dataset_key") != "daily_meeting":
        return None, "not daily_meeting dataset"
    if raw_event.get("collection") != "safePages":
        return None, "not safePages collection"
    if raw_event.get("page_name") not in {"meetingListAdmin", "站班会"}:
        return None, "not daily meeting page"
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
    longitude = _float_value(raw.get("toolBoxTalkLongitude"))
    latitude = _float_value(raw.get("toolBoxTalkLatitude"))
    if longitude is None or latitude is None:
        return None, "invalid toolBoxTalkLongitude/toolBoxTalkLatitude"
    if not _is_valid_coordinate(longitude, latitude):
        return None, "invalid coordinate"
    if not _is_hunan_coordinate(longitude, latitude):
        return None, "coordinate outside hunan range"

    work_point_id = _first_present(
        raw,
        "id",
        "toolBoxTalkId",
        "toolboxTalkId",
        "meetingId",
    ) or raw_event.get("source_record_id")
    if work_point_id in (None, ""):
        return None, "missing work point identity"
    work_date = _source_file_work_date(raw_event)
    if work_date is None:
        work_date = _context_work_date(raw_event, raw)
    if work_date is None:
        return None, "invalid source_file/context work_date"

    person_count = _int_value(
        _first_present(
            raw,
            "currentConstrHeadcount",
            "personCount",
            "toolBoxTalkPersonCount",
            "workerCount",
        )
    )
    risk_level = _normalize_risk_level(
        _first_present(raw, "reAssessmentRiskLevel", "riskLevel", "risk_level")
    )
    work_status = _normalize_work_status(
        _first_present(raw, "currentConstructionStatus", "workStatus", "work_status")
    )

    attributes = {
        "project_code": _first_present_raw_or_context(
            raw, context, ("prjCode", "projectCode"), "project_code"
        ),
        "single_project_code": _first_present_raw_or_context(
            raw, context, ("singleProjectCode", "singlePrjCode"), "single_project_code"
        ),
        "bidding_section_code": _first_present_raw_or_context(
            raw, context, ("biddingSectionCode", "bidSectCode"), "bidding_section_code"
        ),
        "project_name": _first_present(raw, "projectName", "prjName", "project_name"),
        "longitude": longitude,
        "latitude": latitude,
        "person_count": person_count,
        "risk_level": risk_level,
        "work_status": work_status,
        "voltage_level": _first_present(raw, "voltageLevel", "voltage_level"),
        "city": _first_present(raw, "buildUnitName", "city", "cityName"),
        "work_date": work_date,
        "coordinate_in_hunan": _is_hunan_coordinate(longitude, latitude),
        "source_file_work_date": work_date,
        "raw_current_constr_date": raw.get("currentConstrDate"),
        "raw_work_start_time": raw.get("workStartTime"),
        "raw": raw,
    }

    return {
        "entity_type": "work_point",
        "entity_key": f"dcp:work_point:{work_date}:{work_point_id}",
        "entity_date": work_date,
        "dataset_key": "daily_meeting",
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
