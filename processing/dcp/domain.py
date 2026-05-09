"""DCP domain hierarchy and relationship normalizers.

The DCP source is a legacy system with stable response schemas. Extractors in
this module dispatch by api_name and use field names observed in downloader
fixtures/data instead of inventing fallback aliases.
"""

from __future__ import annotations

from datetime import datetime
import re
from typing import Any

from processing.dcp.keys import dcp_tower_key, dcp_unscoped_tower_key, normalize_tower_no


def _parse_epoch(timestamp: Any) -> float | None:
    try:
        return datetime.fromisoformat(str(timestamp).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def _raw(raw_event: dict[str, Any]) -> dict[str, Any] | None:
    payload = raw_event.get("payload") or {}
    value = payload.get("raw")
    return value if isinstance(value, dict) else None


def _source_ref(raw_event: dict[str, Any]) -> dict[str, Any]:
    value = raw_event.get("source_ref") or {}
    return value if isinstance(value, dict) else {}


def _source_context(raw_event: dict[str, Any]) -> dict[str, Any]:
    value = _source_ref(raw_event).get("context")
    return value if isinstance(value, dict) else {}


def _api_name(raw_event: dict[str, Any]) -> str:
    return str(raw_event.get("api_name") or _source_ref(raw_event).get("api_name") or "")


def _source_record_ref(raw_event: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_system": raw_event.get("source_system"),
        "dataset_key": raw_event.get("dataset_key"),
        "source_record_key": raw_event.get("source_record_key"),
        "source_record_id": raw_event.get("source_record_id"),
        "source_record_hash": raw_event.get("source_record_hash"),
        "raw_event_id": raw_event.get("id"),
    }


def _entity(
    *,
    entity_type: str,
    entity_key: str,
    dataset_key: str,
    raw_event: dict[str, Any],
    attributes: dict[str, Any],
) -> dict[str, Any]:
    return {
        "entity_type": entity_type,
        "entity_key": entity_key,
        "entity_date": None,
        "dataset_key": dataset_key,
        "source_system": raw_event.get("source_system"),
        "source_record_key": raw_event.get("source_record_key"),
        "latest_raw_event_id": raw_event.get("id"),
        "latest_collected_at": raw_event.get("collected_at"),
        "latest_collected_at_epoch": _parse_epoch(raw_event.get("collected_at")),
        "latest_source_record_hash": raw_event.get("source_record_hash"),
        "source_refs": [_source_record_ref(raw_event)],
        "attributes": attributes,
    }


def _relationship(
    *,
    relationship_type: str,
    from_entity_type: str,
    from_entity_key: str,
    to_entity_type: str,
    to_entity_key: str,
    dataset_key: str,
    raw_event: dict[str, Any],
    attributes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    relationship_key = ":".join(
        [
            "dcp",
            "relationship",
            relationship_type,
            from_entity_key,
            to_entity_key,
        ]
    )
    return {
        "relationship_key": relationship_key,
        "relationship_type": relationship_type,
        "from_entity_type": from_entity_type,
        "from_entity_key": from_entity_key,
        "to_entity_type": to_entity_type,
        "to_entity_key": to_entity_key,
        "dataset_key": dataset_key,
        "source_system": raw_event.get("source_system"),
        "latest_raw_event_id": raw_event.get("id"),
        "latest_collected_at": raw_event.get("collected_at"),
        "attributes": attributes or {},
    }


def _first_present(raw: dict[str, Any], context: dict[str, Any], raw_key: str, context_key: str) -> Any:
    value = raw.get(raw_key)
    if value not in (None, ""):
        return value
    value = context.get(context_key)
    if value not in (None, ""):
        return value
    return None


def _string_value(value: Any) -> str | None:
    return None if value in (None, "") else str(value)


def _contains_chinese(value: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", value))


def _tower_sequence_node_kind(tower_no: str) -> str:
    if (
        "线#" in tower_no
        or "站" in tower_no
        or "龙门架" in tower_no
        or "间隔" in tower_no
        or (_contains_chinese(tower_no) and "#" in tower_no)
    ):
        return "reference_node"
    return "physical_candidate"


def _codes_from_raw_and_context(raw: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    return {
        "project_code": _string_value(_first_present(raw, context, "prjCode", "project_code")),
        "project_name": _first_present(raw, context, "prjName", "project_name"),
        "single_project_code": _string_value(
            _first_present(raw, context, "singleProjectCode", "single_project_code")
        ),
        "single_project_name": _first_present(
            raw, context, "singleProjectName", "single_project_name"
        ),
        "bidding_section_code": _string_value(
            _first_present(raw, context, "biddingSectionCode", "bidding_section_code")
        ),
        "bidding_section_name": _first_present(
            raw, context, "biddingSectionName", "bidding_section_name"
        ),
        "line_section_id": _string_value(_first_present(raw, context, "id", "line_section_id")),
        "line_section_name": _first_present(
            raw, context, "sectionName", "line_section_name"
        ),
    }


def _project_entity(
    raw_event: dict[str, Any],
    dataset_key: str,
    raw: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any] | None:
    codes = _codes_from_raw_and_context(raw, context)
    if codes["project_code"] is None:
        return None
    return _entity(
        entity_type="project",
        entity_key=f"dcp:project:{codes['project_code']}",
        dataset_key=dataset_key,
        raw_event=raw_event,
        attributes={
            "project_code": codes["project_code"],
            "project_name": codes["project_name"],
        },
    )


def _single_project_entity(
    raw_event: dict[str, Any],
    dataset_key: str,
    raw: dict[str, Any],
    context: dict[str, Any],
    *,
    project_code: str | None,
) -> dict[str, Any] | None:
    codes = _codes_from_raw_and_context(raw, context)
    if codes["single_project_code"] is None:
        return None
    return _entity(
        entity_type="single_project",
        entity_key=f"dcp:single_project:{codes['single_project_code']}",
        dataset_key=dataset_key,
        raw_event=raw_event,
        attributes={
            "project_code": project_code,
            "single_project_code": codes["single_project_code"],
            "single_project_name": codes["single_project_name"],
        },
    )


def _bidding_section_entity(
    raw_event: dict[str, Any],
    dataset_key: str,
    raw: dict[str, Any],
    context: dict[str, Any],
    *,
    project_code: str | None,
    single_project_code: str | None,
) -> dict[str, Any] | None:
    codes = _codes_from_raw_and_context(raw, context)
    if codes["bidding_section_code"] is None:
        return None
    return _entity(
        entity_type="bidding_section",
        entity_key=f"dcp:bidding_section:{codes['bidding_section_code']}",
        dataset_key=dataset_key,
        raw_event=raw_event,
        attributes={
            "project_code": project_code,
            "single_project_code": single_project_code,
            "bidding_section_code": codes["bidding_section_code"],
            "bidding_section_name": codes["bidding_section_name"],
        },
    )


def extract_preconstruction_results_detail(
    raw_event: dict[str, Any], raw: dict[str, Any]
) -> dict[str, list[dict[str, Any]]]:
    """Extract hierarchy from 项目前期成果/preconstruction_results_detail."""
    dataset_key = str(raw_event.get("dataset_key") or "project_preconstruction")
    context = _source_context(raw_event)
    codes = _codes_from_raw_and_context(raw, context)
    entities: list[dict[str, Any]] = []
    relationships: list[dict[str, Any]] = []

    project = _project_entity(raw_event, dataset_key, raw, context)
    project_key = project["entity_key"] if project else None
    if project:
        entities.append(project)

    sin_list = raw.get("sinList")
    if not isinstance(sin_list, list):
        return {"entities": entities, "relationships": relationships}

    for single in sin_list:
        if not isinstance(single, dict):
            continue
        single_entity = _single_project_entity(
            raw_event,
            dataset_key,
            single,
            context,
            project_code=codes["project_code"],
        )
        single_codes = _codes_from_raw_and_context(single, context)
        single_key = single_entity["entity_key"] if single_entity else None
        if single_entity:
            entities.append(single_entity)
        if project_key and single_key:
            relationships.append(
                _relationship(
                    relationship_type="HAS_SINGLE_PROJECT",
                    from_entity_type="project",
                    from_entity_key=project_key,
                    to_entity_type="single_project",
                    to_entity_key=single_key,
                    dataset_key=dataset_key,
                    raw_event=raw_event,
                )
            )

        bid_sect_list = single.get("bidSectList")
        if not isinstance(bid_sect_list, list):
            continue
        for bidding_section in bid_sect_list:
            if not isinstance(bidding_section, dict):
                continue
            bidding_entity = _bidding_section_entity(
                raw_event,
                dataset_key,
                bidding_section,
                context,
                project_code=codes["project_code"],
                single_project_code=single_codes["single_project_code"],
            )
            if not bidding_entity:
                continue
            entities.append(bidding_entity)
            if single_key:
                relationships.append(
                    _relationship(
                        relationship_type="HAS_BIDDING_SECTION",
                        from_entity_type="single_project",
                        from_entity_key=single_key,
                        to_entity_type="bidding_section",
                        to_entity_key=bidding_entity["entity_key"],
                        dataset_key=dataset_key,
                        raw_event=raw_event,
                    )
                )

    return {"entities": entities, "relationships": relationships}


def extract_yearly_progress_analysis(
    raw_event: dict[str, Any], raw: dict[str, Any]
) -> dict[str, list[dict[str, Any]]]:
    """Extract project progress from 年度进度计划分析/yearly_progress_analysis."""
    dataset_key = "year_progress"
    context = _source_context(raw_event)
    codes = _codes_from_raw_and_context(raw, context)
    project = _project_entity(raw_event, dataset_key, raw, context)
    if not project or not codes["project_code"]:
        return {
            "entities": [],
            "relationships": [],
            "skip_summary": {
                "reason": "yearly_progress_analysis missing prjCode",
                "required_fields": ["prjCode"],
            },
        }

    progress_id = raw.get("id") or codes["project_code"]
    progress_key = f"dcp:project_progress:{progress_id}"
    entities = [
        project,
        _entity(
            entity_type="project_progress",
            entity_key=progress_key,
            dataset_key=dataset_key,
            raw_event=raw_event,
            attributes={
                "project_code": codes["project_code"],
                "project_name": codes["project_name"],
                "raw": raw,
            },
        ),
    ]
    relationships = [
        _relationship(
            relationship_type="HAS_PROJECT_PROGRESS",
            from_entity_type="project",
            from_entity_key=project["entity_key"],
            to_entity_type="project_progress",
            to_entity_key=progress_key,
            dataset_key=dataset_key,
            raw_event=raw_event,
        )
    ]

    single_list = raw.get("singleList")
    if isinstance(single_list, list):
        for single in single_list:
            if not isinstance(single, dict):
                continue
            single_entity = _single_project_entity(
                raw_event,
                dataset_key,
                single,
                context,
                project_code=codes["project_code"],
            )
            if not single_entity:
                continue
            entities.append(single_entity)
            relationships.append(
                _relationship(
                    relationship_type="HAS_SINGLE_PROJECT",
                    from_entity_type="project",
                    from_entity_key=project["entity_key"],
                    to_entity_type="single_project",
                    to_entity_key=single_entity["entity_key"],
                    dataset_key=dataset_key,
                    raw_event=raw_event,
                )
            )

    return {"entities": entities, "relationships": relationships}


def extract_section_single_projects(
    raw_event: dict[str, Any], raw: dict[str, Any]
) -> dict[str, list[dict[str, Any]]]:
    """Extract line-section single project and bidding sections."""
    dataset_key = "line_section"
    context = _source_context(raw_event)
    codes = _codes_from_raw_and_context(raw, context)
    entities: list[dict[str, Any]] = []
    relationships: list[dict[str, Any]] = []

    single_entity = _single_project_entity(
        raw_event,
        dataset_key,
        raw,
        context,
        project_code=codes["project_code"],
    )
    if single_entity:
        entities.append(single_entity)

    sect_list = raw.get("sectList")
    if isinstance(sect_list, list):
        for section in sect_list:
            if not isinstance(section, dict):
                continue
            bidding_entity = _bidding_section_entity(
                raw_event,
                dataset_key,
                section,
                context,
                project_code=codes["project_code"],
                single_project_code=codes["single_project_code"],
            )
            if not bidding_entity:
                continue
            entities.append(bidding_entity)
            if single_entity:
                relationships.append(
                    _relationship(
                        relationship_type="HAS_BIDDING_SECTION",
                        from_entity_type="single_project",
                        from_entity_key=single_entity["entity_key"],
                        to_entity_type="bidding_section",
                        to_entity_key=bidding_entity["entity_key"],
                        dataset_key=dataset_key,
                        raw_event=raw_event,
                    )
                )

    return {"entities": entities, "relationships": relationships}


def extract_section_details(
    raw_event: dict[str, Any], raw: dict[str, Any]
) -> dict[str, list[dict[str, Any]]]:
    """Extract line_section and tower sequence from section_details."""
    dataset_key = "line_section"
    context = _source_context(raw_event)
    codes = _codes_from_raw_and_context(raw, context)
    entities: list[dict[str, Any]] = []
    relationships: list[dict[str, Any]] = []

    if codes["line_section_id"] is None and codes["line_section_name"] in (None, ""):
        return {"entities": [], "relationships": []}

    identity = codes["line_section_id"] or (
        f"{codes['single_project_code'] or 'unknown'}:"
        f"{codes['bidding_section_code'] or 'unknown'}:"
        f"{codes['line_section_name']}"
    )
    line_section_key = f"dcp:line_section:{identity}"
    known_issues: list[str] = []
    if not codes["single_project_code"] or not codes["bidding_section_code"]:
        known_issues.append(
            "section_details SourceEvent lacks request context for singleProjectCode/biddingSectionCode; downloader should add source_ref.context with single_project_code and bidding_section_code"
        )

    attributes = {
        "project_code": codes["project_code"],
        "project_name": codes["project_name"],
        "single_project_code": codes["single_project_code"],
        "single_project_name": codes["single_project_name"],
        "bidding_section_code": codes["bidding_section_code"],
        "bidding_section_name": codes["bidding_section_name"],
        "line_section_id": codes["line_section_id"],
        "line_section_name": codes["line_section_name"],
        "raw": raw,
    }
    if known_issues:
        attributes["known_issues"] = known_issues

    entities.append(
        _entity(
            entity_type="line_section",
            entity_key=line_section_key,
            dataset_key=dataset_key,
            raw_event=raw_event,
            attributes=attributes,
        )
    )

    if codes["bidding_section_code"]:
        relationships.append(
            _relationship(
                relationship_type="HAS_LINE_SECTION",
                from_entity_type="bidding_section",
                from_entity_key=f"dcp:bidding_section:{codes['bidding_section_code']}",
                to_entity_type="line_section",
                to_entity_key=line_section_key,
                dataset_key=dataset_key,
                raw_event=raw_event,
            )
        )

    section_vo = raw.get("sectionVo") if isinstance(raw.get("sectionVo"), dict) else {}
    tower_list = section_vo.get("towerNoList")
    if isinstance(tower_list, list):
        for index, tower_item in enumerate(tower_list, start=1):
            tower_no = normalize_tower_no(
                tower_item.get("towerNo") if isinstance(tower_item, dict) else tower_item
            )
            if tower_no in (None, ""):
                continue
            tower_key = (
                dcp_tower_key(
                    codes["single_project_code"], codes["bidding_section_code"], tower_no
                )
                if codes["single_project_code"] and codes["bidding_section_code"]
                else dcp_unscoped_tower_key(tower_no)
            )
            relationships.append(
                _relationship(
                    relationship_type="HAS_TOWER_SEQUENCE",
                    from_entity_type="line_section",
                    from_entity_key=line_section_key,
                    to_entity_type="tower",
                    to_entity_key=tower_key,
                    dataset_key=dataset_key,
                    raw_event=raw_event,
                    attributes={
                        "sequence": index,
                        "sequence_index": index,
                        "tower_no": tower_no,
                        "node_kind": _tower_sequence_node_kind(tower_no),
                    },
                )
            )

    return {"entities": entities, "relationships": relationships}


def extract_flat_hierarchy(
    raw_event: dict[str, Any], raw: dict[str, Any]
) -> dict[str, list[dict[str, Any]]]:
    """Auxiliary extractor for flat tower/station records with top-level DCP codes."""
    dataset_key = str(raw_event.get("dataset_key") or "dcp")
    context = _source_context(raw_event)
    codes = _codes_from_raw_and_context(raw, context)
    entities: list[dict[str, Any]] = []
    relationships: list[dict[str, Any]] = []

    project = _project_entity(raw_event, dataset_key, raw, context)
    project_key = project["entity_key"] if project else None
    if project:
        entities.append(project)

    single_entity = _single_project_entity(
        raw_event,
        dataset_key,
        raw,
        context,
        project_code=codes["project_code"],
    )
    single_key = single_entity["entity_key"] if single_entity else None
    if single_entity:
        entities.append(single_entity)
    if project_key and single_key:
        relationships.append(
            _relationship(
                relationship_type="HAS_SINGLE_PROJECT",
                from_entity_type="project",
                from_entity_key=project_key,
                to_entity_type="single_project",
                to_entity_key=single_key,
                dataset_key=dataset_key,
                raw_event=raw_event,
            )
        )

    bidding_entity = _bidding_section_entity(
        raw_event,
        dataset_key,
        raw,
        context,
        project_code=codes["project_code"],
        single_project_code=codes["single_project_code"],
    )
    if bidding_entity:
        entities.append(bidding_entity)
    if single_key and bidding_entity:
        relationships.append(
            _relationship(
                relationship_type="HAS_BIDDING_SECTION",
                from_entity_type="single_project",
                from_entity_key=single_key,
                to_entity_type="bidding_section",
                to_entity_key=bidding_entity["entity_key"],
                dataset_key=dataset_key,
                raw_event=raw_event,
            )
        )

    return {"entities": entities, "relationships": relationships}


def normalize_project_hierarchy(raw_event: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    raw = _raw(raw_event)
    if raw is None:
        return None, "payload.raw must be an object"

    api_name = _api_name(raw_event)
    context = _source_context(raw_event)
    codes = _codes_from_raw_and_context(raw, context)
    if api_name == "preconstruction_results_detail":
        payload = extract_preconstruction_results_detail(raw_event, raw)
    elif api_name == "yearly_progress_analysis":
        payload = extract_yearly_progress_analysis(raw_event, raw)
    elif api_name == "section_single_projects":
        payload = extract_section_single_projects(raw_event, raw)
    elif api_name in {"tower_details", "substation_coordinates"} or any(
        codes[field] not in (None, "")
        for field in ("project_code", "single_project_code", "bidding_section_code")
    ):
        payload = extract_flat_hierarchy(raw_event, raw)
    else:
        return None, f"unsupported project_hierarchy api: {api_name}"

    if payload.get("skip_summary"):
        return payload, None
    if not payload["entities"] and not payload["relationships"]:
        return None, f"{api_name} yielded no hierarchy entities"
    return payload, None


def normalize_line_section(raw_event: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    if raw_event.get("dataset_key") != "line_section":
        return None, "not line_section dataset"
    raw = _raw(raw_event)
    if raw is None:
        return None, "payload.raw must be an object"

    api_name = _api_name(raw_event)
    if api_name == "section_single_projects":
        payload = extract_section_single_projects(raw_event, raw)
    elif api_name == "section_details":
        payload = extract_section_details(raw_event, raw)
    else:
        return None, f"unsupported line_section api: {api_name}"

    if not payload["entities"] and not payload["relationships"]:
        return None, f"{api_name} yielded no line_section entities"
    return payload, None


def normalize_year_progress(raw_event: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    if raw_event.get("dataset_key") != "year_progress":
        return None, "not year_progress dataset"
    raw = _raw(raw_event)
    if raw is None:
        return None, "payload.raw must be an object"
    if _api_name(raw_event) != "yearly_progress_analysis":
        return None, f"unsupported year_progress api: {_api_name(raw_event)}"
    return extract_yearly_progress_analysis(raw_event, raw), None
