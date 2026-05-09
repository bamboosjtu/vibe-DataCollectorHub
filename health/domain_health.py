from __future__ import annotations

from typing import Any

from storage.sqlite_store import SQLiteStore


ENTITY_TYPES = [
    "project",
    "single_project",
    "bidding_section",
    "line_section",
    "project_progress",
    "tower",
    "station",
    "work_point",
]

RELATIONSHIP_TYPES = [
    "HAS_SINGLE_PROJECT",
    "HAS_BIDDING_SECTION",
    "HAS_LINE_SECTION",
    "HAS_TOWER_SEQUENCE",
    "HAS_PROJECT_PROGRESS",
]


def _is_unscoped_tower_key(entity_key: str) -> bool:
    parts = str(entity_key).split(":")
    return parts[:2] == ["dcp", "tower"] and len(parts) == 3


def get_domain_health(store: SQLiteStore) -> dict[str, Any]:
    entities = store.list_canonical_entities(limit=100000)
    relationships = store.list_canonical_relationships(limit=100000)

    entity_counts = {entity_type: 0 for entity_type in ENTITY_TYPES}
    entity_identities: set[tuple[str, str]] = set()
    unscoped_tower_entity_count = 0
    for entity in entities:
        entity_type = entity["entity_type"]
        if entity_type in entity_counts:
            entity_counts[entity_type] += 1
        entity_identities.add((entity_type, entity["entity_key"]))
        if entity_type == "tower" and _is_unscoped_tower_key(entity["entity_key"]):
            unscoped_tower_entity_count += 1

    relationship_counts = {relationship_type: 0 for relationship_type in RELATIONSHIP_TYPES}
    orphan_relationship_count = 0
    unscoped_tower_sequence_count = 0
    tower_sequence_orphan_count = 0
    tower_sequence_reference_count = 0
    tower_sequence_missing_physical_entity_count = 0
    project_with_single: set[str] = set()
    single_with_bidding: set[str] = set()
    bidding_with_line: set[str] = set()

    for relationship in relationships:
        relationship_type = relationship["relationship_type"]
        if relationship_type in relationship_counts:
            relationship_counts[relationship_type] += 1
        from_identity = (relationship["from_entity_type"], relationship["from_entity_key"])
        to_identity = (relationship["to_entity_type"], relationship["to_entity_key"])
        if (
            from_identity not in entity_identities
            or to_identity not in entity_identities
        ):
            orphan_relationship_count += 1
        if relationship_type == "HAS_TOWER_SEQUENCE":
            if _is_unscoped_tower_key(relationship["to_entity_key"]):
                unscoped_tower_sequence_count += 1
            if to_identity not in entity_identities:
                tower_sequence_orphan_count += 1
                attributes = relationship.get("attributes") or {}
                if attributes.get("node_kind") == "reference_node":
                    tower_sequence_reference_count += 1
                else:
                    tower_sequence_missing_physical_entity_count += 1
        if relationship_type == "HAS_SINGLE_PROJECT":
            project_with_single.add(relationship["from_entity_key"])
        if relationship_type == "HAS_BIDDING_SECTION":
            single_with_bidding.add(relationship["from_entity_key"])
        if relationship_type == "HAS_LINE_SECTION":
            bidding_with_line.add(relationship["from_entity_key"])

    line_section_known_issue_count = 0
    project_without_single_project_count = 0
    single_project_without_bidding_section_count = 0
    bidding_section_without_line_section_count = 0

    for entity in entities:
        entity_type = entity["entity_type"]
        attributes = entity.get("attributes") or {}
        if entity_type == "line_section" and attributes.get("known_issues"):
            line_section_known_issue_count += 1
        if entity_type == "project" and entity["entity_key"] not in project_with_single:
            project_without_single_project_count += 1
        if entity_type == "single_project" and entity["entity_key"] not in single_with_bidding:
            single_project_without_bidding_section_count += 1
        if entity_type == "bidding_section" and entity["entity_key"] not in bidding_with_line:
            bidding_section_without_line_section_count += 1

    critical_relationship_count = sum(
        relationship_counts[relationship_type]
        for relationship_type in (
            "HAS_SINGLE_PROJECT",
            "HAS_BIDDING_SECTION",
            "HAS_TOWER_SEQUENCE",
            "HAS_PROJECT_PROGRESS",
        )
    )

    return {
        "entity_counts": entity_counts,
        "relationship_counts": relationship_counts,
        "unscoped_tower_sequence_count": unscoped_tower_sequence_count,
        "unscoped_tower_entity_count": unscoped_tower_entity_count,
        "tower_sequence_orphan_count": tower_sequence_orphan_count,
        "tower_sequence_reference_count": tower_sequence_reference_count,
        "tower_sequence_missing_physical_entity_count": tower_sequence_missing_physical_entity_count,
        "line_section_known_issue_count": line_section_known_issue_count,
        "orphan_relationship_count": orphan_relationship_count,
        "orphan_relationship_note": "orphan_relationship_count includes tower sequence reference nodes and is not always a data error",
        "project_without_single_project_count": project_without_single_project_count,
        "single_project_without_bidding_section_count": single_project_without_bidding_section_count,
        "bidding_section_without_line_section_count": bidding_section_without_line_section_count,
        "critical_relationship_count": critical_relationship_count,
    }
