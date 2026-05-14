"""Dataset resolution helpers for legacy raw-event migration fixtures."""

from typing import Any


def resolve_dataset_key(
    event: dict[str, Any],
    runtime_config: dict[str, Any] | None = None,
    allow_fallback: bool = True,
) -> str | None:
    """Resolve a dataset key without coupling storage to source-specific rules."""
    source_ref = event.get("source_ref") or {}
    explicit_dataset_key = source_ref.get("dataset_key")
    if explicit_dataset_key:
        return explicit_dataset_key

    if event.get("source_system") != "dcp":
        return None

    if runtime_config:
        datasets = runtime_config.get("datasets") or {}
        for dataset_key, dataset_config in datasets.items():
            if not isinstance(dataset_config, dict):
                continue
            if source_ref_matches_dataset(event, dataset_config)[0]:
                return dataset_key

    if not allow_fallback:
        return None

    collection = source_ref.get("collection")
    page_name = source_ref.get("page_name")
    api_name = source_ref.get("api_name")

    if collection == "safePages" and page_name in {"meetingListAdmin", "站班会"}:
        return "daily_meeting"

    if (
        collection == "planPages"
        and (
            page_name == "年度进度计划分析"
            or api_name in {"yearly_progress_analysis", "year_progress"}
        )
    ):
        return "year_progress"

    if collection == "projectPages" and page_name == "杆塔信息":
        return "tower"

    if collection == "projectPages" and page_name == "变电站坐标":
        return "station"

    if collection == "projectPages" and page_name == "区段划分":
        return "line_section"

    if collection == "projectPages" and page_name == "项目前期成果":
        return "project_preconstruction"

    if api_name in {"tower_single_projects", "tower_details"}:
        return "tower"

    if api_name in {"substation_single_projects", "substation_coordinates"}:
        return "station"

    if api_name in {"section_single_projects", "section_details"}:
        return "line_section"

    if api_name == "preconstruction_results_detail":
        return "project_preconstruction"

    return None


def source_ref_matches_dataset(
    event: dict[str, Any], dataset_config: dict[str, Any]
) -> tuple[bool, str]:
    """Check whether event.source_ref matches a configured dataset."""
    source_ref = event.get("source_ref") or {}
    collection = source_ref.get("collection")
    page_name = source_ref.get("page_name")
    api_name = source_ref.get("api_name")

    expected_collection = dataset_config.get("collection")
    if collection != expected_collection:
        return False, f"collection mismatch: got {collection}, expected {expected_collection}"

    expected_page_name = dataset_config.get("page_name")
    page_aliases = dataset_config.get("page_aliases") or []
    allowed_page_names = {expected_page_name, *page_aliases} - {None, ""}
    if allowed_page_names and page_name not in allowed_page_names:
        return False, (
            f"page_name mismatch: got {page_name}, expected one of "
            f"{sorted(allowed_page_names)}"
        )

    expected_api_names = dataset_config.get("api_names") or []
    if expected_api_names and api_name not in expected_api_names:
        return False, (
            f"api_name mismatch: got {api_name}, expected one of "
            f"{sorted(expected_api_names)}"
        )

    return True, ""
