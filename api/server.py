"""
REST API Server for Data Collector Hub v1.0

Minimal API implementation:
- GET /api/plugins - List all plugins
- POST /api/plugins/{plugin_id}/trigger - Trigger plugin execution
- GET /api/data - Query raw data
- GET /api/data/normalized - Query normalized data

Assumptions:
- Reuses existing core and storage modules
- No authentication (per v1.0 spec)
- FastAPI + Pydantic for type safety
- Field names match v1.0 documentation
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import (
    BackgroundTasks,
    Body,
    FastAPI,
    HTTPException,
    Query,
    Response,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from contextlib import asynccontextmanager
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom
import uuid

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.plugin_manager import PluginManager
from core.plugin_config_validator import validate_plugin_runtime_config
from core.dataset_resolver import resolve_dataset_key, source_ref_matches_dataset
from core.paths import DEFAULT_DB_PATH, PLUGINS_DIR
from core.scheduler import TaskScheduler
from core.websocket_manager import WebSocketBroadcastManager
from core.mcp_tools import MCPTools, TOOL_SCHEMAS
from processing.normalizer_runner import NormalizerRunner, supported_datasets
from storage.sqlite_store import SQLiteStore


# Pydantic Models
class PluginInfo(BaseModel):
    """Plugin information response model."""

    id: str
    name: str
    version: str
    description: str
    author: str
    tags: List[str]
    enabled: bool
    health_status: str
    collection_mode: str = "full"
    plugin_kind: str = "embedded"
    execution_mode: str = "embedded_pipeline"


class PluginTriggerRequest(BaseModel):
    """Plugin trigger request model."""

    config: Optional[Dict[str, Any]] = Field(default_factory=dict)


class PluginRuntimeConfigRequest(BaseModel):
    """Plugin runtime config update request model."""

    config: Dict[str, Any] = Field(default_factory=dict)


class PluginConfigUpdateRequest(BaseModel):
    config: Dict[str, Any]


class PluginTriggerResponse(BaseModel):
    """Plugin trigger response model."""

    success: bool
    plugin_id: str
    items_fetched: int = 0
    raw_saved: int = 0
    normalized_saved: int = 0
    message: str


class RawDataItem(BaseModel):
    """Raw data item response model."""

    id: int
    plugin_id: str
    source: str
    data: Dict[str, Any]
    created_at: str


class NormalizedDataItem(BaseModel):
    """Normalized data item response model."""

    id: int
    plugin_id: str
    event_type: Optional[str]
    event_source: Optional[str]
    entity: List[str]
    event_timestamp: Optional[str]
    unique_key: str
    payload: Dict[str, Any]
    confidence: float
    created_at: str


class DataQueryResponse(BaseModel):
    """Data query response model."""

    total: int
    items: List[Dict[str, Any]]


class IngestionError(BaseModel):
    """Per-event ingestion validation error."""

    index: int
    event_id: Optional[str] = None
    idempotency_key: Optional[str] = None
    error: str


class IngestionResponse(BaseModel):
    """Batch ingestion result."""

    accepted: int
    duplicated: int
    failed: int
    errors: List[IngestionError]


class ProcessingRunRequest(BaseModel):
    """Manual foreground/debug processing run request."""

    dataset_key: str = "station"
    mode: str = "incremental"


class ProcessingJobRequest(BaseModel):
    """Background processing job request."""

    dataset_key: str
    mode: str = "incremental"
    batch_size: int = 1000


# MCP Models
class MCPToolCallRequest(BaseModel):
    """MCP tool call request model."""

    tool: str = Field(..., description="Tool name to call")
    parameters: Dict[str, Any] = Field(
        default_factory=dict, description="Tool parameters"
    )


class MCPToolCallResponse(BaseModel):
    """MCP tool call response model."""

    success: bool
    tool: str
    result: Dict[str, Any]


# Global instances
store: Optional[SQLiteStore] = None
plugin_manager: Optional[PluginManager] = None
scheduler: Optional[TaskScheduler] = None
ws_manager: Optional[WebSocketBroadcastManager] = None
mcp_tools: Optional[MCPTools] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global store, plugin_manager, scheduler, ws_manager, mcp_tools

    # Startup
    print("[API] Starting up...")
    store = SQLiteStore(db_path=DEFAULT_DB_PATH)
    store.init_schema()

    plugin_manager = PluginManager(plugins_dir=PLUGINS_DIR)
    discovered = plugin_manager.discover_plugins()
    registered_count = plugin_manager.save_discovered_plugins(store)
    print(f"[API] Registered {registered_count}/{len(discovered)} discovered plugin(s)")

    scheduler = TaskScheduler(
        store=store,
        plugin_manager=plugin_manager,
        max_concurrency=2,
        default_timeout=30,
    )
    scheduler.start()
    default_job_count = scheduler.register_default_jobs()
    print(f"[API] Registered {default_job_count} default scheduled job(s)")

    # Initialize MCP tools (reuses existing services)
    mcp_tools = MCPTools(store, plugin_manager, scheduler)

    # Start WebSocket broadcast manager
    ws_manager = WebSocketBroadcastManager(db_path=str(DEFAULT_DB_PATH))
    await ws_manager.start()

    print("[API] Server ready")

    yield

    # Shutdown
    print("[API] Shutting down...")
    if ws_manager:
        await ws_manager.stop()
    if scheduler:
        scheduler.stop()
    if store:
        store.close()
    print("[API] Server stopped")


# Create FastAPI app
app = FastAPI(
    title="Data Collector Hub API",
    description="REST API for Data Collector Hub v1.0",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:4173",
        "http://127.0.0.1:4173",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# API Routes

REQUIRED_SOURCE_EVENT_FIELDS = (
    "schema_version",
    "event_id",
    "idempotency_key",
    "source_system",
    "source_event_type",
    "event_granularity",
    "occurred_at",
    "collected_at",
    "payload",
    "source_ref",
)


def _extract_ingestion_events(body: Any) -> list[Any]:
    if isinstance(body, list):
        return body
    if isinstance(body, dict) and isinstance(body.get("events"), list):
        return body["events"]
    raise ValueError(
        "Request body must be a SourceEvent array or an object with an events array."
    )


def _validate_source_event(event: Any) -> list[str]:
    errors: list[str] = []
    if not isinstance(event, dict):
        return ["event must be an object"]

    for field in REQUIRED_SOURCE_EVENT_FIELDS:
        if field not in event or event.get(field) in (None, ""):
            errors.append(f"missing required field: {field}")

    if event.get("schema_version") != "source_event.v1":
        errors.append("schema_version must be source_event.v1")

    if event.get("event_granularity") not in {"envelope", "api_result", "record"}:
        errors.append("event_granularity must be one of: envelope, api_result, record")

    if not event.get("source_record_id") and not event.get("source_record_hash"):
        errors.append("source_record_id or source_record_hash is required")

    if "payload" in event and not isinstance(event.get("payload"), dict):
        errors.append("payload must be an object")

    if "source_ref" in event and not isinstance(event.get("source_ref"), dict):
        errors.append("source_ref must be an object")

    for timestamp_field in ("occurred_at", "collected_at"):
        timestamp_value = event.get(timestamp_field)
        if timestamp_value not in (None, ""):
            try:
                datetime.fromisoformat(str(timestamp_value).replace("Z", "+00:00"))
            except ValueError:
                errors.append(f"{timestamp_field} must be ISO datetime")

    return errors


def _merge_runtime_config(
    current: Dict[str, Any], updates: Dict[str, Any]
) -> Dict[str, Any]:
    merged = dict(current)
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_runtime_config(merged[key], value)
        else:
            merged[key] = value
    return merged


def _resolve_and_validate_ingestion_dataset(event: Dict[str, Any]) -> tuple[Optional[str], list[str]]:
    """Resolve DCP dataset_key and enforce runtime config enablement."""
    if event.get("source_system") != "dcp":
        return None, []

    dcp_plugin = store.get_plugin("dcp")
    if not dcp_plugin:
        return None, ["dataset_key=None: dcp plugin is not registered"]
    if int(dcp_plugin.get("enabled", 0)) == 0:
        return None, ["dataset_key=None: dcp plugin is disabled"]
    if event.get("event_granularity") != "record":
        return None, ["dataset_key=None: DCP event_granularity must be record"]

    payload = event.get("payload")
    if not isinstance(payload, dict) or not isinstance(payload.get("raw"), dict):
        return None, ["dataset_key=None: DCP record event requires payload.raw object"]

    source_ref = event.get("source_ref") if isinstance(event.get("source_ref"), dict) else {}
    required_source_ref_fields = (
        "collection",
        "page_name",
        "api_name",
        "raw_data_index",
        "record_index",
        "record_path",
        "source_file",
    )
    missing_source_ref_fields = [
        field
        for field in required_source_ref_fields
        if field not in source_ref or source_ref.get(field) in (None, "")
    ]
    if missing_source_ref_fields:
        return None, [
            "dataset_key=None: DCP source_ref missing required field(s): "
            + ", ".join(missing_source_ref_fields)
        ]

    if not event.get("source_record_hash"):
        return None, ["dataset_key=None: DCP event requires source_record_hash"]

    try:
        runtime_config = store.get_plugin_runtime_config("dcp")["config"]
    except Exception as exc:
        return None, [f"dataset_key=None: failed to load dcp runtime config: {exc}"]

    dataset_key = resolve_dataset_key(event, runtime_config, allow_fallback=False)
    if not dataset_key:
        return None, ["dataset_key=None: unable to resolve DCP dataset"]

    datasets = runtime_config.get("datasets") or {}
    if dataset_key not in datasets:
        return dataset_key, [f"dataset_key={dataset_key}: not defined in dcp runtime config"]

    matches, mismatch_reason = source_ref_matches_dataset(event, datasets[dataset_key])
    if not matches:
        return dataset_key, [f"dataset_key={dataset_key}: source_ref mismatch: {mismatch_reason}"]

    enabled_datasets = runtime_config.get("enabled_datasets") or []
    if dataset_key not in enabled_datasets:
        return dataset_key, [f"dataset_key={dataset_key}: not listed in enabled_datasets"]

    dataset_config = datasets.get(dataset_key) or {}
    if dataset_config.get("enabled") is not True:
        return dataset_key, [f"dataset_key={dataset_key}: dataset config is disabled"]

    return dataset_key, []


@app.post("/ingestion/v1/events", response_model=IngestionResponse)
async def ingest_source_events(body: Any = Body(...)):
    """
    Ingest SourceEvent v1 events into raw_events.

    This endpoint only validates and stores raw ingestion events. It does not
    normalize, schedule, cache, or serve consumer DTOs.
    """
    try:
        events = _extract_ingestion_events(body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    accepted = 0
    duplicated = 0
    failed = 0
    errors: list[IngestionError] = []

    for index, event in enumerate(events):
        validation_errors = _validate_source_event(event)
        if validation_errors:
            failed += 1
            errors.append(
                IngestionError(
                    index=index,
                    event_id=event.get("event_id") if isinstance(event, dict) else None,
                    idempotency_key=(
                        event.get("idempotency_key")
                        if isinstance(event, dict)
                        else None
                    ),
                    error="; ".join(validation_errors),
                )
            )
            continue

        dataset_key, dataset_errors = _resolve_and_validate_ingestion_dataset(event)
        if dataset_errors:
            failed += 1
            errors.append(
                IngestionError(
                    index=index,
                    event_id=event.get("event_id"),
                    idempotency_key=event.get("idempotency_key"),
                    error="; ".join(dataset_errors),
                )
            )
            continue

        try:
            status, _ = store.save_raw_event(event, dataset_key=dataset_key)
        except Exception as exc:
            failed += 1
            errors.append(
                IngestionError(
                    index=index,
                    event_id=event.get("event_id"),
                    idempotency_key=event.get("idempotency_key"),
                    error=str(exc),
                )
            )
            continue

        if status == "accepted":
            accepted += 1
        elif status == "duplicated":
            duplicated += 1
        else:
            failed += 1
            errors.append(
                IngestionError(
                    index=index,
                    event_id=event.get("event_id"),
                    idempotency_key=event.get("idempotency_key"),
                    error=f"unknown storage status: {status}",
                )
            )

    return IngestionResponse(
        accepted=accepted,
        duplicated=duplicated,
        failed=failed,
        errors=errors,
    )


@app.post("/processing/v1/run")
async def run_processing(request: ProcessingRunRequest):
    """Run a foreground/debug normalizer pass for a supported dataset."""
    supported = supported_datasets()
    if request.dataset_key not in supported:
        raise HTTPException(
            status_code=400,
            detail={
                "error": f"unsupported dataset_key: {request.dataset_key}",
                "supported_datasets": supported,
            },
        )
    return NormalizerRunner(store).run(dataset_key=request.dataset_key, mode=request.mode)


def _run_processing_job(
    *,
    job_id: str,
    dataset_key: str,
    mode: str,
    batch_size: int,
) -> None:
    """Run a processing job using a fresh SQLiteStore connection."""
    job_store = SQLiteStore(db_path=DEFAULT_DB_PATH)
    try:
        job_store.init_schema()
        job_store.mark_processing_job_running(job_id)
        result = NormalizerRunner(job_store).run(
            dataset_key=dataset_key,
            mode=mode,
            batch_size=batch_size,
        )
        job_store.mark_processing_job_succeeded(job_id, result)
    except Exception as exc:
        try:
            job_store.mark_processing_job_failed(job_id, str(exc))
        except Exception:
            pass
    finally:
        job_store.close()


@app.post("/processing/v1/jobs", status_code=202)
async def create_processing_job(
    request: ProcessingJobRequest,
    background_tasks: BackgroundTasks,
):
    """Queue a background normalizer job for a supported dataset."""
    supported = supported_datasets()
    if request.dataset_key not in supported:
        raise HTTPException(
            status_code=400,
            detail={
                "error": f"unsupported dataset_key: {request.dataset_key}",
                "supported_datasets": supported,
            },
        )
    if request.mode not in {"incremental", "full"}:
        raise HTTPException(
            status_code=400,
            detail={"error": f"unsupported processing mode: {request.mode}"},
        )
    if request.batch_size <= 0:
        raise HTTPException(
            status_code=400,
            detail={"error": "batch_size must be greater than 0"},
        )

    active_job = store.get_active_processing_job(request.dataset_key)
    if active_job:
        raise HTTPException(
            status_code=409,
            detail={
                "error": f"processing job already active for dataset_key: {request.dataset_key}",
                "job": active_job,
            },
        )

    job_id = f"proc_{uuid.uuid4().hex}"
    job = store.create_processing_job(
        job_id=job_id,
        dataset_key=request.dataset_key,
        mode=request.mode,
        batch_size=request.batch_size,
    )
    background_tasks.add_task(
        _run_processing_job,
        job_id=job_id,
        dataset_key=request.dataset_key,
        mode=request.mode,
        batch_size=request.batch_size,
    )
    return job


@app.get("/processing/v1/jobs/{job_id}")
async def get_processing_job(job_id: str):
    """Get processing job status."""
    job = store.get_processing_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"processing job not found: {job_id}")
    return job


@app.post("/processing/v1/run-monitor")
async def run_monitor_processing():
    """Run supported normalizers for DCP monitor datasets."""
    try:
        runtime_config = store.get_plugin_runtime_config("dcp")["config"]
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"failed to load dcp runtime config: {exc}")

    monitor_datasets = runtime_config.get("monitor_datasets") or []
    if not isinstance(monitor_datasets, list):
        raise HTTPException(status_code=400, detail="dcp monitor_datasets must be a list")

    supported = supported_datasets()
    supported_set = set(supported)
    runner = NormalizerRunner(store)
    results: dict[str, Any] = {}
    processed = 0
    skipped = 0

    for dataset_key in monitor_datasets:
        dataset_key = str(dataset_key)
        if dataset_key not in supported_set:
            skipped += 1
            results[dataset_key] = {
                "status": "skipped",
                "reason": "unsupported",
            }
            continue

        processed += 1
        results[dataset_key] = {
            "status": "processed",
            "result": runner.run(dataset_key=dataset_key),
        }

    return {
        "plugin_id": "dcp",
        "monitor_datasets": [str(dataset_key) for dataset_key in monitor_datasets],
        "supported_datasets": supported,
        "results": results,
        "summary": {
            "processed": processed,
            "skipped": skipped,
        },
    }


@app.get("/api/v1/sandbox/dates")
async def get_sandbox_dates():
    """Return available sandbox work point dates for Monitor timeline mode."""
    dates = store.list_canonical_entity_dates("work_point")
    return {
        "dates": dates,
        "latest_date": dates[-1] if dates else None,
        "count": len(dates),
    }


@app.get("/api/v1/sandbox/map/skeleton")
async def get_sandbox_map_skeleton(
    limit: int = Query(10000, ge=1, le=50000, description="Maximum stations/towers to return")
):
    """Return a minimal sandbox map skeleton from canonical current entities."""
    stations = []
    station_entities = store.list_canonical_entities(entity_type="station", limit=limit + 1)
    tower_entities = store.list_canonical_entities(entity_type="tower", limit=limit + 1)
    truncated = len(station_entities) > limit or len(tower_entities) > limit
    for entity in station_entities[:limit]:
        attributes = entity.get("attributes") or {}
        stations.append(
            {
                "id": entity["entity_key"],
                "project_code": attributes.get("project_code"),
                "single_project_code": attributes.get("single_project_code"),
                "longitude": attributes.get("longitude"),
                "latitude": attributes.get("latitude"),
            }
        )
    towers = []
    for entity in tower_entities[:limit]:
        attributes = entity.get("attributes") or {}
        towers.append(
            {
                "id": entity["entity_key"],
                "tower_id": attributes.get("tower_id"),
                "single_project_code": attributes.get("single_project_code"),
                "bidding_section_code": attributes.get("bidding_section_code"),
                "tower_no": attributes.get("tower_no"),
                "upstream_tower_no": attributes.get("upstream_tower_no"),
                "longitude": attributes.get("longitude"),
                "latitude": attributes.get("latitude"),
                "tower_type": attributes.get("tower_type"),
                "tower_full_height": attributes.get("tower_full_height"),
                "nominal_height": attributes.get("nominal_height"),
            }
        )
    return {
        "meta": {
            "limit": limit,
            "stations_count": len(stations),
            "towers_count": len(towers),
            "truncated": truncated,
        },
        "stations": stations,
        "towers": towers,
        "lines": [],
    }


@app.get("/api/v1/sandbox/map/summary")
async def get_sandbox_map_summary(
    date: Optional[str] = Query(None, description="Work point date in YYYY-MM-DD format"),
    limit: int = Query(10000, ge=1, le=50000, description="Maximum work points to return")
):
    """Return sandbox work point summary from canonical current entities."""
    selected_date = date or store.get_latest_canonical_entity_date("work_point")
    entities = (
        store.list_canonical_entities(
            entity_type="work_point",
            entity_date=selected_date,
            limit=limit + 1,
        )
        if selected_date
        else []
    )
    truncated = len(entities) > limit
    work_points = []
    for entity in entities[:limit]:
        attributes = entity.get("attributes") or {}
        work_points.append(
            {
                "id": entity["entity_key"],
                "project_name": attributes.get("project_name"),
                "longitude": attributes.get("longitude"),
                "latitude": attributes.get("latitude"),
                "person_count": attributes.get("person_count"),
                "risk_level": attributes.get("risk_level"),
                "work_status": attributes.get("work_status"),
                "voltage_level": attributes.get("voltage_level"),
                "city": attributes.get("city"),
                "work_date": attributes.get("work_date"),
            }
        )
    return {
        "meta": {
            "date": selected_date,
            "limit": limit,
            "work_points_count": len(work_points),
            "truncated": truncated,
        },
        "work_points": work_points,
    }


@app.get("/api/plugins")
async def list_plugins():
    """
    List all registered plugins.

    Returns:
        {"plugins": [...]} matching v1.0 API spec
    """
    plugins = store.list_plugins()

    result = []
    for plugin in plugins:
        result.append(
            {
                "id": plugin["id"],
                "name": plugin["name"],
                "version": plugin["version"],
                "description": plugin.get("description", ""),
                "author": plugin.get("author", ""),
                "tags": plugin.get("tags", []),
                "enabled": bool(plugin.get("enabled", 1)),
                "health_status": plugin.get("health_status", "unknown"),
                "collection_mode": plugin.get("collection_mode", "full"),
                "plugin_kind": plugin.get("plugin_kind", "embedded"),
                "execution_mode": plugin.get("execution_mode", "embedded_pipeline"),
            }
        )

    return {"plugins": result}


@app.get("/api/plugins/{plugin_id}/config")
async def get_plugin_config(plugin_id: str):
    plugin = store.get_plugin(plugin_id)
    if not plugin:
        raise HTTPException(status_code=404, detail=f"plugin not found: {plugin_id}")

    runtime = store.get_plugin_runtime_config(plugin_id)

    return {
        "plugin_id": plugin_id,
        "config": runtime["config"],
        "config_schema": plugin.get("config") or {},
        "source": runtime["source"],
        "updated_at": runtime["updated_at"],
    }


@app.put("/api/plugins/{plugin_id}/config")
async def update_plugin_config(plugin_id: str, request: PluginConfigUpdateRequest):
    plugin = store.get_plugin(plugin_id)
    if not plugin:
        raise HTTPException(status_code=404, detail=f"plugin not found: {plugin_id}")

    runtime = store.get_plugin_runtime_config(plugin_id)
    config = _merge_runtime_config(runtime["config"], request.config)
    errors = validate_plugin_runtime_config(plugin_id, config)
    if errors:
        raise HTTPException(status_code=400, detail=errors)

    store.save_plugin_runtime_config(plugin_id, config)

    return {
        "plugin_id": plugin_id,
        "saved": True,
        "config": config,
    }


@app.post("/api/plugins/{plugin_id}/trigger")
async def trigger_plugin(
    plugin_id: str, request: Optional[PluginTriggerRequest] = None
):
    """
    Manually trigger a plugin execution.

    Args:
        plugin_id: Plugin identifier
        request: Optional configuration override

    Returns:
        Execution result matching v1.0 API spec
    """
    # Check plugin exists
    metadata = plugin_manager.get_plugin_metadata(plugin_id)
    if not metadata:
        raise HTTPException(status_code=404, detail=f"Plugin not found: {plugin_id}")
    if metadata.plugin_kind == "external" or metadata.execution_mode == "external_job":
        raise HTTPException(
            status_code=409,
            detail=f"Plugin {plugin_id} is external and cannot be triggered by the embedded scheduler",
        )

    # Trigger execution via scheduler (reuses pipeline)
    config = request.config if request else {}
    result = await scheduler.trigger_plugin(plugin_id, config)

    # Return format matching v1.0 API spec
    if result.get("success"):
        return {
            "success": True,
            "plugin_id": plugin_id,
            "collected": result.get("items_fetched", 0),
            "saved_ids": [],  # v1.0 spec field (not implemented in MVP)
        }
    else:
        return {
            "success": False,
            "plugin_id": plugin_id,
            "error": result.get("error", "Execution failed"),
        }


@app.get("/api/data")
async def query_raw_data(
    plugin_id: Optional[str] = Query(None, description="Filter by plugin ID"),
    limit: int = Query(20, ge=1, le=100, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
):
    """
    Query raw data.

    Args:
        plugin_id: Optional plugin filter
        limit: Maximum items to return
        offset: Pagination offset

    Returns:
        Raw data items
    """
    conn = store._get_connection()

    # Build query
    where_clause = "WHERE 1=1"
    params = []

    if plugin_id:
        where_clause += " AND plugin_id = ?"
        params.append(plugin_id)

    # Get total count
    count_query = f"SELECT COUNT(*) as count FROM raw_data {where_clause}"
    cursor = conn.execute(count_query, params)
    total = cursor.fetchone()["count"]

    # Get items
    query = f"""
        SELECT id, plugin_id, source, data, created_at
        FROM raw_data
        {where_clause}
        ORDER BY id DESC
        LIMIT ? OFFSET ?
    """
    cursor = conn.execute(query, params + [limit, offset])

    items = []
    for row in cursor.fetchall():
        data = json.loads(row["data"]) if row["data"] else {}
        items.append(
            {
                "id": row["id"],
                "plugin_id": row["plugin_id"],
                "source": row["source"],
                "data": data,
                "created_at": row["created_at"],
            }
        )

    conn.close()

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "data": items,  # Match v1.0 API spec: use "data" not "items"
    }


@app.get("/api/data/normalized")
async def query_normalized_data(
    plugin_id: Optional[str] = Query(None, description="Filter by plugin ID"),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    limit: int = Query(20, ge=1, le=100, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
):
    """
    Query normalized data.

    Args:
        plugin_id: Optional plugin filter
        event_type: Optional event type filter
        limit: Maximum items to return
        offset: Pagination offset

    Returns:
        Normalized data items
    """
    conn = store._get_connection()

    # Build query
    where_clause = "WHERE 1=1"
    params = []

    if plugin_id:
        where_clause += " AND plugin_id = ?"
        params.append(plugin_id)

    if event_type:
        where_clause += " AND event_type = ?"
        params.append(event_type)

    # Get total count
    count_query = f"SELECT COUNT(*) as count FROM normalized_data {where_clause}"
    cursor = conn.execute(count_query, params)
    total = cursor.fetchone()["count"]

    # Get items
    query = f"""
        SELECT id, plugin_id, event_type, event_source, entity,
               event_timestamp, unique_key, payload, confidence, created_at
        FROM normalized_data
        {where_clause}
        ORDER BY id DESC
        LIMIT ? OFFSET ?
    """
    cursor = conn.execute(query, params + [limit, offset])

    items = []
    for row in cursor.fetchall():
        entity = json.loads(row["entity"]) if row["entity"] else []
        payload = json.loads(row["payload"]) if row["payload"] else {}

        items.append(
            {
                "id": row["id"],
                "plugin_id": row["plugin_id"],
                "event_type": row["event_type"],
                "event_source": row["event_source"],
                "entity": entity,
                "event_timestamp": row["event_timestamp"],
                "unique_key": row["unique_key"],
                "payload": payload,
                "confidence": row["confidence"],
                "created_at": row["created_at"],
            }
        )

    conn.close()

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "data": items,  # Match v1.0 API spec: use "data" not "items"
    }


@app.get("/api/stats")
async def get_stats():
    """
    Get system statistics.

    Returns:
        System statistics
    """
    conn = store._get_connection()

    # Count plugins
    cursor = conn.execute("SELECT COUNT(*) as count FROM plugins")
    plugin_count = cursor.fetchone()["count"]

    # Count raw data
    cursor = conn.execute("SELECT COUNT(*) as count FROM raw_data")
    raw_count = cursor.fetchone()["count"]

    # Count normalized data
    cursor = conn.execute("SELECT COUNT(*) as count FROM normalized_data")
    norm_count = cursor.fetchone()["count"]

    # Get task stats
    cursor = conn.execute("""
        SELECT plugin_id, run_count, fail_count, last_run, consecutive_fails
        FROM task_stats
        ORDER BY last_run DESC
    """)
    task_stats = [dict(row) for row in cursor.fetchall()]

    conn.close()

    return {
        "plugins": plugin_count,
        "raw_data": raw_count,
        "normalized_data": norm_count,
        "task_stats": task_stats,
    }


@app.get("/feed/rss", response_class=PlainTextResponse)
async def get_rss_feed(
    tag: Optional[str] = Query(None, description="Filter by plugin tag"),
    limit: int = Query(50, ge=1, le=200, description="Number of items to return"),
):
    """
    RSS Feed endpoint.

    Returns normalized data as RSS 2.0 XML feed.

    Args:
        tag: Filter by plugin tag (optional)
        limit: Maximum number of items (default 50, max 200)

    Returns:
        RSS 2.0 XML (application/rss+xml)
    """
    conn = store._get_connection()

    try:
        # Build query for normalized data
        # If tag is specified, filter by plugins with that tag
        if tag:
            # Get plugins with the specified tag
            cursor = conn.execute(
                "SELECT plugin_id FROM plugin_tags WHERE tag = ?", (tag,)
            )
            plugin_ids = [row["plugin_id"] for row in cursor.fetchall()]

            if not plugin_ids:
                # No plugins with this tag, return empty feed
                items = []
            else:
                # Query normalized data for these plugins
                placeholders = ",".join(["?"] * len(plugin_ids))
                query = f"""
                    SELECT id, plugin_id, event_type, event_source, entity,
                           event_timestamp, unique_key, payload, confidence, created_at
                    FROM normalized_data
                    WHERE plugin_id IN ({placeholders})
                    ORDER BY created_at DESC
                    LIMIT ?
                """
                cursor = conn.execute(query, plugin_ids + [limit])
                items = cursor.fetchall()
        else:
            # No tag filter, get all recent data
            query = """
                SELECT id, plugin_id, event_type, event_source, entity,
                       event_timestamp, unique_key, payload, confidence, created_at
                FROM normalized_data
                ORDER BY created_at DESC
                LIMIT ?
            """
            cursor = conn.execute(query, (limit,))
            items = cursor.fetchall()

        # Build RSS XML
        rss = Element("rss", version="2.0")
        channel = SubElement(rss, "channel")

        # Channel metadata
        title = SubElement(channel, "title")
        title.text = "Data Collector Hub Feed"

        link = SubElement(channel, "link")
        link.text = "http://localhost:8000"

        description = SubElement(channel, "description")
        description.text = "Real-time data collection feed"

        language = SubElement(channel, "language")
        language.text = "zh-CN"

        last_build = SubElement(channel, "lastBuildDate")
        last_build.text = datetime.now().strftime("%a, %d %b %Y %H:%M:%S GMT")

        # Add items
        for row in items:
            payload = json.loads(row["payload"]) if row["payload"] else {}

            item = SubElement(channel, "item")

            # Title from payload or default
            item_title = SubElement(item, "title")
            title_text = payload.get("title", "")
            if not title_text:
                title_text = f"{row['event_source'] or row['plugin_id']} - {row['event_type'] or 'data'}"
            item_title.text = title_text[:200]  # Limit length

            # Link (point to API endpoint)
            item_link = SubElement(item, "link")
            item_link.text = f"http://localhost:8000/api/data/normalized?id={row['id']}"

            # Description from payload summary or entity
            item_desc = SubElement(item, "description")
            desc_text = payload.get("summary", "")
            if not desc_text and payload.get("content"):
                desc_text = payload.get("content", "")[:500]
            if not desc_text:
                desc_text = (
                    f"Event type: {row['event_type']}, Source: {row['event_source']}"
                )
            item_desc.text = desc_text

            # Pub date
            item_pub = SubElement(item, "pubDate")
            if row["event_timestamp"]:
                try:
                    # Try to parse and format
                    if isinstance(row["event_timestamp"], str):
                        item_pub.text = row["event_timestamp"]
                    else:
                        item_pub.text = row["event_timestamp"].strftime(
                            "%a, %d %b %Y %H:%M:%S GMT"
                        )
                except:
                    item_pub.text = row["created_at"]
            else:
                item_pub.text = row["created_at"]

            # GUID (unique identifier)
            item_guid = SubElement(item, "guid")
            item_guid.text = row["unique_key"]
            item_guid.set("isPermaLink", "false")

            # Category (event type)
            if row["event_type"]:
                item_cat = SubElement(item, "category")
                item_cat.text = row["event_type"]

        # Convert to string
        rough_string = tostring(rss, encoding="unicode")
        reparsed = minidom.parseString(rough_string.encode("utf-8"))
        xml_string = reparsed.toprettyxml(indent="  ", encoding="utf-8").decode("utf-8")

        return PlainTextResponse(
            content=xml_string, media_type="application/rss+xml; charset=utf-8"
        )

    finally:
        conn.close()


@app.websocket("/ws/stream")
async def websocket_stream(websocket: WebSocket):
    """
    WebSocket endpoint for real-time data streaming.

    Single-poll broadcast architecture:
    - One background task polls database
    - Multiple clients receive filtered broadcasts
    - Query frequency does NOT scale with client count

    Client messages:
    - {"action": "set_filters", "filters": {"plugins": ["rss_news"], "interval": 5}}

    Server messages:
    - {"type": "data", "timestamp": "...", "count": N, "items": [...]}
    - {"type": "ack", "message": "..."}
    """
    client_id = str(uuid.uuid4())[:8]

    # Register client
    client = await ws_manager.connect(websocket, client_id)

    try:
        # Send welcome message
        await websocket.send_json(
            {
                "type": "connected",
                "client_id": client_id,
                "message": "Connected to Data Collector Hub stream",
            }
        )

        # Handle incoming messages
        while True:
            try:
                message = await websocket.receive_json()
                await ws_manager.handle_client_message(client_id, message)
            except Exception as e:
                print(f"[WebSocket] Client {client_id} message error: {e}")
                break

    except WebSocketDisconnect:
        print(f"[WebSocket] Client {client_id} disconnected")
    except Exception as e:
        print(f"[WebSocket] Client {client_id} error: {e}")
    finally:
        await ws_manager.disconnect(client_id)


@app.get("/ws/stats")
async def websocket_stats():
    """
    Get WebSocket connection statistics.

    Returns:
        WebSocket manager statistics
    """
    return ws_manager.get_stats()


# MCP Endpoints


@app.get("/mcp")
async def mcp_discovery():
    """
    MCP Tool Discovery endpoint.

    Returns available tools and their schemas for LLM clients.
    This is a minimal HTTP-exposed tool interface, not a full MCP protocol implementation.

    Returns:
        {
            "tools": [...],
            "version": "1.0.0"
        }
    """
    return {
        "version": "1.0.0",
        "description": "Data Collector Hub MCP Tool Interface",
        "tools": list(TOOL_SCHEMAS.values()),
    }


@app.post("/mcp/call")
async def mcp_call(request: MCPToolCallRequest):
    """
    MCP Tool Call endpoint.

    Execute a tool with the given parameters.
    Supports: list_plugins, query_data, trigger_plugin

    Args:
        request: Tool call request with tool name and parameters

    Returns:
        Tool execution result
    """
    tool_name = request.tool
    params = request.parameters

    # Validate tool exists
    if tool_name not in TOOL_SCHEMAS:
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "error": f"Unknown tool: {tool_name}",
                "available_tools": list(TOOL_SCHEMAS.keys()),
            },
        )

    try:
        # Route to appropriate tool
        if tool_name == "list_plugins":
            result = mcp_tools.list_plugins(
                enabled_only=params.get("enabled_only", False), tag=params.get("tag")
            )

        elif tool_name == "query_data":
            result = mcp_tools.query_data(
                data_type=params.get("data_type", "normalized"),
                plugin_id=params.get("plugin_id"),
                event_type=params.get("event_type"),
                limit=params.get("limit", 20),
                offset=params.get("offset", 0),
            )

        elif tool_name == "trigger_plugin":
            if "plugin_id" not in params:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "success": False,
                        "error": "Missing required parameter: plugin_id",
                    },
                )
            result = await mcp_tools.trigger_plugin(
                plugin_id=params["plugin_id"], config=params.get("config", {})
            )

        else:
            raise HTTPException(
                status_code=400,
                detail={
                    "success": False,
                    "error": f"Tool not implemented: {tool_name}",
                },
            )

        return {"success": True, "tool": tool_name, "result": result}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"success": False, "tool": tool_name, "error": str(e)},
        )


@app.get("/")
async def root():
    """API root endpoint."""
    return {
        "name": "Data Collector Hub API",
        "version": "1.0.0",
        "endpoints": [
            "/api/plugins",
            "/api/plugins/{plugin_id}/trigger",
            "/api/data",
            "/api/data/normalized",
            "/api/stats",
            "/ingestion/v1/events",
            "/feed/rss",
            "/ws/stream",
            "/ws/stats",
            "/mcp",
            "/mcp/call",
        ],
    }


# For direct execution
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
