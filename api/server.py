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

from fastapi import FastAPI, HTTPException, Query, Response, WebSocket, WebSocketDisconnect
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
from core.paths import DEFAULT_DB_PATH, PLUGINS_DIR
from core.scheduler import TaskScheduler
from core.websocket_manager import WebSocketBroadcastManager
from core.mcp_tools import MCPTools, TOOL_SCHEMAS
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


class PluginTriggerRequest(BaseModel):
    """Plugin trigger request model."""
    config: Optional[Dict[str, Any]] = Field(default_factory=dict)


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


# MCP Models
class MCPToolCallRequest(BaseModel):
    """MCP tool call request model."""
    tool: str = Field(..., description="Tool name to call")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Tool parameters")


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
        default_timeout=30
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
    lifespan=lifespan
)


# API Routes

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
        result.append({
            "id": plugin["id"],
            "name": plugin["name"],
            "version": plugin["version"],
            "description": plugin.get("description", ""),
            "author": plugin.get("author", ""),
            "tags": plugin.get("tags", []),
            "enabled": bool(plugin.get("enabled", 1)),
            "health_status": plugin.get("health_status", "unknown"),
            "collection_mode": plugin.get("collection_mode", "full")
        })

    return {"plugins": result}


@app.post("/api/plugins/{plugin_id}/trigger")
async def trigger_plugin(plugin_id: str, request: Optional[PluginTriggerRequest] = None):
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

    # Trigger execution via scheduler (reuses pipeline)
    config = request.config if request else {}
    result = await scheduler.trigger_plugin(plugin_id, config)

    # Return format matching v1.0 API spec
    if result.get("success"):
        return {
            "success": True,
            "plugin_id": plugin_id,
            "collected": result.get("items_fetched", 0),
            "saved_ids": []  # v1.0 spec field (not implemented in MVP)
        }
    else:
        return {
            "success": False,
            "plugin_id": plugin_id,
            "error": result.get("error", "Execution failed")
        }


@app.get("/api/data")
async def query_raw_data(
    plugin_id: Optional[str] = Query(None, description="Filter by plugin ID"),
    limit: int = Query(20, ge=1, le=100, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination")
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
        items.append({
            "id": row["id"],
            "plugin_id": row["plugin_id"],
            "source": row["source"],
            "data": data,
            "created_at": row["created_at"]
        })

    conn.close()

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "data": items  # Match v1.0 API spec: use "data" not "items"
    }


@app.get("/api/data/normalized")
async def query_normalized_data(
    plugin_id: Optional[str] = Query(None, description="Filter by plugin ID"),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    limit: int = Query(20, ge=1, le=100, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination")
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

        items.append({
            "id": row["id"],
            "plugin_id": row["plugin_id"],
            "event_type": row["event_type"],
            "event_source": row["event_source"],
            "entity": entity,
            "event_timestamp": row["event_timestamp"],
            "unique_key": row["unique_key"],
            "payload": payload,
            "confidence": row["confidence"],
            "created_at": row["created_at"]
        })

    conn.close()

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "data": items  # Match v1.0 API spec: use "data" not "items"
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
        "task_stats": task_stats
    }


@app.get("/feed/rss", response_class=PlainTextResponse)
async def get_rss_feed(
    tag: Optional[str] = Query(None, description="Filter by plugin tag"),
    limit: int = Query(50, ge=1, le=200, description="Number of items to return")
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
                "SELECT plugin_id FROM plugin_tags WHERE tag = ?",
                (tag,)
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
                desc_text = f"Event type: {row['event_type']}, Source: {row['event_source']}"
            item_desc.text = desc_text

            # Pub date
            item_pub = SubElement(item, "pubDate")
            if row["event_timestamp"]:
                try:
                    # Try to parse and format
                    if isinstance(row["event_timestamp"], str):
                        item_pub.text = row["event_timestamp"]
                    else:
                        item_pub.text = row["event_timestamp"].strftime("%a, %d %b %Y %H:%M:%S GMT")
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
            content=xml_string,
            media_type="application/rss+xml; charset=utf-8"
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
        await websocket.send_json({
            "type": "connected",
            "client_id": client_id,
            "message": "Connected to Data Collector Hub stream"
        })

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
        "tools": list(TOOL_SCHEMAS.values())
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
                "available_tools": list(TOOL_SCHEMAS.keys())
            }
        )

    try:
        # Route to appropriate tool
        if tool_name == "list_plugins":
            result = mcp_tools.list_plugins(
                enabled_only=params.get("enabled_only", False),
                tag=params.get("tag")
            )

        elif tool_name == "query_data":
            result = mcp_tools.query_data(
                data_type=params.get("data_type", "normalized"),
                plugin_id=params.get("plugin_id"),
                event_type=params.get("event_type"),
                limit=params.get("limit", 20),
                offset=params.get("offset", 0)
            )

        elif tool_name == "trigger_plugin":
            if "plugin_id" not in params:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "success": False,
                        "error": "Missing required parameter: plugin_id"
                    }
                )
            result = await mcp_tools.trigger_plugin(
                plugin_id=params["plugin_id"],
                config=params.get("config", {})
            )

        else:
            raise HTTPException(
                status_code=400,
                detail={"success": False, "error": f"Tool not implemented: {tool_name}"}
            )

        return {
            "success": True,
            "tool": tool_name,
            "result": result
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "tool": tool_name,
                "error": str(e)
            }
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
            "/feed/rss",
            "/ws/stream",
            "/ws/stats",
            "/mcp",
            "/mcp/call"
        ]
    }


# For direct execution
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
