"""
MCP (Model Context Protocol) Tools for Data Collector Hub v1.0

MCP is a tool wrapper layer over existing services, NOT a separate business system.
This module implements the three required tools:
- list_plugins: Query available plugins
- query_data: Query normalized/raw data
- trigger_plugin: Trigger plugin execution

Assumptions:
- Reuses existing storage and scheduler
- No independent state
- HTTP-exposed tool interface for LLM clients
"""

import json
from typing import Any, Dict, List, Optional


class MCPTools:
    """
    MCP Tool implementations.

    Design principles:
    - Call existing services directly
    - No duplicate business logic
    - Clear parameter schemas for LLM consumption
    """

    def __init__(self, store, plugin_manager, scheduler):
        """
        Initialize MCP tools with existing service instances.

        Args:
            store: SQLiteStore instance
            plugin_manager: PluginManager instance
            scheduler: TaskScheduler instance
        """
        self.store = store
        self.plugin_manager = plugin_manager
        self.scheduler = scheduler

    def list_plugins(self, enabled_only: bool = False, tag: Optional[str] = None) -> Dict[str, Any]:
        """
        List all registered plugins with optional filtering.

        Args:
            enabled_only: If True, return only enabled plugins
            tag: Optional tag filter

        Returns:
            {
                "plugins": [
                    {
                        "id": str,
                        "name": str,
                        "version": str,
                        "description": str,
                        "author": str,
                        "tags": List[str],
                        "enabled": bool,
                        "health_status": str
                    }
                ]
            }
        """
        plugins = self.store.list_plugins()

        result = []
        for plugin in plugins:
            # Filter by enabled status
            if enabled_only and not plugin.get("enabled", 1):
                continue

            # Filter by tag
            if tag:
                plugin_tags = plugin.get("tags", [])
                if tag not in plugin_tags:
                    continue

            result.append({
                "id": plugin["id"],
                "name": plugin["name"],
                "version": plugin["version"],
                "description": plugin.get("description", ""),
                "author": plugin.get("author", ""),
                "tags": plugin.get("tags", []),
                "enabled": bool(plugin.get("enabled", 1)),
                "health_status": plugin.get("health_status", "unknown")
            })

        return {
            "success": True,
            "count": len(result),
            "plugins": result
        }

    def query_data(
        self,
        data_type: str = "normalized",
        plugin_id: Optional[str] = None,
        event_type: Optional[str] = None,
        limit: int = 20,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        Query collected data (raw or normalized).

        Args:
            data_type: "raw" or "normalized"
            plugin_id: Optional plugin filter
            event_type: Optional event type filter (normalized only)
            limit: Maximum items to return (1-100)
            offset: Pagination offset

        Returns:
            {
                "success": True,
                "total": int,
                "limit": int,
                "offset": int,
                "data": [...]
            }
        """
        # Validate parameters
        limit = max(1, min(limit, 100))
        offset = max(0, offset)

        conn = self.store._get_connection()

        try:
            if data_type == "raw":
                return self._query_raw_data(conn, plugin_id, limit, offset)
            else:
                return self._query_normalized_data(conn, plugin_id, event_type, limit, offset)
        finally:
            conn.close()

    def _query_raw_data(
        self,
        conn,
        plugin_id: Optional[str],
        limit: int,
        offset: int
    ) -> Dict[str, Any]:
        """Query raw data (internal)."""
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

        return {
            "success": True,
            "data_type": "raw",
            "total": total,
            "limit": limit,
            "offset": offset,
            "data": items
        }

    def _query_normalized_data(
        self,
        conn,
        plugin_id: Optional[str],
        event_type: Optional[str],
        limit: int,
        offset: int
    ) -> Dict[str, Any]:
        """Query normalized data (internal)."""
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

        return {
            "success": True,
            "data_type": "normalized",
            "total": total,
            "limit": limit,
            "offset": offset,
            "data": items
        }

    async def trigger_plugin(self, plugin_id: str, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Trigger a plugin execution.

        Args:
            plugin_id: Plugin identifier
            config: Optional configuration override

        Returns:
            {
                "success": bool,
                "plugin_id": str,
                "collected": int,
                "message": str
            }
        """
        # Check plugin exists
        metadata = self.plugin_manager.get_plugin_metadata(plugin_id)
        if not metadata:
            return {
                "success": False,
                "plugin_id": plugin_id,
                "error": f"Plugin not found: {plugin_id}"
            }

        # Check plugin is enabled (query from store)
        plugin_info = self.store.get_plugin(plugin_id)
        if plugin_info and not plugin_info.get("enabled", 1):
            return {
                "success": False,
                "plugin_id": plugin_id,
                "error": f"Plugin is disabled: {plugin_id}"
            }

        # Trigger execution via scheduler (reuses pipeline)
        config = config or {}
        result = await self.scheduler.trigger_plugin(plugin_id, config)

        if result.get("success"):
            return {
                "success": True,
                "plugin_id": plugin_id,
                "collected": result.get("items_fetched", 0),
                "message": f"Plugin executed successfully, collected {result.get('items_fetched', 0)} items"
            }
        else:
            return {
                "success": False,
                "plugin_id": plugin_id,
                "error": result.get("error", "Execution failed")
            }


# Tool schema definitions for discovery
TOOL_SCHEMAS = {
    "list_plugins": {
        "name": "list_plugins",
        "description": "List all registered data collection plugins with optional filtering",
        "parameters": {
            "type": "object",
            "properties": {
                "enabled_only": {
                    "type": "boolean",
                    "description": "If true, return only enabled plugins",
                    "default": False
                },
                "tag": {
                    "type": "string",
                    "description": "Filter plugins by tag (e.g., 'news', 'social')",
                    "default": None
                }
            }
        }
    },
    "query_data": {
        "name": "query_data",
        "description": "Query collected data (raw or normalized) with filtering and pagination",
        "parameters": {
            "type": "object",
            "properties": {
                "data_type": {
                    "type": "string",
                    "enum": ["raw", "normalized"],
                    "description": "Type of data to query",
                    "default": "normalized"
                },
                "plugin_id": {
                    "type": "string",
                    "description": "Filter by plugin ID",
                    "default": None
                },
                "event_type": {
                    "type": "string",
                    "description": "Filter by event type (normalized data only)",
                    "default": None
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum items to return (1-100)",
                    "default": 20,
                    "minimum": 1,
                    "maximum": 100
                },
                "offset": {
                    "type": "integer",
                    "description": "Pagination offset",
                    "default": 0,
                    "minimum": 0
                }
            }
        }
    },
    "trigger_plugin": {
        "name": "trigger_plugin",
        "description": "Manually trigger a plugin to collect data immediately",
        "parameters": {
            "type": "object",
            "properties": {
                "plugin_id": {
                    "type": "string",
                    "description": "Plugin identifier (e.g., 'rss_news')"
                },
                "config": {
                    "type": "object",
                    "description": "Optional configuration override",
                    "default": {}
                }
            },
            "required": ["plugin_id"]
        }
    }
}
