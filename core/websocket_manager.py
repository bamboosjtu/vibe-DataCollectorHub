"""
WebSocket Broadcast Manager for Data Collector Hub v1.0

Architecture: Single-poll broadcast (NOT per-client polling)
- One background polling task queries database
- Multiple WebSocket clients receive broadcasts
- Clients can set filters (plugins, interval)
- Database query frequency does NOT scale with client count

Assumptions:
- Uses asyncio for async operations
- Thread-safe client management
- Reuses existing storage query methods
"""

import asyncio
import json
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from fastapi import WebSocket


class WebSocketClient:
    """Represents a connected WebSocket client with filter preferences."""

    def __init__(self, websocket: WebSocket, client_id: str):
        self.websocket = websocket
        self.client_id = client_id
        self.plugins: Optional[List[str]] = None  # Filter by plugin IDs
        self.interval: int = 5  # Polling interval in seconds
        self.last_data_id: int = 0  # Last sent data ID for incremental push
        self.connected: bool = True

    def set_filters(self, plugins: Optional[List[str]] = None, interval: int = 5):
        """Update client filter preferences."""
        self.plugins = plugins
        self.interval = max(1, min(interval, 60))  # Clamp between 1-60 seconds

    def should_receive(self, data: Dict[str, Any]) -> bool:
        """Check if this data item should be sent to this client."""
        if not self.connected:
            return False

        # Filter by plugin
        if self.plugins:
            plugin_id = data.get("plugin_id")
            if plugin_id not in self.plugins:
                return False

        return True


class WebSocketBroadcastManager:
    """
    Manages WebSocket connections with single-poll broadcast architecture.

    Key design:
    - ONE background task polls database
    - ALL connected clients receive filtered broadcasts
    - Query frequency is constant regardless of client count
    """

    def __init__(self, db_path: str = "data/collector.db"):
        self.db_path = db_path
        self.clients: Dict[str, WebSocketClient] = {}
        self._lock = asyncio.Lock()
        self._polling_task: Optional[asyncio.Task] = None
        self._running = False
        self._poll_interval = 5  # Default polling interval (seconds)

        # Statistics for monitoring
        self._stats = {
            "poll_count": 0,           # Total number of polls executed
            "broadcast_count": 0,      # Total number of broadcasts sent
            "last_poll_time": None,    # Last time database was polled
            "last_data_count": 0,      # Number of items in last poll
            "empty_poll_count": 0,     # Number of polls with no new data
        }

    async def start(self):
        """Start the broadcast manager."""
        if self._running:
            return

        self._running = True
        self._polling_task = asyncio.create_task(self._polling_loop())
        print(f"[WebSocketManager] Started with poll_interval={self._poll_interval}s")

    async def stop(self):
        """Stop the broadcast manager."""
        self._running = False

        if self._polling_task:
            self._polling_task.cancel()
            try:
                await self._polling_task
            except asyncio.CancelledError:
                pass

        # Disconnect all clients
        async with self._lock:
            for client in list(self.clients.values()):
                client.connected = False
            self.clients.clear()

        print("[WebSocketManager] Stopped")

    async def connect(self, websocket: WebSocket, client_id: str) -> WebSocketClient:
        """
        Register a new WebSocket client.

        Args:
            websocket: FastAPI WebSocket instance
            client_id: Unique client identifier

        Returns:
            WebSocketClient wrapper
        """
        await websocket.accept()

        client = WebSocketClient(websocket, client_id)

        async with self._lock:
            self.clients[client_id] = client

        print(f"[WebSocketManager] Client connected: {client_id} (total: {len(self.clients)})")
        return client

    async def disconnect(self, client_id: str):
        """Unregister a WebSocket client."""
        async with self._lock:
            if client_id in self.clients:
                self.clients[client_id].connected = False
                del self.clients[client_id]

        print(f"[WebSocketManager] Client disconnected: {client_id} (total: {len(self.clients)})")

    async def _polling_loop(self):
        """
        Background polling loop - SINGLE POLL TASK.

        This is the core of the single-poll broadcast architecture:
        - One task queries database at fixed interval
        - Results are broadcast to all connected clients
        - Query frequency does NOT increase with client count
        """
        print("[WebSocketManager] Polling loop started (single task)")

        last_check_time = datetime.now()

        while self._running:
            try:
                # Calculate effective poll interval (minimum of all client preferences)
                async with self._lock:
                    if self.clients:
                        intervals = [c.interval for c in self.clients.values()]
                        self._poll_interval = min(intervals) if intervals else 5

                # Poll database for new data
                self._stats["poll_count"] += 1
                self._stats["last_poll_time"] = datetime.now().isoformat()

                new_data = await self._query_new_data(last_check_time)
                self._stats["last_data_count"] = len(new_data)

                if new_data:
                    print(f"[WebSocketManager] Poll #{self._stats['poll_count']}: Found {len(new_data)} new items, broadcasting...")
                    await self._broadcast_to_clients(new_data)
                    last_check_time = datetime.now()
                else:
                    self._stats["empty_poll_count"] += 1
                    # No new data - don't broadcast to avoid duplicate pushes
                    if self._stats["poll_count"] % 10 == 0:  # Log every 10 empty polls
                        print(f"[WebSocketManager] Poll #{self._stats['poll_count']}: No new data ({self._stats['empty_poll_count']} empty polls)")

                # Wait before next poll
                await asyncio.sleep(self._poll_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[WebSocketManager] Polling error: {e}")
                await asyncio.sleep(self._poll_interval)

        print("[WebSocketManager] Polling loop stopped")

    async def _query_new_data(self, since: datetime) -> List[Dict[str, Any]]:
        """
        Query database for new normalized data since last check.

        Args:
            since: Last check timestamp

        Returns:
            List of new data items
        """
        # Run database query in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._sync_query_data, since)

    def _sync_query_data(self, since: datetime) -> List[Dict[str, Any]]:
        """Synchronous database query (runs in thread pool)."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row

            cursor = conn.execute(
                """
                SELECT id, plugin_id, event_type, event_source, entity,
                       event_timestamp, unique_key, payload, confidence, created_at
                FROM normalized_data
                WHERE created_at > ?
                ORDER BY created_at ASC
                LIMIT 100
                """,
                (since.isoformat(),)
            )

            results = []
            for row in cursor.fetchall():
                entity = json.loads(row["entity"]) if row["entity"] else []
                payload = json.loads(row["payload"]) if row["payload"] else {}

                results.append({
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
            return results

        except Exception as e:
            print(f"[WebSocketManager] Query error: {e}")
            return []

    async def _broadcast_to_clients(self, data_items: List[Dict[str, Any]]):
        """
        Broadcast data to all connected clients with filtering.

        Args:
            data_items: List of data items to broadcast
        """
        async with self._lock:
            clients = list(self.clients.values())

        # Track if any broadcast was actually sent (for stats)
        broadcast_sent = False

        for client in clients:
            if not client.connected:
                continue

            # Filter items for this client
            filtered_items = [item for item in data_items if client.should_receive(item)]

            if filtered_items:
                try:
                    await client.websocket.send_json({
                        "type": "data",
                        "timestamp": datetime.now().isoformat(),
                        "count": len(filtered_items),
                        "items": filtered_items
                    })
                    # Update last sent ID
                    max_id = max(item["id"] for item in filtered_items)
                    client.last_data_id = max(client.last_data_id, max_id)
                    broadcast_sent = True

                except Exception as e:
                    print(f"[WebSocketManager] Send error to {client.client_id}: {e}")
                    client.connected = False

        # Increment broadcast counter only if we actually sent data
        if broadcast_sent:
            self._stats["broadcast_count"] += 1

    async def handle_client_message(self, client_id: str, message: Dict[str, Any]):
        """
        Handle incoming message from client (filter configuration).

        Args:
            client_id: Client identifier
            message: Parsed JSON message
        """
        async with self._lock:
            if client_id not in self.clients:
                return

            client = self.clients[client_id]

        # Handle filter configuration
        if message.get("action") == "set_filters":
            filters = message.get("filters", {})
            plugins = filters.get("plugins")
            interval = filters.get("interval", 5)

            client.set_filters(plugins=plugins, interval=interval)

            # Acknowledge
            try:
                await client.websocket.send_json({
                    "type": "ack",
                    "message": f"Filters updated: plugins={plugins}, interval={interval}s"
                })
            except:
                pass

            print(f"[WebSocketManager] Client {client_id} set filters: plugins={plugins}, interval={interval}s")

    def get_stats(self) -> Dict[str, Any]:
        """
        Get manager statistics for WebSocket收口验证.

        Key verification fields:
        - client_count: Number of connected clients
        - polling_task_count: Should always be 0 or 1 (single-poll architecture)
        - last_poll_time: When database was last queried
        - total_broadcasts: Total number of broadcasts sent (not per-client)
        - poll_count: Total number of database polls executed
        - empty_poll_count: Number of polls with no new data (verifies no duplicate push)
        """
        return {
            # Connection stats
            "client_count": len(self.clients),
            "client_details": [
                {
                    "client_id": c.client_id,
                    "plugins": c.plugins,
                    "interval": c.interval,
                    "last_data_id": c.last_data_id
                }
                for c in self.clients.values()
            ],
            # Polling task stats (verification: should be 0 or 1)
            "polling_task_count": 1 if self._polling_task and not self._polling_task.done() else 0,
            "poll_interval": self._poll_interval,
            "running": self._running,
            # Database poll stats
            "poll_count": self._stats["poll_count"],
            "last_poll_time": self._stats["last_poll_time"],
            "last_data_count": self._stats["last_data_count"],
            "empty_poll_count": self._stats["empty_poll_count"],
            # Broadcast stats
            "total_broadcasts": self._stats["broadcast_count"],
        }
