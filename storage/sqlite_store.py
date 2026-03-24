"""
SQLite Storage Layer for Data Collector Hub v1.0

Assumptions:
- SQLite is the only storage backend (no PostgreSQL/Redis)
- JSON stored as TEXT (SQLite native JSON support not required)
- Schema matches 04-data-model.md exactly
- Simple explicit SQL, no ORM complexity
"""

import sqlite3
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List

from core.paths import resolve_project_path


# Schema definition matching 04-data-model.md
SCHEMA_SQL = """
-- Plugin info table
CREATE TABLE IF NOT EXISTS plugins (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    version TEXT,
    description TEXT,
    author TEXT,
    config TEXT,                            -- JSON format config
    enabled INTEGER DEFAULT 1,              -- 0=disabled, 1=enabled
    health_status TEXT DEFAULT 'unknown',   -- unknown/healthy/unhealthy
    last_health_check TIMESTAMP,
    dependencies TEXT,                      -- JSON array (MVP must be [])
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Plugin tags table (many-to-many)
CREATE TABLE IF NOT EXISTS plugin_tags (
    plugin_id TEXT NOT NULL,
    tag TEXT NOT NULL,
    PRIMARY KEY (plugin_id, tag),
    FOREIGN KEY (plugin_id) REFERENCES plugins(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_plugin_tags_tag ON plugin_tags(tag);

-- Raw data table
CREATE TABLE IF NOT EXISTS raw_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    plugin_id TEXT NOT NULL,
    source TEXT,
    data TEXT NOT NULL,  -- JSON string, original collected data
    metadata TEXT,       -- JSON string, collection metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_data_plugin ON raw_data(plugin_id);
CREATE INDEX IF NOT EXISTS idx_raw_data_time ON raw_data(created_at);
CREATE INDEX IF NOT EXISTS idx_raw_data_source ON raw_data(source);

-- Normalized data table (semi-structured layer)
CREATE TABLE IF NOT EXISTS normalized_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_data_id INTEGER NOT NULL,
    plugin_id TEXT NOT NULL,
    event_type TEXT,                      -- news/social/finance/alert
    event_source TEXT,                    -- Event source (e.g., Weibo, Zhihu)
    entity TEXT,                          -- Core entities (JSON array, optional)
    event_timestamp TIMESTAMP,            -- Event time
    unique_key TEXT NOT NULL,             -- Deduplication key
    payload TEXT NOT NULL,                -- Standardized container (JSON)
    confidence REAL DEFAULT 1.0,          -- Extraction confidence (0-1)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (raw_data_id) REFERENCES raw_data(id) ON DELETE CASCADE,
    UNIQUE(plugin_id, unique_key)         -- Deduplication constraint
);

CREATE INDEX IF NOT EXISTS idx_normalized_plugin ON normalized_data(plugin_id);
CREATE INDEX IF NOT EXISTS idx_normalized_event_type ON normalized_data(event_type);
CREATE INDEX IF NOT EXISTS idx_normalized_entity ON normalized_data(entity);
CREATE INDEX IF NOT EXISTS idx_normalized_timestamp ON normalized_data(event_timestamp);

-- Task execution stats table
CREATE TABLE IF NOT EXISTS task_stats (
    plugin_id TEXT PRIMARY KEY,
    run_count INTEGER DEFAULT 0,
    fail_count INTEGER DEFAULT 0,
    last_run TIMESTAMP,
    last_fail TIMESTAMP,
    consecutive_fails INTEGER DEFAULT 0
);

-- Plugin state table (for incremental collection)
CREATE TABLE IF NOT EXISTS plugin_state (
    plugin_id TEXT PRIMARY KEY,
    last_cursor TEXT,          -- Cursor: last collected ID, page number
    last_timestamp TIMESTAMP,  -- Timestamp: last collection time point
    last_offset INTEGER,       -- Offset: pagination offset
    state_data TEXT,           -- Extended state (JSON): plugin custom
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (plugin_id) REFERENCES plugins(id) ON DELETE CASCADE
);

-- Collection logs table
CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    plugin_id TEXT,
    task_id INTEGER,
    level TEXT,  -- INFO, WARNING, ERROR
    message TEXT,
    details TEXT,  -- JSON format
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_logs_plugin ON logs(plugin_id);
CREATE INDEX IF NOT EXISTS idx_logs_time ON logs(created_at);
"""


class SQLiteStore:
    """SQLite storage for Data Collector Hub v1.0"""

    def __init__(self, db_path: str | Path = "data/collector.db"):
        self.db_path = resolve_project_path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn: Optional[sqlite3.Connection] = None

    def _get_connection(self) -> sqlite3.Connection:
        """Get or create database connection (thread-safe)"""
        # Always create a new connection for thread safety
        # This is necessary for FastAPI which runs in multiple threads
        conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        # Enable foreign keys
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def init_schema(self) -> None:
        """Initialize database schema"""
        conn = self._get_connection()
        try:
            conn.executescript(SCHEMA_SQL)
            conn.commit()
            print(f"[SQLiteStore] Schema initialized at {self.db_path}")
        finally:
            conn.close()

    def close(self) -> None:
        """Close database connection (no-op for per-operation connections)"""
        pass

    # --- Plugin operations ---

    def save_plugin(self, plugin_id: str, name: str, version: str,
                    description: str, author: str, tags: List[str],
                    config_schema: Dict[str, Any], enabled: bool = True) -> None:
        """Save or update plugin metadata"""
        conn = self._get_connection()
        try:
            # Insert or replace plugin
            conn.execute(
                """
                INSERT OR REPLACE INTO plugins
                (id, name, version, description, author, config, enabled, dependencies, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    plugin_id, name, version, description, author,
                    json.dumps(config_schema, ensure_ascii=False),
                    1 if enabled else 0,
                    json.dumps([]),  # MVP: dependencies must be empty
                    datetime.now()
                )
            )

            # Update tags (delete old, insert new)
            conn.execute("DELETE FROM plugin_tags WHERE plugin_id = ?", (plugin_id,))
            for tag in tags:
                conn.execute(
                    "INSERT INTO plugin_tags (plugin_id, tag) VALUES (?, ?)",
                    (plugin_id, tag)
                )

            conn.commit()
        finally:
            conn.close()

    def get_plugin(self, plugin_id: str) -> Optional[Dict[str, Any]]:
        """Get plugin metadata by ID"""
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                "SELECT * FROM plugins WHERE id = ?", (plugin_id,)
            )
            row = cursor.fetchone()
            if not row:
                return None

            result = dict(row)
            result['config'] = json.loads(result['config']) if result['config'] else {}
            result['dependencies'] = json.loads(result['dependencies']) if result['dependencies'] else []

            # Get tags
            tag_cursor = conn.execute(
                "SELECT tag FROM plugin_tags WHERE plugin_id = ?", (plugin_id,)
            )
            result['tags'] = [r['tag'] for r in tag_cursor.fetchall()]

            return result
        finally:
            conn.close()

    def list_plugins(self) -> List[Dict[str, Any]]:
        """List all plugins"""
        conn = self._get_connection()
        try:
            cursor = conn.execute("SELECT * FROM plugins")
            plugins = []
            for row in cursor.fetchall():
                plugin = dict(row)
                plugin['config'] = json.loads(plugin['config']) if plugin['config'] else {}
                plugin['dependencies'] = json.loads(plugin['dependencies']) if plugin['dependencies'] else []

                # Get tags
                tag_cursor = conn.execute(
                    "SELECT tag FROM plugin_tags WHERE plugin_id = ?", (plugin['id'],)
                )
                plugin['tags'] = [r['tag'] for r in tag_cursor.fetchall()]
                plugins.append(plugin)
            return plugins
        finally:
            conn.close()

    # --- Raw data operations ---

    def save_raw_data(self, plugin_id: str, source: str,
                      data: Dict[str, Any], metadata: Optional[Dict[str, Any]] = None) -> int:
        """Save raw data, return raw_data_id"""
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                """
                INSERT INTO raw_data (plugin_id, source, data, metadata, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    plugin_id,
                    source,
                    json.dumps(data, ensure_ascii=False, default=str),
                    json.dumps(metadata, ensure_ascii=False, default=str) if metadata else None,
                    datetime.now()
                )
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()

    def get_raw_data(self, raw_data_id: int) -> Optional[Dict[str, Any]]:
        """Get raw data by ID"""
        conn = self._get_connection()
        cursor = conn.execute(
            "SELECT * FROM raw_data WHERE id = ?", (raw_data_id,)
        )
        row = cursor.fetchone()
        if not row:
            return None

        result = dict(row)
        result['data'] = json.loads(result['data']) if result['data'] else {}
        result['metadata'] = json.loads(result['metadata']) if result['metadata'] else {}
        return result

    # --- Normalized data operations ---

    def save_normalized_data(self, raw_data_id: int, plugin_id: str,
                             event_type: Optional[str], event_source: Optional[str],
                             entity: Optional[List[str]], event_timestamp: Optional[datetime],
                             unique_key: str, payload: Dict[str, Any],
                             confidence: float = 1.0) -> int:
        """
        Save normalized data with deduplication check.
        Returns: rowid if saved, -1 if duplicate
        """
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                """
                INSERT INTO normalized_data
                (raw_data_id, plugin_id, event_type, event_source, entity,
                 event_timestamp, unique_key, payload, confidence, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    raw_data_id,
                    plugin_id,
                    event_type,
                    event_source,
                    json.dumps(entity, ensure_ascii=False) if entity else None,
                    event_timestamp,
                    unique_key,
                    json.dumps(payload, ensure_ascii=False, default=str),
                    confidence,
                    datetime.now()
                )
            )
            conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError as e:
            if "UNIQUE constraint failed" in str(e):
                return -1  # Duplicate data, ignore
            raise
        finally:
            conn.close()

    def get_normalized_data(self, data_id: int) -> Optional[Dict[str, Any]]:
        """Get normalized data by ID"""
        conn = self._get_connection()
        cursor = conn.execute(
            "SELECT * FROM normalized_data WHERE id = ?", (data_id,)
        )
        row = cursor.fetchone()
        if not row:
            return None

        result = dict(row)
        result['entity'] = json.loads(result['entity']) if result['entity'] else []
        result['payload'] = json.loads(result['payload']) if result['payload'] else {}
        return result

    # --- Plugin state operations (for incremental collection) ---

    def get_plugin_state(self, plugin_id: str) -> Optional[Dict[str, Any]]:
        """Get plugin collection state"""
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                "SELECT * FROM plugin_state WHERE plugin_id = ?", (plugin_id,)
            )
            row = cursor.fetchone()
            if not row:
                return None

            result = dict(row)
            result['state_data'] = json.loads(result['state_data']) if result['state_data'] else {}
            return result
        finally:
            conn.close()

    def save_plugin_state(self, plugin_id: str,
                          last_cursor: Optional[str] = None,
                          last_timestamp: Optional[datetime] = None,
                          last_offset: Optional[int] = None,
                          state_data: Optional[Dict[str, Any]] = None) -> None:
        """Save plugin collection state"""
        conn = self._get_connection()
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO plugin_state
                (plugin_id, last_cursor, last_timestamp, last_offset, state_data, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    plugin_id,
                    last_cursor,
                    last_timestamp,
                    last_offset,
                    json.dumps(state_data, ensure_ascii=False, default=str) if state_data else None,
                    datetime.now()
                )
            )
            conn.commit()
        finally:
            conn.close()

    # --- Task stats operations ---

    def update_task_stats(self, plugin_id: str, success: bool) -> None:
        """Update task execution statistics"""
        conn = self._get_connection()
        try:
            if success:
                conn.execute(
                    """
                    INSERT INTO task_stats (plugin_id, run_count, last_run, consecutive_fails)
                    VALUES (?, 1, ?, 0)
                    ON CONFLICT(plugin_id) DO UPDATE SET
                        run_count = run_count + 1,
                        last_run = excluded.last_run,
                        consecutive_fails = 0
                    """,
                    (plugin_id, datetime.now())
                )
            else:
                conn.execute(
                    """
                    INSERT INTO task_stats (plugin_id, fail_count, last_fail, consecutive_fails)
                    VALUES (?, 1, ?, 1)
                    ON CONFLICT(plugin_id) DO UPDATE SET
                        fail_count = fail_count + 1,
                        last_fail = excluded.last_fail,
                        consecutive_fails = consecutive_fails + 1
                    """,
                    (plugin_id, datetime.now())
                )
            conn.commit()
        finally:
            conn.close()

    # --- Log operations ---

    def write_log(self, plugin_id: Optional[str], level: str,
                  message: str, details: Optional[Dict[str, Any]] = None) -> None:
        """Write log entry"""
        conn = self._get_connection()
        try:
            conn.execute(
                """
                INSERT INTO logs (plugin_id, level, message, details, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    plugin_id,
                    level,
                    message,
                    json.dumps(details, ensure_ascii=False, default=str) if details else None,
                    datetime.now()
                )
            )
            conn.commit()
        finally:
            conn.close()
