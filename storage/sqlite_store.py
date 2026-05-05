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
    collection_mode TEXT DEFAULT 'full',    -- full/incremental
    plugin_kind TEXT DEFAULT 'embedded',    -- embedded/external
    execution_mode TEXT DEFAULT 'embedded_pipeline',
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

-- Plugin runtime configuration table
CREATE TABLE IF NOT EXISTS plugin_runtime_configs (
    plugin_id TEXT PRIMARY KEY,
    config_json TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (plugin_id) REFERENCES plugins(id) ON DELETE CASCADE
);

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

-- Raw SourceEvent ingestion table
CREATE TABLE IF NOT EXISTS raw_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    source_record_key TEXT NOT NULL,
    raw_event_key TEXT NOT NULL,
    source_system TEXT NOT NULL,
    source_event_type TEXT NOT NULL,
    event_granularity TEXT NOT NULL,
    source_record_id TEXT,
    source_record_hash TEXT,
    occurred_at_epoch REAL,
    collected_at_epoch REAL,
    dataset_key TEXT,
    collection TEXT,
    page_name TEXT,
    api_name TEXT,
    source_file TEXT,
    occurred_at TIMESTAMP,
    collected_at TIMESTAMP NOT NULL,
    payload TEXT NOT NULL,
    source_ref TEXT NOT NULL,
    event TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Canonical current entity table
CREATE TABLE IF NOT EXISTS canonical_entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,
    entity_key TEXT NOT NULL,
    entity_date TEXT,
    dataset_key TEXT NOT NULL,
    source_system TEXT NOT NULL,
    source_record_key TEXT NOT NULL,
    latest_raw_event_id INTEGER NOT NULL,
    latest_collected_at TIMESTAMP,
    latest_collected_at_epoch REAL,
    latest_source_record_hash TEXT,
    source_refs TEXT NOT NULL DEFAULT '[]',
    attributes TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(entity_type, entity_key)
);

-- Normalizer incremental checkpoint table
CREATE TABLE IF NOT EXISTS normalizer_state (
    dataset_key TEXT PRIMARY KEY,
    last_raw_event_id INTEGER DEFAULT 0,
    normalizer_version TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Foreground-submitted background processing jobs.
CREATE TABLE IF NOT EXISTS processing_jobs (
    job_id TEXT PRIMARY KEY,
    dataset_key TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'incremental',
    batch_size INTEGER NOT NULL DEFAULT 1000,
    status TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    result TEXT,
    error TEXT
);

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
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        # Enable foreign keys
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def init_schema(self) -> None:
        """Initialize database schema"""
        conn = self._get_connection()
        try:
            conn.executescript(SCHEMA_SQL)
            self._ensure_column(
                conn, "plugins", "collection_mode", "TEXT DEFAULT 'full'"
            )
            self._ensure_column(
                conn, "plugins", "plugin_kind", "TEXT DEFAULT 'embedded'"
            )
            self._ensure_column(
                conn, "plugins", "execution_mode", "TEXT DEFAULT 'embedded_pipeline'"
            )
            self._ensure_column(conn, "raw_events", "source_record_key", "TEXT")
            self._ensure_column(conn, "raw_events", "raw_event_key", "TEXT")
            self._ensure_column(conn, "raw_events", "dataset_key", "TEXT")
            self._ensure_column(conn, "raw_events", "collection", "TEXT")
            self._ensure_column(conn, "raw_events", "page_name", "TEXT")
            self._ensure_column(conn, "raw_events", "api_name", "TEXT")
            self._ensure_column(conn, "raw_events", "source_file", "TEXT")
            self._ensure_column(conn, "raw_events", "occurred_at_epoch", "REAL")
            self._ensure_column(conn, "raw_events", "collected_at_epoch", "REAL")
            if self._has_unique_index_on_columns(conn, "raw_events", ["idempotency_key"]):
                self._rebuild_raw_events_without_idempotency_unique(conn)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_raw_events_dataset_key ON raw_events(dataset_key)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_raw_events_collection ON raw_events(collection)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_raw_events_page_name ON raw_events(page_name)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_raw_events_api_name ON raw_events(api_name)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_raw_events_source_system ON raw_events(source_system)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_raw_events_source_record_id ON raw_events(source_record_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_raw_events_source_record_hash ON raw_events(source_record_hash)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_raw_events_source_record_key ON raw_events(source_record_key)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_raw_events_occurred_at ON raw_events(occurred_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_raw_events_collected_at ON raw_events(collected_at)"
            )
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_events_raw_event_key ON raw_events(raw_event_key)"
            )
            self._ensure_column(conn, "canonical_entities", "latest_collected_at", "TIMESTAMP")
            self._ensure_column(conn, "canonical_entities", "entity_date", "TEXT")
            self._ensure_column(conn, "canonical_entities", "latest_collected_at_epoch", "REAL")
            self._ensure_column(conn, "canonical_entities", "latest_source_record_hash", "TEXT")
            self._ensure_column(conn, "canonical_entities", "source_refs", "TEXT NOT NULL DEFAULT '[]'")
            self._ensure_column(conn, "normalizer_state", "normalizer_version", "TEXT")
            self._ensure_column(conn, "processing_jobs", "mode", "TEXT NOT NULL DEFAULT 'incremental'")
            self._ensure_column(conn, "processing_jobs", "batch_size", "INTEGER NOT NULL DEFAULT 1000")
            self._ensure_column(conn, "processing_jobs", "started_at", "TIMESTAMP")
            self._ensure_column(conn, "processing_jobs", "finished_at", "TIMESTAMP")
            self._ensure_column(conn, "processing_jobs", "result", "TEXT")
            self._ensure_column(conn, "processing_jobs", "error", "TEXT")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_canonical_entities_type ON canonical_entities(entity_type)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_canonical_entities_dataset ON canonical_entities(dataset_key)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_canonical_entities_date ON canonical_entities(entity_type, entity_date)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_processing_jobs_dataset_status ON processing_jobs(dataset_key, status)"
            )
            conn.commit()
            print(f"[SQLiteStore] Schema initialized at {self.db_path}")
        finally:
            conn.close()

    def _ensure_column(
        self,
        conn: sqlite3.Connection,
        table_name: str,
        column_name: str,
        column_definition: str,
    ) -> None:
        """Add an optional column for existing SQLite databases."""
        cursor = conn.execute(f"PRAGMA table_info({table_name})")
        existing_columns = {row["name"] for row in cursor.fetchall()}
        if column_name not in existing_columns:
            conn.execute(
                f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}"
            )

    def _has_unique_index_on_columns(
        self, conn: sqlite3.Connection, table_name: str, columns: list[str]
    ) -> bool:
        """Return True when a table has a unique index exactly on columns."""
        for index in conn.execute(f"PRAGMA index_list({table_name})").fetchall():
            if not index["unique"]:
                continue
            index_columns = [
                row["name"]
                for row in conn.execute(f"PRAGMA index_info({index['name']})").fetchall()
            ]
            if index_columns == columns:
                return True
        return False

    def _rebuild_raw_events_without_idempotency_unique(self, conn: sqlite3.Connection) -> None:
        """Remove the old idempotency_key unique constraint while preserving rows."""
        conn.execute("ALTER TABLE raw_events RENAME TO raw_events_legacy")
        conn.execute(
            """
            CREATE TABLE raw_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                source_record_key TEXT,
                raw_event_key TEXT,
                source_system TEXT NOT NULL,
                source_event_type TEXT NOT NULL,
                event_granularity TEXT NOT NULL,
                source_record_id TEXT,
                source_record_hash TEXT,
                occurred_at_epoch REAL,
                collected_at_epoch REAL,
                dataset_key TEXT,
                collection TEXT,
                page_name TEXT,
                api_name TEXT,
                source_file TEXT,
                occurred_at TIMESTAMP,
                collected_at TIMESTAMP NOT NULL,
                payload TEXT NOT NULL,
                source_ref TEXT NOT NULL,
                event TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            INSERT INTO raw_events (
                id, event_id, idempotency_key, source_record_key, raw_event_key,
                source_system, source_event_type, event_granularity,
                source_record_id, source_record_hash, occurred_at_epoch,
                collected_at_epoch, dataset_key, collection, page_name, api_name,
                source_file, occurred_at, collected_at, payload, source_ref,
                event, created_at
            )
            SELECT
                id, event_id, idempotency_key,
                COALESCE(source_record_key, idempotency_key),
                COALESCE(raw_event_key, idempotency_key || ':' || COALESCE(source_record_hash, '')),
                source_system, source_event_type, event_granularity,
                source_record_id, source_record_hash, NULL, NULL, dataset_key,
                collection, page_name, api_name, source_file, occurred_at,
                collected_at, payload, source_ref, event, created_at
            FROM raw_events_legacy
            """
        )
        conn.execute("DROP TABLE raw_events_legacy")

    def _default_config_from_schema(
        self, config_schema: dict[str, Any]
    ) -> dict[str, Any]:
        defaults = {}
        for field_name, schema in config_schema.items():
            if isinstance(schema, dict) and "default" in schema:
                defaults[field_name] = json.loads(
                    json.dumps(schema["default"], ensure_ascii=False, default=str)
                )
        return defaults

    def _deep_merge_config(
        self, defaults: dict[str, Any], overrides: dict[str, Any]
    ) -> dict[str, Any]:
        merged = json.loads(json.dumps(defaults, ensure_ascii=False, default=str))
        for key, value in overrides.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = self._deep_merge_config(merged[key], value)
            else:
                merged[key] = json.loads(
                    json.dumps(value, ensure_ascii=False, default=str)
                )
        return merged

    def _timestamp_epoch(self, value: Any) -> Optional[float]:
        if value in (None, ""):
            return None
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None

    def get_plugin_runtime_config(self, plugin_id: str) -> dict[str, Any]:
        plugin = self.get_plugin(plugin_id)
        if not plugin:
            raise KeyError(f"plugin not found: {plugin_id}")
        default_config = self._default_config_from_schema(plugin.get("config") or {})

        conn = self._get_connection()
        try:
            cursor = conn.execute(
                "SELECT config_json, updated_at FROM plugin_runtime_configs WHERE plugin_id = ?",
                (plugin_id,),
            )
            row = cursor.fetchone()
            if row:
                runtime_config = json.loads(row["config_json"])
                return {
                    "plugin_id": plugin_id,
                    "config": self._deep_merge_config(default_config, runtime_config),
                    "updated_at": row["updated_at"],
                    "source": "runtime+defaults",
                }
            return {
                "plugin_id": plugin_id,
                "config": default_config,
                "updated_at": None,
                "source": "default",
            }
        finally:
            conn.close()

    def save_plugin_runtime_config(
        self, plugin_id: str, config: dict[str, Any]
    ) -> None:
        """
        Persist plugin runtime config without validation.

        This is a low-level persistence method. Public API callers must validate
        config with validate_plugin_runtime_config before saving.
        """
        conn = self._get_connection()
        try:
            plugin = self.get_plugin(plugin_id)
            if not plugin:
                raise KeyError(f"plugin not found: {plugin_id}")

            conn.execute(
                """
                INSERT INTO plugin_runtime_configs (plugin_id, config_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(plugin_id) DO UPDATE SET
                    config_json = excluded.config_json,
                    updated_at = excluded.updated_at
                """,
                (
                    plugin_id,
                    json.dumps(config, ensure_ascii=False, default=str),
                    datetime.now(),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def close(self) -> None:
        """Close database connection (no-op for per-operation connections)"""
        pass

    # --- Plugin operations ---

    def save_plugin(
        self,
        plugin_id: str,
        name: str,
        version: str,
        description: str,
        author: str,
        tags: List[str],
        config_schema: Dict[str, Any],
        collection_mode: str = "full",
        plugin_kind: str = "embedded",
        execution_mode: str = "embedded_pipeline",
        enabled: bool = True,
    ) -> None:
        """Save or update plugin metadata"""
        conn = self._get_connection()
        try:
            conn.execute(
                """
                INSERT INTO plugins
                (id, name, version, description, author, config, collection_mode, plugin_kind, execution_mode, enabled, dependencies, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    version = excluded.version,
                    description = excluded.description,
                    author = excluded.author,
                    config = excluded.config,
                    collection_mode = excluded.collection_mode,
                    plugin_kind = excluded.plugin_kind,
                    execution_mode = excluded.execution_mode,
                    enabled = excluded.enabled,
                    dependencies = excluded.dependencies,
                    updated_at = excluded.updated_at
                """,
                (
                    plugin_id,
                    name,
                    version,
                    description,
                    author,
                    json.dumps(config_schema, ensure_ascii=False),
                    collection_mode,
                    plugin_kind,
                    execution_mode,
                    1 if enabled else 0,
                    json.dumps([]),  # MVP: dependencies must be empty
                    datetime.now(),
                ),
            )

            # Update tags (delete old, insert new)
            conn.execute("DELETE FROM plugin_tags WHERE plugin_id = ?", (plugin_id,))
            for tag in tags:
                conn.execute(
                    "INSERT INTO plugin_tags (plugin_id, tag) VALUES (?, ?)",
                    (plugin_id, tag),
                )

            conn.commit()
        finally:
            conn.close()

    def get_plugin(self, plugin_id: str) -> Optional[Dict[str, Any]]:
        """Get plugin metadata by ID"""
        conn = self._get_connection()
        try:
            cursor = conn.execute("SELECT * FROM plugins WHERE id = ?", (plugin_id,))
            row = cursor.fetchone()
            if not row:
                return None

            result = dict(row)
            result["config"] = json.loads(result["config"]) if result["config"] else {}
            result["dependencies"] = (
                json.loads(result["dependencies"]) if result["dependencies"] else []
            )

            # Get tags
            tag_cursor = conn.execute(
                "SELECT tag FROM plugin_tags WHERE plugin_id = ?", (plugin_id,)
            )
            result["tags"] = [r["tag"] for r in tag_cursor.fetchall()]

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
                plugin["config"] = (
                    json.loads(plugin["config"]) if plugin["config"] else {}
                )
                plugin["dependencies"] = (
                    json.loads(plugin["dependencies"]) if plugin["dependencies"] else []
                )

                # Get tags
                tag_cursor = conn.execute(
                    "SELECT tag FROM plugin_tags WHERE plugin_id = ?", (plugin["id"],)
                )
                plugin["tags"] = [r["tag"] for r in tag_cursor.fetchall()]
                plugins.append(plugin)
            return plugins
        finally:
            conn.close()

    # --- Raw data operations ---

    def save_raw_data(
        self,
        plugin_id: str,
        source: str,
        data: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> int:
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
                    (
                        json.dumps(metadata, ensure_ascii=False, default=str)
                        if metadata
                        else None
                    ),
                    datetime.now(),
                ),
            )
            conn.commit()
            return cursor.lastrowid
        finally:
            conn.close()

    def get_raw_data(self, raw_data_id: int) -> Optional[Dict[str, Any]]:
        """Get raw data by ID"""
        conn = self._get_connection()
        try:
            cursor = conn.execute("SELECT * FROM raw_data WHERE id = ?", (raw_data_id,))
            row = cursor.fetchone()
            if not row:
                return None

            result = dict(row)
            result["data"] = json.loads(result["data"]) if result["data"] else {}
            result["metadata"] = (
                json.loads(result["metadata"]) if result["metadata"] else {}
            )
            return result
        finally:
            conn.close()

    def _fallback_dataset_key(self, event: Dict[str, Any]) -> Optional[str]:
        from core.dataset_resolver import resolve_dataset_key

        return resolve_dataset_key(event)

    def save_raw_event(
        self, event: Dict[str, Any], dataset_key: Optional[str] = None
    ) -> tuple[str, Optional[int]]:
        """
        Save a SourceEvent v1 payload.

        Returns:
            ("accepted", rowid) for new events
            ("duplicated", existing rowid) for repeated raw_event_key
        """
        conn = self._get_connection()
        try:
            source_ref = event.get("source_ref") or {}
            collection = source_ref.get("collection")
            page_name = source_ref.get("page_name")
            api_name = source_ref.get("api_name")
            source_file = source_ref.get("source_file")
            if dataset_key is None:
                if event.get("source_system") == "dcp":
                    raise ValueError("DCP raw event requires explicit dataset_key")
                dataset_key = self._fallback_dataset_key(event)
            source_record_key = event["idempotency_key"]
            raw_event_key = f"{event['idempotency_key']}:{event.get('source_record_hash') or ''}"
            occurred_at_epoch = self._timestamp_epoch(event.get("occurred_at"))
            collected_at_epoch = self._timestamp_epoch(event.get("collected_at"))

            cursor = conn.execute(
                "SELECT id FROM raw_events WHERE raw_event_key = ?",
                (raw_event_key,),
            )
            existing = cursor.fetchone()
            if existing:
                return "duplicated", existing["id"]

            cursor = conn.execute(
                """
                INSERT INTO raw_events (
                    event_id, idempotency_key, source_record_key, raw_event_key,
                    source_system, source_event_type,
                    event_granularity, source_record_id, source_record_hash,
                    occurred_at_epoch, collected_at_epoch,
                    dataset_key, collection, page_name, api_name, source_file,
                    occurred_at, collected_at, payload, source_ref, event, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event["event_id"],
                    event["idempotency_key"],
                    source_record_key,
                    raw_event_key,
                    event["source_system"],
                    event["source_event_type"],
                    event["event_granularity"],
                    event.get("source_record_id"),
                    event.get("source_record_hash"),
                    occurred_at_epoch,
                    collected_at_epoch,
                    dataset_key,
                    collection,
                    page_name,
                    api_name,
                    source_file,
                    event.get("occurred_at"),
                    event["collected_at"],
                    json.dumps(event["payload"], ensure_ascii=False, default=str),
                    json.dumps(event["source_ref"], ensure_ascii=False, default=str),
                    json.dumps(event, ensure_ascii=False, default=str),
                    datetime.now(),
                ),
            )
            conn.commit()
            return "accepted", cursor.lastrowid
        finally:
            conn.close()

    def count_raw_events(self) -> int:
        """Count raw SourceEvent rows."""
        conn = self._get_connection()
        try:
            cursor = conn.execute("SELECT COUNT(*) AS count FROM raw_events")
            return int(cursor.fetchone()["count"])
        finally:
            conn.close()

    def _decode_raw_event_row(self, row: sqlite3.Row) -> Dict[str, Any]:
        result = dict(row)
        result["payload"] = json.loads(result["payload"]) if result["payload"] else {}
        result["source_ref"] = (
            json.loads(result["source_ref"]) if result["source_ref"] else {}
        )
        result["event"] = json.loads(result["event"]) if result["event"] else {}
        return result

    def get_raw_event_by_idempotency_key(
        self, idempotency_key: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get a raw SourceEvent by idempotency key.

        Compatibility only. Normalizers should use raw_event_key or
        source_record_key version-aware methods.
        """
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                "SELECT * FROM raw_events WHERE idempotency_key = ?",
                (idempotency_key,),
            )
            row = cursor.fetchone()
            if not row:
                return None

            return self._decode_raw_event_row(row)
        finally:
            conn.close()

    def get_raw_event_by_raw_event_key(
        self, raw_event_key: str
    ) -> Optional[Dict[str, Any]]:
        """Get a raw SourceEvent by versioned raw_event_key."""
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                "SELECT * FROM raw_events WHERE raw_event_key = ?",
                (raw_event_key,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return self._decode_raw_event_row(row)
        finally:
            conn.close()

    def list_raw_events_by_source_record_key(
        self, source_record_key: str
    ) -> List[Dict[str, Any]]:
        """List all versions for a source record."""
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                """
                SELECT * FROM raw_events
                WHERE source_record_key = ?
                ORDER BY collected_at ASC, id ASC
                """,
                (source_record_key,),
            )
            return [self._decode_raw_event_row(row) for row in cursor.fetchall()]
        finally:
            conn.close()

    def get_latest_raw_event_by_source_record_key(
        self, source_record_key: str
    ) -> Optional[Dict[str, Any]]:
        """Get the newest raw SourceEvent version for a source record."""
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                """
                SELECT * FROM raw_events
                WHERE source_record_key = ?
                ORDER BY collected_at DESC, id DESC
                LIMIT 1
                """,
                (source_record_key,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return self._decode_raw_event_row(row)
        finally:
            conn.close()

    def list_raw_events(
        self,
        dataset_key: Optional[str] = None,
        limit: int = 1000,
        offset: int = 0,
        after_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """List raw SourceEvents, optionally filtered by dataset."""
        conn = self._get_connection()
        try:
            where = ["1=1"]
            params: list[Any] = []
            if dataset_key:
                where.append("dataset_key = ?")
                params.append(dataset_key)
            if after_id is not None:
                where.append("id > ?")
                params.append(after_id)
            params.extend([limit, offset])
            cursor = conn.execute(
                f"""
                SELECT * FROM raw_events
                WHERE {' AND '.join(where)}
                ORDER BY id ASC
                LIMIT ? OFFSET ?
                """,
                params,
            )
            return [self._decode_raw_event_row(row) for row in cursor.fetchall()]
        finally:
            conn.close()

    # --- Canonical entity operations ---

    def _merge_source_refs(
        self,
        existing_refs: List[Dict[str, Any]],
        incoming_refs: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Merge source refs for the current entity snapshot.

        These refs identify source records contributing to the current entity;
        they are not a complete historical lineage table.
        """
        merged: list[dict[str, Any]] = []
        positions: dict[tuple[Any, Any, Any], int] = {}
        for ref in [*existing_refs, *incoming_refs]:
            if not isinstance(ref, dict):
                continue
            key = (
                ref.get("source_system"),
                ref.get("dataset_key"),
                ref.get("source_record_key"),
            )
            if key in positions:
                merged[positions[key]] = ref
            else:
                positions[key] = len(merged)
                merged.append(ref)
        return merged

    def upsert_canonical_entity(
        self,
        entity_type: str,
        entity_key: str,
        dataset_key: str,
        source_system: str,
        source_record_key: str,
        latest_raw_event_id: int,
        latest_collected_at: Optional[str],
        latest_collected_at_epoch: Optional[float],
        latest_source_record_hash: Optional[str],
        source_refs: List[Dict[str, Any]],
        attributes: Dict[str, Any],
        entity_date: Optional[str] = None,
    ) -> str:
        """Upsert a current canonical entity and return inserted/updated/ignored_older."""
        conn = self._get_connection()
        try:
            now = datetime.now()
            cursor = conn.execute(
                """
                SELECT id, latest_collected_at_epoch, source_refs FROM canonical_entities
                WHERE entity_type = ? AND entity_key = ?
                """,
                (entity_type, entity_key),
            )
            existing = cursor.fetchone()
            existing_source_refs: list[dict[str, Any]] = []
            if existing and existing["source_refs"]:
                try:
                    decoded_refs = json.loads(existing["source_refs"])
                    if isinstance(decoded_refs, list):
                        existing_source_refs = decoded_refs
                except json.JSONDecodeError:
                    existing_source_refs = []
            merged_source_refs = self._merge_source_refs(
                existing_source_refs, source_refs
            )
            source_refs_json = json.dumps(
                merged_source_refs, ensure_ascii=False, default=str
            )
            attributes_json = json.dumps(attributes, ensure_ascii=False, default=str)

            if not existing:
                conn.execute(
                    """
                    INSERT INTO canonical_entities (
                        entity_type, entity_key, entity_date, dataset_key, source_system,
                        source_record_key, latest_raw_event_id, latest_collected_at,
                        latest_collected_at_epoch, latest_source_record_hash,
                        source_refs, attributes, updated_at, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        entity_type,
                        entity_key,
                        entity_date,
                        dataset_key,
                        source_system,
                        source_record_key,
                        latest_raw_event_id,
                        latest_collected_at,
                        latest_collected_at_epoch,
                        latest_source_record_hash,
                        source_refs_json,
                        attributes_json,
                        now,
                        now,
                    ),
                )
                status = "inserted"
            else:
                existing_epoch = existing["latest_collected_at_epoch"]
                should_update = (
                    latest_collected_at_epoch is not None
                    and (
                        existing_epoch is None
                        or latest_collected_at_epoch >= float(existing_epoch)
                    )
                )
                if should_update:
                    conn.execute(
                        """
                        UPDATE canonical_entities
                        SET entity_date = ?,
                            dataset_key = ?,
                            source_system = ?,
                            source_record_key = ?,
                            latest_raw_event_id = ?,
                            latest_collected_at = ?,
                            latest_collected_at_epoch = ?,
                            latest_source_record_hash = ?,
                            source_refs = ?,
                            attributes = ?,
                            updated_at = ?
                        WHERE id = ?
                        """,
                        (
                            entity_date,
                            dataset_key,
                            source_system,
                            source_record_key,
                            latest_raw_event_id,
                            latest_collected_at,
                            latest_collected_at_epoch,
                            latest_source_record_hash,
                            source_refs_json,
                            attributes_json,
                            now,
                            existing["id"],
                        ),
                    )
                    status = "updated"
                else:
                    status = "ignored_older"
            conn.commit()
            return status
        finally:
            conn.close()

    def list_canonical_entities(
        self,
        entity_type: Optional[str] = None,
        dataset_key: Optional[str] = None,
        entity_date: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """List current canonical entities."""
        conn = self._get_connection()
        try:
            where = ["1=1"]
            params: list[Any] = []
            if entity_type:
                where.append("entity_type = ?")
                params.append(entity_type)
            if dataset_key:
                where.append("dataset_key = ?")
                params.append(dataset_key)
            if entity_date is not None:
                where.append("entity_date = ?")
                params.append(entity_date)
            params.append(limit)
            cursor = conn.execute(
                f"""
                SELECT * FROM canonical_entities
                WHERE {' AND '.join(where)}
                ORDER BY updated_at DESC, id DESC
                LIMIT ?
                """,
                params,
            )
            entities = []
            for row in cursor.fetchall():
                entity = dict(row)
                entity["attributes"] = (
                    json.loads(entity["attributes"]) if entity["attributes"] else {}
                )
                entity["source_refs"] = (
                    json.loads(entity["source_refs"]) if entity["source_refs"] else []
                )
                entities.append(entity)
            return entities
        finally:
            conn.close()

    def get_latest_canonical_entity_date(
        self, entity_type: str
    ) -> Optional[str]:
        """Return the latest non-empty entity_date for current canonical entities."""
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                """
                SELECT entity_date FROM canonical_entities
                WHERE entity_type = ? AND entity_date IS NOT NULL AND entity_date != ''
                ORDER BY entity_date DESC
                LIMIT 1
                """,
                (entity_type,),
            )
            row = cursor.fetchone()
            return row["entity_date"] if row else None
        finally:
            conn.close()

    def list_canonical_entity_dates(self, entity_type: str) -> List[str]:
        """Return distinct non-empty canonical entity dates in ascending order."""
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                """
                SELECT DISTINCT entity_date FROM canonical_entities
                WHERE entity_type = ? AND entity_date IS NOT NULL AND entity_date != ''
                ORDER BY entity_date ASC
                """,
                (entity_type,),
            )
            return [row["entity_date"] for row in cursor.fetchall()]
        finally:
            conn.close()

    # --- Normalizer state operations ---

    def get_normalizer_state(self, dataset_key: str) -> Dict[str, Any]:
        """Get incremental normalizer checkpoint for a dataset."""
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                "SELECT * FROM normalizer_state WHERE dataset_key = ?",
                (dataset_key,),
            )
            row = cursor.fetchone()
            if not row:
                return {
                    "dataset_key": dataset_key,
                    "last_raw_event_id": 0,
                    "normalizer_version": None,
                    "updated_at": None,
                }
            return dict(row)
        finally:
            conn.close()

    def save_normalizer_state(
        self,
        dataset_key: str,
        last_raw_event_id: int,
        normalizer_version: Optional[str],
    ) -> None:
        """Persist incremental normalizer checkpoint for a dataset."""
        conn = self._get_connection()
        try:
            conn.execute(
                """
                INSERT INTO normalizer_state (
                    dataset_key, last_raw_event_id, normalizer_version, updated_at
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(dataset_key) DO UPDATE SET
                    last_raw_event_id = excluded.last_raw_event_id,
                    normalizer_version = excluded.normalizer_version,
                    updated_at = excluded.updated_at
                """,
                (dataset_key, last_raw_event_id, normalizer_version, datetime.now()),
            )
            conn.commit()
        finally:
            conn.close()

    # --- Processing job operations ---

    def _deserialize_processing_job(self, row: sqlite3.Row | None) -> Optional[Dict[str, Any]]:
        if not row:
            return None
        job = dict(row)
        job["result"] = json.loads(job["result"]) if job.get("result") else None
        return job

    def create_processing_job(
        self,
        *,
        job_id: str,
        dataset_key: str,
        mode: str = "incremental",
        batch_size: int = 1000,
    ) -> Dict[str, Any]:
        """Create a queued processing job."""
        conn = self._get_connection()
        try:
            conn.execute(
                """
                INSERT INTO processing_jobs (
                    job_id, dataset_key, mode, batch_size, status, created_at
                )
                VALUES (?, ?, ?, ?, 'queued', ?)
                """,
                (job_id, dataset_key, mode, batch_size, datetime.now()),
            )
            conn.commit()
            job = self.get_processing_job(job_id)
            if job is None:
                raise RuntimeError(f"failed to create processing job: {job_id}")
            return job
        finally:
            conn.close()

    def mark_processing_job_running(self, job_id: str) -> None:
        conn = self._get_connection()
        try:
            conn.execute(
                """
                UPDATE processing_jobs
                SET status = 'running', started_at = ?
                WHERE job_id = ?
                """,
                (datetime.now(), job_id),
            )
            conn.commit()
        finally:
            conn.close()

    def mark_processing_job_succeeded(
        self, job_id: str, result: Dict[str, Any]
    ) -> None:
        conn = self._get_connection()
        try:
            conn.execute(
                """
                UPDATE processing_jobs
                SET status = 'succeeded', finished_at = ?, result = ?, error = NULL
                WHERE job_id = ?
                """,
                (
                    datetime.now(),
                    json.dumps(result, ensure_ascii=False, default=str),
                    job_id,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def mark_processing_job_failed(self, job_id: str, error: str) -> None:
        conn = self._get_connection()
        try:
            conn.execute(
                """
                UPDATE processing_jobs
                SET status = 'failed', finished_at = ?, error = ?
                WHERE job_id = ?
                """,
                (datetime.now(), error, job_id),
            )
            conn.commit()
        finally:
            conn.close()

    def get_processing_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                "SELECT * FROM processing_jobs WHERE job_id = ?",
                (job_id,),
            )
            return self._deserialize_processing_job(cursor.fetchone())
        finally:
            conn.close()

    def get_active_processing_job(
        self, dataset_key: str
    ) -> Optional[Dict[str, Any]]:
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                """
                SELECT * FROM processing_jobs
                WHERE dataset_key = ? AND status IN ('queued', 'running')
                ORDER BY created_at ASC
                LIMIT 1
                """,
                (dataset_key,),
            )
            return self._deserialize_processing_job(cursor.fetchone())
        finally:
            conn.close()

    # --- Normalized data operations ---

    def save_normalized_data(
        self,
        raw_data_id: int,
        plugin_id: str,
        event_type: Optional[str],
        event_source: Optional[str],
        entity: Optional[List[str]],
        event_timestamp: Optional[datetime],
        unique_key: str,
        payload: Dict[str, Any],
        confidence: float = 1.0,
    ) -> int:
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
                    datetime.now(),
                ),
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
        cursor = conn.execute("SELECT * FROM normalized_data WHERE id = ?", (data_id,))
        row = cursor.fetchone()
        if not row:
            return None

        result = dict(row)
        result["entity"] = json.loads(result["entity"]) if result["entity"] else []
        result["payload"] = json.loads(result["payload"]) if result["payload"] else {}
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
            result["state_data"] = (
                json.loads(result["state_data"]) if result["state_data"] else {}
            )
            return result
        finally:
            conn.close()

    def save_plugin_state(
        self,
        plugin_id: str,
        last_cursor: Optional[str] = None,
        last_timestamp: Optional[datetime] = None,
        last_offset: Optional[int] = None,
        state_data: Optional[Dict[str, Any]] = None,
    ) -> None:
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
                    (
                        json.dumps(state_data, ensure_ascii=False, default=str)
                        if state_data
                        else None
                    ),
                    datetime.now(),
                ),
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
                    (plugin_id, datetime.now()),
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
                    (plugin_id, datetime.now()),
                )
            conn.commit()
        finally:
            conn.close()

    # --- Log operations ---

    def write_log(
        self,
        plugin_id: Optional[str],
        level: str,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
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
                    (
                        json.dumps(details, ensure_ascii=False, default=str)
                        if details
                        else None
                    ),
                    datetime.now(),
                ),
            )
            conn.commit()
        finally:
            conn.close()
