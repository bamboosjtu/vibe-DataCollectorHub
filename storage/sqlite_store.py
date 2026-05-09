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

-- Canonical relationship current table.
CREATE TABLE IF NOT EXISTS canonical_relationships (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    relationship_key TEXT NOT NULL UNIQUE,
    relationship_type TEXT NOT NULL,
    from_entity_type TEXT NOT NULL,
    from_entity_key TEXT NOT NULL,
    to_entity_type TEXT NOT NULL,
    to_entity_key TEXT NOT NULL,
    dataset_key TEXT NOT NULL,
    source_system TEXT NOT NULL,
    latest_raw_event_id INTEGER NOT NULL,
    latest_collected_at TIMESTAMP,
    attributes TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

-- External collector subprocess jobs.
CREATE TABLE IF NOT EXISTS external_collection_jobs (
    job_id TEXT PRIMARY KEY,
    plugin_id TEXT NOT NULL,
    profile TEXT,
    dataset_keys TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'incremental',
    status TEXT NOT NULL,
    command TEXT NOT NULL,
    cwd TEXT NOT NULL,
    datahub_url TEXT NOT NULL,
    processing_mode TEXT NOT NULL DEFAULT 'none',
    recent_days INTEGER,
    since_date TEXT,
    until_date TEXT,
    include_existing INTEGER DEFAULT 0,
    force INTEGER DEFAULT 0,
    due_only INTEGER DEFAULT 0,
    exit_code INTEGER,
    stdout TEXT,
    stderr TEXT,
    result TEXT,
    error TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    finished_at TIMESTAMP
);

-- Collection schedules for external collection profiles.
CREATE TABLE IF NOT EXISTS collection_schedules (
    schedule_id TEXT PRIMARY KEY,
    plugin_id TEXT NOT NULL,
    profile TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 0,
    schedule_cron TEXT NOT NULL,
    timezone TEXT NOT NULL DEFAULT 'Asia/Shanghai',
    default_request TEXT NOT NULL,
    last_triggered_at TIMESTAMP,
    next_run_at TIMESTAMP,
    last_job_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
CREATE INDEX IF NOT EXISTS idx_external_collection_jobs_plugin_status ON external_collection_jobs(plugin_id, status);
CREATE INDEX IF NOT EXISTS idx_external_collection_jobs_created_at ON external_collection_jobs(created_at);
CREATE INDEX IF NOT EXISTS idx_collection_schedules_plugin_enabled ON collection_schedules(plugin_id, enabled);
CREATE INDEX IF NOT EXISTS idx_collection_schedules_next_run_at ON collection_schedules(next_run_at);
CREATE INDEX IF NOT EXISTS idx_canonical_entities_type_dataset ON canonical_entities(entity_type, dataset_key);
CREATE INDEX IF NOT EXISTS idx_canonical_relationships_type ON canonical_relationships(relationship_type);
CREATE INDEX IF NOT EXISTS idx_canonical_relationships_from ON canonical_relationships(from_entity_type, from_entity_key);
CREATE INDEX IF NOT EXISTS idx_canonical_relationships_to ON canonical_relationships(to_entity_type, to_entity_key);
CREATE INDEX IF NOT EXISTS idx_canonical_relationships_dataset ON canonical_relationships(dataset_key);

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

    @staticmethod
    def _load_json_or_default(
        value: Any,
        default: Any,
    ) -> Any:
        if value in (None, ""):
            return default
        try:
            return json.loads(value)
        except (TypeError, json.JSONDecodeError):
            return default

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
            self._ensure_column(conn, "canonical_relationships", "relationship_key", "TEXT")
            self._ensure_column(conn, "canonical_relationships", "relationship_type", "TEXT")
            self._ensure_column(conn, "canonical_relationships", "from_entity_type", "TEXT")
            self._ensure_column(conn, "canonical_relationships", "from_entity_key", "TEXT")
            self._ensure_column(conn, "canonical_relationships", "to_entity_type", "TEXT")
            self._ensure_column(conn, "canonical_relationships", "to_entity_key", "TEXT")
            self._ensure_column(conn, "canonical_relationships", "dataset_key", "TEXT")
            self._ensure_column(conn, "canonical_relationships", "source_system", "TEXT")
            self._ensure_column(conn, "canonical_relationships", "latest_raw_event_id", "INTEGER")
            self._ensure_column(conn, "canonical_relationships", "latest_collected_at", "TIMESTAMP")
            self._ensure_column(conn, "canonical_relationships", "attributes", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column(conn, "normalizer_state", "normalizer_version", "TEXT")
            self._ensure_column(conn, "processing_jobs", "mode", "TEXT NOT NULL DEFAULT 'incremental'")
            self._ensure_column(conn, "processing_jobs", "batch_size", "INTEGER NOT NULL DEFAULT 1000")
            self._ensure_column(conn, "processing_jobs", "started_at", "TIMESTAMP")
            self._ensure_column(conn, "processing_jobs", "finished_at", "TIMESTAMP")
            self._ensure_column(conn, "processing_jobs", "result", "TEXT")
            self._ensure_column(conn, "processing_jobs", "error", "TEXT")
            self._ensure_column(conn, "external_collection_jobs", "profile", "TEXT")
            self._ensure_column(conn, "external_collection_jobs", "dataset_keys", "TEXT NOT NULL DEFAULT '[]'")
            self._ensure_column(conn, "external_collection_jobs", "mode", "TEXT NOT NULL DEFAULT 'incremental'")
            self._ensure_column(conn, "external_collection_jobs", "command", "TEXT NOT NULL DEFAULT '[]'")
            self._ensure_column(conn, "external_collection_jobs", "cwd", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(conn, "external_collection_jobs", "datahub_url", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(conn, "external_collection_jobs", "processing_mode", "TEXT NOT NULL DEFAULT 'none'")
            self._ensure_column(conn, "external_collection_jobs", "recent_days", "INTEGER")
            self._ensure_column(conn, "external_collection_jobs", "since_date", "TEXT")
            self._ensure_column(conn, "external_collection_jobs", "until_date", "TEXT")
            self._ensure_column(conn, "external_collection_jobs", "include_existing", "INTEGER DEFAULT 0")
            self._ensure_column(conn, "external_collection_jobs", "force", "INTEGER DEFAULT 0")
            self._ensure_column(conn, "external_collection_jobs", "due_only", "INTEGER DEFAULT 0")
            self._ensure_column(conn, "external_collection_jobs", "exit_code", "INTEGER")
            self._ensure_column(conn, "external_collection_jobs", "stdout", "TEXT")
            self._ensure_column(conn, "external_collection_jobs", "stderr", "TEXT")
            self._ensure_column(conn, "external_collection_jobs", "result", "TEXT")
            self._ensure_column(conn, "external_collection_jobs", "error", "TEXT")
            self._ensure_column(conn, "external_collection_jobs", "started_at", "TIMESTAMP")
            self._ensure_column(conn, "external_collection_jobs", "finished_at", "TIMESTAMP")
            self._ensure_column(conn, "collection_schedules", "plugin_id", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(conn, "collection_schedules", "profile", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(conn, "collection_schedules", "enabled", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(conn, "collection_schedules", "schedule_cron", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(conn, "collection_schedules", "timezone", "TEXT NOT NULL DEFAULT 'Asia/Shanghai'")
            self._ensure_column(conn, "collection_schedules", "default_request", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column(conn, "collection_schedules", "last_triggered_at", "TIMESTAMP")
            self._ensure_column(conn, "collection_schedules", "next_run_at", "TIMESTAMP")
            self._ensure_column(conn, "collection_schedules", "last_job_id", "TEXT")
            self._ensure_column(conn, "collection_schedules", "updated_at", "TIMESTAMP")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_canonical_entities_type ON canonical_entities(entity_type)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_canonical_entities_dataset ON canonical_entities(dataset_key)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_canonical_entities_type_dataset ON canonical_entities(entity_type, dataset_key)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_canonical_entities_date ON canonical_entities(entity_type, entity_date)"
            )
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_canonical_relationships_key ON canonical_relationships(relationship_key)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_canonical_relationships_type ON canonical_relationships(relationship_type)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_canonical_relationships_from ON canonical_relationships(from_entity_type, from_entity_key)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_canonical_relationships_to ON canonical_relationships(to_entity_type, to_entity_key)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_canonical_relationships_dataset ON canonical_relationships(dataset_key)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_processing_jobs_dataset_status ON processing_jobs(dataset_key, status)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_external_collection_jobs_plugin_status ON external_collection_jobs(plugin_id, status)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_external_collection_jobs_created_at ON external_collection_jobs(created_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_collection_schedules_plugin_enabled ON collection_schedules(plugin_id, enabled)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_collection_schedules_next_run_at ON collection_schedules(next_run_at)"
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

    def _reconcile_dcp_runtime_config(
        self,
        default_config: dict[str, Any],
        runtime_config: dict[str, Any],
        merged_config: dict[str, Any],
    ) -> dict[str, Any]:
        """Keep old DCP runtime configs compatible with newly required datasets.

        Runtime configs persisted before line_section/year_progress existed often
        contain enabled_datasets with only the original three Monitor datasets.
        If those datasets are absent from the persisted runtime dataset map, treat
        them as newly introduced defaults and enable ingestion for them.
        """
        if not isinstance(merged_config.get("datasets"), dict):
            return merged_config
        if not isinstance(merged_config.get("enabled_datasets"), list):
            return merged_config

        runtime_datasets = runtime_config.get("datasets") or {}
        if not isinstance(runtime_datasets, dict):
            runtime_datasets = {}

        default_enabled = default_config.get("enabled_datasets") or []
        for dataset_key in default_enabled:
            if dataset_key in runtime_datasets:
                continue
            dataset_config = merged_config["datasets"].get(dataset_key)
            if (
                isinstance(dataset_config, dict)
                and dataset_config.get("enabled") is True
                and dataset_key not in merged_config["enabled_datasets"]
            ):
                merged_config["enabled_datasets"].append(dataset_key)

        return merged_config

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
                merged_config = self._deep_merge_config(default_config, runtime_config)
                if plugin_id == "dcp":
                    merged_config = self._reconcile_dcp_runtime_config(
                        default_config,
                        runtime_config,
                        merged_config,
                    )
                return {
                    "plugin_id": plugin_id,
                    "config": merged_config,
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
        result["payload"] = self._load_json_or_default(result.get("payload"), {})
        result["source_ref"] = self._load_json_or_default(result.get("source_ref"), {})
        result["event"] = self._load_json_or_default(result.get("event"), {})
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
            return [
                entity
                for row in cursor.fetchall()
                if (entity := self._decode_canonical_entity(row)) is not None
            ]
        finally:
            conn.close()

    def _decode_canonical_entity(
        self, row: sqlite3.Row | None
    ) -> Optional[Dict[str, Any]]:
        if not row:
            return None
        entity = dict(row)
        entity["attributes"] = self._load_json_or_default(
            entity.get("attributes"), {}
        )
        entity["source_refs"] = self._load_json_or_default(
            entity.get("source_refs"), []
        )
        return entity

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

    # --- Canonical relationship operations ---

    def _decode_canonical_relationship(
        self, row: sqlite3.Row | None
    ) -> Optional[Dict[str, Any]]:
        if not row:
            return None
        relationship = dict(row)
        relationship["attributes"] = self._load_json_or_default(
            relationship.get("attributes"), {}
        )
        return relationship

    def upsert_canonical_relationship(
        self,
        relationship_key: str,
        relationship_type: str,
        from_entity_type: str,
        from_entity_key: str,
        to_entity_type: str,
        to_entity_key: str,
        dataset_key: str,
        source_system: str,
        latest_raw_event_id: int,
        latest_collected_at: Optional[str],
        attributes: Dict[str, Any],
    ) -> str:
        """Upsert a current canonical relationship."""
        conn = self._get_connection()
        try:
            now = datetime.now()
            attributes_json = json.dumps(attributes, ensure_ascii=False, default=str)
            cursor = conn.execute(
                "SELECT id FROM canonical_relationships WHERE relationship_key = ?",
                (relationship_key,),
            )
            existing = cursor.fetchone()
            if not existing:
                conn.execute(
                    """
                    INSERT INTO canonical_relationships (
                        relationship_key, relationship_type,
                        from_entity_type, from_entity_key,
                        to_entity_type, to_entity_key,
                        dataset_key, source_system, latest_raw_event_id,
                        latest_collected_at, attributes, updated_at, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        relationship_key,
                        relationship_type,
                        from_entity_type,
                        from_entity_key,
                        to_entity_type,
                        to_entity_key,
                        dataset_key,
                        source_system,
                        latest_raw_event_id,
                        latest_collected_at,
                        attributes_json,
                        now,
                        now,
                    ),
                )
                status = "inserted"
            else:
                conn.execute(
                    """
                    UPDATE canonical_relationships
                    SET relationship_type = ?,
                        from_entity_type = ?,
                        from_entity_key = ?,
                        to_entity_type = ?,
                        to_entity_key = ?,
                        dataset_key = ?,
                        source_system = ?,
                        latest_raw_event_id = ?,
                        latest_collected_at = ?,
                        attributes = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        relationship_type,
                        from_entity_type,
                        from_entity_key,
                        to_entity_type,
                        to_entity_key,
                        dataset_key,
                        source_system,
                        latest_raw_event_id,
                        latest_collected_at,
                        attributes_json,
                        now,
                        existing["id"],
                    ),
                )
                status = "updated"
            conn.commit()
            return status
        finally:
            conn.close()

    def get_canonical_relationship(
        self, relationship_key: str
    ) -> Optional[Dict[str, Any]]:
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                "SELECT * FROM canonical_relationships WHERE relationship_key = ?",
                (relationship_key,),
            )
            return self._decode_canonical_relationship(cursor.fetchone())
        finally:
            conn.close()

    def list_canonical_relationships(
        self,
        relationship_type: Optional[str] = None,
        from_entity_type: Optional[str] = None,
        from_entity_key: Optional[str] = None,
        to_entity_type: Optional[str] = None,
        to_entity_key: Optional[str] = None,
        dataset_key: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        conn = self._get_connection()
        try:
            where = ["1=1"]
            params: list[Any] = []
            if relationship_type:
                where.append("relationship_type = ?")
                params.append(relationship_type)
            if from_entity_type:
                where.append("from_entity_type = ?")
                params.append(from_entity_type)
            if from_entity_key:
                where.append("from_entity_key = ?")
                params.append(from_entity_key)
            if to_entity_type:
                where.append("to_entity_type = ?")
                params.append(to_entity_type)
            if to_entity_key:
                where.append("to_entity_key = ?")
                params.append(to_entity_key)
            if dataset_key:
                where.append("dataset_key = ?")
                params.append(dataset_key)
            params.append(limit)
            cursor = conn.execute(
                f"""
                SELECT * FROM canonical_relationships
                WHERE {' AND '.join(where)}
                ORDER BY updated_at DESC, id DESC
                LIMIT ?
                """,
                params,
            )
            return [
                relationship
                for row in cursor.fetchall()
                if (
                    relationship := self._decode_canonical_relationship(row)
                )
                is not None
            ]
        finally:
            conn.close()

    # --- Domain read model query helpers ---

    def list_domain_entities(
        self,
        entity_type: str,
        dataset_key: Optional[str] = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        return self.list_domain_entities_paged(
            entity_type,
            dataset_key=dataset_key,
            limit=limit,
            offset=offset,
        )

    def list_domain_entities_paged(
        self,
        entity_type: str,
        dataset_key: Optional[str] = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        conn = self._get_connection()
        try:
            where = ["entity_type = ?"]
            params: list[Any] = [entity_type]
            if dataset_key:
                where.append("dataset_key = ?")
                params.append(dataset_key)
            params.extend([limit, offset])
            cursor = conn.execute(
                f"""
                SELECT * FROM canonical_entities
                WHERE {' AND '.join(where)}
                ORDER BY updated_at DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                params,
            )
            return [
                entity
                for row in cursor.fetchall()
                if (entity := self._decode_canonical_entity(row)) is not None
            ]
        finally:
            conn.close()

    def list_domain_relationships_for_from_keys(
        self,
        *,
        relationship_type: Optional[str],
        from_entity_type: Optional[str],
        from_entity_keys: List[str],
    ) -> List[Dict[str, Any]]:
        if not from_entity_keys:
            return []
        placeholders = ",".join("?" for _ in from_entity_keys)
        conn = self._get_connection()
        try:
            where = [f"from_entity_key IN ({placeholders})"]
            params: list[Any] = list(from_entity_keys)
            if relationship_type:
                where.append("relationship_type = ?")
                params.append(relationship_type)
            if from_entity_type:
                where.append("from_entity_type = ?")
                params.append(from_entity_type)
            cursor = conn.execute(
                f"""
                SELECT * FROM canonical_relationships
                WHERE {' AND '.join(where)}
                ORDER BY updated_at DESC, id DESC
                """,
                params,
            )
            return [
                relationship
                for row in cursor.fetchall()
                if (
                    relationship := self._decode_canonical_relationship(row)
                )
                is not None
            ]
        finally:
            conn.close()

    def list_existing_entity_keys(
        self, *, entity_type: str, entity_keys: List[str]
    ) -> set[str]:
        if not entity_keys:
            return set()
        placeholders = ",".join("?" for _ in entity_keys)
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                f"""
                SELECT entity_key FROM canonical_entities
                WHERE entity_type = ? AND entity_key IN ({placeholders})
                """,
                [entity_type, *entity_keys],
            )
            return {str(row["entity_key"]) for row in cursor.fetchall()}
        finally:
            conn.close()

    def list_existing_tower_scopes(
        self, scopes: List[tuple[str, str]]
    ) -> set[tuple[str, str]]:
        if not scopes:
            return set()
        clauses: list[str] = []
        params: list[Any] = ["tower"]
        for single_project_code, bidding_section_code in scopes:
            clauses.append(
                "(json_extract(attributes, '$.single_project_code') = ? AND json_extract(attributes, '$.bidding_section_code') = ?)"
            )
            params.extend([single_project_code, bidding_section_code])
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                f"""
                SELECT
                    json_extract(attributes, '$.single_project_code') AS single_project_code,
                    json_extract(attributes, '$.bidding_section_code') AS bidding_section_code
                FROM canonical_entities
                WHERE entity_type = ? AND ({' OR '.join(clauses)})
                GROUP BY 1, 2
                """,
                params,
            )
            return {
                (str(row["single_project_code"]), str(row["bidding_section_code"]))
                for row in cursor.fetchall()
                if row["single_project_code"] not in (None, "")
                and row["bidding_section_code"] not in (None, "")
            }
        finally:
            conn.close()

    def list_domain_relationships(
        self,
        relationship_type: Optional[str] = None,
        from_entity_type: Optional[str] = None,
        from_entity_key: Optional[str] = None,
        to_entity_type: Optional[str] = None,
        to_entity_key: Optional[str] = None,
        dataset_key: Optional[str] = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        conn = self._get_connection()
        try:
            where = ["1=1"]
            params: list[Any] = []
            if relationship_type:
                where.append("relationship_type = ?")
                params.append(relationship_type)
            if from_entity_type:
                where.append("from_entity_type = ?")
                params.append(from_entity_type)
            if from_entity_key:
                where.append("from_entity_key = ?")
                params.append(from_entity_key)
            if to_entity_type:
                where.append("to_entity_type = ?")
                params.append(to_entity_type)
            if to_entity_key:
                where.append("to_entity_key = ?")
                params.append(to_entity_key)
            if dataset_key:
                where.append("dataset_key = ?")
                params.append(dataset_key)
            params.extend([limit, offset])
            cursor = conn.execute(
                f"""
                SELECT * FROM canonical_relationships
                WHERE {' AND '.join(where)}
                ORDER BY updated_at DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                params,
            )
            return [
                relationship
                for row in cursor.fetchall()
                if (
                    relationship := self._decode_canonical_relationship(row)
                )
                is not None
            ]
        finally:
            conn.close()

    @staticmethod
    def _attribute(entity: Dict[str, Any], key: str) -> Any:
        attributes = entity.get("attributes") or {}
        return attributes.get(key) if isinstance(attributes, dict) else None

    @staticmethod
    def _tower_scope_from_key(entity_key: str) -> tuple[str, str] | None:
        parts = str(entity_key).split(":")
        if parts[:2] != ["dcp", "tower"] or len(parts) < 5:
            return None
        return parts[2], parts[3]

    def _single_project_keys_by_project(self) -> Dict[str, List[str]]:
        mapping: Dict[str, List[str]] = {}
        for relationship in self.list_domain_relationships(
            relationship_type="HAS_SINGLE_PROJECT",
            from_entity_type="project",
            to_entity_type="single_project",
            limit=100000,
        ):
            mapping.setdefault(relationship["from_entity_key"], []).append(
                relationship["to_entity_key"]
            )
        return mapping

    def _bidding_section_keys_by_single(self) -> Dict[str, List[str]]:
        mapping: Dict[str, List[str]] = {}
        for relationship in self.list_domain_relationships(
            relationship_type="HAS_BIDDING_SECTION",
            from_entity_type="single_project",
            to_entity_type="bidding_section",
            limit=100000,
        ):
            mapping.setdefault(relationship["from_entity_key"], []).append(
                relationship["to_entity_key"]
            )
        return mapping

    def _line_section_keys_by_bidding(self) -> Dict[str, List[str]]:
        mapping: Dict[str, List[str]] = {}
        for relationship in self.list_domain_relationships(
            relationship_type="HAS_LINE_SECTION",
            from_entity_type="bidding_section",
            to_entity_type="line_section",
            limit=100000,
        ):
            mapping.setdefault(relationship["from_entity_key"], []).append(
                relationship["to_entity_key"]
            )
        return mapping

    def _project_entity_by_code(self, project_code: str) -> Optional[Dict[str, Any]]:
        for entity in self.list_domain_entities("project", limit=100000):
            if self._attribute(entity, "project_code") == project_code:
                return entity
        return None

    def list_domain_projects(
        self,
        keyword: Optional[str] = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        conn = self._get_connection()
        try:
            where = ["entity_type = 'project'"]
            params: list[Any] = []
            if keyword:
                where.append(
                    "("
                    "COALESCE(json_extract(attributes, '$.project_code'), '') LIKE ? "
                    "OR COALESCE(json_extract(attributes, '$.project_name'), '') LIKE ?"
                    ")"
                )
                needle = f"%{keyword}%"
                params.extend([needle, needle])
            params.extend([limit, offset])
            cursor = conn.execute(
                f"""
                SELECT * FROM canonical_entities
                WHERE {' AND '.join(where)}
                ORDER BY updated_at DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                params,
            )
            projects = [
                project
                for row in cursor.fetchall()
                if (project := self._decode_canonical_entity(row)) is not None
            ]
            if not projects:
                return []

            project_keys = [project["entity_key"] for project in projects]
            project_codes = [
                self._attribute(project, "project_code")
                for project in projects
                if self._attribute(project, "project_code") not in (None, "")
            ]
            single_relationships = self.list_domain_relationships_for_from_keys(
                relationship_type="HAS_SINGLE_PROJECT",
                from_entity_type="project",
                from_entity_keys=project_keys,
            )
            single_keys = list(
                dict.fromkeys(
                    relationship["to_entity_key"] for relationship in single_relationships
                )
            )
            bidding_relationships = self.list_domain_relationships_for_from_keys(
                relationship_type="HAS_BIDDING_SECTION",
                from_entity_type="single_project",
                from_entity_keys=single_keys,
            )
            bidding_keys = list(
                dict.fromkeys(
                    relationship["to_entity_key"] for relationship in bidding_relationships
                )
            )
            line_relationships = self.list_domain_relationships_for_from_keys(
                relationship_type="HAS_LINE_SECTION",
                from_entity_type="bidding_section",
                from_entity_keys=bidding_keys,
            )

            single_codes_by_project: Dict[str, set[str]] = {}
            for relationship in single_relationships:
                if relationship["to_entity_key"].startswith("dcp:single_project:"):
                    single_codes_by_project.setdefault(
                        relationship["from_entity_key"], set()
                    ).add(relationship["to_entity_key"].split(":")[-1])

            bidding_codes_by_project: Dict[str, set[str]] = {}
            bidding_keys_by_project: Dict[str, set[str]] = {}
            project_by_single_key = {
                relationship["to_entity_key"]: relationship["from_entity_key"]
                for relationship in single_relationships
            }
            for relationship in bidding_relationships:
                project_key = project_by_single_key.get(relationship["from_entity_key"])
                if not project_key:
                    continue
                bidding_keys_by_project.setdefault(project_key, set()).add(
                    relationship["to_entity_key"]
                )
                if relationship["to_entity_key"].startswith("dcp:bidding_section:"):
                    bidding_codes_by_project.setdefault(project_key, set()).add(
                        relationship["to_entity_key"].split(":")[-1]
                    )

            line_keys_by_project: Dict[str, set[str]] = {}
            project_by_bidding_key = {}
            for project_key, keys in bidding_keys_by_project.items():
                for key in keys:
                    project_by_bidding_key[key] = project_key
            for relationship in line_relationships:
                project_key = project_by_bidding_key.get(relationship["from_entity_key"])
                if project_key:
                    line_keys_by_project.setdefault(project_key, set()).add(
                        relationship["to_entity_key"]
                    )

            def _count_for_project(
                *,
                entity_type: str,
                project_code: Any,
                single_codes: set[str] | None = None,
                bidding_codes: set[str] | None = None,
            ) -> int:
                clauses = ["entity_type = ?"]
                clause_params: list[Any] = [entity_type]
                or_clauses: list[str] = []
                if project_code not in (None, ""):
                    or_clauses.append(
                        "json_extract(attributes, '$.project_code') = ?"
                    )
                    clause_params.append(project_code)
                if single_codes:
                    placeholders = ",".join("?" for _ in single_codes)
                    or_clauses.append(
                        f"json_extract(attributes, '$.single_project_code') IN ({placeholders})"
                    )
                    clause_params.extend(sorted(single_codes))
                if bidding_codes:
                    placeholders = ",".join("?" for _ in bidding_codes)
                    or_clauses.append(
                        f"json_extract(attributes, '$.bidding_section_code') IN ({placeholders})"
                    )
                    clause_params.extend(sorted(bidding_codes))
                if not or_clauses:
                    return 0
                cursor = conn.execute(
                    f"""
                    SELECT COUNT(*) AS count
                    FROM canonical_entities
                    WHERE {' AND '.join(clauses)} AND ({' OR '.join(or_clauses)})
                    """,
                    clause_params,
                )
                row = cursor.fetchone()
                return int(row["count"]) if row else 0

            def _work_point_stats(project_code: Any) -> tuple[int, Optional[str]]:
                if project_code in (None, ""):
                    return 0, None
                cursor = conn.execute(
                    """
                    SELECT COUNT(*) AS count, MAX(entity_date) AS latest_work_date
                    FROM canonical_entities
                    WHERE entity_type = 'work_point'
                      AND json_extract(attributes, '$.project_code') = ?
                    """,
                    (project_code,),
                )
                row = cursor.fetchone()
                return (
                    int(row["count"]) if row and row["count"] is not None else 0,
                    row["latest_work_date"] if row else None,
                )

            items: List[Dict[str, Any]] = []
            for project in projects:
                project_key = project["entity_key"]
                project_code = self._attribute(project, "project_code")
                single_codes = single_codes_by_project.get(project_key, set())
                bidding_codes = bidding_codes_by_project.get(project_key, set())
                work_point_count, latest_work_date = _work_point_stats(project_code)
                progress_count = _count_for_project(
                    entity_type="project_progress",
                    project_code=project_code,
                )
                tower_count = _count_for_project(
                    entity_type="tower",
                    project_code=project_code,
                    single_codes=single_codes,
                    bidding_codes=bidding_codes,
                )
                station_count = _count_for_project(
                    entity_type="station",
                    project_code=project_code,
                    single_codes=single_codes,
                )
                items.append(
                    {
                        "project_key": project_key,
                        "project_code": project_code,
                        "project_name": self._attribute(project, "project_name"),
                        "single_project_count": len(single_codes),
                        "bidding_section_count": len(
                            bidding_keys_by_project.get(project_key, set())
                        ),
                        "tower_count": tower_count,
                        "station_count": station_count,
                        "line_section_count": len(
                            line_keys_by_project.get(project_key, set())
                        ),
                        "work_point_count": work_point_count,
                        "progress_count": progress_count,
                        "latest_work_date": latest_work_date,
                        "latest_updated_at": project.get("updated_at"),
                    }
                )
            return items
        finally:
            conn.close()

    def get_domain_project(self, project_code: str) -> Optional[Dict[str, Any]]:
        return self._project_entity_by_code(project_code)

    def get_project_domain_view(
        self,
        project_code: str,
        *,
        date: Optional[str] = None,
        include_work_points: bool = True,
        include_towers: bool = True,
        include_stations: bool = True,
        include_line_sections: bool = True,
        limit: int = 10000,
    ) -> Optional[Dict[str, Any]]:
        project = self._project_entity_by_code(project_code)
        if project is None:
            return None

        project_key = project["entity_key"]
        project_relationships = self.list_domain_relationships(
            relationship_type="HAS_SINGLE_PROJECT",
            from_entity_type="project",
            from_entity_key=project_key,
            limit=100000,
        )
        progress_relationships = self.list_domain_relationships(
            relationship_type="HAS_PROJECT_PROGRESS",
            from_entity_type="project",
            from_entity_key=project_key,
            limit=100000,
        )
        single_keys = list(
            dict.fromkeys(
                relationship["to_entity_key"] for relationship in project_relationships
            )
        )
        single_entities = {
            entity["entity_key"]: entity
            for entity in self.list_domain_entities("single_project", limit=100000)
            if entity["entity_key"] in set(single_keys)
        }
        bidding_relationships_all = self.list_domain_relationships(
            relationship_type="HAS_BIDDING_SECTION",
            from_entity_type="single_project",
            limit=100000,
        )
        bidding_relationships = [
            relationship
            for relationship in bidding_relationships_all
            if relationship["from_entity_key"] in set(single_keys)
        ]
        bidding_keys = list(
            dict.fromkeys(
                relationship["to_entity_key"] for relationship in bidding_relationships
            )
        )
        bidding_entities = {
            entity["entity_key"]: entity
            for entity in self.list_domain_entities("bidding_section", limit=100000)
            if entity["entity_key"] in set(bidding_keys)
        }
        line_relationships_all = self.list_domain_relationships(
            relationship_type="HAS_LINE_SECTION",
            from_entity_type="bidding_section",
            limit=100000,
        )
        line_relationships = [
            relationship
            for relationship in line_relationships_all
            if relationship["from_entity_key"] in set(bidding_keys)
        ]
        line_keys = list(
            dict.fromkeys(
                relationship["to_entity_key"] for relationship in line_relationships
            )
        )
        line_entities = {
            entity["entity_key"]: entity
            for entity in self.list_domain_entities("line_section", limit=100000)
            if entity["entity_key"] in set(line_keys)
        }
        tower_sequence_relationships = (
            [
                relationship
                for relationship in self.list_domain_relationships(
                    relationship_type="HAS_TOWER_SEQUENCE",
                    from_entity_type="line_section",
                    limit=100000,
                )
                if relationship["from_entity_key"] in set(line_keys)
            ]
            if include_line_sections
            else []
        )

        single_project_codes = {
            self._attribute(entity, "single_project_code")
            for entity in single_entities.values()
            if self._attribute(entity, "single_project_code")
        }
        bidding_section_codes = {
            self._attribute(entity, "bidding_section_code")
            for entity in bidding_entities.values()
            if self._attribute(entity, "bidding_section_code")
        }

        tower_entities_all = self.list_domain_entities("tower", limit=100000)
        tower_entities = [
            entity
            for entity in tower_entities_all
            if (
                self._attribute(entity, "project_code") == project_code
                or self._attribute(entity, "single_project_code")
                in single_project_codes
                or self._attribute(entity, "bidding_section_code")
                in bidding_section_codes
            )
        ]
        station_entities_all = self.list_domain_entities("station", limit=100000)
        station_entities = [
            entity
            for entity in station_entities_all
            if (
                self._attribute(entity, "project_code") == project_code
                or self._attribute(entity, "single_project_code")
                in single_project_codes
            )
        ]
        work_point_entities_all = self.list_domain_entities("work_point", limit=100000)
        work_point_entities = [
            entity
            for entity in work_point_entities_all
            if self._attribute(entity, "project_code") == project_code
            and (date is None or entity.get("entity_date") == date)
        ]
        work_point_entities.sort(
            key=lambda entity: (
                entity.get("entity_date") or "",
                entity.get("updated_at") or "",
            ),
            reverse=True,
        )
        progress_keys = list(
            dict.fromkeys(
                relationship["to_entity_key"] for relationship in progress_relationships
            )
        )
        progress_entities = [
            entity
            for entity in self.list_domain_entities("project_progress", limit=100000)
            if entity["entity_key"] in set(progress_keys)
            or self._attribute(entity, "project_code") == project_code
        ]

        relationships = [
            *project_relationships,
            *progress_relationships,
            *bidding_relationships,
            *line_relationships,
            *tower_sequence_relationships,
        ]

        return {
            "project": project,
            "single_projects": list(single_entities.values()),
            "bidding_sections": list(bidding_entities.values()),
            "towers": tower_entities[:limit] if include_towers else [],
            "stations": station_entities[:limit] if include_stations else [],
            "line_sections": list(line_entities.values())[:limit]
            if include_line_sections
            else [],
            "work_points": work_point_entities[:limit] if include_work_points else [],
            "project_progress": progress_entities[:limit],
            "relationships": relationships,
            "summary": {
                "single_project_count": len(single_entities),
                "bidding_section_count": len(bidding_entities),
                "tower_count": len(tower_entities),
                "station_count": len(station_entities),
                "line_section_count": len(line_entities),
                "work_point_count": len(work_point_entities),
                "project_progress_count": len(progress_entities),
            },
        }

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

    # --- External collection job operations ---

    def _deserialize_external_collection_job(
        self, row: sqlite3.Row | None
    ) -> Optional[Dict[str, Any]]:
        if not row:
            return None
        job = dict(row)
        job["dataset_keys"] = (
            json.loads(job["dataset_keys"]) if job.get("dataset_keys") else []
        )
        job["command"] = json.loads(job["command"]) if job.get("command") else []
        job["result"] = json.loads(job["result"]) if job.get("result") else None
        for field in ("include_existing", "force", "due_only"):
            job[field] = bool(job.get(field))
        return job

    def create_external_collection_job(
        self,
        *,
        job_id: str,
        plugin_id: str,
        profile: Optional[str],
        dataset_keys: List[str],
        mode: str,
        command: List[str],
        cwd: str,
        datahub_url: str,
        processing_mode: str = "none",
        recent_days: Optional[int] = None,
        since_date: Optional[str] = None,
        until_date: Optional[str] = None,
        include_existing: bool = False,
        force: bool = False,
        due_only: bool = False,
    ) -> Dict[str, Any]:
        conn = self._get_connection()
        try:
            conn.execute(
                """
                INSERT INTO external_collection_jobs (
                    job_id, plugin_id, profile, dataset_keys, mode, status,
                    command, cwd, datahub_url, processing_mode, recent_days,
                    since_date, until_date, include_existing, force, due_only,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    plugin_id,
                    profile,
                    json.dumps(dataset_keys, ensure_ascii=False),
                    mode,
                    json.dumps(command, ensure_ascii=False),
                    cwd,
                    datahub_url,
                    processing_mode,
                    recent_days,
                    since_date,
                    until_date,
                    1 if include_existing else 0,
                    1 if force else 0,
                    1 if due_only else 0,
                    datetime.now(),
                ),
            )
            conn.commit()
            job = self.get_external_collection_job(job_id)
            if job is None:
                raise RuntimeError(f"failed to create external collection job: {job_id}")
            return job
        finally:
            conn.close()

    def mark_external_collection_job_running(self, job_id: str) -> None:
        conn = self._get_connection()
        try:
            conn.execute(
                """
                UPDATE external_collection_jobs
                SET status = 'running', started_at = ?
                WHERE job_id = ?
                """,
                (datetime.now(), job_id),
            )
            conn.commit()
        finally:
            conn.close()

    def mark_external_collection_job_succeeded(
        self,
        job_id: str,
        exit_code: int,
        stdout: str,
        stderr: str,
        result: Optional[Dict[str, Any]],
    ) -> None:
        conn = self._get_connection()
        try:
            conn.execute(
                """
                UPDATE external_collection_jobs
                SET status = 'succeeded',
                    finished_at = ?,
                    exit_code = ?,
                    stdout = ?,
                    stderr = ?,
                    result = ?,
                    error = NULL
                WHERE job_id = ?
                """,
                (
                    datetime.now(),
                    exit_code,
                    stdout,
                    stderr,
                    (
                        json.dumps(result, ensure_ascii=False, default=str)
                        if result is not None
                        else None
                    ),
                    job_id,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def mark_external_collection_job_failed(
        self,
        job_id: str,
        exit_code: Optional[int],
        stdout: str,
        stderr: str,
        error: str,
    ) -> None:
        conn = self._get_connection()
        try:
            conn.execute(
                """
                UPDATE external_collection_jobs
                SET status = 'failed',
                    finished_at = ?,
                    exit_code = ?,
                    stdout = ?,
                    stderr = ?,
                    error = ?
                WHERE job_id = ?
                """,
                (datetime.now(), exit_code, stdout, stderr, error, job_id),
            )
            conn.commit()
        finally:
            conn.close()

    def get_external_collection_job(
        self, job_id: str
    ) -> Optional[Dict[str, Any]]:
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                "SELECT * FROM external_collection_jobs WHERE job_id = ?",
                (job_id,),
            )
            return self._deserialize_external_collection_job(cursor.fetchone())
        finally:
            conn.close()

    def list_external_collection_jobs(
        self,
        plugin_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        conn = self._get_connection()
        try:
            where = ["1=1"]
            params: list[Any] = []
            if plugin_id:
                where.append("plugin_id = ?")
                params.append(plugin_id)
            if status:
                where.append("status = ?")
                params.append(status)
            params.append(limit)
            cursor = conn.execute(
                f"""
                SELECT * FROM external_collection_jobs
                WHERE {' AND '.join(where)}
                ORDER BY created_at DESC
                LIMIT ?
                """,
                params,
            )
            return [
                job
                for row in cursor.fetchall()
                if (job := self._deserialize_external_collection_job(row)) is not None
            ]
        finally:
            conn.close()

    def get_active_external_collection_job(
        self, plugin_id: str, dataset_keys: List[str]
    ) -> Optional[Dict[str, Any]]:
        requested = set(dataset_keys)
        if not requested:
            return None
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                """
                SELECT * FROM external_collection_jobs
                WHERE plugin_id = ? AND status IN ('queued', 'running')
                ORDER BY created_at ASC
                """,
                (plugin_id,),
            )
            for row in cursor.fetchall():
                job = self._deserialize_external_collection_job(row)
                if job and requested.intersection(set(job["dataset_keys"])):
                    return job
            return None
        finally:
            conn.close()

    # --- Collection schedule operations ---

    def _deserialize_collection_schedule(
        self, row: sqlite3.Row | None
    ) -> Optional[Dict[str, Any]]:
        if not row:
            return None
        schedule = dict(row)
        schedule["default_request"] = (
            json.loads(schedule["default_request"])
            if schedule.get("default_request")
            else {}
        )
        schedule["enabled"] = bool(schedule.get("enabled"))
        return schedule

    def create_or_update_collection_schedule(
        self,
        *,
        schedule_id: str,
        plugin_id: str,
        profile: str,
        schedule_cron: str,
        default_request: Dict[str, Any],
        timezone: str = "Asia/Shanghai",
        enabled: Optional[bool] = None,
        next_run_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        existing = self.get_collection_schedule(schedule_id)
        effective_enabled = (
            existing["enabled"] if existing is not None and enabled is None else bool(enabled)
        )
        conn = self._get_connection()
        try:
            conn.execute(
                """
                INSERT INTO collection_schedules (
                    schedule_id, plugin_id, profile, enabled, schedule_cron,
                    timezone, default_request, next_run_at, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(schedule_id) DO UPDATE SET
                    plugin_id = excluded.plugin_id,
                    profile = excluded.profile,
                    enabled = excluded.enabled,
                    schedule_cron = excluded.schedule_cron,
                    timezone = excluded.timezone,
                    default_request = excluded.default_request,
                    next_run_at = COALESCE(collection_schedules.next_run_at, excluded.next_run_at),
                    updated_at = excluded.updated_at
                """,
                (
                    schedule_id,
                    plugin_id,
                    profile,
                    1 if effective_enabled else 0,
                    schedule_cron,
                    timezone,
                    json.dumps(default_request, ensure_ascii=False, default=str),
                    next_run_at,
                    datetime.now(),
                    datetime.now(),
                ),
            )
            conn.commit()
            schedule = self.get_collection_schedule(schedule_id)
            if schedule is None:
                raise RuntimeError(
                    f"failed to create or update collection schedule: {schedule_id}"
                )
            return schedule
        finally:
            conn.close()

    def list_collection_schedules(
        self,
        *,
        plugin_id: Optional[str] = None,
        enabled: Optional[bool] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        conn = self._get_connection()
        try:
            where = ["1=1"]
            params: list[Any] = []
            if plugin_id:
                where.append("plugin_id = ?")
                params.append(plugin_id)
            if enabled is not None:
                where.append("enabled = ?")
                params.append(1 if enabled else 0)
            params.append(limit)
            cursor = conn.execute(
                f"""
                SELECT * FROM collection_schedules
                WHERE {' AND '.join(where)}
                ORDER BY plugin_id ASC, profile ASC
                LIMIT ?
                """,
                params,
            )
            return [
                schedule
                for row in cursor.fetchall()
                if (schedule := self._deserialize_collection_schedule(row)) is not None
            ]
        finally:
            conn.close()

    def get_collection_schedule(self, schedule_id: str) -> Optional[Dict[str, Any]]:
        conn = self._get_connection()
        try:
            cursor = conn.execute(
                "SELECT * FROM collection_schedules WHERE schedule_id = ?",
                (schedule_id,),
            )
            return self._deserialize_collection_schedule(cursor.fetchone())
        finally:
            conn.close()

    def update_collection_schedule_enabled(
        self,
        schedule_id: str,
        enabled: bool,
        *,
        next_run_at: Optional[str] = None,
    ) -> None:
        conn = self._get_connection()
        try:
            conn.execute(
                """
                UPDATE collection_schedules
                SET enabled = ?, next_run_at = COALESCE(?, next_run_at), updated_at = ?
                WHERE schedule_id = ?
                """,
                (1 if enabled else 0, next_run_at, datetime.now(), schedule_id),
            )
            conn.commit()
        finally:
            conn.close()

    def mark_collection_schedule_triggered(
        self,
        schedule_id: str,
        *,
        job_id: str,
        next_run_at: Optional[str],
    ) -> None:
        conn = self._get_connection()
        try:
            conn.execute(
                """
                UPDATE collection_schedules
                SET last_triggered_at = ?,
                    last_job_id = ?,
                    next_run_at = ?,
                    updated_at = ?
                WHERE schedule_id = ?
                """,
                (datetime.now(), job_id, next_run_at, datetime.now(), schedule_id),
            )
            conn.commit()
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
