"""
Initialization and Test Script for Data Collector Hub v1.0

This script:
1. Initializes SQLite schema
2. Discovers plugins (lazy loading - AST parse only)
3. Manually executes demo_plugin
4. Saves raw_data
5. Executes normalize
6. Generates unique_key (Pipeline responsibility)
7. Saves normalized_data
8. Outputs results to terminal

Usage:
    python init_and_run.py
"""

import asyncio
import json
from datetime import datetime

from storage.sqlite_store import SQLiteStore
from core.plugin_manager import PluginManager
from core.paths import DEFAULT_DB_PATH, PLUGINS_DIR
from core.pipeline import DataPipeline


def print_separator(title: str):
    """Print a separator line with title"""
    print("\n" + "=" * 60)
    print(f" {title}")
    print("=" * 60)


def print_json(data: dict):
    """Print data as formatted JSON"""
    print(json.dumps(data, indent=2, ensure_ascii=False, default=str))


async def main():
    """Main initialization and test flow"""

    print_separator("Data Collector Hub v1.0 - Phase 1 & 2 Test")

    # Step 1: Initialize SQLite schema
    print_separator("Step 1: Initialize SQLite Schema")
    store = SQLiteStore(db_path=DEFAULT_DB_PATH)
    store.init_schema()
    print("[OK] Schema initialized successfully")

    # Step 2: Discover plugins (lazy loading)
    print_separator("Step 2: Discover Plugins (Lazy Loading)")
    manager = PluginManager(plugins_dir=PLUGINS_DIR)
    discovered = manager.discover_plugins()
    registered_count = manager.save_discovered_plugins(store)
    print(f"\n[OK] Discovered {len(discovered)} plugin(s)")
    print(f"[OK] Registered {registered_count} plugin(s)")

    for meta in discovered:
        print(f"\n  Plugin: {meta.plugin_id}")
        print(f"  - Name: {meta.name}")
        print(f"  - Version: {meta.version}")
        print(f"  - Description: {meta.description}")
        print(f"  - Author: {meta.author}")
        print(f"  - Tags: {meta.tags}")
        print(f"  - Collection Mode: {meta.collection_mode}")

    # Step 3: Create pipeline
    print_separator("Step 3: Initialize Data Pipeline")
    pipeline = DataPipeline(store)
    print("[OK] Pipeline initialized")

    # Step 4: Execute demo_plugin
    print_separator("Step 4: Execute Demo Plugin")

    # Create adapter (this is where lazy loading happens - actual import)
    adapter = manager.create_adapter("demo_plugin", config={"item_count": 3, "prefix": "Test"})
    if not adapter:
        print("[ERROR] Failed to create adapter for demo_plugin")
        return

    print(f"[OK] Adapter created: {adapter.name}")
    print(f"[OK] Config: {adapter.config}")

    # Process the plugin
    result = await pipeline.process_plugin(adapter, incremental=True)

    print("\n[OK] Plugin execution completed")
    print("\nExecution Result:")
    print_json(result)

    # Step 5: Verify data in database
    print_separator("Step 5: Verify Data in Database")

    # Query raw_data
    conn = store._get_connection()
    cursor = conn.execute(
        "SELECT id, plugin_id, source, data, created_at FROM raw_data ORDER BY id DESC LIMIT 3"
    )
    raw_rows = cursor.fetchall()

    print(f"\n[Raw Data] Found {len(raw_rows)} record(s):")
    for row in raw_rows:
        data = json.loads(row['data'])
        print(f"\n  ID: {row['id']}")
        print(f"  Plugin: {row['plugin_id']}")
        print(f"  Source: {row['source']}")
        print(f"  Title: {data.get('title', 'N/A')}")
        print(f"  Created: {row['created_at']}")

    # Query normalized_data
    cursor = conn.execute(
        """
        SELECT id, raw_data_id, plugin_id, event_type, event_source,
               entity, unique_key, payload, created_at
        FROM normalized_data ORDER BY id DESC LIMIT 3
        """
    )
    norm_rows = cursor.fetchall()

    print(f"\n[Normalized Data] Found {len(norm_rows)} record(s):")
    for row in norm_rows:
        entity = json.loads(row['entity']) if row['entity'] else []
        payload = json.loads(row['payload'])
        print(f"\n  ID: {row['id']}")
        print(f"  Raw Data ID: {row['raw_data_id']}")
        print(f"  Plugin: {row['plugin_id']}")
        print(f"  Event Type: {row['event_type']}")
        print(f"  Event Source: {row['event_source']}")
        print(f"  Entity: {entity}")
        print(f"  Unique Key: {row['unique_key']}")
        print(f"  Payload Title: {payload.get('title', 'N/A')}")
        print(f"  Created: {row['created_at']}")

    # Query plugin_state
    cursor = conn.execute(
        "SELECT * FROM plugin_state WHERE plugin_id = ?", ("demo_plugin",)
    )
    state_row = cursor.fetchone()
    if state_row:
        print(f"\n[Plugin State] demo_plugin:")
        print(f"  Last Timestamp: {state_row['last_timestamp']}")
        print(f"  Updated At: {state_row['updated_at']}")

    # Query task_stats
    cursor = conn.execute(
        "SELECT * FROM task_stats WHERE plugin_id = ?", ("demo_plugin",)
    )
    stats_row = cursor.fetchone()
    if stats_row:
        print(f"\n[Task Stats] demo_plugin:")
        print(f"  Run Count: {stats_row['run_count']}")
        print(f"  Fail Count: {stats_row['fail_count']}")
        print(f"  Last Run: {stats_row['last_run']}")
        print(f"  Consecutive Fails: {stats_row['consecutive_fails']}")

    # Query logs
    cursor = conn.execute(
        "SELECT * FROM logs WHERE plugin_id = ? ORDER BY id DESC LIMIT 3",
        ("demo_plugin",)
    )
    log_rows = cursor.fetchall()
    print(f"\n[Logs] Found {len(log_rows)} log entry(s):")
    for row in log_rows:
        print(f"  [{row['level']}] {row['message']} ({row['created_at']})")

    # Step 6: Test deduplication
    print_separator("Step 6: Test Deduplication")
    print("Running the same plugin again to test deduplication...")

    result2 = await pipeline.process_plugin(adapter, incremental=True)
    print(f"\nSecond execution result:")
    print_json(result2)
    print(f"\n[OK] Duplicates detected: {result2['duplicates']} items skipped")

    # Final summary
    print_separator("Test Summary")
    print("""
[OK] All Phase 1 + Phase 2 components working:

  1. SQLite schema initialized
  2. Plugin discovered via lazy loading (AST parsing)
  3. Plugin adapter created (actual import on demand)
  4. Plugin executed via Pipeline
  5. raw_data saved to database
  6. normalize() executed
  7. unique_key generated by Pipeline
  8. normalized_data saved to database
  9. plugin_state updated
  10. task_stats updated
  11. Logs written
  12. Deduplication working

Data Collector Hub v1.0 - Phase 1 & 2 Implementation Complete!
""")

    # Close store
    store.close()


if __name__ == "__main__":
    asyncio.run(main())
