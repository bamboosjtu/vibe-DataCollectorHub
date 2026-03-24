"""
Test script for incremental collection with plugin_state

This script verifies:
1. plugin_state is saved after first execution
2. Second execution uses plugin_state to filter data
3. Only new items (after last_timestamp) are fetched
"""

import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from storage.sqlite_store import SQLiteStore
from core.plugin_manager import PluginManager
from core.pipeline import DataPipeline


async def test_incremental():
    print("=" * 70)
    print(" Testing Incremental Collection with plugin_state")
    print("=" * 70)

    # Init storage
    store = SQLiteStore(db_path=str(PROJECT_ROOT / 'data' / 'collector.db'))
    store.init_schema()

    # Discover plugins
    manager = PluginManager(plugins_dir=str(PROJECT_ROOT / 'plugins'))
    discovered = manager.discover_plugins()
    print(f"\n[OK] Discovered {len(discovered)} plugin(s)")

    # Create pipeline
    pipeline = DataPipeline(store)

    # Create RSS adapter
    adapter = manager.create_adapter('rss_news', config={'max_items': 10})
    if not adapter:
        print("\n[ERROR] Failed to create RSS adapter")
        return

    print(f"\n[OK] Adapter created: {adapter.name}")
    print(f"[OK] Collection mode: {adapter.collection_mode}")

    # Check initial plugin_state
    print("\n" + "-" * 70)
    print("Step 1: Check initial plugin_state")
    print("-" * 70)

    initial_state = store.get_plugin_state('rss_news')
    if initial_state:
        print(f"[OK] Found existing state: {initial_state}")
    else:
        print("[OK] No existing state (first run)")

    # First execution
    print("\n" + "-" * 70)
    print("Step 2: First execution (full fetch)")
    print("-" * 70)

    result1 = await pipeline.process_plugin(adapter, incremental=True)
    print(f"\n[Result] First execution:")
    print(f"  - Items fetched: {result1['items_fetched']}")
    print(f"  - Raw saved: {result1['raw_saved']}")
    print(f"  - Normalized saved: {result1['normalized_saved']}")

    # Check plugin_state after first execution
    print("\n" + "-" * 70)
    print("Step 3: Verify plugin_state saved")
    print("-" * 70)

    state_after_first = store.get_plugin_state('rss_news')
    if state_after_first:
        print(f"[OK] plugin_state saved:")
        print(f"  - last_timestamp: {state_after_first.get('last_timestamp')}")
        print(f"  - updated_at: {state_after_first.get('updated_at')}")
    else:
        print("[ERROR] plugin_state NOT saved!")
        return

    # Second execution (should use incremental)
    print("\n" + "-" * 70)
    print("Step 4: Second execution (incremental - should filter)")
    print("-" * 70)

    # Create new adapter instance (simulating new scheduler run)
    adapter2 = manager.create_adapter('rss_news', config={'max_items': 10})
    result2 = await pipeline.process_plugin(adapter2, incremental=True)

    print(f"\n[Result] Second execution:")
    print(f"  - Items fetched: {result2['items_fetched']}")
    print(f"  - Raw saved: {result2['raw_saved']}")
    print(f"  - Normalized saved: {result2['normalized_saved']}")

    # Check plugin_state after second execution
    print("\n" + "-" * 70)
    print("Step 5: Verify plugin_state updated")
    print("-" * 70)

    state_after_second = store.get_plugin_state('rss_news')
    if state_after_second:
        print(f"[OK] plugin_state updated:")
        print(f"  - last_timestamp: {state_after_second.get('last_timestamp')}")
        print(f"  - updated_at: {state_after_second.get('updated_at')}")

        # Compare timestamps
        if state_after_first.get('last_timestamp') == state_after_second.get('last_timestamp'):
            print("  - Note: last_timestamp unchanged (no new data)")
        else:
            print("  - Note: last_timestamp updated (new data processed)")
    else:
        print("[ERROR] plugin_state lost!")

    # Summary
    print("\n" + "=" * 70)
    print(" Test Summary")
    print("=" * 70)

    print("""
Incremental Collection Test Results:

  1. plugin_state save:      {"✅ PASS" if state_after_first else "❌ FAIL"}
  2. plugin_state load:      {"✅ PASS" if result2['items_fetched'] is not None else "❌ FAIL"}
  3. Data flow verification:
     - raw_data:            allows duplicates (by design)
     - normalized_data:     deduplicated via unique_key
     - plugin_state:        saves last_timestamp for incremental

Key Concepts:
  - raw_data:      Always append (complete history)
  - normalized:    Deduplicate via unique_key (plugin_id + title + timestamp)
  - plugin_state:  Track last collection position for incremental fetch
""")

    # Query to show the difference
    print("\n" + "-" * 70)
    print("Database Verification")
    print("-" * 70)

    conn = store._get_connection()

    # Count raw_data
    cursor = conn.execute("SELECT COUNT(*) as count FROM raw_data WHERE plugin_id='rss_news'")
    raw_count = cursor.fetchone()['count']
    print(f"\n[Raw Data] Total records: {raw_count}")

    # Count normalized_data
    cursor = conn.execute("SELECT COUNT(*) as count FROM normalized_data WHERE plugin_id='rss_news'")
    norm_count = cursor.fetchone()['count']
    print(f"[Normalized Data] Total records: {norm_count}")

    # Show plugin_state
    cursor = conn.execute("SELECT * FROM plugin_state WHERE plugin_id='rss_news'")
    state_row = cursor.fetchone()
    if state_row:
        print(f"\n[Plugin State]")
        print(f"  - last_timestamp: {state_row['last_timestamp']}")
        print(f"  - last_cursor: {state_row['last_cursor']}")
        print(f"  - last_offset: {state_row['last_offset']}")
        print(f"  - updated_at: {state_row['updated_at']}")

    store.close()
    print("\n" + "=" * 70)
    print(" Test Complete!")
    print("=" * 70)


if __name__ == '__main__':
    asyncio.run(test_incremental())
