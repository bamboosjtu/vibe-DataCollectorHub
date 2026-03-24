"""
Test script for RSS News Plugin
"""

import asyncio
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from core.plugin_manager import PluginManager
from core.pipeline import DataPipeline
from storage.sqlite_store import SQLiteStore


async def test_rss():
    print('=' * 60)
    print(' Testing RSS News Plugin')
    print('=' * 60)

    # Init storage
    store = SQLiteStore(db_path=str(PROJECT_ROOT / 'data' / 'collector.db'))
    store.init_schema()

    # Discover plugins
    manager = PluginManager(plugins_dir=str(PROJECT_ROOT / 'plugins'))
    discovered = manager.discover_plugins()
    print(f'\nDiscovered {len(discovered)} plugin(s):')
    for meta in discovered:
        print(f'  - {meta.plugin_id}: {meta.description}')

    # Create pipeline
    pipeline = DataPipeline(store)

    # Create RSS adapter
    adapter = manager.create_adapter('rss_news', config={'max_items': 5})
    if not adapter:
        print('\n[ERROR] Failed to create RSS adapter')
        return

    print(f'\n[OK] Adapter created: {adapter.name}')
    print(f'[OK] Config: max_items={adapter.config.get("max_items", 50)}')

    # Execute plugin
    print('\n[Pipeline] Executing RSS plugin...')
    result = await pipeline.process_plugin(adapter, incremental=True)

    print(f'\n[OK] Execution completed:')
    print(f'  - Items fetched: {result["items_fetched"]}')
    print(f'  - Raw saved: {result["raw_saved"]}')
    print(f'  - Normalized saved: {result["normalized_saved"]}')
    print(f'  - Success: {result["success"]}')

    # Verify data
    print('\n' + '=' * 60)
    print(' Verifying Data in Database')
    print('=' * 60)

    conn = store._get_connection()

    # Query normalized data
    cursor = conn.execute('''
        SELECT id, event_type, event_source, payload, unique_key, created_at
        FROM normalized_data
        WHERE plugin_id = 'rss_news'
        ORDER BY id DESC LIMIT 3
    ''')
    rows = cursor.fetchall()

    import json
    print(f'\n[Normalized Data] Found {len(rows)} record(s):')
    for row in rows:
        payload = json.loads(row['payload']) if row['payload'] else {}
        title = payload.get('title', 'N/A')
        title_display = title[:60] + '...' if len(title) > 60 else title
        print(f'\n  ID: {row["id"]}')
        print(f'  Event Type: {row["event_type"]}')
        print(f'  Event Source: {row["event_source"]}')
        print(f'  Title: {title_display}')
        print(f'  URL: {payload.get("url", "N/A")[:50]}...')
        print(f'  Unique Key: {row["unique_key"][:16]}...')

    store.close()
    print('\n' + '=' * 60)
    print(' RSS Plugin Test Complete!')
    print('=' * 60)


if __name__ == '__main__':
    asyncio.run(test_rss())
