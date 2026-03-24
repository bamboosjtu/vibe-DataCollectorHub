import sqlite3
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

conn = sqlite3.connect(PROJECT_ROOT / 'data' / 'collector.db')
conn.row_factory = sqlite3.Row
cursor = conn.execute("SELECT id, payload FROM normalized_data WHERE plugin_id='rss_news' ORDER BY id DESC LIMIT 1")
row = cursor.fetchone()
if row:
    payload = json.loads(row['payload'])
    print(f'ID: {row["id"]}')
    print(f'Payload keys: {list(payload.keys())}')
    print(f'Payload content:')
    print(json.dumps(payload, indent=2, ensure_ascii=False))
conn.close()
