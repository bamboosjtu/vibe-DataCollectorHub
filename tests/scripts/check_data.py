import sqlite3
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

conn = sqlite3.connect(PROJECT_ROOT / 'data' / 'collector.db')
conn.row_factory = sqlite3.Row
cursor = conn.execute("SELECT id, plugin_id, data FROM raw_data WHERE plugin_id='rss_news' ORDER BY id DESC LIMIT 2")
for row in cursor.fetchall():
    data = json.loads(row['data'])
    print(f'ID: {row["id"]}')
    print(f'Title: {data.get("title", "N/A")}')
    print(f'URL: {data.get("url", "N/A")}')
    print('---')
conn.close()
