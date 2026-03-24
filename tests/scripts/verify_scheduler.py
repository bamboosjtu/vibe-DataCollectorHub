"""
Verify scheduler execution results
"""

import sqlite3
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "data" / "collector.db"

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

print("=" * 70)
print(" Scheduler Verification Report")
print("=" * 70)

# 1. Check task_stats
print("\n[Task Statistics]")
cursor = conn.execute("""
    SELECT plugin_id, run_count, fail_count, last_run, consecutive_fails
    FROM task_stats
    WHERE plugin_id = 'rss_news'
""")
row = cursor.fetchone()
if row:
    print(f"  Plugin: {row['plugin_id']}")
    print(f"  Run Count: {row['run_count']}")
    print(f"  Fail Count: {row['fail_count']}")
    print(f"  Last Run: {row['last_run']}")
    print(f"  Consecutive Fails: {row['consecutive_fails']}")
else:
    print("  No task stats found")

# 2. Check plugin_state
print("\n[Plugin State]")
cursor = conn.execute("""
    SELECT plugin_id, last_timestamp, updated_at
    FROM plugin_state
    WHERE plugin_id = 'rss_news'
""")
row = cursor.fetchone()
if row:
    print(f"  Plugin: {row['plugin_id']}")
    print(f"  Last Timestamp: {row['last_timestamp']}")
    print(f"  Updated At: {row['updated_at']}")
else:
    print("  No plugin state found")

# 3. Check logs
print("\n[Recent Logs]")
cursor = conn.execute("""
    SELECT level, message, created_at
    FROM logs
    WHERE plugin_id = 'rss_news'
    ORDER BY id DESC
    LIMIT 5
""")
rows = cursor.fetchall()
for row in rows:
    print(f"  [{row['level']}] {row['message']} ({row['created_at']})")

# 4. Count raw_data
print("\n[Data Counts]")
cursor = conn.execute("SELECT COUNT(*) as count FROM raw_data WHERE plugin_id='rss_news'")
raw_count = cursor.fetchone()['count']
print(f"  Raw Data: {raw_count} records")

cursor = conn.execute("SELECT COUNT(*) as count FROM normalized_data WHERE plugin_id='rss_news'")
norm_count = cursor.fetchone()['count']
print(f"  Normalized Data: {norm_count} records")

conn.close()

print("\n" + "=" * 70)
print(" Verification Complete")
print("=" * 70)
