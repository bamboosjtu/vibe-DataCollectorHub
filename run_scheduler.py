"""
Task Scheduler Runner for Data Collector Hub v1.0

This script starts the scheduler with rss_news plugin running every minute.

Usage:
    python run_scheduler.py

Press Ctrl+C to stop.
"""

import asyncio
import signal
import sys

from core.plugin_manager import PluginManager
from core.paths import DEFAULT_DB_PATH, PLUGINS_DIR
from core.scheduler import TaskScheduler
from storage.sqlite_store import SQLiteStore


# Global flag for graceful shutdown
_running = True


def signal_handler(sig, frame):
    """Handle shutdown signals."""
    global _running
    print("\n[Runner] Received shutdown signal, stopping...")
    _running = False


async def main():
    """Main scheduler runner."""
    global _running

    print("=" * 70)
    print(" Data Collector Hub v1.0 - Task Scheduler")
    print("=" * 70)

    # Initialize storage
    store = SQLiteStore(db_path=DEFAULT_DB_PATH)
    store.init_schema()
    print("[Runner] Database initialized")

    # Initialize plugin manager
    plugin_manager = PluginManager(plugins_dir=PLUGINS_DIR)
    discovered = plugin_manager.discover_plugins()
    registered_count = plugin_manager.save_discovered_plugins(store)
    print(f"[Runner] Discovered {len(discovered)} plugin(s)")
    print(f"[Runner] Registered {registered_count} plugin(s)")

    # Initialize scheduler
    scheduler = TaskScheduler(
        store=store,
        plugin_manager=plugin_manager,
        max_concurrency=2,
        default_timeout=30
    )

    # Start scheduler
    scheduler.start()

    # Register default plugin schedules.
    default_job_count = scheduler.register_default_jobs()
    print(f"[Runner] Registered {default_job_count} default scheduled job(s)")

    # List registered jobs
    jobs = scheduler.list_jobs()
    print(f"\n[Runner] Registered {len(jobs)} job(s):")
    for job in jobs:
        print(f"  - {job['name']}: {job['trigger']}")
        print(f"    Next run: {job['next_run_time']}")

    print("\n" + "=" * 70)
    print(" Scheduler is running. Press Ctrl+C to stop.")
    print("=" * 70)

    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Keep running until shutdown signal
    try:
        while _running:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
    finally:
        # Stop scheduler
        scheduler.stop()
        store.close()
        print("[Runner] Scheduler stopped. Goodbye!")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Runner] Interrupted by user")
        sys.exit(0)
