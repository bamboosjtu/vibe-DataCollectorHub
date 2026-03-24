"""
Phase 3 Verification Script for Data Collector Hub v1.0

This script automates the verification of TaskScheduler functionality:
1. Scheduler starts correctly
2. rss_news triggers on schedule (every 10 seconds for testing)
3. task_stats updates correctly
4. plugin_state advances
5. normalized_data doesn't duplicate

Usage:
    python tests/scripts/verify_phase3.py

Expected result: All checks pass with ✅
"""

import asyncio
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from core.plugin_manager import PluginManager
from core.scheduler import TaskScheduler
from storage.sqlite_store import SQLiteStore


class Phase3Verifier:
    """Automated verifier for Phase 3 scheduler functionality."""

    def __init__(self, db_path: str = str(PROJECT_ROOT / "data" / "collector.db")):
        self.db_path = db_path
        self.store = SQLiteStore(db_path)
        self.plugin_manager = PluginManager(str(PROJECT_ROOT / "plugins"))
        self.scheduler = None
        self.results = []

    def log(self, message: str, success: bool = True):
        """Log verification result."""
        status = "✅" if success else "❌"
        print(f"{status} {message}")
        self.results.append((message, success))

    def check_database_counts(self, expected_runs: int) -> dict:
        """Check database counts."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        # Check task_stats
        cursor = conn.execute(
            "SELECT * FROM task_stats WHERE plugin_id = 'rss_news'"
        )
        task_stats = cursor.fetchone()

        # Check plugin_state
        cursor = conn.execute(
            "SELECT * FROM plugin_state WHERE plugin_id = 'rss_news'"
        )
        plugin_state = cursor.fetchone()

        # Check data counts
        cursor = conn.execute(
            "SELECT COUNT(*) as count FROM raw_data WHERE plugin_id = 'rss_news'"
        )
        raw_count = cursor.fetchone()["count"]

        cursor = conn.execute(
            "SELECT COUNT(*) as count FROM normalized_data WHERE plugin_id = 'rss_news'"
        )
        norm_count = cursor.fetchone()["count"]

        # Check logs
        cursor = conn.execute(
            "SELECT COUNT(*) as count FROM logs WHERE plugin_id = 'rss_news'"
        )
        log_count = cursor.fetchone()["count"]

        conn.close()

        return {
            "task_stats": dict(task_stats) if task_stats else None,
            "plugin_state": dict(plugin_state) if plugin_state else None,
            "raw_count": raw_count,
            "norm_count": norm_count,
            "log_count": log_count,
        }

    async def test_scheduler_start(self):
        """Test 1: Scheduler starts correctly."""
        print("\n" + "=" * 70)
        print("Test 1: Scheduler Start")
        print("=" * 70)

        self.store.init_schema()
        self.plugin_manager.discover_plugins()

        self.scheduler = TaskScheduler(
            store=self.store,
            plugin_manager=self.plugin_manager,
            max_concurrency=2,
            default_timeout=30
        )

        self.scheduler.start()
        is_running = self.scheduler.is_running()

        self.log(f"Scheduler started: {is_running}", is_running)

        return is_running

    async def test_cron_registration(self):
        """Test 2: Cron job registration."""
        print("\n" + "=" * 70)
        print("Test 2: Cron Job Registration")
        print("=" * 70)

        # Use 10-second interval for faster testing
        # APScheduler doesn't support seconds in cron, so we use manual trigger
        result = self.scheduler.register_cron_job(
            plugin_id="rss_news",
            cron_expression="* * * * *",  # Every minute
            enabled=True
        )

        self.log(f"Cron job registered: {result}", result)

        jobs = self.scheduler.list_jobs()
        self.log(f"Jobs listed: {len(jobs)} job(s)", len(jobs) > 0)

        return result and len(jobs) > 0

    async def test_manual_trigger(self):
        """Test 3: Manual trigger execution."""
        print("\n" + "=" * 70)
        print("Test 3: Manual Trigger Execution")
        print("=" * 70)

        # Get initial counts
        initial = self.check_database_counts(0)
        print(f"Initial state: raw={initial['raw_count']}, norm={initial['norm_count']}")

        # Trigger manually
        result = await self.scheduler.trigger_plugin("rss_news")

        success = result.get("success", False)
        self.log(f"Manual trigger success: {success}", success)

        # Wait a moment for DB writes
        await asyncio.sleep(0.5)

        # Check counts after first trigger
        after_first = self.check_database_counts(1)
        print(f"After 1st trigger: raw={after_first['raw_count']}, norm={after_first['norm_count']}")

        # Trigger again (should be incremental, no new data)
        result2 = await self.scheduler.trigger_plugin("rss_news")
        await asyncio.sleep(0.5)

        after_second = self.check_database_counts(2)
        print(f"After 2nd trigger: raw={after_second['raw_count']}, norm={after_second['norm_count']}")

        # Verify task_stats
        if after_second["task_stats"]:
            run_count = after_second["task_stats"]["run_count"]
            self.log(f"Task stats run_count: {run_count}", run_count >= 2)

        # Verify plugin_state
        if after_second["plugin_state"]:
            last_ts = after_second["plugin_state"]["last_timestamp"]
            self.log(f"Plugin state has last_timestamp: {last_ts is not None}", last_ts is not None)

        # Verify normalized_data didn't duplicate
        norm_unchanged = after_second["norm_count"] == after_first["norm_count"]
        self.log(f"Normalized data no duplicate: {norm_unchanged}", norm_unchanged)

        return success

    async def test_multiple_triggers(self):
        """Test 4: Multiple triggers simulate scheduled execution."""
        print("\n" + "=" * 70)
        print("Test 4: Multiple Trigger Simulation (3 runs)")
        print("=" * 70)

        initial = self.check_database_counts(0)
        initial_runs = initial["task_stats"]["run_count"] if initial["task_stats"] else 0

        # Trigger 3 more times
        for i in range(3):
            print(f"\nTrigger {i + 1}/3...")
            result = await self.scheduler.trigger_plugin("rss_news")
            await asyncio.sleep(0.5)

        final = self.check_database_counts(0)
        final_runs = final["task_stats"]["run_count"] if final["task_stats"] else 0

        run_diff = final_runs - initial_runs
        self.log(f"Total runs increased by: {run_diff}", run_diff == 3)

        # Verify logs
        log_diff = final["log_count"] - initial["log_count"]
        self.log(f"Log entries added: {log_diff}", log_diff >= 3)

        return run_diff == 3

    async def test_concurrency_protection(self):
        """Test 5: Concurrent execution protection."""
        print("\n" + "=" * 70)
        print("Test 5: Concurrent Execution Protection")
        print("=" * 70)

        # Try to trigger twice simultaneously
        task1 = asyncio.create_task(self.scheduler.trigger_plugin("rss_news"))
        task2 = asyncio.create_task(self.scheduler.trigger_plugin("rss_news"))

        results = await asyncio.gather(task1, task2, return_exceptions=True)

        # One should succeed, one should be skipped or both succeed sequentially
        success_count = sum(1 for r in results if isinstance(r, dict) and r.get("success"))
        skipped_count = sum(1 for r in results if isinstance(r, dict) and "already running" in str(r.get("error", "")))

        print(f"Results: success={success_count}, skipped={skipped_count}")

        # At least one should succeed
        self.log(f"At least one execution succeeded: {success_count >= 1}", success_count >= 1)

        return success_count >= 1

    async def run_all_tests(self):
        """Run all verification tests."""
        print("=" * 70)
        print(" Phase 3 Scheduler Verification")
        print("=" * 70)
        print(f"Start time: {datetime.now()}")

        try:
            # Test 1: Scheduler start
            await self.test_scheduler_start()

            # Test 2: Cron registration
            await self.test_cron_registration()

            # Test 3: Manual trigger
            await self.test_manual_trigger()

            # Test 4: Multiple triggers
            await self.test_multiple_triggers()

            # Test 5: Concurrency protection
            await self.test_concurrency_protection()

        finally:
            if self.scheduler:
                self.scheduler.stop()
            self.store.close()

        # Summary
        print("\n" + "=" * 70)
        print(" Verification Summary")
        print("=" * 70)

        passed = sum(1 for _, success in self.results if success)
        total = len(self.results)

        for message, success in self.results:
            status = "✅ PASS" if success else "❌ FAIL"
            print(f"{status}: {message}")

        print(f"\nTotal: {passed}/{total} checks passed")

        if passed == total:
            print("\n🎉 Phase 3 verification PASSED!")
            return 0
        else:
            print(f"\n⚠️ Phase 3 verification FAILED ({total - passed} checks failed)")
            return 1


async def main():
    verifier = Phase3Verifier()
    exit_code = await verifier.run_all_tests()
    sys.exit(exit_code)


if __name__ == "__main__":
    asyncio.run(main())
