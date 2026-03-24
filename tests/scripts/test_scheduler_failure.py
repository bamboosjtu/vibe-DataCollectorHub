"""
Scheduler Failure Scenario Test for Data Collector Hub v1.0

This script tests failure handling:
1. Plugin execution failure
2. Timeout handling
3. fail_count increment
4. consecutive_fails tracking
5. Error logs writing

Usage:
    python tests/scripts/test_scheduler_failure.py
"""

import asyncio
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from core.base_adapter import BaseAdapter, DataItem
from core.plugin_manager import PluginManager
from core.scheduler import TaskScheduler
from storage.sqlite_store import SQLiteStore


class FailingPluginAdapter(BaseAdapter):
    """Test plugin that always fails."""

    name = "failing_plugin"
    version = "1.0.0"
    description = "A plugin that always fails for testing"
    author = "test"
    tags = ["test", "failure"]
    config_schema = {
        "fail_mode": {
            "type": "string",
            "required": False,
            "default": "exception",
            "description": "How to fail: exception, timeout, or return_error"
        }
    }
    dependencies = []
    collection_mode = "full"

    async def fetch(self, **kwargs) -> list:
        """Always fail based on config."""
        fail_mode = self.config.get("fail_mode", "exception")

        if fail_mode == "exception":
            raise Exception("Simulated plugin failure")
        elif fail_mode == "timeout":
            await asyncio.sleep(60)  # Will be caught by timeout
            return []
        elif fail_mode == "return_error":
            return []  # Return empty but we'll check this differently

        return []


class SlowPluginAdapter(BaseAdapter):
    """Test plugin that times out."""

    name = "slow_plugin"
    version = "1.0.0"
    description = "A plugin that takes too long"
    author = "test"
    tags = ["test", "slow"]
    config_schema = {}
    dependencies = []
    collection_mode = "full"

    async def fetch(self, **kwargs) -> list:
        """Sleep longer than timeout."""
        await asyncio.sleep(60)  # Scheduler timeout is 30s
        return []


class FailureTestRunner:
    """Run failure scenario tests."""

    def __init__(self, db_path: str = str(PROJECT_ROOT / "data" / "collector.db")):
        self.db_path = db_path
        self.store = SQLiteStore(db_path)
        self.plugin_manager = PluginManager(str(PROJECT_ROOT / "plugins"))
        self.scheduler = None
        self.results = []

    def log(self, message: str, success: bool = True):
        """Log test result."""
        status = "✅" if success else "❌"
        print(f"{status} {message}")
        self.results.append((message, success))

    def get_plugin_stats(self, plugin_id: str) -> dict:
        """Get plugin statistics from database."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        cursor = conn.execute(
            "SELECT * FROM task_stats WHERE plugin_id = ?",
            (plugin_id,)
        )
        task_stats = cursor.fetchone()

        cursor = conn.execute(
            "SELECT COUNT(*) as count FROM logs WHERE plugin_id = ? AND level = 'ERROR'",
            (plugin_id,)
        )
        error_count = cursor.fetchone()["count"]

        conn.close()

        return {
            "task_stats": dict(task_stats) if task_stats else None,
            "error_count": error_count
        }

    async def test_plugin_failure(self):
        """Test 1: Plugin execution failure handling."""
        print("\n" + "=" * 70)
        print("Test 1: Plugin Execution Failure")
        print("=" * 70)

        # Create failing plugin adapter directly
        adapter = FailingPluginAdapter(config={"fail_mode": "exception"})

        # Register plugin in DB
        self.store.save_plugin(
            plugin_id=adapter.name,
            name=adapter.name,
            version=adapter.version,
            description=adapter.description,
            author=adapter.author,
            tags=adapter.tags,
            config_schema=adapter.config_schema,
            enabled=True
        )

        # Get initial stats
        initial = self.get_plugin_stats("failing_plugin")
        initial_fails = initial["task_stats"]["fail_count"] if initial["task_stats"] else 0
        print(f"Initial fail_count: {initial_fails}")

        # Trigger execution (will fail)
        result = await self.scheduler.trigger_plugin("failing_plugin")
        await asyncio.sleep(0.5)

        print(f"Result: success={result.get('success')}, error={result.get('error', 'N/A')}")

        # Verify failure recorded
        self.log(f"Execution reported failure: {not result.get('success', True)}",
                 not result.get("success", True))

        # Check fail_count incremented
        after = self.get_plugin_stats("failing_plugin")
        after_fails = after["task_stats"]["fail_count"] if after["task_stats"] else 0
        print(f"After fail_count: {after_fails}")

        self.log(f"fail_count incremented: {after_fails > initial_fails}",
                 after_fails > initial_fails)

        # Check consecutive_fails
        consecutive = after["task_stats"]["consecutive_fails"] if after["task_stats"] else 0
        self.log(f"consecutive_fails >= 1: {consecutive >= 1}", consecutive >= 1)

        # Check error log
        self.log(f"Error log written: {after['error_count'] > 0}", after["error_count"] > 0)

        return True

    async def test_consecutive_failures(self):
        """Test 2: Multiple consecutive failures."""
        print("\n" + "=" * 70)
        print("Test 2: Consecutive Failures Tracking")
        print("=" * 70)

        # Get current stats
        before = self.get_plugin_stats("failing_plugin")
        initial_consecutive = before["task_stats"]["consecutive_fails"] if before["task_stats"] else 0
        print(f"Initial consecutive_fails: {initial_consecutive}")

        # Fail 3 more times
        for i in range(3):
            result = await self.scheduler.trigger_plugin("failing_plugin")
            await asyncio.sleep(0.3)
            print(f"  Run {i+1}: success={result.get('success')}")

        # Check consecutive_fails
        after = self.get_plugin_stats("failing_plugin")
        final_consecutive = after["task_stats"]["consecutive_fails"] if after["task_stats"] else 0
        print(f"Final consecutive_fails: {final_consecutive}")

        self.log(f"consecutive_fails increased by 3: {final_consecutive - initial_consecutive >= 3}",
                 final_consecutive - initial_consecutive >= 3)

        return True

    async def test_timeout_handling(self):
        """Test 3: Task timeout handling."""
        print("\n" + "=" * 70)
        print("Test 3: Task Timeout Handling")
        print("=" * 70)

        # Create slow plugin
        adapter = SlowPluginAdapter()

        # Register in DB
        self.store.save_plugin(
            plugin_id=adapter.name,
            name=adapter.name,
            version=adapter.version,
            description=adapter.description,
            author=adapter.author,
            tags=adapter.tags,
            config_schema=adapter.config_schema,
            enabled=True
        )

        # Get initial stats
        initial = self.get_plugin_stats("slow_plugin")
        initial_fails = initial["task_stats"]["fail_count"] if initial["task_stats"] else 0

        print("Triggering slow plugin (should timeout after 30s)...")
        start = datetime.now()

        # Trigger (will timeout)
        result = await self.scheduler.trigger_plugin("slow_plugin")

        elapsed = (datetime.now() - start).total_seconds()
        print(f"Elapsed: {elapsed:.2f}s")

        # Should timeout quickly (scheduler timeout is 30s)
        self.log(f"Timeout occurred quickly (< 35s): {elapsed < 35}", elapsed < 35)

        # Check error message
        error_msg = result.get("error", "")
        has_timeout_error = "timeout" in error_msg.lower()
        self.log(f"Error message indicates timeout: {has_timeout_error}", has_timeout_error)

        # Check fail_count
        after = self.get_plugin_stats("slow_plugin")
        after_fails = after["task_stats"]["fail_count"] if after["task_stats"] else 0
        self.log(f"fail_count incremented: {after_fails > initial_fails}", after_fails > initial_fails)

        return True

    async def test_recovery(self):
        """Test 4: Recovery after failure (consecutive_fails reset)."""
        print("\n" + "=" * 70)
        print("Test 4: Recovery After Failure")
        print("=" * 70)

        # We need a plugin that fails then succeeds
        # For this test, we'll manually update the stats to simulate recovery

        # Get current consecutive_fails for failing_plugin
        before = self.get_plugin_stats("failing_plugin")
        consecutive_before = before["task_stats"]["consecutive_fails"] if before["task_stats"] else 0
        print(f"Current consecutive_fails: {consecutive_before}")

        # Simulate a successful run by updating stats manually
        self.store.update_task_stats("failing_plugin", success=True)
        await asyncio.sleep(0.3)

        # Check consecutive_fails reset
        after = self.get_plugin_stats("failing_plugin")
        consecutive_after = after["task_stats"]["consecutive_fails"] if after["task_stats"] else 0
        print(f"After 'success' consecutive_fails: {consecutive_after}")

        self.log(f"consecutive_fails reset to 0: {consecutive_after == 0}", consecutive_after == 0)

        return True

    async def run_all_tests(self):
        """Run all failure tests."""
        print("=" * 70)
        print(" Phase 3 Scheduler Failure Tests")
        print("=" * 70)
        print(f"Start time: {datetime.now()}")

        # Initialize
        self.store.init_schema()
        self.plugin_manager.discover_plugins()

        self.scheduler = TaskScheduler(
            store=self.store,
            plugin_manager=self.plugin_manager,
            max_concurrency=2,
            default_timeout=30
        )
        self.scheduler.start()

        try:
            # Run tests
            await self.test_plugin_failure()
            await self.test_consecutive_failures()
            await self.test_timeout_handling()
            await self.test_recovery()

        finally:
            self.scheduler.stop()
            self.store.close()

        # Summary
        print("\n" + "=" * 70)
        print(" Failure Test Summary")
        print("=" * 70)

        passed = sum(1 for _, success in self.results if success)
        total = len(self.results)

        for message, success in self.results:
            status = "✅ PASS" if success else "❌ FAIL"
            print(f"{status}: {message}")

        print(f"\nTotal: {passed}/{total} checks passed")

        if passed == total:
            print("\n🎉 Failure handling tests PASSED!")
            return 0
        else:
            print(f"\n⚠️ Some tests failed ({total - passed} checks)")
            return 1


async def main():
    runner = FailureTestRunner()
    exit_code = await runner.run_all_tests()
    sys.exit(exit_code)


if __name__ == "__main__":
    asyncio.run(main())
