"""
Task Scheduler for Data Collector Hub v1.0

Assumptions:
- Single instance scheduler using APScheduler
- Controlled concurrency with Semaphore (default: 2)
- Task timeout protection with asyncio.wait_for (default: 30s)
- Reuses existing DataPipeline for execution
- Updates task_stats and logs via SQLiteStore
- Supports cron scheduling and manual trigger
"""

import asyncio
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from core.base_adapter import BaseAdapter
from core.pipeline import DataPipeline
from core.plugin_manager import PluginManager
from storage.sqlite_store import SQLiteStore


class TaskScheduler:
    """
    Task scheduler with controlled concurrency and timeout protection.

    Features:
    - APScheduler-based cron scheduling
    - Semaphore-based concurrency control
    - asyncio.wait_for timeout protection
    - Manual trigger support
    - Automatic stats and logs update
    """

    def __init__(
        self,
        store: SQLiteStore,
        plugin_manager: PluginManager,
        max_concurrency: int = 2,
        default_timeout: int = 30
    ):
        """
        Initialize task scheduler.

        Args:
            store: SQLite storage instance
            plugin_manager: Plugin manager instance
            max_concurrency: Maximum concurrent tasks (semaphore)
            default_timeout: Default task timeout in seconds
        """
        self.store = store
        self.plugin_manager = plugin_manager
        self.pipeline = DataPipeline(store)
        self.max_concurrency = max_concurrency
        self.default_timeout = default_timeout

        # Concurrency control
        self._semaphore = asyncio.Semaphore(max_concurrency)

        # APScheduler instance
        self._scheduler: Optional[AsyncIOScheduler] = None

        # Track running tasks
        self._running_tasks: Dict[str, asyncio.Task] = {}

    def start(self) -> None:
        """Start the scheduler."""
        if self._scheduler is None:
            self._scheduler = AsyncIOScheduler()
            self._scheduler.start()
            print(f"[Scheduler] Started with max_concurrency={self.max_concurrency}, "
                  f"default_timeout={self.default_timeout}s")

    def stop(self) -> None:
        """Stop the scheduler."""
        if self._scheduler:
            self._scheduler.shutdown(wait=True)
            self._scheduler = None
            print("[Scheduler] Stopped")

    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self._scheduler is not None and self._scheduler.running

    def register_cron_job(
        self,
        plugin_id: str,
        cron_expression: str,
        enabled: bool = True
    ) -> bool:
        """
        Register a cron job for a plugin.

        Args:
            plugin_id: Plugin identifier
            cron_expression: Cron expression (e.g., "*/1 * * * *" for every minute)
            enabled: Whether the job is enabled

        Returns:
            True if registered successfully
        """
        if not self._scheduler:
            print("[Scheduler] Error: Scheduler not started")
            return False

        # Get plugin metadata
        metadata = self.plugin_manager.get_plugin_metadata(plugin_id)
        if not metadata:
            print(f"[Scheduler] Error: Plugin not found: {plugin_id}")
            return False

        # Skip disabled plugins
        plugin = self.store.get_plugin(plugin_id)
        if plugin and not plugin.get("enabled", True):
            print(f"[Scheduler] Skipping disabled plugin: {plugin_id}")
            return False

        # Parse cron expression
        try:
            minute, hour, day, month, day_of_week = cron_expression.split()
            trigger = CronTrigger(
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week
            )
        except ValueError as e:
            print(f"[Scheduler] Error: Invalid cron expression '{cron_expression}': {e}")
            return False

        # Remove existing job if any
        job_id = f"plugin_{plugin_id}"
        existing_job = self._scheduler.get_job(job_id)
        if existing_job:
            existing_job.remove()
            print(f"[Scheduler] Removed existing job for {plugin_id}")

        # Add new job
        if enabled:
            self._scheduler.add_job(
                func=self._execute_plugin_task,
                trigger=trigger,
                id=job_id,
                name=f"Plugin: {plugin_id}",
                args=[plugin_id],
                replace_existing=True
            )
            print(f"[Scheduler] Registered cron job for {plugin_id}: {cron_expression}")

        return True

    def register_default_jobs(self) -> int:
        """Register cron jobs for discovered plugins that declare a default schedule."""
        registered = 0
        for metadata in self.plugin_manager.list_plugins():
            adapter = self.plugin_manager.create_adapter(metadata.plugin_id)
            if not adapter:
                continue

            cron_expression = adapter.get_default_schedule()
            if not cron_expression:
                continue

            if self.register_cron_job(metadata.plugin_id, cron_expression, enabled=True):
                registered += 1

        return registered

    def unregister_job(self, plugin_id: str) -> bool:
        """
        Unregister a cron job.

        Args:
            plugin_id: Plugin identifier

        Returns:
            True if unregistered successfully
        """
        if not self._scheduler:
            return False

        job_id = f"plugin_{plugin_id}"
        existing_job = self._scheduler.get_job(job_id)
        if existing_job:
            existing_job.remove()
            print(f"[Scheduler] Unregistered job for {plugin_id}")
            return True

        return False

    async def trigger_plugin(self, plugin_id: str, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Manually trigger a plugin execution.

        Args:
            plugin_id: Plugin identifier
            config: Optional configuration override

        Returns:
            Execution result
        """
        print(f"[Scheduler] Manual trigger for {plugin_id}")
        return await self._execute_plugin_task(plugin_id, config)

    async def _execute_plugin_task(
        self,
        plugin_id: str,
        config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Execute a plugin task with concurrency control and timeout.

        Args:
            plugin_id: Plugin identifier
            config: Optional configuration override

        Returns:
            Execution result
        """
        # Check if already running
        if plugin_id in self._running_tasks:
            print(f"[Scheduler] Task already running for {plugin_id}, skipping")
            return {
                "plugin_id": plugin_id,
                "success": False,
                "error": "Task already running"
            }

        async with self._semaphore:
            print(f"[Scheduler] Executing task for {plugin_id}")
            start_time = datetime.now()

            try:
                # Create adapter
                adapter = self.plugin_manager.create_adapter(plugin_id, config=config)
                if not adapter:
                    error_msg = f"Failed to create adapter for {plugin_id}"
                    print(f"[Scheduler] {error_msg}")
                    self.store.write_log(plugin_id, "ERROR", error_msg)
                    return {
                        "plugin_id": plugin_id,
                        "success": False,
                        "error": error_msg
                    }

                # Check if plugin is enabled
                plugin_info = self.store.get_plugin(plugin_id)
                if plugin_info and not plugin_info.get("enabled", True):
                    print(f"[Scheduler] Plugin {plugin_id} is disabled, skipping")
                    return {
                        "plugin_id": plugin_id,
                        "success": False,
                        "error": "Plugin disabled"
                    }

                # Execute with timeout
                task_coro = self.pipeline.process_plugin(adapter, incremental=True)
                result = await asyncio.wait_for(
                    task_coro,
                    timeout=self.default_timeout
                )

                elapsed = (datetime.now() - start_time).total_seconds()
                print(f"[Scheduler] Task completed for {plugin_id} in {elapsed:.2f}s: "
                      f"{result.get('items_fetched', 0)} items")

                return result

            except asyncio.TimeoutError:
                elapsed = (datetime.now() - start_time).total_seconds()
                error_msg = f"Task timeout after {elapsed:.2f}s"
                print(f"[Scheduler] {error_msg}: {plugin_id}")

                # Update stats and logs
                self.store.update_task_stats(plugin_id, success=False)
                self.store.write_log(plugin_id, "ERROR", error_msg)

                return {
                    "plugin_id": plugin_id,
                    "success": False,
                    "error": error_msg
                }

            except Exception as e:
                elapsed = (datetime.now() - start_time).total_seconds()
                error_msg = f"Task failed after {elapsed:.2f}s: {e}"
                print(f"[Scheduler] {error_msg}: {plugin_id}")

                # Update stats and logs
                self.store.update_task_stats(plugin_id, success=False)
                self.store.write_log(plugin_id, "ERROR", error_msg)

                return {
                    "plugin_id": plugin_id,
                    "success": False,
                    "error": str(e)
                }

            finally:
                # Remove from running tasks
                self._running_tasks.pop(plugin_id, None)

    def list_jobs(self) -> list:
        """
        List all registered jobs.

        Returns:
            List of job info dictionaries
        """
        if not self._scheduler:
            return []

        jobs = []
        for job in self._scheduler.get_jobs():
            jobs.append({
                "id": job.id,
                "name": job.name,
                "next_run_time": job.next_run_time,
                "trigger": str(job.trigger)
            })
        return jobs

    def get_job_status(self, plugin_id: str) -> Optional[Dict[str, Any]]:
        """
        Get job status for a plugin.

        Args:
            plugin_id: Plugin identifier

        Returns:
            Job status dictionary or None
        """
        if not self._scheduler:
            return None

        job_id = f"plugin_{plugin_id}"
        job = self._scheduler.get_job(job_id)
        if job:
            return {
                "id": job.id,
                "name": job.name,
                "next_run_time": job.next_run_time,
                "trigger": str(job.trigger)
            }
        return None
