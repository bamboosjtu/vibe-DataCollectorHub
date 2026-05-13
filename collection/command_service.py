from __future__ import annotations

from typing import Any

from storage.sqlite_store import SQLiteStore


class CommandBatchService:
    """Small storage-backed service for collection batch orchestration."""

    def __init__(self, store: SQLiteStore):
        self.store = store

    def create_collection_batch(self, **batch: Any) -> dict[str, Any]:
        return self.store.create_collection_batch(**batch)

    def create_collection_command(self, **command: Any) -> dict[str, Any]:
        return self.store.create_collection_command(**command)

    def list_pending_commands(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        return self.store.list_pending_commands(batch_id=batch_id)

    def mark_command_running(
        self,
        command_run_id: str,
        *,
        downloader_job_id: str | None = None,
        scope_snapshot: dict[str, Any] | None = None,
    ) -> None:
        self.store.mark_command_running(
            command_run_id,
            downloader_job_id=downloader_job_id,
            scope_snapshot=scope_snapshot,
        )

    def mark_command_succeeded(
        self,
        command_run_id: str,
        *,
        request_count: int = 0,
        raw_record_count: int = 0,
        success_request_count: int = 0,
        failed_request_count: int = 0,
        error_count: int = 0,
        result_summary: dict[str, Any] | None = None,
    ) -> None:
        self.store.mark_command_succeeded(
            command_run_id,
            request_count=request_count,
            raw_record_count=raw_record_count,
            success_request_count=success_request_count,
            failed_request_count=failed_request_count,
            error_count=error_count,
            result_summary=result_summary,
        )

    def mark_command_failed(
        self,
        command_run_id: str,
        *,
        error: str,
        error_count: int = 1,
        result_summary: dict[str, Any] | None = None,
    ) -> None:
        self.store.mark_command_failed(
            command_run_id,
            error=error,
            error_count=error_count,
            result_summary=result_summary,
        )
