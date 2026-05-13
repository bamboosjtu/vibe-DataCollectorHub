from __future__ import annotations

from datetime import datetime
from typing import Any

from collection.command_service import CommandBatchService
from collection.downloader_client import DownloaderClient, create_downloader_client
from collection.scope_selector import CanonicalScopeSelector
from processing.normalizer_runner import NormalizerRunner
from storage.sqlite_store import SQLiteStore


class SyncOrchestrator:
    """Minimal command batch orchestration for downloader sync jobs."""

    def __init__(
        self,
        *,
        store: SQLiteStore,
        downloader_client: DownloaderClient,
        scope_selector: CanonicalScopeSelector | None = None,
    ):
        self.store = store
        self.command_service = CommandBatchService(store)
        self.downloader_client = downloader_client
        self.scope_selector = scope_selector or CanonicalScopeSelector(store)

    @classmethod
    def from_config(
        cls,
        *,
        store: SQLiteStore,
        config: dict[str, Any],
        scope_selector: CanonicalScopeSelector | None = None,
    ) -> "SyncOrchestrator":
        return cls(
            store=store,
            downloader_client=create_downloader_client(config),
            scope_selector=scope_selector,
        )

    def create_batch_with_commands(
        self,
        *,
        batch_id: str,
        batch_key: str,
        commands: list[dict[str, Any]],
        source_system: str = "dcp",
        plugin_id: str = "dcp",
        downloader_name: str = "vibe-downloader-dcp",
        trigger_type: str = "manual",
        metadata_snapshot: dict[str, Any] | None = None,
        config_snapshot: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        batch = self.command_service.create_collection_batch(
            batch_id=batch_id,
            batch_key=batch_key,
            source_system=source_system,
            plugin_id=plugin_id,
            downloader_name=downloader_name,
            trigger_type=trigger_type,
            status="queued",
            command_count=len(commands),
            metadata_snapshot=metadata_snapshot or {},
            config_snapshot=config_snapshot or {"commands": commands},
            started_at=datetime.now(),
        )
        for command in commands:
            self.command_service.create_collection_command(
                batch_id=batch_id,
                source_system=source_system,
                plugin_id=plugin_id,
                downloader_name=downloader_name,
                status="queued",
                **command,
            )
        return batch

    def run_pending_commands(
        self,
        *,
        batch_id: str,
        auto_process: bool = True,
    ) -> dict[str, Any]:
        results: list[dict[str, Any]] = []
        for command in self.command_service.list_pending_commands(batch_id=batch_id):
            results.append(self.run_command(command, auto_process=auto_process))
        return {
            "batch_id": batch_id,
            "commands_run": len(results),
            "results": results,
        }

    def run_command(
        self,
        command: dict[str, Any],
        *,
        auto_process: bool = True,
    ) -> dict[str, Any]:
        scope_items = self.scope_selector.select_scope_items(command.get("scope_selector"))
        try:
            job_id = self.downloader_client.sync(command, scope_items)
            self.command_service.mark_command_running(
                command["command_run_id"],
                downloader_job_id=job_id,
                scope_snapshot={"items": scope_items},
            )
            wait_for_terminal_status = getattr(
                self.downloader_client,
                "wait_for_terminal_status",
                None,
            )
            if callable(wait_for_terminal_status):
                status_payload = wait_for_terminal_status(job_id)
                if status_payload.get("status") in {"failed", "cancelled"}:
                    raise RuntimeError(
                        status_payload.get("error")
                        or f"downloader sync {status_payload.get('status')}"
                    )
            sync_result = self.downloader_client.get_result(job_id)
            if sync_result.get("status") in {"failed", "cancelled"}:
                raise RuntimeError(sync_result.get("error") or "downloader sync failed")

            ingestion_batch = sync_result.get("ingestion_batch")
            ingestion_stats = {}
            if ingestion_batch:
                ingestion_stats = self.store.save_ingestion_batch(ingestion_batch)

            for error in sync_result.get("errors") or []:
                self.store.record_collection_error(
                    batch_id=command["batch_id"],
                    command_run_id=command["command_run_id"],
                    source_system=command.get("source_system", "dcp"),
                    plugin_id=command.get("plugin_id"),
                    downloader_name=command.get("downloader_name"),
                    error_stage=error.get("error_stage") or error.get("stage") or "request",
                    error_type=error.get("error_type") or "DownloaderError",
                    message=error.get("message") or str(error),
                    details=error.get("details") or error,
                    retryable=bool(error.get("retryable")),
                    dataset_key=error.get("dataset_key"),
                    request_id=error.get("request_id"),
                )

            processing_result = {}
            policy = command.get("processing_policy") or {}
            should_process = auto_process and policy.get("auto_process") is True
            if should_process:
                for dataset_key in command.get("dataset_keys") or []:
                    processing_result[dataset_key] = NormalizerRunner(self.store).run(
                        dataset_key=dataset_key,
                        mode=policy.get("mode", "incremental"),
                    )

            request_count = int(
                sync_result.get("request_count")
                or ingestion_stats.get("collection_requests_upserted")
                or 0
            )
            raw_record_count = int(
                sync_result.get("raw_record_count")
                or ingestion_stats.get("raw_events_inserted")
                or 0
            )
            error_count = int(
                sync_result.get("error_count")
                or ingestion_stats.get("collection_errors_inserted")
                or len(sync_result.get("errors") or [])
            )
            self.command_service.mark_command_succeeded(
                command["command_run_id"],
                request_count=request_count,
                raw_record_count=raw_record_count,
                success_request_count=max(request_count - error_count, 0),
                failed_request_count=error_count,
                error_count=error_count,
                result_summary={
                    "downloader_job_id": job_id,
                    "ingestion": ingestion_stats,
                    "processing": processing_result,
                },
            )
            return {
                "command_run_id": command["command_run_id"],
                "status": "succeeded",
                "downloader_job_id": job_id,
                "scope_items": scope_items,
                "ingestion": ingestion_stats,
                "processing": processing_result,
            }
        except Exception as exc:
            self.command_service.mark_command_failed(
                command["command_run_id"],
                error=str(exc),
                result_summary={"scope_items": scope_items},
            )
            self.store.record_collection_error(
                batch_id=command["batch_id"],
                command_run_id=command["command_run_id"],
                source_system=command.get("source_system", "dcp"),
                plugin_id=command.get("plugin_id"),
                downloader_name=command.get("downloader_name"),
                dataset_key=(command.get("dataset_keys") or [None])[0],
                error_stage="orchestration",
                error_type=type(exc).__name__,
                message=str(exc),
                details={"command": command, "scope_items": scope_items},
                retryable=False,
            )
            return {
                "command_run_id": command["command_run_id"],
                "status": "failed",
                "error": str(exc),
                "scope_items": scope_items,
            }
