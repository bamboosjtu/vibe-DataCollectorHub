from __future__ import annotations

import time
from typing import Any, Protocol
from urllib import request as urlrequest
import json


class DownloaderClient(Protocol):
    def sync(self, command: dict[str, Any], scope_items: list[dict[str, Any]]) -> str:
        ...

    def get_status(self, job_id: str) -> dict[str, Any]:
        ...

    def get_result(self, job_id: str) -> dict[str, Any]:
        ...


class HttpDownloaderClient:
    """Minimal HTTP client for downloader /sync service."""

    def __init__(
        self,
        base_url: str,
        *,
        datahub_ingestion_url: str = "http://127.0.0.1:8000/ingestion/v1/batch",
        timeout_seconds: int = 60,
        poll_interval_seconds: float = 0.5,
        poll_timeout_seconds: float = 300,
    ):
        self.base_url = base_url.rstrip("/")
        self.datahub_ingestion_url = datahub_ingestion_url
        self.timeout_seconds = timeout_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self.poll_timeout_seconds = poll_timeout_seconds

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "HttpDownloaderClient":
        return cls(
            str(config["base_url"]),
            datahub_ingestion_url=str(
                config.get("datahub_ingestion_url")
                or "http://127.0.0.1:8000/ingestion/v1/batch"
            ),
            timeout_seconds=int(config.get("timeout_seconds") or 60),
            poll_interval_seconds=float(config.get("poll_interval_seconds") or 0.5),
            poll_timeout_seconds=float(config.get("poll_timeout_seconds") or 300),
        )

    def sync(self, command: dict[str, Any], scope_items: list[dict[str, Any]]) -> str:
        payload = {
            "schema_version": "downloader.sync.request.v1",
            "batch_id": command["batch_id"],
            "command_run_id": command["command_run_id"],
            "command_key": command["command_key"],
            "datasets": command.get("dataset_keys") or [],
            "params": command.get("params") or {},
            "scope_items": scope_items,
            "datahub": {
                "ingestion_url": command.get("ingestion_url")
                or self.datahub_ingestion_url,
                "timeout_seconds": self.timeout_seconds,
            },
        }
        response = self._post_json("/sync", payload)
        job_id = response.get("job_id") or response.get("downloader_job_id")
        if not job_id:
            raise RuntimeError("downloader /sync response missing job_id")
        return str(job_id)

    def get_status(self, job_id: str) -> dict[str, Any]:
        return self._get_json(f"/sync/jobs/{job_id}")

    def wait_for_terminal_status(self, job_id: str) -> dict[str, Any]:
        deadline = time.monotonic() + self.poll_timeout_seconds
        last_status: dict[str, Any] = {}
        while time.monotonic() <= deadline:
            last_status = self.get_status(job_id)
            status = last_status.get("status")
            if status in {"succeeded", "failed", "partial", "cancelled"}:
                return last_status
            time.sleep(self.poll_interval_seconds)
        raise TimeoutError(
            f"downloader job {job_id} did not finish within {self.poll_timeout_seconds} seconds; "
            f"last status={last_status.get('status')!r}"
        )

    def get_result(self, job_id: str) -> dict[str, Any]:
        return self._get_json(f"/sync/jobs/{job_id}/result")

    def _post_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        data = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        req = urlrequest.Request(
            f"{self.base_url}{path}",
            data=data,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urlrequest.urlopen(req, timeout=self.timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))

    def _get_json(self, path: str) -> dict[str, Any]:
        req = urlrequest.Request(
            f"{self.base_url}{path}",
            method="GET",
            headers={"Content-Type": "application/json"},
        )
        with urlrequest.urlopen(req, timeout=self.timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))


class FakeDownloaderClient:
    """Test stub that returns predefined ingestion batch results."""

    def __init__(self, results_by_command_key: dict[str, dict[str, Any]]):
        self.results_by_command_key = results_by_command_key
        self.jobs: dict[str, dict[str, Any]] = {}
        self.sync_calls: list[dict[str, Any]] = []

    def sync(self, command: dict[str, Any], scope_items: list[dict[str, Any]]) -> str:
        job_id = f"fake_job_{command['command_run_id']}"
        self.sync_calls.append({"command": command, "scope_items": scope_items})
        result = self.results_by_command_key.get(command["command_key"])
        if result is None:
            result = self.results_by_command_key.get(command["dataset_keys"][0])
        if result is None:
            result = {
                "status": "failed",
                "error": f"no fake result for command: {command['command_key']}",
            }
        self.jobs[job_id] = result
        return job_id

    def get_status(self, job_id: str) -> dict[str, Any]:
        if job_id not in self.jobs:
            raise RuntimeError(f"fake downloader job not found: {job_id}")
        result = self.jobs[job_id]
        return {
            "schema_version": "downloader.sync.status.v1",
            "job_id": job_id,
            "downloader_job_id": job_id,
            "status": result.get("status", "succeeded"),
            "request_count": result.get("request_count", 0),
            "raw_record_count": result.get("raw_record_count", 0),
            "error_count": result.get("error_count", 0),
            "error": result.get("error"),
        }

    def get_result(self, job_id: str) -> dict[str, Any]:
        if job_id not in self.jobs:
            raise RuntimeError(f"fake downloader job not found: {job_id}")
        return self.jobs[job_id]


def create_downloader_client(config: dict[str, Any]) -> DownloaderClient:
    client_type = str(config.get("type") or "http").lower()
    if client_type == "http":
        return HttpDownloaderClient.from_config(config)
    if client_type == "fake":
        return FakeDownloaderClient(config.get("results_by_command_key") or {})
    raise ValueError(f"Unsupported downloader client type: {client_type}")
