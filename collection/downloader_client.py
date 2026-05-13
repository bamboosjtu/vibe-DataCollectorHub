from __future__ import annotations

from typing import Any, Protocol
from urllib import request as urlrequest
import json


class DownloaderClient(Protocol):
    def sync(self, command: dict[str, Any], scope_items: list[dict[str, Any]]) -> str:
        ...

    def get_result(self, job_id: str) -> dict[str, Any]:
        ...


class HttpDownloaderClient:
    """Minimal HTTP client for downloader /sync service."""

    def __init__(self, base_url: str, *, timeout_seconds: int = 60):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

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
                or "http://127.0.0.1:8000/ingestion/v1/batch"
            },
        }
        response = self._post_json("/sync", payload)
        job_id = response.get("job_id") or response.get("downloader_job_id")
        if not job_id:
            raise RuntimeError("downloader /sync response missing job_id")
        return str(job_id)

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

    def get_result(self, job_id: str) -> dict[str, Any]:
        if job_id not in self.jobs:
            raise RuntimeError(f"fake downloader job not found: {job_id}")
        return self.jobs[job_id]
