"""Run a notebook in Deepnote Cloud through the public API."""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable, Mapping
from typing import Any, Literal
from urllib.request import Request, urlopen

from .http import OpenUrl, request_json
from .models import InputBlock, RunnerInfo
from .run_result import RunResult
from .runner import RunnerError

TokenProvider = Callable[[], str]
StorageMode = Literal["read_write", "readonly"]
Sleep = Callable[[float], None]

TERMINAL_RUN_STATUSES = frozenset({"success", "error", "internal_error", "stopped"})
DEFAULT_API_ORIGIN = "https://api.deepnote.com"
MAX_TRANSIENT_POLL_FAILURES = 5
SNAPSHOT_SETTLE_ATTEMPTS = 3


class DeepnoteCloudRunner:
    """Run an existing notebook directly through the Deepnote public API.

    The token comes from `token`, `token_provider` or the `DEEPNOTE_TOKEN`
    environment variable. A token provider is called for every request, which lets
    a long-lived process use short-lived credentials.

    `storage_mode="readonly"` keeps the run from changing the project's stored
    files. None leaves the choice to the API, which allows writes.
    """

    def __init__(
        self,
        notebook_id: str,
        *,
        token: str | None = None,
        token_provider: TokenProvider | None = None,
        base_url: str = DEFAULT_API_ORIGIN,
        storage_mode: StorageMode | None = None,
        timeout: float = 600,
        poll_interval: float = 2,
        opener: OpenUrl = urlopen,
        sleep: Sleep = time.sleep,
    ):
        if not notebook_id:
            raise ValueError("notebook_id is required")
        if token is not None and token_provider is not None:
            raise ValueError("Pass token or token_provider, not both")
        self.notebook_id = notebook_id
        self.base_url = base_url.rstrip("/")
        self.storage_mode = storage_mode
        self.timeout = timeout
        self.poll_interval = poll_interval
        self._static_token = token
        self._token_provider = token_provider
        self._open = opener
        self._sleep = sleep

    def info(self) -> RunnerInfo:
        """Read the notebook's name and input blocks from the public API."""

        payload = self._request("GET", f"/v2/notebooks/{self.notebook_id}")
        notebook = payload.get("notebook")
        if not isinstance(notebook, Mapping):
            raise RunnerError("Deepnote API response did not include a notebook")
        raw_inputs = notebook.get("inputs")
        inputs = tuple(
            InputBlock.from_api({**value, "variableName": value["name"]})
            for value in raw_inputs or []
            if isinstance(value, Mapping)
            and isinstance(value.get("name"), str)
            and isinstance(value.get("type"), str)
        )
        return RunnerInfo(
            notebook=str(notebook.get("name", "Untitled notebook")),
            inputs=inputs,
            run_target="cloud",
        )

    def run(self, inputs: Mapping[str, Any]) -> RunResult:
        """Start a detached run with the given input values and wait for its result."""

        body: dict[str, Any] = {
            "notebookId": self.notebook_id,
            "detached": True,
            "inputs": _normalize_cloud_inputs(inputs),
        }
        if self.storage_mode is not None:
            body["detachedRunStorageMode"] = self.storage_mode
        started = self._run_payload(self._request("POST", "/v2/runs", body))
        run_id = _required_run_id(started)
        deadline = time.monotonic() + self.timeout
        current = started
        transient_failures = 0
        while str(current.get("status", "")) not in TERMINAL_RUN_STATUSES:
            if time.monotonic() >= deadline:
                raise RunnerError(
                    f"Deepnote run {run_id} did not finish in {self.timeout:g} seconds"
                )
            self._sleep(self.poll_interval)
            try:
                current = self._get_run(run_id)
                transient_failures = 0
            except RunnerError as error:
                transient_failures += 1
                if (
                    not error.transient
                    or transient_failures > MAX_TRANSIENT_POLL_FAILURES
                ):
                    raise

        # The snapshot can attach shortly after the status turns terminal.
        for _ in range(SNAPSHOT_SETTLE_ATTEMPTS):
            is_pending = current.get("snapshotStatus", "pending") == "pending"
            if not is_pending or isinstance(current.get("snapshotBlocks"), list):
                break
            self._sleep(self.poll_interval)
            try:
                current = self._get_run(run_id)
            except RunnerError as error:
                if not error.transient:
                    raise

        status = str(current.get("status", ""))
        error = current.get("error")
        if isinstance(error, Mapping):
            error = error.get("message") or json.dumps(error)
        return RunResult(
            {
                "target": "cloud",
                "success": status == "success",
                "runId": run_id,
                "status": status,
                "error": str(error) if error is not None else None,
                "snapshotStatus": current.get("snapshotStatus"),
                "snapshotBlocks": current.get("snapshotBlocks"),
                "viewUrl": current.get("viewUrl"),
            }
        )

    def _get_run(self, run_id: str) -> Mapping[str, Any]:
        # The blocks delivery holds the executed notebook alone, not the whole project.
        return self._run_payload(
            self._request("GET", f"/v2/runs/{run_id}?snapshotDelivery=blocks")
        )

    def _request(
        self, method: str, path: str, body: Mapping[str, Any] | None = None
    ) -> Mapping[str, Any]:
        token, api_origin = self._credentials()
        request = Request(
            f"{api_origin}{path}",
            data=json.dumps(body).encode() if body is not None else None,
            method=method,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        return request_json(
            self._open,
            request,
            timeout=min(self.timeout, 30),
            service="Deepnote API",
            origin=api_origin,
        )

    def _credentials(self) -> tuple[str, str]:
        """Return the bearer token and the API origin to send it to."""

        if self._token_provider is not None:
            return self._required_token(self._token_provider()), self.base_url
        if self._static_token is not None:
            return self._required_token(self._static_token), self.base_url
        return self._required_token(os.environ.get("DEEPNOTE_TOKEN")), self.base_url

    @staticmethod
    def _required_token(token: str | None) -> str:
        if not token:
            raise RunnerError("A Deepnote API token is required")
        return token

    @staticmethod
    def _run_payload(payload: Mapping[str, Any]) -> Mapping[str, Any]:
        run = payload.get("run")
        return run if isinstance(run, Mapping) else payload


def _required_run_id(run: Mapping[str, Any]) -> str:
    run_id = run.get("runId") or run.get("id")
    if not isinstance(run_id, str) or not run_id:
        raise RunnerError("Deepnote API response did not include a run id")
    return run_id


def _normalize_cloud_inputs(
    inputs: Mapping[str, Any],
) -> dict[str, str | bool | list[str]]:
    normalized: dict[str, str | bool | list[str]] = {}
    for name, value in inputs.items():
        if isinstance(value, bool):
            normalized[name] = value
        elif isinstance(value, list):
            normalized[name] = [str(item) for item in value]
        else:
            normalized[name] = str(value)
    return normalized
