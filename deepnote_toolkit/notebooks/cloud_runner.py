"""Run a notebook in Deepnote Cloud through the public API."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from typing import Any

from .api_client import CloudRun, DeepnoteApiClient
from .api_types import StorageMode
from .credentials import (
    DEFAULT_API_ORIGIN,
    CredentialsProvider,
    TokenProvider,
    token_credentials,
)
from .models import RunnerInfo
from .run_result import RunResult
from .runner import RunnerError
from .transport import Transport

Sleep = Callable[[float], None]

MAX_TRANSIENT_POLL_FAILURES = 5
SNAPSHOT_SETTLE_ATTEMPTS = 3


class DeepnoteCloudRunner:
    """Run an existing notebook directly through the Deepnote public API.

    The token comes from `token`, `token_provider` or the `DEEPNOTE_TOKEN`
    environment variable, and is sent to `base_url`. A token provider is called for
    every request, which lets a long-lived process use short-lived credentials.
    `credentials` replaces all three with one provider of the token and its origin.

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
        credentials: CredentialsProvider | None = None,
        storage_mode: StorageMode | None = None,
        timeout: float = 600,
        poll_interval: float = 2,
        transport: Transport | None = None,
        sleep: Sleep = time.sleep,
    ):
        if not notebook_id:
            raise ValueError("notebook_id is required")
        if credentials is not None and (
            token is not None or token_provider is not None
        ):
            raise ValueError("Pass credentials or a token, not both")
        self.notebook_id = notebook_id
        self.storage_mode = storage_mode
        self.timeout = timeout
        self.poll_interval = poll_interval
        self._client = DeepnoteApiClient(
            credentials or token_credentials(token, token_provider, base_url=base_url),
            transport=transport,
            request_timeout=min(timeout, 30),
        )
        self._sleep = sleep

    def info(self) -> RunnerInfo:
        """Read the notebook's name and input blocks from the public API."""

        notebook = self._client.get_notebook(self.notebook_id)
        return RunnerInfo(
            notebook=notebook.name, inputs=notebook.inputs, run_target="cloud"
        )

    def run(self, inputs: Mapping[str, Any]) -> RunResult:
        """Start a detached run with the given input values and wait for its result."""

        run = self._client.create_run(
            self.notebook_id, inputs, storage_mode=self.storage_mode
        )
        run = self._settle_snapshot(self._wait_until_finished(run))
        return RunResult(
            target="cloud",
            success=run.status == "success",
            outputs=run.outputs or (),
            run_id=run.run_id,
            status=run.status,
            error=run.error,
            snapshot_status=run.snapshot_status,
            view_url=run.view_url,
        )

    def _wait_until_finished(self, run: CloudRun) -> CloudRun:
        deadline = time.monotonic() + self.timeout
        transient_failures = 0
        while not run.is_finished:
            if time.monotonic() >= deadline:
                raise RunnerError(
                    f"Deepnote run {run.run_id} did not finish in "
                    f"{self.timeout:g} seconds"
                )
            self._sleep(self.poll_interval)
            try:
                run = self._client.get_run(run.run_id)
                transient_failures = 0
            except RunnerError as error:
                transient_failures += 1
                if (
                    not error.transient
                    or transient_failures > MAX_TRANSIENT_POLL_FAILURES
                ):
                    raise
        return run

    def _settle_snapshot(self, run: CloudRun) -> CloudRun:
        # The snapshot can attach shortly after the status turns terminal.
        for _ in range(SNAPSHOT_SETTLE_ATTEMPTS):
            is_pending = run.snapshot_status in (None, "pending")
            if not is_pending or run.outputs is not None:
                break
            self._sleep(self.poll_interval)
            try:
                run = self._client.get_run(run.run_id)
            except RunnerError as error:
                if not error.transient:
                    raise
        return run
