"""Run a notebook in Deepnote Cloud through the public API."""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Mapping
from typing import Any

import requests

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

Sleep = Callable[[float], None]

MAX_TRANSIENT_POLL_FAILURES = 5


class DeepnoteCloudRunner:
    """Run an existing notebook directly through the Deepnote public API.

    The token comes from `token`, `token_provider` or the `DEEPNOTE_TOKEN`
    environment variable, and is sent to `base_url`. A token provider is called for
    every request, which lets a long-lived process use short-lived credentials.
    `credentials` replaces all three with one provider of the token and its origin.

    `storage_mode="readonly"` keeps the run from changing the project's stored
    files. None leaves the choice to the API, which allows writes.

    The outputs can arrive after the run finishes. `snapshot_timeout` is how many
    seconds to wait for them. A result whose `snapshot_status` is still `pending`
    has none because that wait ran out.
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
        snapshot_timeout: float = 10,
        poll_interval: float = 2,
        session: requests.Session | None = None,
        sleep: Sleep = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ):
        if not notebook_id:
            raise ValueError("notebook_id is required")
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout must be positive and finite")
        if not math.isfinite(snapshot_timeout) or snapshot_timeout < 0:
            raise ValueError("snapshot_timeout must be non-negative and finite")
        if not math.isfinite(poll_interval) or poll_interval <= 0:
            raise ValueError("poll_interval must be positive")
        if credentials is not None and (
            token is not None or token_provider is not None
        ):
            raise ValueError("Pass credentials or a token, not both")
        self.notebook_id = notebook_id
        self.storage_mode = storage_mode
        self.timeout = timeout
        self.snapshot_timeout = snapshot_timeout
        self.poll_interval = poll_interval
        self._client = DeepnoteApiClient(
            credentials or token_credentials(token, token_provider, base_url=base_url),
            session=session,
            request_timeout=min(timeout, 30),
            clock=clock,
        )
        self._sleep = sleep
        self._clock = clock

    def info(self) -> RunnerInfo:
        """Read the notebook's name and input blocks from the public API."""

        notebook = self._client.get_notebook(self.notebook_id)
        return RunnerInfo(
            notebook=notebook.name, inputs=notebook.inputs, run_target="cloud"
        )

    def run(self, inputs: Mapping[str, Any]) -> RunResult:
        """Start a detached run with the given input values and wait for its result."""

        deadline = self._clock() + self.timeout
        run = self._client.create_run(
            self.notebook_id,
            inputs,
            storage_mode=self.storage_mode,
            timeout=self.timeout,
        )
        if self._clock() >= deadline:
            self._timed_out(run)
        run = self._wait_until_finished(run, deadline)
        run = self._settle_snapshot(
            run, min(deadline, self._clock() + self.snapshot_timeout)
        )
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

    def _timed_out(self, run: CloudRun) -> None:
        raise RunnerError(
            f"Deepnote run {run.run_id} did not finish in {self.timeout:g} seconds"
        )

    def _pause(self, deadline: float) -> bool:
        remaining = deadline - self._clock()
        if remaining <= 0:
            return False
        self._sleep(min(self.poll_interval, remaining))
        return self._clock() < deadline

    def _wait_until_finished(self, run: CloudRun, deadline: float) -> CloudRun:
        transient_failures = 0
        while not run.is_finished:
            if not self._pause(deadline):
                self._timed_out(run)
            try:
                run = self._client.get_run(run.run_id, timeout=deadline - self._clock())
                transient_failures = 0
            except RunnerError as error:
                transient_failures += 1
                if (
                    not error.transient
                    or transient_failures > MAX_TRANSIENT_POLL_FAILURES
                ):
                    raise
            if self._clock() >= deadline:
                self._timed_out(run)
        return run

    def _settle_snapshot(self, run: CloudRun, deadline: float) -> CloudRun:
        while run.outputs is None and run.snapshot_status == "pending":
            if not self._pause(deadline):
                break
            try:
                updated = self._client.get_run(
                    run.run_id, timeout=deadline - self._clock()
                )
                if self._clock() >= deadline:
                    break
                run = updated
            except RunnerError as error:
                if not error.transient:
                    raise
        return run
