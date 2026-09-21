"""The Deepnote public API operations a runner needs."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import requests
from pydantic import ValidationError

from ._schemas import ApiRun, NotebookResponse
from .api_types import (
    SNAPSHOT_STATUSES,
    TERMINAL_RUN_STATUSES,
    InputValue,
    SnapshotStatus,
    StorageMode,
)
from .credentials import CredentialsProvider
from .models import InputBlock, NotebookOutput
from .runner import RunnerError
from .transport import request_json
from .wire import decode_block_outputs, decode_inputs


@dataclass(frozen=True)
class CloudNotebook:
    """A notebook's name and input blocks."""

    name: str
    inputs: tuple[InputBlock, ...]


@dataclass(frozen=True)
class CloudRun:
    """The state of one run. `outputs` is None until the run's snapshot is stored."""

    run_id: str
    status: str
    snapshot_status: SnapshotStatus | None
    outputs: tuple[NotebookOutput, ...] | None
    error: str | None
    view_url: str | None

    @property
    def is_finished(self) -> bool:
        """Whether the run has reached a final status."""

        return self.status in TERMINAL_RUN_STATUSES


class DeepnoteApiClient:
    """Sends requests to the Deepnote public API and validates what comes back."""

    def __init__(
        self,
        credentials: CredentialsProvider,
        *,
        session: requests.Session | None = None,
        request_timeout: float = 30,
        clock: Callable[[], float] = time.monotonic,
    ):
        self._credentials = credentials
        self._clock = clock
        self._session = session if session is not None else requests.Session()
        self._request_timeout = request_timeout

    def get_notebook(self, notebook_id: str) -> CloudNotebook:
        """Read a notebook's name and input blocks."""

        payload = self._request("GET", f"/v2/notebooks/{quote(notebook_id, safe='')}")
        try:
            notebook = NotebookResponse(**payload).notebook
        except ValidationError as error:
            raise RunnerError(
                "Deepnote API returned an invalid notebook response"
            ) from error
        return CloudNotebook(
            name=notebook.name,
            inputs=decode_inputs(payload["notebook"].get("inputs"), name_key="name"),
        )

    def create_run(
        self,
        notebook_id: str,
        inputs: Mapping[str, Any],
        *,
        storage_mode: StorageMode | None = None,
        timeout: float | None = None,
    ) -> CloudRun:
        """Start a detached run of the whole notebook."""

        body: dict[str, Any] = {
            "notebookId": notebook_id,
            "detached": True,
            "inputs": {
                name: _encode_input(name, value) for name, value in inputs.items()
            },
        }
        if storage_mode is not None:
            body["detachedRunStorageMode"] = storage_mode
        return _decode_run(self._request("POST", "/v2/runs", body, timeout=timeout))

    def get_run(self, run_id: str, *, timeout: float | None = None) -> CloudRun:
        """Read a run with the outputs of the notebook it executed."""

        # The blocks delivery holds the executed notebook alone, not the whole project.
        payload = self._request(
            "GET",
            f"/v2/runs/{quote(run_id, safe='')}?snapshotDelivery=blocks",
            timeout=timeout,
        )
        return _decode_run(payload, run_id=run_id)

    def _request(
        self,
        method: str,
        path: str,
        body: Mapping[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Mapping[str, Any]:
        budget = (
            self._request_timeout
            if timeout is None
            else min(timeout, self._request_timeout)
        )
        deadline = self._clock() + budget
        credentials = self._credentials(timeout=budget)
        remaining = deadline - self._clock()
        if remaining <= 0:
            raise RunnerError(
                "API credentials exhausted the request timeout", transient=True
            )
        return request_json(
            self._session,
            method,
            f"{credentials.api_origin}{path}",
            headers={"Authorization": f"Bearer {credentials.token}"},
            body=body,
            timeout=remaining,
        )


def _encode_input(name: str, value: Any) -> InputValue:
    """Convert a value to the form the runs API accepts, or raise `ValueError`."""

    if isinstance(value, bool):
        return value
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value]
    if value is None or isinstance(value, (Mapping, set, frozenset, bytes)):
        raise ValueError(
            f'Input "{name}" has a {type(value).__name__} value. '
            "Pass text, a number, a boolean or a list of texts."
        )
    return str(value)


def _decode_run(payload: Mapping[str, Any], *, run_id: str | None = None) -> CloudRun:
    nested = payload.get("run")
    run = nested if isinstance(nested, Mapping) else payload
    run_id = run.get("runId") or run.get("id") or run_id
    if not isinstance(run_id, str) or not run_id:
        raise RunnerError("Deepnote API response did not include a run id")
    try:
        parsed = ApiRun(**{**run, "runId": run_id})
    except ValidationError as error:
        raise RunnerError(
            f"Deepnote API returned an invalid run response for {run_id}"
        ) from error
    error = parsed.error
    if isinstance(error, Mapping):
        error = error.get("message") or json.dumps(error)
    return CloudRun(
        run_id=parsed.run_id,
        status=parsed.status,
        snapshot_status=(
            parsed.snapshot_status
            if parsed.snapshot_status in SNAPSHOT_STATUSES
            else None
        ),
        outputs=(
            decode_block_outputs(parsed.snapshot_blocks, id_key="id")
            if parsed.snapshot_blocks is not None
            else None
        ),
        error=str(error) if error is not None else None,
        view_url=parsed.view_url,
    )
