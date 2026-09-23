"""The Deepnote public API operations a runner needs."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, TypeVar, cast
from urllib.parse import quote

import requests
from pydantic import BaseModel, ValidationError

from ._schemas import ApiInput, ApiRun, GetRunResponse, NotebookResponse
from .api_types import (
    INPUT_BLOCK_TYPES,
    TERMINAL_RUN_STATUSES,
    InputBlockType,
    InputValue,
    RunStatus,
    SnapshotStatus,
    StorageMode,
)
from .credentials import CredentialsProvider
from .models import InputBlock, NotebookOutput
from .runner import RunnerError
from .transport import request_json
from .wire import decode_block_outputs

Schema = TypeVar("Schema", bound=BaseModel)


@dataclass(frozen=True)
class CloudNotebook:
    """A notebook's name and input blocks."""

    name: str
    inputs: tuple[InputBlock, ...]


@dataclass(frozen=True)
class CloudRun:
    """The state of one run. `outputs` is None until the run's snapshot is stored."""

    run_id: str
    status: RunStatus
    snapshot_status: SnapshotStatus | None
    outputs: tuple[NotebookOutput, ...] | None
    error: str | None

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
        notebook = _validate(NotebookResponse, payload, "notebook").notebook
        return CloudNotebook(
            name=notebook.name,
            inputs=tuple(
                _input_block(value)
                for value in notebook.inputs
                if value.type in INPUT_BLOCK_TYPES
            ),
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
        payload = self._request("POST", "/v2/runs", body, timeout=timeout)
        return _cloud_run(_validate(ApiRun, payload, "run"))

    def get_run(self, run_id: str, *, timeout: float | None = None) -> CloudRun:
        """Read a run with the outputs of the notebook it executed."""

        # The blocks delivery holds the executed notebook alone, not the whole project.
        payload = self._request(
            "GET",
            f"/v2/runs/{quote(run_id, safe='')}?snapshotDelivery=blocks",
            timeout=timeout,
        )
        return _cloud_run(_validate(GetRunResponse, payload, "run").run)

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
        if budget <= 0:
            raise RunnerError("API request deadline expired", transient=True)
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


def _validate(schema: type[Schema], payload: Mapping[str, Any], what: str) -> Schema:
    """Validate an API response, reporting schema failures as `RunnerError`."""

    try:
        return schema(**payload)
    except ValidationError as error:
        raise RunnerError(
            f"Deepnote API returned an invalid {what} response"
        ) from error


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


def _input_block(value: ApiInput) -> InputBlock:
    """Convert validated API input metadata to a notebook input block."""

    return InputBlock(
        variable_name=value.name,
        type=cast(InputBlockType, value.type),
        value=value.value,
        label=value.label,
        options=tuple(value.options),
        multiple=value.multiple,
        min=value.min,
        max=value.max,
        step=value.step,
    )


def _cloud_run(run: ApiRun) -> CloudRun:
    """Convert a validated run and any available snapshot blocks to runner data."""

    return CloudRun(
        run_id=run.run_id,
        status=run.status,
        snapshot_status=run.snapshot_status,
        outputs=(
            decode_block_outputs(run.snapshot_blocks, id_key="id")
            if run.snapshot_blocks is not None
            else None
        ),
        error=run.error,
    )
