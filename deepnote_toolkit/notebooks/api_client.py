"""The Deepnote public API operations a runner needs."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, cast

from .api_types import (
    SNAPSHOT_STATUSES,
    TERMINAL_RUN_STATUSES,
    InputValue,
    RunStatus,
    SnapshotStatus,
    StorageMode,
)
from .credentials import CredentialsProvider
from .models import InputBlock, NotebookOutput
from .runner import RunnerError
from .transport import Transport, UrllibTransport
from .wire import decode_block_outputs, decode_inputs, optional_string


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
        transport: Transport | None = None,
        request_timeout: float = 30,
    ):
        self._credentials = credentials
        self._transport = transport or UrllibTransport()
        self._request_timeout = request_timeout

    def get_notebook(self, notebook_id: str) -> CloudNotebook:
        """Read a notebook's name and input blocks."""

        notebook = self._request("GET", f"/v2/notebooks/{notebook_id}").get("notebook")
        if not isinstance(notebook, Mapping):
            raise RunnerError("Deepnote API response did not include a notebook")
        return CloudNotebook(
            name=str(notebook.get("name", "Untitled notebook")),
            inputs=decode_inputs(notebook.get("inputs"), name_key="name"),
        )

    def create_run(
        self,
        notebook_id: str,
        inputs: Mapping[str, Any],
        *,
        storage_mode: StorageMode | None = None,
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
        return _decode_run(self._request("POST", "/v2/runs", body))

    def get_run(self, run_id: str) -> CloudRun:
        """Read a run with the outputs of the notebook it executed."""

        # The blocks delivery holds the executed notebook alone, not the whole project.
        payload = self._request("GET", f"/v2/runs/{run_id}?snapshotDelivery=blocks")
        return _decode_run(payload, run_id=run_id)

    def _request(
        self, method: str, path: str, body: Mapping[str, Any] | None = None
    ) -> Mapping[str, Any]:
        credentials = self._credentials()
        return self._transport.request_json(
            method,
            f"{credentials.api_origin}{path}",
            headers={"Authorization": f"Bearer {credentials.token}"},
            body=body,
            timeout=self._request_timeout,
        )


def _encode_input(name: str, value: Any) -> InputValue:
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
    snapshot_status = run.get("snapshotStatus")
    blocks = run.get("snapshotBlocks")
    error = run.get("error")
    if isinstance(error, Mapping):
        error = error.get("message") or json.dumps(error)
    return CloudRun(
        run_id=run_id,
        status=cast(RunStatus, str(run.get("status", ""))),
        snapshot_status=(
            snapshot_status if snapshot_status in SNAPSHOT_STATUSES else None
        ),
        outputs=(
            decode_block_outputs(blocks, id_key="id")
            if isinstance(blocks, list)
            else None
        ),
        error=str(error) if error is not None else None,
        view_url=optional_string(run.get("viewUrl")),
    )
