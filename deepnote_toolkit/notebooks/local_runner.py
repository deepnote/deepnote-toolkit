"""Run a notebook through a local `@deepnote/local-runner` sidecar."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .document import DeepnoteDocument
from .models import RunnerInfo
from .run_result import RunResult
from .transport import Transport, UrllibTransport
from .wire import decode_block_outputs, decode_inputs, optional_string


class DeepnoteRunner:
    """One client for a runner configured for Deepnote Cloud or a local kernel."""

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8787",
        *,
        timeout: float = 600,
        transport: Transport | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._transport = transport or UrllibTransport()

    def info(self) -> RunnerInfo:
        """Read the notebook's name and input blocks from the sidecar."""

        payload = self._request("GET", "/api/info")
        return RunnerInfo(
            notebook=str(payload.get("notebook", "Untitled project")),
            inputs=decode_inputs(payload.get("inputs"), name_key="variableName"),
            run_target=str(payload.get("runTarget", "")),
        )

    def run(self, inputs: Mapping[str, Any]) -> RunResult:
        """Run the notebook in the sidecar with the given input values."""

        return _decode_run_result(
            self._request("POST", "/api/run", {"inputs": dict(inputs)})
        )

    def _request(
        self, method: str, path: str, body: Mapping[str, Any] | None = None
    ) -> Mapping[str, Any]:
        return self._transport.request_json(
            method,
            f"{self.base_url}{path}",
            headers={},
            body=body,
            timeout=self.timeout,
        )


def _decode_run_result(payload: Mapping[str, Any]) -> RunResult:
    snapshot = None
    snapshot_yaml = payload.get("snapshotYaml")
    if isinstance(snapshot_yaml, str) and snapshot_yaml:
        try:
            snapshot = DeepnoteDocument.parse(snapshot_yaml)
        except ValueError:
            pass
    return RunResult(
        target=str(payload.get("target", "")),
        success=payload.get("success") is True,
        outputs=(
            snapshot.outputs
            if snapshot
            else decode_block_outputs(payload.get("outputs"), id_key="blockId")
        ),
        run_id=optional_string(payload.get("runId")),
        status=optional_string(payload.get("status")),
        error=optional_string(payload.get("error")),
        view_url=optional_string(payload.get("viewUrl")),
        snapshot=snapshot,
        created=payload.get("created") is True,
    )
