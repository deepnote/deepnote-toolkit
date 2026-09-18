"""The result of one notebook run, from either runner."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .document import DeepnoteDocument
from .models import NotebookOutput, optional_string
from .outputs import OutputCollection


class RunResult(OutputCollection):
    """The normalized result of `POST /api/run`, for either cloud or local execution."""

    def __init__(self, raw: Mapping[str, Any]):
        self.raw = raw
        self.target = str(raw.get("target", ""))
        self.success = raw.get("success") is True
        self.run_id = optional_string(raw.get("runId"))
        self.status = optional_string(raw.get("status"))
        self.created = raw.get("created") is True
        self.view_url = optional_string(raw.get("viewUrl"))
        self.error = optional_string(raw.get("error"))
        self.snapshot_status = optional_string(raw.get("snapshotStatus"))
        self.snapshot_yaml = optional_string(raw.get("snapshotYaml"))
        self.snapshot = None
        if self.snapshot_yaml:
            try:
                self.snapshot = DeepnoteDocument.parse(self.snapshot_yaml)
            except ValueError:
                pass
        if self.snapshot:
            self.outputs = self.snapshot.outputs
        else:
            snapshot_blocks = raw.get("snapshotBlocks")
            self.outputs = (
                _outputs_from_snapshot_blocks(snapshot_blocks)
                if isinstance(snapshot_blocks, list)
                else _outputs_from_run(raw.get("outputs"))
            )


def _outputs_from_run(value: Any) -> tuple[NotebookOutput, ...]:
    if not isinstance(value, list):
        return ()
    outputs: list[NotebookOutput] = []
    for block in value:
        if not isinstance(block, Mapping):
            continue
        block_id = str(block.get("blockId", ""))
        raw_outputs = block.get("outputs")
        if not isinstance(raw_outputs, list):
            continue
        outputs.extend(
            NotebookOutput(block_id=block_id, block_type=None, raw=output)
            for output in raw_outputs
            if isinstance(output, Mapping)
        )
    return tuple(outputs)


def _outputs_from_snapshot_blocks(value: Any) -> tuple[NotebookOutput, ...]:
    if not isinstance(value, list):
        return ()
    outputs: list[NotebookOutput] = []
    for block in value:
        if not isinstance(block, Mapping):
            continue
        block_id = str(block.get("id", ""))
        block_type = optional_string(block.get("type"))
        raw_outputs = block.get("outputs")
        if not isinstance(raw_outputs, list):
            continue
        outputs.extend(
            NotebookOutput(
                block_id=block_id,
                block_type=block_type,
                raw=output,
            )
            for output in raw_outputs
            if isinstance(output, Mapping)
        )
    return tuple(outputs)
