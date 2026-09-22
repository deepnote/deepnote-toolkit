"""The result of one notebook run, from either runner."""

from __future__ import annotations

from dataclasses import dataclass

from .api_types import SnapshotStatus
from .document import DeepnoteDocument
from .models import NotebookOutput
from .outputs import OutputCollection


@dataclass(frozen=True)
class RunResult(OutputCollection):
    """What one run produced, whether it ran in Deepnote Cloud or locally.

    `snapshot_status` is set for cloud runs. `view_url`, `snapshot` and `created`
    are set by the local runner.
    """

    target: str
    success: bool
    outputs: tuple[NotebookOutput, ...] = ()
    run_id: str | None = None
    status: str | None = None
    error: str | None = None
    snapshot_status: SnapshotStatus | None = None
    view_url: str | None = None
    snapshot: DeepnoteDocument | None = None
    created: bool = False
