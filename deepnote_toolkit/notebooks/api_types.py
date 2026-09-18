"""Value types of the Deepnote public API."""

from __future__ import annotations

from typing import Literal, Union

InputBlockType = Literal[
    "input-checkbox",
    "input-date",
    "input-date-range",
    "input-file",
    "input-select",
    "input-slider",
    "input-text",
    "input-textarea",
]
RunStatus = Literal[
    "pending", "running", "success", "error", "internal_error", "stopped"
]
SnapshotStatus = Literal["pending", "available", "unavailable"]
StorageMode = Literal["read_write", "readonly"]
InputValue = Union[str, bool, list[str]]

TERMINAL_RUN_STATUSES: frozenset[RunStatus] = frozenset(
    {"success", "error", "internal_error", "stopped"}
)
SNAPSHOT_STATUSES: frozenset[SnapshotStatus] = frozenset(
    {"pending", "available", "unavailable"}
)
