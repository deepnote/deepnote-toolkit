"""Value types of the Deepnote public API."""

from __future__ import annotations

from typing import Literal, Union, get_args

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

INPUT_BLOCK_TYPES: frozenset[InputBlockType] = frozenset(get_args(InputBlockType))
TERMINAL_RUN_STATUSES: frozenset[RunStatus] = frozenset(
    {"success", "error", "internal_error", "stopped"}
)
