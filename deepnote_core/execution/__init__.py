"""Execution abstractions and registry for runtime actions.

This module contains the shared execution protocols and action registry
that can be used by both the CLI (deepnote_toolkit) and installer modules.
"""

from .context import ExecutionContext, ExecutionResult, ProcessHandle
from .registry import ActionExecutor, execute_action

__all__ = [
    "ExecutionContext",
    "ExecutionResult",
    "ProcessHandle",
    "ActionExecutor",
    "execute_action",
]
