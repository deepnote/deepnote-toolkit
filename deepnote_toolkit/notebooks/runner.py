"""The interface every notebook runner implements."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from .models import RunnerInfo
from .run_result import RunResult


class RunnerError(RuntimeError):
    """The Deepnote runner was unavailable or rejected a request."""

    def __init__(self, message: str, *, transient: bool = False):
        super().__init__(message)
        self.transient = transient


class Runner(Protocol):
    """Runs one notebook and reports the inputs it accepts."""

    def info(self) -> RunnerInfo:
        """Return the notebook's name and the inputs it accepts."""

    def run(self, inputs: Mapping[str, Any]) -> RunResult:
        """Run the notebook with the given input values and wait for the result."""
