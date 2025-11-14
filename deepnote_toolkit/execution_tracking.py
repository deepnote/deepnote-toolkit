"""
This module provides execution tracking for Jupyter notebook cells.
It monitors execution start/end events, duration, and publishes metadata to help
debug stuck executions and improve observability.
"""

import time
from typing import Any, Dict, Optional

from IPython.core.interactiveshell import ExecutionInfo, ExecutionResult

from .ipython_utils import publish_execution_metadata
from .logging import LoggerManager


class ExecutionTracker:
    """Tracks execution state of notebook cells."""

    def __init__(self):
        self.logger = LoggerManager().get_logger()
        self.current_execution: Optional[Dict[str, Any]] = None
        self.execution_count = 0

    def on_pre_execute(self, info: ExecutionInfo) -> None:
        """
        Called before executing a cell.
        Logs execution start and tracks execution metadata.
        """
        self.execution_count += 1
        cell_preview = info.raw_cell[:100] if info.raw_cell else "<empty>"
        cell_id = hash(info.raw_cell) if info.raw_cell else 0

        self.current_execution = {
            "cell_id": cell_id,
            "code_preview": cell_preview,
            "start_time": time.time(),
            "execution_count": self.execution_count,
        }

        self.logger.info(
            "EXEC_START | count=%d | cell_id=%s | preview=%s",
            self.execution_count,
            cell_id,
            cell_preview[:50].replace("\n", "\\n"),
        )

    def on_post_execute(self, result: ExecutionResult) -> None:
        """
        Called after executing a cell.
        Logs execution end, duration, and success status.
        Publishes execution metadata for webapp consumption.
        """
        if not self.current_execution:
            self.logger.warning("EXEC_END called without matching EXEC_START")
            return

        duration = time.time() - self.current_execution["start_time"]
        success = result.error_in_exec is None
        error_name = (
            type(result.error_in_exec).__name__ if result.error_in_exec else None
        )

        self.logger.info(
            "EXEC_END | count=%d | duration=%.2fs | success=%s%s",
            self.current_execution["execution_count"],
            duration,
            success,
            f" | error={error_name}" if error_name else "",
        )

        # Publish metadata to webapp
        try:
            publish_execution_metadata(
                execution_count=self.current_execution["execution_count"],
                duration=duration,
                success=success,
                error_type=error_name,
            )
        except Exception as e:  # pylint: disable=broad-exception-caught
            self.logger.error("Failed to publish execution metadata: %s", e)

        self.current_execution = None

    def on_pre_run_cell(self, info: ExecutionInfo) -> None:
        """Called before running a cell (before pre_execute)."""
        cell_preview = info.raw_cell[:30] if info.raw_cell else "<empty>"
        self.logger.debug(
            "PRE_RUN | preview=%s", cell_preview.replace("\n", "\\n")
        )

    def on_post_run_cell(self, result: ExecutionResult) -> None:
        """Called after running a cell (after post_execute)."""
        self.logger.debug("POST_RUN | exec_count=%s", result.execution_count)


# Global instance
_execution_tracker: Optional[ExecutionTracker] = None


def setup_execution_tracking() -> None:
    """
    Set up IPython event handlers for execution tracking.
    This should be called during runtime initialization.
    """
    global _execution_tracker  # pylint: disable=global-statement

    try:
        from IPython import get_ipython

        ip = get_ipython()
        if ip is None:
            LoggerManager().get_logger().warning(
                "IPython instance not available, skipping execution tracking setup"
            )
            return

        _execution_tracker = ExecutionTracker()

        # Register event handlers
        ip.events.register("pre_execute", _execution_tracker.on_pre_execute)
        ip.events.register("post_execute", _execution_tracker.on_post_execute)
        ip.events.register("pre_run_cell", _execution_tracker.on_pre_run_cell)
        ip.events.register("post_run_cell", _execution_tracker.on_post_run_cell)

        LoggerManager().get_logger().info(
            "Execution tracking initialized successfully"
        )

    except Exception as e:  # pylint: disable=broad-exception-caught
        LoggerManager().get_logger().error(
            "Failed to set up execution tracking: %s", e
        )
