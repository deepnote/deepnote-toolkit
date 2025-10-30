"""Adapter to execute runtime plans in installer environment using action registry."""

import logging
from typing import List, Optional, Sequence

from deepnote_core.execution import ActionExecutor, ProcessHandle
from deepnote_core.runtime.types import RuntimeAction

from .execution_context import InstallerExecutionContext, VenvProtocol


def run_actions_in_installer_env(
    venv: VenvProtocol,
    actions: Sequence[RuntimeAction],
    logger: Optional[logging.Logger] = None,
) -> List[ProcessHandle]:
    """
    Execute runtime actions in installer environment using VirtualEnvironment.

    Uses the new ActionExecutor with singledispatch registry for
    clean polymorphic dispatch without if/elif chains.

    Args:
        venv: Configured VirtualEnvironment instance
        actions: Sequence of RuntimeAction specifications
        logger: Optional logger instance

    Returns:
        List of ProcessHandle instances for started processes
    """
    # Create installer execution context
    context = InstallerExecutionContext(
        venv=venv,
        env={},  # Installer manages its own environment
        logger=logger or logging.getLogger(__name__),
    )

    # Create executor and run all actions
    executor = ActionExecutor(context)
    processes = executor.execute_all(actions)

    return processes
