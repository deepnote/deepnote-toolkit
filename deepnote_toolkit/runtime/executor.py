"""Process executor for pip-native CLI environment using action registry."""

import logging
import os
from typing import Dict, List, Optional, Sequence

from deepnote_core.config import prepare_runtime_resources
from deepnote_core.config.models import DeepnoteConfig
from deepnote_core.execution import ActionExecutor, ProcessHandle
from deepnote_core.runtime.types import RuntimeAction

from .execution_context import PipExecutionContext


def _base_env(cfg: DeepnoteConfig) -> Dict[str, str]:
    """Build base environment for child processes."""
    env = os.environ.copy()
    # Ensure real-time output from Python processes
    env["PYTHONUNBUFFERED"] = "1"
    if cfg.paths.log_dir is not None:
        env["DEEPNOTE_LOG_DIR"] = str(cfg.paths.log_dir)
    return env


def _need(mod: str, hint: str) -> None:
    """Check if a module is available, exit with hint if not.

    Legacy function kept for backward compatibility.
    New code should use ExecutionContext.check_dependency.
    """
    try:
        __import__(mod)
    except ImportError:
        raise SystemExit(f"Missing dependency: {mod}. Install: {hint}")


def run_actions_pip(
    cfg: DeepnoteConfig,
    actions: Sequence[RuntimeAction],
    logger: Optional[logging.Logger] = None,
) -> List[ProcessHandle]:
    """
    Execute runtime actions as subprocesses in pip environment.

    Uses the new ActionExecutor with singledispatch registry for
    clean polymorphic dispatch without if/elif chains.

    Args:
        cfg: DeepnoteConfig instance
        actions: Sequence of RuntimeAction specifications
        logger: Optional logger instance

    Returns:
        List of subprocess.Popen instances
    """
    # Set up runtime resources for pip installation using unified interface
    prepare_runtime_resources(cfg, apply_env=True, persist_config=False)

    # Create execution context with resource environment variables
    env = _base_env(cfg)
    context = PipExecutionContext(env=env, logger=logger or logging.getLogger(__name__))

    # Create executor and run all actions
    executor = ActionExecutor(context)
    processes = executor.execute_all(actions)

    return processes
