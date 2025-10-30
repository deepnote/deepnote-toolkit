"""Action execution registry using singledispatch.

This module provides a registry for executing runtime actions using
Python's singledispatch for clean polymorphic dispatch without large
if/elif chains.
"""

import os
import sys
from functools import singledispatch
from pathlib import Path
from typing import List, Sequence

from deepnote_core.runtime.types import (
    EnableJupyterTerminalsAction,
    ExtraServerSpec,
    JupyterServerSpec,
    PythonLSPSpec,
    RuntimeAction,
    StreamlitSpec,
)

from .context import ExecutionContext, ExecutionResult, ProcessHandle


@singledispatch
def execute_action(action: RuntimeAction, context: ExecutionContext) -> ExecutionResult:
    """Execute a runtime action based on its type.

    This is the main dispatch function that routes to specific handlers
    based on the action type using Python's singledispatch.

    Args:
        action: The runtime action to execute
        context: The execution context providing environment and utilities

    Returns:
        ExecutionResult with process handle and metadata

    Raises:
        TypeError: If no handler is registered for the action type
    """
    raise TypeError(f"Unsupported action type: {type(action).__name__}")


@execute_action.register
def _execute_jupyter_terminals(
    action: EnableJupyterTerminalsAction, context: ExecutionContext
) -> ExecutionResult:
    """Enable Jupyter terminals extension.

    This is an idempotent action that enables the terminals extension.
    It doesn't start a long-running process.

    Args:
        action: EnableJupyterTerminalsAction specification
        context: Execution context

    Returns:
        ExecutionResult with success=True, is_long_running=False
    """
    context.check_dependency(
        "jupyter_server_terminals", "pip install deepnote-toolkit[server]"
    )

    # Build command to enable the extension
    argv = [
        context.python_executable(),
        "-m",
        "jupyter",
        "server",
        "extension",
        "enable",
        "jupyter_server_terminals",
    ]

    # Use run_one_off for this idempotent command
    exit_code = context.run_one_off(argv)

    if exit_code != 0:
        context.logger.warning(
            f"Failed to enable Jupyter terminals (exit code: {exit_code})"
        )
        return ExecutionResult(
            success=False,
            is_long_running=False,
            message=f"Failed to enable Jupyter terminals (exit code: {exit_code})",
        )

    context.logger.info("Jupyter terminals extension enabled")
    return ExecutionResult(
        success=True, is_long_running=False, message="Terminals enabled"
    )


@execute_action.register
def _execute_jupyter_server(
    action: JupyterServerSpec, context: ExecutionContext
) -> ExecutionResult:
    """Execute Jupyter server action.

    Args:
        action: JupyterServerSpec with server configuration
        context: Execution context

    Returns:
        ExecutionResult with long-running process handle
    """
    context.check_dependency("jupyter_server", "pip install deepnote-toolkit[server]")

    # Build the command - use explicit Python invocation to ensure it works
    # We use -c to import and launch the server app directly
    argv = [
        context.python_executable(),
        "-m",
        "jupyter",
        "server",
        "--ip",
        action.host,
        "--port",
        str(action.port),
    ]

    if action.allow_root:
        argv.append("--allow-root")

    if action.no_browser:
        argv.append("--no-browser")

    # Add any extra arguments
    if action.extra_args:
        argv.extend(action.extra_args)

    # Log warning for security
    token_warnings = [
        "--ServerApp.token=''",
        "--ServerApp.token=",
        "--NotebookApp.token=''",
        "--NotebookApp.token=",
    ]
    if any(arg for arg in argv if any(warn in arg for warn in token_warnings)):
        context.logger.warning(
            "⚠️  Running Jupyter without token authentication is insecure!"
        )

    # Start the server
    context.logger.info(f"Starting Jupyter server on {action.host}:{action.port}")

    env_override = {}

    if os.environ.get("DEEPNOTE_ENFORCE_PIP_CONSTRAINTS", False) in ("on", "1", "true"):
        # https://deepnote.com/docs/custom-environment#package-installation
        env_override["PIP_CONSTRAINT"] = (
            f"https://tk.deepnote.com/constraints{sys.version_info[0]}.{sys.version_info[1]}.txt"
        )

    if "DEEPNOTE_JUPYTER_TOKEN" in os.environ:
        env_override["JUPYTER_TOKEN"] = os.environ["DEEPNOTE_JUPYTER_TOKEN"]

    proc = context.spawn(argv, env_override=env_override)

    return ExecutionResult(process=proc, is_long_running=True, success=True)


@execute_action.register
def _execute_python_lsp(
    action: PythonLSPSpec, context: ExecutionContext
) -> ExecutionResult:
    """Execute Python LSP server.

    Args:
        action: PythonLSPSpec with LSP configuration
        context: Execution context

    Returns:
        ExecutionResult with long-running process handle
    """
    context.check_dependency("pylsp", "pip install 'python-lsp-server[all]'")

    # Build command: python -m pylsp --tcp --host <host> --port <port>
    argv = [
        context.python_executable(),
        "-m",
        "pylsp",
        "--tcp",
        "--host",
        action.host,
        "--port",
        str(action.port),
    ]

    if action.verbose:
        argv.append("-v")

    # Start the LSP server
    context.logger.info(f"Starting Python LSP server on {action.host}:{action.port}")
    proc = context.spawn(argv)

    return ExecutionResult(process=proc, is_long_running=True, success=True)


@execute_action.register
def _execute_streamlit(
    action: StreamlitSpec, context: ExecutionContext
) -> ExecutionResult:
    """Execute Streamlit application.

    Args:
        action: StreamlitSpec with app configuration
        context: Execution context

    Returns:
        ExecutionResult with long-running process handle
    """
    context.check_dependency("streamlit", "pip install streamlit")

    # Build command: python -m streamlit run <script> [--server.port <port>] [args...]
    argv = [
        context.python_executable(),
        "-m",
        "streamlit",
        "run",
        action.script,
    ]

    # Add port if specified
    if action.port is not None:
        argv.extend(["--server.port", str(action.port)])

    # Add any additional arguments
    if action.args:
        argv.extend(action.args)

    # Extract directory from script path for working directory
    cwd = None
    if action.script:
        script_path = Path(action.script)
        parent = script_path.parent
        # Only set cwd if there's an actual directory component
        if parent != Path("."):
            cwd = str(parent)

    # Start the Streamlit app
    context.logger.info(f"Starting Streamlit app: {action.script}")
    proc = context.spawn(argv, env_override=None, cwd=cwd)

    return ExecutionResult(process=proc, is_long_running=True, success=True)


@execute_action.register
def _execute_extra_server(
    action: ExtraServerSpec, context: ExecutionContext
) -> ExecutionResult:
    """Execute custom server command.

    Args:
        action: ExtraServerSpec with custom command
        context: Execution context

    Returns:
        ExecutionResult with long-running process handle
    """
    # Handle environment variables if present
    if action.env:
        # For pip context, we can pass env_override
        # In installer context, env_override is applied via env var prefixes
        merged_env = context.env.copy()
        merged_env.update(action.env)
        proc = context.spawn(action.command, env_override=merged_env)
    else:
        # No special environment
        proc = context.spawn(action.command)

    context.logger.info(f"Starting extra server: {' '.join(action.command)}")
    return ExecutionResult(process=proc, is_long_running=True, success=True)


class ActionExecutor:
    """Unified executor for runtime actions using the registry."""

    def __init__(self, context: ExecutionContext):
        """Initialize the executor with an execution context.

        Args:
            context: The execution context to use for all actions
        """
        self.context = context
        self.processes: List[ProcessHandle] = []

    def execute_all(self, actions: Sequence[RuntimeAction]) -> List[ProcessHandle]:
        """Execute all actions and return long-running process handles.

        Args:
            actions: Sequence of runtime actions to execute

        Returns:
            List of process handles for long-running processes
        """
        for action in actions:
            result = self.execute_action(action)
            if result.is_long_running and result.process:
                self.processes.append(result.process)

        return list(self.processes)

    def execute_action(self, action: RuntimeAction) -> ExecutionResult:
        """Execute a single action.

        Args:
            action: The action to execute

        Returns:
            ExecutionResult with process handle and status
        """
        return execute_action(action, self.context)

    def cleanup(self) -> None:
        """Terminate all managed processes gracefully."""
        import subprocess

        for proc in self.processes:
            try:
                # First try graceful termination
                proc.terminate()

                # Wait up to 5 seconds for process to exit
                try:
                    exit_code = proc.wait(timeout=5.0)
                    self.context.logger.debug(
                        f"Process terminated gracefully with code {exit_code}"
                    )
                except subprocess.TimeoutExpired:
                    # Process didn't exit, escalate to kill
                    self.context.logger.warning(
                        "Process did not terminate gracefully, forcing kill"
                    )
                    proc.kill()

                    # Wait briefly for kill to take effect
                    try:
                        proc.wait(timeout=2.0)
                        self.context.logger.debug("Process killed successfully")
                    except subprocess.TimeoutExpired:
                        self.context.logger.error(
                            "Failed to kill process - may be zombied"
                        )

            except Exception:
                # Log exception with automatic stack trace
                self.context.logger.exception("Error terminating process")

        # Clear the list after attempting cleanup for all processes
        self.processes.clear()
