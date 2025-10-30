"""PipExecutionContext implementation for CLI environment."""

import logging
import os
import subprocess
import sys
from types import MappingProxyType
from typing import Dict, List, Mapping, Optional

from deepnote_core.execution import ProcessHandle


class PipExecutionContext:
    """Execution context for pip/CLI environment."""

    def __init__(
        self,
        env: Optional[Dict[str, str]] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Initialize pip execution context.

        Args:
            env: Environment variables (defaults to os.environ.copy())
            logger: Logger instance (defaults to module logger)
        """
        self._env = env if env is not None else os.environ.copy()
        self._env["PYTHONUNBUFFERED"] = "1"  # Ensure real-time output
        self._logger = logger or logging.getLogger(__name__)

    @property
    def env(self) -> Mapping[str, str]:
        """Effective environment variables for child processes (read-only)."""
        return MappingProxyType(self._env)

    @property
    def logger(self) -> logging.Logger:
        """Logger for observability."""
        return self._logger

    def spawn(
        self,
        argv: List[str],
        env_override: Optional[Dict[str, str]] = None,
        cwd: Optional[str] = None,
    ) -> ProcessHandle:
        """Start a process and return a ProcessHandle.

        Args:
            argv: Command and arguments as a list
            env_override: Optional environment variables to merge with context env
            cwd: Optional working directory for the process

        Returns:
            ProcessHandle (deepnote_core.execution.ProcessHandle): Handle to the
            spawned process, which in this implementation is a subprocess.Popen instance
        """
        # Merge environment if override provided
        effective_env = self._env
        if env_override:
            effective_env = self._env.copy()
            effective_env.update(env_override)

        self._logger.debug(f"Spawning process: {argv}")
        # Create new session for better process group management
        proc = subprocess.Popen(
            argv, env=effective_env, cwd=cwd, start_new_session=True
        )

        # Use poll() instead of wait() to avoid blocking
        exit_code = proc.poll()
        if exit_code is not None and exit_code != 0:
            self._logger.warning(
                f"Process failed immediately with exit code {exit_code}: {argv}"
            )

        return proc

    def run_one_off(
        self,
        argv: List[str],
        env_override: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> int:
        """Execute a one-off command using subprocess.run.

        Args:
            argv: Command and arguments as a list
            env_override: Optional environment variables to merge with context env
            timeout: Optional timeout in seconds

        Returns:
            Exit code of the command
        """
        # Merge environment if override provided
        effective_env = self._env
        if env_override:
            effective_env = self._env.copy()
            effective_env.update(env_override)

        self._logger.debug(f"Running one-off command: {argv}")
        try:
            result = subprocess.run(
                argv, env=effective_env, capture_output=True, text=True, timeout=timeout
            )
        except subprocess.TimeoutExpired:
            self._logger.warning(f"Command timed out after {timeout} seconds: {argv}")
            return 124

        if result.returncode != 0:
            self._logger.warning(
                f"Command failed with exit code {result.returncode}: {argv}"
            )
            if result.stderr:
                self._logger.debug(f"stderr: {result.stderr}")

        return result.returncode

    def check_dependency(self, module: str, hint: str) -> None:
        """Check if a module is available using __import__.

        Args:
            module: Module name to check
            hint: User-friendly installation hint

        Raises:
            SystemExit: If module is not available
        """
        try:
            __import__(module)
            self._logger.debug(f"Dependency {module} is available")
        except ImportError:
            self._logger.error(f"Missing dependency: {module}")
            raise SystemExit(f"Missing dependency: {module}. Install: {hint}")

    def python_executable(self) -> str:
        """Return sys.executable for pip environment.

        Returns:
            Path to current Python interpreter
        """
        return sys.executable
