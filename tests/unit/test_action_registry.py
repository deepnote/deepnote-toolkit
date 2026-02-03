"""Unit tests for action registry and executor."""

import logging
import sys
from unittest import mock

import pytest

from deepnote_core.execution import ActionExecutor, ExecutionResult, execute_action
from deepnote_core.runtime.types import (
    EnableJupyterTerminalsAction,
    ExtraServerSpec,
    JupyterServerSpec,
    PythonLSPSpec,
    StreamlitSpec,
)


class TestActionRegistry:
    """Test the singledispatch action registry."""

    def test_unsupported_action_type(self):
        """Test that unsupported action types raise TypeError."""
        mock_context = mock.Mock()
        unsupported_action = mock.Mock()  # Not a recognized action type

        with pytest.raises(TypeError) as exc:
            execute_action(unsupported_action, mock_context)

        assert "Unsupported action type" in str(exc.value)

    def test_jupyter_terminals_action(self):
        """Test EnableJupyterTerminalsAction execution."""
        action = EnableJupyterTerminalsAction()
        mock_context = mock.Mock()
        mock_context.python_executable.return_value = "python"
        mock_context.env = {"PATH": "/usr/bin"}
        mock_context.logger = logging.getLogger("test")
        mock_context.run_one_off.return_value = 0  # Success

        result = execute_action(action, mock_context)

        assert result.success is True
        assert result.is_long_running is False
        assert result.message is not None
        assert "Terminals enabled" in result.message

        # Check that run_one_off was called with the correct command
        expected_argv = [
            "python",
            "-m",
            "jupyter",
            "server",
            "extension",
            "enable",
            "jupyter_server_terminals",
        ]
        mock_context.run_one_off.assert_called_once_with(expected_argv)
        mock_context.check_dependency.assert_called_once_with(
            "jupyter_server_terminals", "pip install deepnote-toolkit[server]"
        )

    def test_jupyter_terminals_action_failure(self):
        """Test EnableJupyterTerminalsAction with failure."""
        action = EnableJupyterTerminalsAction()
        mock_context = mock.Mock(
            spec=[
                "python_executable",
                "env",
                "logger",
                "check_dependency",
                "run_one_off",
            ]
        )
        mock_context.python_executable.return_value = "python"
        mock_context.env = {"PATH": "/usr/bin"}
        mock_context.logger = logging.getLogger("test")
        mock_context.run_one_off.return_value = 1  # Failure

        result = execute_action(action, mock_context)

        assert result.success is False
        assert result.message is not None
        assert "Failed to enable Jupyter terminals" in result.message
        assert "exit code: 1" in result.message

    def test_jupyter_server_action(self):
        """Test JupyterServerSpec execution."""
        action = JupyterServerSpec(
            host="0.0.0.0",
            port=8888,
            allow_root=True,
            no_browser=True,
            extra_args=["--debug"],
        )

        mock_context = mock.Mock()
        mock_context.python_executable.return_value = "python"
        mock_context.logger = logging.getLogger("test")
        mock_proc = mock.Mock()
        mock_context.spawn.return_value = mock_proc

        with mock.patch.dict(
            "os.environ",
            {
                "DEEPNOTE_ENFORCE_PIP_CONSTRAINTS": "true",
                "DEEPNOTE_JUPYTER_TOKEN": "token",
            },
        ):
            result = execute_action(action, mock_context)

        assert result.success is True
        assert result.is_long_running is True
        assert result.process is mock_proc

        # Verify the command was built correctly - no token in argv
        expected_argv = [
            "python",
            "-m",
            "jupyter",
            "server",
            "--ip",
            "0.0.0.0",
            "--port",
            "8888",
            "--allow-root",
            "--no-browser",
            "--debug",
        ]
        # Verify token is passed via environment, not CLI
        mock_context.spawn.assert_called_once_with(
            expected_argv,
            env_override={
                "PIP_CONSTRAINT": f"https://tk.deepnote.com/constraints{sys.version_info[0]}.{sys.version_info[1]}.txt",
                "JUPYTER_TOKEN": "token",
            },
        )
        mock_context.check_dependency.assert_called_once_with(
            "jupyter_server", "pip install deepnote-toolkit[server]"
        )

    def test_jupyter_server_action_without_pip_constraints(self):
        """Test JupyterServerSpec execution."""
        action = JupyterServerSpec(
            host="0.0.0.0",
            port=8888,
            allow_root=True,
            no_browser=True,
            extra_args=["--debug"],
        )

        mock_context = mock.Mock()
        mock_context.python_executable.return_value = "python"
        mock_context.logger = logging.getLogger("test")
        mock_proc = mock.Mock()
        mock_context.spawn.return_value = mock_proc

        with mock.patch.dict(
            "os.environ",
            {
                "DEEPNOTE_JUPYTER_TOKEN": "token",
            },
        ):
            result = execute_action(action, mock_context)

        assert result.success is True
        assert result.is_long_running is True
        assert result.process is mock_proc

        # Verify the command was built correctly - no token in argv
        expected_argv = [
            "python",
            "-m",
            "jupyter",
            "server",
            "--ip",
            "0.0.0.0",
            "--port",
            "8888",
            "--allow-root",
            "--no-browser",
            "--debug",
        ]
        # Verify token is passed via environment, not CLI
        mock_context.spawn.assert_called_once_with(
            expected_argv,
            env_override={
                "JUPYTER_TOKEN": "token",
            },
        )
        mock_context.check_dependency.assert_called_once_with(
            "jupyter_server", "pip install deepnote-toolkit[server]"
        )

    def test_jupyter_server_without_token_warning(self):
        """Test JupyterServerSpec warns when no token is set."""
        action = JupyterServerSpec(
            host="localhost", port=8888, extra_args=["--NotebookApp.token=''"]
        )

        mock_context = mock.Mock()
        mock_context.python_executable.return_value = "python"
        mock_context.logger = mock.Mock(spec=logging.Logger)
        mock_proc = mock.Mock()
        mock_context.spawn.return_value = mock_proc

        result = execute_action(action, mock_context)

        assert result.success is True
        mock_context.logger.warning.assert_called_once()
        assert "insecure" in mock_context.logger.warning.call_args[0][0]

    def test_jupyter_server_with_root_dir(self):
        """Test JupyterServerSpec with custom root directory."""
        action = JupyterServerSpec(
            host="0.0.0.0",
            port=8888,
            root_dir="/work",
        )

        mock_context = mock.Mock()
        mock_context.python_executable.return_value = "python"
        mock_context.logger = logging.getLogger("test")
        mock_proc = mock.Mock()
        mock_context.spawn.return_value = mock_proc

        result = execute_action(action, mock_context)

        assert result.success is True
        assert result.is_long_running is True

        # Verify the command includes --ServerApp.root_dir
        expected_argv = [
            "python",
            "-m",
            "jupyter",
            "server",
            "--ip",
            "0.0.0.0",
            "--port",
            "8888",
            "--no-browser",
            "--ServerApp.root_dir=/work",
        ]
        mock_context.spawn.assert_called_once_with(expected_argv, env_override={})

    def test_python_lsp_action(self):
        """Test PythonLSPSpec execution."""
        action = PythonLSPSpec(host="localhost", port=2087, verbose=True)

        mock_context = mock.Mock()
        mock_context.python_executable.return_value = "python"
        mock_context.logger = logging.getLogger("test")
        mock_proc = mock.Mock()
        mock_context.spawn.return_value = mock_proc

        result = execute_action(action, mock_context)

        assert result.success is True
        assert result.is_long_running is True
        assert result.process is mock_proc

        expected_argv = [
            "python",
            "-m",
            "pylsp",
            "--tcp",
            "--host",
            "localhost",
            "--port",
            "2087",
            "-v",
        ]
        mock_context.spawn.assert_called_once_with(expected_argv)
        mock_context.check_dependency.assert_called_once_with(
            "pylsp", "pip install 'python-lsp-server[all]'"
        )

    def test_streamlit_action(self):
        """Test StreamlitSpec execution."""
        action = StreamlitSpec(
            script="app.py",
            port=8501,
            args=["--theme.base", "dark"],
        )

        mock_context = mock.Mock()
        mock_context.python_executable.return_value = "python"
        mock_context.logger = logging.getLogger("test")
        mock_proc = mock.Mock()
        mock_context.spawn.return_value = mock_proc

        result = execute_action(action, mock_context)

        assert result.success is True
        assert result.is_long_running is True
        assert result.process is mock_proc

        expected_argv = [
            "python",
            "-m",
            "streamlit",
            "run",
            "app.py",
            "--server.port",
            "8501",
            "--theme.base",
            "dark",
        ]
        mock_context.spawn.assert_called_once_with(
            expected_argv, env_override=None, cwd=None
        )
        mock_context.check_dependency.assert_called_once_with(
            "streamlit", "pip install streamlit"
        )

    def test_streamlit_action_no_port(self):
        """Test StreamlitSpec with auto-assigned port."""
        action = StreamlitSpec(script="app.py")

        mock_context = mock.Mock()
        mock_context.python_executable.return_value = "python"
        mock_context.logger = logging.getLogger("test")
        mock_proc = mock.Mock()
        mock_context.spawn.return_value = mock_proc

        result = execute_action(action, mock_context)

        assert result.success is True

        expected_argv = ["python", "-m", "streamlit", "run", "app.py"]
        mock_context.spawn.assert_called_once_with(
            expected_argv, env_override=None, cwd=None
        )

    def test_extra_server_action(self):
        """Test ExtraServerSpec execution."""
        action = ExtraServerSpec(
            command=["custom-server", "--port", "9000"],
            env={},
        )

        mock_context = mock.Mock()
        mock_context.logger = logging.getLogger("test")
        mock_proc = mock.Mock()
        mock_context.spawn.return_value = mock_proc

        result = execute_action(action, mock_context)

        assert result.success is True
        assert result.is_long_running is True
        assert result.process is mock_proc

        mock_context.spawn.assert_called_once_with(["custom-server", "--port", "9000"])

    def test_extra_server_with_env_pip(self):
        """Test ExtraServerSpec with environment variables in pip context."""
        action = ExtraServerSpec(
            command=["server"],
            env={"API_KEY": "secret", "PORT": "3000"},
        )

        mock_context = mock.Mock(spec=["spawn", "env", "logger"])
        mock_context.env = {"PATH": "/usr/bin"}
        mock_context.logger = logging.getLogger("test")

        mock_proc = mock.Mock()
        mock_context.spawn.return_value = mock_proc

        result = execute_action(action, mock_context)

        assert result.success is True
        assert result.process == mock_proc

        # Check that spawn was called with env_override
        expected_env = {
            "PATH": "/usr/bin",
            "API_KEY": "secret",
            "PORT": "3000",
        }
        mock_context.spawn.assert_called_once_with(
            ["server"], env_override=expected_env
        )

    def test_extra_server_with_env_installer(self):
        """Test ExtraServerSpec with environment variables in installer context."""
        from installer.module.execution_context import InstallerExecutionContext

        action = ExtraServerSpec(
            command=["server"],
            env={"API_KEY": "secret value", "PORT": "3000"},
        )

        mock_venv = mock.Mock()
        mock_proc = mock.Mock()
        mock_proc.poll.return_value = None  # Process is running
        mock_venv.start_server.return_value = mock_proc

        context = InstallerExecutionContext(venv=mock_venv)

        result = execute_action(action, context)

        assert result.success is True
        assert result.process is mock_proc

        # Check that env vars were prefixed to command
        # The spawn method in InstallerExecutionContext will prefix the env vars
        expected_cmd = "API_KEY='secret value' PORT=3000 server"
        mock_venv.start_server.assert_called_once_with(expected_cmd, cwd=None)


class TestActionExecutor:
    """Test the unified ActionExecutor class."""

    def test_executor_initialization(self):
        """Test ActionExecutor initialization."""
        mock_context = mock.Mock()
        executor = ActionExecutor(mock_context)

        assert executor.context is mock_context
        assert executor.processes == []

    def test_execute_all_long_running(self):
        """Test execute_all with long-running processes."""
        mock_context = mock.Mock()
        executor = ActionExecutor(mock_context)

        # Create test actions
        actions = [
            JupyterServerSpec(host="localhost", port=8888),
            PythonLSPSpec(host="localhost", port=2087),
        ]

        # Mock execute_action to return long-running results
        mock_proc1 = mock.Mock()
        mock_proc2 = mock.Mock()

        with mock.patch(
            "deepnote_core.execution.registry.execute_action"
        ) as mock_execute:
            mock_execute.side_effect = [
                ExecutionResult(process=mock_proc1, is_long_running=True),
                ExecutionResult(process=mock_proc2, is_long_running=True),
            ]

            processes = executor.execute_all(actions)

            assert len(processes) == 2
            assert mock_proc1 in processes
            assert mock_proc2 in processes
            assert executor.processes == processes

    def test_execute_all_mixed(self):
        """Test execute_all with mixed long-running and one-off actions."""
        mock_context = mock.Mock()
        executor = ActionExecutor(mock_context)

        actions = [
            EnableJupyterTerminalsAction(),  # One-off
            JupyterServerSpec(host="localhost", port=8888),  # Long-running
        ]

        mock_proc = mock.Mock()

        with mock.patch(
            "deepnote_core.execution.registry.execute_action"
        ) as mock_execute:
            mock_execute.side_effect = [
                ExecutionResult(success=True, is_long_running=False),  # One-off
                ExecutionResult(
                    process=mock_proc, is_long_running=True
                ),  # Long-running
            ]

            processes = executor.execute_all(actions)

            assert len(processes) == 1
            assert mock_proc in processes

    def test_execute_action_single(self):
        """Test executing a single action."""
        mock_context = mock.Mock()
        executor = ActionExecutor(mock_context)

        action = JupyterServerSpec(host="localhost", port=8888)
        mock_proc = mock.Mock()

        with mock.patch(
            "deepnote_core.execution.registry.execute_action"
        ) as mock_execute:
            mock_execute.return_value = ExecutionResult(
                process=mock_proc, is_long_running=True
            )

            result = executor.execute_action(action)

            assert result.process is mock_proc
            assert result.is_long_running is True
            mock_execute.assert_called_once_with(action, mock_context)

    def test_cleanup(self):
        """Test cleanup terminates all processes."""
        mock_context = mock.Mock()
        mock_context.logger = logging.getLogger("test")

        executor = ActionExecutor(mock_context)

        # Add some mock processes
        mock_proc1 = mock.Mock()
        mock_proc2 = mock.Mock()
        executor.processes = [mock_proc1, mock_proc2]

        executor.cleanup()

        mock_proc1.terminate.assert_called_once()
        mock_proc2.terminate.assert_called_once()

    def test_cleanup_with_error(self):
        """Test cleanup handles termination errors gracefully."""
        mock_context = mock.Mock()
        mock_context.logger = mock.Mock(spec=logging.Logger)

        executor = ActionExecutor(mock_context)

        # Add a process that raises on terminate
        mock_proc = mock.Mock()
        mock_proc.terminate.side_effect = Exception("Cannot terminate")
        executor.processes = [mock_proc]

        executor.cleanup()  # Should not raise

        mock_context.logger.exception.assert_called_once()
        assert (
            "Error terminating process" in mock_context.logger.exception.call_args[0][0]
        )
