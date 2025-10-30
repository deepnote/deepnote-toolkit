"""Unit tests for installer.executor module."""

import shlex
from unittest import mock

from deepnote_core.runtime.types import (
    EnableJupyterTerminalsAction,
    ExtraServerSpec,
    JupyterServerSpec,
    PythonLSPSpec,
    StreamlitSpec,
)
from installer.module.executor import run_actions_in_installer_env


class TestRunActionsInInstallerEnv:
    """Test run_actions_in_installer_env function."""

    def test_enable_jupyter_terminals(self):
        """Test enabling Jupyter terminals extension."""
        mock_venv = mock.MagicMock()
        action = EnableJupyterTerminalsAction()

        processes = run_actions_in_installer_env(mock_venv, [action])

        assert processes == []

        # The new implementation checks dependency first, then runs the command
        assert mock_venv.execute.call_count == 2
        mock_venv.execute.assert_any_call('python -c "import jupyter_server_terminals"')
        mock_venv.execute.assert_any_call(
            "python -m jupyter server extension enable jupyter_server_terminals"
        )
        # Verify start_server was not called
        mock_venv.start_server.assert_not_called()

    def test_jupyter_server_basic(self):
        """Test starting basic Jupyter server."""
        mock_venv = mock.MagicMock()
        mock_process = mock.MagicMock()
        mock_venv.start_server.return_value = mock_process

        action = JupyterServerSpec(
            host="0.0.0.0",
            port=8888,
            allow_root=False,
        )

        processes = run_actions_in_installer_env(mock_venv, [action])

        assert len(processes) == 1
        assert processes[0] == mock_process
        expected_cmd = shlex.join(
            [
                "python",
                "-m",
                "jupyter",
                "server",
                "--ip",
                "0.0.0.0",
                "--port",
                "8888",
                "--no-browser",
            ]
        )
        mock_venv.start_server.assert_called_once_with(expected_cmd, cwd=None)

    def test_jupyter_server_with_allow_root(self):
        """Test Jupyter server with allow_root flag."""
        mock_venv = mock.MagicMock()
        mock_process = mock.MagicMock()
        mock_venv.start_server.return_value = mock_process

        action = JupyterServerSpec(
            host="localhost",
            port=8888,
            allow_root=True,
        )

        processes = run_actions_in_installer_env(mock_venv, [action])

        assert len(processes) == 1
        expected_cmd = shlex.join(
            [
                "python",
                "-m",
                "jupyter",
                "server",
                "--ip",
                "localhost",
                "--port",
                "8888",
                "--allow-root",
                "--no-browser",
            ]
        )
        mock_venv.start_server.assert_called_once_with(expected_cmd, cwd=None)

    def test_jupyter_server_with_extra_args(self):
        """Test Jupyter server with extra arguments."""
        mock_venv = mock.MagicMock()
        mock_process = mock.MagicMock()
        mock_venv.start_server.return_value = mock_process

        action = JupyterServerSpec(
            host="0.0.0.0",
            port=8888,
            allow_root=True,
            extra_args=["--debug", "--no-mathjax"],
        )

        processes = run_actions_in_installer_env(mock_venv, [action])

        assert len(processes) == 1
        expected_cmd = shlex.join(
            [
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
                "--no-mathjax",
            ]
        )
        mock_venv.start_server.assert_called_once_with(expected_cmd, cwd=None)

    def test_jupyter_server_no_browser_false(self):
        """Test Jupyter server with no_browser=False."""
        mock_venv = mock.MagicMock()
        mock_process = mock.MagicMock()
        mock_venv.start_server.return_value = mock_process

        action = JupyterServerSpec(
            host="localhost",
            port=9999,
            allow_root=True,
            no_browser=False,
            extra_args=["--debug"],
        )

        processes = run_actions_in_installer_env(mock_venv, [action])

        assert len(processes) == 1
        assert processes[0] == mock_process
        # Should NOT include --no-browser when no_browser=False
        expected_cmd = shlex.join(
            [
                "python",
                "-m",
                "jupyter",
                "server",
                "--ip",
                "localhost",
                "--port",
                "9999",
                "--allow-root",
                "--debug",
            ]
        )
        mock_venv.start_server.assert_called_once_with(expected_cmd, cwd=None)

    def test_python_lsp_basic(self):
        """Test starting Python LSP server."""
        mock_venv = mock.MagicMock()
        mock_process = mock.MagicMock()
        mock_venv.start_server.return_value = mock_process

        action = PythonLSPSpec(
            host="localhost",
            port=8889,
            verbose=False,
        )

        processes = run_actions_in_installer_env(mock_venv, [action])

        assert len(processes) == 1
        assert processes[0] == mock_process
        expected_cmd = shlex.join(
            [
                "python",
                "-m",
                "pylsp",
                "--tcp",
                "--host",
                "localhost",
                "--port",
                "8889",
            ]
        )
        mock_venv.start_server.assert_called_once_with(expected_cmd, cwd=None)

    def test_python_lsp_verbose(self):
        """Test Python LSP server with verbose flag."""
        mock_venv = mock.MagicMock()
        mock_process = mock.MagicMock()
        mock_venv.start_server.return_value = mock_process

        action = PythonLSPSpec(
            host="0.0.0.0",
            port=8889,
            verbose=True,
        )

        processes = run_actions_in_installer_env(mock_venv, [action])

        assert len(processes) == 1
        expected_cmd = shlex.join(
            [
                "python",
                "-m",
                "pylsp",
                "--tcp",
                "--host",
                "0.0.0.0",
                "--port",
                "8889",
                "-v",
            ]
        )
        mock_venv.start_server.assert_called_once_with(expected_cmd, cwd=None)

    def test_streamlit_basic(self):
        """Test starting Streamlit server."""
        mock_venv = mock.MagicMock()
        mock_process = mock.MagicMock()
        mock_venv.start_server.return_value = mock_process

        action = StreamlitSpec(
            script="app.py",
        )

        processes = run_actions_in_installer_env(mock_venv, [action])

        assert len(processes) == 1
        assert processes[0] == mock_process
        expected_cmd = shlex.join(["python", "-m", "streamlit", "run", "app.py"])
        mock_venv.start_server.assert_called_once_with(expected_cmd, cwd=None)

    def test_streamlit_with_port(self):
        """Test Streamlit server with custom port."""
        mock_venv = mock.MagicMock()
        mock_process = mock.MagicMock()
        mock_venv.start_server.return_value = mock_process

        action = StreamlitSpec(
            script="dashboard.py",
            port=8501,
        )

        processes = run_actions_in_installer_env(mock_venv, [action])

        assert len(processes) == 1
        expected_cmd = shlex.join(
            [
                "python",
                "-m",
                "streamlit",
                "run",
                "dashboard.py",
                "--server.port",
                "8501",
            ]
        )
        mock_venv.start_server.assert_called_once_with(expected_cmd, cwd=None)

    def test_streamlit_with_small_port(self):
        """Test Streamlit server with minimum valid port."""
        mock_venv = mock.MagicMock()
        mock_process = mock.MagicMock()
        mock_venv.start_server.return_value = mock_process

        action = StreamlitSpec(
            script="app.py",
            port=1,  # Minimum valid port
        )

        processes = run_actions_in_installer_env(mock_venv, [action])

        assert len(processes) == 1
        # Should include --server.port 1 when port=1
        expected_cmd = shlex.join(
            [
                "python",
                "-m",
                "streamlit",
                "run",
                "app.py",
                "--server.port",
                "1",
            ]
        )
        mock_venv.start_server.assert_called_once_with(expected_cmd, cwd=None)

    def test_streamlit_with_args(self):
        """Test Streamlit server with custom arguments."""
        mock_venv = mock.MagicMock()
        mock_process = mock.MagicMock()
        mock_venv.start_server.return_value = mock_process

        action = StreamlitSpec(
            script="app.py",
            port=8502,
            args=["--theme.base", "dark", "--server.headless", "true"],
        )

        processes = run_actions_in_installer_env(mock_venv, [action])

        assert len(processes) == 1
        expected_cmd = shlex.join(
            [
                "python",
                "-m",
                "streamlit",
                "run",
                "app.py",
                "--server.port",
                "8502",
                "--theme.base",
                "dark",
                "--server.headless",
                "true",
            ]
        )
        mock_venv.start_server.assert_called_once_with(expected_cmd, cwd=None)

    def test_extra_server_basic(self):
        """Test starting extra server without environment variables."""
        mock_venv = mock.MagicMock()
        mock_process = mock.MagicMock()
        mock_venv.start_server.return_value = mock_process

        action = ExtraServerSpec(
            command=["redis-server", "--port", "6379"],
            env={},
        )

        processes = run_actions_in_installer_env(mock_venv, [action])

        assert len(processes) == 1
        assert processes[0] == mock_process
        expected_cmd = shlex.join(["redis-server", "--port", "6379"])
        mock_venv.start_server.assert_called_once_with(expected_cmd, cwd=None)

    def test_extra_server_with_env(self):
        """Test starting extra server with environment variables."""
        mock_venv = mock.MagicMock()
        mock_process = mock.MagicMock()
        mock_venv.start_server.return_value = mock_process

        action = ExtraServerSpec(
            command=["node", "server.js"],
            env={"NODE_ENV": "production", "PORT": "3000"},
        )

        processes = run_actions_in_installer_env(mock_venv, [action])

        assert len(processes) == 1
        assert processes[0] == mock_process
        # Environment variables should be properly quoted and prefixed
        expected_cmd = "NODE_ENV=production PORT=3000 node server.js"
        mock_venv.start_server.assert_called_once_with(expected_cmd, cwd=None)

    def test_extra_server_with_special_chars_in_env(self):
        """Test extra server with special characters in environment variables."""
        mock_venv = mock.MagicMock()
        mock_process = mock.MagicMock()
        mock_venv.start_server.return_value = mock_process

        action = ExtraServerSpec(
            command=["./run.sh"],
            env={"MESSAGE": "Hello World!", "PATH_VAR": "/path/to/dir"},
        )

        processes = run_actions_in_installer_env(mock_venv, [action])

        assert len(processes) == 1
        # Keys should NOT be quoted, only values should be quoted
        expected_cmd = f"MESSAGE={shlex.quote('Hello World!')} PATH_VAR={shlex.quote('/path/to/dir')} ./run.sh"
        mock_venv.start_server.assert_called_once_with(expected_cmd, cwd=None)

    def test_multiple_actions(self):
        """Test running multiple actions in sequence."""
        mock_venv = mock.MagicMock()
        mock_proc1 = mock.MagicMock()
        mock_proc2 = mock.MagicMock()
        mock_proc3 = mock.MagicMock()
        mock_venv.start_server.side_effect = [mock_proc1, mock_proc2, mock_proc3]

        actions = [
            EnableJupyterTerminalsAction(),
            JupyterServerSpec(host="0.0.0.0", port=8888, allow_root=True),
            PythonLSPSpec(host="localhost", port=8889),
            StreamlitSpec(script="app.py", port=8501),
        ]

        processes = run_actions_in_installer_env(mock_venv, actions)

        assert (
            len(processes) == 3
        )  # EnableJupyterTerminalsAction doesn't return a process
        assert processes[0] == mock_proc1
        assert processes[1] == mock_proc2
        assert processes[2] == mock_proc3

        # Verify execute was called for terminal extension and dependency checks
        # Should have at least the terminal extension enable call
        assert mock_venv.execute.call_count >= 1
        mock_venv.execute.assert_any_call(
            "python -m jupyter server extension enable jupyter_server_terminals"
        )
        # Verify dependency checks were made (exact count may vary)
        # Each server type has its dependency check
        assert any(
            "import jupyter_server" in str(call)
            for call in mock_venv.execute.call_args_list
        )

        # Verify start_server was called 3 times
        assert mock_venv.start_server.call_count == 3

    def test_empty_actions_list(self):
        """Test with empty actions list."""
        mock_venv = mock.MagicMock()

        processes = run_actions_in_installer_env(mock_venv, [])

        assert processes == []
        mock_venv.execute.assert_not_called()
        mock_venv.start_server.assert_not_called()

    def test_unknown_action_type(self):
        """Test handling of unknown action type."""
        import pytest

        mock_venv = mock.MagicMock()

        # Create a mock action that doesn't match any known type
        mock_action = mock.MagicMock()
        mock_action.__class__.__name__ = "UnknownAction"

        # The function should raise TypeError for unknown action types
        with pytest.raises(TypeError) as exc_info:
            run_actions_in_installer_env(mock_venv, [mock_action])

        assert "Unsupported action type: UnknownAction" in str(exc_info.value)
        mock_venv.execute.assert_not_called()
        mock_venv.start_server.assert_not_called()

    def test_command_with_spaces(self):
        """Test handling commands with spaces in arguments."""
        mock_venv = mock.MagicMock()
        mock_process = mock.MagicMock()
        mock_venv.start_server.return_value = mock_process

        action = ExtraServerSpec(
            command=["python", "my script.py", "--title", "Test Server"],
            env={},
        )

        processes = run_actions_in_installer_env(mock_venv, [action])

        assert len(processes) == 1
        # shlex.join should properly quote arguments with spaces
        expected_cmd = shlex.join(["python", "my script.py", "--title", "Test Server"])
        mock_venv.start_server.assert_called_once_with(expected_cmd, cwd=None)

    def test_streamlit_with_directory_path(self):
        """Test Streamlit server with script in a directory."""
        mock_venv = mock.MagicMock()
        mock_process = mock.MagicMock()
        mock_venv.start_server.return_value = mock_process

        action = StreamlitSpec(
            script="/work/apps/dashboard.py",
            port=8501,
        )

        processes = run_actions_in_installer_env(mock_venv, [action])

        assert len(processes) == 1
        expected_cmd = shlex.join(
            [
                "python",
                "-m",
                "streamlit",
                "run",
                "/work/apps/dashboard.py",
                "--server.port",
                "8501",
            ]
        )
        mock_venv.start_server.assert_called_once_with(expected_cmd, cwd="/work/apps")

    def test_streamlit_none_port(self):
        """Test Streamlit with None port (should not add --server.port)."""
        mock_venv = mock.MagicMock()
        mock_process = mock.MagicMock()
        mock_venv.start_server.return_value = mock_process

        action = StreamlitSpec(
            script="app.py",
            port=None,  # Explicitly None
        )

        processes = run_actions_in_installer_env(mock_venv, [action])

        assert len(processes) == 1
        # Should not include --server.port when port is None
        expected_cmd = shlex.join(["python", "-m", "streamlit", "run", "app.py"])
        mock_venv.start_server.assert_called_once_with(expected_cmd, cwd=None)

    def test_streamlit_empty_args(self):
        """Test Streamlit with empty args list."""
        mock_venv = mock.MagicMock()
        mock_process = mock.MagicMock()
        mock_venv.start_server.return_value = mock_process

        action = StreamlitSpec(
            script="app.py",
            args=[],  # Empty list
        )

        processes = run_actions_in_installer_env(mock_venv, [action])

        assert len(processes) == 1
        # Should not add any extra args when list is empty
        expected_cmd = shlex.join(["python", "-m", "streamlit", "run", "app.py"])
        mock_venv.start_server.assert_called_once_with(expected_cmd, cwd=None)

    def test_jupyter_empty_extra_args(self):
        """Test Jupyter with empty extra_args list."""
        mock_venv = mock.MagicMock()
        mock_process = mock.MagicMock()
        mock_venv.start_server.return_value = mock_process

        action = JupyterServerSpec(
            host="0.0.0.0",
            port=8888,
            allow_root=False,
            extra_args=[],  # Empty list
        )

        processes = run_actions_in_installer_env(mock_venv, [action])

        assert len(processes) == 1
        # Should not add any extra args when list is empty
        expected_cmd = shlex.join(
            [
                "python",
                "-m",
                "jupyter",
                "server",
                "--ip",
                "0.0.0.0",
                "--port",
                "8888",
                "--no-browser",
            ]
        )
        mock_venv.start_server.assert_called_once_with(expected_cmd, cwd=None)
