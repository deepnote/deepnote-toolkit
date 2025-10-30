"""Unit tests for deepnote_toolkit.runtime.executor module."""

import os
import sys
from pathlib import Path
from unittest import mock

import pytest

from deepnote_core.runtime.types import (
    EnableJupyterTerminalsAction,
    ExtraServerSpec,
    JupyterServerSpec,
    PythonLSPSpec,
    StreamlitSpec,
)
from deepnote_toolkit.runtime.executor import _base_env, _need, run_actions_pip


class TestBaseEnv:
    """Test _base_env function."""

    def test_base_env_basic(self):
        """Test basic environment building."""
        cfg = mock.MagicMock()
        cfg.paths.home_dir = "/home/test"
        cfg.paths.cache_dir = None
        cfg.paths.config_dir = None
        cfg.paths.log_dir = None

        with mock.patch.dict(os.environ, {"HOME": "/original/home"}, clear=True):
            env = _base_env(cfg)

            assert env["HOME"] == "/original/home"  # Copies from os.environ
            assert env["PYTHONUNBUFFERED"] == "1"
            assert "DEEPNOTE_LOG_DIR" not in env  # log_dir is None

    def test_base_env_with_log_dir(self):
        """Test environment with log dir set."""
        cfg = mock.MagicMock()
        cfg.paths.home_dir = "/home/test"
        cfg.paths.log_dir = "/logs"

        env = _base_env(cfg)

        assert env["PYTHONUNBUFFERED"] == "1"
        assert env["DEEPNOTE_LOG_DIR"] == "/logs"

    def test_base_env_inherits_os_environ(self):
        """Test that environment inherits from os.environ."""
        with mock.patch.dict(os.environ, {"EXISTING_VAR": "value", "HOME": "/test"}):
            cfg = mock.MagicMock()
            cfg.paths.home_dir = "/home/test"  # Not used
            cfg.paths.log_dir = None

            env = _base_env(cfg)

            assert env["EXISTING_VAR"] == "value"
            assert env["HOME"] == "/test"  # From os.environ, not config
            assert env["PYTHONUNBUFFERED"] == "1"

    def test_base_env_missing_paths_attr(self):
        """Test environment building when log_dir is None."""
        from deepnote_core.config.models import DeepnoteConfig, PathConfig

        # Create config with log_dir=None (its default)
        cfg = DeepnoteConfig(
            paths=PathConfig(home_dir=Path("/home/test"))  # log_dir defaults to None
        )

        env = _base_env(cfg)

        assert env["PYTHONUNBUFFERED"] == "1"
        assert "DEEPNOTE_LOG_DIR" not in env


class TestNeed:
    """Test _need function."""

    def test_need_existing_module(self):
        """Test _need with existing module."""
        # 'os' module always exists
        _need("os", "pip install os")  # Should not raise

    def test_need_missing_module(self):
        """Test _need with missing module."""
        with pytest.raises(SystemExit) as exc:
            _need("nonexistent_module_12345", "pip install fake")

        assert (
            exc.value.code
            == "Missing dependency: nonexistent_module_12345. Install: pip install fake"
        )

    def test_need_only_catches_import_error(self):
        """Test that _need only catches ImportError."""
        with mock.patch("builtins.__import__", side_effect=ValueError("Other error")):
            # Should not catch non-ImportError exceptions
            with pytest.raises(ValueError):
                _need("test_module", "pip install test")


class TestRunActionsPip:
    """Test run_actions_pip function."""

    @mock.patch("subprocess.Popen")
    def test_run_jupyter_server(self, mock_popen):
        """Test running Jupyter server."""
        mock_proc = mock.MagicMock()
        mock_popen.return_value = mock_proc

        cfg = mock.MagicMock()
        cfg.paths.home_dir = "/home/test"
        cfg.paths.log_dir = None

        action = JupyterServerSpec(
            host="0.0.0.0",
            port=8888,
            allow_root=True,
            extra_args=["--debug"],
        )

        with mock.patch(
            "deepnote_toolkit.runtime.execution_context.PipExecutionContext.check_dependency"
        ):
            processes = run_actions_pip(cfg, [action])

        assert len(processes) == 1
        assert processes[0] == mock_proc

        # Check the command that was executed
        mock_popen.assert_called_once()
        args = mock_popen.call_args[0][0]
        assert args == [
            sys.executable,
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

    @mock.patch("subprocess.Popen")
    def test_run_python_lsp(self, mock_popen):
        """Test running Python LSP server."""
        mock_proc = mock.MagicMock()
        mock_popen.return_value = mock_proc

        cfg = mock.MagicMock()
        cfg.paths.log_dir = None

        action = PythonLSPSpec(
            host="localhost",
            port=8889,
            verbose=True,
        )

        with mock.patch(
            "deepnote_toolkit.runtime.execution_context.PipExecutionContext.check_dependency"
        ):
            processes = run_actions_pip(cfg, [action])

        assert len(processes) == 1
        assert processes[0] == mock_proc

        # Check the command
        mock_popen.assert_called_once()
        args = mock_popen.call_args[0][0]
        assert args == [
            sys.executable,
            "-m",
            "pylsp",
            "--tcp",
            "--host",
            "localhost",
            "--port",
            "8889",
            "-v",
        ]

    @mock.patch("subprocess.Popen")
    def test_run_streamlit(self, mock_popen):
        """Test running Streamlit server."""
        mock_proc = mock.MagicMock()
        mock_popen.return_value = mock_proc

        cfg = mock.MagicMock()
        cfg.paths.log_dir = None

        action = StreamlitSpec(
            script="app.py", port=8501, args=["--theme.base", "dark"]
        )

        with mock.patch(
            "deepnote_toolkit.runtime.execution_context.PipExecutionContext.check_dependency"
        ):
            processes = run_actions_pip(cfg, [action])

        assert len(processes) == 1
        assert processes[0] == mock_proc

        # Check the command
        mock_popen.assert_called_once()
        args = mock_popen.call_args[0][0]
        assert args == [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            "app.py",
            "--server.port",
            "8501",
            "--theme.base",
            "dark",
        ]

    @mock.patch("subprocess.Popen")
    def test_run_extra_server(self, mock_popen):
        """Test running extra server."""
        mock_proc = mock.MagicMock()
        mock_popen.return_value = mock_proc

        cfg = mock.MagicMock()
        cfg.paths.log_dir = None

        action = ExtraServerSpec(
            command=["custom-server", "--port", "9000"],
            env={},  # No extra env vars
        )

        processes = run_actions_pip(cfg, [action])

        assert len(processes) == 1
        assert processes[0] == mock_proc

        # Check the command and environment
        mock_popen.assert_called_once()
        args = mock_popen.call_args[0][0]
        assert args == ["custom-server", "--port", "9000"]

        # Check environment includes PYTHONUNBUFFERED
        env = mock_popen.call_args[1]["env"]
        assert env["PYTHONUNBUFFERED"] == "1"

    @mock.patch("subprocess.run")
    def test_enable_jupyter_terminals(self, mock_run):
        """Test enabling Jupyter terminals."""
        cfg = mock.MagicMock()
        cfg.paths.log_dir = None

        action = EnableJupyterTerminalsAction()

        # Mock successful subprocess run
        mock_run.return_value = mock.Mock(returncode=0, stderr="")

        with mock.patch(
            "deepnote_toolkit.runtime.execution_context.PipExecutionContext.check_dependency"
        ) as mock_check:
            processes = run_actions_pip(cfg, [action])

        assert processes == []  # No process returned for this action

        # Verify dependency check was called
        mock_check.assert_called_once_with(
            "jupyter_server_terminals", "pip install deepnote-toolkit[server]"
        )

        # Check the command was run
        mock_run.assert_called_once()
        args = mock_run.call_args[0][0]
        assert args == [
            sys.executable,
            "-m",
            "jupyter",
            "server",
            "extension",
            "enable",
            "jupyter_server_terminals",
        ]

        # Verify environment was passed
        assert "env" in mock_run.call_args[1]
        assert isinstance(mock_run.call_args[1]["env"], dict)
        assert len(mock_run.call_args[1]["env"]) > 0

    @mock.patch("subprocess.Popen")
    @mock.patch("subprocess.run")
    def test_run_multiple_actions(self, mock_run, mock_popen):
        """Test running multiple actions."""
        mock_proc1 = mock.MagicMock()
        mock_proc2 = mock.MagicMock()
        mock_popen.side_effect = [mock_proc1, mock_proc2]

        cfg = mock.MagicMock()
        cfg.paths.log_dir = None

        actions = [
            EnableJupyterTerminalsAction(),
            JupyterServerSpec(host="0.0.0.0", port=8888, allow_root=False),
            PythonLSPSpec(host="localhost", port=8889),
        ]

        with mock.patch(
            "deepnote_toolkit.runtime.execution_context.PipExecutionContext.check_dependency"
        ):
            processes = run_actions_pip(cfg, actions)

        assert len(processes) == 2
        assert processes[0] == mock_proc1
        assert processes[1] == mock_proc2

        # Verify extension enable was called
        mock_run.assert_called_once()

        # Verify two servers were started
        assert mock_popen.call_count == 2

    @mock.patch("subprocess.Popen")
    def test_empty_actions_list(self, mock_popen):
        """Test with empty actions list."""
        cfg = mock.MagicMock()
        cfg.paths.log_dir = None

        processes = run_actions_pip(cfg, [])

        assert processes == []
        mock_popen.assert_not_called()

    @mock.patch("subprocess.Popen")
    def test_environment_passed_correctly(self, mock_popen):
        """Test that environment is passed correctly to subprocess."""
        mock_proc = mock.MagicMock()
        mock_popen.return_value = mock_proc

        cfg = mock.MagicMock()
        cfg.paths.log_dir = "/custom/logs"

        action = JupyterServerSpec(host="localhost", port=8888, allow_root=False)

        with mock.patch(
            "deepnote_toolkit.runtime.execution_context.PipExecutionContext.check_dependency"
        ):
            run_actions_pip(cfg, [action])

        # Check environment was passed
        env = mock_popen.call_args[1]["env"]
        assert env["PYTHONUNBUFFERED"] == "1"
        assert env["DEEPNOTE_LOG_DIR"] == "/custom/logs"

    def test_dependency_check_for_jupyter(self):
        """Test that dependency check is called for Jupyter."""
        cfg = mock.MagicMock()
        cfg.paths.log_dir = None

        action = JupyterServerSpec(host="localhost", port=8888, allow_root=False)

        with mock.patch(
            "deepnote_toolkit.runtime.execution_context.PipExecutionContext.check_dependency"
        ) as mock_check:
            with mock.patch("subprocess.Popen"):
                run_actions_pip(cfg, [action])

        mock_check.assert_any_call(
            "jupyter_server", "pip install deepnote-toolkit[server]"
        )

    def test_dependency_check_for_lsp(self):
        """Test that dependency check is called for LSP."""
        cfg = mock.MagicMock()
        cfg.paths.log_dir = None

        action = PythonLSPSpec(host="localhost", port=8889)

        with mock.patch(
            "deepnote_toolkit.runtime.execution_context.PipExecutionContext.check_dependency"
        ) as mock_check:
            with mock.patch("subprocess.Popen"):
                run_actions_pip(cfg, [action])

        mock_check.assert_any_call("pylsp", "pip install 'python-lsp-server[all]'")

    def test_dependency_check_for_streamlit(self):
        """Test that dependency check is called for Streamlit."""
        cfg = mock.MagicMock()
        cfg.paths.log_dir = None

        action = StreamlitSpec(script="app.py")

        with mock.patch(
            "deepnote_toolkit.runtime.execution_context.PipExecutionContext.check_dependency"
        ) as mock_check:
            with mock.patch("subprocess.Popen"):
                run_actions_pip(cfg, [action])

        mock_check.assert_any_call("streamlit", "pip install streamlit")
