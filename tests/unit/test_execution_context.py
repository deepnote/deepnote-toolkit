"""Unit tests for execution context implementations."""

import logging
import sys
from unittest import mock

import pytest

from deepnote_core.execution import ExecutionResult, ProcessHandle
from deepnote_toolkit.runtime.execution_context import PipExecutionContext


class TestExecutionResult:
    """Test ExecutionResult dataclass."""

    def test_default_values(self):
        """Test default values of ExecutionResult."""
        result = ExecutionResult()
        assert result.process is None
        assert result.is_long_running is False
        assert result.success is True
        assert result.message is None

    def test_with_process(self):
        """Test ExecutionResult with process handle."""
        mock_process = mock.Mock(spec=ProcessHandle)
        result = ExecutionResult(process=mock_process, is_long_running=True)
        assert result.process is mock_process
        assert result.is_long_running is True
        assert result.success is True

    def test_failure_result(self):
        """Test ExecutionResult for failure case."""
        result = ExecutionResult(success=False, message="Test error")
        assert result.success is False
        assert result.message == "Test error"
        assert result.process is None


class TestPipExecutionContext:
    """Test PipExecutionContext implementation."""

    def test_initialization_default(self):
        """Test default initialization."""
        context = PipExecutionContext()
        assert "PYTHONUNBUFFERED" in context.env
        assert context.env["PYTHONUNBUFFERED"] == "1"
        assert isinstance(context.logger, logging.Logger)

    def test_initialization_with_env(self):
        """Test initialization with custom environment."""
        custom_env = {"CUSTOM_VAR": "value"}
        context = PipExecutionContext(env=custom_env)
        assert context.env["CUSTOM_VAR"] == "value"
        assert context.env["PYTHONUNBUFFERED"] == "1"

    def test_initialization_with_logger(self):
        """Test initialization with custom logger."""
        custom_logger = logging.getLogger("test")
        context = PipExecutionContext(logger=custom_logger)
        assert context.logger is custom_logger

    def test_python_executable(self):
        """Test python_executable returns sys.executable."""
        context = PipExecutionContext()
        assert context.python_executable() == sys.executable

    @mock.patch("subprocess.Popen")
    def test_spawn_success(self, mock_popen):
        """Test successful process spawn."""
        mock_proc = mock.Mock()
        mock_proc.poll.return_value = None  # Still running
        mock_popen.return_value = mock_proc

        context = PipExecutionContext()
        result = context.spawn(["python", "--version"])

        assert result is mock_proc
        mock_popen.assert_called_once()
        call_args = mock_popen.call_args
        assert call_args[0][0] == ["python", "--version"]
        # Check that env contains at least PYTHONUNBUFFERED
        assert "PYTHONUNBUFFERED" in call_args[1]["env"]
        assert call_args[1]["env"]["PYTHONUNBUFFERED"] == "1"
        assert call_args[1]["cwd"] is None
        assert call_args[1]["start_new_session"] is True
        mock_proc.poll.assert_called_once()

    @mock.patch("subprocess.Popen")
    def test_spawn_quick_failure(self, mock_popen):
        """Test spawn with process that fails immediately."""
        mock_proc = mock.Mock()
        mock_proc.poll.return_value = 1  # Exit code 1
        mock_popen.return_value = mock_proc

        context = PipExecutionContext()
        with mock.patch.object(context.logger, "warning") as mock_warning:
            result = context.spawn(["python", "-c", "exit(1)"])

            assert result is mock_proc
            mock_warning.assert_called_once()
            assert "Process failed immediately" in mock_warning.call_args[0][0]

    def test_check_dependency_exists(self):
        """Test check_dependency with existing module."""
        context = PipExecutionContext()
        # 'os' module always exists
        context.check_dependency("os", "pip install os")  # Should not raise

    def test_check_dependency_missing(self):
        """Test check_dependency with missing module."""
        context = PipExecutionContext()
        with pytest.raises(SystemExit) as exc:
            context.check_dependency("nonexistent_module_xyz", "pip install fake")

        assert "Missing dependency: nonexistent_module_xyz" in str(exc.value)

    def test_check_dependency_logging(self):
        """Test that check_dependency logs appropriately."""
        context = PipExecutionContext()

        with mock.patch.object(context.logger, "debug") as mock_debug:
            context.check_dependency("sys", "pip install sys")
            mock_debug.assert_called_once()
            assert "Dependency sys is available" in mock_debug.call_args[0][0]

        with mock.patch.object(context.logger, "error") as mock_error:
            with pytest.raises(SystemExit):
                context.check_dependency("fake_module_123", "pip install fake")
            mock_error.assert_called_once()
            assert "Missing dependency: fake_module_123" in mock_error.call_args[0][0]

    def test_spawn_with_env_override(self):
        """Test spawn with env_override parameter."""
        context = PipExecutionContext(env={"BASE": "value"})

        with mock.patch("subprocess.Popen") as mock_popen:
            mock_proc = mock.Mock()
            mock_popen.return_value = mock_proc

            result = context.spawn(["echo", "test"], env_override={"EXTRA": "added"})

            assert result is mock_proc

            # Check merged environment was used
            call_env = mock_popen.call_args[1]["env"]
            assert "BASE" in call_env
            assert call_env["BASE"] == "value"
            assert "EXTRA" in call_env
            assert call_env["EXTRA"] == "added"

    def test_run_one_off_success(self):
        """Test run_one_off with successful command."""
        context = PipExecutionContext()

        with mock.patch("subprocess.run") as mock_run:
            mock_run.return_value = mock.Mock(returncode=0, stdout="output", stderr="")

            exit_code = context.run_one_off(["echo", "test"])

            assert exit_code == 0
            mock_run.assert_called_once_with(
                ["echo", "test"],
                env=context.env,
                capture_output=True,
                text=True,
                timeout=None,
            )

    def test_run_one_off_failure(self):
        """Test run_one_off with failing command."""
        context = PipExecutionContext()

        with mock.patch("subprocess.run") as mock_run:
            mock_run.return_value = mock.Mock(
                returncode=1, stdout="", stderr="error message"
            )

            with mock.patch.object(context.logger, "warning") as mock_warning:
                with mock.patch.object(context.logger, "debug") as mock_debug:
                    exit_code = context.run_one_off(["false"])

                    assert exit_code == 1
                    mock_warning.assert_called_once()
                    mock_debug.assert_any_call("stderr: error message")

    def test_spawn_with_cwd(self):
        """Test spawn with working directory parameter."""
        context = PipExecutionContext(env={})

        with mock.patch("subprocess.Popen") as mock_popen:
            mock_proc = mock.Mock()
            mock_proc.poll.return_value = None
            mock_popen.return_value = mock_proc

            result = context.spawn(
                ["python", "app.py"], env_override=None, cwd="/work/apps"
            )

            assert result is mock_proc
            mock_popen.assert_called_once()
            assert mock_popen.call_args[1]["cwd"] == "/work/apps"
            assert mock_popen.call_args[1]["start_new_session"] is True


class TestInstallerExecutionContext:
    """Test InstallerExecutionContext implementation."""

    def test_initialization(self):
        """Test InstallerExecutionContext initialization."""
        from installer.module.execution_context import InstallerExecutionContext

        mock_venv = mock.Mock()
        custom_env = {"KEY": "value"}
        custom_logger = logging.getLogger("test")

        context = InstallerExecutionContext(
            venv=mock_venv, env=custom_env, logger=custom_logger
        )

        assert context.venv is mock_venv
        assert context.env == custom_env
        assert context.logger is custom_logger

    def test_python_executable(self):
        """Test python_executable returns 'python' for venv."""
        from installer.module.execution_context import InstallerExecutionContext

        mock_venv = mock.Mock()
        context = InstallerExecutionContext(venv=mock_venv)
        assert context.python_executable() == "python"

    def test_spawn_success(self):
        """Test successful spawn in installer context."""
        from installer.module.execution_context import InstallerExecutionContext

        mock_venv = mock.Mock()
        mock_proc = mock.Mock()
        mock_proc.poll.return_value = None  # Still running
        mock_venv.start_server.return_value = mock_proc

        context = InstallerExecutionContext(venv=mock_venv)
        result = context.spawn(["python", "-m", "http.server"])

        assert result is mock_proc
        mock_venv.start_server.assert_called_once_with(
            "python -m http.server", cwd=None
        )
        mock_proc.poll.assert_called_once()

    def test_spawn_quick_failure(self):
        """Test spawn with quick failure in installer context."""
        from installer.module.execution_context import InstallerExecutionContext

        mock_venv = mock.Mock()
        mock_proc = mock.Mock()
        mock_proc.poll.return_value = 1  # Failed
        mock_venv.start_server.return_value = mock_proc

        context = InstallerExecutionContext(venv=mock_venv)

        with mock.patch.object(context.logger, "warning") as mock_warning:
            result = context.spawn(["python", "-c", "exit(1)"])

            assert result is mock_proc
            mock_warning.assert_called_once()
            assert "Process failed quickly" in mock_warning.call_args[0][0]

    def test_check_dependency_exists(self):
        """Test check_dependency with existing module in venv."""
        from installer.module.execution_context import InstallerExecutionContext

        mock_venv = mock.Mock()
        mock_venv.execute.return_value = None  # Success

        context = InstallerExecutionContext(venv=mock_venv)
        context.check_dependency("pytest", "pip install pytest")

        mock_venv.execute.assert_called_once_with('python -c "import pytest"')

    def test_check_dependency_missing(self):
        """Test check_dependency with missing module in venv."""
        from installer.module.execution_context import InstallerExecutionContext

        mock_venv = mock.Mock()
        mock_venv.execute.side_effect = Exception("Module not found")

        context = InstallerExecutionContext(venv=mock_venv)

        with pytest.raises(SystemExit) as exc:
            context.check_dependency("fake_module", "pip install fake")

        assert "Missing dependency: fake_module" in str(exc.value)

    def test_spawn_with_special_chars(self):
        """Test spawn handles special characters correctly."""
        from installer.module.execution_context import InstallerExecutionContext

        mock_venv = mock.Mock()
        mock_proc = mock.Mock()
        mock_proc.poll.return_value = None
        mock_venv.start_server.return_value = mock_proc

        context = InstallerExecutionContext(venv=mock_venv)
        result = context.spawn(["echo", "hello world", "special$char"])

        assert result is mock_proc
        # shlex.join should properly escape special characters
        mock_venv.start_server.assert_called_once_with(
            "echo 'hello world' 'special$char'", cwd=None
        )

    def test_spawn_with_env_override(self):
        """Test spawn prefixes env_override in installer context."""
        from installer.module.execution_context import InstallerExecutionContext

        mock_venv = mock.Mock()
        mock_proc = mock.Mock()
        mock_proc.poll.return_value = None
        mock_venv.start_server.return_value = mock_proc

        context = InstallerExecutionContext(venv=mock_venv)

        result = context.spawn(["echo", "test"], env_override={"KEY": "value"})

        assert result is mock_proc
        # Command should have environment variable prefix
        mock_venv.start_server.assert_called_once_with("KEY=value echo test", cwd=None)

    def test_run_one_off_success(self):
        """Test run_one_off with successful command in installer."""
        from installer.module.execution_context import InstallerExecutionContext

        mock_venv = mock.Mock()
        context = InstallerExecutionContext(venv=mock_venv)

        exit_code = context.run_one_off(["echo", "test"])

        assert exit_code == 0
        mock_venv.execute.assert_called_once_with("echo test")

    def test_run_one_off_failure(self):
        """Test run_one_off with failing command in installer."""
        from installer.module.execution_context import InstallerExecutionContext

        mock_venv = mock.Mock()
        mock_venv.execute.side_effect = Exception("Command failed")

        context = InstallerExecutionContext(venv=mock_venv)

        with mock.patch.object(context.logger, "warning") as mock_warning:
            exit_code = context.run_one_off(["false"])

            assert exit_code == 1
            mock_warning.assert_called_once()
            assert "Command failed: false" in mock_warning.call_args[0][0]

    def test_spawn_with_cwd(self):
        """Test spawn with working directory parameter."""
        from installer.module.execution_context import InstallerExecutionContext

        mock_venv = mock.Mock()
        mock_proc = mock.Mock()
        mock_proc.poll.return_value = None
        mock_venv.start_server.return_value = mock_proc

        context = InstallerExecutionContext(venv=mock_venv)
        result = context.spawn(
            ["python", "script.py"], env_override=None, cwd="/work/apps"
        )

        assert result is mock_proc
        mock_venv.start_server.assert_called_once_with(
            "python script.py", cwd="/work/apps"
        )

    def test_spawn_with_cwd_and_env(self):
        """Test spawn with both cwd and env_override."""
        from installer.module.execution_context import InstallerExecutionContext

        mock_venv = mock.Mock()
        mock_proc = mock.Mock()
        mock_proc.poll.return_value = None
        mock_venv.start_server.return_value = mock_proc

        context = InstallerExecutionContext(venv=mock_venv)
        result = context.spawn(
            ["python", "app.py"], env_override={"PORT": "8080"}, cwd="/app"
        )

        assert result is mock_proc
        mock_venv.start_server.assert_called_once_with(
            "PORT=8080 python app.py", cwd="/app"
        )

    def test_run_one_off_with_env_override(self):
        """Test run_one_off with environment override."""
        from installer.module.execution_context import InstallerExecutionContext

        mock_venv = mock.Mock()
        context = InstallerExecutionContext(venv=mock_venv)

        exit_code = context.run_one_off(
            ["echo", "$VAR"], env_override={"VAR": "test_value"}
        )

        assert exit_code == 0
        mock_venv.execute.assert_called_once_with("VAR=test_value echo '$VAR'")

    def test_run_one_off_with_timeout(self):
        """Test run_one_off with timeout parameter."""
        from installer.module.execution_context import InstallerExecutionContext

        mock_venv = mock.Mock()
        context = InstallerExecutionContext(venv=mock_venv)

        exit_code = context.run_one_off(["sleep", "10"], timeout=5)

        assert exit_code == 0
        mock_venv.execute.assert_called_once_with("timeout 5 sleep 10")

    def test_run_one_off_timeout_failure(self):
        """Test run_one_off when timeout expires."""
        from installer.module.execution_context import InstallerExecutionContext

        mock_venv = mock.Mock()
        mock_venv.execute.side_effect = Exception("timeout: command terminated")

        context = InstallerExecutionContext(venv=mock_venv)

        with mock.patch.object(context.logger, "warning"):
            exit_code = context.run_one_off(["sleep", "10"], timeout=1)

            assert exit_code == 124  # Standard timeout exit code

    def test_run_one_off_with_env_and_timeout(self):
        """Test run_one_off with both env_override and timeout."""
        from installer.module.execution_context import InstallerExecutionContext

        mock_venv = mock.Mock()
        context = InstallerExecutionContext(venv=mock_venv)

        exit_code = context.run_one_off(
            ["python", "script.py"], env_override={"PYTHONPATH": "/custom"}, timeout=30
        )

        assert exit_code == 0
        mock_venv.execute.assert_called_once_with(
            "timeout 30 PYTHONPATH=/custom python script.py"
        )
