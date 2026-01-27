"""Unit tests for deepnote_toolkit CLI."""

import argparse
from pathlib import Path
from unittest import mock

import pytest

from deepnote_toolkit.cli.main import build_parser, main
from deepnote_toolkit.cli.server import add_server_subparser, run_server_command


class TestCLIMain:
    """Test main CLI functionality."""

    def test_build_parser(self):
        """Test parser construction."""
        parser = build_parser()
        assert parser.prog == "deepnote-toolkit"
        assert parser.description is not None
        assert "Deepnote Toolkit CLI" in parser.description

    def test_parse_global_options(self):
        """Test parsing global options."""
        parser = build_parser()

        # Test verbose
        args = parser.parse_args(["-v", "server"])
        assert args.verbose == 1

        args = parser.parse_args(["-vv", "server"])
        assert args.verbose == 2

        # Test quiet
        args = parser.parse_args(["-q", "server"])
        assert args.quiet is True

        # Test debug
        args = parser.parse_args(["-d", "server"])
        assert args.debug is True

    def test_version_output(self, capsys):
        """Test version output."""
        # Get version from the same source as the CLI
        from deepnote_toolkit._version import __version__

        parser = build_parser()
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["--version"])
        assert exc.value.code == 0
        captured = capsys.readouterr()
        assert __version__ in captured.out
        assert "deepnote-toolkit" in captured.out

    def test_no_command_shows_help(self, capsys):
        """Test that no command shows help."""
        with mock.patch("deepnote_toolkit.logging.get_logger"):
            with mock.patch("logging.basicConfig"):
                ret = main([])
        assert ret == 1
        captured = capsys.readouterr()
        assert "usage:" in captured.out.lower()

    def test_server_command_dispatch(self):
        """Test server command dispatch."""
        with mock.patch("deepnote_toolkit.logging.get_logger"):
            with mock.patch("logging.basicConfig"):
                with mock.patch("deepnote_toolkit.cli.server.ConfigurationLoader"):
                    with mock.patch(
                        "deepnote_toolkit.cli.server.ensure_effective_config"
                    ):
                        with mock.patch(
                            "deepnote_toolkit.cli.server.build_server_plan",
                            return_value=[],
                        ):
                            with mock.patch(
                                "deepnote_toolkit.cli.server.run_actions_pip",
                                return_value=[],
                            ):
                                with mock.patch(
                                    "deepnote_toolkit.cli.server.time.sleep",
                                    side_effect=KeyboardInterrupt,
                                ):
                                    ret = main(["server"])
                                    assert ret == 0

    def test_config_command_dispatch(self):
        """Test config command dispatch."""
        # Need to mock at the import level to prevent actual config commands
        with mock.patch(
            "deepnote_core.config.cli.run_config_command"
        ) as mock_run_config:
            mock_run_config.return_value = 0

            ret = main(["config", "show"])

            assert ret == 0
            mock_run_config.assert_called_once()
            args = mock_run_config.call_args[0][0]
            assert args._cmd == "config"
            assert args.config_cmd == "show"


class TestServerCommand:
    """Test server subcommand."""

    def test_add_server_subparser(self):
        """Test adding server subparser."""
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers()

        server_parser = add_server_subparser(subparsers)

        assert server_parser is not None
        assert server_parser.prog.endswith("server")

    def test_server_options(self):
        """Test server command options."""
        parser = build_parser()

        # Test basic server command
        args = parser.parse_args(["server"])
        assert args._cmd == "server"

        # Test with port options
        args = parser.parse_args(
            ["server", "--jupyter-port", "9000", "--ls-port", "9001"]
        )
        assert args.jupyter_port == 9000
        assert args.ls_port == 9001

        # Test with config file
        args = parser.parse_args(["server", "--config", "/path/to/config.toml"])
        assert args.config == "/path/to/config.toml"

        # Test boolean flags
        args = parser.parse_args(
            ["server", "--no-enable-terminals", "--python-kernel-only"]
        )
        assert args.enable_terminals is False
        assert args.python_kernel_only is True

    @mock.patch("deepnote_toolkit.cli.server.run_actions_pip")
    @mock.patch("deepnote_toolkit.cli.server.build_server_plan")
    @mock.patch("deepnote_toolkit.cli.server.ensure_effective_config")
    @mock.patch("deepnote_toolkit.cli.server.ConfigurationLoader")
    def test_run_server_command(
        self, mock_loader, mock_ensure, mock_build_plan, mock_run_actions
    ):
        """Test running server command."""
        # Setup mocks
        mock_cfg = mock.MagicMock()
        mock_loader.return_value.load_with_args.return_value = mock_cfg
        mock_build_plan.return_value = []
        mock_run_actions.return_value = []

        # Create args
        args = argparse.Namespace(
            _cmd="server",
            config=None,
            jupyter_port=None,
            ls_port=None,
            enable_terminals=None,
            python_kernel_only=None,
        )

        # Test keyboard interrupt handling
        with mock.patch("time.sleep", side_effect=KeyboardInterrupt):
            ret = run_server_command(args)

        assert ret == 0
        mock_loader.assert_called_once()
        mock_ensure.assert_called_once_with(mock_cfg)
        mock_build_plan.assert_called_once_with(mock_cfg)

    @mock.patch("deepnote_toolkit.cli.server.ConfigurationLoader")
    def test_run_server_command_with_config_path(self, mock_loader):
        """Test server command with custom config path."""
        mock_cfg = mock.MagicMock()
        mock_loader.return_value.load_with_args.return_value = mock_cfg

        args = argparse.Namespace(
            _cmd="server",
            config="/custom/config.toml",
            jupyter_port=None,
            ls_port=None,
            enable_terminals=None,
            python_kernel_only=None,
        )

        with mock.patch("time.sleep", side_effect=KeyboardInterrupt):
            with mock.patch(
                "deepnote_toolkit.cli.server.run_actions_pip", return_value=[]
            ):
                with mock.patch(
                    "deepnote_toolkit.cli.server.build_server_plan", return_value=[]
                ):
                    with mock.patch(
                        "deepnote_toolkit.cli.server.ensure_effective_config"
                    ):
                        run_server_command(args)

        # Verify config path was expanded and passed
        mock_loader.assert_called_once()
        call_args = mock_loader.call_args
        assert call_args[1]["config_path"] == Path("/custom/config.toml")


class TestProcessCleanup:
    """Test process cleanup functionality."""

    @mock.patch("deepnote_toolkit.cli.server.managed_processes")
    @mock.patch("deepnote_toolkit.cli.server.run_actions_pip")
    @mock.patch("deepnote_toolkit.cli.server.build_server_plan")
    @mock.patch("deepnote_toolkit.cli.server.ensure_effective_config")
    @mock.patch("deepnote_toolkit.cli.server.ConfigurationLoader")
    def test_process_cleanup_on_interrupt(
        self, mock_loader, mock_ensure, mock_build_plan, mock_run_actions, mock_context
    ):
        """Test that processes are properly cleaned up on interrupt."""
        # Create mock processes
        mock_proc1 = mock.MagicMock()
        mock_proc1.pid = 1234
        mock_proc1.poll.return_value = None  # Process still running

        mock_proc2 = mock.MagicMock()
        mock_proc2.pid = 5678
        mock_proc2.poll.return_value = None  # Process still running

        mock_cfg = mock.MagicMock()
        mock_loader.return_value.load_with_args.return_value = mock_cfg
        mock_build_plan.return_value = []
        mock_run_actions.return_value = [mock_proc1, mock_proc2]

        # Mock the process manager
        mock_manager = mock.MagicMock()
        mock_manager.processes = [mock_proc1, mock_proc2]
        mock_manager.check_processes.side_effect = KeyboardInterrupt
        mock_context.return_value.__enter__.return_value = mock_manager

        args = argparse.Namespace(
            _cmd="server",
            config=None,
            jupyter_port=None,
            ls_port=None,
            enable_terminals=None,
            python_kernel_only=None,
        )

        ret = run_server_command(args)

        assert ret == 0

        # Verify the full flow was called
        mock_ensure.assert_called_once()

        # Verify processes were added to manager
        assert mock_manager.add_process.call_count == 2
        mock_manager.add_process.assert_any_call(mock_proc1)
        mock_manager.add_process.assert_any_call(mock_proc2)

        # Verify monitoring was attempted (interrupt occurred during check)
        mock_manager.check_processes.assert_called()


class TestConfigIntegration:
    """Test config subcommand integration."""

    def test_config_commands(self):
        """Test various config commands."""
        with mock.patch(
            "deepnote_core.config.cli.run_config_command"
        ) as mock_run_config:
            with mock.patch("deepnote_toolkit.logging.get_logger"):
                with mock.patch("logging.basicConfig"):
                    mock_run_config.return_value = 0

                    # Test config show
                    ret = main(["config", "show"])
                    assert ret == 0

                    # Test config get
                    ret = main(["config", "get", "server.jupyter_port"])
                    assert ret == 0

                    # Test config set
                    ret = main(["config", "set", "server.jupyter_port", "8888"])
                    assert ret == 0

                    # Test config validate
                    ret = main(["config", "validate"])
                    assert ret == 0

                    # Test config generate
                    ret = main(["config", "generate"])
                    assert ret == 0

                    # Verify all calls were made
                    assert mock_run_config.call_count == 5


class TestErrorHandling:
    """Test error handling in CLI."""

    def test_invalid_command(self):
        """Test handling of invalid command."""
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["invalid-command"])

    def test_invalid_config_subcommand(self):
        """Test handling of invalid config subcommand."""
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["config", "invalid"])

    def test_invalid_port_values(self):
        """Test handling of invalid port values."""
        parser = build_parser()

        # Non-numeric port
        with pytest.raises(SystemExit):
            parser.parse_args(["server", "--jupyter-port", "abc"])

        # Negative port (argparse doesn't validate negative ints by default)
        args = parser.parse_args(["server", "--ls-port", "-1"])
        assert args.ls_port == -1  # Parser accepts it, validation happens later


class TestCLIEndToEnd:
    """End-to-end CLI tests."""

    @mock.patch("deepnote_toolkit.cli.server.run_actions_pip")
    @mock.patch("deepnote_toolkit.cli.server.build_server_plan")
    @mock.patch("deepnote_toolkit.cli.server.ensure_effective_config")
    @mock.patch("deepnote_toolkit.cli.server.ConfigurationLoader")
    def test_full_server_flow(
        self, mock_loader, mock_ensure, mock_build_plan, mock_run_actions
    ):
        """Test complete server flow."""
        mock_cfg = mock.MagicMock()
        mock_loader.return_value.load_with_args.return_value = mock_cfg
        mock_build_plan.return_value = ["action1", "action2"]
        mock_run_actions.return_value = []

        # Run with custom ports
        with mock.patch("time.sleep", side_effect=KeyboardInterrupt):
            ret = main(
                [
                    "-v",
                    "server",
                    "--jupyter-port",
                    "9999",
                    "--ls-port",
                    "9998",
                    "--config",
                    "~/custom.toml",
                ]
            )

        assert ret == 0

        # Verify the flow
        mock_loader.assert_called_once()
        mock_ensure.assert_called_once_with(mock_cfg)
        mock_build_plan.assert_called_once_with(mock_cfg)
        mock_run_actions.assert_called_once_with(mock_cfg, ["action1", "action2"])

    def test_help_output(self, capsys):
        """Test help output for various commands."""
        parser = build_parser()

        # Main help
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["--help"])
        assert exc.value.code == 0
        captured = capsys.readouterr()
        assert "Deepnote Toolkit CLI" in captured.out
        assert "server" in captured.out
        assert "config" in captured.out

        # Server help
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["server", "--help"])
        assert exc.value.code == 0
        captured = capsys.readouterr()
        assert "jupyter-port" in captured.out
        assert "ls-port" in captured.out

        # Config help
        with pytest.raises(SystemExit) as exc:
            parser.parse_args(["config", "--help"])
        assert exc.value.code == 0
        captured = capsys.readouterr()
        assert "show" in captured.out
        assert "get" in captured.out
        assert "set" in captured.out
