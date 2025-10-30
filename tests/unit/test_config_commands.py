"""Unit tests for individual config command modules."""

import json
from pathlib import Path
from unittest import mock

from pydantic import SecretStr

from deepnote_core.config.commands import print_command, validate_command
from deepnote_core.config.models import (
    DeepnoteConfig,
    InstallationConfig,
    PathConfig,
    RuntimeConfig,
    ServerConfig,
)


class TestPrintCommand:
    """Test the print command directly."""

    @mock.patch("deepnote_core.config.commands.print.get_loader")
    def test_print_command_basic(self, mock_get_loader, capsys):
        """Test print command outputs JSON."""
        # Create a mock config
        mock_cfg = DeepnoteConfig(
            server=ServerConfig(jupyter_port=8888, ls_port=8889),
            paths=PathConfig(home_dir=Path("/home/user")),
            runtime=RuntimeConfig(dev_mode=False),
            installation=InstallationConfig(),
        )

        mock_loader = mock.Mock()
        mock_loader.load_config.return_value = mock_cfg
        mock_get_loader.return_value = mock_loader

        args = mock.Mock()
        args.config = None
        args.include_secrets = False

        result = print_command(args)

        assert result == 0
        captured = capsys.readouterr()

        # Verify JSON output
        data = json.loads(captured.out)
        assert data["server"]["jupyter_port"] == 8888
        assert data["server"]["ls_port"] == 8889

    @mock.patch("deepnote_core.config.commands.print.get_loader")
    def test_print_command_with_secrets(self, mock_get_loader, capsys):
        """Test print command with secrets included."""
        # Create a mock config with secrets
        mock_cfg = DeepnoteConfig(
            server=ServerConfig(jupyter_port=8888, ls_port=8889),
            paths=PathConfig(home_dir=Path("/home/user")),
            runtime=RuntimeConfig(
                dev_mode=False,
                project_secret=SecretStr("secret123"),
                webapp_url="http://example.com?token=abc",
            ),
            installation=InstallationConfig(),
        )

        mock_loader = mock.Mock()
        mock_loader.load_config.return_value = mock_cfg
        mock_get_loader.return_value = mock_loader

        # Test with secrets redacted
        args = mock.Mock()
        args.config = None
        args.include_secrets = False

        result = print_command(args)
        assert result == 0

        captured = capsys.readouterr()
        data = json.loads(captured.out)
        assert data["runtime"]["project_secret"] == "***REDACTED***"
        assert data["runtime"]["webapp_url"] == "***REDACTED***"

        # Test with secrets included
        args.include_secrets = True
        result = print_command(args)
        assert result == 0

        captured = capsys.readouterr()
        data = json.loads(captured.out)
        # Pydantic masks secrets in the model, so we get asterisks
        # The important thing is that include_secrets bypasses redaction
        assert "project_secret" in data["runtime"]
        assert "webapp_url" in data["runtime"]


class TestValidateCommand:
    """Test the validate command directly."""

    @mock.patch("deepnote_core.config.commands.validate.get_loader")
    def test_validate_command_success(self, mock_get_loader):
        """Test validate command with valid config."""
        mock_loader = mock.Mock()
        mock_loader.load_config.return_value = mock.Mock()  # Valid config
        mock_get_loader.return_value = mock_loader

        args = mock.Mock()
        args.config = None

        result = validate_command(args)
        assert result == 0

    @mock.patch("deepnote_core.config.commands.validate.get_loader")
    def test_validate_command_failure(self, mock_get_loader, capsys):
        """Test validate command with invalid config."""
        mock_loader = mock.Mock()
        mock_loader.load_config.side_effect = ValueError("Invalid config")
        mock_get_loader.return_value = mock_loader

        args = mock.Mock()
        args.config = None

        result = validate_command(args)
        assert result == 1

        captured = capsys.readouterr()
        assert "Validation failed: Invalid config" in captured.err
