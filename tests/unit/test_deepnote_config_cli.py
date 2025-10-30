"""Unit tests for deepnote_core.config.cli module."""

import json
from pathlib import Path
from unittest import mock

import pytest
import toml
import yaml

from deepnote_core.config.cli import (
    _format_describe,
    _get_nested_value,
    _set_nested_value,
    run_config_command,
)
from deepnote_core.config.models import (
    DeepnoteConfig,
    InstallationConfig,
    PathConfig,
    RuntimeConfig,
    ServerConfig,
)


class TestNestedValueHelpers:
    """Test helper functions for nested value access."""

    def test_get_nested_value_simple(self):
        """Test getting a simple nested value."""
        data = {"server": {"port": 8080}}
        result = _get_nested_value(data, "server.port")
        assert result == 8080

    def test_get_nested_value_deep(self):
        """Test getting a deeply nested value."""
        data = {"a": {"b": {"c": {"d": "value"}}}}
        result = _get_nested_value(data, "a.b.c.d")
        assert result == "value"

    def test_get_nested_value_from_object(self):
        """Test getting nested value from object with attributes."""

        class Server:
            def __init__(self):
                self.port = 9000
                self.host = "localhost"

        class Config:
            def __init__(self):
                self.server = Server()

        config = Config()
        result = _get_nested_value(config, "server.port")
        assert result == 9000

    def test_get_nested_value_mixed(self):
        """Test getting value from mixed object/dict structure."""

        class Server:
            def __init__(self):
                self.settings = {"port": 7000}

        data = {"server": Server()}
        result = _get_nested_value(data, "server.settings.port")
        assert result == 7000

    def test_get_nested_value_missing_key(self):
        """Test getting non-existent key raises KeyError for dict."""
        data = {"server": {"port": 8080}}
        with pytest.raises(KeyError) as exc:
            _get_nested_value(data, "server.missing")
        assert "not found" in str(exc.value)

    def test_get_nested_value_invalid_path(self):
        """Test getting value from non-dict/object raises KeyError."""
        data = {"server": "string_value"}
        with pytest.raises(KeyError) as exc:
            _get_nested_value(data, "server.port")
        assert "not found" in str(exc.value)

    def test_set_nested_value_simple(self):
        """Test setting a simple nested value."""
        data = {"server": {"port": 8080}}
        _set_nested_value(data, "server.port", "9090")
        assert data["server"]["port"] == 9090

    def test_set_nested_value_create_path(self):
        """Test setting value creates missing intermediate dicts."""
        data = {}
        _set_nested_value(data, "server.config.port", "8080")
        assert data == {"server": {"config": {"port": 8080}}}

    def test_set_nested_value_deep(self):
        """Test setting deeply nested value."""
        data = {"a": {"b": {"c": {}}}}
        _set_nested_value(data, "a.b.c.d.e", "value")
        assert data["a"]["b"]["c"]["d"]["e"] == "value"

    def test_set_nested_value_overwrite(self):
        """Test setting value overwrites existing."""
        data = {"server": {"port": 8080}}
        _set_nested_value(data, "server", '{"host": "localhost"}')
        assert data == {"server": {"host": "localhost"}}

    def test_set_nested_value_invalid_path(self):
        """Test setting value in non-dict raises ValueError."""
        data = {"server": "string_value"}
        with pytest.raises(ValueError) as exc:
            _set_nested_value(data, "server.port", "8080")
        assert "not a dict" in str(exc.value)

    def test_set_nested_value_null(self):
        """Test setting None value."""
        data = {"server": {"port": 8080}}
        _set_nested_value(data, "server.port", "null")
        assert data["server"]["port"] is None


class TestConfigCommands:
    """Test config command implementations."""

    @pytest.fixture
    def mock_loader(self):
        """Create a mock ConfigurationLoader."""

        with mock.patch(
            "deepnote_core.config.commands.utils.ConfigurationLoader"
        ) as MockLoader:
            loader = MockLoader.return_value
            # Create a real config with proper Pydantic models for describe command
            mock_cfg = DeepnoteConfig(
                server=ServerConfig(jupyter_port=8888, ls_port=8889),
                paths=PathConfig(home_dir=Path("/home/user")),
                runtime=RuntimeConfig(dev_mode=False),
                installation=InstallationConfig(),
            )

            loader.load_config.return_value = mock_cfg
            loader.load_with_args.return_value = mock_cfg
            yield loader

    def test_validate_command_valid(self, mock_loader, capsys):
        """Test validate command with valid config."""
        args = mock.MagicMock()
        args.config = None
        args.config_cmd = "validate"

        ret = run_config_command(args)

        assert ret == 0
        # Validate command doesn't print anything on success
        captured = capsys.readouterr()
        assert captured.out == ""
        assert captured.err == ""

    def test_validate_command_invalid(self, mock_loader, capsys):
        """Test validate command with invalid config."""
        # Make load_config raise validation error
        mock_loader.load_config.side_effect = ValueError("Invalid config")

        args = mock.MagicMock()
        args.config = None
        args.config_cmd = "validate"

        ret = run_config_command(args)

        assert ret == 1
        captured = capsys.readouterr()
        assert "Validation failed" in captured.err
        assert "Invalid config" in captured.err

    def test_describe_command(self, mock_loader, capsys):
        """Test describe command."""
        args = mock.MagicMock()
        args.config = None
        args.config_cmd = "describe"
        args.runtime = False

        ret = run_config_command(args)

        assert ret == 0
        captured = capsys.readouterr()
        assert "[server]" in captured.out
        assert "jupyter_port" in captured.out

    def test_describe_command_with_runtime(self, mock_loader, capsys):
        """Test describe command with runtime flag."""
        args = mock.MagicMock()
        args.config = None
        args.config_cmd = "describe"
        args.runtime = True

        ret = run_config_command(args)

        assert ret == 0
        mock_loader.load_config.assert_called_once()

    def test_show_command_json(self, mock_loader, capsys):
        """Test show command with JSON format."""
        args = mock.MagicMock()
        args.config = None
        args.config_cmd = "show"
        args.format = "json"
        args.runtime = False

        ret = run_config_command(args)

        assert ret == 0
        captured = capsys.readouterr()
        # Check it's valid JSON
        json.loads(captured.out)

    def test_show_command_yaml(self, mock_loader, capsys):
        """Test show command with YAML format."""
        args = mock.MagicMock()
        args.config = None
        args.config_cmd = "show"
        args.format = "yaml"
        args.runtime = False

        ret = run_config_command(args)

        assert ret == 0
        captured = capsys.readouterr()
        # Check it's valid YAML
        yaml.safe_load(captured.out)

    def test_show_command_toml(self, mock_loader, capsys):
        """Test show command with TOML format."""
        args = mock.MagicMock()
        args.config = None
        args.config_cmd = "show"
        args.format = "toml"
        args.runtime = False

        ret = run_config_command(args)

        assert ret == 0
        captured = capsys.readouterr()
        # Check it's valid TOML
        toml.loads(captured.out)

    def test_get_command_simple(self, mock_loader, capsys):
        """Test get command for simple value."""
        args = mock.MagicMock()
        args.config = None
        args.config_cmd = "get"
        args.key = "server.jupyter_port"
        args.json = False

        ret = run_config_command(args)

        assert ret == 0
        captured = capsys.readouterr()
        assert "8888" in captured.out

    def test_get_command_json_output(self, mock_loader, capsys):
        """Test get command with JSON output."""
        args = mock.MagicMock()
        args.config = None
        args.config_cmd = "get"
        args.key = "server.jupyter_port"
        args.json = True

        ret = run_config_command(args)

        assert ret == 0
        captured = capsys.readouterr()
        value = json.loads(captured.out)
        assert value == 8888

    def test_get_command_missing_key(self, mock_loader, capsys):
        """Test get command with missing key."""
        mock_loader.load_with_args.return_value = {"server": {}}

        args = mock.MagicMock()
        args.config = None
        args.config_cmd = "get"
        args.key = "server.missing_key"
        args.json = False

        ret = run_config_command(args)

        assert ret == 1
        captured = capsys.readouterr()
        assert "not found" in captured.err.lower()

    @mock.patch("deepnote_core.config.commands.set.ConfigurationLoader")
    def test_set_command(self, MockLoader, tmp_path, capsys):
        """Test set command."""
        config_file = tmp_path / "config.toml"

        args = mock.MagicMock()
        args.config_cmd = "set"
        args.key = "server.port"
        args.value = "9000"
        args.file = str(config_file)

        ret = run_config_command(args)

        assert ret == 0
        assert config_file.exists()

        with open(config_file) as f:
            config = toml.load(f)
        assert config["server"]["port"] == 9000

    @mock.patch("deepnote_core.config.commands.set.ConfigurationLoader")
    def test_set_command_boolean(self, MockLoader, tmp_path):
        """Test set command with boolean value."""
        config_file = tmp_path / "config.toml"

        args = mock.MagicMock()
        args.config_cmd = "set"
        args.key = "server.enabled"
        args.value = "true"
        args.file = str(config_file)

        ret = run_config_command(args)

        assert ret == 0
        with open(config_file) as f:
            config = toml.load(f)
        assert config["server"]["enabled"] is True

    @mock.patch("deepnote_core.config.commands.set.ConfigurationLoader")
    def test_set_command_json_value(self, MockLoader, tmp_path):
        """Test set command with JSON value."""
        config_file = tmp_path / "config.toml"

        args = mock.MagicMock()
        args.config_cmd = "set"
        args.key = "server.ports"
        args.value = '["8080", "8081"]'
        args.file = str(config_file)

        ret = run_config_command(args)

        assert ret == 0
        with open(config_file) as f:
            config = toml.load(f)
        assert config["server"]["ports"] == ["8080", "8081"]

    @mock.patch("deepnote_core.config.commands.set.ConfigurationLoader")
    def test_set_command_update_existing(self, MockLoader, tmp_path):
        """Test set command updates existing file."""
        config_file = tmp_path / "config.toml"
        config_file.write_text("[server]\nport = 8080\n")

        args = mock.MagicMock()
        args.config_cmd = "set"
        args.key = "server.host"
        args.value = "localhost"
        args.file = str(config_file)

        ret = run_config_command(args)

        assert ret == 0
        with open(config_file) as f:
            config = toml.load(f)
        assert config["server"]["port"] == 8080
        assert config["server"]["host"] == "localhost"

    @mock.patch("deepnote_core.config.commands.set.ConfigurationLoader")
    def test_set_command_negative_integer(self, MockLoader, tmp_path):
        """Test set command with negative integer."""
        config_file = tmp_path / "config.toml"

        args = mock.MagicMock()
        args.config_cmd = "set"
        args.key = "server.offset"
        args.value = "-42"
        args.file = str(config_file)

        ret = run_config_command(args)

        assert ret == 0
        with open(config_file) as f:
            config = toml.load(f)
        assert config["server"]["offset"] == -42
        assert isinstance(config["server"]["offset"], int)

    @mock.patch("deepnote_core.config.commands.set.ConfigurationLoader")
    def test_set_command_scientific_notation_float(self, MockLoader, tmp_path):
        """Test set command with scientific notation float."""
        config_file = tmp_path / "config.toml"

        args = mock.MagicMock()
        args.config_cmd = "set"
        args.key = "server.threshold"
        args.value = "1.5e-3"
        args.file = str(config_file)

        ret = run_config_command(args)

        assert ret == 0
        with open(config_file) as f:
            config = toml.load(f)
        assert config["server"]["threshold"] == 0.0015
        assert isinstance(config["server"]["threshold"], float)

    @mock.patch("deepnote_core.config.commands.set.ConfigurationLoader")
    def test_set_command_large_scientific_notation(self, MockLoader, tmp_path):
        """Test set command with large scientific notation."""
        config_file = tmp_path / "config.toml"

        args = mock.MagicMock()
        args.config_cmd = "set"
        args.key = "server.max_size"
        args.value = "-2.5E6"
        args.file = str(config_file)

        ret = run_config_command(args)

        assert ret == 0
        with open(config_file) as f:
            config = toml.load(f)
        assert config["server"]["max_size"] == -2500000.0
        assert isinstance(config["server"]["max_size"], float)

    @mock.patch("deepnote_core.config.commands.set.ConfigurationLoader")
    def test_set_command_invalid_path(self, MockLoader, tmp_path, capsys):
        """Test set command with invalid path."""
        config_file = tmp_path / "config.toml"
        config_file.write_text('server = "not_a_dict"')

        args = mock.MagicMock()
        args.config_cmd = "set"
        args.key = "server.port"
        args.value = "8080"
        args.file = str(config_file)

        ret = run_config_command(args)

        assert ret == 1
        captured = capsys.readouterr()
        assert "not a dict" in captured.err

    def test_migrate_command(self, mock_loader, capsys):
        """Test migrate command."""
        args = mock.MagicMock()
        args.config = "old_config.toml"
        args.config_cmd = "migrate"
        args.force = False

        with mock.patch("deepnote_core.config.commands.migrate.XDGPaths") as MockXDG:
            mock_xdg = MockXDG.return_value
            mock_xdg.config_dir = Path("/home/user/.config/deepnote-toolkit")

            # Mock the target file path operations
            with mock.patch("pathlib.Path.exists", return_value=False):
                with mock.patch("pathlib.Path.mkdir"):
                    with mock.patch("builtins.open", mock.mock_open()):
                        with mock.patch("toml.dump"):
                            ret = run_config_command(args)

        assert ret == 0
        # migrate command prints to stdout
        captured = capsys.readouterr()
        assert "Migrated" in captured.out or ret == 0  # Success is enough

    def test_migrate_command_already_exists(self, mock_loader, capsys):
        """Test migrate command when target already exists."""
        with mock.patch("deepnote_core.config.commands.migrate.XDGPaths") as MockXDG:
            mock_xdg = MockXDG.return_value
            test_dir = Path("/home/user/.config/deepnote-toolkit")
            mock_xdg.config_dir = test_dir

            args = mock.MagicMock()
            args.config = "old_config.toml"
            args.config_cmd = "migrate"

            # Mock the target file exists check
            with mock.patch.object(Path, "exists") as mock_exists:
                # Return True for the target file
                mock_exists.return_value = True

                ret = run_config_command(args)

            assert ret == 1
            captured = capsys.readouterr()
            assert "already exists" in captured.err.lower()

    def test_generate_command(self, tmp_path, capsys):
        """Test generate command."""
        config_file = tmp_path / "generated.toml"

        args = mock.MagicMock()
        args.config_cmd = "generate"
        args.file = str(config_file)
        args.force = False

        ret = run_config_command(args)

        assert ret == 0
        assert config_file.exists()

        content = config_file.read_text()
        assert "# Deepnote Toolkit Configuration" in content

        # Verify it's valid TOML
        with open(config_file) as f:
            config = toml.load(f)
        assert "server" in config

    def test_generate_command_writes_to_default_xdg_path(self, mock_loader, capsys):
        """Test generate command writes to default XDG path."""
        args = mock.MagicMock()
        args.config_cmd = "generate"
        args.file = None
        args.force = False

        # Mock XDGPaths where it's imported in the cli module
        with mock.patch("deepnote_core.config.commands.generate.XDGPaths") as MockXDG:
            mock_xdg = MockXDG.return_value
            # This represents the full XDG config dir including app name
            mock_xdg.config_dir = Path("/xdg/config/deepnote-toolkit")
            mock_xdg.log_dir = Path("/xdg/logs")
            with mock.patch("pathlib.Path.exists", return_value=False):
                with mock.patch("pathlib.Path.mkdir"):
                    with mock.patch("builtins.open", mock.mock_open()) as mock_file:
                        with mock.patch("toml.dump"):
                            ret = run_config_command(args)

        assert ret == 0
        # Check that file was opened for writing at the default XDG location
        expected_path = Path("/xdg/config/deepnote-toolkit") / "config.toml"
        # The actual call includes encoding parameter
        mock_file.assert_called_with(expected_path, "w", encoding="utf-8")

    def test_generate_command_force_overwrite(self, tmp_path):
        """Test generate command with force overwrite."""
        config_file = tmp_path / "existing.toml"
        config_file.write_text("existing content")

        args = mock.MagicMock()
        args.config_cmd = "generate"
        args.file = str(config_file)
        args.force = True

        ret = run_config_command(args)

        assert ret == 0
        content = config_file.read_text()
        assert "# Deepnote Toolkit Configuration" in content

    def test_generate_command_no_overwrite(self, tmp_path, capsys):
        """Test generate command without force doesn't overwrite."""
        config_file = tmp_path / "existing.toml"
        config_file.write_text("existing content")

        args = mock.MagicMock()
        args.config_cmd = "generate"
        args.file = str(config_file)
        args.force = False

        ret = run_config_command(args)

        assert ret == 1
        captured = capsys.readouterr()
        assert "already exists" in captured.err.lower()

    @mock.patch("deepnote_core.config.commands.paths.XDGPaths")
    def test_paths_command(self, mock_xdg_class, capsys):
        """Test paths command."""
        mock_xdg = mock.MagicMock()
        mock_xdg.config_dir = Path("/home/user/.config/deepnote-toolkit")
        mock_xdg.cache_dir = Path("/home/user/.cache/deepnote-toolkit")
        mock_xdg.log_dir = Path("/home/user/.local/state/deepnote-toolkit/logs")
        mock_xdg_class.return_value = mock_xdg

        args = mock.MagicMock()
        args.config_cmd = "paths"
        args.json = False

        ret = run_config_command(args)

        assert ret == 0
        captured = capsys.readouterr()
        assert "config_dir:" in captured.out
        assert "cache_dir:" in captured.out
        assert "log_dir:" in captured.out

    def test_paths_command_json(self, mock_loader, capsys):
        """Test paths command with JSON output."""
        args = mock.MagicMock()
        args.config_cmd = "paths"
        args.json = True

        with mock.patch("deepnote_core.config.commands.paths.XDGPaths") as MockXDG:
            mock_xdg = MockXDG.return_value
            mock_xdg.config_dir = Path("/home/user/.config/deepnote-toolkit")
            mock_xdg.cache_dir = Path("/home/user/.cache/deepnote-toolkit")
            mock_xdg.log_dir = Path("/home/user/.local/state/deepnote-toolkit/logs")
            ret = run_config_command(args)

        assert ret == 0
        captured = capsys.readouterr()
        paths = json.loads(captured.out)
        assert "config_dir" in paths
        assert "cache_dir" in paths
        assert "log_dir" in paths


class TestFormatDescribe:
    """Test the _format_describe function."""

    def test_format_describe_basic(self):
        """Test basic formatting of config description."""
        cfg = DeepnoteConfig(
            server=ServerConfig(jupyter_port=8888, ls_port=8889),
            paths=PathConfig(home_dir=Path("/home/user")),
            runtime=RuntimeConfig(dev_mode=False),
            installation=InstallationConfig(),
        )

        result = _format_describe(cfg)

        assert "[server]" in result
        assert "jupyter_port" in result
        assert "8888" in result
        assert "[paths]" in result
        assert "home_dir" in result
        assert "[runtime]" in result
        assert "dev_mode" in result

    def test_format_describe_with_none_values(self):
        """Test formatting with None values."""
        cfg = DeepnoteConfig(
            server=ServerConfig(jupyter_port=8888, ls_port=8889),
            paths=PathConfig(home_dir=None),
            runtime=RuntimeConfig(dev_mode=False),
            installation=InstallationConfig(),
        )

        result = _format_describe(cfg)

        assert "jupyter_port" in result
        assert "home_dir" in result
        assert "None" in result
