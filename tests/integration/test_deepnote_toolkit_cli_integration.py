"""Integration tests for deepnote_toolkit CLI."""

import json
import tempfile
from pathlib import Path
from unittest import mock

import pytest
import toml
import yaml

from deepnote_toolkit.cli.main import main


@pytest.fixture
def temp_config_file():
    """Create a temporary config file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
        config = {
            "server": {
                "jupyter_port": 8888,
                "ls_port": 8889,
                "start_jupyter": True,
                "start_ls": True,
                "enable_terminals": True,
            },
            "paths": {
                "home_dir": str(Path.home()),
                "log_dir": "/tmp/deepnote/logs",
            },
            "runtime": {
                "running_in_detached_mode": False,
                "dev_mode": False,
            },
        }
        toml.dump(config, f)
        temp_path = Path(f.name)

    try:
        yield temp_path
    finally:
        if temp_path.exists():
            temp_path.unlink()


class TestCLIIntegration:
    """Integration tests for CLI commands."""

    def test_server_start_with_config_file(self, temp_config_file):
        """Test server start with config file."""
        # Mock the actual server execution
        with (
            mock.patch("deepnote_toolkit.cli.server.run_actions_pip") as mock_run,
            mock.patch("deepnote_toolkit.cli.server.build_server_plan") as mock_plan,
            mock.patch("time.sleep", side_effect=KeyboardInterrupt),
        ):
            mock_run.return_value = []
            mock_plan.return_value = []

            ret = main(["server", "--config", str(temp_config_file)])

            assert ret == 0
            mock_plan.assert_called_once()
            mock_run.assert_called_once()

    def test_config_show_command(self, temp_config_file, capsys):
        """Test config show command."""
        ret = main(["config", "show", "--config", str(temp_config_file)])

        assert ret == 0
        captured = capsys.readouterr()
        config_output = json.loads(captured.out)

        assert config_output["server"]["jupyter_port"] == 8888
        assert config_output["server"]["ls_port"] == 8889

    def test_config_show_yaml_format(self, temp_config_file, capsys):
        """Test config show with YAML format."""
        ret = main(
            ["config", "show", "--format", "yaml", "--config", str(temp_config_file)]
        )

        assert ret == 0
        captured = capsys.readouterr()
        config_output = yaml.safe_load(captured.out)

        assert config_output["server"]["jupyter_port"] == 8888

    def test_config_show_toml_format(self, temp_config_file, capsys):
        """Test config show with TOML format."""
        ret = main(
            ["config", "show", "--format", "toml", "--config", str(temp_config_file)]
        )

        assert ret == 0
        captured = capsys.readouterr()
        config_output = toml.loads(captured.out)

        assert config_output["server"]["jupyter_port"] == 8888

    def test_config_get_command(self, temp_config_file, capsys):
        """Test config get command."""
        ret = main(
            ["config", "get", "server.jupyter_port", "--config", str(temp_config_file)]
        )

        assert ret == 0
        captured = capsys.readouterr()
        assert "8888" in captured.out

    def test_config_get_nested_value(self, temp_config_file, capsys):
        """Test getting nested config value."""
        ret = main(
            [
                "config",
                "get",
                "runtime.dev_mode",
                "--json",
                "--config",
                str(temp_config_file),
            ]
        )

        assert ret == 0
        captured = capsys.readouterr()
        value = json.loads(captured.out)
        assert value is False

    def test_config_get_json_output(self, temp_config_file, capsys):
        """Test config get with JSON output."""
        ret = main(
            [
                "config",
                "get",
                "server.jupyter_port",
                "--json",
                "--config",
                str(temp_config_file),
            ]
        )

        assert ret == 0
        captured = capsys.readouterr()
        value = json.loads(captured.out)
        assert value == 8888

    def test_config_set_command(self, tmp_path):
        """Test config set command."""
        config_file = tmp_path / "config.toml"

        # Set a value
        ret = main(
            ["config", "set", "server.jupyter_port", "9999", "--file", str(config_file)]
        )

        assert ret == 0
        assert config_file.exists()

        # Verify the value was set
        with open(config_file) as f:
            config = toml.load(f)
        assert config["server"]["jupyter_port"] == 9999

    def test_config_set_nested_value(self, tmp_path):
        """Test setting nested config value."""
        config_file = tmp_path / "config.toml"

        # Set nested values
        ret = main(
            [
                "config",
                "set",
                "server.extra_config.custom_value",
                "test",
                "--file",
                str(config_file),
            ]
        )

        assert ret == 0

        with open(config_file) as f:
            config = toml.load(f)
        assert config["server"]["extra_config"]["custom_value"] == "test"

    def test_config_set_boolean_value(self, tmp_path):
        """Test setting boolean values."""
        config_file = tmp_path / "config.toml"

        # Set boolean true
        ret = main(
            [
                "config",
                "set",
                "server.start_jupyter",
                "true",
                "--file",
                str(config_file),
            ]
        )
        assert ret == 0

        with open(config_file) as f:
            config = toml.load(f)
        assert config["server"]["start_jupyter"] is True

        # Set boolean false
        ret = main(
            [
                "config",
                "set",
                "server.start_jupyter",
                "false",
                "--file",
                str(config_file),
            ]
        )
        assert ret == 0

        with open(config_file) as f:
            config = toml.load(f)
        assert config["server"]["start_jupyter"] is False

    def test_config_set_numeric_values(self, tmp_path):
        """Test setting numeric values."""
        config_file = tmp_path / "config.toml"

        # Set integer
        ret = main(["config", "set", "server.port", "8080", "--file", str(config_file)])
        assert ret == 0

        # Set negative integer
        ret = main(
            ["config", "set", "server.timeout", "-1", "--file", str(config_file)]
        )
        assert ret == 0

        # Set float
        ret = main(["config", "set", "server.ratio", "1.5", "--file", str(config_file)])
        assert ret == 0

        with open(config_file) as f:
            config = toml.load(f)
        assert config["server"]["port"] == 8080
        assert config["server"]["timeout"] == -1
        assert config["server"]["ratio"] == 1.5

    def test_config_set_json_values(self, tmp_path):
        """Test setting JSON array and object values."""
        config_file = tmp_path / "config.toml"

        # Set array
        ret = main(
            [
                "config",
                "set",
                "server.ports",
                '["8080", "8081", "8082"]',
                "--file",
                str(config_file),
            ]
        )
        assert ret == 0

        # Set object
        ret = main(
            [
                "config",
                "set",
                "server.metadata",
                '{"version": "1.0", "enabled": true}',
                "--file",
                str(config_file),
            ]
        )
        assert ret == 0

        with open(config_file) as f:
            config = toml.load(f)
        assert config["server"]["ports"] == ["8080", "8081", "8082"]
        assert config["server"]["metadata"] == {"version": "1.0", "enabled": True}

    def test_config_set_null_value(self, tmp_path):
        """Test setting null value."""
        config_file = tmp_path / "config.toml"

        # First set a value
        main(["config", "set", "server.optional", "value", "--file", str(config_file)])

        # Then set it to null
        ret = main(
            ["config", "set", "server.optional", "null", "--file", str(config_file)]
        )
        assert ret == 0

        with open(config_file) as f:
            config = toml.load(f)
        assert config["server"]["optional"] is None

    def test_config_validate_valid(self, temp_config_file):
        """Test config validate with valid config."""
        ret = main(["config", "validate", "--config", str(temp_config_file)])
        assert ret == 0

    def test_config_validate_invalid(self, tmp_path, capsys):
        """Test config validate with invalid config."""
        config_file = tmp_path / "invalid.toml"
        config_file.write_text(
            """
[server]
jupyter_port = "not_a_number"
        """
        )

        ret = main(["config", "validate", "--config", str(config_file)])
        assert ret == 1
        captured = capsys.readouterr()
        assert "Validation failed" in captured.err

    def test_config_generate_command(self, tmp_path):
        """Test config generate command."""
        config_file = tmp_path / "generated.toml"

        ret = main(["config", "generate", "--file", str(config_file)])

        assert ret == 0
        assert config_file.exists()

        with open(config_file) as f:
            config = toml.load(f)

        assert "server" in config
        assert "paths" in config
        assert "runtime" in config

    def test_config_generate_force_overwrite(self, tmp_path):
        """Test config generate with force overwrite."""
        config_file = tmp_path / "existing.toml"
        config_file.write_text("existing content")

        # Without force should fail
        ret = main(["config", "generate", "--file", str(config_file)])
        assert ret == 1

        # With force should succeed
        ret = main(["config", "generate", "--file", str(config_file), "--force"])
        assert ret == 0

        with open(config_file) as f:
            content = f.read()
            assert "# Deepnote Toolkit Configuration" in content

    def test_config_describe_command(self, temp_config_file, capsys):
        """Test config describe command."""
        ret = main(["config", "describe", "--config", str(temp_config_file)])

        assert ret == 0
        captured = capsys.readouterr()

        # Check for section headers
        assert "[server]" in captured.out
        assert "[paths]" in captured.out
        assert "[runtime]" in captured.out

        # Check for field descriptions
        assert "jupyter_port" in captured.out
        assert "8888" in captured.out  # The value

    def test_config_paths_command(self, capsys):
        """Test config paths command."""
        ret = main(["config", "paths"])

        assert ret == 0
        captured = capsys.readouterr()

        assert "config_dir:" in captured.out
        assert "cache_dir:" in captured.out
        assert "log_dir:" in captured.out

    def test_config_paths_json_output(self, capsys):
        """Test config paths with JSON output."""
        ret = main(["config", "paths", "--json"])

        assert ret == 0
        captured = capsys.readouterr()
        paths = json.loads(captured.out)

        assert "config_dir" in paths
        assert "cache_dir" in paths
        assert "log_dir" in paths

    def test_config_migrate_command(self, tmp_path, monkeypatch):
        """Test config migrate command."""
        # Set XDG config dir to temp location
        config_dir = tmp_path / ".config" / "deepnote"
        monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / ".config"))

        # Create a legacy config
        legacy_config = tmp_path / "legacy.toml"
        legacy_config.write_text(
            """
[server]
jupyter_port = 7777
        """
        )

        # Run migrate
        ret = main(["config", "migrate", "--config", str(legacy_config)])

        assert ret == 0

        # Check migrated file exists
        migrated = config_dir / "config.toml"
        assert migrated.exists()

        with open(migrated) as f:
            config = toml.load(f)
        assert config["server"]["jupyter_port"] == 7777


class TestCLIErrorHandling:
    """Test CLI error handling."""

    def test_config_get_missing_key(self, temp_config_file, capsys):
        """Test getting non-existent config key."""
        ret = main(
            ["config", "get", "nonexistent.key", "--config", str(temp_config_file)]
        )

        assert ret == 1
        captured = capsys.readouterr()
        assert "not found" in captured.err.lower()

    def test_config_set_invalid_path(self, tmp_path, capsys):
        """Test setting value with invalid path."""
        config_file = tmp_path / "config.toml"

        # Create initial config with non-dict value
        config_file.write_text(
            """
server = "not_a_dict"
        """
        )

        ret = main(["config", "set", "server.port", "8080", "--file", str(config_file)])

        assert ret == 1
        captured = capsys.readouterr()
        assert "not a dict" in captured.err

    def test_server_start_missing_dependencies(self):
        """Test server start with missing dependencies."""
        with mock.patch("deepnote_toolkit.runtime.executor._need") as mock_need:
            mock_need.side_effect = SystemExit(1)

            ret = main(["server"])
            assert ret == 1


class TestCLIWithEnvironment:
    """Test CLI with environment variables."""

    def test_config_from_environment(self, tmp_path, monkeypatch, capsys):
        """Test loading config from environment variable."""
        config_file = tmp_path / "env_config.toml"
        config_file.write_text(
            """
[server]
jupyter_port = 5555
        """
        )

        monkeypatch.setenv("DEEPNOTE_CONFIG_FILE", str(config_file))

        ret = main(["config", "get", "server.jupyter_port"])

        assert ret == 0
        captured = capsys.readouterr()
        assert "5555" in captured.out

    def test_server_with_env_overrides(self, monkeypatch):
        """Test server start with environment overrides."""
        monkeypatch.setenv("DEEPNOTE_JUPYTER_PORT", "6666")
        monkeypatch.setenv("DEEPNOTE_LS_PORT", "6667")

        with (
            mock.patch("deepnote_toolkit.cli.server.run_actions_pip") as mock_run,
            mock.patch("deepnote_toolkit.cli.server.build_server_plan") as mock_plan,
            mock.patch("time.sleep", side_effect=KeyboardInterrupt),
        ):
            mock_run.return_value = []
            mock_plan.return_value = []

            ret = main(["server", "start"])

            assert ret == 0

            # Verify config was loaded with env overrides
            cfg = mock_plan.call_args[0][0]
            assert cfg.server.jupyter_port == 6666
            assert cfg.server.ls_port == 6667
