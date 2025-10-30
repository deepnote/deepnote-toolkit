"""Tests for XDGPaths configuration utilities."""

from pathlib import Path

from deepnote_core.config.xdg_paths import XDGPaths


class TestXDGPaths:
    """Tests for XDGPaths class."""

    def test_default_paths_without_env_vars(self, tmp_path, monkeypatch):
        """Test default XDG paths when no environment variables are set."""
        # Clear XDG environment variables
        monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
        monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
        monkeypatch.delenv("XDG_STATE_HOME", raising=False)
        monkeypatch.delenv("XDG_DATA_HOME", raising=False)

        # Set a fake home directory
        fake_home = tmp_path / "home"
        fake_home.mkdir()
        monkeypatch.setenv("HOME", str(fake_home))

        xdg = XDGPaths("test-app")

        # Check default paths follow XDG specification
        assert xdg.config_dir == fake_home / ".config" / "test-app"
        assert xdg.cache_dir == fake_home / ".cache" / "test-app"
        assert xdg.log_dir == fake_home / ".local" / "state" / "test-app" / "logs"
        assert xdg.data_home == fake_home / ".local" / "share" / "test-app"

    def test_paths_with_xdg_env_vars(self, tmp_path, monkeypatch):
        """Test XDG paths when environment variables are set."""
        # Set XDG environment variables
        xdg_config = tmp_path / "custom_config"
        xdg_cache = tmp_path / "custom_cache"
        xdg_state = tmp_path / "custom_state"
        xdg_data = tmp_path / "custom_data"

        xdg_config.mkdir()
        xdg_cache.mkdir()
        xdg_state.mkdir()
        xdg_data.mkdir()

        monkeypatch.setenv("XDG_CONFIG_HOME", str(xdg_config))
        monkeypatch.setenv("XDG_CACHE_HOME", str(xdg_cache))
        monkeypatch.setenv("XDG_STATE_HOME", str(xdg_state))
        monkeypatch.setenv("XDG_DATA_HOME", str(xdg_data))

        xdg = XDGPaths("test-app")

        # Check paths respect environment variables
        assert xdg.config_dir == xdg_config / "test-app"
        assert xdg.cache_dir == xdg_cache / "test-app"
        assert xdg.log_dir == xdg_state / "test-app" / "logs"
        assert xdg.data_home == xdg_data / "test-app"

    def test_tilde_expansion_in_env_vars(self, tmp_path, monkeypatch):
        """Test that ~ is properly expanded in XDG environment variables."""
        fake_home = tmp_path / "user_home"
        fake_home.mkdir()
        monkeypatch.setenv("HOME", str(fake_home))

        # Set XDG vars with tilde
        monkeypatch.setenv("XDG_CONFIG_HOME", "~/my_config")
        monkeypatch.setenv("XDG_CACHE_HOME", "~/my_cache")
        monkeypatch.setenv("XDG_STATE_HOME", "~/my_state")
        monkeypatch.setenv("XDG_DATA_HOME", "~/my_data")

        xdg = XDGPaths("test-app")

        # Check that tilde is expanded to actual home directory
        assert xdg.config_dir == fake_home / "my_config" / "test-app"
        assert xdg.cache_dir == fake_home / "my_cache" / "test-app"
        assert xdg.log_dir == fake_home / "my_state" / "test-app" / "logs"
        assert xdg.data_home == fake_home / "my_data" / "test-app"

    def test_app_name_in_paths(self):
        """Test that app name is properly included in all paths."""
        app_name = "deepnote-toolkit"
        xdg = XDGPaths(app_name)

        # All paths should include the app name
        assert app_name in str(xdg.config_dir)
        assert app_name in str(xdg.cache_dir)
        assert app_name in str(xdg.log_dir)
        assert app_name in str(xdg.data_home)

    def test_log_dir_uses_state_home(self, tmp_path, monkeypatch):
        """Test that log_dir uses XDG_STATE_HOME specification."""
        # Clear XDG environment variables to use defaults
        monkeypatch.delenv("XDG_STATE_HOME", raising=False)

        fake_home = tmp_path / "home"
        fake_home.mkdir()
        monkeypatch.setenv("HOME", str(fake_home))

        xdg = XDGPaths("test-app")

        # log_dir should be ~/.local/state/app-name/logs by default
        expected_log_dir = fake_home / ".local" / "state" / "test-app" / "logs"
        assert xdg.log_dir == expected_log_dir

        # Test with XDG_STATE_HOME set
        custom_state = tmp_path / "custom_state"
        custom_state.mkdir()
        monkeypatch.setenv("XDG_STATE_HOME", str(custom_state))

        xdg_with_env = XDGPaths("test-app")
        expected_custom_log_dir = custom_state / "test-app" / "logs"
        assert xdg_with_env.log_dir == expected_custom_log_dir

    def test_paths_are_pathlib_objects(self):
        """Test that all returned paths are pathlib.Path objects."""
        xdg = XDGPaths("test-app")

        assert isinstance(xdg.config_dir, Path)
        assert isinstance(xdg.cache_dir, Path)
        assert isinstance(xdg.log_dir, Path)
        assert isinstance(xdg.data_home, Path)

    def test_different_app_names(self):
        """Test that different app names produce different paths."""
        xdg1 = XDGPaths("app1")
        xdg2 = XDGPaths("app2")

        # Paths should be different for different app names
        assert xdg1.config_dir != xdg2.config_dir
        assert xdg1.cache_dir != xdg2.cache_dir
        assert xdg1.log_dir != xdg2.log_dir
        assert xdg1.data_home != xdg2.data_home

        # But structure should be the same
        assert xdg1.config_dir.parent == xdg2.config_dir.parent
        assert xdg1.cache_dir.parent == xdg2.cache_dir.parent
        assert xdg1.data_home.parent == xdg2.data_home.parent

    def test_mixed_env_var_scenarios(self, tmp_path, monkeypatch):
        """Test scenarios where only some XDG environment variables are set."""
        fake_home = tmp_path / "home"
        fake_home.mkdir()
        monkeypatch.setenv("HOME", str(fake_home))

        # Only set XDG_CONFIG_HOME
        custom_config = tmp_path / "custom_config"
        custom_config.mkdir()
        monkeypatch.setenv("XDG_CONFIG_HOME", str(custom_config))
        monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
        monkeypatch.delenv("XDG_STATE_HOME", raising=False)
        monkeypatch.delenv("XDG_DATA_HOME", raising=False)

        xdg = XDGPaths("test-app")

        # Config should use custom path, others should use defaults
        assert xdg.config_dir == custom_config / "test-app"
        assert xdg.cache_dir == fake_home / ".cache" / "test-app"
        assert xdg.log_dir == fake_home / ".local" / "state" / "test-app" / "logs"
        assert xdg.data_home == fake_home / ".local" / "share" / "test-app"

    def test_empty_env_var_falls_back_to_default(self, tmp_path, monkeypatch):
        """Test that empty environment variables fall back to defaults."""
        fake_home = tmp_path / "home"
        fake_home.mkdir()
        monkeypatch.setenv("HOME", str(fake_home))

        # Set empty XDG environment variables
        monkeypatch.setenv("XDG_CONFIG_HOME", "")
        monkeypatch.setenv("XDG_CACHE_HOME", "")
        monkeypatch.setenv("XDG_STATE_HOME", "")
        monkeypatch.setenv("XDG_DATA_HOME", "")

        xdg = XDGPaths("test-app")

        # Should fall back to defaults when env vars are empty
        assert xdg.config_dir == fake_home / ".config" / "test-app"
        assert xdg.cache_dir == fake_home / ".cache" / "test-app"
        assert xdg.log_dir == fake_home / ".local" / "state" / "test-app" / "logs"
        assert xdg.data_home == fake_home / ".local" / "share" / "test-app"

    def test_data_home_property_specifically(self, tmp_path, monkeypatch):
        """Test the data_home property specifically (newly added)."""
        fake_home = tmp_path / "home"
        fake_home.mkdir()
        monkeypatch.setenv("HOME", str(fake_home))

        # Test without XDG_DATA_HOME
        monkeypatch.delenv("XDG_DATA_HOME", raising=False)
        xdg = XDGPaths("test-app")
        assert xdg.data_home == fake_home / ".local" / "share" / "test-app"

        # Test with XDG_DATA_HOME
        custom_data = tmp_path / "my_data"
        custom_data.mkdir()
        monkeypatch.setenv("XDG_DATA_HOME", str(custom_data))
        xdg = XDGPaths("test-app")
        assert xdg.data_home == custom_data / "test-app"

        # Test with XDG_DATA_HOME containing tilde
        monkeypatch.setenv("XDG_DATA_HOME", "~/custom_data")
        xdg = XDGPaths("test-app")
        assert xdg.data_home == fake_home / "custom_data" / "test-app"
