"""Unit tests for deepnote_core.runtime.plan module."""

from unittest import mock

from deepnote_core.runtime.plan import build_server_plan
from deepnote_core.runtime.types import (
    EnableJupyterTerminalsAction,
    ExtraServerSpec,
    JupyterServerSpec,
    PythonLSPSpec,
    StreamlitSpec,
)


class TestBuildServerPlan:
    """Test build_server_plan function."""

    def test_jupyter_server_basic(self):
        """Test basic Jupyter server configuration."""
        cfg = mock.MagicMock()
        cfg.server.start_jupyter = True
        cfg.server.jupyter_port = 8888
        cfg.server.enable_terminals = False
        cfg.server.start_ls = False
        cfg.server.start_streamlit_servers = False
        cfg.server.start_extra_servers = False
        cfg.installation.install_method = "pip"  # Not bundle, so allow_root=False

        actions = build_server_plan(cfg)

        assert len(actions) == 1
        assert isinstance(actions[0], JupyterServerSpec)
        assert actions[0].port == 8888
        assert actions[0].allow_root is False
        assert actions[0].enable_terminals is False
        assert actions[0].no_browser is True
        assert actions[0].host == "0.0.0.0"

    def test_jupyter_server_with_terminals(self):
        """Test Jupyter server with terminals enabled."""
        cfg = mock.MagicMock()
        cfg.server.start_jupyter = True
        cfg.server.jupyter_port = 8888
        cfg.server.enable_terminals = True
        cfg.server.start_ls = False
        cfg.server.start_streamlit_servers = False
        cfg.server.start_extra_servers = False
        cfg.installation.install_method = "bundle"  # bundle means allow_root=True

        actions = build_server_plan(cfg)

        assert len(actions) == 2
        assert isinstance(actions[0], EnableJupyterTerminalsAction)
        assert isinstance(actions[1], JupyterServerSpec)
        assert actions[1].allow_root is True
        assert actions[1].enable_terminals is True
        assert actions[1].no_browser is True
        assert actions[1].host == "0.0.0.0"

    def test_jupyter_server_with_root_dir(self):
        """Test Jupyter server with custom root directory."""
        from pathlib import Path

        cfg = mock.MagicMock()
        cfg.server.start_jupyter = True
        cfg.server.jupyter_port = 8888
        cfg.server.enable_terminals = False
        cfg.server.start_ls = False
        cfg.server.start_streamlit_servers = False
        cfg.server.start_extra_servers = False
        cfg.installation.install_method = "pip"
        cfg.paths.notebook_root = Path("/custom/root")

        actions = build_server_plan(cfg)

        assert len(actions) == 1
        assert isinstance(actions[0], JupyterServerSpec)
        assert actions[0].root_dir == "/custom/root"

    def test_python_lsp_server(self):
        """Test Python LSP server configuration."""
        cfg = mock.MagicMock()
        cfg.server.start_jupyter = False
        cfg.server.enable_terminals = False
        cfg.server.start_ls = True
        cfg.server.ls_port = 8889
        cfg.server.start_streamlit_servers = False
        cfg.server.start_extra_servers = False

        actions = build_server_plan(cfg)

        assert len(actions) == 1
        assert isinstance(actions[0], PythonLSPSpec)
        assert actions[0].port == 8889

    def test_streamlit_servers(self):
        """Test Streamlit server configuration."""
        cfg = mock.MagicMock()
        cfg.server.start_jupyter = False
        cfg.server.enable_terminals = False
        cfg.server.start_ls = False
        cfg.server.start_streamlit_servers = True
        cfg.server.streamlit_scripts = ["app.py", "dashboard.py"]
        cfg.server.start_extra_servers = False

        actions = build_server_plan(cfg)

        assert len(actions) == 2
        assert isinstance(actions[0], StreamlitSpec)
        assert actions[0].script == "app.py"
        assert isinstance(actions[1], StreamlitSpec)
        assert actions[1].script == "dashboard.py"

    def test_streamlit_with_non_string_items(self):
        """Test Streamlit with non-string items in list."""
        cfg = mock.MagicMock()
        cfg.server.start_jupyter = False
        cfg.server.enable_terminals = False
        cfg.server.start_ls = False
        cfg.server.start_streamlit_servers = True
        cfg.server.streamlit_scripts = ["app.py", None, 123, "dashboard.py"]
        cfg.server.start_extra_servers = False

        actions = build_server_plan(cfg)

        # Only string items should be included
        assert len(actions) == 2
        assert isinstance(actions[0], StreamlitSpec)
        assert isinstance(actions[1], StreamlitSpec)
        assert actions[0].script == "app.py"
        assert actions[1].script == "dashboard.py"

    def test_extra_servers_string_commands(self):
        """Test extra servers with string commands."""
        cfg = mock.MagicMock()
        cfg.server.start_jupyter = False
        cfg.server.enable_terminals = False
        cfg.server.start_ls = False
        cfg.server.start_streamlit_servers = False
        cfg.server.start_extra_servers = True
        cfg.server.extra_servers = [
            "python -m http.server 8000",
            "redis-server --port 6379",
        ]

        actions = build_server_plan(cfg)

        assert len(actions) == 2
        assert isinstance(actions[0], ExtraServerSpec)
        assert actions[0].command == ["python", "-m", "http.server", "8000"]
        assert isinstance(actions[1], ExtraServerSpec)
        assert actions[1].command == ["redis-server", "--port", "6379"]

    def test_extra_servers_list_commands(self):
        """Test extra servers with list/tuple commands."""
        cfg = mock.MagicMock()
        cfg.server.start_jupyter = False
        cfg.server.enable_terminals = False
        cfg.server.start_ls = False
        cfg.server.start_streamlit_servers = False
        cfg.server.start_extra_servers = True
        cfg.server.extra_servers = [
            ["python", "-m", "http.server", "8000"],
            ("redis-server", "--port", "6379"),
        ]

        actions = build_server_plan(cfg)

        assert len(actions) == 2
        assert isinstance(actions[0], ExtraServerSpec)
        assert actions[0].command == ["python", "-m", "http.server", "8000"]
        assert isinstance(actions[1], ExtraServerSpec)
        assert actions[1].command == ["redis-server", "--port", "6379"]

    def test_extra_servers_mixed_types(self):
        """Test extra servers with mixed command types."""
        cfg = mock.MagicMock()
        cfg.server.start_jupyter = False
        cfg.server.enable_terminals = False
        cfg.server.start_ls = False
        cfg.server.start_streamlit_servers = False
        cfg.server.start_extra_servers = True
        cfg.server.extra_servers = [
            "python -m http.server 8000",
            ["redis-server", "--port", "6379"],
            "",  # Empty string should be skipped
            ["", "  ", ""],  # List with only empty strings should be skipped
            None,  # None should be skipped
            123,  # Non-string/list should be skipped
        ]

        actions = build_server_plan(cfg)

        assert len(actions) == 2
        assert isinstance(actions[0], ExtraServerSpec)
        assert actions[0].command == ["python", "-m", "http.server", "8000"]
        assert isinstance(actions[1], ExtraServerSpec)
        assert actions[1].command == ["redis-server", "--port", "6379"]

    def test_all_servers_combined(self):
        """Test all server types combined."""
        cfg = mock.MagicMock()
        cfg.server.start_jupyter = True
        cfg.server.jupyter_port = 8888
        cfg.server.enable_terminals = True
        cfg.server.start_ls = True
        cfg.server.ls_port = 8889
        cfg.server.start_streamlit_servers = True
        cfg.server.streamlit_scripts = ["app.py"]
        cfg.server.start_extra_servers = True
        cfg.server.extra_servers = ["custom-server --port 9000"]
        cfg.installation.install_method = "pip"  # Not bundle

        actions = build_server_plan(cfg)

        assert len(actions) == 5
        assert isinstance(actions[0], EnableJupyterTerminalsAction)
        assert isinstance(actions[1], JupyterServerSpec)
        assert actions[1].no_browser is True
        assert actions[1].host == "0.0.0.0"
        assert isinstance(actions[2], PythonLSPSpec)
        assert isinstance(actions[3], StreamlitSpec)
        assert isinstance(actions[4], ExtraServerSpec)

    def test_no_servers_enabled(self):
        """Test when no servers are enabled."""
        cfg = mock.MagicMock()
        cfg.server.start_jupyter = False
        cfg.server.enable_terminals = False
        cfg.server.start_ls = False
        cfg.server.start_streamlit_servers = False
        cfg.server.start_extra_servers = False

        actions = build_server_plan(cfg)

        assert actions == []

    def test_missing_optional_attributes(self):
        """Test that Pydantic models provide proper defaults."""
        # Use actual Pydantic models to test default behavior
        from deepnote_core.config.models import (
            DeepnoteConfig,
            InstallationConfig,
            ServerConfig,
        )

        # Create config with minimal required fields
        # All boolean fields will use their defaults
        cfg = DeepnoteConfig(
            server=ServerConfig(),  # Uses all defaults
            installation=InstallationConfig(install_method="pip"),
        )

        actions = build_server_plan(cfg)

        # With defaults: enable_terminals=True, start_jupyter=True, start_ls=True
        assert len(actions) == 3
        assert isinstance(actions[0], EnableJupyterTerminalsAction)
        assert isinstance(actions[1], JupyterServerSpec)
        assert isinstance(actions[2], PythonLSPSpec)
        assert actions[1].allow_root is False  # pip install method
        assert actions[1].enable_terminals is True
        assert actions[2].port == 2087  # Default ls_port

    def test_none_values_in_lists(self):
        """Test handling of None values in server lists."""
        cfg = mock.MagicMock()
        cfg.server.start_jupyter = False
        cfg.server.enable_terminals = False
        cfg.server.start_ls = False
        cfg.server.start_streamlit_servers = True
        cfg.server.streamlit_scripts = None  # None instead of list
        cfg.server.start_extra_servers = True
        cfg.server.extra_servers = None  # None instead of list

        actions = build_server_plan(cfg)

        assert actions == []
