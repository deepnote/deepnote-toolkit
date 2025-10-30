import pytest
from pydantic import ValidationError

from deepnote_core.config.loader import ConfigurationLoader
from deepnote_core.config.models import DeepnoteConfig


def test_port_validation_range():
    # Invalid port should raise
    with pytest.raises(ValidationError):
        DeepnoteConfig.model_validate({"server": {"jupyter_port": 80}})


def test_legacy_env_mapping(monkeypatch):
    monkeypatch.setenv("DEEPNOTE_JUPYTER_PORT", "9999")
    monkeypatch.setenv("DEEPNOTE_WEBAPP_URL", "https://app.example")
    cfg = ConfigurationLoader().load_config()
    assert cfg.server.jupyter_port == 9999
    assert cfg.runtime.webapp_url == "https://app.example"


def test_runtime_coerce_float_inverted_flag(monkeypatch):
    # When legacy flag is set, coerce_float becomes False
    monkeypatch.setenv("DEEPNOTE_DO_NOT_COERCE_FLOAT", "1")
    cfg = ConfigurationLoader().load_config()
    assert cfg.runtime.coerce_float is False


def test_loader_precedence_cli_over_env_over_file(tmp_path, monkeypatch):
    # File value: 7777
    config_file = tmp_path / "deepnote-toolkit.toml"
    config_file.write_text(
        """
    [server]
    jupyter_port = 7777
    """.strip()
    )

    # Env overlay: 8888
    monkeypatch.setenv("DEEPNOTE_SERVER__JUPYTER_PORT", "8888")

    # CLI override: 9999
    class Args:
        jupyter_port = 9999
        ls_port = None
        enable_terminals = True
        python_kernel_only = False
        start_jupyter = True
        start_ls = True
        start_streamlit_servers = False
        start_extra_servers = False
        start_servers = True
        root_dir = None
        home_dir = None
        log_dir = None
        work_mountpoint = "/datasets/_deepnote_work"
        venv_path = "~/venv"
        version = None
        index_url = None
        bundle_path = None
        cache_path = None
        run_in_detached_mode = False
        venv_without_pip = False

    loader = ConfigurationLoader(config_path=config_file)
    cfg = loader.load_with_args(Args())  # type: ignore
    assert cfg.server.jupyter_port == 9999


def test_runtime_loader_env_over_file(tmp_path, monkeypatch):
    config_file = tmp_path / "conf.toml"
    config_file.write_text(
        """
    [server]
    jupyter_port = 7777
    """.strip()
    )

    monkeypatch.setenv("DEEPNOTE_CONFIG_FILE", str(config_file))
    monkeypatch.setenv("DEEPNOTE_SERVER__JUPYTER_PORT", "8888")

    cfg = ConfigurationLoader().load_config()
    assert cfg.server.jupyter_port == 8888


def test_env_extra_servers_into_config(monkeypatch):
    # Simulate env-defined extra servers
    monkeypatch.setenv("DEEPNOTE_TOOLKIT_EXTRA_SERVER_1", "cmd-one")
    monkeypatch.setenv("DEEPNOTE_TOOLKIT_EXTRA_SERVER_2", "cmd-two")
    cfg = ConfigurationLoader().load_config()
    assert cfg.server.extra_servers == ["cmd-one", "cmd-two"]
