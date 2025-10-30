import types

from installer.module.config_adapter import deepnote_to_installer


def _cfg():
    return types.SimpleNamespace(
        paths=types.SimpleNamespace(
            work_mountpoint="/w",
            venv_path="/v",
            root_dir=None,
            home_dir=None,
            log_dir=None,
        ),
        installation=types.SimpleNamespace(
            version="1.0",
            index_url="https://idx",
            cache_path=None,
            bundle_path=None,
        ),
        server=types.SimpleNamespace(
            python_kernel_only=True,
            start_jupyter=True,
            start_ls=True,
            start_streamlit_servers=False,
            start_extra_servers=False,
            enable_terminals=True,
            jupyter_port=8888,
            ls_port=2087,
        ),
        runtime=types.SimpleNamespace(
            running_in_detached_mode=False,
            venv_without_pip=False,
        ),
    )


def test_deepnote_to_installer_basic():
    out = deepnote_to_installer(_cfg())  # type: ignore
    # Bundle-related fields (version/index_url) are handled separately via BundleConfig now.
    # Here we validate only the legacy InstallerConfig mapping.
    assert out.work_mountpoint == "/w"
    assert out.venv_path == "/v"
    assert out.python_kernel_only is True
    assert out.start_jupyter is True
    assert out.start_ls is True
    assert out.start_streamlit_servers is False
    assert out.start_extra_servers is False
    assert out.enable_terminals is True
    assert out.run_in_detached_mode is False
