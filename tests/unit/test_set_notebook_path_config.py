import json
from unittest import mock


def test_set_notebook_path_uses_config_home_and_port(tmp_path, monkeypatch):
    # Prepare config: home_dir and jupyter_port, detached mode true
    cfg_path = tmp_path / "cfg.toml"
    home_dir = tmp_path / "home"
    (home_dir / "work" / "folder").mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(
        f"""
    [paths]
    home_dir = "{home_dir}"

    [server]
    jupyter_port = 9999

    [runtime]
    running_in_detached_mode = true
    """.strip()
    )

    monkeypatch.setenv("DEEPNOTE_CONFIG_FILE", str(cfg_path))

    # Fake ipykernel connection file
    monkeypatch.setattr(
        "ipykernel.connect.get_connection_file",
        lambda: str(tmp_path / "kernel-1234.json"),
    )

    # Mock requests.get to return a fake session list
    sessions = [
        {
            "kernel": {"id": "1234"},
            "path": "folder/notebook.ipynb",
            "name": "1:type:proj-xyz:rest",
        }
    ]
    mock_resp = mock.MagicMock()
    mock_resp.text = json.dumps(sessions)
    mock_resp.json.return_value = sessions

    with mock.patch("requests.get", return_value=mock_resp) as mock_get:
        # Avoid changing process CWD in test
        monkeypatch.setattr("os.chdir", lambda p: None)

        from deepnote_toolkit import env as dnenv
        from deepnote_toolkit.set_notebook_path import set_notebook_path

        set_notebook_path()

        mock_get.assert_called_once()
        assert mock_get.call_args[0][0] == "http://0.0.0.0:9999/api/sessions"
        # Project ID should be injected via env bridge (detached mode)
        assert dnenv.get_env("DEEPNOTE_PROJECT_ID") == "proj-xyz"

        # Clean up the project ID we set
        dnenv.unset_env("DEEPNOTE_PROJECT_ID")


def test_set_notebook_path_uses_explicit_notebook_root(tmp_path, monkeypatch):
    cfg_path = tmp_path / "cfg.toml"
    root_dir = tmp_path / "root"
    (root_dir / "x" / "y").mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(
        f"""
    [paths]
    notebook_root = "{root_dir}"

    [server]
    jupyter_port = 8888
    """.strip()
    )

    monkeypatch.setenv("DEEPNOTE_CONFIG_FILE", str(cfg_path))

    # Fake ipykernel connection file and sessions result
    monkeypatch.setattr(
        "ipykernel.connect.get_connection_file",
        lambda: str(tmp_path / "kernel-1234.json"),
    )
    sessions = [{"kernel": {"id": "1234"}, "path": "x/y/notebook.ipynb"}]
    mock_resp = mock.MagicMock()
    mock_resp.text = json.dumps(sessions)

    with mock.patch("requests.get", return_value=mock_resp):
        with mock.patch("os.chdir", lambda p: None):
            import sys

            from deepnote_toolkit.set_notebook_path import set_notebook_path

            # initialize sys.path baseline
            original_sys_path = sys.path[:]
            try:
                sys.path = ["/"]
                set_notebook_path()
                assert str(root_dir / "x" / "y") in sys.path
            finally:
                sys.path = original_sys_path
