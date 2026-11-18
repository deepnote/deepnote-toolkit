import json
import os
import sys
import types
from unittest import mock

import pytest

from deepnote_toolkit import env as dnenv
from deepnote_toolkit.set_notebook_path import set_notebook_path


@pytest.fixture
def kernel(tmp_path, monkeypatch):
    """Kernel whose Jupyter session opens ``folder/notebook.ipynb``."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("DEEPNOTE_PROJECT_ID", raising=False)
    monkeypatch.setattr(
        "ipykernel.connect.get_connection_file",
        lambda: str(tmp_path / "kernel-1234.json"),
    )
    session = {"kernel": {"id": "1234"}, "path": "folder/notebook.ipynb", "name": ""}
    monkeypatch.setattr(
        "requests.get",
        lambda url, headers: mock.MagicMock(text=json.dumps([session])),
    )
    chdirs = []
    monkeypatch.setattr("os.chdir", chdirs.append)
    monkeypatch.setattr(sys, "path", ["", "/opt/site-packages"])

    yield types.SimpleNamespace(home=tmp_path, session=session, chdirs=chdirs)

    # set_notebook_path exports to os.environ, which monkeypatch does not restore
    dnenv.unset_env("DEEPNOTE_PROJECT_ID")


def test_detached_without_paths_keeps_kernel_cwd_and_exports_project_id(
    kernel, monkeypatch
):
    monkeypatch.setenv("DEEPNOTE_RUNTIME__RUNNING_IN_DETACHED_MODE", "true")
    monkeypatch.setenv("DEEPNOTE_RUNTIME__PROJECT_ID", "proj-1")
    kernel.session["name"] = "report-1234.deepnote"

    set_notebook_path()

    assert kernel.chdirs == []
    assert sys.path == ["", "/opt/site-packages"]
    assert os.environ.get("DEEPNOTE_PROJECT_ID") == "proj-1"


def test_detached_without_paths_or_project_id_ignores_session_name(kernel, monkeypatch):
    monkeypatch.setenv("DEEPNOTE_RUNTIME__RUNNING_IN_DETACHED_MODE", "true")
    monkeypatch.delenv("DEEPNOTE_RUNTIME__PROJECT_ID", raising=False)
    kernel.session["name"] = "sales:q1:report-1234.deepnote"

    set_notebook_path()

    assert kernel.chdirs == []
    assert sys.path == ["", "/opt/site-packages"]
    assert os.environ.get("DEEPNOTE_PROJECT_ID") is None


@pytest.mark.parametrize(
    "detached, dev_mode, notebook_dir",
    [
        ("false", "false", "{home}/work/folder"),
        ("false", "true", "/work/folder"),
        ("true", "true", "/work/folder"),
    ],
    ids=["attached", "dev-mode", "detached-dev-mode"],
)
def test_rewrites_sys_path_and_cwd_when_attached_or_in_dev_mode(
    kernel, monkeypatch, detached, dev_mode, notebook_dir
):
    monkeypatch.setenv("DEEPNOTE_RUNTIME__RUNNING_IN_DETACHED_MODE", detached)
    monkeypatch.setenv("DEEPNOTE_RUNTIME__DEV_MODE", dev_mode)
    notebook_dir = notebook_dir.format(home=kernel.home)
    kernel.session["name"] = "1:type:proj-xyz:rest"

    set_notebook_path()

    assert kernel.chdirs == [notebook_dir]
    assert sys.path == ["/opt/site-packages", notebook_dir]


def test_dev_mode_exports_configured_project_id_over_session_name(kernel, monkeypatch):
    monkeypatch.setenv("DEEPNOTE_RUNTIME__DEV_MODE", "true")
    monkeypatch.setenv("DEEPNOTE_RUNTIME__PROJECT_ID", "proj-1")
    kernel.session["name"] = "1:type:other-id:rest"

    set_notebook_path()

    assert os.environ.get("DEEPNOTE_PROJECT_ID") == "proj-1"


def test_set_notebook_path_uses_config_home_and_port(tmp_path, monkeypatch):
    # Prepare config: home_dir and jupyter_port, detached mode true
    cfg_path = tmp_path / "cfg.toml"
    home_dir = tmp_path / "home"
    (home_dir / "work" / "folder").mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(f"""
    [paths]
    home_dir = "{home_dir}"

    [server]
    jupyter_port = 9999

    [runtime]
    running_in_detached_mode = true
    """.strip())

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
    cfg_path.write_text(f"""
    [paths]
    notebook_root = "{root_dir}"

    [server]
    jupyter_port = 8888
    """.strip())

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
