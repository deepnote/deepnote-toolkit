import importlib
import json
import logging
import os
import types
from pathlib import Path
from unittest import mock

from deepnote_toolkit import env as dnenv
from deepnote_toolkit.set_notebook_path import set_notebook_path


def _cfg(jupyter_port=9999, notebook_root: str = "/tmp", detached=True, dev=False):
    return types.SimpleNamespace(
        server=types.SimpleNamespace(jupyter_port=jupyter_port),
        paths=types.SimpleNamespace(notebook_root=notebook_root, home_dir=None),
        runtime=types.SimpleNamespace(running_in_detached_mode=detached, dev_mode=dev),
    )


def test_set_notebook_path_updates_chdir_and_env(tmp_path, monkeypatch):
    # Simulate current kernel
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("DEEPNOTE_JUPYTER_TOKEN", "tok")

    # Fake connection file id
    # Patch ipykernel.connect directly instead of module attribute resolution inside our module
    monkeypatch.setattr(
        "ipykernel.connect.get_connection_file", lambda: "kernel-abc-123.json"
    )

    # Fake Jupyter sessions API
    sessions = [
        {"kernel": {"id": "abc-123"}, "name": "1:2:proj-xyz:more", "path": "nb/f.ipynb"}
    ]

    class DummyResp:
        def __init__(self, payload):
            self._payload = payload
            self.text = json.dumps(payload)
            self.status_code = 200
            self.ok = True

        def json(self):
            return self._payload

    # Patch module attributes via the imported module object to avoid package attribute masking
    mod = importlib.import_module("deepnote_toolkit.set_notebook_path")

    # Recorder for HTTP call details
    seen = {"url": None, "headers": None}

    def recorder(url, headers):
        seen["url"] = url
        seen["headers"] = headers
        return DummyResp(sessions)

    monkeypatch.setattr(mod.requests, "get", recorder)
    notebook_root = tmp_path / "root"
    notebook_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(
        mod, "get_config", lambda: _cfg(notebook_root=str(notebook_root))
    )

    # Capture chdir target
    changedir = {"path": None}

    def fake_chdir(p):
        changedir["path"] = p

    monkeypatch.setattr(os, "chdir", fake_chdir)

    # Ensure project id not preset
    dnenv.unset_env("DEEPNOTE_PROJECT_ID")

    # Call
    set_notebook_path()

    # Expect chdir to notebook directory under notebook_root
    assert changedir["path"] is not None
    assert Path(changedir["path"]) == notebook_root / "nb"
    # Expect detached mode to set project id from session name
    assert dnenv.get_env("DEEPNOTE_PROJECT_ID") == "proj-xyz"
    # Assert HTTP call used expected URL and token header
    assert seen["url"] == "http://0.0.0.0:9999/api/sessions"
    assert seen["headers"] is not None
    assert seen["headers"]["Authorization"] == "token tok"


def test_set_notebook_path_logs_error_on_failure(monkeypatch, capsys):
    """Test that exceptions are caught and logged properly."""
    mod = importlib.import_module("deepnote_toolkit.set_notebook_path")

    # Make get_connection_file raise an exception
    monkeypatch.setattr(
        "ipykernel.connect.get_connection_file",
        mock.Mock(side_effect=RuntimeError("No kernel connection")),
    )

    # Mock the logger to capture log calls
    mock_logger = mock.Mock(spec=logging.Logger)
    monkeypatch.setattr(mod, "get_logger", lambda: mock_logger)

    # Call - should not raise
    set_notebook_path()

    # Verify error was logged
    mock_logger.error.assert_called_once()
    call_args = mock_logger.error.call_args[0]
    assert "Failed to set notebook path" in call_args[0]
    assert isinstance(call_args[1], RuntimeError)
    assert "No kernel connection" in str(call_args[1])
