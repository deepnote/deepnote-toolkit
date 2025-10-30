import textwrap

from deepnote_toolkit.get_webapp_url import (
    get_absolute_notebook_functions_api_url,
    get_absolute_userpod_api_url,
    get_project_auth_headers,
)


def test_attached_mode_local_urls(tmp_path, monkeypatch):
    # Attached mode (not detached, not dev)
    cfg_path = tmp_path / "cfg.toml"
    cfg_path.write_text(
        textwrap.dedent(
            """
        [runtime]
        running_in_detached_mode = false
        dev_mode = false
    """
        ).strip()
    )
    monkeypatch.setenv("DEEPNOTE_CONFIG_FILE", str(cfg_path))

    assert (
        get_absolute_userpod_api_url("foo") == "http://localhost:19456/userpod-api/foo"
    )
    assert (
        get_absolute_notebook_functions_api_url("bar")
        == "http://localhost:19456/api/notebook-functions/bar"
    )
    assert get_project_auth_headers() == {}


def test_detached_headers_from_config(tmp_path, monkeypatch):
    cfg_path = tmp_path / "cfg.toml"
    cfg_path.write_text(
        textwrap.dedent(
            """
        [runtime]
        running_in_detached_mode = true
        project_id = "pid"
        project_secret = "sec"
        webapp_url = "https://wa.example"
    """
        ).strip()
    )
    monkeypatch.setenv("DEEPNOTE_CONFIG_FILE", str(cfg_path))

    assert get_project_auth_headers() == {
        "RuntimeUuid": "pid",
        "Authorization": "Bearer sec",
    }
    assert get_absolute_userpod_api_url("x") == "https://wa.example/userpod-api/pid/x"
    assert (
        get_absolute_notebook_functions_api_url("x")
        == "https://wa.example/api/notebook-functions/x"
    )
