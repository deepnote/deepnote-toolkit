from logging import FileHandler
from pathlib import Path

from deepnote_toolkit.get_webapp_url import get_absolute_userpod_api_url
from deepnote_toolkit.logging import LoggerManager


def reset_logger_singleton():
    """Reset LoggerManager singleton between tests."""
    LoggerManager.reset()


def test_logger_uses_config_log_dir(tmp_path, monkeypatch):
    # Prepare config file with custom log_dir
    cfg_path = tmp_path / "deepnote-toolkit.toml"
    log_dir = tmp_path / "logs"
    cfg_path.write_text(
        f"""
    [paths]
    log_dir = "{log_dir}"
    """.strip()
    )

    monkeypatch.setenv("DEEPNOTE_CONFIG_FILE", str(cfg_path))
    # Ensure env fixture default does not override file (Env > File precedence)
    monkeypatch.delenv("DEEPNOTE_PATHS__LOG_DIR", raising=False)
    reset_logger_singleton()

    logger = LoggerManager().get_logger()
    # Find FileHandler and ensure it points to our log_dir/helpers.log
    file_handlers = [
        h
        for h in logger.handlers
        if getattr(h, "baseFilename", "").endswith("helpers.log")
    ]
    assert file_handlers, "Expected a FileHandler for helpers.log"
    assert isinstance(file_handlers[0], FileHandler)
    assert Path(file_handlers[0].baseFilename).parent == log_dir


def test_get_webapp_url_uses_config(tmp_path, monkeypatch):
    # Clear any stale runtime in-memory env state (defensive - handled by fixture)
    from deepnote_toolkit import env as dnenv

    try:
        lock = getattr(dnenv, "_STATE_LOCK", None) or getattr(dnenv, "_LOCK", None)
        state = getattr(dnenv, "_STATE", None)
        if lock and state is not None:
            with lock:
                state.clear()
    except Exception:
        pass

    # Prepare config for detached mode with webapp_url and project_id
    cfg_path = tmp_path / "deepnote-toolkit.toml"
    cfg_path.write_text(
        """
    [runtime]
    running_in_detached_mode = true
    webapp_url = "https://webapp.example"
    project_id = "abc-123"
    """.strip()
    )
    monkeypatch.setenv("DEEPNOTE_CONFIG_FILE", str(cfg_path))

    url = get_absolute_userpod_api_url("foo")
    expected = "https://webapp.example/userpod-api/abc-123/foo"
    assert url == expected, f"Expected {expected}, got {url}"
