import types
from unittest import mock

import pytest

from deepnote_toolkit import env as dnenv
from deepnote_toolkit.set_integrations_env import set_integration_env


def _mock_cfg(enabled: bool) -> types.SimpleNamespace:
    return types.SimpleNamespace(
        runtime=types.SimpleNamespace(env_integration_enabled=enabled)
    )


@mock.patch(
    "deepnote_toolkit.set_integrations_env.get_config",
    side_effect=lambda: _mock_cfg(True),
)
def test_set_integration_env_success(mock_get_config, monkeypatch):  # noqa: ARG001
    # Fake session with two variables
    class DummyResp:
        ok = True

        def json(self):
            return [{"name": "X_A", "value": "1"}, {"name": "X_B", "value": "2"}]

    class DummySession:
        def mount(self, *a, **k):
            pass

        def get(self, *a, **k):
            return DummyResp()

    monkeypatch.setattr(
        "deepnote_toolkit.set_integrations_env.requests.Session", lambda: DummySession()
    )
    monkeypatch.setenv("DEEPNOTE_PROJECT_ID", "pid")
    # Clean env
    dnenv.unset_env("X_A")
    dnenv.unset_env("X_B")
    try:
        set_integration_env()
        assert dnenv.get_env("X_A") == "1"
        assert dnenv.get_env("X_B") == "2"
    finally:
        dnenv.unset_env("X_A")
        dnenv.unset_env("X_B")


@mock.patch(
    "deepnote_toolkit.set_integrations_env.get_config",
    side_effect=lambda: _mock_cfg(True),
)
def test_set_integration_env_http_error(mock_get_config, monkeypatch):  # noqa: ARG001
    class DummyResp:
        ok = False

        def json(self):
            return []

    class DummySession:
        def mount(self, *a, **k):
            pass

        def get(self, *a, **k):
            return DummyResp()

    monkeypatch.setattr(
        "deepnote_toolkit.set_integrations_env.requests.Session", lambda: DummySession()
    )
    monkeypatch.setenv("DEEPNOTE_PROJECT_ID", "pid")
    with pytest.raises(Exception, match="Failed to fetch integration variables"):
        set_integration_env()


def test_set_integration_env_disabled_gate(monkeypatch):
    # get_config returns disabled gate; Session should not be constructed
    from deepnote_toolkit import set_integrations_env as sie

    monkeypatch.setattr(sie, "get_config", lambda: _mock_cfg(False))

    class Boom:
        def __init__(self, *a, **k):  # noqa: ARG002
            raise AssertionError("Session should not be constructed when disabled")

    monkeypatch.setattr(sie.requests, "Session", Boom)
    # Ensure no env is set and no exception raised
    prev = dnenv.get_env("SHOULD_NOT_EXIST")
    sie.set_integration_env()
    assert dnenv.get_env("SHOULD_NOT_EXIST") == prev
