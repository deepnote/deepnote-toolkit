import base64
import importlib
from unittest import mock

from deepnote_toolkit import env as dnenv


def test_post_start_hooks_sets_env(monkeypatch):
    mod = importlib.import_module("deepnote_toolkit.execute_post_start_hooks")

    # Capture tunnel args
    seen_tunnel = {}

    def fake_create_ssh_tunnel(**kwargs):
        seen_tunnel.update(kwargs)
        return mock.MagicMock(local_bind_host="127.0.0.1", local_bind_port=1234)

    # Prepare a dummy tunnel server
    monkeypatch.setattr(mod, "create_ssh_tunnel", fake_create_ssh_tunnel)

    # Fake code hooks response via the module's requests.Session
    class S:
        def mount(self, *a, **k):
            pass

        def get(self, url, timeout, headers):  # noqa: ARG002
            class R:
                def json(self):
                    return [
                        {
                            "id": "iid",
                            "type": "ssh_tunnel",
                            "metadata": {
                                "sshHost": "h",
                                "sshPort": 22,
                                "sshUser": "u",
                                "remoteHost": "rh",
                                "remotePort": 5432,
                            },
                            "sshPrivateKey": base64.b64encode(b"k").decode("utf-8"),
                            "envVariablePrefix": "DN",
                        }
                    ]

            return R()

    # Record last relative path used for URL construction
    seen_url = {"last": None}

    def fake_get_url(p):
        seen_url["last"] = p
        return f"http://u/{p}"

    monkeypatch.setattr(mod, "get_absolute_userpod_api_url", fake_get_url)
    monkeypatch.setattr(mod.requests, "Session", lambda: S())

    # Ensure clean env and restore after
    try:
        dnenv.unset_env("DN_LOCAL_HOST")
        dnenv.unset_env("DN_LOCAL_PORT")
        mod.execute_post_start_hooks()
        assert dnenv.get_env("DN_LOCAL_HOST") == "127.0.0.1"
        assert dnenv.get_env("DN_LOCAL_PORT") == "1234"
        # Validate tunnel args
        assert seen_tunnel["ssh_host"] == "h"
        assert seen_tunnel["ssh_port"] == 22
        assert seen_tunnel["ssh_user"] == "u"
        assert seen_tunnel["remote_host"] == "rh"
        assert seen_tunnel["remote_port"] == 5432
        # Validate last URL seen was errors endpoint for the code hook id
        assert seen_url["last"] is not None
        assert seen_url["last"].startswith("integrations/code-hooks")
    finally:
        dnenv.unset_env("DN_LOCAL_HOST")
        dnenv.unset_env("DN_LOCAL_PORT")
