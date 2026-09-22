import time
from pathlib import Path
from typing import Any

import pytest
import requests
import responses

from deepnote_toolkit.notebooks import RunnerError
from deepnote_toolkit.streamlit import StreamlitCloudRunner, auth
from tests.unit.helpers.notebook_api import Clock, add_run, body, run_response, session
from tests.unit.helpers.streamlit_runtime import FakeStreamlitRuntime

APP_ID = "11111111-2222-3333-4444-555555555555"
TOKEN_URL = f"http://localhost:19456/userpod-api/streamlit-apps/{APP_ID}/api-token"
VIEWER_TOKEN = {
    "token": "viewer",
    "apiOrigin": "https://api.deepnote.com",
    "expiresAtSeconds": time.time() + 900,
}


@pytest.fixture(autouse=True)
def env(monkeypatch):
    monkeypatch.delenv("DEEPNOTE_STREAMLIT_APP_ID", raising=False)
    monkeypatch.delenv("DEEPNOTE_PROJECT_ID", raising=False)
    monkeypatch.setenv("DEEPNOTE_TOKEN", "owner-token")


@pytest.fixture
def http():
    with responses.RequestsMock() as mock:
        yield mock


def hosted(**overrides):
    return FakeStreamlitRuntime(
        **{"app": APP_ID, "cookie": "viewer-cookie", **overrides}
    )


@pytest.mark.parametrize(
    "explicit", [{}, {"token": "owner"}, {"token_provider": lambda: "owner"}]
)
def test_script_thread_without_local_mode_fails_closed(http, explicit):
    runner = StreamlitCloudRunner(
        "n", session=session(), runtime=FakeStreamlitRuntime(), **explicit
    )
    with pytest.raises(RunnerError, match="app ID"):
        runner.run({})
    assert len(http.calls) == 0


@pytest.mark.parametrize("local", [False, True])
def test_hosted_run_uses_viewer_and_readonly_even_with_explicit_owner_token(
    http, local
):
    http.post(
        TOKEN_URL,
        json={**VIEWER_TOKEN, "apiOrigin": "https://api.deepnote-staging.com"},
    )
    add_run(
        http,
        run_response(snapshotBlocks=[]),
        create=True,
        origin="https://api.deepnote-staging.com",
    )
    runner = StreamlitCloudRunner(
        "n",
        token="owner",
        base_url="https://wrong.example",
        local=local,
        session=session(),
        runtime=hosted(),
    )
    assert runner.run({}).success
    assert http.calls[0].request.headers["StreamlitToken"] == "viewer-cookie"
    assert http.calls[1].request.headers["Authorization"] == "Bearer viewer"
    assert body(http.calls[1])["detachedRunStorageMode"] == "readonly"


@pytest.mark.parametrize("explicit", [{}, {"local": True, "token": "owner"}])
def test_project_marker_fails_closed_without_an_app_id(http, monkeypatch, explicit):
    monkeypatch.setenv("DEEPNOTE_PROJECT_ID", "project")
    runner = StreamlitCloudRunner(
        "n", session=session(), runtime=FakeStreamlitRuntime(), **explicit
    )
    with pytest.raises(RunnerError, match="app ID"):
        runner.run({})
    assert len(http.calls) == 0


@pytest.mark.parametrize("ambient_auth", ["netrc", "session"])
def test_resolved_viewer_token_overrides_requests_auth(
    http: responses.RequestsMock,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    ambient_auth: str,
) -> None:
    """Keep the viewer bearer authoritative over ambient Requests authentication."""
    http.post(TOKEN_URL, json=VIEWER_TOKEN)
    add_run(http, run_response(snapshotBlocks=[]), create=True)
    transport = requests.Session()
    if ambient_auth == "netrc":
        netrc = tmp_path / "credentials.netrc"
        netrc.write_text("machine api.deepnote.com login unrelated password dummy\n")
        monkeypatch.setenv("NETRC", str(netrc))
    else:

        def owner_auth(request: requests.PreparedRequest) -> requests.PreparedRequest:
            request.headers["Authorization"] = "Bearer owner"
            return request

        transport.auth = owner_auth

    with transport:
        result = StreamlitCloudRunner("n", session=transport, runtime=hosted()).run({})

    assert result.success
    assert http.calls[1].request.headers["Authorization"] == "Bearer viewer"
    assert transport.trust_env is True


@pytest.mark.parametrize("marker", ["", "invalid", APP_ID])
def test_malformed_app_marker_never_falls_back(http, marker):
    if marker == APP_ID:
        http.post(
            TOKEN_URL,
            status=403,
            json={"message": "API access is not available for this app"},
        )
    runner = StreamlitCloudRunner(
        "n", token="owner", local=True, session=session(), runtime=hosted(app=marker)
    )
    with pytest.raises(RunnerError):
        runner.info()
    assert all(c.request.url == TOKEN_URL for c in http.calls)


@pytest.mark.parametrize(
    "explicit", [{}, {"token": "owner"}, {"token_provider": lambda: "owner"}]
)
def test_worker_thread_fails_closed(http, explicit):
    runtime = FakeStreamlitRuntime(request=False, worker=True)
    with pytest.raises(RunnerError, match="No viewer request"):
        StreamlitCloudRunner("n", session=session(), runtime=runtime, **explicit).info()
    assert not http.calls


@pytest.mark.parametrize("script", [False, True])
def test_local_streamlit_requires_explicit_opt_in_and_token(
    http: responses.RequestsMock, script: bool
) -> None:
    """Explicit local credentials work with or without an active Streamlit script."""
    runtime = FakeStreamlitRuntime(request=script)
    http.get(
        "https://api.deepnote.com/v2/notebooks/n", json={"notebook": {"name": "N"}}
    )
    runner = StreamlitCloudRunner(
        "n", local=True, token="local", session=session(), runtime=runtime
    )
    assert runner.info().notebook == "N"
    assert http.calls[0].request.headers["Authorization"] == "Bearer local"
    with pytest.raises(RunnerError, match="explicitly"):
        StreamlitCloudRunner("n", local=True, session=session(), runtime=runtime).info()


@pytest.mark.parametrize("explicit", [{}, {"token": "owner"}, {"local": True}])
def test_bare_python_requires_explicit_local_credentials(
    http: responses.RequestsMock, explicit: dict[str, Any]
) -> None:
    """The Streamlit adapter cannot use an ambient owner token outside the runtime."""
    runtime = FakeStreamlitRuntime(request=False)
    with pytest.raises(RunnerError, match=r"local=True.*explicitly"):
        StreamlitCloudRunner("n", session=session(), runtime=runtime, **explicit).info()
    assert not http.calls


def test_transient_exchange_failure_during_poll_is_retried(http):
    http.post(TOKEN_URL, json=VIEWER_TOKEN)
    http.post(TOKEN_URL, status=503)
    http.post(TOKEN_URL, json=VIEWER_TOKEN)
    add_run(http, run_response("running"), create=True)
    add_run(http, run_response(snapshotBlocks=[]))
    clock = Clock()
    runner = StreamlitCloudRunner(
        "n",
        session=session(),
        clock=clock,
        sleep=clock.sleep,
        runtime=hosted(state=None),
    )
    assert runner.run({}).success
    assert len(http.calls) == 5


def test_real_streamlit_script_and_worker_keep_viewer_identity(
    monkeypatch, http, streamlit_app_test
):
    monkeypatch.setenv("DEEPNOTE_STREAMLIT_APP_ID", APP_ID)
    monkeypatch.setattr(auth, "read_streamlit_token_from_context", lambda: "cookie")
    http.post(TOKEN_URL, json=VIEWER_TOKEN)
    add_run(http, run_response(snapshotBlocks=[]), create=True)

    def app():
        import threading

        import streamlit as st

        from deepnote_toolkit.notebooks import RunnerError
        from deepnote_toolkit.streamlit import StreamlitCloudRunner

        runner = StreamlitCloudRunner("n", token="owner", local=True)
        st.session_state["success"] = runner.run({}).success
        errors = []

        def worker():
            try:
                runner.run({})
            except RunnerError as error:
                errors.append(str(error))

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join(timeout=5)
        st.session_state["worker_errors"] = errors

    at = streamlit_app_test.from_function(app).run()
    assert not at.exception
    assert at.session_state["success"] is True
    assert "No viewer request" in at.session_state["worker_errors"][0]
    assert len(http.calls) == 2
    assert http.calls[1].request.headers["Authorization"] == "Bearer viewer"
