import time
from typing import Any

import pytest
import responses

from deepnote_toolkit.notebooks import RunnerError
from deepnote_toolkit.streamlit import StreamlitCloudRunner, auth
from deepnote_toolkit.streamlit import viewer_credentials as viewer
from tests.unit.helpers.notebook_api import Clock, add_run, body, run_response, session

APP_ID = "11111111-2222-3333-4444-555555555555"
TOKEN_URL = f"http://localhost:19456/userpod-api/streamlit-apps/{APP_ID}/api-token"


@pytest.fixture
def context(monkeypatch):
    monkeypatch.delenv("DEEPNOTE_STREAMLIT_APP_ID", raising=False)
    monkeypatch.delenv("DEEPNOTE_PROJECT_ID", raising=False)
    monkeypatch.setenv("DEEPNOTE_TOKEN", "owner-token")
    state = {"script": True, "hosted": False, "worker": False}
    monkeypatch.setattr(viewer, "_has_script_run_context", lambda: state["script"])
    monkeypatch.setattr(
        viewer, "_has_hosted_streamlit_context", lambda: state["hosted"]
    )
    monkeypatch.setattr(
        viewer, "_is_streamlit_thread_without_request", lambda: state["worker"]
    )
    monkeypatch.setattr(auth, "_read_streamlit_app_id_from_context", lambda: None)
    monkeypatch.setattr(
        auth, "read_streamlit_token_from_context", lambda: "viewer-cookie"
    )
    monkeypatch.setattr(auth, "_read_streamlit_session_state", lambda: {})
    return state


@pytest.fixture
def http():
    with responses.RequestsMock() as mock:
        yield mock


@pytest.mark.parametrize(
    "explicit", [{}, {"token": "owner"}, {"token_provider": lambda: "owner"}]
)
def test_missing_hosted_signals_fail_closed(context, http, explicit):
    with pytest.raises(RunnerError, match="app ID"):
        StreamlitCloudRunner("n", session=session(), **explicit).run({})
    assert len(http.calls) == 0


@pytest.mark.parametrize("marker", ["DEEPNOTE_STREAMLIT_APP_ID", "DEEPNOTE_PROJECT_ID"])
@pytest.mark.parametrize("local", [False, True])
def test_hosted_run_uses_viewer_and_readonly_even_with_explicit_owner_token(
    context, http, monkeypatch, marker, local
):
    monkeypatch.setenv(marker, APP_ID)
    monkeypatch.setattr(auth, "_read_streamlit_app_id_from_context", lambda: APP_ID)
    http.post(
        TOKEN_URL,
        json={
            "token": "viewer",
            "apiOrigin": "https://api.deepnote-staging.com",
            "expiresAtSeconds": time.time() + 900,
        },
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
    )
    assert runner.run({}).success
    assert http.calls[0].request.headers["StreamlitToken"] == "viewer-cookie"
    assert http.calls[1].request.headers["Authorization"] == "Bearer viewer"
    assert body(http.calls[1])["detachedRunStorageMode"] == "readonly"


@pytest.mark.parametrize("marker", ["", "invalid", APP_ID])
def test_malformed_app_marker_never_falls_back(context, http, monkeypatch, marker):
    monkeypatch.setenv("DEEPNOTE_STREAMLIT_APP_ID", marker)
    (
        http.post(
            TOKEN_URL,
            status=403,
            json={"message": "API access is not available for this app"},
        )
        if marker == APP_ID
        else None
    )
    with pytest.raises(RunnerError):
        StreamlitCloudRunner("n", token="owner", local=True, session=session()).info()
    assert all(c.request.url == TOKEN_URL for c in http.calls)


@pytest.mark.parametrize(
    "explicit", [{}, {"token": "owner"}, {"token_provider": lambda: "owner"}]
)
def test_worker_thread_fails_closed(context, http, explicit):
    context.update(script=False, worker=True)
    with pytest.raises(RunnerError, match="No viewer request"):
        StreamlitCloudRunner("n", session=session(), **explicit).info()
    assert not http.calls


@pytest.mark.parametrize("script", [False, True])
def test_local_streamlit_requires_explicit_opt_in_and_token(
    context: dict[str, bool], http: responses.RequestsMock, script: bool
) -> None:
    """Explicit local credentials work with or without an active Streamlit script."""
    context["script"] = script
    http.get(
        "https://api.deepnote.com/v2/notebooks/n", json={"notebook": {"name": "N"}}
    )
    runner = StreamlitCloudRunner("n", local=True, token="local", session=session())
    assert runner.info().notebook == "N"
    assert http.calls[0].request.headers["Authorization"] == "Bearer local"
    with pytest.raises(RunnerError, match="explicitly"):
        StreamlitCloudRunner("n", local=True, session=session()).info()


@pytest.mark.parametrize("explicit", [{}, {"token": "owner"}, {"local": True}])
def test_bare_python_requires_explicit_local_credentials(
    context: dict[str, bool], http: responses.RequestsMock, explicit: dict[str, Any]
) -> None:
    """The Streamlit adapter cannot use an ambient owner token outside the runtime."""
    context["script"] = False
    with pytest.raises(RunnerError, match=r"local=True.*explicitly"):
        StreamlitCloudRunner("n", session=session(), **explicit).info()
    assert not http.calls


def test_transient_exchange_failure_during_poll_is_retried(context, http, monkeypatch):
    monkeypatch.setenv("DEEPNOTE_STREAMLIT_APP_ID", APP_ID)
    payload = {
        "token": "viewer",
        "apiOrigin": "https://api.deepnote.com",
        "expiresAtSeconds": time.time() + 900,
    }
    http.post(TOKEN_URL, json=payload)
    http.post(TOKEN_URL, status=503)
    http.post(TOKEN_URL, json=payload)
    add_run(http, run_response("running"), create=True)
    add_run(http, run_response(snapshotBlocks=[]))
    clock = Clock()
    assert (
        StreamlitCloudRunner("n", session=session(), clock=clock, sleep=clock.sleep)
        .run({})
        .success
    )
    assert len(http.calls) == 5


def test_real_streamlit_script_and_worker_keep_viewer_identity(
    monkeypatch, http, streamlit_app_test
):

    monkeypatch.setenv("DEEPNOTE_STREAMLIT_APP_ID", APP_ID)
    monkeypatch.setenv("DEEPNOTE_TOKEN", "owner-token")
    monkeypatch.setattr(auth, "read_streamlit_token_from_context", lambda: "cookie")
    http.post(
        TOKEN_URL,
        json={
            "token": "viewer",
            "apiOrigin": "https://api.deepnote.com",
            "expiresAtSeconds": time.time() + 900,
        },
    )
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
