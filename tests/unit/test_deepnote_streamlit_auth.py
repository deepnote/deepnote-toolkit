import sys
import time
from types import SimpleNamespace

import pytest
import requests
import responses

from deepnote_toolkit.streamlit import auth
from tests.unit.helpers.notebook_api import session

APP_ID = "3853c7f5-2048-4b57-946d-6c5592c3317e"
TOKEN_URL = f"http://localhost:19456/userpod-api/streamlit-apps/{APP_ID}/api-token"


@pytest.fixture
def state(monkeypatch):
    state = {}
    monkeypatch.delenv("DEEPNOTE_STREAMLIT_APP_ID", raising=False)
    monkeypatch.setattr(auth, "_read_streamlit_session_state", lambda: state)
    return state


@pytest.fixture
def http():
    with responses.RequestsMock() as mock:
        yield mock


def credentials(http_session, **kwargs):
    return auth.current_user_api_credentials(
        app_id=APP_ID, streamlit_token="cookie", session=http_session, **kwargs
    )


def payload(**overrides):
    return {
        "token": "viewer",
        "apiOrigin": "https://api.deepnote-staging.com/",
        "expiresAtSeconds": time.time() + 900,
        **overrides,
    }


def test_exchange_uses_cookie_and_reuses_credentials_only_in_same_session(http, state):
    http.post(TOKEN_URL, json=payload())
    transport = session()
    first = credentials(transport)
    assert credentials(transport) is first
    assert (
        first.token == "viewer"
        and first.api_origin == "https://api.deepnote-staging.com"
    )
    assert len(http.calls) == 1
    assert http.calls[0].request.headers["StreamlitToken"] == "cookie"
    assert "Authorization" not in http.calls[0].request.headers
    state.clear()
    assert credentials(transport) is not first
    assert len(http.calls) == 2


def test_changed_cookie_or_expiry_refreshes_credentials(http, state):
    http.post(TOKEN_URL, json=payload(expiresAtSeconds=time.time() + 30))
    http.post(TOKEN_URL, json=payload(token="second"))
    http.post(TOKEN_URL, json=payload(token="third"))
    transport = session()
    assert credentials(transport).token == "viewer"
    assert credentials(transport).token == "second"
    assert (
        auth.current_user_api_credentials(
            app_id=APP_ID, streamlit_token="changed", session=transport
        ).token
        == "third"
    )


@pytest.mark.parametrize("value", ["bad/path", "../apps", "", "x?query", "x#fragment"])
def test_explicit_app_id_is_validated_before_network(http, state, value):
    with pytest.raises(auth.CurrentUserApiTokenError):
        auth.current_user_api_credentials(
            app_id=value, streamlit_token="cookie", session=session()
        )
    assert not http.calls


@pytest.mark.parametrize(
    "overrides",
    [
        {"token": ""},
        {"token": 1},
        {"expiresAtSeconds": True},
        {"expiresAtSeconds": 0},
        {"expiresAtSeconds": "99999999999"},
        {"apiOrigin": "https://user:pass@example.com"},
        {"apiOrigin": "https://example.com/path"},
    ],
)
def test_malformed_credentials_are_not_cached(http, state, overrides):
    http.post(TOKEN_URL, json=payload(**overrides))
    with pytest.raises(auth.CurrentUserApiTokenError):
        credentials(session())
    assert state == {}


@pytest.mark.parametrize(
    "status,transient", [(401, False), (403, False), (429, True), (503, True)]
)
def test_exchange_preserves_server_reason_and_retry_classification(
    http, state, status, transient
):
    http.post(
        TOKEN_URL,
        status=status,
        json={"message": "API access is not available for this app"},
    )
    with pytest.raises(
        auth.CurrentUserApiTokenError, match="API access is not available"
    ) as exc:
        credentials(session())
    assert exc.value.transient is transient
    assert len(http.calls) == 1


@pytest.mark.parametrize(
    "failure", [requests.Timeout(), requests.ConnectionError("closed")]
)
def test_exchange_network_failures_are_transient(http, state, failure):
    http.post(TOKEN_URL, body=failure)
    with pytest.raises(auth.CurrentUserApiTokenError) as exc:
        credentials(session())
    assert exc.value.transient


def test_exchange_never_follows_redirects_or_exposes_html(http, state):
    http.post(TOKEN_URL, status=302, headers={"Location": "https://other.example"})
    with pytest.raises(auth.CurrentUserApiTokenError, match="Refused a redirect"):
        credentials(session())
    http.replace(responses.POST, TOKEN_URL, status=502, body="<html>private</html>")
    with pytest.raises(auth.CurrentUserApiTokenError) as exc:
        credentials(session())
    assert "private" not in str(exc.value)


@pytest.mark.parametrize(
    "headers,expected",
    [
        ({"Host": f"streamlit-{APP_ID}.example"}, APP_ID),
        (
            {"Host": "localhost", "X-Original-Host": f"streamlit-{APP_ID}.example"},
            APP_ID,
        ),
        ({"Host": "localhost:8501"}, None),
        ({}, None),
    ],
)
def test_host_id_resolution(monkeypatch, headers, expected):
    monkeypatch.setitem(
        sys.modules,
        "streamlit",
        SimpleNamespace(context=SimpleNamespace(headers=headers)),
    )
    assert auth._read_streamlit_app_id_from_context() == expected


def test_session_state_lookup_suppresses_missing_context_warning(monkeypatch):
    calls = []

    def get_ctx(*, suppress_warning):
        calls.append(suppress_warning)
        return None

    monkeypatch.setitem(sys.modules, "streamlit", SimpleNamespace())
    monkeypatch.setitem(
        sys.modules,
        "streamlit.runtime.scriptrunner",
        SimpleNamespace(get_script_run_ctx=get_ctx),
    )
    assert auth._read_streamlit_session_state() is None
    assert calls == [True]
