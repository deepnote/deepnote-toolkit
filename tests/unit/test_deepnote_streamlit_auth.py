import sys
import time
from types import SimpleNamespace

import pytest
import requests
import responses

from deepnote_toolkit.streamlit import auth
from tests.unit.helpers.notebook_api import session
from tests.unit.helpers.streamlit_runtime import FakeStreamlitRuntime

APP_ID = "3853c7f5-2048-4b57-946d-6c5592c3317e"
TOKEN_URL = f"http://localhost:19456/userpod-api/streamlit-apps/{APP_ID}/api-token"


@pytest.fixture
def runtime():
    return FakeStreamlitRuntime(app=APP_ID, cookie="cookie")


@pytest.fixture
def http():
    with responses.RequestsMock() as mock:
        yield mock


def credentials(http_session, runtime):
    return auth.current_user_api_credentials(session=http_session, runtime=runtime)


def payload(**overrides):
    return {
        "token": "viewer",
        "apiOrigin": "https://api.deepnote-staging.com/",
        "expiresAtSeconds": time.time() + 900,
        **overrides,
    }


def test_exchange_uses_cookie_and_reuses_credentials_only_in_same_session(
    http, runtime
):
    http.post(TOKEN_URL, json=payload())
    transport = session()
    first = credentials(transport, runtime)
    assert credentials(transport, runtime) is first
    assert (
        first.token == "viewer"
        and first.api_origin == "https://api.deepnote-staging.com"
    )
    assert len(http.calls) == 1
    assert http.calls[0].request.headers["StreamlitToken"] == "cookie"
    assert "Authorization" not in http.calls[0].request.headers
    runtime.state.clear()
    assert credentials(transport, runtime) is not first
    assert len(http.calls) == 2


def test_changed_cookie_or_expiry_refreshes_credentials(http, runtime):
    http.post(TOKEN_URL, json=payload(expiresAtSeconds=time.time() + 30))
    http.post(TOKEN_URL, json=payload(token="second"))
    http.post(TOKEN_URL, json=payload(token="third"))
    transport = session()
    assert credentials(transport, runtime).token == "viewer"
    assert credentials(transport, runtime).token == "second"
    runtime.cookie = "changed"
    assert credentials(transport, runtime).token == "third"


@pytest.mark.parametrize(
    "cache_shape",
    ["empty", "short", "long", "none", "text", "mapping", "old_type", "list"],
)
def test_malformed_cached_credentials_are_refreshed(
    http: responses.RequestsMock,
    runtime: FakeStreamlitRuntime,
    cache_shape: str,
) -> None:
    """Replace malformed or stale session state with a fresh credential exchange."""
    http.post(TOKEN_URL, json=payload(token="old"))
    http.post(TOKEN_URL, json=payload(token="fresh"))
    with session() as transport:
        initial = credentials(transport, runtime)
        assert runtime.state is not None
        key, _ = runtime.state[auth._SESSION_STATE_KEY]
        malformed = {
            "empty": (),
            "short": (key,),
            "long": (key, initial, "extra"),
            "none": (key, None),
            "text": (key, "old-token"),
            "mapping": (key, {"expires_at_seconds": time.time() + 900}),
            "old_type": (key, SimpleNamespace(**vars(initial))),
            "list": [key, initial],
        }
        runtime.state[auth._SESSION_STATE_KEY] = malformed[cache_shape]

        refreshed = credentials(transport, runtime)
        assert refreshed.token == "fresh"
        assert runtime.state[auth._SESSION_STATE_KEY] == (key, refreshed)
        assert credentials(transport, runtime) is refreshed
    assert len(http.calls) == 2


@pytest.mark.parametrize("value", ["bad/path", "../apps", "", "x?query", "x#fragment"])
def test_app_id_is_validated_before_network(http, runtime, value):
    runtime.app = value
    with pytest.raises(auth.CurrentUserApiTokenError):
        credentials(session(), runtime)
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
        {"apiOrigin": "https://example.com/?next=1"},
        {"apiOrigin": "ftp://example.com"},
        {"apiOrigin": "https://[::1"},
        {"apiOrigin": "https://example.com;/"},
        {"apiOrigin": "https://example.com;"},
        {"apiOrigin": "https://exam;ple.com/"},
        {"apiOrigin": "https://example.com:8443;/"},
        {"apiOrigin": "https://example.com:invalid/"},
        {"apiOrigin": "https://example.com:65536/"},
        {"apiOrigin": "https://exa mple.com/"},
        {"apiOrigin": "https://example.com\t/"},
        {"apiOrigin": "https://example.com\\other/"},
    ],
)
def test_malformed_credentials_are_not_cached(http, runtime, overrides):
    http.post(TOKEN_URL, json=payload(**overrides))
    with pytest.raises(auth.CurrentUserApiTokenError):
        credentials(session(), runtime)
    assert runtime.state == {}


@pytest.mark.parametrize("suffix", ["", "/", "/?", "/#", "?#"])
@pytest.mark.parametrize(
    "origin,expected",
    [
        ("HTTPS://api.example.com:8443", "https://api.example.com:8443"),
        ("http://localhost:8080", "http://localhost:8080"),
        ("https://[::1]:8443", "https://[::1]:8443"),
    ],
)
def test_api_origin_is_reduced_to_scheme_and_host(
    http: responses.RequestsMock,
    runtime: FakeStreamlitRuntime,
    suffix: str,
    origin: str,
    expected: str,
) -> None:
    """Preserve hosts and ports while removing empty URL components."""
    http.post(TOKEN_URL, json=payload(apiOrigin=origin + suffix))
    assert credentials(session(), runtime).api_origin == expected


@pytest.mark.parametrize(
    "status,transient", [(401, False), (403, False), (429, True), (503, True)]
)
def test_exchange_preserves_server_reason_and_retry_classification(
    http, runtime, status, transient
):
    http.post(
        TOKEN_URL,
        status=status,
        json={"message": "API access is not available for this app"},
    )
    with pytest.raises(
        auth.CurrentUserApiTokenError, match="API access is not available"
    ) as exc:
        credentials(session(), runtime)
    assert exc.value.transient is transient
    assert len(http.calls) == 1


@pytest.mark.parametrize(
    "failure", [requests.Timeout(), requests.ConnectionError("closed")]
)
def test_exchange_network_failures_are_transient(http, runtime, failure):
    http.post(TOKEN_URL, body=failure)
    with pytest.raises(auth.CurrentUserApiTokenError) as exc:
        credentials(session(), runtime)
    assert exc.value.transient


def test_exchange_never_follows_redirects_or_exposes_html(http, runtime):
    http.post(TOKEN_URL, status=302, headers={"Location": "https://other.example"})
    with pytest.raises(auth.CurrentUserApiTokenError, match="Refused a redirect"):
        credentials(session(), runtime)
    http.replace(responses.POST, TOKEN_URL, status=502, body="<html>private</html>")
    with pytest.raises(auth.CurrentUserApiTokenError) as exc:
        credentials(session(), runtime)
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
    assert auth.streamlit_runtime.session_state() is None
    assert calls == [True]


def test_credential_validation_traceback_does_not_expose_bearer(http, runtime):
    import traceback

    http.post(TOKEN_URL, json=payload(token={"secret": "private-token"}))
    with pytest.raises(auth.CurrentUserApiTokenError) as exc:
        credentials(session(), runtime)
    assert "private-token" not in "".join(traceback.format_exception(exc.value))
