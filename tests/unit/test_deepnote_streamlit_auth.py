from __future__ import annotations

import io
import json
import sys
import time
from http.client import RemoteDisconnected
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch
from urllib.error import HTTPError

import pytest

from deepnote_toolkit.streamlit import (
    CurrentUserApiCredentials,
    CurrentUserApiTokenError,
    current_user_api_credentials,
    current_user_api_token,
)
from deepnote_toolkit.streamlit.auth import _read_streamlit_app_id_from_context

APP_ID = "3853c7f5-2048-4b57-946d-6c5592c3317e"


class FakeResponse:
    def __init__(self, payload: Any):
        self.payload = payload

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return json.dumps(self.payload).encode()


def test_reads_app_id_from_original_host_before_host() -> None:
    streamlit = SimpleNamespace(
        context=SimpleNamespace(
            headers={
                "Host": "streamlit-00000000-0000-0000-0000-000000000000.example",
                "X-Original-Host": f"streamlit-{APP_ID}.deepnote.com",
            }
        )
    )

    with patch.dict(sys.modules, {"streamlit": streamlit}):
        assert _read_streamlit_app_id_from_context() == APP_ID


def test_reads_app_id_from_host_fallback() -> None:
    streamlit = SimpleNamespace(
        context=SimpleNamespace(
            headers={"host": f"streamlit-{APP_ID}.deepnote.com:443"}
        )
    )

    with patch.dict(sys.modules, {"streamlit": streamlit}):
        assert _read_streamlit_app_id_from_context() == APP_ID


@pytest.mark.parametrize(
    "streamlit",
    [
        SimpleNamespace(context=SimpleNamespace(headers={})),
        SimpleNamespace(context=SimpleNamespace(headers={"host": "localhost:8501"})),
    ],
)
def test_app_id_is_unavailable_outside_hosted_app(streamlit: object) -> None:
    with patch.dict(sys.modules, {"streamlit": streamlit}):
        assert _read_streamlit_app_id_from_context() is None


def test_exchanges_opaque_cookie_for_public_api_credentials() -> None:
    captured = {}

    def open_request(request: Any, *, timeout: float) -> FakeResponse:
        captured["url"] = request.full_url
        captured["method"] = request.method
        captured["headers"] = dict(request.header_items())
        captured["body"] = request.data
        captured["timeout"] = timeout
        return FakeResponse(
            {
                "token": "viewer-api-token",
                "apiOrigin": "https://api.deepnote-staging.com/",
                "expiresAtSeconds": 1_800_000_000,
            }
        )

    credentials = current_user_api_credentials(
        app_id=APP_ID,
        streamlit_token="opaque-cookie",
        timeout=7,
        opener=open_request,
    )

    assert captured["url"] == (
        f"http://localhost:19456/userpod-api/streamlit-apps/{APP_ID}/api-token"
    )
    assert captured["method"] == "POST"
    assert captured["body"] == b""
    assert captured["timeout"] == 7
    headers = {key.lower(): value for key, value in captured["headers"].items()}
    assert headers["streamlittoken"] == "opaque-cookie"
    assert "authorization" not in headers
    assert credentials.token == "viewer-api-token"
    assert credentials.api_origin == "https://api.deepnote-staging.com"
    assert credentials.expires_at_seconds == 1_800_000_000


def _hosted_session_modules(session_state: dict[str, Any]) -> dict[str, Any]:
    scriptrunner = SimpleNamespace(get_script_run_ctx=lambda: object())
    return {
        "streamlit": SimpleNamespace(session_state=session_state),
        "streamlit.runtime": SimpleNamespace(scriptrunner=scriptrunner),
        "streamlit.runtime.scriptrunner": scriptrunner,
    }


def _counting_opener(expires_at_seconds: float) -> tuple[list[Any], Any]:
    requests: list[Any] = []

    def open_request(request: Any, *, timeout: float) -> FakeResponse:
        requests.append(request)
        return FakeResponse(
            {
                "token": f"viewer-api-token-{len(requests)}",
                "apiOrigin": "https://api.deepnote.com",
                "expiresAtSeconds": expires_at_seconds,
            }
        )

    return requests, open_request


def test_reuses_credentials_within_a_streamlit_session() -> None:
    requests, open_request = _counting_opener(time.time() + 15 * 60)

    with patch.dict(sys.modules, _hosted_session_modules({})):
        first = current_user_api_credentials(
            app_id=APP_ID, streamlit_token="opaque-cookie", opener=open_request
        )
        second = current_user_api_credentials(
            app_id=APP_ID, streamlit_token="opaque-cookie", opener=open_request
        )

    assert len(requests) == 1
    assert second == first


def test_does_not_share_credentials_between_sessions() -> None:
    requests, open_request = _counting_opener(time.time() + 15 * 60)

    for _session in range(2):
        with patch.dict(sys.modules, _hosted_session_modules({})):
            current_user_api_credentials(
                app_id=APP_ID, streamlit_token="opaque-cookie", opener=open_request
            )

    assert len(requests) == 2


@pytest.mark.parametrize(
    ("expires_in_seconds", "second_cookie"),
    [(30, "opaque-cookie"), (15 * 60, "another-cookie")],
)
def test_exchanges_again_near_expiry_or_for_another_cookie(
    expires_in_seconds: int, second_cookie: str
) -> None:
    requests, open_request = _counting_opener(time.time() + expires_in_seconds)

    with patch.dict(sys.modules, _hosted_session_modules({})):
        current_user_api_credentials(
            app_id=APP_ID, streamlit_token="opaque-cookie", opener=open_request
        )
        current_user_api_credentials(
            app_id=APP_ID, streamlit_token=second_cookie, opener=open_request
        )

    assert len(requests) == 2


def test_public_token_provider_returns_the_current_credentials_token() -> None:
    with patch(
        "deepnote_toolkit.streamlit.auth.current_user_api_credentials"
    ) as exchange:
        exchange.side_effect = [
            SimpleNamespace(token="first"),
            SimpleNamespace(token="second"),
        ]

        assert current_user_api_token() == "first"
        assert current_user_api_token() == "second"

    assert exchange.call_count == 2


def test_exchange_requires_hosted_streamlit_context() -> None:
    with (
        patch(
            "deepnote_toolkit.streamlit.auth._read_streamlit_app_id_from_context",
            return_value=None,
        ),
        pytest.raises(CurrentUserApiTokenError, match="app ID"),
    ):
        current_user_api_token()


def test_exchange_requires_viewer_cookie() -> None:
    with (
        patch(
            "deepnote_toolkit.streamlit.auth._read_streamlit_app_id_from_context",
            return_value=APP_ID,
        ),
        patch(
            "deepnote_toolkit.streamlit.auth.read_streamlit_token_from_context",
            return_value=None,
        ),
        pytest.raises(CurrentUserApiTokenError, match="streamlit-token"),
    ):
        current_user_api_token()


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"token": "token"},
        {
            "token": "token",
            "apiOrigin": "javascript:alert(1)",
            "expiresAtSeconds": 123,
        },
        {
            "token": "token",
            "apiOrigin": "https://api.deepnote.com/unexpected",
            "expiresAtSeconds": 123,
        },
        {
            "token": "token",
            "apiOrigin": "https://api.deepnote.com?secret=value",
            "expiresAtSeconds": 123,
        },
    ],
)
def test_exchange_rejects_invalid_response(payload: dict[str, Any]) -> None:
    with pytest.raises(CurrentUserApiTokenError):
        current_user_api_credentials(
            app_id=APP_ID,
            streamlit_token="opaque-cookie",
            opener=lambda *_args, **_kwargs: FakeResponse(payload),
        )


def test_exchange_error_includes_the_server_message() -> None:
    def open_request(*_args: Any, **_kwargs: Any) -> FakeResponse:
        raise HTTPError(
            "http://localhost:19456/userpod-api/streamlit-apps/id/api-token",
            403,
            "Forbidden",
            {},
            io.BytesIO(
                json.dumps(
                    {"error": "API access is not available for this app"}
                ).encode()
            ),
        )

    with pytest.raises(CurrentUserApiTokenError) as exc_info:
        current_user_api_credentials(
            app_id=APP_ID,
            streamlit_token="opaque-cookie",
            opener=open_request,
        )

    assert str(exc_info.value) == (
        "Current viewer API-token exchange returned HTTP 403: "
        "API access is not available for this app"
    )


def test_exchange_error_does_not_expose_a_raw_response_body() -> None:
    def open_request(*_args: Any, **_kwargs: Any) -> FakeResponse:
        raise HTTPError(
            "http://localhost:19456/userpod-api/streamlit-apps/id/api-token",
            502,
            "Bad Gateway",
            {},
            io.BytesIO(b"<html>must-not-leak</html>"),
        )

    with pytest.raises(CurrentUserApiTokenError) as exc_info:
        current_user_api_credentials(
            app_id=APP_ID,
            streamlit_token="opaque-cookie",
            opener=open_request,
        )

    assert str(exc_info.value) == "Current viewer API-token exchange returned HTTP 502."


def test_credentials_repr_hides_the_token() -> None:
    credentials = CurrentUserApiCredentials(
        token="secret-token",
        api_origin="https://api.deepnote.com",
        expires_at_seconds=1_800_000_000,
    )

    assert "secret-token" not in repr(credentials)


def test_dropped_connection_during_exchange_is_transient() -> None:
    def open_request(_request: Any, *, timeout: float) -> Any:
        raise RemoteDisconnected("Remote end closed connection without response")

    with pytest.raises(CurrentUserApiTokenError) as exc_info:
        current_user_api_credentials(
            app_id="11111111-2222-3333-4444-555555555555",
            streamlit_token="cookie",
            opener=open_request,
        )

    assert exc_info.value.transient is True


def test_exchange_prefers_the_app_id_exported_by_the_launcher(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEEPNOTE_STREAMLIT_APP_ID", APP_ID.upper())
    urls = []

    def open_request(request: Any, *, timeout: float) -> FakeResponse:
        urls.append(request.full_url)
        return FakeResponse(
            {
                "token": "viewer-api-token",
                "apiOrigin": "https://api.deepnote.com",
                "expiresAtSeconds": 1_800_000_000,
            }
        )

    with patch(
        "deepnote_toolkit.streamlit.auth._read_streamlit_app_id_from_context",
        return_value="00000000-0000-0000-0000-000000000000",
    ):
        current_user_api_credentials(
            streamlit_token="opaque-cookie", opener=open_request
        )

    assert urls == [
        f"http://localhost:19456/userpod-api/streamlit-apps/{APP_ID}/api-token"
    ]
