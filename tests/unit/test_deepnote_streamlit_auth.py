from __future__ import annotations

import io
import json
import sys
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch
from urllib.error import HTTPError

import pytest

from deepnote_toolkit.streamlit import (
    CurrentUserApiTokenError,
    current_user_api_credentials,
    current_user_api_token,
)
from deepnote_toolkit.streamlit.auth import (
    _read_streamlit_app_id_from_context,
)

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
        webapp_url="https://deepnote-staging.com/",
        streamlit_token="opaque-cookie",
        timeout=7,
        opener=open_request,
    )

    assert captured["url"] == (
        f"https://deepnote-staging.com/api/streamlit-apps/{APP_ID}/api-token"
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


def test_public_token_provider_exchanges_on_every_call() -> None:
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
            "deepnote_toolkit.streamlit.auth._read_streamlit_token_from_context",
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
            webapp_url="https://deepnote.com",
            streamlit_token="opaque-cookie",
            opener=lambda *_args, **_kwargs: FakeResponse(payload),
        )


def test_exchange_error_does_not_expose_response_body() -> None:
    secret_response = "must-not-leak"

    def open_request(*_args: Any, **_kwargs: Any) -> FakeResponse:
        raise HTTPError(
            "https://deepnote.com/api/streamlit-apps/id/api-token",
            401,
            "Unauthorized",
            {},
            io.BytesIO(json.dumps({"error": secret_response}).encode()),
        )

    with pytest.raises(CurrentUserApiTokenError) as exc_info:
        current_user_api_credentials(
            app_id=APP_ID,
            webapp_url="https://deepnote.com",
            streamlit_token="opaque-cookie",
            opener=open_request,
        )

    assert "HTTP 401" in str(exc_info.value)
    assert secret_response not in str(exc_info.value)
